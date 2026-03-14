// Copyright (C) 2014 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	ldap "github.com/go-ldap/ldap/v3"
	"github.com/syncthing/syncthing/internal/cloudreve"
	"github.com/syncthing/syncthing/internal/db"
	"github.com/syncthing/syncthing/internal/slogutil"
	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/events"
	libmodel "github.com/syncthing/syncthing/lib/model"
	"github.com/syncthing/syncthing/lib/osutil"
	"github.com/syncthing/syncthing/lib/rand"
)

const (
	maxSessionLifetime         = 7 * 24 * time.Hour
	maxActiveSessions          = 25
	randomTokenLength          = 64
	maxLoginRequestSize        = 1 << 10 // one kibibyte for username+password
	cloudreveStateCookieMaxAge = int((10 * time.Minute) / time.Second)
	cloudreveAuthDetailMaxLen  = 512
)

const (
	cloudreveAuthErrorDenied        = "denied"
	cloudreveAuthErrorFailed        = "failed"
	cloudreveAuthErrorNotConfigured = "notConfigured"
	cloudreveAuthErrorStateExpired  = "stateExpired"
)

func emitLoginAttempt(success bool, username string, r *http.Request, evLogger events.Logger) {
	remoteAddress, proxy := remoteAddress(r)
	evData := map[string]any{
		"success":       success,
		"username":      username,
		"remoteAddress": remoteAddress,
	}
	if proxy != "" {
		evData["proxy"] = proxy
	}
	evLogger.Log(events.LoginAttempt, evData)

	if success {
		return
	}
	l := slog.Default().With(slogutil.Address(remoteAddress), slog.String("username", username))
	if proxy != "" {
		l = l.With("proxy", proxy)
	}
	l.Warn("Bad credentials supplied during API authorization")
}

func remoteAddress(r *http.Request) (remoteAddr, proxy string) {
	remoteAddr = r.RemoteAddr
	remoteIP := osutil.IPFromString(r.RemoteAddr)

	// parse X-Forwarded-For only if the proxy connects via unix socket, localhost or a LAN IP
	var localProxy bool
	if remoteIP != nil {
		remoteAddr = remoteIP.String()
		localProxy = remoteIP.IsLoopback() || remoteIP.IsPrivate() || remoteIP.IsLinkLocalUnicast()
	} else if remoteAddr == "@" {
		localProxy = true
	}

	if !localProxy {
		return
	}

	forwardedAddr, _, _ := strings.Cut(r.Header.Get("X-Forwarded-For"), ",")
	forwardedAddr = strings.TrimSpace(forwardedAddr)
	forwardedIP := osutil.IPFromString(forwardedAddr)

	if forwardedIP != nil {
		proxy = remoteAddr
		remoteAddr = forwardedIP.String()
	}
	return
}

func antiBruteForceSleep() {
	time.Sleep(time.Duration(rand.Intn(100)+100) * time.Millisecond)
}

func unauthorized(w http.ResponseWriter, shortID string) {
	w.Header().Set("WWW-Authenticate", fmt.Sprintf(`Basic realm="Authorization Required (%s)"`, shortID))
	http.Error(w, "Not Authorized", http.StatusUnauthorized)
}

func forbidden(w http.ResponseWriter) {
	http.Error(w, "Forbidden", http.StatusForbidden)
}

func isNoAuthPath(path string, metricsWithoutAuth bool) bool {
	// Local variable instead of module var to prevent accidental mutation
	noAuthPaths := []string{
		"/",
		"/index.html",
		"/modal.html",
		"/rest/svc/lang", // Required to load language settings on login page
	}

	if metricsWithoutAuth {
		noAuthPaths = append(noAuthPaths, "/metrics")
	}

	// Local variable instead of module var to prevent accidental mutation
	noAuthPrefixes := []string{
		// Static assets
		"/assets/",
		"/syncthing/",
		"/vendor/",
		"/theme-assets/", // This leaks information from config, but probably not sensitive

		// No-auth API endpoints
		"/rest/noauth",
	}

	return slices.Contains(noAuthPaths, path) ||
		slices.ContainsFunc(noAuthPrefixes, func(prefix string) bool {
			return strings.HasPrefix(path, prefix)
		})
}

type basicAuthAndSessionMiddleware struct {
	tokenCookieManager *tokenCookieManager
	guiCfg             config.GUIConfiguration
	ldapCfg            config.LDAPConfiguration
	model              libmodel.Model
	cloudreveOAuth     *cloudreve.OAuthManager
	cloudreveStateName string
	cloudreveStayName  string
	cloudrevePKCEName  string
	next               http.Handler
	evLogger           events.Logger
}

func newBasicAuthAndSessionMiddleware(tokenCookieManager *tokenCookieManager, guiCfg config.GUIConfiguration, ldapCfg config.LDAPConfiguration, cfg config.Wrapper, miscDB *db.Typed, model libmodel.Model, next http.Handler, evLogger events.Logger) *basicAuthAndSessionMiddleware {
	return &basicAuthAndSessionMiddleware{
		tokenCookieManager: tokenCookieManager,
		guiCfg:             guiCfg,
		ldapCfg:            ldapCfg,
		model:              model,
		cloudreveOAuth:     cloudreve.NewOAuthManager(cfg, miscDB, nil),
		cloudreveStateName: "cloudreve-auth-state-" + tokenCookieManager.shortID,
		cloudreveStayName:  "cloudreve-auth-stay-" + tokenCookieManager.shortID,
		cloudrevePKCEName:  "cloudreve-auth-pkce-" + tokenCookieManager.shortID,
		next:               next,
		evLogger:           evLogger,
	}
}

func (m *basicAuthAndSessionMiddleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if hasValidAPIKeyHeader(r, m.guiCfg) {
		m.next.ServeHTTP(w, r)
		return
	}

	if m.tokenCookieManager.hasValidSession(r) {
		m.next.ServeHTTP(w, r)
		return
	}

	// Fall back to Basic auth if provided
	if username, ok := attemptBasicAuth(r, m.guiCfg, m.ldapCfg, m.evLogger); ok {
		m.tokenCookieManager.createSession(username, false, w, r)
		m.next.ServeHTTP(w, r)
		return
	}

	// Exception for static assets and REST calls that don't require authentication.
	if isNoAuthPath(r.URL.Path, m.guiCfg.MetricsWithoutAuth) {
		m.next.ServeHTTP(w, r)
		return
	}

	// Some browsers don't send the Authorization request header unless prompted by a 401 response.
	// This enables https://user:pass@localhost style URLs to keep working.
	if m.guiCfg.SendBasicAuthPrompt {
		unauthorized(w, m.tokenCookieManager.shortID)
		return
	}

	forbidden(w)
}

func (m *basicAuthAndSessionMiddleware) passwordAuthHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Username     string
		Password     string
		StayLoggedIn bool
	}
	if err := unmarshalTo(http.MaxBytesReader(w, r.Body, maxLoginRequestSize), &req); err != nil {
		l.Debugln("Failed to parse username and password:", err)
		http.Error(w, "Failed to parse username and password.", http.StatusBadRequest)
		return
	}

	if auth(req.Username, req.Password, m.guiCfg, m.ldapCfg) {
		m.tokenCookieManager.createSession(req.Username, req.StayLoggedIn, w, r)
		w.WriteHeader(http.StatusNoContent)
		return
	}

	emitLoginAttempt(false, req.Username, r, m.evLogger)
	antiBruteForceSleep()
	forbidden(w)
}

func attemptBasicAuth(r *http.Request, guiCfg config.GUIConfiguration, ldapCfg config.LDAPConfiguration, evLogger events.Logger) (string, bool) {
	username, password, ok := r.BasicAuth()
	if !ok {
		return "", false
	}

	slog.Debug("Sessionless HTTP request with authentication; this is expensive.")

	if auth(username, password, guiCfg, ldapCfg) {
		return username, true
	}

	usernameFromIso := string(iso88591ToUTF8([]byte(username)))
	passwordFromIso := string(iso88591ToUTF8([]byte(password)))
	if auth(usernameFromIso, passwordFromIso, guiCfg, ldapCfg) {
		return usernameFromIso, true
	}

	emitLoginAttempt(false, username, r, evLogger)
	antiBruteForceSleep()
	return "", false
}

func (m *basicAuthAndSessionMiddleware) handleLogout(w http.ResponseWriter, r *http.Request) {
	m.tokenCookieManager.destroySession(w, r)
	w.WriteHeader(http.StatusNoContent)
}

func (m *basicAuthAndSessionMiddleware) cloudreveAuthStatusHandler(w http.ResponseWriter, _ *http.Request) {
	sendJSON(w, m.cloudreveOAuth.Status())
}

func (m *basicAuthAndSessionMiddleware) cloudreveAuthLoginHandler(w http.ResponseWriter, r *http.Request) {
	stayLoggedIn, _ := strconv.ParseBool(r.URL.Query().Get("stayLoggedIn"))
	state := rand.String(randomTokenLength)
	codeVerifier := rand.String(randomTokenLength)
	codeChallenge := cloudreve.PKCECodeChallenge(codeVerifier)

	authURL, err := m.cloudreveOAuth.AuthorizationURL(r, state, codeChallenge)
	if err != nil {
		slog.Warn("Failed to start Cloudreve OAuth login", slogutil.Error(err))
		m.redirectCloudreveAuthError(w, r, cloudreveAuthErrorNotConfigured, "")
		return
	}

	m.setCookie(w, r, m.cloudreveStateName, state, cloudreveStateCookieMaxAge)
	m.setCookie(w, r, m.cloudrevePKCEName, codeVerifier, cloudreveStateCookieMaxAge)
	if stayLoggedIn {
		m.setCookie(w, r, m.cloudreveStayName, "1", cloudreveStateCookieMaxAge)
	} else {
		m.setCookie(w, r, m.cloudreveStayName, "0", cloudreveStateCookieMaxAge)
	}

	http.Redirect(w, r, authURL, http.StatusFound)
}

func (m *basicAuthAndSessionMiddleware) cloudreveAuthCallbackHandler(w http.ResponseWriter, r *http.Request) {
	defer m.clearCookie(w, r, m.cloudreveStateName)
	defer m.clearCookie(w, r, m.cloudreveStayName)
	defer m.clearCookie(w, r, m.cloudrevePKCEName)

	if r.URL.Query().Get("error") != "" {
		m.redirectCloudreveAuthError(w, r, cloudreveAuthErrorDenied, "")
		return
	}

	state, ok := m.cookieValue(r, m.cloudreveStateName)
	if !ok || state == "" || state != r.URL.Query().Get("state") {
		m.redirectCloudreveAuthError(w, r, cloudreveAuthErrorStateExpired, "")
		return
	}

	code := strings.TrimSpace(r.URL.Query().Get("code"))
	if code == "" {
		m.redirectCloudreveAuthError(w, r, cloudreveAuthErrorFailed, "")
		return
	}

	codeVerifier, _ := m.cookieValue(r, m.cloudrevePKCEName)
	session, err := m.cloudreveOAuth.ExchangeCode(r.Context(), code, cloudreve.RedirectURIForRequest(r), codeVerifier)
	if err != nil {
		slog.Warn("Cloudreve OAuth token exchange failed", slogutil.Error(err))
		m.redirectCloudreveAuthError(w, r, cloudreveAuthErrorFailed, cloudreveAuthErrorDetail(err))
		return
	}

	if m.guiCfg.IsAuthEnabled() && !m.tokenCookieManager.hasValidSession(r) {
		stayLoggedIn, _ := m.cookieValue(r, m.cloudreveStayName)
		m.tokenCookieManager.createSession(firstNonEmpty(session.UserName, session.UserEmail, session.UserSub, "cloudreve"), stayLoggedIn == "1", w, r)
	}

	if m.model != nil {
		go m.model.ScanFolders()
	}

	http.Redirect(w, r, "/", http.StatusFound)
}

func (m *basicAuthAndSessionMiddleware) redirectCloudreveAuthError(w http.ResponseWriter, r *http.Request, reason, detail string) {
	query := url.Values{
		"cloudreveAuthError": []string{reason},
	}
	if detail != "" {
		query.Set("cloudreveAuthErrorDetail", detail)
	}
	http.Redirect(w, r, "/?"+query.Encode(), http.StatusFound)
}

func cloudreveAuthErrorDetail(err error) string {
	if err == nil {
		return ""
	}

	msg := strings.TrimSpace(err.Error())
	msg = strings.TrimPrefix(msg, "cloudreve oauth request failed: ")
	msg = strings.TrimSpace(msg)
	if msg == "" {
		return ""
	}
	if len(msg) > cloudreveAuthDetailMaxLen {
		msg = msg[:cloudreveAuthDetailMaxLen-3] + "..."
	}
	return msg
}

func (m *basicAuthAndSessionMiddleware) cookieValue(r *http.Request, name string) (string, bool) {
	cookie, err := r.Cookie(name)
	if err != nil {
		return "", false
	}
	return cookie.Value, true
}

func (m *basicAuthAndSessionMiddleware) setCookie(w http.ResponseWriter, r *http.Request, name, value string, maxAge int) {
	http.SetCookie(w, &http.Cookie{
		Name:     name,
		Value:    value,
		Path:     "/",
		MaxAge:   maxAge,
		HttpOnly: true,
		Secure:   m.useSecureCookie(r),
		SameSite: http.SameSiteLaxMode,
	})
}

func (m *basicAuthAndSessionMiddleware) clearCookie(w http.ResponseWriter, r *http.Request, name string) {
	http.SetCookie(w, &http.Cookie{
		Name:     name,
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		HttpOnly: true,
		Secure:   m.useSecureCookie(r),
		SameSite: http.SameSiteLaxMode,
	})
}

func (m *basicAuthAndSessionMiddleware) useSecureCookie(r *http.Request) bool {
	connectionIsHTTPS := r.TLS != nil ||
		strings.ToLower(r.Header.Get("X-Forwarded-Proto")) == "https" ||
		strings.Contains(strings.ToLower(r.Header.Get("Forwarded")), "proto=https")
	return connectionIsHTTPS || m.guiCfg.UseTLS()
}

func auth(username string, password string, guiCfg config.GUIConfiguration, ldapCfg config.LDAPConfiguration) bool {
	if guiCfg.AuthMode == config.AuthModeLDAP {
		return authLDAP(username, password, ldapCfg)
	} else {
		return authStatic(username, password, guiCfg)
	}
}

func authStatic(username string, password string, guiCfg config.GUIConfiguration) bool {
	return guiCfg.CompareHashedPassword(password) == nil && username == guiCfg.User
}

func authLDAP(username string, password string, cfg config.LDAPConfiguration) bool {
	address := cfg.Address
	hostname, _, err := net.SplitHostPort(address)
	if err != nil {
		hostname = address
	}
	var connection *ldap.Conn
	if cfg.Transport == config.LDAPTransportTLS {
		connection, err = ldap.DialTLS("tcp", address, &tls.Config{
			ServerName:         hostname,
			InsecureSkipVerify: cfg.InsecureSkipVerify,
		})
	} else {
		connection, err = ldap.Dial("tcp", address)
	}

	if err != nil {
		slog.Error("Failed to dial LDAP server", slogutil.Error(err))
		return false
	}

	if cfg.Transport == config.LDAPTransportStartTLS {
		err = connection.StartTLS(&tls.Config{InsecureSkipVerify: cfg.InsecureSkipVerify})
		if err != nil {
			slog.Error("Failed to handshake start TLS With LDAP server", slogutil.Error(err))
			return false
		}
	}

	defer connection.Close()

	bindDN := formatOptionalPercentS(cfg.BindDN, escapeForLDAPDN(username))
	err = connection.Bind(bindDN, password)
	if err != nil {
		slog.Error("Failed to bind with LDAP server", slogutil.Error(err))
		return false
	}

	if cfg.SearchFilter == "" && cfg.SearchBaseDN == "" {
		// We're done here.
		return true
	}

	if cfg.SearchFilter == "" || cfg.SearchBaseDN == "" {
		slog.Error("Bad LDAP configuration: both searchFilter and searchBaseDN must be set, or neither")
		return false
	}

	// If a search filter and search base is set we do an LDAP search for
	// the user. If this matches precisely one user then we are good to go.
	// The search filter uses the same %s interpolation as the bind DN.

	searchString := formatOptionalPercentS(cfg.SearchFilter, escapeForLDAPFilter(username))
	const sizeLimit = 2  // we search for up to two users -- we only want to match one, so getting any number >1 is a failure.
	const timeLimit = 60 // Search for up to a minute...
	searchReq := ldap.NewSearchRequest(cfg.SearchBaseDN, ldap.ScopeWholeSubtree, ldap.DerefFindingBaseObj, sizeLimit, timeLimit, false, searchString, nil, nil)

	res, err := connection.Search(searchReq)
	if err != nil {
		slog.Warn("Failed LDAP search", slogutil.Error(err))
		return false
	}
	if len(res.Entries) != 1 {
		slog.Warn("Incorrect number of LDAP search results (expected one)", slog.Int("results", len(res.Entries)))
		return false
	}

	return true
}

// escapeForLDAPFilter escapes a value that will be used in a filter clause
func escapeForLDAPFilter(value string) string {
	// https://social.technet.microsoft.com/wiki/contents/articles/5392.active-directory-ldap-syntax-filters.aspx#Special_Characters
	// Backslash must always be first in the list so we don't double escape them.
	return escapeRunes(value, []rune{'\\', '*', '(', ')', 0})
}

// escapeForLDAPDN escapes a value that will be used in a bind DN
func escapeForLDAPDN(value string) string {
	// https://social.technet.microsoft.com/wiki/contents/articles/5312.active-directory-characters-to-escape.aspx
	// Backslash must always be first in the list so we don't double escape them.
	return escapeRunes(value, []rune{'\\', ',', '#', '+', '<', '>', ';', '"', '=', ' ', 0})
}

func escapeRunes(value string, runes []rune) string {
	for _, e := range runes {
		value = strings.ReplaceAll(value, string(e), fmt.Sprintf("\\%X", e))
	}
	return value
}

func formatOptionalPercentS(template string, username string) string {
	var replacements []any
	nReps := strings.Count(template, "%s") - strings.Count(template, "%%s")
	if nReps < 0 {
		nReps = 0
	}
	for range nReps {
		replacements = append(replacements, username)
	}
	return fmt.Sprintf(template, replacements...)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

// Convert an ISO-8859-1 encoded byte string to UTF-8. Works by the
// principle that ISO-8859-1 bytes are equivalent to unicode code points,
// that a rune slice is a list of code points, and that stringifying a slice
// of runes generates UTF-8 in Go.
func iso88591ToUTF8(s []byte) []byte {
	runes := make([]rune, len(s))
	for i := range s {
		runes[i] = rune(s[i])
	}
	return []byte(string(runes))
}
