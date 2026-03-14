// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package cloudreve

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/syncthing/syncthing/internal/db"
	"github.com/syncthing/syncthing/lib/config"
)

const (
	DefaultOAuthScopes       = "profile email openid offline_access UserInfo.Write Workflow.Write Files.Write Shares.Write"
	oauthSessionKey          = "cloudreve/oauth/session"
	oauthRefreshInterval     = time.Minute
	oauthRefreshLeeway       = 2 * time.Minute
	oauthStateCookieLifetime = 10 * time.Minute
)

var ErrOAuthNotConfigured = errors.New("cloudreve oauth is not configured")
var ErrOAuthNotAuthorized = errors.New("cloudreve oauth is not authorized")

type OAuthSession struct {
	AccessToken    string    `json:"accessToken"`
	RefreshToken   string    `json:"refreshToken"`
	AccessExpires  time.Time `json:"accessExpires"`
	RefreshExpires time.Time `json:"refreshExpires"`
	Scope          string    `json:"scope,omitempty"`
	UserSub        string    `json:"userSub,omitempty"`
	UserName       string    `json:"userName,omitempty"`
	UserEmail      string    `json:"userEmail,omitempty"`
}

type OAuthStatus struct {
	Available  bool   `json:"available"`
	Authorized bool   `json:"authorized"`
	Server     string `json:"server,omitempty"`
	UserName   string `json:"userName,omitempty"`
	UserEmail  string `json:"userEmail,omitempty"`
	Scope      string `json:"scope,omitempty"`
}

type oauthTokenExchangeResponse struct {
	AccessToken           string `json:"access_token"`
	TokenType             string `json:"token_type"`
	ExpiresIn             int64  `json:"expires_in"`
	RefreshTokenExpiresIn int64  `json:"refresh_token_expires_in"`
	RefreshToken          string `json:"refresh_token"`
	Scope                 string `json:"scope"`
}

type oauthUserInfoResponse struct {
	Sub               string `json:"sub"`
	Name              string `json:"name"`
	PreferredUsername string `json:"preferred_username"`
	Email             string `json:"email"`
}

type oauthRefreshRequest struct {
	RefreshToken string `json:"refresh_token"`
}

type oauthRefreshResponse struct {
	AccessToken    string    `json:"access_token"`
	RefreshToken   string    `json:"refresh_token"`
	AccessExpires  time.Time `json:"access_expires"`
	RefreshExpires time.Time `json:"refresh_expires"`
}

type OAuthManager struct {
	cfg        config.Wrapper
	store      *db.Typed
	httpClient *http.Client
	now        func() time.Time
	mut        sync.Mutex
}

func NewOAuthManager(cfg config.Wrapper, store *db.Typed, httpClient *http.Client) *OAuthManager {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &OAuthManager{
		cfg:        cfg,
		store:      store,
		httpClient: httpClient,
		now:        time.Now,
	}
}

func (m *OAuthManager) Serve(ctx context.Context) error {
	ticker := time.NewTicker(oauthRefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			_, _ = m.AccessToken(ctx)
		}
	}
}

func (m *OAuthManager) Status() OAuthStatus {
	cfg := m.config()
	status := OAuthStatus{
		Available: m.isConfigured(cfg),
		Server:    strings.TrimSpace(cfg.Server),
	}
	if !status.Available {
		return status
	}
	session, ok, err := m.LoadSession()
	if err == nil && ok && session.RefreshToken != "" && session.RefreshExpires.After(m.now()) {
		status.Authorized = true
		status.UserName = session.UserName
		status.UserEmail = session.UserEmail
		status.Scope = session.Scope
	}
	return status
}

func (m *OAuthManager) AuthorizationURL(r *http.Request, state string, codeChallenge string) (string, error) {
	cfg := m.config()
	if !m.isConfigured(cfg) {
		return "", ErrOAuthNotConfigured
	}
	rawURL := strings.TrimRight(cfg.Server, "/") + "/session/authorize"
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}
	q := u.Query()
	q.Set("client_id", cfg.OAuthClientID)
	q.Set("response_type", "code")
	q.Set("redirect_uri", RedirectURIForRequest(r))
	q.Set("scope", oauthScopes(cfg))
	q.Set("state", state)
	if codeChallenge != "" {
		q.Set("code_challenge", codeChallenge)
		q.Set("code_challenge_method", "S256")
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func RedirectURIForRequest(r *http.Request) string {
	scheme := "http"
	switch {
	case r.TLS != nil:
		scheme = "https"
	case strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https"):
		scheme = "https"
	case strings.Contains(strings.ToLower(r.Header.Get("Forwarded")), "proto=https"):
		scheme = "https"
	}

	host := r.Host
	if forwardedHost := strings.TrimSpace(r.Header.Get("X-Forwarded-Host")); forwardedHost != "" {
		host = strings.Split(forwardedHost, ",")[0]
	}

	return fmt.Sprintf("%s://%s/rest/noauth/auth/cloudreve/callback", scheme, strings.TrimSpace(host))
}

func (m *OAuthManager) ExchangeCode(ctx context.Context, code string, redirectURI string, codeVerifier string) (OAuthSession, error) {
	cfg := m.config()
	if !m.isConfigured(cfg) {
		return OAuthSession{}, ErrOAuthNotConfigured
	}

	form := url.Values{
		"client_id":     []string{cfg.OAuthClientID},
		"client_secret": []string{cfg.OAuthClientSecret},
		"grant_type":    []string{"authorization_code"},
		"code":          []string{code},
		"redirect_uri":  []string{redirectURI},
	}
	if strings.TrimSpace(codeVerifier) != "" {
		form.Set("code_verifier", strings.TrimSpace(codeVerifier))
	}
	var tokenResp oauthTokenExchangeResponse
	if err := m.sendFormRaw(ctx, cfg.Server, "/api/v4/session/oauth/token", form, &tokenResp); err != nil {
		if isInvalidScopeError(err) {
			return OAuthSession{}, fmt.Errorf("cloudreve oauth app scope mismatch; ensure the Cloudreve OAuth application explicitly includes `openid` and all requested scopes. current requested scopes: %q: %w", oauthScopes(cfg), err)
		}
		return OAuthSession{}, err
	}
	if tokenResp.AccessToken == "" {
		return OAuthSession{}, errors.New("cloudreve oauth did not return an access token")
	}
	if tokenResp.RefreshToken == "" {
		return OAuthSession{}, errors.New("cloudreve oauth did not return a refresh token; include offline_access in the OAuth app scopes")
	}

	var userInfo oauthUserInfoResponse
	if err := m.sendWithBearerRaw(ctx, cfg.Server, http.MethodGet, "/api/v4/session/oauth/userinfo", nil, tokenResp.AccessToken, &userInfo); err != nil {
		return OAuthSession{}, err
	}

	now := m.now()
	session := OAuthSession{
		AccessToken:    tokenResp.AccessToken,
		RefreshToken:   tokenResp.RefreshToken,
		AccessExpires:  now.Add(time.Duration(tokenResp.ExpiresIn) * time.Second),
		RefreshExpires: now.Add(time.Duration(tokenResp.RefreshTokenExpiresIn) * time.Second),
		Scope:          tokenResp.Scope,
		UserSub:        userInfo.Sub,
		UserName:       firstNonEmpty(userInfo.PreferredUsername, userInfo.Name, userInfo.Email, userInfo.Sub),
		UserEmail:      userInfo.Email,
	}
	if err := m.SaveSession(session); err != nil {
		return OAuthSession{}, err
	}
	return session, nil
}

func (m *OAuthManager) AccessToken(ctx context.Context) (string, error) {
	cfg := m.config()
	staticToken := strings.TrimSpace(cfg.Token)
	if !m.isConfigured(cfg) {
		if staticToken != "" {
			return staticToken, nil
		}
		return "", ErrOAuthNotConfigured
	}

	m.mut.Lock()
	defer m.mut.Unlock()

	session, ok, err := m.loadSessionLocked()
	if err != nil {
		return "", err
	}
	if !ok {
		if staticToken != "" {
			return staticToken, nil
		}
		return "", ErrOAuthNotAuthorized
	}

	if session.AccessToken != "" && session.AccessExpires.After(m.now().Add(oauthRefreshLeeway)) {
		return session.AccessToken, nil
	}
	if session.RefreshToken == "" || !session.RefreshExpires.After(m.now()) {
		if staticToken != "" {
			return staticToken, nil
		}
		return "", ErrOAuthNotAuthorized
	}
	if err := m.refreshLocked(ctx, cfg, &session); err != nil {
		if staticToken != "" {
			return staticToken, nil
		}
		return "", err
	}
	return session.AccessToken, nil
}

func (m *OAuthManager) LoadSession() (OAuthSession, bool, error) {
	m.mut.Lock()
	defer m.mut.Unlock()
	return m.loadSessionLocked()
}

func (m *OAuthManager) SaveSession(session OAuthSession) error {
	m.mut.Lock()
	defer m.mut.Unlock()
	return m.saveSessionLocked(session)
}

func (m *OAuthManager) ClearSession() error {
	m.mut.Lock()
	defer m.mut.Unlock()
	return m.store.Delete(oauthSessionKey)
}

func (m *OAuthManager) loadSessionLocked() (OAuthSession, bool, error) {
	var session OAuthSession
	bs, ok, err := m.store.Bytes(oauthSessionKey)
	if err != nil || !ok || len(bs) == 0 {
		return session, ok, err
	}
	if err := json.Unmarshal(bs, &session); err != nil {
		return OAuthSession{}, false, err
	}
	return session, true, nil
}

func (m *OAuthManager) saveSessionLocked(session OAuthSession) error {
	bs, err := json.Marshal(session)
	if err != nil {
		return err
	}
	return m.store.PutBytes(oauthSessionKey, bs)
}

func (m *OAuthManager) refreshLocked(ctx context.Context, cfg config.CloudreveConfiguration, session *OAuthSession) error {
	var resp oauthRefreshResponse
	if err := m.sendJSON(ctx, cfg.Server, http.MethodPost, "/api/v4/session/token/refresh", oauthRefreshRequest{
		RefreshToken: session.RefreshToken,
	}, &resp); err != nil {
		return err
	}
	if resp.AccessToken == "" {
		return errors.New("cloudreve token refresh did not return an access token")
	}
	session.AccessToken = resp.AccessToken
	if resp.RefreshToken != "" {
		session.RefreshToken = resp.RefreshToken
	}
	if !resp.AccessExpires.IsZero() {
		session.AccessExpires = resp.AccessExpires
	}
	if !resp.RefreshExpires.IsZero() {
		session.RefreshExpires = resp.RefreshExpires
	}
	return m.saveSessionLocked(*session)
}

func (m *OAuthManager) sendForm(ctx context.Context, server, endpoint string, form url.Values, respBody any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(server, "/")+endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return m.do(req, respBody)
}

func (m *OAuthManager) sendFormRaw(ctx context.Context, server, endpoint string, form url.Values, respBody any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(server, "/")+endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return m.doRaw(req, respBody)
}

func (m *OAuthManager) sendJSON(ctx context.Context, server, method, endpoint string, reqBody, respBody any) error {
	var body io.Reader
	if reqBody != nil {
		payload, err := json.Marshal(reqBody)
		if err != nil {
			return err
		}
		body = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(server, "/")+endpoint, body)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return m.do(req, respBody)
}

func (m *OAuthManager) sendWithBearer(ctx context.Context, server, method, endpoint string, reqBody io.Reader, token string, respBody any) error {
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(server, "/")+endpoint, reqBody)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	return m.do(req, respBody)
}

func (m *OAuthManager) sendWithBearerRaw(ctx context.Context, server, method, endpoint string, reqBody io.Reader, token string, respBody any) error {
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(server, "/")+endpoint, reqBody)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	return m.doRaw(req, respBody)
}

func (m *OAuthManager) do(req *http.Request, respBody any) error {
	resp, err := m.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("cloudreve oauth request failed: %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}

	var envelope apiResponse[json.RawMessage]
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		return err
	}
	if envelope.Code != 0 {
		return fmt.Errorf("cloudreve api error %d: %s", envelope.Code, envelope.Msg)
	}
	if respBody == nil || len(envelope.Data) == 0 || string(envelope.Data) == "null" {
		return nil
	}
	return json.Unmarshal(envelope.Data, respBody)
}

func (m *OAuthManager) doRaw(req *http.Request, respBody any) error {
	resp, err := m.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode/100 != 2 {
		var oauthErr struct {
			Error            string `json:"error"`
			ErrorDescription string `json:"error_description"`
		}
		if json.Unmarshal(body, &oauthErr) == nil {
			msg := firstNonEmpty(oauthErr.ErrorDescription, oauthErr.Error)
			if msg != "" {
				return fmt.Errorf("cloudreve oauth request failed: %s: %s", resp.Status, msg)
			}
		}
		return fmt.Errorf("cloudreve oauth request failed: %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	if respBody == nil || len(body) == 0 {
		return nil
	}
	return json.Unmarshal(body, respBody)
}

func (m *OAuthManager) config() config.CloudreveConfiguration {
	return m.cfg.Options().Cloudreve.Normalized()
}

func (m *OAuthManager) isConfigured(cfg config.CloudreveConfiguration) bool {
	return strings.TrimSpace(cfg.Server) != "" && strings.TrimSpace(cfg.OAuthClientID) != "" && strings.TrimSpace(cfg.OAuthClientSecret) != ""
}

func oauthScopes(cfg config.CloudreveConfiguration) string {
	fields := strings.Fields(cfg.OAuthScopes)
	if len(fields) == 0 {
		fields = strings.Fields(DefaultOAuthScopes)
	}

	seen := make(map[string]struct{}, len(fields)+3)
	result := make([]string, 0, len(fields)+3)
	for _, scope := range fields {
		scope = strings.TrimSpace(scope)
		if scope == "" {
			continue
		}
		if _, ok := seen[scope]; ok {
			continue
		}
		seen[scope] = struct{}{}
		result = append(result, scope)
	}

	for _, required := range []string{"openid", "offline_access", "Files.Write"} {
		if _, ok := seen[required]; ok {
			continue
		}
		seen[required] = struct{}{}
		result = append(result, required)
	}

	return strings.Join(result, " ")
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func PKCECodeChallenge(codeVerifier string) string {
	sum := sha256.Sum256([]byte(codeVerifier))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

func isInvalidScopeError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, `"error":"Invalid scope"`) ||
		strings.Contains(msg, "Invalid scope") ||
		strings.Contains(msg, "error_codes\":[40001]")
}
