// Copyright (C) 2014 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/syncthing/syncthing/internal/cloudreve"
	"github.com/syncthing/syncthing/internal/db"
	"github.com/syncthing/syncthing/internal/db/sqlite"
	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/events"
	modelmocks "github.com/syncthing/syncthing/lib/model/mocks"
	"github.com/syncthing/syncthing/lib/protocol"
)

var guiCfg config.GUIConfiguration

func init() {
	guiCfg.User = "user"
	guiCfg.SetPassword("pass")
}

func TestStaticAuthOK(t *testing.T) {
	t.Parallel()

	ok := authStatic("user", "pass", guiCfg)
	if !ok {
		t.Fatalf("should pass auth")
	}
}

func TestSimpleAuthUsernameFail(t *testing.T) {
	t.Parallel()

	ok := authStatic("userWRONG", "pass", guiCfg)
	if ok {
		t.Fatalf("should fail auth")
	}
}

func TestStaticAuthPasswordFail(t *testing.T) {
	t.Parallel()

	ok := authStatic("user", "passWRONG", guiCfg)
	if ok {
		t.Fatalf("should fail auth")
	}
}

func TestFormatOptionalPercentS(t *testing.T) {
	t.Parallel()

	cases := []struct {
		template string
		username string
		expected string
	}{
		{"cn=%s,dc=some,dc=example,dc=com", "username", "cn=username,dc=some,dc=example,dc=com"},
		{"cn=fixedusername,dc=some,dc=example,dc=com", "username", "cn=fixedusername,dc=some,dc=example,dc=com"},
		{"cn=%%s,dc=%s,dc=example,dc=com", "username", "cn=%s,dc=username,dc=example,dc=com"},
		{"cn=%%s,dc=%%s,dc=example,dc=com", "username", "cn=%s,dc=%s,dc=example,dc=com"},
		{"cn=%s,dc=%s,dc=example,dc=com", "username", "cn=username,dc=username,dc=example,dc=com"},
	}

	for _, c := range cases {
		templatedDn := formatOptionalPercentS(c.template, c.username)
		if c.expected != templatedDn {
			t.Fatalf("result should be %s != %s", c.expected, templatedDn)
		}
	}
}

func TestEscapeForLDAPFilter(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in  string
		out string
	}{
		{"username", `username`},
		{"user(name", `user\28name`},
		{"user)name", `user\29name`},
		{"user\\name", `user\5Cname`},
		{"user*name", `user\2Aname`},
		{"*,CN=asdf", `\2A,CN=asdf`},
	}

	for _, c := range cases {
		res := escapeForLDAPFilter(c.in)
		if c.out != res {
			t.Fatalf("result should be %s != %s", c.out, res)
		}
	}
}

func TestEscapeForLDAPDN(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in  string
		out string
	}{
		{"username", `username`},
		{"* ,CN=asdf", `*\20\2CCN\3Dasdf`},
	}

	for _, c := range cases {
		res := escapeForLDAPDN(c.in)
		if c.out != res {
			t.Fatalf("result should be %s != %s", c.out, res)
		}
	}
}

type mockClock struct {
	now time.Time
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func (c *mockClock) Now() time.Time {
	c.now = c.now.Add(1) // time always ticks by at least 1 ns
	return c.now
}

func (c *mockClock) wind(t time.Duration) {
	c.now = c.now.Add(t)
}

func TestTokenManager(t *testing.T) {
	t.Parallel()

	mdb, err := sqlite.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		mdb.Close()
	})
	kdb := db.NewMiscDB(mdb)
	clock := &mockClock{now: time.Now()}

	// Token manager keeps up to three tokens with a validity time of 24 hours.
	tm := newTokenManager("testTokens", kdb, 24*time.Hour, 3)
	tm.timeNow = clock.Now

	// Create three tokens
	t0 := tm.New()
	t1 := tm.New()
	t2 := tm.New()

	// Check that the tokens are valid
	if !tm.Check(t0) {
		t.Errorf("token %q should be valid", t0)
	}
	if !tm.Check(t1) {
		t.Errorf("token %q should be valid", t1)
	}
	if !tm.Check(t2) {
		t.Errorf("token %q should be valid", t2)
	}

	// Create a fourth token
	t3 := tm.New()
	// It should be valid
	if !tm.Check(t3) {
		t.Errorf("token %q should be valid", t3)
	}
	// But the first token should have been removed
	if tm.Check(t0) {
		t.Errorf("token %q should be invalid", t0)
	}

	// Wind the clock by 12 hours
	clock.wind(12 * time.Hour)
	// The second token should still be valid (and checking it will give it more life)
	if !tm.Check(t1) {
		t.Errorf("token %q should be valid", t1)
	}

	// Wind the clock by 12 hours
	clock.wind(12 * time.Hour)
	// The second token should still be valid
	if !tm.Check(t1) {
		t.Errorf("token %q should be valid", t1)
	}
	// But the third and fourth tokens should have expired
	if tm.Check(t2) {
		t.Errorf("token %q should be invalid", t2)
	}
	if tm.Check(t3) {
		t.Errorf("token %q should be invalid", t3)
	}
}

func TestCloudreveAuthLoginHandler(t *testing.T) {
	t.Parallel()

	_, _, authMW := newTestCloudreveAuthMiddleware(t)

	req := httptest.NewRequest(http.MethodGet, "https://syncthing.example.com/rest/noauth/auth/cloudreve/login?stayLoggedIn=true", nil)
	rec := httptest.NewRecorder()

	authMW.cloudreveAuthLoginHandler(rec, req)

	resp := rec.Result()
	if resp.StatusCode != http.StatusFound {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	}

	location := resp.Header.Get("Location")
	if !strings.HasPrefix(location, "https://cloudreve.example.com/session/authorize?") {
		t.Fatalf("unexpected authorize redirect: %s", location)
	}
	if !strings.Contains(location, "client_id=syncthing-client") {
		t.Fatalf("missing client_id in redirect: %s", location)
	}
	if !strings.Contains(location, "redirect_uri=https%3A%2F%2Fsyncthing.example.com%2Frest%2Fnoauth%2Fauth%2Fcloudreve%2Fcallback") {
		t.Fatalf("missing callback in redirect: %s", location)
	}
	if !strings.Contains(location, "code_challenge=") || !strings.Contains(location, "code_challenge_method=S256") {
		t.Fatalf("missing pkce parameters in redirect: %s", location)
	}

	var hasStateCookie bool
	var hasStayCookie bool
	var hasPKCECookie bool
	for _, cookie := range resp.Cookies() {
		switch cookie.Name {
		case authMW.cloudreveStateName:
			hasStateCookie = cookie.Value != ""
		case authMW.cloudreveStayName:
			hasStayCookie = cookie.Value == "1"
		case authMW.cloudrevePKCEName:
			hasPKCECookie = cookie.Value != ""
		}
	}
	if !hasStateCookie {
		t.Fatal("expected oauth state cookie to be set")
	}
	if !hasStayCookie {
		t.Fatal("expected stay-logged-in cookie to be set")
	}
	if !hasPKCECookie {
		t.Fatal("expected pkce verifier cookie to be set")
	}
}

func TestCloudreveAuthLoginHandlerUpdatesServerFromQuery(t *testing.T) {
	t.Parallel()

	_, wrapped, authMW := newTestCloudreveAuthMiddleware(t)
	if err := authMW.cloudreveOAuth.SaveSession(cloudreve.OAuthSession{
		RefreshToken:   "refresh-token",
		RefreshExpires: time.Now().Add(time.Hour),
		UserName:       "old-user",
	}); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "https://syncthing.example.com/rest/noauth/auth/cloudreve/login?server=cloudreve.local%3A5212%2Fdrive%2F", nil)
	rec := httptest.NewRecorder()

	authMW.cloudreveAuthLoginHandler(rec, req)

	resp := rec.Result()
	if resp.StatusCode != http.StatusFound {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	}

	location := resp.Header.Get("Location")
	if !strings.HasPrefix(location, "https://cloudreve.local:5212/drive/session/authorize?") {
		t.Fatalf("unexpected authorize redirect: %s", location)
	}

	if got := wrapped.Options().Cloudreve.Server; got != "https://cloudreve.local:5212/drive" {
		t.Fatalf("unexpected configured server: %q", got)
	}

	status := authMW.cloudreveOAuth.Status()
	if status.Authorized {
		t.Fatalf("expected oauth session to be cleared after server change: %#v", status)
	}
	if status.Server != "https://cloudreve.local:5212/drive" {
		t.Fatalf("unexpected oauth status server: %q", status.Server)
	}
}

func TestCloudreveAuthCallbackHandler(t *testing.T) {
	t.Parallel()

	store, wrapped, authMW := newTestCloudreveAuthMiddleware(t)
	model := authMW.model.(*modelmocks.Model)
	model.ScanFoldersReturns(map[string]error{})

	authMW.cloudreveOAuth = cloudreve.NewOAuthManager(wrapped, store, &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			var body string
			switch req.URL.Path {
			case "/api/v4/session/oauth/token":
				if err := req.ParseForm(); err != nil {
					t.Fatalf("parse form: %v", err)
				}
				if req.Form.Get("code_verifier") != "pkce-verifier" {
					t.Fatalf("unexpected code_verifier: %q", req.Form.Get("code_verifier"))
				}
				if req.Form.Get("redirect_uri") != "https://syncthing.example.com/rest/noauth/auth/cloudreve/callback" {
					t.Fatalf("unexpected redirect_uri: %q", req.Form.Get("redirect_uri"))
				}
				body = `{"access_token":"access-token","token_type":"Bearer","expires_in":3600,"refresh_token_expires_in":7200,"refresh_token":"refresh-token","scope":"openid profile offline_access Files.Read Files.Write"}`
			case "/api/v4/session/oauth/userinfo":
				body = `{"sub":"user-1","preferred_username":"cloudreve-user","email":"user@example.com"}`
			default:
				t.Fatalf("unexpected oauth request path: %s", req.URL.Path)
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewBufferString(body)),
				Header:     make(http.Header),
			}, nil
		}),
	})

	req := httptest.NewRequest(http.MethodGet, "https://syncthing.example.com/rest/noauth/auth/cloudreve/callback?state=test-state&code=test-code", nil)
	req.AddCookie(&http.Cookie{Name: authMW.cloudreveStateName, Value: "test-state"})
	req.AddCookie(&http.Cookie{Name: authMW.cloudreveStayName, Value: "1"})
	req.AddCookie(&http.Cookie{Name: authMW.cloudrevePKCEName, Value: "pkce-verifier"})
	rec := httptest.NewRecorder()

	authMW.cloudreveAuthCallbackHandler(rec, req)

	resp := rec.Result()
	if resp.StatusCode != http.StatusFound {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	}
	if location := resp.Header.Get("Location"); location != "/" {
		t.Fatalf("unexpected redirect target: %s", location)
	}

	var hasSessionCookie bool
	for _, cookie := range resp.Cookies() {
		if cookie.Name == authMW.tokenCookieManager.cookieName && cookie.Value != "" {
			hasSessionCookie = true
		}
	}
	if !hasSessionCookie {
		t.Fatal("expected GUI session cookie to be created")
	}

	deadline := time.Now().Add(time.Second)
	for model.ScanFoldersCallCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if model.ScanFoldersCallCount() != 1 {
		t.Fatalf("expected rescan to be triggered once, got %d", model.ScanFoldersCallCount())
	}

	session, ok, err := authMW.cloudreveOAuth.LoadSession()
	if err != nil {
		t.Fatal(err)
	}
	if !ok || session.AccessToken != "access-token" || session.RefreshToken != "refresh-token" {
		t.Fatalf("unexpected stored oauth session: %#v", session)
	}
}

func TestCloudreveAuthCallbackHandlerCreatesSessionWithoutGUIAuth(t *testing.T) {
	t.Parallel()

	store, wrapped, authMW := newTestCloudreveAuthMiddleware(t)
	authMW.guiCfg = config.GUIConfiguration{}

	authMW.cloudreveOAuth = cloudreve.NewOAuthManager(wrapped, store, &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			var body string
			switch req.URL.Path {
			case "/api/v4/session/oauth/token":
				body = `{"access_token":"access-token","token_type":"Bearer","expires_in":3600,"refresh_token_expires_in":7200,"refresh_token":"refresh-token","scope":"openid profile offline_access Files.Write"}`
			case "/api/v4/session/oauth/userinfo":
				body = `{"sub":"user-1","preferred_username":"cloudreve-user","email":"user@example.com"}`
			default:
				t.Fatalf("unexpected oauth request path: %s", req.URL.Path)
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewBufferString(body)),
				Header:     make(http.Header),
			}, nil
		}),
	})

	req := httptest.NewRequest(http.MethodGet, "https://syncthing.example.com/rest/noauth/auth/cloudreve/callback?state=test-state&code=test-code", nil)
	req.AddCookie(&http.Cookie{Name: authMW.cloudreveStateName, Value: "test-state"})
	req.AddCookie(&http.Cookie{Name: authMW.cloudreveStayName, Value: "1"})
	req.AddCookie(&http.Cookie{Name: authMW.cloudrevePKCEName, Value: "pkce-verifier"})
	rec := httptest.NewRecorder()

	authMW.cloudreveAuthCallbackHandler(rec, req)

	resp := rec.Result()
	if resp.StatusCode != http.StatusFound {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	}

	var hasSessionCookie bool
	for _, cookie := range resp.Cookies() {
		if cookie.Name == authMW.tokenCookieManager.cookieName && cookie.Value != "" {
			hasSessionCookie = true
		}
	}
	if !hasSessionCookie {
		t.Fatal("expected GUI session cookie to be created for cloudreve-only auth")
	}
}

func TestCloudreveAuthCallbackHandlerIncludesFailureDetail(t *testing.T) {
	t.Parallel()

	_, wrapped, authMW := newTestCloudreveAuthMiddleware(t)

	authMW.cloudreveOAuth = cloudreve.NewOAuthManager(wrapped, nil, &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.URL.Path != "/api/v4/session/oauth/token" {
				t.Fatalf("unexpected oauth request path: %s", req.URL.Path)
			}
			return &http.Response{
				StatusCode: http.StatusBadRequest,
				Status:     "400 Bad Request",
				Body:       io.NopCloser(bytes.NewBufferString(`{"error":"Invalid client secret","error_description":"Invalid client secret"}`)),
				Header:     make(http.Header),
			}, nil
		}),
	})

	req := httptest.NewRequest(http.MethodGet, "https://syncthing.example.com/rest/noauth/auth/cloudreve/callback?state=test-state&code=test-code", nil)
	req.AddCookie(&http.Cookie{Name: authMW.cloudreveStateName, Value: "test-state"})
	req.AddCookie(&http.Cookie{Name: authMW.cloudrevePKCEName, Value: "pkce-verifier"})
	rec := httptest.NewRecorder()

	authMW.cloudreveAuthCallbackHandler(rec, req)

	resp := rec.Result()
	if resp.StatusCode != http.StatusFound {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	}

	location := resp.Header.Get("Location")
	u, err := url.Parse(location)
	if err != nil {
		t.Fatalf("parse redirect location: %v", err)
	}
	if reason := u.Query().Get("cloudreveAuthError"); reason != cloudreveAuthErrorFailed {
		t.Fatalf("unexpected cloudreveAuthError: %q", reason)
	}
	if detail := u.Query().Get("cloudreveAuthErrorDetail"); detail != "400 Bad Request: Invalid client secret" {
		t.Fatalf("unexpected cloudreveAuthErrorDetail: %q", detail)
	}
}

func newTestCloudreveAuthMiddleware(t *testing.T) (*db.Typed, config.Wrapper, *basicAuthAndSessionMiddleware) {
	t.Helper()

	mdb, err := sqlite.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		mdb.Close()
	})

	cfg := config.New(protocol.LocalDeviceID)
	cfg.GUI.User = "user"
	cfg.GUI.SetPassword("pass")
	cfg.Options.Cloudreve = config.CloudreveConfiguration{
		Enabled:           true,
		Server:            "https://cloudreve.example.com",
		OAuthClientID:     "syncthing-client",
		OAuthClientSecret: "syncthing-secret",
	}
	wrapped := config.Wrap("/dev/null", cfg, protocol.LocalDeviceID, events.NoopLogger)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go wrapped.Serve(ctx)
	store := db.NewMiscDB(mdb)
	model := &modelmocks.Model{}
	authMW := newBasicAuthAndSessionMiddleware(
		newTokenCookieManager("TEST123", cfg.GUI, events.NoopLogger, store),
		cfg.GUI,
		cfg.LDAP,
		wrapped,
		store,
		model,
		http.NotFoundHandler(),
		events.NoopLogger,
	)
	return store, wrapped, authMW
}
