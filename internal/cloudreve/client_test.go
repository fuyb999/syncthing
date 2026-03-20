// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package cloudreve

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

type gatedReader struct {
	total   int64
	gate    int64
	sent    int64
	release <-chan struct{}
}

type zeroReader struct{}

func (fn roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

func (r *gatedReader) Read(p []byte) (int, error) {
	if r.sent >= r.total {
		return 0, io.EOF
	}

	if r.sent >= r.gate {
		<-r.release
	}

	allowed := r.total - r.sent
	if r.sent < r.gate && allowed > r.gate-r.sent {
		allowed = r.gate - r.sent
	}
	if allowed <= 0 {
		<-r.release
		allowed = r.total - r.sent
	}
	if int64(len(p)) > allowed {
		p = p[:allowed]
	}
	for i := range p {
		p[i] = byte('a' + (r.sent+int64(i))%26)
	}
	r.sent += int64(len(p))
	if r.sent >= r.total {
		return len(p), io.EOF
	}
	return len(p), nil
}

func (zeroReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}

func newJSONResponse(status int, payload string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(payload)),
	}
}

func TestJoinURI(t *testing.T) {
	t.Parallel()

	if got := JoinURI("cloudreve://syncthing", "foo/bar"); got != "cloudreve://syncthing/foo/bar" {
		t.Fatalf("unexpected uri: %q", got)
	}
}

func TestListDirectories(t *testing.T) {
	t.Parallel()

	var requests []string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/v4/file" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer token" {
			t.Fatalf("unexpected auth header: %q", got)
		}
		if got := r.URL.Query().Get("uri"); got != "cloudreve://my/test" {
			t.Fatalf("unexpected browse uri: %q", got)
		}
		if got := r.URL.Query().Get("page_size"); got != "1000" {
			t.Fatalf("unexpected page size: %q", got)
		}

		requests = append(requests, r.URL.RawQuery)
		switch r.URL.Query().Get("next_page_token") {
		case "":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": 0,
				"data": map[string]any{
					"files": []map[string]any{
						{"type": 1, "name": "beta", "path": "cloudreve://my/test/beta"},
						{"type": 0, "name": "note.txt", "path": "cloudreve://my/test/note.txt"},
						{"type": 1, "name": "alpha", "path": "cloudreve://my/test/alpha"},
					},
					"parent": map[string]any{
						"type": 1,
						"name": "my",
						"path": "cloudreve://my",
					},
					"pagination": map[string]any{
						"next_token": "page-2",
					},
				},
			})
		case "page-2":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": 0,
				"data": map[string]any{
					"files": []map[string]any{
						{"type": 1, "name": "gamma", "path": "cloudreve://my/test/gamma"},
						{"type": 1, "name": "beta", "path": "cloudreve://my/test/beta"},
					},
					"pagination": map[string]any{},
				},
			})
		default:
			t.Fatalf("unexpected next page token: %q", r.URL.Query().Get("next_page_token"))
		}
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "token", srv.Client())
	listing, err := client.ListDirectories(context.Background(), "cloudreve://my/test")
	if err != nil {
		t.Fatal(err)
	}

	if len(requests) != 2 {
		t.Fatalf("expected 2 requests, got %d", len(requests))
	}
	if listing.Parent == nil || listing.Parent.Path != "cloudreve://my" {
		t.Fatalf("unexpected parent: %#v", listing.Parent)
	}
	if len(listing.Directories) != 3 {
		t.Fatalf("unexpected directories: %#v", listing.Directories)
	}
	gotNames := []string{
		listing.Directories[0].Name,
		listing.Directories[1].Name,
		listing.Directories[2].Name,
	}
	expectedNames := []string{"alpha", "beta", "gamma"}
	for i := range expectedNames {
		if gotNames[i] != expectedNames[i] {
			t.Fatalf("unexpected sorted directories: %#v", gotNames)
		}
	}
}

func TestCloudreveParentURI(t *testing.T) {
	t.Parallel()

	cases := []struct {
		uri      string
		expected string
	}{
		{uri: "cloudreve://my", expected: ""},
		{uri: "cloudreve://public", expected: ""},
		{uri: "cloudreve://my/demo", expected: "cloudreve://my"},
		{uri: "cloudreve://public/foo/bar", expected: "cloudreve://public/foo"},
		{uri: "cloudreve://public/%E6%B5%8B%E8%AF%95/bar", expected: "cloudreve://public/%E6%B5%8B%E8%AF%95"},
	}

	for _, tc := range cases {
		if got := cloudreveParentURI(tc.uri); got != tc.expected {
			t.Fatalf("unexpected parent uri for %q: %q != %q", tc.uri, got, tc.expected)
		}
	}
}

func TestCurrentUser(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/v4/user/me" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer token" {
			t.Fatalf("unexpected auth header: %q", got)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code": 0,
			"data": map[string]any{
				"email":    "admin@example.com",
				"nickname": "Admin",
				"group": map[string]any{
					"name": "admin",
				},
			},
		})
	}))
	defer srv.Close()

	user, err := NewClient(srv.URL, "token", srv.Client()).CurrentUser(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if user.GroupName() != "admin" {
		t.Fatalf("unexpected group: %q", user.GroupName())
	}
	if !user.CanAccessPublic() {
		t.Fatal("expected admin to access public files")
	}
}

func TestReportDeviceReturnsRestorePayload(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || r.URL.Path != "/api/v4/devices/syncthing/report" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer token" {
			t.Fatalf("unexpected auth header: %q", got)
		}
		if got := r.Header.Get(cloudreveClientIDHeader); got != "NEW-DEVICE" {
			t.Fatalf("unexpected client id header: %q", got)
		}

		var req DeviceReportRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		if req.DeviceID != "NEW-DEVICE" {
			t.Fatalf("unexpected device id: %#v", req)
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"code": 0,
			"data": map[string]any{
				"restore_config": map[string]any{
					"version": 51,
					"profile": "restored",
				},
				"restore_from_device_id": "OLD-DEVICE",
			},
		})
	}))
	defer srv.Close()

	resp, err := NewClient(srv.URL, "token", srv.Client()).ReportDevice(context.Background(), DeviceReportRequest{
		DeviceID: "NEW-DEVICE",
		ShortID:  "NEW123",
		APIKey:   "api-key",
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.RestoreFromDeviceID != "OLD-DEVICE" {
		t.Fatalf("unexpected restore source: %#v", resp)
	}
	if got := resp.RestoreConfig["profile"]; got != "restored" {
		t.Fatalf("unexpected restore payload: %#v", resp.RestoreConfig)
	}
}

func TestDeviceRegistrationErrorHelpers(t *testing.T) {
	t.Parallel()

	if !IsDeviceRegistrationConflict(&apiError{code: cloudreveErrorSyncthingIPConflict, msg: "conflict"}) {
		t.Fatal("expected ip conflict error to be recognized")
	}
	if IsDeviceRegistrationConflict(&apiError{code: cloudreveErrorObjectExisted, msg: "other"}) {
		t.Fatal("did not expect unrelated error code to be treated as ip conflict")
	}
	if !IsDeviceNotRegistered(&apiError{code: cloudreveErrorSyncthingDeviceNotRegistered, msg: "missing"}) {
		t.Fatal("expected not registered error to be recognized")
	}
	if IsDeviceNotRegistered(errors.New("plain error")) {
		t.Fatal("did not expect generic errors to be treated as not registered")
	}
}

func TestIsRetryableError(t *testing.T) {
	t.Parallel()

	if !IsRetryableError(&apiError{code: cloudreveErrorDBOperationFailed, msg: "Failed to start transaction"}) {
		t.Fatal("expected database operation errors to be retryable")
	}
	if IsRetryableError(&apiError{code: cloudreveErrorObjectExisted, msg: "Object already exists"}) {
		t.Fatal("expected conflict errors to remain non-retryable")
	}
}

func TestUploadFileLocalFlow(t *testing.T) {
	t.Parallel()

	var (
		createMethod string
		createURI    string
		createMime   string
		chunks       []string
		lengths      []int64
		progresses   []int64
	)

	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && r.URL.Path == "/api/v4/file/upload":
			createMethod = r.Method
			var req map[string]any
			_ = json.NewDecoder(r.Body).Decode(&req)
			createURI, _ = req["uri"].(string)
			createMime, _ = req["mime_type"].(string)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": 0,
				"data": map[string]any{
					"session_id": "session-local",
					"chunk_size": 4,
					"storage_policy": map[string]any{
						"type": "local",
					},
				},
			})
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/api/v4/file/upload/session-local/"):
			if got := r.Header.Get("Authorization"); got != "Bearer token" {
				t.Fatalf("unexpected auth header: %q", got)
			}
			lengths = append(lengths, r.ContentLength)
			body, _ := io.ReadAll(r.Body)
			chunks = append(chunks, string(body))
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": nil})
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "token", srv.Client())
	err := client.UploadFile(context.Background(), "cloudreve://root/file.txt", int64(len("abcdefgh")), 123, "text/plain", strings.NewReader("abcdefgh"), func(done, _ int64) {
		progresses = append(progresses, done)
	})
	if err != nil {
		t.Fatal(err)
	}

	if createMethod != http.MethodPut {
		t.Fatalf("expected PUT create session, got %q", createMethod)
	}
	if createURI != "cloudreve://root/file.txt" {
		t.Fatalf("unexpected uri: %q", createURI)
	}
	if createMime != "text/plain" {
		t.Fatalf("unexpected mime type: %q", createMime)
	}
	if len(chunks) != 2 || chunks[0] != "abcd" || chunks[1] != "efgh" {
		t.Fatalf("unexpected chunks: %#v", chunks)
	}
	if len(lengths) != 2 || lengths[0] != 4 || lengths[1] != 4 {
		t.Fatalf("unexpected content lengths: %#v", lengths)
	}
	if len(progresses) < 2 || progresses[0] <= 0 || progresses[len(progresses)-1] != 8 {
		t.Fatalf("unexpected progress updates: %#v", progresses)
	}
}

func TestUploadFileRemoteFlow(t *testing.T) {
	t.Parallel()

	var (
		uploaded []string
		lengths  []int64
	)

	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && r.URL.Path == "/api/v4/file/upload":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": 0,
				"data": map[string]any{
					"session_id":  "session-remote",
					"chunk_size":  3,
					"upload_urls": []string{srv.URL + "/slave/upload"},
					"credential":  "Bearer slave-token",
					"storage_policy": map[string]any{
						"type": "remote",
					},
				},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/slave/upload":
			if got := r.URL.Query().Get("chunk"); got == "" {
				t.Fatal("missing chunk query parameter")
			}
			if got := r.Header.Get("Authorization"); got != "Bearer slave-token" {
				t.Fatalf("unexpected slave auth: %q", got)
			}
			lengths = append(lengths, r.ContentLength)
			body, _ := io.ReadAll(r.Body)
			uploaded = append(uploaded, string(body))
			w.WriteHeader(http.StatusOK)
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "token", srv.Client())
	if err := client.UploadFile(context.Background(), "cloudreve://root/file.bin", 5, 123, "", strings.NewReader("abcde"), nil); err != nil {
		t.Fatal(err)
	}

	if len(uploaded) != 2 || uploaded[0] != "abc" || uploaded[1] != "de" {
		t.Fatalf("unexpected remote chunks: %#v", uploaded)
	}
	if len(lengths) != 2 || lengths[0] != 3 || lengths[1] != 2 {
		t.Fatalf("unexpected remote content lengths: %#v", lengths)
	}
}

func TestUploadFileStartsFirstChunkBeforeReadingWholeSource(t *testing.T) {
	t.Parallel()

	firstRequest := make(chan struct{}, 1)
	release := make(chan struct{})

	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && r.URL.Path == "/api/v4/file/upload":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": 0,
				"data": map[string]any{
					"session_id": "session-local",
					"chunk_size": 4,
					"storage_policy": map[string]any{
						"type": "local",
					},
				},
			})
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/api/v4/file/upload/session-local/"):
			select {
			case firstRequest <- struct{}{}:
			default:
			}
			if _, err := io.Copy(io.Discard, r.Body); err != nil {
				t.Fatal(err)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": nil})
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "token", srv.Client())
	errCh := make(chan error, 1)
	go func() {
		errCh <- client.UploadFile(context.Background(), "cloudreve://root/file.bin", 8, 123, "", &gatedReader{
			total:   8,
			gate:    4,
			release: release,
		}, nil)
	}()

	select {
	case <-firstRequest:
	case <-time.After(2 * time.Second):
		t.Fatal("first upload request did not start before the remaining source bytes were released")
	}

	close(release)

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for upload to complete")
	}
}

func TestUploadFileRetriesAsVersionWhenObjectExists(t *testing.T) {
	t.Parallel()

	var (
		createBodies []map[string]any
		uploaded     []string
	)

	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && r.URL.Path == "/api/v4/file/upload":
			var req map[string]any
			_ = json.NewDecoder(r.Body).Decode(&req)
			createBodies = append(createBodies, req)
			if len(createBodies) == 1 {
				_ = json.NewEncoder(w).Encode(map[string]any{
					"code": 40004,
					"msg":  "Object existed",
					"data": nil,
				})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": 0,
				"data": map[string]any{
					"session_id": "session-version",
					"chunk_size": 4,
					"storage_policy": map[string]any{
						"type": "local",
					},
				},
			})
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/api/v4/file/upload/session-version/"):
			body, _ := io.ReadAll(r.Body)
			uploaded = append(uploaded, string(body))
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": nil})
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "token", srv.Client())
	if err := client.UploadFile(context.Background(), "cloudreve://root/file.txt", int64(len("abcd")), 123, "text/plain", strings.NewReader("abcd"), nil); err != nil {
		t.Fatal(err)
	}

	if len(createBodies) != 2 {
		t.Fatalf("expected two create-session requests, got %#v", createBodies)
	}
	if _, ok := createBodies[0]["entity_type"]; ok {
		t.Fatalf("expected first create-session request to not specify versioning, got %#v", createBodies[0])
	}
	if got, _ := createBodies[1]["entity_type"].(string); got != "version" {
		t.Fatalf("expected second create-session request to set entity_type=version, got %#v", createBodies[1])
	}
	if len(uploaded) != 1 || uploaded[0] != "abcd" {
		t.Fatalf("unexpected uploaded chunks after version retry: %#v", uploaded)
	}
}

func TestDeleteFile(t *testing.T) {
	t.Parallel()

	var (
		method string
		body   map[string]any
	)

	client := NewClient("https://cloudreve.example", "token", &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			if r.URL.Path != "/api/v4/file" {
				t.Fatalf("unexpected path: %s", r.URL.Path)
			}
			method = r.Method
			if got := r.Header.Get("Authorization"); got != "Bearer token" {
				t.Fatalf("unexpected auth header: %q", got)
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			return newJSONResponse(http.StatusOK, `{"code":0,"data":null}`), nil
		}),
	})
	if err := client.Delete(context.Background(), "cloudreve://root/file.txt"); err != nil {
		t.Fatal(err)
	}

	if method != http.MethodDelete {
		t.Fatalf("expected DELETE request, got %q", method)
	}
	uris, ok := body["uris"].([]any)
	if !ok || len(uris) != 1 || uris[0] != "cloudreve://root/file.txt" {
		t.Fatalf("unexpected uris: %#v", body["uris"])
	}
	if body["skip_soft_delete"] != true {
		t.Fatal("expected skip_soft_delete to be true")
	}
	if body["unlink"] == true {
		t.Fatal("expected unlink to be false")
	}
}

func TestReportDevice(t *testing.T) {
	t.Parallel()

	var (
		method   string
		path     string
		clientID string
		body     map[string]any
	)

	client := NewClient("https://cloudreve.example", "token", &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			method = r.Method
			path = r.URL.Path
			clientID = r.Header.Get(cloudreveClientIDHeader)
			if got := r.Header.Get("Authorization"); got != "Bearer token" {
				t.Fatalf("unexpected auth header: %q", got)
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			return newJSONResponse(http.StatusOK, `{"code":0,"data":null}`), nil
		}),
	})

	resp, err := client.ReportDevice(context.Background(), DeviceReportRequest{
		DeviceID:      "DEVICE-ID",
		ShortID:       "SHORTID",
		APIKey:        "api-key",
		JSONRaw:       map[string]any{"foo": "bar"},
		BindURI:       "cloudreve://my",
		ClientVersion: "v1.2.3",
		Platform:      "linux/amd64",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.RestoreConfig) != 0 || resp.RestoreFromDeviceID != "" {
		t.Fatalf("expected empty restore payload, got %#v", resp)
	}

	if method != http.MethodPut {
		t.Fatalf("expected PUT request, got %q", method)
	}
	if path != "/api/v4/devices/syncthing/report" {
		t.Fatalf("unexpected path: %q", path)
	}
	if clientID != "DEVICE-ID" {
		t.Fatalf("unexpected client id header: %q", clientID)
	}
	if got, _ := body["device_id"].(string); got != "DEVICE-ID" {
		t.Fatalf("unexpected device_id in body: %#v", body)
	}
	if got, _ := body["api_key"].(string); got != "api-key" {
		t.Fatalf("unexpected api_key in body: %#v", body)
	}
}

func TestListSyncthingDevices(t *testing.T) {
	t.Parallel()

	var (
		method string
		path   string
		auth   string
	)

	now := time.Date(2026, time.March, 20, 12, 0, 0, 0, time.UTC)
	client := NewClient("https://cloudreve.example", "token", &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			method = r.Method
			path = r.URL.Path
			auth = r.Header.Get("Authorization")
			return newJSONResponse(http.StatusOK, `{"code":0,"data":{"devices":[{"device_id":"DEVICE-A","short_id":"AAAA","bind_uri":"cloudreve://my/docs","last_seen_at":"2026-03-20T12:00:00Z","online":true,"is_bound":true}]}}`), nil
		}),
	})

	devices, err := client.ListSyncthingDevices(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if method != http.MethodGet {
		t.Fatalf("expected GET request, got %q", method)
	}
	if path != "/api/v4/devices/syncthing" {
		t.Fatalf("unexpected path: %q", path)
	}
	if auth != "Bearer token" {
		t.Fatalf("unexpected auth header: %q", auth)
	}
	if len(devices) != 1 {
		t.Fatalf("expected one device, got %#v", devices)
	}
	if devices[0].DeviceID != "DEVICE-A" || !devices[0].IsBound || !devices[0].Online {
		t.Fatalf("unexpected device payload: %#v", devices[0])
	}
	if devices[0].LastSeenAt == nil || !devices[0].LastSeenAt.Equal(now) {
		t.Fatalf("unexpected last_seen_at: %#v", devices[0].LastSeenAt)
	}
}

func TestReportSyncActivity(t *testing.T) {
	t.Parallel()

	var (
		method   string
		path     string
		clientID string
		body     map[string]any
	)

	client := NewClient("https://cloudreve.example", "token", &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			method = r.Method
			path = r.URL.Path
			clientID = r.Header.Get(cloudreveClientIDHeader)
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			return newJSONResponse(http.StatusOK, `{"code":0,"data":null}`), nil
		}),
	})

	syncAt := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	err := client.ReportSyncActivity(context.Background(), DeviceActivityRequest{
		DeviceID: "DEVICE-ID",
		ShortID:  "SHORTID",
		BindURI:  "cloudreve://my",
		SyncedAt: syncAt,
	})
	if err != nil {
		t.Fatal(err)
	}

	if method != http.MethodPost {
		t.Fatalf("expected POST request, got %q", method)
	}
	if path != "/api/v4/devices/syncthing/activity" {
		t.Fatalf("unexpected path: %q", path)
	}
	if clientID != "DEVICE-ID" {
		t.Fatalf("unexpected client id header: %q", clientID)
	}
	if got, _ := body["device_id"].(string); got != "DEVICE-ID" {
		t.Fatalf("unexpected activity body: %#v", body)
	}
	if got, _ := body["bind_uri"].(string); got != "cloudreve://my" {
		t.Fatalf("unexpected bind_uri in activity body: %#v", body)
	}
}

func TestDeleteFileIgnoresNotFound(t *testing.T) {
	t.Parallel()

	client := NewClient("https://cloudreve.example", "token", &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			if r.URL.Path != "/api/v4/file" {
				t.Fatalf("unexpected path: %s", r.URL.Path)
			}
			return newJSONResponse(http.StatusOK, `{"code":404,"msg":"file not found","data":null}`), nil
		}),
	})
	if err := client.Delete(context.Background(), "cloudreve://root/missing.txt"); err != nil {
		t.Fatal(err)
	}
}

func TestUploadProgressTrackerReportsIncrementally(t *testing.T) {
	t.Parallel()

	var samples []int64
	tracker := newUploadProgressTracker(10, func(done, _ int64) {
		samples = append(samples, done)
	})
	reader := tracker.Wrap(3, strings.NewReader("abcdefg"))
	buf := make([]byte, 2)
	for {
		_, err := reader.Read(buf)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}

	if len(samples) == 0 {
		t.Fatal("expected progress samples")
	}
	if samples[0] <= 3 {
		t.Fatalf("expected incremental progress above base offset, got %#v", samples)
	}
	if samples[len(samples)-1] != 10 {
		t.Fatalf("expected final progress to reach total size, got %#v", samples)
	}
}

func TestUploadFileS3Flow(t *testing.T) {
	t.Parallel()

	var (
		putBodies      []string
		partLengths    []int64
		completeBody   string
		callbackMethod string
	)

	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && r.URL.Path == "/api/v4/file/upload":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": 0,
				"data": map[string]any{
					"session_id":      "session-s3",
					"chunk_size":      4,
					"upload_urls":     []string{srv.URL + "/s3/part/1", srv.URL + "/s3/part/2"},
					"completeURL":     srv.URL + "/s3/complete",
					"callback_secret": "secret",
					"storage_policy": map[string]any{
						"type": "s3",
					},
				},
			})
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/s3/part/"):
			partLengths = append(partLengths, r.ContentLength)
			body, _ := io.ReadAll(r.Body)
			putBodies = append(putBodies, string(body))
			w.Header().Set("ETag", `"`+r.URL.Path+`"`)
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/s3/complete":
			body, _ := io.ReadAll(r.Body)
			completeBody = string(body)
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet && r.URL.Path == "/api/v4/callback/s3/session-s3/secret":
			callbackMethod = r.Method
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": nil})
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "token", srv.Client())
	if err := client.UploadFile(context.Background(), "cloudreve://root/file.bin", 8, 123, "", strings.NewReader("abcdefgh"), nil); err != nil {
		t.Fatal(err)
	}

	if len(putBodies) != 2 || putBodies[0] != "abcd" || putBodies[1] != "efgh" {
		t.Fatalf("unexpected s3 parts: %#v", putBodies)
	}
	if len(partLengths) != 2 || partLengths[0] != 4 || partLengths[1] != 4 {
		t.Fatalf("unexpected s3 content lengths: %#v", partLengths)
	}
	if !strings.Contains(completeBody, "<PartNumber>1</PartNumber>") || !strings.Contains(completeBody, "<PartNumber>2</PartNumber>") {
		t.Fatalf("unexpected complete body: %s", completeBody)
	}
	if callbackMethod != http.MethodGet {
		t.Fatalf("expected callback request, got %q", callbackMethod)
	}
}

func TestUploadFileQiniuFlow(t *testing.T) {
	t.Parallel()

	var (
		partAuth   []string
		partBodies []string
		partLens   []int64
		finalBody  string
	)

	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && r.URL.Path == "/api/v4/file/upload":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": 0,
				"data": map[string]any{
					"session_id":  "session-qiniu",
					"chunk_size":  4,
					"upload_urls": []string{srv.URL + "/qiniu"},
					"credential":  "up-token",
					"mime_type":   "text/plain",
					"storage_policy": map[string]any{
						"type": "qiniu",
					},
				},
			})
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/qiniu/"):
			partAuth = append(partAuth, r.Header.Get("Authorization"))
			partLens = append(partLens, r.ContentLength)
			body, _ := io.ReadAll(r.Body)
			partBodies = append(partBodies, string(body))
			_ = json.NewEncoder(w).Encode(map[string]any{"etag": "etag-" + r.URL.Path})
		case r.Method == http.MethodPost && r.URL.Path == "/qiniu":
			body, _ := io.ReadAll(r.Body)
			finalBody = string(body)
			if got := r.Header.Get("Authorization"); got != "UpToken up-token" {
				t.Fatalf("unexpected qiniu finalize auth: %q", got)
			}
			w.WriteHeader(http.StatusOK)
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "token", srv.Client())
	if err := client.UploadFile(context.Background(), "cloudreve://root/file.txt", 8, 123, "text/plain", strings.NewReader("abcdefgh"), nil); err != nil {
		t.Fatal(err)
	}

	if len(partBodies) != 2 || partBodies[0] != "abcd" || partBodies[1] != "efgh" {
		t.Fatalf("unexpected qiniu parts: %#v", partBodies)
	}
	if len(partLens) != 2 || partLens[0] != 4 || partLens[1] != 4 {
		t.Fatalf("unexpected qiniu content lengths: %#v", partLens)
	}
	for _, auth := range partAuth {
		if auth != "UpToken up-token" {
			t.Fatalf("unexpected qiniu part auth: %q", auth)
		}
	}
	if !strings.Contains(finalBody, `"partNumber":1`) || !strings.Contains(finalBody, `"mimeType":"text/plain"`) {
		t.Fatalf("unexpected qiniu finish body: %s", finalBody)
	}
}

func TestUploadFileUpyunFlow(t *testing.T) {
	t.Parallel()

	var (
		contentType string
		body        string
	)

	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && r.URL.Path == "/api/v4/file/upload":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": 0,
				"data": map[string]any{
					"session_id":    "session-upyun",
					"chunk_size":    0,
					"upload_urls":   []string{srv.URL + "/upyun"},
					"credential":    "auth-value",
					"upload_policy": "policy-value",
					"mime_type":     "text/plain; charset=utf-8",
					"storage_policy": map[string]any{
						"type": "upyun",
					},
				},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/upyun":
			contentType = r.Header.Get("Content-Type")
			raw, _ := io.ReadAll(r.Body)
			body = string(raw)
			w.WriteHeader(http.StatusOK)
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "token", srv.Client())
	if err := client.UploadFile(context.Background(), "cloudreve://root/file.txt", 3, 123, mime.TypeByExtension(".txt"), strings.NewReader("abc"), nil); err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(contentType, "multipart/form-data") {
		t.Fatalf("unexpected content type: %q", contentType)
	}
	if !strings.Contains(body, `name="policy"`) || !strings.Contains(body, "policy-value") {
		t.Fatalf("missing policy in multipart body: %s", body)
	}
	if !strings.Contains(body, `name="authorization"`) || !strings.Contains(body, "auth-value") {
		t.Fatalf("missing authorization in multipart body: %s", body)
	}
	if !strings.Contains(body, `name="file"; filename="upload"`) || !strings.Contains(body, "abc") {
		t.Fatalf("missing file in multipart body: %s", body)
	}
}

func TestUploadFileOneDriveFlow(t *testing.T) {
	t.Parallel()

	var (
		ranges   []string
		uploaded []string
		lengths  []int64
	)

	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && r.URL.Path == "/api/v4/file/upload":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": 0,
				"data": map[string]any{
					"session_id":      "session-onedrive",
					"chunk_size":      4,
					"upload_urls":     []string{srv.URL + "/onedrive/session"},
					"callback_secret": "done-secret",
					"storage_policy": map[string]any{
						"type": "onedrive",
					},
				},
			})
		case r.Method == http.MethodPut && r.URL.Path == "/onedrive/session":
			ranges = append(ranges, r.Header.Get("Content-Range"))
			lengths = append(lengths, r.ContentLength)
			body, _ := io.ReadAll(r.Body)
			uploaded = append(uploaded, string(body))
			w.WriteHeader(http.StatusAccepted)
		case r.Method == http.MethodPost && r.URL.Path == "/api/v4/callback/onedrive/session-onedrive/done-secret":
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": nil})
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "token", srv.Client())
	if err := client.UploadFile(context.Background(), "cloudreve://root/file.txt", 8, 123, "text/plain", strings.NewReader("abcdefgh"), nil); err != nil {
		t.Fatal(err)
	}

	if len(ranges) != 2 || ranges[0] != "bytes 0-3/8" || ranges[1] != "bytes 4-7/8" {
		t.Fatalf("unexpected ranges: %#v", ranges)
	}
	if len(lengths) != 2 || lengths[0] != 4 || lengths[1] != 4 {
		t.Fatalf("unexpected onedrive content lengths: %#v", lengths)
	}
	if len(uploaded) != 2 || uploaded[0] != "abcd" || uploaded[1] != "efgh" {
		t.Fatalf("unexpected onedrive chunks: %#v", uploaded)
	}
}

func TestUploadFileLargeSourceStreamsInChunks(t *testing.T) {
	t.Parallel()

	const (
		totalSize = 32 << 20
		chunkSize = 4 << 20
	)

	var (
		chunks     int
		totalRead  int64
		progresses []int64
	)

	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && r.URL.Path == "/api/v4/file/upload":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": 0,
				"data": map[string]any{
					"session_id": "session-local",
					"chunk_size": chunkSize,
					"storage_policy": map[string]any{
						"type": "local",
					},
				},
			})
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/api/v4/file/upload/session-local/"):
			n, err := io.Copy(io.Discard, r.Body)
			if err != nil {
				t.Fatal(err)
			}
			chunks++
			totalRead += n
			if r.ContentLength != chunkSize && r.ContentLength != totalSize%chunkSize {
				t.Fatalf("unexpected content length: %d", r.ContentLength)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": nil})
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "token", srv.Client())
	if err := client.UploadFile(context.Background(), "cloudreve://root/large.bin", totalSize, 123, "application/octet-stream", io.LimitReader(zeroReader{}, totalSize), func(done, _ int64) {
		progresses = append(progresses, done)
	}); err != nil {
		t.Fatal(err)
	}

	if chunks != totalSize/chunkSize {
		t.Fatalf("unexpected chunk count: %d", chunks)
	}
	if totalRead != totalSize {
		t.Fatalf("unexpected streamed size: %d", totalRead)
	}
	if len(progresses) == 0 || progresses[len(progresses)-1] != totalSize {
		t.Fatalf("unexpected progress updates: %#v", progresses)
	}
}
