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
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
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
