// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/syncthing/syncthing/internal/cloudreve"
	"github.com/syncthing/syncthing/internal/db"
	"github.com/syncthing/syncthing/internal/db/sqlite"
	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/events"
	"github.com/syncthing/syncthing/lib/protocol"
)

func TestGetSystemCloudreveBrowse(t *testing.T) {
	t.Parallel()

	var (
		gotAuth string
		gotURI  string
	)

	cloudreveSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/v4/file" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		gotAuth = r.Header.Get("Authorization")
		gotURI = r.URL.Query().Get("uri")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code": 0,
			"data": map[string]any{
				"files": []map[string]any{
					{"type": 1, "name": "docs", "path": "cloudreve://my/base/docs"},
					{"type": 0, "name": "readme.txt", "path": "cloudreve://my/base/readme.txt"},
				},
				"parent": map[string]any{
					"type": 1,
					"name": "my",
					"path": "cloudreve://my",
				},
			},
		})
	}))
	defer cloudreveSrv.Close()

	mdb, err := sqlite.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		mdb.Close()
	})

	wrapped := config.Wrap("/dev/null", config.Configuration{
		Options: config.OptionsConfiguration{
			Cloudreve: config.CloudreveConfiguration{
				Enabled: true,
				Server:  cloudreveSrv.URL,
				Token:   "static-token",
				BaseURI: "cloudreve://my",
			},
		},
	}, protocol.LocalDeviceID, events.NoopLogger)

	svc := &service{
		id:     protocol.LocalDeviceID,
		cfg:    wrapped,
		miscDB: db.NewMiscDB(mdb),
	}

	req := httptest.NewRequest(http.MethodGet, "/rest/system/cloudreve/browse?uri="+url.QueryEscape("cloudreve://my/base"), nil)
	rec := httptest.NewRecorder()
	svc.getSystemCloudreveBrowse(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status %d: %s", rec.Code, rec.Body.String())
	}
	if gotAuth != "Bearer static-token" {
		t.Fatalf("unexpected auth header: %q", gotAuth)
	}
	if gotURI != "cloudreve://my/base" {
		t.Fatalf("unexpected uri: %q", gotURI)
	}

	var resp cloudreveBrowseResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.Current != "cloudreve://my/base" {
		t.Fatalf("unexpected current path: %q", resp.Current)
	}
	if resp.Parent == nil || resp.Parent.Path != "cloudreve://my" {
		t.Fatalf("unexpected parent: %#v", resp.Parent)
	}
	if len(resp.Directories) != 1 || resp.Directories[0].Path != "cloudreve://my/base/docs" {
		t.Fatalf("unexpected directories: %#v", resp.Directories)
	}
}

func TestGetSystemCloudreveProfile(t *testing.T) {
	t.Parallel()

	cloudreveSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v4/user/me":
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
		case "/api/v4/devices/syncthing":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": 0,
				"data": map[string]any{
					"devices": []map[string]any{
						{
							"device_id": protocol.LocalDeviceID.String(),
							"short_id":  protocol.LocalDeviceID.Short().String(),
							"bind_uri":  "cloudreve://my/sync",
							"is_bound":  true,
							"online":    true,
						},
					},
				},
			})
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer cloudreveSrv.Close()

	mdb, err := sqlite.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		mdb.Close()
	})

	svc := &service{
		id: protocol.LocalDeviceID,
		cfg: config.Wrap("/dev/null", config.Configuration{
			Options: config.OptionsConfiguration{
				Cloudreve: config.CloudreveConfiguration{
					Enabled: true,
					Server:  cloudreveSrv.URL,
					Token:   "static-token",
				},
			},
		}, protocol.LocalDeviceID, events.NoopLogger),
		miscDB: db.NewMiscDB(mdb),
	}

	req := httptest.NewRequest(http.MethodGet, "/rest/system/cloudreve/profile", nil)
	rec := httptest.NewRecorder()
	svc.getSystemCloudreveProfile(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status %d: %s", rec.Code, rec.Body.String())
	}

	var resp cloudreveProfileResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if !resp.Configured || !resp.Authenticated {
		t.Fatalf("unexpected auth state: %#v", resp)
	}
	if resp.UserGroup != "admin" || !resp.CanAccessPublic {
		t.Fatalf("unexpected profile: %#v", resp)
	}
	if !resp.CurrentDeviceBound || resp.CurrentDeviceBindURI != "cloudreve://my/sync" {
		t.Fatalf("expected current device to be bound, got %#v", resp)
	}
	if resp.NeedsDeviceUnbind {
		t.Fatalf("did not expect unbind requirement: %#v", resp)
	}
	if resp.DeviceManagementURL != cloudreveSrv.URL+"/connect?tab=syncthing" {
		t.Fatalf("unexpected device management url: %#v", resp)
	}
}

func TestGetSystemCloudreveProfileRequiresUnbindForOtherDevice(t *testing.T) {
	t.Parallel()

	cloudreveSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v4/user/me":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": 0,
				"data": map[string]any{
					"email":    "user@example.com",
					"nickname": "User",
					"group": map[string]any{
						"name": "user",
					},
				},
			})
		case "/api/v4/devices/syncthing":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": 0,
				"data": map[string]any{
					"devices": []map[string]any{
						{
							"device_id": protocol.LocalDeviceID.String(),
							"short_id":  protocol.LocalDeviceID.Short().String(),
							"is_bound":  false,
							"online":    false,
						},
						{
							"device_id": "BLOCKED-DEVICE",
							"short_id":  "BLOCKED",
							"bind_uri":  "cloudreve://my/legacy",
							"is_bound":  true,
							"online":    true,
						},
					},
				},
			})
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer cloudreveSrv.Close()

	mdb, err := sqlite.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		mdb.Close()
	})

	svc := &service{
		id: protocol.LocalDeviceID,
		cfg: config.Wrap("/dev/null", config.Configuration{
			Options: config.OptionsConfiguration{
				Cloudreve: config.CloudreveConfiguration{
					Enabled: true,
					Server:  cloudreveSrv.URL,
					Token:   "static-token",
				},
			},
		}, protocol.LocalDeviceID, events.NoopLogger),
		miscDB: db.NewMiscDB(mdb),
	}

	req := httptest.NewRequest(http.MethodGet, "/rest/system/cloudreve/profile", nil)
	rec := httptest.NewRecorder()
	svc.getSystemCloudreveProfile(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status %d: %s", rec.Code, rec.Body.String())
	}

	var resp cloudreveProfileResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.CurrentDeviceBound {
		t.Fatalf("did not expect current device to be bound: %#v", resp)
	}
	if !resp.NeedsDeviceUnbind {
		t.Fatalf("expected unbind requirement: %#v", resp)
	}
	if resp.BlockingDeviceID != "BLOCKED-DEVICE" || resp.BlockingDeviceShortID != "BLOCKED" {
		t.Fatalf("unexpected blocking device: %#v", resp)
	}
	if resp.BlockingDeviceBindURI != "cloudreve://my/legacy" {
		t.Fatalf("unexpected blocking bind uri: %#v", resp)
	}
}

func TestGetSystemCloudreveTraffic(t *testing.T) {
	cloudreveSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/v4/user/me" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
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
	defer cloudreveSrv.Close()

	before := cloudreve.SnapshotTrafficStats()
	if _, err := cloudreve.NewClient(cloudreveSrv.URL, "static-token", nil).CurrentUser(context.Background()); err != nil {
		t.Fatal(err)
	}

	svc := &service{}
	req := httptest.NewRequest(http.MethodGet, "/rest/system/cloudreve/traffic", nil)
	rec := httptest.NewRecorder()
	svc.getSystemCloudreveTraffic(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status %d: %s", rec.Code, rec.Body.String())
	}

	var resp cloudreveTrafficResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.InBytesTotal <= before.InBytesTotal {
		t.Fatalf("expected Cloudreve inbound traffic to increase, before=%d after=%d", before.InBytesTotal, resp.InBytesTotal)
	}
}

func TestGetSystemCloudreveBrowseRejectsPublicForNonAdmin(t *testing.T) {
	t.Parallel()

	var userMeCalls int

	cloudreveSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v4/user/me":
			userMeCalls++
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": 0,
				"data": map[string]any{
					"email":    "user@example.com",
					"nickname": "User",
					"group": map[string]any{
						"name": "user",
					},
				},
			})
		case "/api/v4/file":
			t.Fatalf("public browse should not reach file listing for non-admin")
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer cloudreveSrv.Close()

	mdb, err := sqlite.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		mdb.Close()
	})

	svc := &service{
		id: protocol.LocalDeviceID,
		cfg: config.Wrap("/dev/null", config.Configuration{
			Options: config.OptionsConfiguration{
				Cloudreve: config.CloudreveConfiguration{
					Enabled: true,
					Server:  cloudreveSrv.URL,
					Token:   "static-token",
				},
			},
		}, protocol.LocalDeviceID, events.NoopLogger),
		miscDB: db.NewMiscDB(mdb),
	}

	req := httptest.NewRequest(http.MethodGet, "/rest/system/cloudreve/browse?uri="+url.QueryEscape("cloudreve://public/base"), nil)
	rec := httptest.NewRecorder()
	svc.getSystemCloudreveBrowse(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("unexpected status %d: %s", rec.Code, rec.Body.String())
	}
	if userMeCalls != 1 {
		t.Fatalf("expected one user profile request, got %d", userMeCalls)
	}
}
