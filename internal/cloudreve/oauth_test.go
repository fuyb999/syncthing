// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package cloudreve

import (
	"strings"
	"testing"
	"time"

	"github.com/syncthing/syncthing/internal/db"
	"github.com/syncthing/syncthing/internal/db/sqlite"
	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/events"
	"github.com/syncthing/syncthing/lib/protocol"
)

func TestOAuthScopesDefaultsToDesktopCompatibleScopes(t *testing.T) {
	scopes := oauthScopes(config.CloudreveConfiguration{})
	expected := "profile email openid offline_access UserInfo.Read UserInfo.Write Workflow.Write Files.Read Files.Write Shares.Write"
	if scopes != expected {
		t.Fatalf("unexpected default scopes: %q != %q", scopes, expected)
	}
}

func TestOAuthScopesAlwaysIncludesRequiredScopes(t *testing.T) {
	scopes := oauthScopes(config.CloudreveConfiguration{
		OAuthScopes: "profile email",
	})

	for _, required := range []string{"openid", "offline_access", "UserInfo.Read", "Files.Read", "Files.Write"} {
		if !strings.Contains(scopes, required) {
			t.Fatalf("expected required scope %q in %q", required, scopes)
		}
	}
}

func TestOAuthStatusAvailableWithoutUploadSyncEnabled(t *testing.T) {
	mdb, err := sqlite.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		mdb.Close()
	})

	manager := NewOAuthManager(config.Wrap("/dev/null", config.Configuration{
		Options: config.OptionsConfiguration{
			Cloudreve: config.CloudreveConfiguration{
				Server:            "https://cloudreve.example.com",
				OAuthClientID:     "client-id",
				OAuthClientSecret: "client-secret",
			},
		},
	}, protocol.LocalDeviceID, events.NoopLogger), db.NewMiscDB(mdb), nil)

	status := manager.Status()
	if !status.Available {
		t.Fatal("expected oauth status to be available without enabling upload sync")
	}
	if status.Server != "https://cloudreve.example.com" {
		t.Fatalf("unexpected server: %q", status.Server)
	}
}

func TestOAuthStatusIncludesCurrentUserAccess(t *testing.T) {
	mdb, err := sqlite.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		mdb.Close()
	})

	manager := NewOAuthManager(config.Wrap("/dev/null", config.Configuration{
		Options: config.OptionsConfiguration{
			Cloudreve: config.CloudreveConfiguration{
				Server:            "https://cloudreve.example.com",
				OAuthClientID:     "client-id",
				OAuthClientSecret: "client-secret",
			},
		},
	}, protocol.LocalDeviceID, events.NoopLogger), db.NewMiscDB(mdb), nil)

	if err := manager.SaveSession(OAuthSession{
		RefreshToken:    "refresh-token",
		RefreshExpires:  time.Now().Add(time.Hour),
		UserName:        "admin",
		UserEmail:       "admin@example.com",
		UserGroup:       "admin",
		CanAccessPublic: true,
	}); err != nil {
		t.Fatal(err)
	}

	status := manager.Status()
	if !status.Authorized {
		t.Fatal("expected oauth status to be authorized")
	}
	if status.UserGroup != "admin" {
		t.Fatalf("unexpected user group: %q", status.UserGroup)
	}
	if !status.CanAccessPublic {
		t.Fatal("expected admin user to access public files")
	}
}
