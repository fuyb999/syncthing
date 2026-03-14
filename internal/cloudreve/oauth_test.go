// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package cloudreve

import (
	"strings"
	"testing"

	"github.com/syncthing/syncthing/lib/config"
)

func TestOAuthScopesDefaultsToDesktopCompatibleScopes(t *testing.T) {
	scopes := oauthScopes(config.CloudreveConfiguration{})
	expected := "profile email openid offline_access UserInfo.Write Workflow.Write Files.Write Shares.Write"
	if scopes != expected {
		t.Fatalf("unexpected default scopes: %q != %q", scopes, expected)
	}
}

func TestOAuthScopesAlwaysIncludesRequiredScopes(t *testing.T) {
	scopes := oauthScopes(config.CloudreveConfiguration{
		OAuthScopes: "profile email",
	})

	for _, required := range []string{"openid", "offline_access", "Files.Write"} {
		if !strings.Contains(scopes, required) {
			t.Fatalf("expected required scope %q in %q", required, scopes)
		}
	}
}
