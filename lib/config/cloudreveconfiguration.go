// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package config

type CloudreveConfiguration struct {
	Enabled     bool   `json:"enabled" xml:"enabled"`
	Server      string `json:"server" xml:"server"`
	Token       string `json:"token" xml:"token"`
	BaseURI     string `json:"baseURI" xml:"baseURI" default:"cloudreve://syncthing"`
	WorkerCount int    `json:"workerCount" xml:"workerCount" default:"2"`
}

func (c CloudreveConfiguration) Copy() CloudreveConfiguration {
	return c
}

func (c CloudreveConfiguration) IsReady() bool {
	return c.Enabled && c.Server != "" && c.Token != ""
}

func (c CloudreveConfiguration) HasCredentials() bool {
	return c.Enabled || c.Server != "" || c.Token != ""
}

func (c *CloudreveConfiguration) prepare() {
	if c.BaseURI == "" {
		c.BaseURI = "cloudreve://syncthing"
	}
	if c.WorkerCount < 1 {
		c.WorkerCount = 2
	}
}

func (c CloudreveConfiguration) Normalized() CloudreveConfiguration {
	normalized := c
	normalized.prepare()
	return normalized
}
