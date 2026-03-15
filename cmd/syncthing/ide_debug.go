// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package main

// ideDebugCmd runs the normal Syncthing serve flow without the outer monitor
// process so IDE debuggers stay attached to the actual application process.
type ideDebugCmd struct {
	serveCmd `embed:""`
}

func (c *ideDebugCmd) Run() error {
	c.InternalInnerProcess = true
	c.NoRestart = true
	return c.serveCmd.Run()
}
