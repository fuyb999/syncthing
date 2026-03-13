// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"context"

	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/events"
	"github.com/syncthing/syncthing/lib/ignore"
	"github.com/syncthing/syncthing/lib/semaphore"
	"github.com/syncthing/syncthing/lib/versioner"
)

func init() {
	folderFactories[config.FolderTypeUploadOnly] = newUploadOnlyFolder
}

type uploadOnlyFolder struct {
	*folder
}

func newUploadOnlyFolder(model *model, ignores *ignore.Matcher, cfg config.FolderConfiguration, _ versioner.Versioner, evLogger events.Logger, ioLimiter *semaphore.Semaphore) service {
	f := &uploadOnlyFolder{
		folder: newFolder(model, ignores, cfg, evLogger, ioLimiter, nil),
	}
	f.puller = f
	return f
}

func (*uploadOnlyFolder) PullErrors() []FileError {
	return nil
}

func (*uploadOnlyFolder) pull(context.Context) (bool, error) {
	return true, nil
}
