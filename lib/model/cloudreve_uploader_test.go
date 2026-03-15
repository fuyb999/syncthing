// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"context"
	"testing"
	"time"

	"github.com/syncthing/syncthing/internal/db"
	"github.com/syncthing/syncthing/internal/db/sqlite"
	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/events"
	"github.com/syncthing/syncthing/lib/protocol"
)

func TestCloudreveUploaderWorkerCountLimitCanIncreaseAtRuntime(t *testing.T) {
	cfg := config.New(myID)
	cfg.Options.Cloudreve.WorkerCount = 1
	wrapper, cancel := newConfigWrapper(cfg)
	defer cancel()

	uploader := &cloudreveUploader{
		model:              &model{cfg: wrapper},
		folders:            make(map[string]*cloudreveFolderState),
		uploadSlotsChanged: make(chan struct{}, 1),
	}

	if !uploader.acquireUploadSlot(context.Background(), "", 1) {
		t.Fatal("expected first upload slot to be acquired")
	}
	defer uploader.releaseUploadSlot()

	waitCtx, waitCancel := context.WithTimeout(context.Background(), time.Second)
	defer waitCancel()

	secondSlot := make(chan bool, 1)
	go func() {
		secondSlot <- uploader.acquireUploadSlot(waitCtx, "", 1)
	}()

	select {
	case ok := <-secondSlot:
		if ok {
			uploader.releaseUploadSlot()
		}
		t.Fatal("expected second upload slot to wait for concurrency limit")
	case <-time.After(150 * time.Millisecond):
	}

	waiter, err := wrapper.Modify(func(cfg *config.Configuration) {
		cfg.Options.Cloudreve.WorkerCount = 2
	})
	if err != nil {
		t.Fatal(err)
	}
	waiter.Wait()

	select {
	case ok := <-secondSlot:
		if !ok {
			t.Fatal("expected second upload slot to succeed after increasing worker count")
		}
		uploader.releaseUploadSlot()
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for second upload slot after increasing worker count")
	}
}

func TestCloudreveUploaderPauseCancelsAndClearsFolderUploads(t *testing.T) {
	canceled := make(chan struct{}, 1)

	uploader := &cloudreveUploader{
		ev:                 events.NoopLogger,
		folders:            make(map[string]*cloudreveFolderState),
		uploadSlotsChanged: make(chan struct{}, 1),
	}
	uploader.folders["default"] = &cloudreveFolderState{
		files: map[string]*cloudreveFileState{
			"uploading.txt": {
				Path:      "uploading.txt",
				Uploading: true,
				cancel: func() {
					select {
					case canceled <- struct{}{}:
					default:
					}
				},
			},
			"queued.txt": {
				Path:   "queued.txt",
				Queued: true,
			},
		},
		failed: map[string]FileError{
			"uploading.txt": {Path: "uploading.txt", Err: "boom"},
			"queued.txt":    {Path: "queued.txt", Err: "boom"},
		},
	}

	uploader.pauseFolderUploads("default")

	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("expected in-flight upload to be canceled")
	}

	state := uploader.folders["default"]
	if _, ok := state.files["queued.txt"]; ok {
		t.Fatalf("expected queued upload to be cleared, got %#v", state.files)
	}
	file, ok := state.files["uploading.txt"]
	if !ok {
		t.Fatal("expected in-flight upload to remain tracked until cancellation completes")
	}
	if file.Obsolete != cloudreveObsoleteDrop {
		t.Fatalf("expected in-flight upload to be marked obsolete, got %#v", file)
	}
	if file.cancel != nil {
		t.Fatalf("expected in-flight upload cancel func to be cleared, got %#v", file)
	}
	if len(state.failed) != 0 {
		t.Fatalf("expected upload failures to be cleared, got %#v", state.failed)
	}
}

func TestCloudreveUploaderPauseKeepsPersistentPendingAndResumeRequeues(t *testing.T) {
	uploader, _, mdb := newCloudreveUploaderTestHarness(t)
	putCloudreveLocalFile(t, mdb, "queued.txt", 1)

	entry, ok, err := uploader.recordUploadPending("default", "queued.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected upload pending entry to be recorded")
	}
	if !uploader.enqueue("default", "queued.txt") {
		t.Fatal("expected upload to be queued")
	}

	uploader.pauseFolderUploads("default")

	if _, ok := uploader.folders["default"].files["queued.txt"]; ok {
		t.Fatalf("expected queued upload to be removed from memory on pause, got %#v", uploader.folders["default"].files)
	}

	stored, ok, err := uploader.pendingEntry("default", "queued.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || stored.Sequence != entry.Sequence || stored.Action != cloudrevePendingUpload {
		t.Fatalf("expected upload pending entry to remain persisted, got %#v ok=%v", stored, ok)
	}

	restoreCtx, cancel := context.WithCancel(context.Background())
	cancel()
	uploader.restorePending(restoreCtx, "default")

	file, ok := uploader.folders["default"].files["queued.txt"]
	if !ok || !file.Queued {
		t.Fatalf("expected paused upload to be requeued on restore, got %#v", uploader.folders["default"].files)
	}
}

func TestCloudreveUploaderRestorePendingRequeuesPersistedUploadsAfterRestart(t *testing.T) {
	uploader, wrapper, mdb := newCloudreveUploaderTestHarness(t)
	putCloudreveLocalFile(t, mdb, "restart.txt", 1)

	if _, ok, err := uploader.recordUploadPending("default", "restart.txt"); err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Fatal("expected upload pending entry to be recorded")
	}

	restarted := newCloudreveUploader(&model{
		cfg:      wrapper,
		sdb:      mdb,
		evLogger: events.NoopLogger,
	})
	restarted.ev = events.NoopLogger

	restoreCtx, cancel := context.WithCancel(context.Background())
	cancel()
	restarted.restorePending(restoreCtx, "default")

	file, ok := restarted.folders["default"].files["restart.txt"]
	if !ok || !file.Queued {
		t.Fatalf("expected restarted uploader to restore queued upload, got %#v", restarted.folders["default"].files)
	}
}

func TestCloudreveUploaderFinishUploadClearsOnlyCurrentPendingEntry(t *testing.T) {
	uploader, _, _ := newCloudreveUploaderTestHarness(t)

	t.Run("clears current entry", func(t *testing.T) {
		attempt := cloudrevePendingEntry{
			Folder:   "default",
			Path:     "same.txt",
			Action:   cloudrevePendingUpload,
			Type:     "file",
			Sequence: 1,
		}
		if err := uploader.savePendingEntry(attempt); err != nil {
			t.Fatal(err)
		}
		uploader.folders["default"] = &cloudreveFolderState{
			files: map[string]*cloudreveFileState{
				"same.txt": {
					Path:      "same.txt",
					Uploading: true,
				},
			},
			failed: make(map[string]FileError),
		}

		requeue, deleteAfter, err := uploader.finishUpload("default", "same.txt", &attempt, nil)
		if err != nil {
			t.Fatal(err)
		}
		if requeue || deleteAfter {
			t.Fatalf("expected upload to finish without requeue/deleteAfter, got requeue=%v deleteAfter=%v", requeue, deleteAfter)
		}
		if _, ok, err := uploader.pendingEntry("default", "same.txt"); err != nil {
			t.Fatal(err)
		} else if ok {
			t.Fatal("expected matching pending entry to be cleared after successful upload")
		}
	})

	t.Run("keeps newer entry", func(t *testing.T) {
		attempt := cloudrevePendingEntry{
			Folder:   "default",
			Path:     "same.txt",
			Action:   cloudrevePendingUpload,
			Type:     "file",
			Sequence: 1,
		}
		newer := attempt
		newer.Sequence = 2

		if err := uploader.savePendingEntry(newer); err != nil {
			t.Fatal(err)
		}
		uploader.folders["default"] = &cloudreveFolderState{
			files: map[string]*cloudreveFileState{
				"same.txt": {
					Path:      "same.txt",
					Uploading: true,
				},
			},
			failed: make(map[string]FileError),
		}

		requeue, deleteAfter, err := uploader.finishUpload("default", "same.txt", &attempt, nil)
		if err != nil {
			t.Fatal(err)
		}
		if requeue || deleteAfter {
			t.Fatalf("expected upload to finish without requeue/deleteAfter, got requeue=%v deleteAfter=%v", requeue, deleteAfter)
		}
		stored, ok, err := uploader.pendingEntry("default", "same.txt")
		if err != nil {
			t.Fatal(err)
		}
		if !ok || stored.Sequence != newer.Sequence {
			t.Fatalf("expected newer pending entry to remain, got %#v ok=%v", stored, ok)
		}
	})
}

func TestCloudreveUploaderRecursiveDeleteReplacesChildPendingEntries(t *testing.T) {
	uploader, _, _ := newCloudreveUploaderTestHarness(t)

	if err := uploader.savePendingEntry(cloudrevePendingEntry{
		Folder:   "default",
		Path:     "dir/file.txt",
		Action:   cloudrevePendingUpload,
		Type:     "file",
		Sequence: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := uploader.savePendingEntry(cloudrevePendingEntry{
		Folder:   "default",
		Path:     "dir/sub",
		Action:   cloudrevePendingMkdir,
		Type:     "dir",
		Sequence: 2,
	}); err != nil {
		t.Fatal(err)
	}

	entry, ok, err := uploader.recordDeletePending("default", "dir", true, "dir")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected recursive delete pending entry to be recorded")
	}
	if entry.Action != cloudrevePendingDelete {
		t.Fatalf("expected delete pending action, got %#v", entry)
	}

	entries, err := uploader.pendingEntries("default")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected only recursive delete entry to remain, got %#v", entries)
	}
	if entries[0].Path != "dir" || entries[0].Action != cloudrevePendingDelete || !entries[0].Recursive {
		t.Fatalf("expected recursive delete entry for dir to remain, got %#v", entries[0])
	}
}

func newCloudreveUploaderTestHarness(t *testing.T) (*cloudreveUploader, config.Wrapper, db.DB) {
	t.Helper()

	cfg := config.New(myID)
	cfg.Options.Cloudreve.Enabled = true
	cfg.Options.Cloudreve.Server = "http://cloudreve.example"
	cfg.Options.Cloudreve.Token = "token"

	wrapper, cancel := newConfigWrapper(cfg)
	t.Cleanup(cancel)

	fcfg := newFolderConfiguration(wrapper, "default", "default", config.FilesystemTypeFake, "default")
	fcfg.Type = config.FolderTypeUploadOnly
	setFolder(t, wrapper, fcfg)

	mdb, err := sqlite.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		mdb.Close()
	})

	uploader := newCloudreveUploader(&model{
		cfg:      wrapper,
		sdb:      mdb,
		evLogger: events.NoopLogger,
	})
	uploader.ev = events.NoopLogger
	return uploader, wrapper, mdb
}

func putCloudreveLocalFile(t *testing.T, mdb db.DB, path string, sequence int64) {
	t.Helper()
	if err := mdb.Update("default", protocol.LocalDeviceID, []protocol.FileInfo{{
		Name:      path,
		Sequence:  sequence,
		Type:      protocol.FileInfoTypeFile,
		Size:      128,
		ModifiedS: 1,
		Blocks: []protocol.BlockInfo{{
			Offset: 0,
			Size:   128,
			Hash:   []byte{1},
		}},
	}}); err != nil {
		t.Fatal(err)
	}
}
