// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/syncthing/syncthing/internal/cloudreve"
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

func TestCloudreveUploaderSummaryKeepsCompletedBytesUntilBatchFinishes(t *testing.T) {
	uploader := &cloudreveUploader{
		ev:                 events.NoopLogger,
		folders:            make(map[string]*cloudreveFolderState),
		uploadSlotsChanged: make(chan struct{}, 1),
		ensureSlotsChanged: make(chan struct{}, 1),
		ensuringDirs:       make(map[string]chan struct{}),
	}
	uploader.folders["default"] = &cloudreveFolderState{
		files: map[string]*cloudreveFileState{
			"queued.bin": {
				Path:         "queued.bin",
				BytesTotal:   100,
				TrackedBytes: 100,
				Queued:       true,
			},
			"uploading.bin": {
				Path:         "uploading.bin",
				BytesDone:    25,
				BytesTotal:   100,
				TrackedBytes: 100,
				Uploading:    true,
			},
		},
		failed:             make(map[string]FileError),
		ensuredDirs:        make(map[string]struct{}),
		progressTotalBytes: 200,
	}

	summary := uploader.Summary("default")
	if summary.TotalBytes != 200 || summary.DoneBytes != 25 {
		t.Fatalf("unexpected initial summary: %#v", summary)
	}

	requeue, deleteAfter, err := uploader.finishUpload("default", "uploading.bin", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if requeue || deleteAfter {
		t.Fatalf("expected upload to finish cleanly, got requeue=%v deleteAfter=%v", requeue, deleteAfter)
	}

	summary = uploader.Summary("default")
	if summary.TotalItems != 1 || summary.PendingItems != 1 || summary.UploadingItems != 0 {
		t.Fatalf("unexpected summary after first completion: %#v", summary)
	}
	if summary.TotalBytes != 200 || summary.DoneBytes != 100 {
		t.Fatalf("expected completed bytes to stay counted until the batch finishes, got %#v", summary)
	}

	uploader.folders["default"].files["queued.bin"].Queued = false
	uploader.folders["default"].files["queued.bin"].Uploading = true

	requeue, deleteAfter, err = uploader.finishUpload("default", "queued.bin", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if requeue || deleteAfter {
		t.Fatalf("expected final upload to finish cleanly, got requeue=%v deleteAfter=%v", requeue, deleteAfter)
	}

	summary = uploader.Summary("default")
	if summary.TotalBytes != 0 || summary.DoneBytes != 0 || summary.TotalItems != 0 {
		t.Fatalf("expected progress state to reset after the batch becomes idle, got %#v", summary)
	}
}

func TestCloudreveUploaderApplyRestoredConfigRewritesLocalDeviceID(t *testing.T) {
	uploader, wrapper, _ := newCloudreveUploaderTestHarness(t)

	restoredCfg := config.New(device1)
	restoredCfg.GUI.RawAddress = "127.0.0.1:19090"
	restoredCfg.Options.RawListenAddresses = []string{"tcp://0.0.0.0:25000"}
	restoredCfg.Defaults.Device.DeviceID = device1
	restoredCfg.Defaults.Device.IntroducedBy = device1
	restoredCfg.Defaults.Folder.FilesystemType = config.FilesystemTypeFake
	restoredCfg.Defaults.Folder.Path = "restored-default?content=true"
	restoredCfg.Defaults.Folder.Devices = []config.FolderDeviceConfiguration{
		{DeviceID: device1},
		{DeviceID: device2, IntroducedBy: device1},
	}
	restoredCfg.SetDevice(newDeviceConfiguration(restoredCfg.Defaults.Device, device2, "device2"))
	restoredCfg.Devices[1].IntroducedBy = device1

	folder := restoredCfg.Defaults.Folder.Copy()
	folder.ID = "restored"
	folder.Label = "restored"
	folder.Path = "restored-folder?content=true"
	folder.Devices = []config.FolderDeviceConfiguration{
		{DeviceID: device1},
		{DeviceID: device2, IntroducedBy: device1},
	}
	restoredCfg.SetFolder(folder)

	payload, err := json.Marshal(restoredCfg)
	if err != nil {
		t.Fatal(err)
	}

	var snapshot map[string]any
	if err := json.Unmarshal(payload, &snapshot); err != nil {
		t.Fatal(err)
	}

	if err := uploader.applyRestoredConfig(cloudreve.DeviceReportResponse{
		RestoreConfig:       snapshot,
		RestoreFromDeviceID: device1.String(),
	}); err != nil {
		t.Fatal(err)
	}

	got := wrapper.RawCopy()
	if got.GUI.RawAddress != "127.0.0.1:19090" {
		t.Fatalf("expected restored gui address, got %#v", got.GUI)
	}
	if _, _, ok := got.Device(device1); ok {
		t.Fatalf("expected old local device id to be replaced, got %#v", got.Devices)
	}
	if _, _, ok := got.Device(myID); !ok {
		t.Fatalf("expected current device to exist after restore, got %#v", got.Devices)
	}
	if _, _, ok := got.Device(device2); !ok {
		t.Fatalf("expected remote device to remain present after restore, got %#v", got.Devices)
	}
	if got.Defaults.Device.DeviceID == device1 || got.Defaults.Device.IntroducedBy == device1 {
		t.Fatalf("expected defaults device to be rewritten, got %#v", got.Defaults.Device)
	}
	if len(got.Folders) != 1 {
		t.Fatalf("expected restored folder to be applied, got %#v", got.Folders)
	}
	if folderDevice, ok := got.Folders[0].Device(myID); !ok {
		t.Fatalf("expected folder to include current device after restore, got %#v", got.Folders[0].Devices)
	} else if folderDevice.DeviceID != myID {
		t.Fatalf("expected folder local device to be rewritten, got %#v", got.Folders[0].Devices)
	}
	if _, ok := got.Folders[0].Device(device2); !ok {
		t.Fatalf("expected folder remote device to remain present after restore, got %#v", got.Folders[0].Devices)
	}
	for _, folderDevice := range got.Folders[0].Devices {
		if folderDevice.DeviceID == device1 || folderDevice.IntroducedBy == device1 {
			t.Fatalf("expected restored folder devices to stop referencing the old local device, got %#v", got.Folders[0].Devices)
		}
	}
	for _, folderDevice := range got.Defaults.Folder.Devices {
		if folderDevice.DeviceID == device1 || folderDevice.IntroducedBy == device1 {
			t.Fatalf("expected default folder devices to be rewritten, got %#v", got.Defaults.Folder.Devices)
		}
	}
}

func TestCloudreveUploaderRunHeartbeatRequestsReRegistrationWhenDeviceIsUnbound(t *testing.T) {
	uploader, wrapper, _ := newCloudreveUploaderTestHarness(t)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v4/devices/syncthing/heartbeat" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code": 40091,
			"msg":  "device not registered",
		})
	}))
	defer srv.Close()

	waiter, err := wrapper.Modify(func(cfg *config.Configuration) {
		cfg.Options.Cloudreve.Server = srv.URL
	})
	if err != nil {
		t.Fatal(err)
	}
	waiter.Wait()

	if err := uploader.runHeartbeat(context.Background()); err != nil {
		t.Fatal(err)
	}

	select {
	case <-uploader.reportRequests:
	case <-time.After(time.Second):
		t.Fatal("expected heartbeat to request device re-registration")
	}
}

func TestBufferCloudreveEventsDrainsBurstWithoutLoss(t *testing.T) {
	logger := events.NewLogger()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- logger.Serve(ctx)
	}()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}()

	sub := logger.Subscribe(events.LocalChangeDetected)
	defer sub.Unsubscribe()

	eventCh := bufferCloudreveEvents(ctx, sub, events.BufferSize*4)

	logger.Log(events.LocalChangeDetected, -1)
	select {
	case ev := <-eventCh:
		if got, ok := ev.Data.(int); !ok || got != -1 {
			t.Fatalf("unexpected warm-up event: %#v", ev)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for warm-up event")
	}

	const burstEvents = events.BufferSize * 4
	for i := 0; i < burstEvents; i++ {
		logger.Log(events.LocalChangeDetected, i)
	}

	received := make(map[int]struct{}, burstEvents)
	deadline := time.After(5 * time.Second)
	for len(received) < burstEvents {
		select {
		case ev := <-eventCh:
			got, ok := ev.Data.(int)
			if !ok {
				t.Fatalf("unexpected event payload type: %#v", ev.Data)
			}
			received[got] = struct{}{}
		case <-deadline:
			t.Fatalf("timed out after receiving %d/%d burst events", len(received), burstEvents)
		}
	}
}

func TestCloudreveUploaderEnsureRemoteFolderDeduplicatesAndCachesAncestors(t *testing.T) {
	uploader, wrapper, _ := newCloudreveUploaderTestHarness(t)
	const workers = 64

	var (
		mut     sync.Mutex
		uris    []string
		started = make(chan struct{}, 1)
		release = make(chan struct{})
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v4/file/create" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		select {
		case started <- struct{}{}:
		default:
		}
		<-release
		mut.Lock()
		uris = append(uris, body["uri"].(string))
		mut.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": nil})
	}))
	defer srv.Close()

	waiter, err := wrapper.Modify(func(cfg *config.Configuration) {
		cfg.Options.Cloudreve.Server = srv.URL
	})
	if err != nil {
		t.Fatal(err)
	}
	waiter.Wait()

	fcfg, ok := wrapper.Folder("default")
	if !ok {
		t.Fatal("missing folder config")
	}
	cloudCfg := wrapper.Options().Cloudreve.Normalized()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		go func() {
			errs <- uploader.ensureRemoteFolder(ctx, fcfg, cloudCfg, "a/b")
		}()
	}

	<-started
	close(release)

	for i := 0; i < workers; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}

	if err := uploader.ensureRemoteFolder(ctx, fcfg, cloudCfg, "a"); err != nil {
		t.Fatal(err)
	}

	mut.Lock()
	defer mut.Unlock()
	if len(uris) != 1 {
		t.Fatalf("expected a single remote ensure request, got %#v", uris)
	}
	if uris[0] != cloudreve.JoinURI(cloudreve.JoinURI(cloudCfg.BaseURI, fcfg.Label), "a/b") {
		t.Fatalf("unexpected ensure uri: %#v", uris)
	}
}

func TestCloudreveUploaderUploadFileRetriesTransientCloudreveErrors(t *testing.T) {
	uploader, wrapper, mdb := newCloudreveUploaderTestHarness(t)

	fcfg, ok := wrapper.Folder("default")
	if !ok {
		t.Fatal("missing folder config")
	}

	const relPath = "retry.bin"
	content := []byte("retry-content")
	fd, err := fcfg.Filesystem().Create(relPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fd.Write(content); err != nil {
		t.Fatal(err)
	}
	if err := fd.Close(); err != nil {
		t.Fatal(err)
	}
	putCloudreveLocalFileWithContent(t, mdb, relPath, 1, int64(len(content)))

	var (
		createCalls int32
		uploadCalls int32
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/v4/file/create":
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": nil})
		case r.Method == http.MethodPut && r.URL.Path == "/api/v4/file/upload":
			call := atomic.AddInt32(&createCalls, 1)
			if call == 1 {
				_ = json.NewEncoder(w).Encode(map[string]any{
					"code": 50001,
					"msg":  "Failed to start transaction",
				})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": 0,
				"data": map[string]any{
					"session_id": "retry-session",
					"chunk_size": len(content),
					"storage_policy": map[string]any{
						"type": "local",
					},
				},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v4/file/upload/retry-session/0":
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatal(err)
			}
			if len(body) != len(content) {
				t.Fatalf("unexpected upload length: got=%d want=%d", len(body), len(content))
			}
			atomic.AddInt32(&uploadCalls, 1)
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": nil})
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer srv.Close()

	waiter, err := wrapper.Modify(func(cfg *config.Configuration) {
		cfg.Options.Cloudreve.Server = srv.URL
	})
	if err != nil {
		t.Fatal(err)
	}
	waiter.Wait()

	if err := uploader.uploadFile(context.Background(), fcfg, relPath); err != nil {
		t.Fatal(err)
	}

	if got := atomic.LoadInt32(&createCalls); got != 2 {
		t.Fatalf("expected upload session creation to be retried once, got %d attempts", got)
	}
	if got := atomic.LoadInt32(&uploadCalls); got != 1 {
		t.Fatalf("expected a single successful upload, got %d", got)
	}
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

func TestCloudreveUploaderSyncDirectoryPendingSkipsStaleEntry(t *testing.T) {
	uploader, wrapper, _ := newCloudreveUploaderTestHarness(t)

	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		t.Fatalf("unexpected request for stale mkdir entry: %s %s", r.Method, r.URL.Path)
	}))
	defer srv.Close()

	waiter, err := wrapper.Modify(func(cfg *config.Configuration) {
		cfg.Options.Cloudreve.Server = srv.URL
	})
	if err != nil {
		t.Fatal(err)
	}
	waiter.Wait()

	entry := cloudrevePendingEntry{
		Folder:   "default",
		Path:     "dir",
		Action:   cloudrevePendingMkdir,
		Type:     "dir",
		Sequence: 1,
	}
	newer := entry
	newer.Sequence = 2
	if err := uploader.savePendingEntry(newer); err != nil {
		t.Fatal(err)
	}

	fcfg, ok := wrapper.Folder("default")
	if !ok {
		t.Fatal("missing folder config")
	}

	if err := uploader.syncDirectoryPending(context.Background(), fcfg, entry); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 0 {
		t.Fatalf("expected stale mkdir entry to be skipped, got %d requests", calls.Load())
	}
}

func TestCloudreveUploaderSyncDeletePendingSkipsStaleEntry(t *testing.T) {
	uploader, wrapper, _ := newCloudreveUploaderTestHarness(t)

	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		t.Fatalf("unexpected request for stale delete entry: %s %s", r.Method, r.URL.Path)
	}))
	defer srv.Close()

	waiter, err := wrapper.Modify(func(cfg *config.Configuration) {
		cfg.Options.Cloudreve.Server = srv.URL
	})
	if err != nil {
		t.Fatal(err)
	}
	waiter.Wait()

	entry := cloudrevePendingEntry{
		Folder:   "default",
		Path:     "file.txt",
		Action:   cloudrevePendingDelete,
		Type:     "file",
		Sequence: 1,
	}
	newer := entry
	newer.Action = cloudrevePendingUpload
	newer.Sequence = 2
	if err := uploader.savePendingEntry(newer); err != nil {
		t.Fatal(err)
	}

	fcfg, ok := wrapper.Folder("default")
	if !ok {
		t.Fatal("missing folder config")
	}

	if err := uploader.syncDeletePending(context.Background(), fcfg, entry); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 0 {
		t.Fatalf("expected stale delete entry to be skipped, got %d requests", calls.Load())
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
	putCloudreveLocalFileWithContent(t, mdb, path, sequence, 128)
}

func putCloudreveLocalFileWithContent(t *testing.T, mdb db.DB, path string, sequence int64, size int64) {
	t.Helper()
	if err := mdb.Update("default", protocol.LocalDeviceID, []protocol.FileInfo{{
		Name:      path,
		Sequence:  sequence,
		Type:      protocol.FileInfoTypeFile,
		Size:      size,
		ModifiedS: 1,
		Blocks: []protocol.BlockInfo{{
			Offset: 0,
			Size:   int(size),
			Hash:   []byte{1},
		}},
	}}); err != nil {
		t.Fatal(err)
	}
}
