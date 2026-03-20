// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"mime"
	"path"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/thejerf/suture/v4"

	"github.com/syncthing/syncthing/internal/cloudreve"
	"github.com/syncthing/syncthing/internal/db"
	"github.com/syncthing/syncthing/internal/slogutil"
	"github.com/syncthing/syncthing/lib/build"
	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/events"
	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/protocol"
	"github.com/syncthing/syncthing/lib/svcutil"
)

type cloudreveUploadSummary struct {
	TotalItems     int
	PendingItems   int
	UploadingItems int
	TotalBytes     int64
	DoneBytes      int64
}

type CloudreveUploadItem struct {
	Path       string `json:"path"`
	Status     string `json:"status"`
	BytesDone  int64  `json:"bytesDone"`
	BytesTotal int64  `json:"bytesTotal"`
	Error      string `json:"error,omitempty"`
}

type CloudreveUploadStatus struct {
	Page    int                   `json:"page"`
	Perpage int                   `json:"perpage"`
	Total   int                   `json:"total"`
	Items   []CloudreveUploadItem `json:"items"`
}

type cloudreveUploader struct {
	*suture.Supervisor

	model *model
	ev    events.Logger

	mut                sync.Mutex
	folders            map[string]*cloudreveFolderState
	oauth              *cloudreve.OAuthManager
	pendingKV          db.KV
	activeUploads      int
	uploadSlotsChanged chan struct{}
	uploadQueue        chan uploadJob
	activeEnsures      int
	ensureSlotsChanged chan struct{}
	pendingQueue       chan cloudrevePendingEntry
	ensuringDirs       map[string]chan struct{}
	reportRequests     chan struct{}
	activityRequests   chan time.Time
	activityMut        sync.Mutex
	latestSyncActivity time.Time
}

type cloudreveFolderState struct {
	files              map[string]*cloudreveFileState
	failed             map[string]FileError
	ensuredDirs        map[string]struct{}
	progressTotalBytes int64
	progressDoneBytes  int64
}

type cloudreveFileState struct {
	Path         string
	BytesDone    int64
	BytesTotal   int64
	TrackedBytes int64
	Queued       bool
	Uploading    bool
	Retry        bool
	Obsolete     cloudreveObsoleteAction
	cancel       context.CancelFunc
}

type cloudreveEventData struct {
	Folder string `json:"folder"`
	Action string `json:"action"`
	Type   string `json:"type"`
	Path   string `json:"path"`
}

type uploadJob struct {
	folder string
	path   string
}

type cloudrevePendingAction string

const (
	cloudrevePendingNamespace        = "misc/cloudreve/pending"
	cloudreveDeviceHeartbeatInterval = time.Minute
	cloudreveDeviceReportTimeout     = 20 * time.Second
	cloudreveSyncActivityDebounce    = 5 * time.Second
	cloudreveEnsureWorkerCount       = 1
	cloudreveEventBufferSize         = 16384
	cloudrevePendingQueueSize        = cloudreveEventBufferSize
	cloudrevePendingWorkerCount      = 1
	cloudreveUploadQueueSize         = 32768
	cloudreveUploadWorkerCount       = 64
	cloudreveUploadMaxAttempts       = 5
	cloudreveUploadRetryBaseDelay    = 100 * time.Millisecond
	cloudreveUploadRetryMaxDelay     = time.Second

	cloudrevePendingUpload cloudrevePendingAction = "upload"
	cloudrevePendingDelete cloudrevePendingAction = "delete"
	cloudrevePendingMkdir  cloudrevePendingAction = "mkdir"
)

type cloudreveObsoleteAction uint8

const (
	cloudreveObsoleteNone cloudreveObsoleteAction = iota
	cloudreveObsoleteDrop
	cloudreveObsoleteDelete
)

type cloudrevePendingEntry struct {
	Folder    string                 `json:"folder"`
	Path      string                 `json:"path"`
	Action    cloudrevePendingAction `json:"action"`
	Type      string                 `json:"type"`
	Sequence  int64                  `json:"sequence"`
	Recursive bool                   `json:"recursive,omitempty"`
}

func newCloudreveUploader(m *model) *cloudreveUploader {
	u := &cloudreveUploader{
		Supervisor:         suture.New("cloudreveUploader", svcutil.SpecWithDebugLogger()),
		model:              m,
		ev:                 m.evLogger,
		folders:            make(map[string]*cloudreveFolderState),
		oauth:              cloudreve.NewOAuthManager(m.cfg, db.NewMiscDB(m.sdb), nil),
		pendingKV:          m.sdb,
		uploadSlotsChanged: make(chan struct{}, 1),
		uploadQueue:        make(chan uploadJob, cloudreveUploadQueueSize),
		ensureSlotsChanged: make(chan struct{}, 1),
		pendingQueue:       make(chan cloudrevePendingEntry, cloudrevePendingQueueSize),
		ensuringDirs:       make(map[string]chan struct{}),
		reportRequests:     make(chan struct{}, 1),
		activityRequests:   make(chan time.Time, 1),
	}
	u.Add(svcutil.AsService(u.oauth.Serve, "cloudreveUploader/oauth"))
	u.Add(svcutil.AsService(u.listen, "cloudreveUploader/listen"))
	u.Add(svcutil.AsService(u.reportLoop, "cloudreveUploader/report"))
	return u
}

func (u *cloudreveUploader) String() string {
	return fmt.Sprintf("cloudreveUploader@%p", u)
}

func (u *cloudreveUploader) CommitConfiguration(_, _ config.Configuration) bool {
	u.mut.Lock()
	for _, state := range u.folders {
		clear(state.ensuredDirs)
	}
	u.mut.Unlock()
	u.requestDeviceReport()
	return true
}

func (u *cloudreveUploader) requestDeviceReport() {
	select {
	case u.reportRequests <- struct{}{}:
	default:
	}
}

func (u *cloudreveUploader) requestSyncActivity() {
	u.activityMut.Lock()
	u.latestSyncActivity = time.Now().UTC()
	u.activityMut.Unlock()

	select {
	case u.activityRequests <- time.Time{}:
	default:
	}
}

func (u *cloudreveUploader) reportLoop(ctx context.Context) error {
	cfg := u.model.cfg.Subscribe(u)
	defer u.model.cfg.Unsubscribe(u)

	u.CommitConfiguration(config.Configuration{}, cfg)

	heartbeatTicker := time.NewTicker(cloudreveDeviceHeartbeatInterval)
	defer heartbeatTicker.Stop()

	var (
		activityTimer  *time.Timer
		activityTimerC <-chan time.Time
		pendingSyncAt  time.Time
	)

	for {
		select {
		case <-ctx.Done():
			if activityTimer != nil {
				activityTimer.Stop()
			}
			return nil
		case <-u.reportRequests:
			if err := u.runDeviceReport(ctx); err != nil {
				if cloudreve.IsDeviceRegistrationConflict(err) {
					slog.Error(
						"Cloudreve device registration is blocked because another device with the same IP is still bound. Unbind the old device on the Cloudreve devices page and wait for automatic re-registration.",
						slogutil.Error(err),
					)
				} else {
					slog.Warn("Cloudreve device report failed", slogutil.Error(err))
				}
			}
		case <-heartbeatTicker.C:
			if err := u.runHeartbeat(ctx); err != nil {
				slog.Warn("Cloudreve device heartbeat failed", slogutil.Error(err))
			}
		case syncAt := <-u.activityRequests:
			u.activityMut.Lock()
			if u.latestSyncActivity.After(syncAt) {
				syncAt = u.latestSyncActivity
			}
			u.activityMut.Unlock()
			if syncAt.IsZero() {
				syncAt = time.Now().UTC()
			}
			if pendingSyncAt.Before(syncAt) {
				pendingSyncAt = syncAt
			}
			if activityTimer == nil {
				activityTimer = time.NewTimer(cloudreveSyncActivityDebounce)
			} else {
				if !activityTimer.Stop() {
					select {
					case <-activityTimer.C:
					default:
					}
				}
				activityTimer.Reset(cloudreveSyncActivityDebounce)
			}
			activityTimerC = activityTimer.C
		case <-activityTimerC:
			activityTimerC = nil
			activityTimer = nil
			if pendingSyncAt.IsZero() {
				continue
			}
			if err := u.runSyncActivity(ctx, pendingSyncAt); err != nil {
				slog.Warn("Cloudreve device sync activity report failed", slogutil.Error(err))
			}
			pendingSyncAt = time.Time{}
		}
	}
}

func (u *cloudreveUploader) runDeviceReport(parent context.Context) error {
	ctx, cancel := context.WithTimeout(parent, cloudreveDeviceReportTimeout)
	defer cancel()
	return u.reportDevice(ctx)
}

func (u *cloudreveUploader) runHeartbeat(parent context.Context) error {
	ctx, cancel := context.WithTimeout(parent, cloudreveDeviceReportTimeout)
	defer cancel()
	err := u.reportHeartbeat(ctx)
	if cloudreve.IsDeviceNotRegistered(err) {
		u.requestDeviceReport()
		return nil
	}
	return err
}

func (u *cloudreveUploader) runSyncActivity(parent context.Context, syncAt time.Time) error {
	ctx, cancel := context.WithTimeout(parent, cloudreveDeviceReportTimeout)
	defer cancel()
	err := u.reportSyncActivity(ctx, syncAt)
	if cloudreve.IsDeviceNotRegistered(err) {
		u.requestDeviceReport()
		return nil
	}
	return err
}

func (u *cloudreveUploader) reportDevice(ctx context.Context) error {
	client, cloudCfg, err := u.syncthingClient(ctx)
	if err != nil || client == nil {
		return filterCloudreveReportError(err)
	}

	rawCfg, err := u.configSnapshot()
	if err != nil {
		return err
	}

	resp, err := client.ReportDevice(ctx, cloudreve.DeviceReportRequest{
		DeviceID:      u.model.id.String(),
		ShortID:       u.model.shortID.String(),
		APIKey:        strings.TrimSpace(u.model.cfg.GUI().APIKey),
		JSONRaw:       rawCfg,
		BindURI:       u.deviceBindURI(cloudCfg),
		ClientVersion: build.Version,
		Platform:      runtime.GOOS + "/" + runtime.GOARCH,
	})
	if err != nil {
		return err
	}

	return u.applyRestoredConfig(resp)
}

func (u *cloudreveUploader) reportHeartbeat(ctx context.Context) error {
	client, cloudCfg, err := u.syncthingClient(ctx)
	if err != nil || client == nil {
		return filterCloudreveReportError(err)
	}

	return client.HeartbeatDevice(ctx, cloudreve.DeviceHeartbeatRequest{
		DeviceID: u.model.id.String(),
		ShortID:  u.model.shortID.String(),
		BindURI:  u.deviceBindURI(cloudCfg),
	})
}

func (u *cloudreveUploader) reportSyncActivity(ctx context.Context, syncAt time.Time) error {
	client, cloudCfg, err := u.syncthingClient(ctx)
	if err != nil || client == nil {
		return filterCloudreveReportError(err)
	}

	return client.ReportSyncActivity(ctx, cloudreve.DeviceActivityRequest{
		DeviceID: u.model.id.String(),
		ShortID:  u.model.shortID.String(),
		BindURI:  u.deviceBindURI(cloudCfg),
		SyncedAt: syncAt.UTC(),
	})
}

func (u *cloudreveUploader) syncthingClient(ctx context.Context) (*cloudreve.Client, config.CloudreveConfiguration, error) {
	cloudCfg := u.model.cfg.Options().Cloudreve.Normalized()
	if !cloudCfg.Enabled || strings.TrimSpace(cloudCfg.Server) == "" {
		return nil, cloudCfg, nil
	}

	token, err := u.oauth.AccessToken(ctx)
	if err != nil {
		return nil, cloudCfg, err
	}
	if token != "" {
		cloudCfg.Token = token
	}
	if !cloudCfg.IsReady() {
		return nil, cloudCfg, cloudreve.ErrOAuthNotConfigured
	}

	return cloudreve.NewClient(cloudCfg.Server, cloudCfg.Token, nil), cloudCfg, nil
}

func (u *cloudreveUploader) configSnapshot() (map[string]any, error) {
	raw, err := json.Marshal(u.model.cfg.RawCopy())
	if err != nil {
		return nil, err
	}

	var res map[string]any
	if err := json.Unmarshal(raw, &res); err != nil {
		return nil, err
	}
	if res == nil {
		res = map[string]any{}
	}
	return res, nil
}

func (u *cloudreveUploader) deviceBindURI(cloudCfg config.CloudreveConfiguration) string {
	return strings.TrimSpace(cloudCfg.BaseURI)
}

func (u *cloudreveUploader) applyRestoredConfig(resp cloudreve.DeviceReportResponse) error {
	if len(resp.RestoreConfig) == 0 {
		return nil
	}

	restoreFromText := strings.TrimSpace(resp.RestoreFromDeviceID)
	if restoreFromText == "" {
		return errors.New("cloudreve restore config missing restore_from_device_id")
	}

	restoreFrom, err := protocol.DeviceIDFromString(restoreFromText)
	if err != nil {
		return fmt.Errorf("cloudreve restore config has invalid restore_from_device_id: %w", err)
	}

	payload, err := json.Marshal(resp.RestoreConfig)
	if err != nil {
		return fmt.Errorf("marshal cloudreve restore config: %w", err)
	}

	restoredCfg, err := config.ReadJSON(bytes.NewReader(payload), restoreFrom)
	if err != nil {
		return fmt.Errorf("decode cloudreve restore config: %w", err)
	}

	replaceCloudreveRestoredDeviceID(&restoredCfg, restoreFrom, u.model.id)

	waiter, err := u.model.cfg.Modify(func(cfg *config.Configuration) {
		*cfg = restoredCfg
	})
	if err != nil {
		return fmt.Errorf("apply cloudreve restore config: %w", err)
	}
	waiter.Wait()

	slog.Info("Applied restored Syncthing configuration from Cloudreve", slog.String("sourceDeviceID", restoreFrom.String()))
	return nil
}

func filterCloudreveReportError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, cloudreve.ErrOAuthNotConfigured) || errors.Is(err, cloudreve.ErrOAuthNotAuthorized) {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return nil
	}
	return err
}

func replaceCloudreveRestoredDeviceID(cfg *config.Configuration, from, to protocol.DeviceID) {
	if from == to {
		return
	}

	for i := range cfg.Devices {
		if cfg.Devices[i].DeviceID == from {
			cfg.Devices[i].DeviceID = to
		}
		if cfg.Devices[i].IntroducedBy == from {
			cfg.Devices[i].IntroducedBy = to
		}
	}

	for i := range cfg.Folders {
		for j := range cfg.Folders[i].Devices {
			if cfg.Folders[i].Devices[j].DeviceID == from {
				cfg.Folders[i].Devices[j].DeviceID = to
			}
			if cfg.Folders[i].Devices[j].IntroducedBy == from {
				cfg.Folders[i].Devices[j].IntroducedBy = to
			}
		}
	}

	if cfg.Defaults.Device.DeviceID == from {
		cfg.Defaults.Device.DeviceID = to
	}
	if cfg.Defaults.Device.IntroducedBy == from {
		cfg.Defaults.Device.IntroducedBy = to
	}

	for i := range cfg.Defaults.Folder.Devices {
		if cfg.Defaults.Folder.Devices[i].DeviceID == from {
			cfg.Defaults.Folder.Devices[i].DeviceID = to
		}
		if cfg.Defaults.Folder.Devices[i].IntroducedBy == from {
			cfg.Defaults.Folder.Devices[i].IntroducedBy = to
		}
	}

	for i := range cfg.IgnoredDevices {
		if cfg.IgnoredDevices[i].ID == from {
			cfg.IgnoredDevices[i].ID = to
		}
	}
}

func (u *cloudreveUploader) Summary(folder string) cloudreveUploadSummary {
	u.mut.Lock()
	defer u.mut.Unlock()

	state, ok := u.folders[folder]
	if !ok {
		return cloudreveUploadSummary{}
	}

	var summary cloudreveUploadSummary
	summary.TotalBytes = state.progressTotalBytes
	summary.DoneBytes = state.progressDoneBytes
	for _, file := range state.files {
		if !file.Queued && !file.Uploading {
			continue
		}
		summary.TotalItems++
		done := file.BytesDone
		if file.TrackedBytes > 0 && done > file.TrackedBytes {
			done = file.TrackedBytes
		}
		summary.DoneBytes += done
		if file.Uploading {
			summary.UploadingItems++
		}
		if file.Queued {
			summary.PendingItems++
		}
	}
	if summary.DoneBytes > summary.TotalBytes {
		summary.DoneBytes = summary.TotalBytes
	}
	return summary
}

func (u *cloudreveUploader) Errors(folder string) []FileError {
	u.mut.Lock()
	defer u.mut.Unlock()

	state, ok := u.folders[folder]
	if !ok {
		return nil
	}

	errs := make([]FileError, 0, len(state.failed))
	for _, err := range state.failed {
		errs = append(errs, err)
	}
	slices.SortFunc(errs, func(a, b FileError) int {
		return strings.Compare(a.Path, b.Path)
	})
	return errs
}

func (u *cloudreveUploader) UploadStatus(folder string, page, perpage int) CloudreveUploadStatus {
	u.mut.Lock()
	defer u.mut.Unlock()

	res := CloudreveUploadStatus{
		Page:    page,
		Perpage: perpage,
	}
	if perpage <= 0 {
		perpage = 10
		res.Perpage = perpage
	}
	if page <= 0 {
		page = 1
		res.Page = page
	}

	state, ok := u.folders[folder]
	if !ok {
		return res
	}

	items := make([]CloudreveUploadItem, 0, len(state.files)+len(state.failed))
	for _, file := range state.files {
		if !file.Queued && !file.Uploading {
			continue
		}
		status := "queued"
		if file.Uploading {
			status = "uploading"
		}
		if file.Retry {
			status = "rescan-pending"
		}
		items = append(items, CloudreveUploadItem{
			Path:       file.Path,
			Status:     status,
			BytesDone:  file.BytesDone,
			BytesTotal: file.BytesTotal,
		})
	}
	slices.SortFunc(items, func(a, b CloudreveUploadItem) int {
		return strings.Compare(a.Path, b.Path)
	})

	res.Total = len(items)
	start := (page - 1) * perpage
	if start >= len(items) {
		res.Items = []CloudreveUploadItem{}
		return res
	}
	end := start + perpage
	if end > len(items) {
		end = len(items)
	}
	res.Items = items[start:end]
	return res
}

func (u *cloudreveUploader) listen(ctx context.Context) error {
	sub := u.ev.Subscribe(events.LocalChangeDetected | events.FolderPaused | events.FolderResumed)
	defer sub.Unsubscribe()
	eventCh := bufferCloudreveEvents(ctx, sub, cloudreveEventBufferSize)

	var workers sync.WaitGroup
	startCloudreveWorkers(&workers, ctx, cloudrevePendingWorkerCount, func() {
		for {
			select {
			case <-ctx.Done():
				return
			case entry := <-u.pendingQueue:
				u.processPendingEntry(ctx, entry)
			}
		}
	})
	startCloudreveWorkers(&workers, ctx, cloudreveUploadWorkerCount, func() {
		for {
			select {
			case <-ctx.Done():
				return
			case job := <-u.uploadQueue:
				u.processJobLoop(ctx, job)
			}
		}
	})
	defer workers.Wait()

	u.restorePending(ctx, "")

	for {
		select {
		case <-ctx.Done():
			return nil
		case ev, ok := <-eventCh:
			if !ok {
				return nil
			}
			switch ev.Type {
			case events.FolderPaused:
				payload, ok := ev.Data.(map[string]string)
				if !ok {
					continue
				}
				u.pauseFolderUploads(payload["id"])
			case events.FolderResumed:
				payload, ok := ev.Data.(map[string]string)
				if !ok {
					continue
				}
				u.restorePending(ctx, payload["id"])
			case events.LocalChangeDetected:
				payload, ok := ev.Data.(map[string]string)
				if !ok {
					continue
				}
				folderID := payload["folder"]
				cfg, ok := u.model.cfg.Folder(folderID)
				if !ok || cfg.Type != config.FolderTypeUploadOnly || cfg.Paused {
					continue
				}
				if !u.cloudreveEnabled(cfg) {
					continue
				}

				relPath := filepath.ToSlash(payload["path"])
				switch payload["type"] {
				case "dir":
					if payload["action"] == "deleted" {
						entry, ok, err := u.recordDeletePending(folderID, relPath, true, "dir")
						if err != nil {
							u.setFailure(folderID, relPath, err)
							slog.Warn("Cloudreve delete sync failed", cfg.LogAttr(), slog.String("path", relPath), slogutil.Error(err))
							continue
						}
						if ok {
							u.enqueuePendingEntry(ctx, entry)
						}
						continue
					}
					entry, ok, err := u.recordDirectoryPending(folderID, relPath)
					if err != nil {
						u.setFailure(folderID, relPath, err)
						slog.Warn("Cloudreve directory sync failed", cfg.LogAttr(), slog.String("path", relPath), slogutil.Error(err))
						continue
					}
					if ok {
						u.enqueuePendingEntry(ctx, entry)
					}
				case "file":
					if payload["action"] == "deleted" {
						entry, ok, err := u.recordDeletePending(folderID, relPath, false, "file")
						if err != nil {
							u.setFailure(folderID, relPath, err)
							slog.Warn("Cloudreve delete sync failed", cfg.LogAttr(), slog.String("path", relPath), slogutil.Error(err))
							continue
						}
						if ok {
							u.enqueuePendingEntry(ctx, entry)
						}
						continue
					}
					if _, ok, err := u.recordUploadPending(folderID, relPath); err != nil {
						u.setFailure(folderID, relPath, err)
						slog.Warn("Cloudreve file upload failed", cfg.LogAttr(), slog.String("path", relPath), slogutil.Error(err))
						continue
					} else if !ok {
						continue
					}
					if u.enqueue(folderID, relPath) {
						u.enqueueUploadJob(ctx, uploadJob{folder: folderID, path: relPath})
					}
				}
			}
		}
	}
}

func (u *cloudreveUploader) processJobLoop(ctx context.Context, job uploadJob) {
	for u.processJob(ctx, job) {
	}
}

func (u *cloudreveUploader) processJob(ctx context.Context, job uploadJob) bool {
	cfg, ok := u.model.cfg.Folder(job.folder)
	if !ok || cfg.Type != config.FolderTypeUploadOnly || cfg.Paused {
		u.removeFile(job.folder, job.path)
		return false
	}
	cloudCfg, err := u.cloudreveConfig(ctx, cfg)
	if err != nil {
		u.removeFile(job.folder, job.path)
		u.setFailure(job.folder, job.path, err)
		return false
	}

	if !u.acquireUploadSlot(ctx, job.folder, cloudCfg.WorkerCount) {
		return false
	}
	defer u.releaseUploadSlot()

	uploadCtx, started := u.markUploading(ctx, job.folder, job.path)
	if !started {
		return false
	}

	attempt, ok, err := u.pendingEntry(job.folder, job.path)
	if err != nil {
		_, _, _ = u.finishUpload(job.folder, job.path, nil, err)
		return false
	}
	if !ok || attempt.Action != cloudrevePendingUpload || attempt.Type != "file" {
		_, _, _ = u.finishUpload(job.folder, job.path, nil, nil)
		return false
	}

	err = u.uploadFile(uploadCtx, cfg, job.path)
	requeue, deleteAfter, reportedErr := u.finishUpload(job.folder, job.path, &attempt, err)
	if reportedErr != nil {
		slog.Warn("Cloudreve file upload failed", cfg.LogAttr(), slog.String("path", job.path), slogutil.Error(err))
		return false
	}

	if deleteAfter {
		if err := u.processCurrentDelete(ctx, cfg, job.path); err != nil {
			u.setFailure(job.folder, job.path, err)
			slog.Warn("Cloudreve delete-after-upload failed", cfg.LogAttr(), slog.String("path", job.path), slogutil.Error(err))
			return false
		}
	}
	if !requeue {
		u.requestSyncActivity()
	}
	return requeue
}

func (u *cloudreveUploader) uploadFile(ctx context.Context, cfg config.FolderConfiguration, relPath string) error {
	info, ok, err := u.model.sdb.GetDeviceFile(cfg.ID, protocol.LocalDeviceID, relPath)
	if err != nil {
		return err
	}
	if !ok || info.IsDeleted() || info.IsDirectory() || info.IsSymlink() || info.IsInvalid() || len(info.Blocks) == 0 {
		return nil
	}

	cloudCfg, err := u.cloudreveConfig(ctx, cfg)
	if err != nil {
		return err
	}
	client := cloudreve.NewClient(cloudCfg.Server, cloudCfg.Token, nil)
	rootURI := u.cloudreveFolderRootURI(cfg, cloudCfg)
	if err := u.ensureRemoteFolder(ctx, cfg, cloudCfg, path.Dir(relPath)); err != nil {
		return err
	}

	uri := cloudreve.JoinURI(rootURI, relPath)
	mimeType := mime.TypeByExtension(path.Ext(relPath))
	progress := func(done, total int64) {
		u.updateProgress(cfg.ID, relPath, done, total)
	}

	var lastErr error
	for attempt := 1; attempt <= cloudreveUploadMaxAttempts; attempt++ {
		fd, err := cfg.Filesystem().Open(relPath)
		if err != nil {
			if fs.IsNotExist(err) {
				return nil
			}
			return err
		}

		u.updateProgress(cfg.ID, relPath, 0, info.Size)
		err = client.UploadFile(ctx, uri, info.Size, info.ModTime().UnixMilli(), mimeType, fd, progress)
		_ = fd.Close()
		if err == nil {
			return nil
		}
		lastErr = err
		if !cloudreve.IsRetryableError(err) || attempt == cloudreveUploadMaxAttempts {
			return err
		}
		if err := sleepCloudreveRetry(ctx, cloudreveUploadRetryDelay(attempt)); err != nil {
			return err
		}
	}

	return lastErr
}

func (u *cloudreveUploader) cloudreveConfig(ctx context.Context, folder config.FolderConfiguration) (config.CloudreveConfiguration, error) {
	cloudCfg := u.model.cfg.Options().Cloudreve.Normalized()
	if token, err := u.oauth.AccessToken(ctx); err == nil && token != "" {
		cloudCfg.Token = token
	}
	if cloudCfg.IsReady() {
		return cloudCfg, nil
	}
	if folder.Cloudreve.HasCredentials() {
		return folder.Cloudreve.Normalized(), nil
	}
	if u.cloudreveEnabled(folder) {
		return cloudCfg, cloudreve.ErrOAuthNotAuthorized
	}
	return cloudCfg, nil
}

func (u *cloudreveUploader) cloudreveEnabled(folder config.FolderConfiguration) bool {
	cloudCfg := u.model.cfg.Options().Cloudreve.Normalized()
	if cloudCfg.Enabled && strings.TrimSpace(cloudCfg.Server) != "" && (strings.TrimSpace(cloudCfg.Token) != "" || cloudCfg.HasOAuthCredentials()) {
		return true
	}
	return folder.Cloudreve.HasCredentials()
}

func (u *cloudreveUploader) cloudreveFolderRootURI(folder config.FolderConfiguration, cloudCfg config.CloudreveConfiguration) string {
	name := strings.TrimSpace(folder.Label)
	if name == "" {
		name = strings.TrimSpace(folder.ID)
	}
	if name == "" {
		name = "folder"
	}
	return cloudreve.JoinURI(cloudCfg.BaseURI, name)
}

func (u *cloudreveUploader) folderState(folder string) *cloudreveFolderState {
	u.mut.Lock()
	defer u.mut.Unlock()
	return u.folderStateLocked(folder)
}

func (u *cloudreveUploader) folderStateLocked(folder string) *cloudreveFolderState {
	state, ok := u.folders[folder]
	if ok {
		return state
	}
	state = &cloudreveFolderState{
		files:       make(map[string]*cloudreveFileState),
		failed:      make(map[string]FileError),
		ensuredDirs: make(map[string]struct{}),
	}
	u.folders[folder] = state
	return state
}

func normalizeCloudreveDirectoryPath(relPath string) string {
	relPath = path.Clean(strings.TrimSpace(filepath.ToSlash(relPath)))
	if relPath == "." || relPath == "/" {
		return ""
	}
	return strings.TrimPrefix(relPath, "/")
}

func cloudreveEnsuredPathPrefixes(relPath string) []string {
	relPath = normalizeCloudreveDirectoryPath(relPath)
	if relPath == "" {
		return []string{""}
	}

	parts := strings.Split(relPath, "/")
	res := make([]string, 0, len(parts)+1)
	res = append(res, "")
	for i := range parts {
		res = append(res, strings.Join(parts[:i+1], "/"))
	}
	return res
}

func (u *cloudreveUploader) markEnsuredDirLocked(folder, relPath string) {
	state := u.folderStateLocked(folder)
	for _, ensured := range cloudreveEnsuredPathPrefixes(relPath) {
		state.ensuredDirs[ensured] = struct{}{}
	}
}

func (u *cloudreveUploader) isEnsuredDirLocked(folder, relPath string) bool {
	state, ok := u.folders[folder]
	if !ok {
		return false
	}
	_, ok = state.ensuredDirs[normalizeCloudreveDirectoryPath(relPath)]
	return ok
}

func (u *cloudreveUploader) clearEnsuredDirLocked(folder, relPath string, recursive bool) {
	state, ok := u.folders[folder]
	if !ok {
		return
	}
	relPath = normalizeCloudreveDirectoryPath(relPath)
	for ensured := range state.ensuredDirs {
		if matchesCloudrevePath(ensured, relPath, recursive) {
			delete(state.ensuredDirs, ensured)
		}
	}
}

func (s *cloudreveFolderState) adjustProgressTotal(delta int64) {
	s.progressTotalBytes += delta
	if s.progressTotalBytes < 0 {
		s.progressTotalBytes = 0
	}
	if s.progressDoneBytes > s.progressTotalBytes {
		s.progressDoneBytes = s.progressTotalBytes
	}
}

func (s *cloudreveFolderState) updateTrackedBytes(file *cloudreveFileState, size int64) {
	s.adjustProgressTotal(size - file.TrackedBytes)
	file.TrackedBytes = size
}

func (s *cloudreveFolderState) dropTrackedBytes(file *cloudreveFileState) {
	if file.TrackedBytes == 0 {
		return
	}
	s.adjustProgressTotal(-file.TrackedBytes)
	file.TrackedBytes = 0
}

func (s *cloudreveFolderState) commitTrackedBytes(file *cloudreveFileState) {
	if file.TrackedBytes <= 0 {
		return
	}
	s.progressDoneBytes += file.TrackedBytes
	if s.progressDoneBytes > s.progressTotalBytes {
		s.progressDoneBytes = s.progressTotalBytes
	}
	file.TrackedBytes = 0
}

func (s *cloudreveFolderState) hasActiveUploads() bool {
	for _, file := range s.files {
		if file.Queued || file.Uploading {
			return true
		}
	}
	return false
}

func (s *cloudreveFolderState) resetProgressIfIdle() {
	if s.hasActiveUploads() {
		return
	}
	s.progressTotalBytes = 0
	s.progressDoneBytes = 0
}

func (u *cloudreveUploader) enqueue(folder, relPath string) bool {
	var size int64
	if info, ok, err := u.model.sdb.GetDeviceFile(folder, protocol.LocalDeviceID, relPath); err == nil && ok {
		size = info.Size
	}

	u.mut.Lock()
	defer u.mut.Unlock()

	state := u.folderStateLocked(folder)
	file, ok := state.files[relPath]
	if !ok {
		state.files[relPath] = &cloudreveFileState{
			Path:         relPath,
			BytesTotal:   size,
			TrackedBytes: size,
			Queued:       true,
		}
		state.adjustProgressTotal(size)
		u.emitProgress(folder)
		return true
	}
	if file.Uploading {
		file.Retry = true
		if size != file.TrackedBytes {
			state.updateTrackedBytes(file, size)
			u.emitProgress(folder)
		}
		return false
	}
	if file.Queued {
		if size != file.TrackedBytes || file.BytesTotal != size {
			state.updateTrackedBytes(file, size)
			file.BytesTotal = size
			u.emitProgress(folder)
		}
		return false
	}
	state.updateTrackedBytes(file, size)
	file.Queued = true
	file.BytesDone = 0
	file.BytesTotal = size
	u.emitProgress(folder)
	return true
}

func (u *cloudreveUploader) markUploading(parentCtx context.Context, folder, relPath string) (context.Context, bool) {
	u.mut.Lock()
	defer u.mut.Unlock()

	state, ok := u.folders[folder]
	if !ok {
		return nil, false
	}
	file, ok := state.files[relPath]
	if !ok || !file.Queued {
		return nil, false
	}
	file.Queued = false
	file.Uploading = true
	file.BytesDone = 0
	file.Obsolete = cloudreveObsoleteNone
	uploadCtx, cancel := context.WithCancel(parentCtx)
	file.cancel = cancel
	u.emitProgress(folder)
	return uploadCtx, true
}

func (u *cloudreveUploader) updateProgress(folder, relPath string, done, total int64) {
	u.mut.Lock()
	defer u.mut.Unlock()

	state, ok := u.folders[folder]
	if !ok {
		return
	}
	file, ok := state.files[relPath]
	if !ok {
		return
	}
	file.BytesDone = done
	file.BytesTotal = total
	u.emitProgress(folder)
}

func (u *cloudreveUploader) finishUpload(folder, relPath string, attempt *cloudrevePendingEntry, err error) (bool, bool, error) {
	var (
		clearPending bool
		deleteAfter  bool
	)

	u.mut.Lock()

	state, ok := u.folders[folder]
	if !ok {
		u.mut.Unlock()
		if err != nil {
			return false, false, err
		}
		return false, false, nil
	}
	file, ok := state.files[relPath]
	if !ok {
		u.mut.Unlock()
		if err != nil {
			return false, false, err
		}
		return false, false, nil
	}
	file.Uploading = false
	file.cancel = nil
	requeue := file.Retry
	file.Retry = false

	if file.Obsolete == cloudreveObsoleteDrop {
		requeue = false
	}

	if file.Obsolete != cloudreveObsoleteNone {
		clearPending = err == nil && !requeue && file.Obsolete == cloudreveObsoleteDrop
		deleteAfter = err == nil && !requeue && file.Obsolete == cloudreveObsoleteDelete
		if requeue {
			file.Queued = true
			file.Obsolete = cloudreveObsoleteNone
			file.BytesDone = 0
			file.BytesTotal = file.TrackedBytes
			delete(state.failed, relPath)
		} else {
			state.dropTrackedBytes(file)
			delete(state.files, relPath)
			delete(state.failed, relPath)
		}
		state.resetProgressIfIdle()
		u.emitProgress(folder)
		u.emitErrorsLocked(folder)
		u.mut.Unlock()
		if clearPending && attempt != nil {
			if _, err := u.deletePendingIfCurrent(*attempt); err != nil {
				u.setFailure(folder, relPath, err)
				return false, false, err
			}
		}
		return requeue, deleteAfter, nil
	}

	if err != nil {
		state.dropTrackedBytes(file)
		state.resetProgressIfIdle()
		state.failed[relPath] = FileError{Path: relPath, Err: err.Error()}
		u.emitProgress(folder)
		u.emitErrorsLocked(folder)
		u.mut.Unlock()
		return false, false, err
	}

	clearPending = !requeue
	if requeue {
		file.Queued = true
		file.BytesDone = 0
		file.BytesTotal = file.TrackedBytes
		delete(state.failed, relPath)
	} else {
		state.commitTrackedBytes(file)
		delete(state.files, relPath)
		delete(state.failed, relPath)
	}
	state.resetProgressIfIdle()
	u.emitProgress(folder)
	u.emitErrorsLocked(folder)
	u.mut.Unlock()

	if clearPending && attempt != nil {
		if _, err := u.deletePendingIfCurrent(*attempt); err != nil {
			u.setFailure(folder, relPath, err)
			return false, false, err
		}
	}
	return requeue, false, nil
}

func (u *cloudreveUploader) deleteRemotePath(ctx context.Context, cfg config.FolderConfiguration, relPath string) error {
	cloudCfg, err := u.cloudreveConfig(ctx, cfg)
	if err != nil {
		return err
	}
	client := cloudreve.NewClient(cloudCfg.Server, cloudCfg.Token, nil)
	return client.Delete(ctx, cloudreve.JoinURI(u.cloudreveFolderRootURI(cfg, cloudCfg), relPath))
}

func (u *cloudreveUploader) obsoletePath(folder, relPath string, recursive bool, action cloudreveObsoleteAction) {
	var cancels []context.CancelFunc

	u.mut.Lock()
	state, ok := u.folders[folder]
	if ok {
		for path, file := range state.files {
			if !matchesCloudrevePath(path, relPath, recursive) {
				continue
			}
			delete(state.failed, path)
			if file.Uploading {
				file.Obsolete = mergeCloudreveObsoleteAction(file.Obsolete, action)
				if file.cancel != nil {
					cancels = append(cancels, file.cancel)
					file.cancel = nil
				}
				continue
			}
			state.dropTrackedBytes(file)
			delete(state.files, path)
		}
		state.resetProgressIfIdle()
		u.emitProgress(folder)
		u.emitErrorsLocked(folder)
	}
	u.mut.Unlock()

	for _, cancel := range cancels {
		cancel()
	}
}

func (u *cloudreveUploader) pauseFolderUploads(folder string) {
	if strings.TrimSpace(folder) == "" {
		return
	}
	u.obsoletePath(folder, "", true, cloudreveObsoleteDrop)
}

func (u *cloudreveUploader) clearFailuresMatching(folder, relPath string, recursive bool) {
	u.mut.Lock()
	defer u.mut.Unlock()

	state, ok := u.folders[folder]
	if !ok {
		return
	}
	for path := range state.failed {
		if matchesCloudrevePath(path, relPath, recursive) {
			delete(state.failed, path)
		}
	}
	u.emitErrorsLocked(folder)
}

func matchesCloudrevePath(candidate, target string, recursive bool) bool {
	if candidate == target {
		return true
	}
	if !recursive {
		return false
	}
	target = strings.TrimSuffix(target, "/")
	if target == "" {
		return true
	}
	return strings.HasPrefix(candidate, target+"/")
}

func (u *cloudreveUploader) setFailure(folder, relPath string, err error) {
	u.mut.Lock()
	defer u.mut.Unlock()

	state := u.folderStateLocked(folder)
	state.failed[relPath] = FileError{Path: relPath, Err: err.Error()}
	u.emitErrorsLocked(folder)
}

func (u *cloudreveUploader) clearFailure(folder, relPath string) {
	u.mut.Lock()
	defer u.mut.Unlock()

	state, ok := u.folders[folder]
	if !ok {
		return
	}
	delete(state.failed, relPath)
	u.emitErrorsLocked(folder)
}

func (u *cloudreveUploader) removeFile(folder, relPath string) {
	u.mut.Lock()
	defer u.mut.Unlock()

	state, ok := u.folders[folder]
	if !ok {
		return
	}
	if file, exists := state.files[relPath]; exists {
		state.dropTrackedBytes(file)
	}
	delete(state.files, relPath)
	delete(state.failed, relPath)
	state.resetProgressIfIdle()
	u.emitProgress(folder)
	u.emitErrorsLocked(folder)
}

func (u *cloudreveUploader) emitProgress(folder string) {
	summary := u.summaryLocked(folder)
	u.ev.Log(events.DownloadProgress, map[string]map[string]*PullerProgress{
		folder: {
			".cloudreve-upload": {
				Total:      summary.TotalItems,
				Pulling:    summary.UploadingItems,
				BytesTotal: summary.TotalBytes,
				BytesDone:  summary.DoneBytes,
			},
		},
	})
}

func (u *cloudreveUploader) summaryLocked(folder string) cloudreveUploadSummary {
	state, ok := u.folders[folder]
	if !ok {
		return cloudreveUploadSummary{}
	}

	var summary cloudreveUploadSummary
	summary.TotalBytes = state.progressTotalBytes
	summary.DoneBytes = state.progressDoneBytes
	for _, file := range state.files {
		if !file.Queued && !file.Uploading {
			continue
		}
		summary.TotalItems++
		done := file.BytesDone
		if file.TrackedBytes > 0 && done > file.TrackedBytes {
			done = file.TrackedBytes
		}
		summary.DoneBytes += done
		if file.Queued {
			summary.PendingItems++
		}
		if file.Uploading {
			summary.UploadingItems++
		}
	}
	if summary.DoneBytes > summary.TotalBytes {
		summary.DoneBytes = summary.TotalBytes
	}
	return summary
}

func (u *cloudreveUploader) emitErrorsLocked(folder string) {
	errors := make([]FileError, 0)
	if state, ok := u.folders[folder]; ok {
		errors = make([]FileError, 0, len(state.failed))
		for _, err := range state.failed {
			errors = append(errors, err)
		}
		slices.SortFunc(errors, func(a, b FileError) int {
			return strings.Compare(a.Path, b.Path)
		})
	}
	u.ev.Log(events.FolderErrors, map[string]interface{}{
		"folder": folder,
		"errors": errors,
	})
}

func (u *cloudreveUploader) ensureRemoteFolder(ctx context.Context, cfg config.FolderConfiguration, cloudCfg config.CloudreveConfiguration, relPath string) error {
	relPath = normalizeCloudreveDirectoryPath(relPath)

	key := cfg.ID + "\x00" + relPath
	for {
		u.mut.Lock()
		if u.isEnsuredDirLocked(cfg.ID, relPath) {
			u.mut.Unlock()
			return nil
		}
		if wait, ok := u.ensuringDirs[key]; ok {
			u.mut.Unlock()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-wait:
				continue
			}
		}
		wait := make(chan struct{})
		u.ensuringDirs[key] = wait
		u.mut.Unlock()

		var err error
		if !u.acquireEnsureSlot(ctx) {
			err = ctx.Err()
		} else {
			client := cloudreve.NewClient(cloudCfg.Server, cloudCfg.Token, nil)
			err = client.EnsureFolder(ctx, cloudreve.JoinURI(u.cloudreveFolderRootURI(cfg, cloudCfg), relPath))
			u.releaseEnsureSlot()
		}

		u.mut.Lock()
		delete(u.ensuringDirs, key)
		if err == nil {
			u.markEnsuredDirLocked(cfg.ID, relPath)
		}
		close(wait)
		u.mut.Unlock()
		return err
	}
}

func (u *cloudreveUploader) uploadWorkerCount(folder string, fallback int) int {
	if cfg, ok := u.model.cfg.Folder(folder); ok && cfg.Cloudreve.HasCredentials() {
		if workerCount := cfg.Cloudreve.Normalized().WorkerCount; workerCount > 0 {
			return workerCount
		}
	}
	if workerCount := u.model.cfg.Options().Cloudreve.Normalized().WorkerCount; workerCount > 0 {
		return workerCount
	}
	if fallback > 0 {
		return fallback
	}
	return 1
}

func (u *cloudreveUploader) acquireEnsureSlot(ctx context.Context) bool {
	for {
		u.mut.Lock()
		if u.activeEnsures < cloudreveEnsureWorkerCount {
			u.activeEnsures++
			u.mut.Unlock()
			return true
		}
		u.mut.Unlock()

		timer := time.NewTimer(250 * time.Millisecond)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return false
		case <-u.ensureSlotsChanged:
			if !timer.Stop() {
				<-timer.C
			}
		case <-timer.C:
		}
	}
}

func (u *cloudreveUploader) releaseEnsureSlot() {
	u.mut.Lock()
	if u.activeEnsures > 0 {
		u.activeEnsures--
	}
	u.mut.Unlock()

	select {
	case u.ensureSlotsChanged <- struct{}{}:
	default:
	}
}

func (u *cloudreveUploader) acquireUploadSlot(ctx context.Context, folder string, fallback int) bool {
	for {
		u.mut.Lock()
		if u.activeUploads < u.uploadWorkerCount(folder, fallback) {
			u.activeUploads++
			u.mut.Unlock()
			return true
		}
		u.mut.Unlock()

		timer := time.NewTimer(250 * time.Millisecond)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return false
		case <-u.uploadSlotsChanged:
			if !timer.Stop() {
				<-timer.C
			}
		case <-timer.C:
		}
	}
}

func (u *cloudreveUploader) releaseUploadSlot() {
	u.mut.Lock()
	if u.activeUploads > 0 {
		u.activeUploads--
	}
	u.mut.Unlock()

	select {
	case u.uploadSlotsChanged <- struct{}{}:
	default:
	}
}

func mergeCloudreveObsoleteAction(current, next cloudreveObsoleteAction) cloudreveObsoleteAction {
	if current == cloudreveObsoleteDelete || next == cloudreveObsoleteDelete {
		return cloudreveObsoleteDelete
	}
	if current == cloudreveObsoleteDrop || next == cloudreveObsoleteDrop {
		return cloudreveObsoleteDrop
	}
	return cloudreveObsoleteNone
}

func (u *cloudreveUploader) pendingDB() *db.Typed {
	if u.pendingKV == nil {
		return nil
	}
	return db.NewTyped(u.pendingKV, cloudrevePendingNamespace)
}

func cloudrevePendingRelativeKey(folder, relPath string) string {
	return folder + "/" + relPath
}

func cloudrevePendingPrefix(folder string) string {
	prefix := cloudrevePendingNamespace + "/"
	if folder != "" {
		prefix += folder + "/"
	}
	return prefix
}

func (u *cloudreveUploader) savePendingEntry(entry cloudrevePendingEntry) error {
	store := u.pendingDB()
	if store == nil {
		return nil
	}
	bs, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	return store.PutBytes(cloudrevePendingRelativeKey(entry.Folder, entry.Path), bs)
}

func (u *cloudreveUploader) deletePendingEntry(folder, relPath string) error {
	store := u.pendingDB()
	if store == nil {
		return nil
	}
	return store.Delete(cloudrevePendingRelativeKey(folder, relPath))
}

func (u *cloudreveUploader) pendingEntry(folder, relPath string) (cloudrevePendingEntry, bool, error) {
	store := u.pendingDB()
	if store == nil {
		return cloudrevePendingEntry{}, false, nil
	}
	bs, ok, err := store.Bytes(cloudrevePendingRelativeKey(folder, relPath))
	if err != nil || !ok {
		return cloudrevePendingEntry{}, ok, err
	}
	var entry cloudrevePendingEntry
	if err := json.Unmarshal(bs, &entry); err != nil {
		if delErr := store.Delete(cloudrevePendingRelativeKey(folder, relPath)); delErr != nil {
			return cloudrevePendingEntry{}, false, delErr
		}
		return cloudrevePendingEntry{}, false, nil
	}
	entry.Folder = folder
	entry.Path = relPath
	return entry, true, nil
}

func (u *cloudreveUploader) pendingEntries(folder string) ([]cloudrevePendingEntry, error) {
	if u.pendingKV == nil {
		return nil, nil
	}
	it, errFn := u.pendingKV.PrefixKV(cloudrevePendingPrefix(folder))
	entries := make([]cloudrevePendingEntry, 0)
	for kv := range it {
		entry, ok, err := u.decodePendingKV(kv)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		entries = append(entries, entry)
	}
	if err := errFn(); err != nil {
		return nil, err
	}
	slices.SortFunc(entries, func(a, b cloudrevePendingEntry) int {
		if cmp := strings.Compare(a.Folder, b.Folder); cmp != 0 {
			return cmp
		}
		return strings.Compare(a.Path, b.Path)
	})
	return entries, nil
}

func (u *cloudreveUploader) decodePendingKV(kv db.KeyValue) (cloudrevePendingEntry, bool, error) {
	prefix := cloudrevePendingPrefix("")
	if !strings.HasPrefix(kv.Key, prefix) {
		return cloudrevePendingEntry{}, false, nil
	}
	rest := strings.TrimPrefix(kv.Key, prefix)
	folder, relPath, ok := strings.Cut(rest, "/")
	if !ok || folder == "" || relPath == "" {
		if err := u.pendingKV.DeleteKV(kv.Key); err != nil {
			return cloudrevePendingEntry{}, false, err
		}
		return cloudrevePendingEntry{}, false, nil
	}
	var entry cloudrevePendingEntry
	if err := json.Unmarshal(kv.Value, &entry); err != nil {
		if err := u.pendingKV.DeleteKV(kv.Key); err != nil {
			return cloudrevePendingEntry{}, false, err
		}
		return cloudrevePendingEntry{}, false, nil
	}
	entry.Folder = folder
	entry.Path = relPath
	return entry, true, nil
}

func (u *cloudreveUploader) deletePendingMatching(folder, relPath string, recursive bool) error {
	if !recursive {
		return u.deletePendingEntry(folder, relPath)
	}
	entries, err := u.pendingEntries(folder)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !matchesCloudrevePath(entry.Path, relPath, true) {
			continue
		}
		if err := u.deletePendingEntry(entry.Folder, entry.Path); err != nil {
			return err
		}
	}
	return nil
}

func (u *cloudreveUploader) deletePendingIfCurrent(entry cloudrevePendingEntry) (bool, error) {
	current, ok, err := u.pendingEntry(entry.Folder, entry.Path)
	if err != nil || !ok {
		return false, err
	}
	if current.Action != entry.Action || current.Type != entry.Type || current.Sequence != entry.Sequence || current.Recursive != entry.Recursive {
		return false, nil
	}
	return true, u.deletePendingEntry(entry.Folder, entry.Path)
}

func (u *cloudreveUploader) recordUploadPending(folder, relPath string) (cloudrevePendingEntry, bool, error) {
	info, ok, err := u.model.sdb.GetDeviceFile(folder, protocol.LocalDeviceID, relPath)
	if err != nil {
		return cloudrevePendingEntry{}, false, err
	}
	if !ok || info.IsDeleted() || info.IsDirectory() || info.IsSymlink() || info.IsInvalid() || len(info.Blocks) == 0 {
		return cloudrevePendingEntry{}, false, nil
	}
	entry := cloudrevePendingEntry{
		Folder:   folder,
		Path:     relPath,
		Action:   cloudrevePendingUpload,
		Type:     "file",
		Sequence: info.Sequence,
	}
	return entry, true, u.savePendingEntry(entry)
}

func (u *cloudreveUploader) recordDirectoryPending(folder, relPath string) (cloudrevePendingEntry, bool, error) {
	info, ok, err := u.model.sdb.GetDeviceFile(folder, protocol.LocalDeviceID, relPath)
	if err != nil {
		return cloudrevePendingEntry{}, false, err
	}
	if !ok || info.IsDeleted() || !info.IsDirectory() || info.IsInvalid() {
		return cloudrevePendingEntry{}, false, nil
	}
	entry := cloudrevePendingEntry{
		Folder:   folder,
		Path:     relPath,
		Action:   cloudrevePendingMkdir,
		Type:     "dir",
		Sequence: info.Sequence,
	}
	return entry, true, u.savePendingEntry(entry)
}

func (u *cloudreveUploader) recordDeletePending(folder, relPath string, recursive bool, objType string) (cloudrevePendingEntry, bool, error) {
	var sequence int64
	if info, ok, err := u.model.sdb.GetDeviceFile(folder, protocol.LocalDeviceID, relPath); err != nil {
		return cloudrevePendingEntry{}, false, err
	} else if ok {
		sequence = info.Sequence
	}
	entry := cloudrevePendingEntry{
		Folder:    folder,
		Path:      relPath,
		Action:    cloudrevePendingDelete,
		Type:      objType,
		Sequence:  sequence,
		Recursive: recursive,
	}
	if err := u.savePendingEntry(entry); err != nil {
		return cloudrevePendingEntry{}, false, err
	}
	if recursive {
		entries, err := u.pendingEntries(folder)
		if err != nil {
			return cloudrevePendingEntry{}, false, err
		}
		for _, child := range entries {
			if child.Path == relPath || !matchesCloudrevePath(child.Path, relPath, true) {
				continue
			}
			if err := u.deletePendingEntry(child.Folder, child.Path); err != nil {
				return cloudrevePendingEntry{}, false, err
			}
		}
	}
	return entry, true, nil
}

func (u *cloudreveUploader) restorePending(ctx context.Context, folder string) {
	entries, err := u.pendingEntries(folder)
	if err != nil {
		slog.Warn("Cloudreve pending restore failed", slog.String("folder", folder), slogutil.Error(err))
		return
	}
	runAsync := ctx.Err() == nil
	for _, entry := range entries {
		cfg, ok := u.model.cfg.Folder(entry.Folder)
		if !ok || cfg.Type != config.FolderTypeUploadOnly || cfg.Paused {
			continue
		}
		if !u.cloudreveEnabled(cfg) {
			continue
		}
		switch entry.Action {
		case cloudrevePendingUpload:
			if entry.Type != "file" {
				continue
			}
			if u.enqueue(entry.Folder, entry.Path) && runAsync {
				u.enqueueUploadJob(ctx, uploadJob{folder: entry.Folder, path: entry.Path})
			}
		case cloudrevePendingDelete, cloudrevePendingMkdir:
			if runAsync {
				u.enqueuePendingEntry(ctx, entry)
			}
		}
	}
}

func (u *cloudreveUploader) processPendingEntry(ctx context.Context, entry cloudrevePendingEntry) {
	cfg, ok := u.model.cfg.Folder(entry.Folder)
	if !ok || cfg.Type != config.FolderTypeUploadOnly || cfg.Paused || !u.cloudreveEnabled(cfg) {
		return
	}

	var err error
	switch entry.Action {
	case cloudrevePendingDelete:
		u.obsoletePath(entry.Folder, entry.Path, entry.Recursive, cloudreveObsoleteDelete)
		err = u.syncDeletePending(ctx, cfg, entry)
	case cloudrevePendingMkdir:
		err = u.syncDirectoryPending(ctx, cfg, entry)
	default:
		return
	}
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}
	u.setFailure(entry.Folder, entry.Path, err)

	message := "Cloudreve pending sync failed"
	switch entry.Action {
	case cloudrevePendingDelete:
		message = "Cloudreve delete sync failed"
	case cloudrevePendingMkdir:
		message = "Cloudreve directory sync failed"
	}
	slog.Warn(message, cfg.LogAttr(), slog.String("path", entry.Path), slogutil.Error(err))
}

func (u *cloudreveUploader) enqueuePendingEntry(ctx context.Context, entry cloudrevePendingEntry) bool {
	return queueCloudreveWork(ctx, u.pendingQueue, entry)
}

func (u *cloudreveUploader) enqueueUploadJob(ctx context.Context, job uploadJob) bool {
	return queueCloudreveWork(ctx, u.uploadQueue, job)
}

func (u *cloudreveUploader) syncDirectoryPending(ctx context.Context, cfg config.FolderConfiguration, entry cloudrevePendingEntry) error {
	current, err := u.isCurrentPendingEntry(entry)
	if err != nil {
		return err
	}
	if !current {
		return nil
	}

	cloudCfg, err := u.cloudreveConfig(ctx, cfg)
	if err != nil {
		return err
	}
	if !cloudCfg.IsReady() {
		return nil
	}
	if err := u.ensureRemoteFolder(ctx, cfg, cloudCfg, entry.Path); err != nil {
		return err
	}
	if _, err := u.deletePendingIfCurrent(entry); err != nil {
		return err
	}
	u.clearFailure(entry.Folder, entry.Path)
	u.requestSyncActivity()
	return nil
}

func (u *cloudreveUploader) syncDeletePending(ctx context.Context, cfg config.FolderConfiguration, entry cloudrevePendingEntry) error {
	current, err := u.isCurrentPendingEntry(entry)
	if err != nil {
		return err
	}
	if !current {
		return nil
	}

	if err := u.deleteRemotePath(ctx, cfg, entry.Path); err != nil {
		return err
	}
	if entry.Type == "dir" || entry.Recursive {
		u.mut.Lock()
		u.clearEnsuredDirLocked(entry.Folder, entry.Path, true)
		u.mut.Unlock()
	}
	if _, err := u.deletePendingIfCurrent(entry); err != nil {
		return err
	}
	u.clearFailuresMatching(entry.Folder, entry.Path, entry.Recursive)
	u.requestSyncActivity()
	return nil
}

func (u *cloudreveUploader) processCurrentDelete(ctx context.Context, cfg config.FolderConfiguration, relPath string) error {
	entry, ok, err := u.pendingEntry(cfg.ID, relPath)
	if err != nil || !ok {
		return err
	}
	if entry.Action != cloudrevePendingDelete {
		return nil
	}
	return u.syncDeletePending(ctx, cfg, entry)
}

func cloudreveUploadRetryDelay(attempt int) time.Duration {
	if attempt <= 0 {
		return cloudreveUploadRetryBaseDelay
	}
	delay := cloudreveUploadRetryBaseDelay
	for i := 1; i < attempt; i++ {
		if delay >= cloudreveUploadRetryMaxDelay/2 {
			return cloudreveUploadRetryMaxDelay
		}
		delay *= 2
	}
	if delay > cloudreveUploadRetryMaxDelay {
		return cloudreveUploadRetryMaxDelay
	}
	return delay
}

func sleepCloudreveRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func bufferCloudreveEvents(ctx context.Context, sub events.Subscription, size int) <-chan events.Event {
	if size <= 0 {
		return sub.C()
	}

	out := make(chan events.Event, size)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case ev, ok := <-sub.C():
				if !ok {
					return
				}
				select {
				case out <- ev:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}

func startCloudreveWorkers(wg *sync.WaitGroup, ctx context.Context, count int, fn func()) {
	if count <= 0 {
		count = 1
	}
	for range count {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fn()
		}()
	}
}

func queueCloudreveWork[T any](ctx context.Context, ch chan T, item T) bool {
	select {
	case ch <- item:
		return true
	case <-ctx.Done():
		return false
	}
}

func (u *cloudreveUploader) isCurrentPendingEntry(entry cloudrevePendingEntry) (bool, error) {
	current, ok, err := u.pendingEntry(entry.Folder, entry.Path)
	if err != nil || !ok {
		return false, err
	}
	return current.Action == entry.Action &&
		current.Type == entry.Type &&
		current.Sequence == entry.Sequence &&
		current.Recursive == entry.Recursive, nil
}
