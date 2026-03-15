// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"mime"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/thejerf/suture/v4"

	"github.com/syncthing/syncthing/internal/cloudreve"
	"github.com/syncthing/syncthing/internal/db"
	"github.com/syncthing/syncthing/internal/slogutil"
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
}

type cloudreveFolderState struct {
	files  map[string]*cloudreveFileState
	failed map[string]FileError
}

type cloudreveFileState struct {
	Path       string
	BytesDone  int64
	BytesTotal int64
	Queued     bool
	Uploading  bool
	Retry      bool
	Obsolete   cloudreveObsoleteAction
	cancel     context.CancelFunc
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
	cloudrevePendingNamespace = "misc/cloudreve/pending"

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
	}
	u.Add(svcutil.AsService(u.oauth.Serve, "cloudreveUploader/oauth"))
	u.Add(svcutil.AsService(u.listen, "cloudreveUploader/listen"))
	return u
}

func (u *cloudreveUploader) Summary(folder string) cloudreveUploadSummary {
	u.mut.Lock()
	defer u.mut.Unlock()

	state, ok := u.folders[folder]
	if !ok {
		return cloudreveUploadSummary{}
	}

	var summary cloudreveUploadSummary
	for _, file := range state.files {
		if !file.Queued && !file.Uploading {
			continue
		}
		summary.TotalItems++
		summary.TotalBytes += file.BytesTotal
		summary.DoneBytes += file.BytesDone
		if file.Uploading {
			summary.UploadingItems++
		}
		if file.Queued {
			summary.PendingItems++
		}
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

	u.restorePending(ctx, "")

	for {
		select {
		case <-ctx.Done():
			return nil
		case ev, ok := <-sub.C():
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
							go u.processPendingEntry(ctx, entry)
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
						go u.processPendingEntry(ctx, entry)
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
							go u.processPendingEntry(ctx, entry)
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
						go u.processJobLoop(ctx, uploadJob{folder: folderID, path: relPath})
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
		}
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
	dirURI := cloudreve.JoinURI(rootURI, path.Dir(relPath))
	if err := client.EnsureFolder(ctx, dirURI); err != nil {
		return err
	}

	fd, err := cfg.Filesystem().Open(relPath)
	if err != nil {
		if fs.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer fd.Close()

	progress := func(done, total int64) {
		u.updateProgress(cfg.ID, relPath, done, total)
	}
	uri := cloudreve.JoinURI(rootURI, relPath)
	mimeType := mime.TypeByExtension(path.Ext(relPath))
	err = client.UploadFile(ctx, uri, info.Size, info.ModTime().UnixMilli(), mimeType, fd, progress)
	if err != nil {
		return err
	}
	return nil
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
		files:  make(map[string]*cloudreveFileState),
		failed: make(map[string]FileError),
	}
	u.folders[folder] = state
	return state
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
			Path:       relPath,
			BytesTotal: size,
			Queued:     true,
		}
		u.emitProgress(folder)
		return true
	}
	if file.Uploading {
		file.Retry = true
		return false
	}
	if file.Queued {
		return false
	}
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
			delete(state.failed, relPath)
		} else {
			delete(state.files, relPath)
			delete(state.failed, relPath)
		}
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
		delete(state.failed, relPath)
	} else {
		delete(state.files, relPath)
		delete(state.failed, relPath)
	}
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
			delete(state.files, path)
		}
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
	delete(state.files, relPath)
	delete(state.failed, relPath)
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
	for _, file := range state.files {
		if !file.Queued && !file.Uploading {
			continue
		}
		summary.TotalItems++
		summary.TotalBytes += file.BytesTotal
		summary.DoneBytes += file.BytesDone
		if file.Queued {
			summary.PendingItems++
		}
		if file.Uploading {
			summary.UploadingItems++
		}
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
			if u.enqueue(entry.Folder, entry.Path) {
				go u.processJobLoop(ctx, uploadJob{folder: entry.Folder, path: entry.Path})
			}
		case cloudrevePendingDelete, cloudrevePendingMkdir:
			go u.processPendingEntry(ctx, entry)
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

func (u *cloudreveUploader) syncDirectoryPending(ctx context.Context, cfg config.FolderConfiguration, entry cloudrevePendingEntry) error {
	cloudCfg, err := u.cloudreveConfig(ctx, cfg)
	if err != nil {
		return err
	}
	if !cloudCfg.IsReady() {
		return nil
	}
	client := cloudreve.NewClient(cloudCfg.Server, cloudCfg.Token, nil)
	if err := client.EnsureFolder(ctx, cloudreve.JoinURI(u.cloudreveFolderRootURI(cfg, cloudCfg), entry.Path)); err != nil {
		return err
	}
	if _, err := u.deletePendingIfCurrent(entry); err != nil {
		return err
	}
	u.clearFailure(entry.Folder, entry.Path)
	return nil
}

func (u *cloudreveUploader) syncDeletePending(ctx context.Context, cfg config.FolderConfiguration, entry cloudrevePendingEntry) error {
	if err := u.deleteRemotePath(ctx, cfg, entry.Path); err != nil {
		return err
	}
	if _, err := u.deletePendingIfCurrent(entry); err != nil {
		return err
	}
	u.clearFailuresMatching(entry.Folder, entry.Path, entry.Recursive)
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
