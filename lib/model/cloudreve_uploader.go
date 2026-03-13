// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"context"
	"log/slog"
	"mime"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/thejerf/suture/v4"

	"github.com/syncthing/syncthing/internal/cloudreve"
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

	mut     sync.Mutex
	folders map[string]*cloudreveFolderState
}

type cloudreveFolderState struct {
	workers chan struct{}
	files   map[string]*cloudreveFileState
	failed  map[string]FileError
}

type cloudreveFileState struct {
	Path       string
	BytesDone  int64
	BytesTotal int64
	Queued     bool
	Uploading  bool
	Retry      bool
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

func newCloudreveUploader(m *model) *cloudreveUploader {
	u := &cloudreveUploader{
		Supervisor: suture.New("cloudreveUploader", svcutil.SpecWithDebugLogger()),
		model:      m,
		ev:         m.evLogger,
		folders:    make(map[string]*cloudreveFolderState),
	}
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
	sub := u.ev.Subscribe(events.LocalChangeDetected)
	defer sub.Unsubscribe()

	jobs := make(chan uploadJob, 128)
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			u.worker(ctx, jobs)
		}()
	}
	defer func() {
		close(jobs)
		wg.Wait()
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		case ev, ok := <-sub.C():
			if !ok {
				return nil
			}
			payload, ok := ev.Data.(map[string]string)
			if !ok {
				continue
			}
			folderID := payload["folder"]
			cfg, ok := u.model.cfg.Folder(folderID)
			if !ok || cfg.Type != config.FolderTypeUploadOnly {
				continue
			}
			cloudCfg := u.cloudreveConfig(cfg)
			if !cloudCfg.IsReady() {
				continue
			}

			relPath := filepath.ToSlash(payload["path"])
			switch payload["type"] {
			case "dir":
				if payload["action"] == "deleted" {
					continue
				}
				if err := cloudreve.NewClient(cloudCfg.Server, cloudCfg.Token, nil).EnsureFolder(ctx, cloudreve.JoinURI(cloudCfg.BaseURI, relPath)); err != nil {
					u.setFailure(folderID, relPath, err)
					slog.Warn("Cloudreve directory sync failed", cfg.LogAttr(), slog.String("path", relPath), slogutil.Error(err))
				} else {
					u.clearFailure(folderID, relPath)
				}
			case "file":
				if payload["action"] == "deleted" {
					continue
				}
				if u.enqueue(folderID, relPath, cloudCfg.WorkerCount) {
					select {
					case jobs <- uploadJob{folder: folderID, path: relPath}:
					case <-ctx.Done():
						return nil
					}
				}
			}
		}
	}
}

func (u *cloudreveUploader) worker(ctx context.Context, jobs chan uploadJob) {
	for {
		select {
		case <-ctx.Done():
			return
		case job, ok := <-jobs:
			if !ok {
				return
			}
			u.processJob(ctx, job, jobs)
		}
	}
}

func (u *cloudreveUploader) processJob(ctx context.Context, job uploadJob, jobs chan<- uploadJob) {
	cfg, ok := u.model.cfg.Folder(job.folder)
	if !ok || cfg.Type != config.FolderTypeUploadOnly {
		u.removeFile(job.folder, job.path)
		return
	}
	cloudCfg := u.cloudreveConfig(cfg)

	state := u.folderState(job.folder, cloudCfg.WorkerCount)
	select {
	case state.workers <- struct{}{}:
	case <-ctx.Done():
		return
	}
	defer func() { <-state.workers }()

	started := u.markUploading(job.folder, job.path)
	if !started {
		return
	}

	requeue, err := u.uploadFile(ctx, cfg, job.path)
	if err != nil {
		u.finishUpload(job.folder, job.path, true, err)
		slog.Warn("Cloudreve file upload failed", cfg.LogAttr(), slog.String("path", job.path), slogutil.Error(err))
		return
	}

	u.finishUpload(job.folder, job.path, false, nil)
	if requeue {
		select {
		case jobs <- job:
		case <-ctx.Done():
		}
	}
}

func (u *cloudreveUploader) uploadFile(ctx context.Context, cfg config.FolderConfiguration, relPath string) (bool, error) {
	info, ok, err := u.model.sdb.GetDeviceFile(cfg.ID, protocol.LocalDeviceID, relPath)
	if err != nil {
		return false, err
	}
	if !ok || info.IsDeleted() || info.IsDirectory() || info.IsSymlink() || info.IsInvalid() || len(info.Blocks) == 0 {
		return false, nil
	}

	cloudCfg := u.cloudreveConfig(cfg)
	client := cloudreve.NewClient(cloudCfg.Server, cloudCfg.Token, nil)
	dirURI := cloudreve.JoinURI(cloudCfg.BaseURI, path.Dir(relPath))
	if err := client.EnsureFolder(ctx, dirURI); err != nil {
		return false, err
	}

	fd, err := cfg.Filesystem().Open(relPath)
	if err != nil {
		if fs.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	defer fd.Close()

	progress := func(done, total int64) {
		u.updateProgress(cfg.ID, relPath, done, total)
	}
	uri := cloudreve.JoinURI(cloudCfg.BaseURI, relPath)
	mimeType := mime.TypeByExtension(path.Ext(relPath))
	err = client.UploadFile(ctx, uri, info.Size, info.ModTime().UnixMilli(), mimeType, fd, progress)
	if err != nil {
		return false, err
	}
	return u.markUploadedAndCheckRetry(cfg.ID, relPath), nil
}

func (u *cloudreveUploader) cloudreveConfig(folder config.FolderConfiguration) config.CloudreveConfiguration {
	cloudCfg := u.model.cfg.Options().Cloudreve.Normalized()
	if cloudCfg.IsReady() {
		return cloudCfg
	}
	if folder.Cloudreve.HasCredentials() {
		return folder.Cloudreve.Normalized()
	}
	return cloudCfg
}

func (u *cloudreveUploader) folderState(folder string, workerCount int) *cloudreveFolderState {
	u.mut.Lock()
	defer u.mut.Unlock()
	return u.folderStateLocked(folder, workerCount)
}

func (u *cloudreveUploader) folderStateLocked(folder string, workerCount int) *cloudreveFolderState {
	state, ok := u.folders[folder]
	if ok {
		return state
	}
	if workerCount < 1 {
		workerCount = 1
	}
	state = &cloudreveFolderState{
		workers: make(chan struct{}, workerCount),
		files:   make(map[string]*cloudreveFileState),
		failed:  make(map[string]FileError),
	}
	u.folders[folder] = state
	return state
}

func (u *cloudreveUploader) enqueue(folder, relPath string, workerCount int) bool {
	var size int64
	if info, ok, err := u.model.sdb.GetDeviceFile(folder, protocol.LocalDeviceID, relPath); err == nil && ok {
		size = info.Size
	}

	u.mut.Lock()
	defer u.mut.Unlock()

	state := u.folderStateLocked(folder, workerCount)
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

func (u *cloudreveUploader) markUploading(folder, relPath string) bool {
	u.mut.Lock()
	defer u.mut.Unlock()

	state, ok := u.folders[folder]
	if !ok {
		return false
	}
	file, ok := state.files[relPath]
	if !ok || !file.Queued {
		return false
	}
	file.Queued = false
	file.Uploading = true
	file.BytesDone = 0
	u.emitProgress(folder)
	return true
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

func (u *cloudreveUploader) markUploadedAndCheckRetry(folder, relPath string) bool {
	u.mut.Lock()
	defer u.mut.Unlock()

	state, ok := u.folders[folder]
	if !ok {
		return false
	}
	file, ok := state.files[relPath]
	if !ok {
		return false
	}
	retry := file.Retry
	file.Retry = false
	if retry {
		file.Queued = true
		file.Uploading = false
		file.BytesDone = 0
	}
	u.emitProgress(folder)
	return retry
}

func (u *cloudreveUploader) finishUpload(folder, relPath string, failed bool, err error) {
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
	file.Uploading = false
	if failed {
		state.failed[relPath] = FileError{Path: relPath, Err: err.Error()}
	} else if !file.Queued {
		delete(state.files, relPath)
		delete(state.failed, relPath)
	}
	u.emitProgress(folder)
	u.emitErrorsLocked(folder)
}

func (u *cloudreveUploader) setFailure(folder, relPath string, err error) {
	u.mut.Lock()
	defer u.mut.Unlock()

	state := u.folderStateLocked(folder, 1)
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
