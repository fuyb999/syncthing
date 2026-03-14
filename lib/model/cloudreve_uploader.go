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

	mut     sync.Mutex
	folders map[string]*cloudreveFolderState
	oauth   *cloudreve.OAuthManager
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
	Obsolete   bool
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

func newCloudreveUploader(m *model) *cloudreveUploader {
	u := &cloudreveUploader{
		Supervisor: suture.New("cloudreveUploader", svcutil.SpecWithDebugLogger()),
		model:      m,
		ev:         m.evLogger,
		folders:    make(map[string]*cloudreveFolderState),
		oauth:      cloudreve.NewOAuthManager(m.cfg, db.NewMiscDB(m.sdb), nil),
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
			if !u.cloudreveEnabled(cfg) {
				continue
			}

			relPath := filepath.ToSlash(payload["path"])
			switch payload["type"] {
			case "dir":
				if payload["action"] == "deleted" {
					u.handleDelete(ctx, cfg, relPath, true)
					continue
				}
				cloudCfg, err := u.cloudreveConfig(ctx, cfg)
				if err != nil || !cloudCfg.IsReady() {
					continue
				}
				if err := cloudreve.NewClient(cloudCfg.Server, cloudCfg.Token, nil).EnsureFolder(ctx, cloudreve.JoinURI(u.cloudreveFolderRootURI(cfg, cloudCfg), relPath)); err != nil {
					u.setFailure(folderID, relPath, err)
					slog.Warn("Cloudreve directory sync failed", cfg.LogAttr(), slog.String("path", relPath), slogutil.Error(err))
				} else {
					u.clearFailure(folderID, relPath)
				}
			case "file":
				if payload["action"] == "deleted" {
					u.handleDelete(ctx, cfg, relPath, false)
					continue
				}
				cloudCfg, err := u.cloudreveConfig(ctx, cfg)
				if err != nil || !cloudCfg.IsReady() {
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
	cloudCfg, err := u.cloudreveConfig(ctx, cfg)
	if err != nil {
		u.removeFile(job.folder, job.path)
		u.setFailure(job.folder, job.path, err)
		return
	}

	state := u.folderState(job.folder, cloudCfg.WorkerCount)
	select {
	case state.workers <- struct{}{}:
	case <-ctx.Done():
		return
	}
	defer func() { <-state.workers }()

	uploadCtx, started := u.markUploading(ctx, job.folder, job.path)
	if !started {
		return
	}

	err = u.uploadFile(uploadCtx, cfg, job.path)
	requeue, deleteAfter, reportedErr := u.finishUpload(job.folder, job.path, err)
	if reportedErr != nil {
		slog.Warn("Cloudreve file upload failed", cfg.LogAttr(), slog.String("path", job.path), slogutil.Error(err))
		return
	}

	if deleteAfter {
		if err := u.deleteRemotePath(ctx, cfg, job.path); err != nil {
			u.setFailure(job.folder, job.path, err)
			slog.Warn("Cloudreve delete-after-upload failed", cfg.LogAttr(), slog.String("path", job.path), slogutil.Error(err))
		}
	}
	if requeue {
		select {
		case jobs <- job:
		case <-ctx.Done():
		}
	}
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
	file.Obsolete = false
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

func (u *cloudreveUploader) finishUpload(folder, relPath string, err error) (bool, bool, error) {
	u.mut.Lock()
	defer u.mut.Unlock()

	state, ok := u.folders[folder]
	if !ok {
		if err != nil {
			return false, false, err
		}
		return false, false, nil
	}
	file, ok := state.files[relPath]
	if !ok {
		if err != nil {
			return false, false, err
		}
		return false, false, nil
	}
	file.Uploading = false
	file.cancel = nil
	requeue := file.Retry
	file.Retry = false

	if file.Obsolete {
		deleteAfter := err == nil && !requeue
		if requeue {
			file.Queued = true
			file.Obsolete = false
			file.BytesDone = 0
			delete(state.failed, relPath)
		} else {
			delete(state.files, relPath)
			delete(state.failed, relPath)
		}
		u.emitProgress(folder)
		u.emitErrorsLocked(folder)
		return requeue, deleteAfter, nil
	}

	if err != nil {
		state.failed[relPath] = FileError{Path: relPath, Err: err.Error()}
		u.emitProgress(folder)
		u.emitErrorsLocked(folder)
		return false, false, err
	}

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
	return requeue, false, nil
}

func (u *cloudreveUploader) handleDelete(ctx context.Context, cfg config.FolderConfiguration, relPath string, recursive bool) {
	u.obsoletePath(cfg.ID, relPath, recursive)
	if err := u.deleteRemotePath(ctx, cfg, relPath); err != nil {
		u.setFailure(cfg.ID, relPath, err)
		slog.Warn("Cloudreve delete sync failed", cfg.LogAttr(), slog.String("path", relPath), slogutil.Error(err))
		return
	}
	u.clearFailuresMatching(cfg.ID, relPath, recursive)
}

func (u *cloudreveUploader) deleteRemotePath(ctx context.Context, cfg config.FolderConfiguration, relPath string) error {
	cloudCfg, err := u.cloudreveConfig(ctx, cfg)
	if err != nil {
		return err
	}
	client := cloudreve.NewClient(cloudCfg.Server, cloudCfg.Token, nil)
	return client.Delete(ctx, cloudreve.JoinURI(u.cloudreveFolderRootURI(cfg, cloudCfg), relPath))
}

func (u *cloudreveUploader) obsoletePath(folder, relPath string, recursive bool) {
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
				file.Obsolete = true
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
