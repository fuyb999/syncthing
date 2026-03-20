// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package cloudreve

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"
)

var ErrUnsupportedUploadTarget = errors.New("cloudreve returned an unsupported upload target")

const uploadProgressInterval = 500 * time.Millisecond

const cloudreveErrorObjectExisted = 40004
const cloudreveErrorDBOperationFailed = 50001
const cloudreveErrorSyncthingIPConflict = 40090
const cloudreveErrorSyncthingDeviceNotRegistered = 40091
const cloudreveClientIDHeader = "X-Cr-Client-Id"

type Client struct {
	baseURL    string
	token      string
	httpClient *http.Client
}

type StoragePolicy struct {
	Type string `json:"type"`
}

type uploadSession struct {
	SessionID      string         `json:"session_id"`
	UploadID       string         `json:"upload_id"`
	ChunkSize      int64          `json:"chunk_size"`
	Expires        int64          `json:"expires"`
	UploadURLs     []string       `json:"upload_urls"`
	Credential     string         `json:"credential"`
	AccessKey      string         `json:"ak"`
	KeyTime        string         `json:"keyTime"`
	CompleteURL    string         `json:"completeURL"`
	StoragePolicy  *StoragePolicy `json:"storage_policy"`
	URI            string         `json:"uri"`
	CallbackSecret string         `json:"callback_secret"`
	MimeType       string         `json:"mime_type"`
	UploadPolicy   string         `json:"upload_policy"`
}

type apiResponse[T any] struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
	Data T      `json:"data"`
}

type createUploadSessionRequest struct {
	URI          string `json:"uri"`
	Size         int64  `json:"size"`
	LastModified int64  `json:"last_modified,omitempty"`
	EntityType   string `json:"entity_type,omitempty"`
	Previous     string `json:"previous,omitempty"`
	MimeType     string `json:"mime_type,omitempty"`
}

type apiError struct {
	code int
	msg  string
}

func (e *apiError) Error() string {
	return fmt.Sprintf("cloudreve api error %d: %s", e.code, e.msg)
}

type createFileRequest struct {
	URI           string `json:"uri"`
	Type          string `json:"type"`
	ErrOnConflict bool   `json:"err_on_conflict"`
}

type deleteFileRequest struct {
	Uris           []string `json:"uris"`
	Unlink         bool     `json:"unlink"`
	SkipSoftDelete bool     `json:"skip_soft_delete"`
}

type DirectoryEntry struct {
	Name string `json:"name"`
	Path string `json:"path"`
}

type DirectoryListing struct {
	Parent      *DirectoryEntry  `json:"parent,omitempty"`
	Directories []DirectoryEntry `json:"directories"`
}

type CurrentUserGroup struct {
	Name string `json:"name"`
}

type CurrentUser struct {
	Email    string            `json:"email,omitempty"`
	Nickname string            `json:"nickname,omitempty"`
	Group    *CurrentUserGroup `json:"group,omitempty"`
}

type DeviceReportRequest struct {
	DeviceID      string         `json:"device_id"`
	ShortID       string         `json:"short_id"`
	APIKey        string         `json:"api_key"`
	JSONRaw       map[string]any `json:"json_raw,omitempty"`
	BindURI       string         `json:"bind_uri,omitempty"`
	ClientVersion string         `json:"client_version,omitempty"`
	Platform      string         `json:"platform,omitempty"`
}

type DeviceHeartbeatRequest struct {
	DeviceID string `json:"device_id"`
	ShortID  string `json:"short_id,omitempty"`
	BindURI  string `json:"bind_uri,omitempty"`
}

type DeviceReportResponse struct {
	RestoreConfig       map[string]any `json:"restore_config,omitempty"`
	RestoreFromDeviceID string         `json:"restore_from_device_id,omitempty"`
}

type DeviceActivityRequest struct {
	DeviceID string    `json:"device_id"`
	ShortID  string    `json:"short_id,omitempty"`
	BindURI  string    `json:"bind_uri,omitempty"`
	SyncedAt time.Time `json:"synced_at,omitempty"`
}

type qiniuChunkResponse struct {
	ETag string `json:"etag"`
}

type listDirectoryItem struct {
	Type int    `json:"type"`
	Name string `json:"name"`
	Path string `json:"path"`
}

type listDirectoryPagination struct {
	NextPageToken string `json:"next_token,omitempty"`
}

type listDirectoryResponse struct {
	Files      []listDirectoryItem      `json:"files"`
	Parent     listDirectoryItem        `json:"parent,omitempty"`
	Pagination *listDirectoryPagination `json:"pagination,omitempty"`
}

type qiniuCompleteRequest struct {
	MimeType string          `json:"mimeType,omitempty"`
	Parts    []qiniuPartInfo `json:"parts"`
}

type qiniuPartInfo struct {
	ETag       string `json:"etag"`
	PartNumber int    `json:"partNumber"`
}

type completeMultipartUpload struct {
	XMLName xml.Name              `xml:"CompleteMultipartUpload"`
	Parts   []completePartElement `xml:"Part"`
}

type completePartElement struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

type uploadProgressTracker struct {
	total          int64
	progress       func(done, total int64)
	lastReported   int64
	lastReportedAt time.Time
}

type uploadProgressReader struct {
	reader  io.Reader
	tracker *uploadProgressTracker
	base    int64
	done    int64
}

func NewClient(server, token string, httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &Client{
		baseURL:    strings.TrimRight(server, "/"),
		token:      token,
		httpClient: httpClient,
	}
}

func (c *Client) EnsureFolder(ctx context.Context, uri string) error {
	if uri == "" || uri == "cloudreve://" {
		return nil
	}
	return c.sendJSON(ctx, http.MethodPost, "/api/v4/file/create", createFileRequest{
		URI:           uri,
		Type:          "folder",
		ErrOnConflict: false,
	}, nil)
}

func (c *Client) Delete(ctx context.Context, uris ...string) error {
	filtered := make([]string, 0, len(uris))
	for _, uri := range uris {
		uri = strings.TrimSpace(uri)
		if uri == "" || uri == "cloudreve://" {
			continue
		}
		filtered = append(filtered, uri)
	}
	if len(filtered) == 0 {
		return nil
	}

	err := c.sendJSON(ctx, http.MethodDelete, "/api/v4/file", deleteFileRequest{
		Uris:           filtered,
		SkipSoftDelete: true,
	}, nil)
	if isNotFoundDeleteError(err) {
		return nil
	}
	return err
}

func (c *Client) ListDirectories(ctx context.Context, uri string) (DirectoryListing, error) {
	uri = strings.TrimSpace(uri)
	if uri == "" {
		return DirectoryListing{}, errors.New("cloudreve browse uri cannot be empty")
	}

	listing := DirectoryListing{
		Directories: []DirectoryEntry{},
	}
	if parent := cloudreveParentDirectory(uri); parent != nil {
		listing.Parent = parent
	}
	seen := make(map[string]struct{})
	nextToken := ""

	for {
		target, err := addQuery(c.baseURL+"/api/v4/file", "uri", uri)
		if err != nil {
			return DirectoryListing{}, err
		}
		target, err = addQuery(target, "page_size", "1000")
		if err != nil {
			return DirectoryListing{}, err
		}
		if nextToken == "" {
			target, err = addQuery(target, "page", "1")
		} else {
			target, err = addQuery(target, "next_page_token", nextToken)
		}
		if err != nil {
			return DirectoryListing{}, err
		}

		var resp listDirectoryResponse
		if err := c.sendAPIRequest(ctx, http.MethodGet, target, nil, &resp); err != nil {
			return DirectoryListing{}, err
		}
		for _, item := range resp.Files {
			if item.Type != 1 || strings.TrimSpace(item.Path) == "" {
				continue
			}
			if _, ok := seen[item.Path]; ok {
				continue
			}
			seen[item.Path] = struct{}{}
			listing.Directories = append(listing.Directories, DirectoryEntry{
				Name: item.Name,
				Path: item.Path,
			})
		}

		if resp.Pagination == nil || strings.TrimSpace(resp.Pagination.NextPageToken) == "" {
			break
		}
		nextToken = strings.TrimSpace(resp.Pagination.NextPageToken)
	}

	slices.SortFunc(listing.Directories, func(a, b DirectoryEntry) int {
		if diff := cmp.Compare(strings.ToLower(a.Name), strings.ToLower(b.Name)); diff != 0 {
			return diff
		}
		return cmp.Compare(a.Path, b.Path)
	})

	return listing, nil
}

func (c *Client) CurrentUser(ctx context.Context) (CurrentUser, error) {
	var user CurrentUser
	if err := c.sendAPIRequest(ctx, http.MethodGet, c.baseURL+"/api/v4/user/me", nil, &user); err != nil {
		return CurrentUser{}, err
	}
	return user, nil
}

func cloudreveParentDirectory(uri string) *DirectoryEntry {
	parentURI := cloudreveParentURI(uri)
	if parentURI == "" {
		return nil
	}
	return &DirectoryEntry{Path: parentURI}
}

func cloudreveParentURI(uri string) string {
	u, err := url.Parse(strings.TrimSpace(uri))
	if err != nil || strings.TrimSpace(u.Scheme) == "" || strings.TrimSpace(u.Host) == "" {
		return ""
	}

	currentPath := strings.TrimSuffix(u.EscapedPath(), "/")
	if currentPath == "" || currentPath == "/" {
		return ""
	}

	parentPath := path.Dir(currentPath)
	if parentPath == "." || parentPath == "/" {
		return fmt.Sprintf("%s://%s", u.Scheme, u.Host)
	}
	return fmt.Sprintf("%s://%s%s", u.Scheme, u.Host, parentPath)
}

func (u CurrentUser) GroupName() string {
	if u.Group == nil {
		return ""
	}
	return strings.TrimSpace(u.Group.Name)
}

func (u CurrentUser) CanAccessPublic() bool {
	return strings.EqualFold(u.GroupName(), "admin")
}

func (c *Client) ReportDevice(ctx context.Context, req DeviceReportRequest) (DeviceReportResponse, error) {
	var resp DeviceReportResponse
	err := c.sendJSONWithHeaders(ctx, http.MethodPut, "/api/v4/devices/syncthing/report", req, &resp, map[string]string{
		cloudreveClientIDHeader: strings.TrimSpace(req.DeviceID),
	})
	if err != nil {
		return DeviceReportResponse{}, err
	}
	return resp, nil
}

func (c *Client) HeartbeatDevice(ctx context.Context, req DeviceHeartbeatRequest) error {
	return c.sendJSONWithHeaders(ctx, http.MethodPost, "/api/v4/devices/syncthing/heartbeat", req, nil, map[string]string{
		cloudreveClientIDHeader: strings.TrimSpace(req.DeviceID),
	})
}

func (c *Client) ReportSyncActivity(ctx context.Context, req DeviceActivityRequest) error {
	return c.sendJSONWithHeaders(ctx, http.MethodPost, "/api/v4/devices/syncthing/activity", req, nil, map[string]string{
		cloudreveClientIDHeader: strings.TrimSpace(req.DeviceID),
	})
}

func (c *Client) UploadFile(ctx context.Context, uri string, size int64, lastModified int64, mimeType string, src io.Reader, progress func(done, total int64)) error {
	session, err := c.createUploadSession(ctx, uri, size, lastModified, mimeType)
	if err != nil {
		return err
	}

	chunkSize, chunkCount, err := normalizeChunkLayout(size, session.ChunkSize)
	if err != nil {
		return err
	}

	switch session.policyType() {
	case "local":
		return c.uploadLocalChunks(ctx, session, src, size, chunkSize, progress)
	case "remote":
		return c.uploadRemoteChunks(ctx, session, src, size, chunkSize, progress)
	case "s3":
		return c.uploadS3Like(ctx, session, src, size, chunkSize, chunkCount, progress, false, nil, true)
	case "cos":
		return c.uploadS3Like(ctx, session, src, size, chunkSize, chunkCount, progress, false, map[string]string{"x-cos-forbid-overwrite": "true"}, true)
	case "ks3":
		return c.uploadS3Like(ctx, session, src, size, chunkSize, chunkCount, progress, false, nil, true)
	case "oss":
		return c.uploadS3Like(ctx, session, src, size, chunkSize, chunkCount, progress, true, map[string]string{
			"x-oss-forbid-overwrite": "true",
			"x-oss-complete-all":     "yes",
		}, false)
	case "obs":
		return c.uploadOBS(ctx, session, src, size, chunkSize, chunkCount, progress)
	case "qiniu":
		return c.uploadQiniu(ctx, session, src, size, chunkSize, progress)
	case "upyun":
		return c.uploadUpyun(ctx, session, src, size, chunkSize, chunkCount, progress)
	case "onedrive":
		return c.uploadOneDrive(ctx, session, src, size, chunkSize, progress)
	default:
		return fmt.Errorf("%w: %q", ErrUnsupportedUploadTarget, session.policyType())
	}
}

func (c *Client) createUploadSession(ctx context.Context, uri string, size int64, lastModified int64, mimeType string) (uploadSession, error) {
	req := createUploadSessionRequest{
		URI:          uri,
		Size:         size,
		LastModified: lastModified,
		MimeType:     mimeType,
	}
	return c.createUploadSessionWithRequest(ctx, req)
}

func (c *Client) createUploadSessionWithRequest(ctx context.Context, req createUploadSessionRequest) (uploadSession, error) {
	var resp uploadSession
	if err := c.sendJSON(ctx, http.MethodPut, "/api/v4/file/upload", req, &resp); err != nil {
		if isAPIErrorCode(err, cloudreveErrorObjectExisted) && req.EntityType == "" {
			req.EntityType = "version"
			if err := c.sendJSON(ctx, http.MethodPut, "/api/v4/file/upload", req, &resp); err != nil {
				return uploadSession{}, err
			}
		} else {
			return uploadSession{}, err
		}
	}
	if resp.SessionID == "" {
		return uploadSession{}, errors.New("cloudreve did not return a session id")
	}
	return resp, nil
}

func isAPIErrorCode(err error, code int) bool {
	var apiErr *apiError
	return errors.As(err, &apiErr) && apiErr.code == code
}

func IsRetryableError(err error) bool {
	return isAPIErrorCode(err, cloudreveErrorDBOperationFailed)
}

func IsDeviceRegistrationConflict(err error) bool {
	return isAPIErrorCode(err, cloudreveErrorSyncthingIPConflict)
}

func IsDeviceNotRegistered(err error) bool {
	return isAPIErrorCode(err, cloudreveErrorSyncthingDeviceNotRegistered)
}

func (c *Client) uploadLocalChunks(ctx context.Context, session uploadSession, src io.Reader, total, chunkSize int64, progress func(done, total int64)) error {
	var uploaded int64
	tracker := newUploadProgressTracker(total, progress)
	return forEachUploadChunk(src, total, chunkSize, func(idx int, chunk uploadChunk) error {
		if err := c.uploadRequest(ctx, http.MethodPost, c.baseURL+"/api/v4/file/upload/"+session.SessionID+"/"+strconv.Itoa(idx), tracker.Wrap(uploaded, chunk.Reader), requestOptions{
			headers: map[string]string{
				"Authorization": "Bearer " + c.token,
				"Content-Type":  "application/octet-stream",
			},
			expectAPIResponse: true,
			contentLength:     chunk.Size,
		}); err != nil {
			return err
		}
		uploaded += chunk.Size
		tracker.Report(uploaded, true)
		return nil
	})
}

func (c *Client) uploadRemoteChunks(ctx context.Context, session uploadSession, src io.Reader, total, chunkSize int64, progress func(done, total int64)) error {
	if len(session.UploadURLs) == 0 {
		return ErrUnsupportedUploadTarget
	}
	var uploaded int64
	tracker := newUploadProgressTracker(total, progress)
	return forEachUploadChunk(src, total, chunkSize, func(idx int, chunk uploadChunk) error {
		target, err := addQuery(session.UploadURLs[0], "chunk", strconv.Itoa(idx))
		if err != nil {
			return err
		}
		if err := c.uploadRequest(ctx, http.MethodPost, target, tracker.Wrap(uploaded, chunk.Reader), requestOptions{
			headers: map[string]string{
				"Authorization": session.Credential,
				"Content-Type":  "application/octet-stream",
			},
			contentLength: chunk.Size,
		}); err != nil {
			return err
		}
		uploaded += chunk.Size
		tracker.Report(uploaded, true)
		return nil
	})
}

func (c *Client) uploadS3Like(ctx context.Context, session uploadSession, src io.Reader, total, chunkSize, chunkCount int64, progress func(done, total int64), isOSS bool, finishHeaders map[string]string, callback bool) error {
	if int64(len(session.UploadURLs)) < chunkCount || session.CompleteURL == "" {
		return ErrUnsupportedUploadTarget
	}
	parts := make([]completePartElement, 0, chunkCount)
	var uploaded int64
	tracker := newUploadProgressTracker(total, progress)
	if err := forEachUploadChunk(src, total, chunkSize, func(idx int, chunk uploadChunk) error {
		etag, err := c.uploadRawChunk(ctx, session.UploadURLs[idx], tracker.Wrap(uploaded, chunk.Reader), chunk.Size, nil)
		if err != nil {
			return err
		}
		if !isOSS {
			parts = append(parts, completePartElement{PartNumber: idx + 1, ETag: etag})
		}
		uploaded += chunk.Size
		tracker.Report(uploaded, true)
		return nil
	}); err != nil {
		return err
	}

	var body []byte
	if !isOSS {
		payload, err := xml.Marshal(completeMultipartUpload{Parts: parts})
		if err != nil {
			return err
		}
		body = payload
	}
	if _, err := c.uploadRawChunkWithResponse(ctx, session.CompleteURL, http.MethodPost, bytes.NewReader(body), requestOptions{
		headers: finishHeaders,
	}); err != nil {
		return err
	}

	if callback {
		return c.sendAPIRequest(ctx, http.MethodGet, c.baseURL+"/api/v4/callback/"+session.policyType()+"/"+session.SessionID+"/"+session.CallbackSecret, nil, nil)
	}
	return nil
}

func (c *Client) uploadOBS(ctx context.Context, session uploadSession, src io.Reader, total, chunkSize, chunkCount int64, progress func(done, total int64)) error {
	if int64(len(session.UploadURLs)) < chunkCount || session.CompleteURL == "" {
		return ErrUnsupportedUploadTarget
	}
	parts := make([]completePartElement, 0, chunkCount)
	var uploaded int64
	tracker := newUploadProgressTracker(total, progress)
	if err := forEachUploadChunk(src, total, chunkSize, func(idx int, chunk uploadChunk) error {
		etag, err := c.uploadRawChunk(ctx, session.UploadURLs[idx], tracker.Wrap(uploaded, chunk.Reader), chunk.Size, nil)
		if err != nil {
			return err
		}
		parts = append(parts, completePartElement{PartNumber: idx + 1, ETag: etag})
		uploaded += chunk.Size
		tracker.Report(uploaded, true)
		return nil
	}); err != nil {
		return err
	}
	payload, err := xml.Marshal(completeMultipartUpload{Parts: parts})
	if err != nil {
		return err
	}
	_, err = c.uploadRawChunkWithResponse(ctx, session.CompleteURL, http.MethodPost, bytes.NewReader(payload), requestOptions{})
	return err
}

func (c *Client) uploadQiniu(ctx context.Context, session uploadSession, src io.Reader, total, chunkSize int64, progress func(done, total int64)) error {
	if len(session.UploadURLs) == 0 {
		return ErrUnsupportedUploadTarget
	}
	_, chunkCount, err := normalizeChunkLayout(total, chunkSize)
	if err != nil {
		return err
	}
	parts := make([]qiniuPartInfo, 0, chunkCount)
	var uploaded int64
	tracker := newUploadProgressTracker(total, progress)
	if err := forEachUploadChunk(src, total, chunkSize, func(idx int, chunk uploadChunk) error {
		target := strings.TrimRight(session.UploadURLs[0], "/") + "/" + strconv.Itoa(idx+1)
		body, err := c.uploadRawChunkWithResponse(ctx, target, http.MethodPut, tracker.Wrap(uploaded, chunk.Reader), requestOptions{
			headers: map[string]string{
				"Authorization": "UpToken " + session.Credential,
			},
			contentLength: chunk.Size,
		})
		if err != nil {
			return err
		}
		var resp qiniuChunkResponse
		if err := json.Unmarshal(body, &resp); err != nil {
			return err
		}
		parts = append(parts, qiniuPartInfo{ETag: resp.ETag, PartNumber: idx + 1})
		uploaded += chunk.Size
		tracker.Report(uploaded, true)
		return nil
	}); err != nil {
		return err
	}

	payload, err := json.Marshal(qiniuCompleteRequest{
		MimeType: session.MimeType,
		Parts:    parts,
	})
	if err != nil {
		return err
	}
	_, err = c.uploadRawChunkWithResponse(ctx, session.UploadURLs[0], http.MethodPost, bytes.NewReader(payload), requestOptions{
		headers: map[string]string{
			"Authorization": "UpToken " + session.Credential,
			"Content-Type":  "application/json",
		},
	})
	return err
}

func (c *Client) uploadUpyun(ctx context.Context, session uploadSession, src io.Reader, total, chunkSize, chunkCount int64, progress func(done, total int64)) error {
	if len(session.UploadURLs) == 0 || chunkCount != 1 {
		return ErrUnsupportedUploadTarget
	}
	tracker := newUploadProgressTracker(total, progress)
	pr, pw := io.Pipe()
	writer := multipart.NewWriter(pw)
	go func() {
		var writeErr error
		defer func() {
			if writeErr != nil {
				_ = pw.CloseWithError(writeErr)
				return
			}
			writeErr = writer.Close()
			_ = pw.CloseWithError(writeErr)
		}()
		if writeErr = writer.WriteField("policy", session.UploadPolicy); writeErr != nil {
			return
		}
		if writeErr = writer.WriteField("authorization", session.Credential); writeErr != nil {
			return
		}
		if session.MimeType != "" {
			if writeErr = writer.WriteField("content-type", session.MimeType); writeErr != nil {
				return
			}
		}
		header := textproto.MIMEHeader{}
		header.Set("Content-Disposition", `form-data; name="file"; filename="upload"`)
		if session.MimeType != "" {
			header.Set("Content-Type", session.MimeType)
		} else {
			header.Set("Content-Type", "application/octet-stream")
		}
		part, err := writer.CreatePart(header)
		if err != nil {
			writeErr = err
			return
		}
		writeErr = forEachUploadChunk(src, total, chunkSize, func(_ int, chunk uploadChunk) error {
			_, err := io.Copy(part, tracker.Wrap(0, chunk.Reader))
			if err == nil {
				tracker.Report(chunk.Size, true)
			}
			return err
		})
	}()
	if _, err := c.uploadRawChunkWithResponse(ctx, session.UploadURLs[0], http.MethodPost, pr, requestOptions{
		headers: map[string]string{
			"Content-Type": writer.FormDataContentType(),
		},
	}); err != nil {
		return err
	}
	reportProgress(progress, total, total)
	return nil
}

func (c *Client) uploadOneDrive(ctx context.Context, session uploadSession, src io.Reader, total, chunkSize int64, progress func(done, total int64)) error {
	if len(session.UploadURLs) == 0 {
		return ErrUnsupportedUploadTarget
	}
	if total == 0 {
		return fmt.Errorf("%w: %q", ErrUnsupportedUploadTarget, "onedrive empty file")
	}
	var uploaded int64
	tracker := newUploadProgressTracker(total, progress)
	if err := forEachUploadChunk(src, total, chunkSize, func(_ int, chunk uploadChunk) error {
		start := uploaded
		end := uploaded + chunk.Size - 1
		if _, err := c.uploadRawChunkWithResponse(ctx, session.UploadURLs[0], http.MethodPut, tracker.Wrap(uploaded, chunk.Reader), requestOptions{
			headers: map[string]string{
				"Content-Range": fmt.Sprintf("bytes %d-%d/%d", start, end, total),
			},
			contentLength: chunk.Size,
		}); err != nil {
			return err
		}
		uploaded += chunk.Size
		tracker.Report(uploaded, true)
		return nil
	}); err != nil {
		return err
	}
	return c.sendAPIRequest(ctx, http.MethodPost, c.baseURL+"/api/v4/callback/onedrive/"+session.SessionID+"/"+session.CallbackSecret, nil, nil)
}

type requestOptions struct {
	headers           map[string]string
	expectAPIResponse bool
	contentLength     int64
}

func (c *Client) sendJSON(ctx context.Context, method, endpoint string, reqBody, respBody any) error {
	return c.sendJSONWithHeaders(ctx, method, endpoint, reqBody, respBody, nil)
}

func (c *Client) sendJSONWithHeaders(ctx context.Context, method, endpoint string, reqBody, respBody any, headers map[string]string) error {
	var body io.Reader
	if reqBody != nil {
		payload, err := json.Marshal(reqBody)
		if err != nil {
			return err
		}
		body = bytes.NewReader(payload)
	}
	return c.sendAPIRequestWithHeaders(ctx, method, c.baseURL+endpoint, body, respBody, headers)
}

func (c *Client) sendAPIRequest(ctx context.Context, method, target string, body io.Reader, respBody any) error {
	return c.sendAPIRequestWithHeaders(ctx, method, target, body, respBody, nil)
}

func (c *Client) sendAPIRequestWithHeaders(ctx context.Context, method, target string, body io.Reader, respBody any, headers map[string]string) error {
	req, err := http.NewRequestWithContext(ctx, method, target, body)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for key, value := range headers {
		if strings.TrimSpace(key) == "" || strings.TrimSpace(value) == "" {
			continue
		}
		req.Header.Set(key, value)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		respText, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("cloudreve request failed: %s: %s", resp.Status, strings.TrimSpace(string(respText)))
	}

	var envelope apiResponse[json.RawMessage]
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		return err
	}
	if envelope.Code != 0 {
		return &apiError{code: envelope.Code, msg: envelope.Msg}
	}
	if respBody == nil || len(envelope.Data) == 0 || string(envelope.Data) == "null" {
		return nil
	}
	return json.Unmarshal(envelope.Data, respBody)
}

func (c *Client) uploadRequest(ctx context.Context, method, target string, body io.Reader, opts requestOptions) error {
	_, err := c.uploadRawChunkWithResponse(ctx, target, method, body, opts)
	return err
}

func (c *Client) uploadRawChunk(ctx context.Context, target string, body io.Reader, contentLength int64, headers map[string]string) (string, error) {
	resp, err := c.uploadRawChunkWithResponse(ctx, target, http.MethodPut, body, requestOptions{headers: headers, contentLength: contentLength})
	if err != nil {
		return "", err
	}
	return string(resp), nil
}

func (c *Client) uploadRawChunkWithResponse(ctx context.Context, target, method string, body io.Reader, opts requestOptions) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, target, body)
	if err != nil {
		return nil, err
	}
	if opts.contentLength > 0 {
		req.ContentLength = opts.contentLength
	}
	if _, ok := opts.headers["Content-Type"]; !ok && body != nil {
		req.Header.Set("Content-Type", "application/octet-stream")
	}
	for key, value := range opts.headers {
		req.Header.Set(key, value)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("cloudreve upload failed: %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	if opts.expectAPIResponse {
		var envelope apiResponse[json.RawMessage]
		if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
			return nil, err
		}
		if envelope.Code != 0 {
			return nil, &apiError{code: envelope.Code, msg: envelope.Msg}
		}
		return envelope.Data, nil
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if etag := resp.Header.Get("ETag"); etag != "" {
		return []byte(etag), nil
	}
	return data, nil
}

type uploadChunk struct {
	Offset int64
	Size   int64
	Reader io.Reader
}

type uploadChunkReader struct {
	reader    io.Reader
	remaining int64
}

func normalizeChunkLayout(total, chunkSize int64) (int64, int64, error) {
	if total < 0 {
		return 0, 0, fmt.Errorf("invalid size %d", total)
	}
	if chunkSize <= 0 || chunkSize > total {
		chunkSize = total
	}
	if total == 0 {
		return 0, 1, nil
	}
	if chunkSize <= 0 {
		chunkSize = total
	}
	return chunkSize, (total + chunkSize - 1) / chunkSize, nil
}

func forEachUploadChunk(src io.Reader, total, chunkSize int64, fn func(idx int, chunk uploadChunk) error) error {
	chunkSize, _, err := normalizeChunkLayout(total, chunkSize)
	if err != nil {
		return err
	}
	if total == 0 {
		return fn(0, uploadChunk{Reader: bytes.NewReader(nil)})
	}

	var offset int64
	for idx := 0; offset < total; idx++ {
		size := chunkSize
		if remaining := total - offset; remaining < size {
			size = remaining
		}
		chunk := uploadChunk{
			Offset: offset,
			Size:   size,
			Reader: &uploadChunkReader{
				reader:    src,
				remaining: size,
			},
		}
		if err := fn(idx, chunk); err != nil {
			return err
		}
		offset += size
	}
	return nil
}

func (r *uploadChunkReader) Read(buf []byte) (int, error) {
	if r.remaining == 0 {
		return 0, io.EOF
	}
	if int64(len(buf)) > r.remaining {
		buf = buf[:r.remaining]
	}
	n, err := r.reader.Read(buf)
	r.remaining -= int64(n)
	if errors.Is(err, io.EOF) && r.remaining > 0 {
		return n, io.ErrUnexpectedEOF
	}
	if r.remaining == 0 && err == nil {
		return n, io.EOF
	}
	return n, err
}

func addQuery(rawURL, key, value string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}
	q := u.Query()
	q.Set(key, value)
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func reportProgress(progress func(done, total int64), done, total int64) {
	if progress != nil {
		progress(done, total)
	}
}

func newUploadProgressTracker(total int64, progress func(done, total int64)) *uploadProgressTracker {
	if progress == nil {
		return nil
	}
	return &uploadProgressTracker{
		total:    total,
		progress: progress,
	}
}

func (t *uploadProgressTracker) Wrap(base int64, body io.Reader) io.Reader {
	if t == nil || body == nil {
		return body
	}
	return &uploadProgressReader{
		reader:  body,
		tracker: t,
		base:    base,
	}
}

func (t *uploadProgressTracker) Report(done int64, force bool) {
	if t == nil || t.progress == nil {
		return
	}
	if done < 0 {
		done = 0
	}
	if done > t.total {
		done = t.total
	}
	if done == t.lastReported && !t.lastReportedAt.IsZero() {
		return
	}
	now := time.Now()
	if !force && !t.lastReportedAt.IsZero() && now.Sub(t.lastReportedAt) < uploadProgressInterval {
		return
	}
	t.lastReported = done
	t.lastReportedAt = now
	reportProgress(t.progress, done, t.total)
}

func (r *uploadProgressReader) Read(buf []byte) (int, error) {
	n, err := r.reader.Read(buf)
	if n > 0 {
		r.done += int64(n)
		r.tracker.Report(r.base+r.done, false)
	}
	if errors.Is(err, io.EOF) {
		r.tracker.Report(r.base+r.done, true)
	}
	return n, err
}

func isNotFoundDeleteError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "cloudreve api error 404:") ||
		strings.Contains(msg, "cloudreve api error 40016:") ||
		strings.Contains(msg, "cloudreve api error 40077:") ||
		strings.Contains(msg, "not found") ||
		strings.Contains(msg, "not exist")
}

func (s uploadSession) policyType() string {
	if s.StoragePolicy == nil {
		return ""
	}
	return s.StoragePolicy.Type
}

func JoinURI(baseURI, relPath string) string {
	baseURI = strings.TrimSpace(baseURI)
	if baseURI == "" {
		baseURI = "cloudreve://"
	}
	if relPath == "" || relPath == "." {
		return strings.TrimRight(baseURI, "/")
	}
	trimmedRel := path.Clean(strings.ReplaceAll(relPath, "\\", "/"))
	if trimmedRel == "." {
		return strings.TrimRight(baseURI, "/")
	}
	return strings.TrimRight(baseURI, "/") + "/" + strings.TrimPrefix(trimmedRel, "/")
}
