package filetransfer

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func testService(t *testing.T) *Service {
	t.Helper()
	root := t.TempDir()
	s := &Service{
		dataDir: root, downloadDir: filepath.Join(root, "downloads"),
		stagingDir: filepath.Join(root, "downloads", ".bridge-drop-staging"),
		legacyDir:  filepath.Join(root, "legacy"), allowedRoots: []string{root},
		authorize: func(r *http.Request) bool { return r.Header.Get("Authorization") == "Bearer secret" },
		now:       func() time.Time { return time.UnixMilli(1_800_000_000_000) },
	}
	for _, dir := range []string{s.dataDirPath(), s.downloadDir, s.stagingDir, s.legacyDir} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

func authorizedRequest(method, path string, body []byte) *http.Request {
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer secret")
	return req
}

func decodeState(t *testing.T, response *httptest.ResponseRecorder) dropState {
	t.Helper()
	var state dropState
	if err := json.Unmarshal(response.Body.Bytes(), &state); err != nil {
		t.Fatalf("decode response %q: %v", response.Body.String(), err)
	}
	return state
}

func TestDropRequiresAuthAndRejectsUntrustedOrigin(t *testing.T) {
	s := testService(t)
	request := httptest.NewRequest(http.MethodPost, "/api/drop/v1/uploads", strings.NewReader(`{}`))
	response := httptest.NewRecorder()
	s.DropHandler().ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d", response.Code)
	}
	request = httptest.NewRequest(http.MethodOptions, "/api/drop/v1/uploads", nil)
	request.Header.Set("Origin", "https://evil.example")
	response = httptest.NewRecorder()
	s.DropHandler().ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("origin status=%d", response.Code)
	}
}

func TestDropResumesAcrossServiceRestartAndFinalizesAtomically(t *testing.T) {
	s := testService(t)
	payload := []byte("a resumable image payload")
	digest := sha256.Sum256(payload)
	metadata, _ := json.Marshal(initRequest{RequestID: "request-1", DeviceID: "note20", Name: "proof.png",
		MediaType: "image/png", SizeBytes: int64(len(payload)), SHA256: hex.EncodeToString(digest[:]), Destination: "downloads"})
	response := httptest.NewRecorder()
	s.DropHandler().ServeHTTP(response, authorizedRequest(http.MethodPost, "/api/drop/v1/uploads", metadata))
	if response.Code != http.StatusCreated {
		t.Fatalf("init status=%d body=%s", response.Code, response.Body.String())
	}
	state := decodeState(t, response)

	first := payload[:8]
	request := authorizedRequest(http.MethodPut, "/api/drop/v1/uploads/"+state.UploadID, first)
	request.Header.Set("Upload-Offset", "0")
	response = httptest.NewRecorder()
	s.DropHandler().ServeHTTP(response, request)
	if response.Code != http.StatusAccepted || decodeState(t, response).ReceivedBytes != int64(len(first)) {
		t.Fatalf("first chunk status=%d body=%s", response.Code, response.Body.String())
	}

	// Recreate the service as a Bridge restart would. The durable state returns
	// the exact acknowledged cursor and accepts only the next offset.
	restarted := Service{
		dataDir: s.dataDir, downloadDir: s.downloadDir, stagingDir: s.stagingDir,
		legacyDir: s.legacyDir, allowedRoots: append([]string(nil), s.allowedRoots...),
		authorize: s.authorize, now: s.now,
	}
	response = httptest.NewRecorder()
	restarted.DropHandler().ServeHTTP(response, authorizedRequest(http.MethodPost, "/api/drop/v1/uploads", metadata))
	if response.Code != http.StatusOK || decodeState(t, response).ReceivedBytes != int64(len(first)) {
		t.Fatalf("resume status=%d body=%s", response.Code, response.Body.String())
	}
	request = authorizedRequest(http.MethodPut, "/api/drop/v1/uploads/"+state.UploadID, payload[len(first):])
	request.Header.Set("Upload-Offset", "0")
	response = httptest.NewRecorder()
	restarted.DropHandler().ServeHTTP(response, request)
	if response.Code != http.StatusConflict || response.Header().Get("Upload-Offset") != "8" {
		t.Fatalf("conflict status=%d offset=%q", response.Code, response.Header().Get("Upload-Offset"))
	}
	request = authorizedRequest(http.MethodPut, "/api/drop/v1/uploads/"+state.UploadID, payload[len(first):])
	request.Header.Set("Upload-Offset", "8")
	response = httptest.NewRecorder()
	restarted.DropHandler().ServeHTTP(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("second chunk status=%d body=%s", response.Code, response.Body.String())
	}
	response = httptest.NewRecorder()
	restarted.DropHandler().ServeHTTP(response, authorizedRequest(http.MethodPost, "/api/drop/v1/uploads/"+state.UploadID+"/complete", nil))
	if response.Code != http.StatusCreated {
		t.Fatalf("complete status=%d body=%s", response.Code, response.Body.String())
	}
	complete := decodeState(t, response)
	if !complete.Complete || complete.SHA256 != hex.EncodeToString(digest[:]) || filepath.Dir(complete.FinalPath) != restarted.downloadDir {
		t.Fatalf("complete=%+v", complete)
	}
	if got, err := os.ReadFile(complete.FinalPath); err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("final payload=%q err=%v", got, err)
	}
	if _, err := os.Stat(filepath.Join(restarted.stagingDir, state.UploadID+".part")); !os.IsNotExist(err) {
		t.Fatalf("partial file remains: %v", err)
	}
	// Completion is idempotent.
	response = httptest.NewRecorder()
	restarted.DropHandler().ServeHTTP(response, authorizedRequest(http.MethodPost, "/api/drop/v1/uploads/"+state.UploadID+"/complete", nil))
	if response.Code != http.StatusOK || decodeState(t, response).FinalPath != complete.FinalPath {
		t.Fatalf("repeat complete status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestDropSHAFailureDoesNotPromotePartialFile(t *testing.T) {
	s := testService(t)
	payload := []byte("bad checksum")
	metadata, _ := json.Marshal(initRequest{RequestID: "request-bad", DeviceID: "phone", Name: "bad.jpg",
		SizeBytes: int64(len(payload)), SHA256: strings.Repeat("0", 64)})
	response := httptest.NewRecorder()
	s.DropHandler().ServeHTTP(response, authorizedRequest(http.MethodPost, "/api/drop/v1/uploads", metadata))
	state := decodeState(t, response)
	request := authorizedRequest(http.MethodPut, "/api/drop/v1/uploads/"+state.UploadID, payload)
	request.Header.Set("Upload-Offset", "0")
	response = httptest.NewRecorder()
	s.DropHandler().ServeHTTP(response, request)
	response = httptest.NewRecorder()
	s.DropHandler().ServeHTTP(response, authorizedRequest(http.MethodPost, "/api/drop/v1/uploads/"+state.UploadID+"/complete", nil))
	if response.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if _, err := os.Stat(state.PartPath); err != nil {
		t.Fatalf("partial should remain retryable: %v", err)
	}
	if entries, _ := os.ReadDir(s.downloadDir); len(entries) != 1 || entries[0].Name() != ".bridge-drop-staging" {
		t.Fatalf("unexpected promoted files: %+v", entries)
	}
}

func TestLegacyUploadIsAuthenticatedAndAtomic(t *testing.T) {
	s := testService(t)
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, _ := writer.CreateFormFile("file", "photo.png")
	_, _ = part.Write([]byte("image"))
	_ = writer.Close()
	request := httptest.NewRequest(http.MethodPost, "/upload", bytes.NewReader(body.Bytes()))
	request.Header.Set("Content-Type", writer.FormDataContentType())
	response := httptest.NewRecorder()
	s.UploadHandler().ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("anonymous status=%d", response.Code)
	}
	request = httptest.NewRequest(http.MethodPost, "/upload", bytes.NewReader(body.Bytes()))
	request.Header.Set("Content-Type", writer.FormDataContentType())
	request.Header.Set("Authorization", "Bearer secret")
	response = httptest.NewRecorder()
	s.UploadHandler().ServeHTTP(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	entries, _ := os.ReadDir(s.legacyDir)
	if len(entries) != 1 || strings.HasSuffix(entries[0].Name(), ".part") {
		t.Fatalf("legacy files=%v", entries)
	}
}

func TestDownloadRequiresAuthAndStaysWithinRoots(t *testing.T) {
	s := testService(t)
	inside := filepath.Join(s.downloadDir, "inside.txt")
	if err := os.WriteFile(inside, []byte("ok"), 0o600); err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	s.DownloadHandler().ServeHTTP(response, authorizedRequest(http.MethodGet, "/files?path="+url.QueryEscape(inside), nil))
	if response.Code != http.StatusOK || response.Body.String() != "ok" {
		t.Fatalf("inside status=%d body=%q", response.Code, response.Body.String())
	}
	response = httptest.NewRecorder()
	s.DownloadHandler().ServeHTTP(response, authorizedRequest(http.MethodGet, "/files?path="+url.QueryEscape("/etc/passwd"), nil))
	if response.Code != http.StatusForbidden {
		t.Fatalf("outside status=%d", response.Code)
	}
}
