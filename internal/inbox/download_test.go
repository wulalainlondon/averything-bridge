package inbox

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func TestDownloadSurvivesAckAndReloadAndSupportsRange(t *testing.T) {
	dir := t.TempDir()
	s := New(dir)
	body := "immutable APK content"
	item, err := s.Push(writeTemp(t, dir, "test.apk", []byte(body)), "sender", []string{"phone"})
	if err != nil {
		t.Fatal(err)
	}
	wire, err := s.DownloadItem(item)
	if err != nil {
		t.Fatal(err)
	}
	if wire.Data != "" || !strings.HasPrefix(wire.URL, DownloadPrefix) {
		t.Fatal("expected metadata and capability")
	}
	again, err := s.DownloadItem(item)
	if err != nil || again.URL != wire.URL {
		t.Fatal("download URL is not stable")
	}
	if !s.Ack(item.FileID, "phone") {
		t.Fatal("receipt did not remove inbox entry")
	}
	s = New(dir)
	if len(s.Pending("phone")) != 0 {
		t.Fatal("reload restored acknowledged file")
	}
	for _, tc := range []struct {
		method, rangeHeader, expected string
		status                        int
	}{
		{http.MethodGet, "", body, http.StatusOK},
		{http.MethodHead, "", "", http.StatusOK},
		{http.MethodGet, "bytes=0-8", "immutable", http.StatusPartialContent},
	} {
		r := httptest.NewRequest(tc.method, wire.URL, nil)
		r.Header.Set("Range", tc.rangeHeader)
		w := httptest.NewRecorder()
		s.DownloadHandler().ServeHTTP(w, r)
		if w.Code != tc.status || w.Body.String() != tc.expected {
			t.Fatalf("download status=%d body=%q", w.Code, w.Body.String())
		}
	}
	for _, path := range []string{DownloadPrefix + item.FileID, DownloadPrefix + item.FileID + "?token=wrong", DownloadPrefix + "../inbox.json?token=wrong"} {
		w := httptest.NewRecorder()
		s.DownloadHandler().ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != http.StatusNotFound {
			t.Fatalf("unauthorized download: %d", w.Code)
		}
	}
}

func TestDownloadExpirationRevokesAndCollectsBlob(t *testing.T) {
	dir := t.TempDir()
	s := New(dir)
	item, _ := s.Push(writeTemp(t, dir, "file.txt", []byte("secret")), "sender", nil)
	wire, err := s.DownloadItem(item)
	if err != nil {
		t.Fatal(err)
	}
	record := s.downloads[item.FileID]
	record.ExpiresAt = nowSecs() - 1
	s.downloads[item.FileID] = record
	if err := s.saveDownloadsLocked(); err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	s.DownloadHandler().ServeHTTP(w, httptest.NewRequest("GET", wire.URL, nil))
	if w.Code != http.StatusNotFound {
		t.Fatal("expired capability still works")
	}
	s = New(dir)
	if _, err := os.Stat(s.blobPath(item.FileID)); !os.IsNotExist(err) {
		t.Fatal("expired blob not collected")
	}
}

func TestDownloadLegacyContentValidationDoesNotLoseInbox(t *testing.T) {
	dir := t.TempDir()
	s := New(dir)
	item, _ := s.Push(writeTemp(t, dir, "file.txt", []byte("content")), "sender", []string{"phone"})
	item.Data = "not base64!"
	if _, err := s.DownloadItem(item); err == nil {
		t.Fatal("corrupt content accepted")
	}
	if len(s.Pending("phone")) != 1 {
		t.Fatal("failed conversion lost original item")
	}
	item = s.Pending("phone")[0]
	item.Size++
	if _, err := s.DownloadItem(item); err == nil {
		t.Fatal("mismatched content size accepted")
	}
	if len(s.downloads) != 0 {
		t.Fatal("failed conversion published capability")
	}
}

func TestDownloadHTTPBytesMatchOriginal(t *testing.T) {
	dir := t.TempDir()
	s := New(dir)
	content := strings.Repeat("binary\x00\xff", 20000)
	item, _ := s.Push(writeTemp(t, dir, "file.bin", []byte(content)), "sender", nil)
	wire, err := s.DownloadItem(item)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(s.DownloadHandler())
	defer server.Close()
	resp, err := http.Get(server.URL + wire.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	got, err := io.ReadAll(resp.Body)
	if err != nil || string(got) != content {
		t.Fatal("download changed original bytes")
	}
	var records map[string]downloadRecord
	data, err := os.ReadFile(s.downloadIndexPath())
	if err != nil {
		t.Fatal(err)
	}
	if json.Unmarshal(data, &records) != nil || records[item.FileID].Token == "" {
		t.Fatal("capability not persisted")
	}
}
