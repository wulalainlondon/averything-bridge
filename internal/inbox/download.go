package inbox

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const DownloadPrefix = "/api/inbox/v1/files/"

// Downloads outlive receipt ACKs: existing clients acknowledge an inbox item
// before the user clicks Download. Each immutable blob has its own capability,
// never a bridge credential, and expires at the original push's seven-day TTL.
type downloadRecord struct {
	Token     string  `json:"token"`
	Filename  string  `json:"filename"`
	Size      int64   `json:"size"`
	MimeType  string  `json:"mime_type"`
	ExpiresAt float64 `json:"expires_at"`
}

func (s *Store) downloadIndexPath() string { return filepath.Join(s.dir, "inbox_downloads.json") }

func (s *Store) blobPath(id string) string {
	hash := sha256.Sum256([]byte(id))
	return filepath.Join(s.dir, "inbox-downloads", hex.EncodeToString(hash[:]))
}

func (s *Store) loadDownloads() {
	data, err := os.ReadFile(s.downloadIndexPath())
	if err == nil {
		_ = json.Unmarshal(data, &s.downloads)
	}
	if s.downloads == nil {
		s.downloads = map[string]downloadRecord{}
	}
	// Only remove known expired blobs, never arbitrary files from the directory.
	changed := false
	for id, record := range s.downloads {
		if record.ExpiresAt <= nowSecs() {
			_ = os.Remove(s.blobPath(id))
			delete(s.downloads, id)
			changed = true
		}
	}
	if changed {
		_ = s.saveDownloadsLocked()
	}
}

func (s *Store) saveDownloadsLocked() error {
	data, err := json.Marshal(s.downloads)
	if err != nil {
		return err
	}
	return atomicPrivateFile(s.downloadIndexPath(), func(w io.Writer) error {
		_, err := w.Write(data)
		return err
	})
}

func atomicPrivateFile(path string, write func(io.Writer) error) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".inbox-download-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if err = write(f); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	return os.Rename(f.Name(), path)
}

// DownloadItem materializes legacy inline entries without changing inbox.json
// or its targeting/ACK state. The returned wire item contains only metadata and
// a relative download URL, resolved against each client's actual WS origin.
func (s *Store) DownloadItem(item Item) (Item, error) {
	if item.Data == "" {
		return item, nil
	}
	if item.Size < 0 || item.Size > inlineMaxBytes || item.PushedAt+ttl.Seconds() <= nowSecs() {
		return Item{}, errors.New("file push is expired or too large")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record, exists := s.downloads[item.FileID]
	if !exists || record.ExpiresAt <= nowSecs() {
		var token [32]byte
		if _, err := rand.Read(token[:]); err != nil {
			return Item{}, err
		}
		record = downloadRecord{Token: hex.EncodeToString(token[:]), Filename: item.Filename,
			Size: item.Size, MimeType: item.MimeType, ExpiresAt: item.PushedAt + ttl.Seconds()}
	}
	path := s.blobPath(item.FileID)
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() != item.Size {
		err = atomicPrivateFile(path, func(w io.Writer) error {
			n, err := io.Copy(w, io.LimitReader(base64.NewDecoder(base64.StdEncoding, strings.NewReader(item.Data)), inlineMaxBytes+1))
			if err != nil {
				return err
			}
			if n != item.Size {
				return errors.New("file push content size mismatch")
			}
			return nil
		})
		if err != nil {
			return Item{}, err
		}
	}
	if !exists || s.downloads[item.FileID] != record {
		previous, hadPrevious := s.downloads[item.FileID]
		s.downloads[item.FileID] = record
		if err := s.saveDownloadsLocked(); err != nil {
			if hadPrevious {
				s.downloads[item.FileID] = previous
			} else {
				delete(s.downloads, item.FileID)
			}
			return Item{}, err
		}
	}
	item.Data = ""
	item.URL = DownloadPrefix + url.PathEscape(item.FileID) + "?token=" + record.Token
	return item, nil
}

// DownloadHandler serves only a stored immutable push blob with the matching
// capability. ACK removal from the inbox does not invalidate this download.
func (s *Store) DownloadHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		id := strings.TrimPrefix(r.URL.Path, DownloadPrefix)
		s.mu.Lock()
		record, ok := s.downloads[id]
		s.mu.Unlock()
		token := r.URL.Query().Get("token")
		if !strings.HasPrefix(r.URL.Path, DownloadPrefix) || !ok || record.ExpiresAt <= nowSecs() ||
			token == "" || subtle.ConstantTimeCompare([]byte(token), []byte(record.Token)) != 1 {
			http.NotFound(w, r)
			return
		}
		file, err := os.Open(s.blobPath(id))
		if err != nil {
			http.NotFound(w, r)
			return
		}
		defer file.Close()
		info, err := file.Stat()
		if err != nil || !info.Mode().IsRegular() || info.Size() != record.Size {
			http.Error(w, "file unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", record.MimeType)
		w.Header().Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": record.Filename}))
		w.Header().Set("Cache-Control", "private, no-store")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		http.ServeContent(w, r, record.Filename, time.Time{}, file)
	})
}
