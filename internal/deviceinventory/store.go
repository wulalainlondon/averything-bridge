// Package deviceinventory records authenticated client observations. It never
// treats an install, push registration or missing report as proof of an update.
package deviceinventory

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"

	"everything-go/internal/protocol"
)

const maxRecords = 256

type Binding struct{ Token, DeviceID string }
type record struct {
	ID               string               `json:"id"`
	DeviceID         string               `json:"registered_device_id"`
	Paired           bool                 `json:"paired"`
	Name             string               `json:"name,omitempty"`
	Surface          string               `json:"surface,omitempty"`
	Info             *protocol.ClientInfo `json:"client_info,omitempty"`
	FirstSeen        int64                `json:"first_seen_at,omitempty"`
	LastSeen         int64                `json:"last_seen_at,omitempty"`
	ReportedAt       int64                `json:"version_reported_at,omitempty"`
	VersionFirstSeen int64                `json:"version_first_seen_at,omitempty"`
}
type diskState struct {
	Schema  int                `json:"schema_version"`
	Salt    string             `json:"credential_hmac_salt"`
	Records map[string]*record `json:"records"`
}
type Device struct {
	ID               string               `json:"id"`
	Paired           bool                 `json:"paired"`
	Name             string               `json:"name,omitempty"`
	Surface          string               `json:"surface,omitempty"`
	Info             *protocol.ClientInfo `json:"client_info,omitempty"`
	Online           bool                 `json:"online"`
	FirstSeen        int64                `json:"first_seen_at,omitempty"`
	LastSeen         int64                `json:"last_seen_at,omitempty"`
	ReportedAt       int64                `json:"version_reported_at,omitempty"`
	VersionFirstSeen int64                `json:"version_first_seen_at,omitempty"`
	VersionStatus    string               `json:"version_status"`
}
type Snapshot struct {
	Schema       int      `json:"schema_version"`
	ObservedAt   int64    `json:"observed_at"`
	Devices      []Device `json:"devices"`
	CatalogError string   `json:"catalog_error,omitempty"`
}
type Store struct {
	mu        sync.Mutex
	dir       string
	state     diskState
	active    map[string]map[string]bool
	lastWrite time.Time
}

func Open(dir string) (*Store, error) {
	if dir == "" {
		return nil, errors.New("device inventory requires an explicit data directory")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	s := &Store{dir: dir, state: diskState{Schema: 1, Records: map[string]*record{}}, active: map[string]map[string]bool{}}
	p := filepath.Join(dir, "device_inventory.json")
	if fi, statErr := os.Lstat(p); statErr == nil && !fi.Mode().IsRegular() {
		return nil, errors.New("device inventory must be a regular file")
	} else if statErr != nil && !os.IsNotExist(statErr) {
		return nil, statErr
	}
	data, err := os.ReadFile(p)
	if err == nil {
		if len(data) > 2<<20 {
			return nil, errors.New("device inventory too large")
		}
		if err = json.Unmarshal(data, &s.state); err != nil {
			return nil, err
		}
		if s.state.Schema != 1 || s.state.Records == nil || len(s.state.Records) > maxRecords {
			return nil, errors.New("invalid device inventory")
		}
		if s.state.Salt == "" {
			return nil, errors.New("missing inventory credential salt")
		}
		for _, r := range s.state.Records {
			if r == nil || r.ID == "" || r.DeviceID == "" || (r.Info != nil && !ValidInfo(r.Info)) {
				return nil, errors.New("invalid inventory record")
			}
		}
		if err = os.Chmod(p, 0600); err != nil {
			return nil, err
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	if s.state.Salt == "" {
		b := make([]byte, 32)
		if _, err = rand.Read(b); err != nil {
			return nil, err
		}
		s.state.Salt = hex.EncodeToString(b)
		if err = s.saveLocked(); err != nil {
			return nil, err
		}
	}
	if b, err := hex.DecodeString(s.state.Salt); err != nil || len(b) != 32 {
		return nil, errors.New("invalid inventory credential salt")
	}
	return s, nil
}

func (s *Store) key(token string) string {
	mac := hmac.New(sha256.New, []byte(s.state.Salt))
	_, _ = mac.Write([]byte(token))
	return hex.EncodeToString(mac.Sum(nil))
}

// SyncBindings is fed only by the trusted pairing registry, never by a query.
func (s *Store) SyncBindings(bindings []Binding) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	live := map[string]Binding{}
	for _, b := range bindings {
		if b.Token != "" && b.DeviceID != "" {
			live[s.key(b.Token)] = b
		}
	}
	changed := false
	for key, r := range s.state.Records {
		if r == nil {
			delete(s.state.Records, key)
			changed = true
			continue
		}
		if _, ok := live[key]; !ok && r.Paired {
			r.Paired = false
			delete(s.active, key)
			changed = true
		}
	}
	for key, b := range live {
		r := s.state.Records[key]
		if r == nil {
			if len(s.state.Records) >= maxRecords {
				return errors.New("device inventory capacity reached")
			}
			id := make([]byte, 16)
			if _, err := rand.Read(id); err != nil {
				return err
			}
			r = &record{ID: hex.EncodeToString(id), DeviceID: b.DeviceID}
			s.state.Records[key] = r
			changed = true
		}
		if r.DeviceID != b.DeviceID {
			// Reusing a credential for another device must not carry over its version.
			*r = record{ID: r.ID, DeviceID: b.DeviceID}
			delete(s.active, key)
			changed = true
		}
		if !r.Paired {
			r.Paired = true
			changed = true
		}
	}
	if changed {
		return s.saveLocked()
	}
	return nil
}

// Bind refuses to treat possession of a copied token as a different device's
// version report. The connection itself remains governed by existing auth rules.
func (s *Store) Bind(token, deviceID string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := s.key(token)
	r := s.state.Records[key]
	if r == nil || !r.Paired || r.DeviceID != deviceID {
		return ""
	}
	return key
}

func safeText(value string, max int) bool {
	return strings.TrimSpace(value) != "" && len(value) <= max && !strings.ContainsFunc(value, unicode.IsControl)
}

func ValidInfo(info *protocol.ClientInfo) bool {
	if info == nil {
		return false
	}
	if info.Platform != "ios" && info.Platform != "android" && info.Platform != "macos" && info.Platform != "web" {
		return false
	}
	switch info.Channel {
	case "unknown", "development", "apple-sandbox", "app-store", "google-play", "direct":
	default:
		return false
	}
	return safeText(info.AppID, 160) && safeText(info.Version, 64) && safeText(info.Build, 64)
}

func (s *Store) Observe(key, connectionID, deviceID, name, surface string, info *protocol.ClientInfo) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.state.Records[key]
	if r == nil || !r.Paired || r.DeviceID != deviceID {
		return nil
	}
	if info != nil && (!ValidInfo(info) || (surface != "" && info.Platform != surface)) {
		return errors.New("invalid client version report")
	}
	now := time.Now()
	stamp := now.UnixMilli()
	if s.active[key] == nil {
		s.active[key] = map[string]bool{}
	}
	s.active[key][connectionID] = true
	changed := false
	if safeText(name, 128) && r.Name != name {
		r.Name = name
		changed = true
	}
	switch surface {
	case "ios", "android", "macos", "web":
		if r.Surface != surface {
			r.Surface = surface
			changed = true
		}
	}
	if r.FirstSeen == 0 {
		r.FirstSeen = stamp
		changed = true
	}
	r.LastSeen = stamp
	if info != nil {
		if r.Info == nil || *r.Info != *info {
			r.VersionFirstSeen = stamp
			changed = true
		}
		copyInfo := *info
		r.Info = &copyInfo
		r.ReportedAt = stamp
	}
	if changed || now.Sub(s.lastWrite) >= 30*time.Second {
		return s.saveLocked()
	}
	return nil
}

func (s *Store) Disconnect(key, connectionID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.active[key], connectionID)
}

func (s *Store) Snapshot() Snapshot {
	catalog, err := LoadCatalog(filepath.Join(s.dir, "device_release_targets.json"))
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UnixMilli()
	out := Snapshot{Schema: 1, ObservedAt: now, Devices: []Device{}}
	if err != nil {
		out.CatalogError = "Release target catalog is invalid or unavailable"
	}
	for key, r := range s.state.Records {
		status := catalog.Status(r.Info)
		if r.Info != nil && err != nil {
			status = "target_unavailable"
		}
		if !r.Paired {
			status = "unpaired"
		}
		var info *protocol.ClientInfo
		if r.Info != nil {
			copyInfo := *r.Info
			info = &copyInfo
		}
		out.Devices = append(out.Devices, Device{ID: r.ID, Paired: r.Paired, Name: r.Name, Surface: r.Surface, Info: info,
			Online: r.Paired && len(s.active[key]) > 0 && now-r.LastSeen < 90000, FirstSeen: r.FirstSeen, LastSeen: r.LastSeen,
			ReportedAt: r.ReportedAt, VersionFirstSeen: r.VersionFirstSeen, VersionStatus: status})
	}
	sort.Slice(out.Devices, func(i, j int) bool { return out.Devices[i].ID < out.Devices[j].ID })
	return out
}

func (s *Store) saveLocked() error {
	data, err := json.MarshalIndent(s.state, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(s.dir, ".device-inventory-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if err = f.Chmod(0600); err == nil {
		_, err = f.Write(data)
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(tmp, filepath.Join(s.dir, "device_inventory.json")); err != nil {
		return fmt.Errorf("persist device inventory: %w", err)
	}
	s.lastWrite = time.Now()
	return nil
}
