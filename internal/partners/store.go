// Package partners holds host-owned presentation profiles. A profile never owns
// a native thread, a permission grant, a message stream or a task receipt.
package partners

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"image"
	_ "image/png"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/rivo/uniseg"
	"golang.org/x/text/unicode/norm"
)

type Profile struct {
	ID             string `json:"id"`
	SessionID      string `json:"session_id"`
	NativeThreadID string `json:"native_thread_id"`
	Revision       uint64 `json:"revision"`
	RoleRevision   uint64 `json:"role_revision"`
	Name           string `json:"name"`
	Label          string `json:"label"`
	Color          string `json:"color"`
	Description    string `json:"description"`
	Section        string `json:"section"`
	Order          int    `json:"order"`
	Pinned         bool   `json:"pinned"`
	Main           bool   `json:"main"`
	Hidden         bool   `json:"hidden"`
	Avatar         string `json:"avatar"`
}
type Request struct {
	Action                 string   `json:"action"`
	Authority              string   `json:"authority"`
	MutationID             string   `json:"mutation_id"`
	SessionID              string   `json:"session_id"`
	ExpectedThread         *string  `json:"expected_thread"`
	ExpectedConfigRevision *uint64  `json:"expected_config_revision"`
	ExpectedRevision       uint64   `json:"expected_revision"`
	Profile                *Profile `json:"profile"`
	AvatarPNG              []byte   `json:"avatar_png,omitempty"`
	Digest                 string   `json:"digest,omitempty"`
	Direction              int      `json:"direction,omitempty"`
	ExpectedRosterRevision uint64   `json:"expected_roster_revision,omitempty"`
}
type Receipt struct {
	Hash    string  `json:"hash"`
	Profile Profile `json:"profile"`
}
type diskState struct {
	Version  int                `json:"version"`
	Revision uint64             `json:"revision"`
	Profiles map[string]Profile `json:"profiles"`
	Receipts map[string]Receipt `json:"receipts"`
}
type Store struct {
	mu    sync.Mutex
	root  string
	state diskState
}

func Open(root string) (*Store, error) {
	if root == "" {
		return nil, errors.New("partner_storage_unavailable")
	}
	root = filepath.Join(root, "partners-v1")
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	s := &Store{root: root, state: diskState{Version: 1, Profiles: map[string]Profile{}, Receipts: map[string]Receipt{}}}
	b, err := os.ReadFile(filepath.Join(root, "profiles.json"))
	if os.IsNotExist(err) {
		return s, nil
	}
	if err != nil {
		return nil, err
	}
	if err = json.Unmarshal(b, &s.state); err != nil {
		return nil, err
	}
	if s.state.Version != 1 || s.state.Profiles == nil || s.state.Receipts == nil {
		return nil, errors.New("partner_schema_invalid")
	}
	return s, nil
}
func (s *Store) Snapshot() (uint64, []Profile) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Profile, 0, len(s.state.Profiles))
	for _, p := range s.state.Profiles {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Order != out[j].Order {
			return out[i].Order < out[j].Order
		}
		return out[i].ID < out[j].ID
	})
	return s.state.Revision, out
}
func (s *Store) ForSession(id string) (Profile, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.state.Profiles[id]
	return p, ok
}
func (s *Store) Lookup(actor, key string) (Profile, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.state.Receipts[actor+"\x00"+key]
	return r.Profile, ok
}

// PinInitialThread is only called with provider-observed initialization of the
// original logical Session. A non-empty existing binding is never replaced.
func (s *Store) PinInitialThread(sessionID, thread string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.state.Profiles[sessionID]
	if !ok || thread == "" {
		return nil
	}
	if p.NativeThreadID != "" {
		if p.NativeThreadID != thread {
			return errors.New("profile_thread_changed")
		}
		return nil
	}
	next := diskState{Version: 1, Revision: s.state.Revision + 1, Profiles: map[string]Profile{}, Receipts: map[string]Receipt{}}
	for k, v := range s.state.Profiles {
		next.Profiles[k] = v
	}
	for k, v := range s.state.Receipts {
		next.Receipts[k] = v
	}
	p.NativeThreadID = thread
	p.Revision++
	next.Profiles[sessionID] = p
	if e := s.persist(next); e != nil {
		return e
	}
	s.state = next
	return nil
}
func normalized(v string) string { return norm.NFC.String(strings.TrimSpace(v)) }
func Validate(p *Profile) error {
	p.Name = normalized(p.Name)
	p.Label = normalized(p.Label)
	p.Description = normalized(p.Description)
	p.Section = normalized(p.Section)
	if n := uniseg.GraphemeClusterCount(p.Name); n < 1 || n > 40 {
		return errors.New("invalid_name")
	}
	if uniseg.GraphemeClusterCount(p.Label) > 32 || uniseg.GraphemeClusterCount(p.Section) > 32 || uniseg.GraphemeClusterCount(p.Description) > 12000 {
		return errors.New("profile_limit")
	}
	if len(p.Name)+len(p.Label)+len(p.Description)+len(p.Section) > 64000 {
		return errors.New("profile_limit")
	}
	switch p.Color {
	case "mint", "blue", "sand", "rose", "lavender":
	default:
		return errors.New("invalid_color")
	}
	if p.Order < 0 || p.Order > 1000000 {
		return errors.New("invalid_order")
	}
	if p.Avatar != "" {
		b, e := hex.DecodeString(p.Avatar)
		if e != nil || len(b) != 32 {
			return errors.New("invalid_avatar")
		}
	}
	return nil
}

// Save is a single durable CAS. Repeating the exact mutation returns its original
// receipt even after another edit; another body under that key is rejected.
func (s *Store) Save(actor, key string, expected uint64, p Profile) (Profile, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(key) < 8 || len(key) > 128 || actor == "" || p.SessionID == "" {
		return Profile{}, errors.New("invalid_mutation")
	}
	if e := Validate(&p); e != nil {
		return Profile{}, e
	}
	body, _ := json.Marshal(struct {
		Expected uint64
		Profile  Profile
	}{expected, p})
	sum := sha256.Sum256(body)
	digest := hex.EncodeToString(sum[:])
	rk := actor + "\x00" + key
	if old, ok := s.state.Receipts[rk]; ok {
		if old.Hash != digest {
			return Profile{}, errors.New("idempotency_conflict")
		}
		return old.Profile, nil
	}
	old, exists := s.state.Profiles[p.SessionID]
	if (!exists && expected != 0) || (exists && old.Revision != expected) {
		return Profile{}, errors.New("profile_revision_conflict")
	}
	if exists && old.NativeThreadID != "" && p.NativeThreadID != old.NativeThreadID {
		return Profile{}, errors.New("profile_thread_changed")
	}
	hash := sha256.Sum256([]byte(p.SessionID))
	p.ID = "partner_" + hex.EncodeToString(hash[:16])
	p.Revision = expected + 1
	p.RoleRevision = old.RoleRevision
	if !exists || p.Description != old.Description {
		p.RoleRevision++
	}
	next := diskState{Version: 1, Revision: s.state.Revision + 1, Profiles: map[string]Profile{}, Receipts: map[string]Receipt{}}
	for k, v := range s.state.Profiles {
		if p.Main && v.Main && k != p.SessionID {
			v.Main = false
			v.Revision++
		}
		next.Profiles[k] = v
	}
	for k, v := range s.state.Receipts {
		next.Receipts[k] = v
	}
	next.Profiles[p.SessionID] = p
	next.Receipts[rk] = Receipt{Hash: digest, Profile: p}
	if e := s.persist(next); e != nil {
		return Profile{}, e
	}
	s.state = next
	return p, nil
}

// Move swaps adjacent teammates as one durable roster operation. No partial
// two-profile update and no name-based or unstable equal-rank ordering.
func (s *Store) Move(actor, key, id string, expected, roster uint64, direction int) (Profile, error) {
	return s.MoveScoped(actor, key, id, expected, roster, direction, nil)
}
func (s *Store) MoveScoped(actor, key, id string, expected, roster uint64, direction int, allowed map[string]bool) (Profile, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if actor == "" || len(key) < 8 || len(key) > 128 || (direction != -1 && direction != 1) {
		return Profile{}, errors.New("invalid_mutation")
	}
	raw, _ := json.Marshal([]any{"move", id, expected, roster, direction})
	digest := sha256.Sum256(raw)
	hash := hex.EncodeToString(digest[:])
	rk := actor + "\x00" + key
	if prior, ok := s.state.Receipts[rk]; ok {
		if prior.Hash != hash {
			return Profile{}, errors.New("idempotency_conflict")
		}
		return prior.Profile, nil
	}
	p, ok := s.state.Profiles[id]
	if !ok || p.Revision != expected {
		return Profile{}, errors.New("profile_revision_conflict")
	}
	if s.state.Revision != roster {
		return Profile{}, errors.New("roster_revision_conflict")
	}
	group := []Profile{}
	for _, v := range s.state.Profiles {
		if (allowed == nil || allowed[v.SessionID]) && v.Section == p.Section && v.Pinned == p.Pinned && v.Hidden == p.Hidden && !v.Main {
			group = append(group, v)
		}
	}
	sort.Slice(group, func(i, j int) bool {
		if group[i].Order != group[j].Order {
			return group[i].Order < group[j].Order
		}
		return group[i].ID < group[j].ID
	})
	index := -1
	for i, v := range group {
		if v.SessionID == id {
			index = i
			break
		}
	}
	if index < 0 || index+direction < 0 || index+direction >= len(group) {
		return Profile{}, errors.New("move_boundary")
	}
	group[index], group[index+direction] = group[index+direction], group[index]
	next := diskState{Version: 1, Revision: s.state.Revision + 1, Profiles: map[string]Profile{}, Receipts: map[string]Receipt{}}
	for k, v := range s.state.Profiles {
		next.Profiles[k] = v
	}
	for k, v := range s.state.Receipts {
		next.Receipts[k] = v
	}
	for i, v := range group {
		v.Order = i * 1024
		v.Revision++
		next.Profiles[v.SessionID] = v
	}
	p = next.Profiles[id]
	next.Receipts[rk] = Receipt{Hash: hash, Profile: p}
	if e := s.persist(next); e != nil {
		return Profile{}, e
	}
	s.state = next
	return p, nil
}
func (s *Store) persist(d diskState) error {
	b, e := json.Marshal(d)
	if e != nil {
		return e
	}
	f, e := os.CreateTemp(s.root, ".profile-")
	if e != nil {
		return e
	}
	name := f.Name()
	defer os.Remove(name)
	if e = f.Chmod(0600); e == nil {
		_, e = f.Write(b)
	}
	if e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e == nil {
		e = ce
	}
	if e != nil {
		return e
	}
	if e = os.Rename(name, filepath.Join(s.root, "profiles.json")); e != nil {
		return e
	}
	// A rename is already committed. Do not report a pre-execution refusal if the
	// filesystem cannot sync the directory: the exact durable receipt is readable.
	if d, e := os.Open(s.root); e == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}
func (s *Store) PutAvatar(b []byte) (string, error) {
	if len(b) == 0 || len(b) > 2*1024*1024 {
		return "", errors.New("avatar_limit")
	}
	cfg, format, e := image.DecodeConfig(bytes.NewReader(b))
	if e != nil || format != "png" || cfg.Width != 512 || cfg.Height != 512 {
		return "", errors.New("invalid_avatar")
	}
	if _, _, e = image.Decode(bytes.NewReader(b)); e != nil {
		return "", errors.New("invalid_avatar")
	}
	sum := sha256.Sum256(b)
	digest := hex.EncodeToString(sum[:])
	path := filepath.Join(s.root, digest+".png")
	if old, e := os.ReadFile(path); e == nil {
		if !bytes.Equal(old, b) {
			return "", errors.New("avatar_collision")
		}
		return digest, nil
	}
	f, e := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		return "", e
	}
	_, e = f.Write(b)
	if e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e == nil {
		e = ce
	}
	if e != nil {
		_ = os.Remove(path)
	}
	return digest, e
}
func (s *Store) Avatar(digest string) ([]byte, error) {
	b, e := hex.DecodeString(digest)
	if e != nil || len(b) != 32 {
		return nil, errors.New("invalid_avatar")
	}
	v, e := os.ReadFile(filepath.Join(s.root, digest+".png"))
	if e != nil {
		return nil, e
	}
	h := sha256.Sum256(v)
	if hex.EncodeToString(h[:]) != digest {
		return nil, errors.New("avatar_changed")
	}
	return v, nil
}

type RoleSnapshot struct {
	ProfileID   string `json:"profile_id"`
	Revision    uint64 `json:"revision"`
	Description string `json:"description"`
	Hash        string `json:"hash"`
}

func (p Profile) Role() *RoleSnapshot {
	if p.Description == "" {
		return nil
	}
	sum := sha256.Sum256([]byte(p.Description))
	return &RoleSnapshot{p.ID, p.RoleRevision, p.Description, hex.EncodeToString(sum[:])}
}
