package linebridge

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"path/filepath"
	"strings"
	"time"
)

const MaxResourceBytes = 8 * 1024 * 1024

type ResourceInput struct {
	Bytes []byte
	Name  string
	MIME  string
}
type ResourceResolver func(context.Context, Caller, string) (ResourceInput, error)
type Resource struct {
	Ref       string `json:"resource_ref"`
	Account   string `json:"account_ref"`
	Name      string `json:"name"`
	MIME      string `json:"mime_type"`
	Size      int    `json:"size_bytes"`
	SHA256    string `json:"sha256"`
	ExpiresAt int64  `json:"expires_at"`
	Data      []byte `json:"-"`
	Owner     string `json:"-"`
}

func (s *Service) ImportResource(ctx context.Context, c Caller, account, source string) (Resource, error) {
	if _, err := s.account(ctx, c, account, "resources"); err != nil {
		return Resource{}, err
	}
	if s.config.Resources == nil {
		return Resource{}, Fail("line_resource_source_unavailable")
	}
	input, err := s.config.Resources(ctx, c, source)
	if err != nil {
		return Resource{}, err
	}
	return s.cacheResource(ctx, c, account, source, input)
}
func (s *Service) cacheResource(ctx context.Context, c Caller, account, source string, input ResourceInput) (Resource, error) {
	if len(input.Bytes) == 0 || len(input.Bytes) > MaxResourceBytes {
		return Resource{}, Fail("line_resource_size_invalid")
	}
	name := filepath.Base(input.Name)
	if name == "." || name == "" || len(name) > 240 || strings.ContainsAny(name, "\r\n\x00") {
		return Resource{}, Fail("line_resource_name_invalid")
	}
	mime := http.DetectContentType(input.Bytes)
	if input.MIME == "application/pdf" && strings.HasPrefix(string(input.Bytes), "%PDF-") {
		mime = input.MIME
	}
	sum := sha256.Sum256(input.Bytes)
	digest := hex.EncodeToString(sum[:])
	ref := "lr_" + s.store.digest(account + "\x00" + c.Subject + "\x00" + source + "\x00" + digest)[:32]
	r := Resource{Ref: ref, Account: account, Name: name, MIME: mime, Size: len(input.Bytes), SHA256: digest, ExpiresAt: nowMS() + int64((30*24*time.Hour)/time.Millisecond), Data: input.Bytes, Owner: c.Subject}
	encoded, err := s.store.seal(account, input.Bytes)
	if err != nil {
		return Resource{}, err
	}
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	_, _ = s.store.db.ExecContext(ctx, "DELETE FROM line_resources WHERE expires_at<?", nowMS())
	var size int64
	var count int
	if err = s.store.db.QueryRowContext(ctx, "SELECT COALESCE(SUM(size),0),COUNT(*) FROM line_resources WHERE account=? AND ref<>?", account, ref).Scan(&size, &count); err != nil {
		return Resource{}, err
	}
	if size+int64(r.Size) > 200*1024*1024 || count >= 100 {
		return Resource{}, Fail("line_resource_quota")
	}
	_, err = s.store.db.ExecContext(ctx, "INSERT OR IGNORE INTO line_resources VALUES(?,?,?,?,?,?,?,?,?,?)", ref, account, c.Subject, name, mime, r.Size, digest, r.ExpiresAt, encoded, nowMS())
	return r, err
}
func (s *Store) Resource(ctx context.Context, c Caller, account, ref string) (Resource, error) {
	var r Resource
	var encoded []byte
	err := s.db.QueryRowContext(ctx, "SELECT ref,account,subject,name,mime,size,sha256,expires_at,data FROM line_resources WHERE ref=? AND account=?", ref, account).Scan(&r.Ref, &r.Account, &r.Owner, &r.Name, &r.MIME, &r.Size, &r.SHA256, &r.ExpiresAt, &encoded)
	if err != nil {
		return Resource{}, Fail("line_resource_not_found")
	}
	if !s.Allowed(ctx, c, account, "resources") {
		return Resource{}, Fail("line_forbidden")
	}
	if r.ExpiresAt <= nowMS() {
		return Resource{}, Fail("line_resource_expired")
	}
	if err = s.unseal(account, encoded, &r.Data); err != nil {
		return Resource{}, err
	}
	digest := sha256.Sum256(r.Data)
	if hex.EncodeToString(digest[:]) != r.SHA256 || len(r.Data) != r.Size {
		return Resource{}, Fail("line_resource_integrity")
	}
	return r, nil
}

func (s *Service) DownloadResource(ctx context.Context, c Caller, account, target, message string) (Resource, error) {
	if _, err := s.account(ctx, c, account, "resources"); err != nil {
		return Resource{}, err
	}
	_, driver, err := s.driver(ctx, c, account, "messages.read")
	if err != nil {
		return Resource{}, err
	}
	room, err := s.store.Target(ctx, account, target)
	if err != nil {
		return Resource{}, err
	}
	if room.Kind != "user" {
		return Resource{}, Fail("line_resource_room_unverified")
	}
	external, err := s.store.ResolveMessage(ctx, account, target, message)
	if err != nil {
		return Resource{}, err
	}
	var response struct {
		Data []byte `json:"data"`
		Name string `json:"name"`
		MIME string `json:"mime"`
	}
	if err = driver.Invoke(ctx, "download_resource", map[string]any{"target": room.ExternalID, "message": external}, &response); err != nil {
		return Resource{}, err
	}
	return s.cacheResource(ctx, c, account, message, ResourceInput{Bytes: response.Data, Name: response.Name, MIME: response.MIME})
}
