package mediaservice

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"syscall"
	"time"
)

var IntentID = regexp.MustCompile(`^vop_[A-Za-z0-9_-]{8,100}$`)

type Manifest struct {
	Version  int    `json:"version"`
	Socket   string `json:"socket"`
	Instance string `json:"instance"`
}
type Envelope struct {
	Instance string  `json:"instance"`
	Request  Request `json:"request"`
}
type Reply struct {
	Result *Result `json:"result,omitempty"`
	Error  string  `json:"error,omitempty"`
}
type Server struct {
	listener      net.Listener
	lock          *os.File
	dir, manifest string
	once          sync.Once
	wg            sync.WaitGroup
	cancel        context.CancelFunc
}

func DefaultManifest() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".everything-go-runtime", "media-control", "current.json")
}
func privateDir(path string) error {
	if e := os.MkdirAll(path, 0700); e != nil {
		return e
	}
	return checkPrivateDir(path)
}
func checkPrivateDir(path string) error {
	s, e := os.Lstat(path)
	if e != nil {
		return e
	}
	st, ok := s.Sys().(*syscall.Stat_t)
	if !ok || !s.IsDir() || s.Mode()&os.ModeSymlink != 0 || s.Mode().Perm() != 0700 || st.Uid != uint32(os.Getuid()) {
		return errors.New("media_private_directory_required")
	}
	return nil
}
func OpenServer(root, instance, manifest string, handler func(context.Context, Request) (Result, error)) (*Server, error) {
	if instance == "" || root == "" || !filepath.IsAbs(manifest) {
		return nil, errors.New("media_configuration_invalid")
	}
	abs, e := filepath.Abs(root)
	if e != nil {
		return nil, e
	}
	hash := sha256.Sum256([]byte(abs + "\x00" + instance))
	dir := filepath.Join(os.TempDir(), fmt.Sprintf("egm-%d-%x", os.Getuid(), hash[:6]))
	if len(filepath.Join(dir, "m.sock")) > 100 {
		return nil, errors.New("media_socket_path_too_long")
	}
	if e = privateDir(dir); e != nil {
		return nil, e
	}
	if e = privateDir(filepath.Dir(manifest)); e != nil {
		return nil, e
	}
	lock, e := os.OpenFile(filepath.Join(dir, "lock"), os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return nil, e
	}
	if e = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		lock.Close()
		return nil, errors.New("media_service_already_running")
	}
	fail := func(err error) (*Server, error) {
		syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
		lock.Close()
		return nil, err
	}
	socket := filepath.Join(dir, "m.sock")
	if st, e := os.Lstat(socket); e == nil {
		if st.Mode()&os.ModeSocket == 0 {
			return fail(errors.New("media_socket_path_conflict"))
		}
		if e = os.Remove(socket); e != nil {
			return fail(e)
		}
	} else if !os.IsNotExist(e) {
		return fail(e)
	}
	l, e := net.Listen("unix", socket)
	if e != nil {
		return fail(e)
	}
	if e = os.Chmod(socket, 0600); e != nil {
		l.Close()
		return fail(e)
	}
	if b, e := os.ReadFile(manifest); e == nil {
		var old Manifest
		if json.Unmarshal(b, &old) != nil || old.Instance != instance || old.Socket != socket {
			l.Close()
			return fail(errors.New("media_manifest_conflict"))
		}
	}
	b, _ := json.Marshal(Manifest{Version: 1, Socket: socket, Instance: instance})
	tmp, e := os.CreateTemp(filepath.Dir(manifest), ".media-*")
	if e != nil {
		l.Close()
		return fail(e)
	}
	name := tmp.Name()
	if e = tmp.Chmod(0600); e == nil {
		_, e = tmp.Write(b)
	}
	if e == nil {
		e = tmp.Sync()
	}
	tmp.Close()
	if e == nil {
		e = os.Rename(name, manifest)
	}
	os.Remove(name)
	if e != nil {
		l.Close()
		return fail(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{listener: l, lock: lock, dir: dir, manifest: manifest, cancel: cancel}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		sem := make(chan struct{}, 8)
		for {
			conn, e := l.Accept()
			if e != nil {
				return
			}
			select {
			case sem <- struct{}{}:
			default:
				conn.Close()
				continue
			}
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				defer func() { <-sem }()
				defer conn.Close()
				conn.SetDeadline(time.Now().Add(8 * time.Second))
				var q Envelope
				dec := json.NewDecoder(io.LimitReader(conn, 16385))
				dec.DisallowUnknownFields()
				reply := Reply{}
				if dec.Decode(&q) != nil || q.Instance != instance || !IntentID.MatchString(q.Request.ID) || q.Request.Thread == "" || len(q.Request.Thread) > 200 || q.Request.Operation != "" && !IntentID.MatchString(q.Request.Operation) {
					reply.Error = "media_request_invalid"
				} else {
					callctx, done := context.WithTimeout(ctx, 5*time.Second)
					r, e := handler(callctx, q.Request)
					done()
					if e != nil {
						reply.Error = e.Error()
					} else {
						reply.Result = &r
					}
				}
				json.NewEncoder(conn).Encode(reply)
			}()
		}
	}()
	return s, nil
}
func (s *Server) Close() {
	s.once.Do(func() {
		s.cancel()
		s.listener.Close()
		s.wg.Wait()
		os.Remove(filepath.Join(s.dir, "m.sock"))
		syscall.Flock(int(s.lock.Fd()), syscall.LOCK_UN)
		s.lock.Close()
	})
}
func Send(ctx context.Context, path string, q Request) (Result, error) {
	var result Result
	st, e := os.Lstat(path)
	if e != nil {
		return result, errors.New("media_service_unavailable")
	}
	owner, ok := st.Sys().(*syscall.Stat_t)
	if !ok || !st.Mode().IsRegular() || st.Mode().Perm() != 0600 || owner.Uid != uint32(os.Getuid()) {
		return result, errors.New("media_manifest_invalid")
	}
	b, e := os.ReadFile(path)
	if e != nil || len(b) > 4096 {
		return result, errors.New("media_manifest_invalid")
	}
	var m Manifest
	if json.Unmarshal(b, &m) != nil || m.Version != 1 || m.Instance == "" || !filepath.IsAbs(m.Socket) {
		return result, errors.New("media_manifest_invalid")
	}
	if e = checkPrivateDir(filepath.Dir(m.Socket)); e != nil {
		return result, e
	}
	st, e = os.Lstat(m.Socket)
	if e != nil || st.Mode()&os.ModeSocket == 0 || st.Mode().Perm() != 0600 {
		return result, errors.New("media_service_unavailable")
	}
	c, e := (&net.Dialer{}).DialContext(ctx, "unix", m.Socket)
	if e != nil {
		return result, errors.New("media_service_unavailable")
	}
	defer c.Close()
	deadline := time.Now().Add(8 * time.Second)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	c.SetDeadline(deadline)
	if e = json.NewEncoder(c).Encode(Envelope{Instance: m.Instance, Request: q}); e != nil {
		return result, errors.New("media_outcome_unknown_do_not_replay")
	}
	var reply Reply
	if json.NewDecoder(io.LimitReader(c, 32769)).Decode(&reply) != nil {
		return result, errors.New("media_outcome_unknown_do_not_replay")
	}
	if reply.Error != "" {
		return result, errors.New(reply.Error)
	}
	if reply.Result == nil {
		return result, errors.New("media_outcome_unknown_do_not_replay")
	}
	return *reply.Result, nil
}
