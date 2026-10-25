package linebridge

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sync"
	"time"
)

type ExecFactory struct{ Node, Entry, Shared, Manifest string }
type response struct {
	ID      string          `json:"id"`
	Type    string          `json:"type"`
	Account string          `json:"account_ref"`
	Epoch   string          `json:"epoch"`
	Version int             `json:"protocol_version"`
	Result  json.RawMessage `json:"result"`
	Error   *struct {
		Code   string `json:"code"`
		Before bool   `json:"before_effect"`
	} `json:"error"`
}
type execDriver struct {
	cmd            *exec.Cmd
	input          io.WriteCloser
	account, epoch string
	mu             sync.Mutex
	writer         sync.Mutex
	pending        map[string]chan response
	ready          chan struct{}
	done           chan struct{}
	closeOnce      sync.Once
	emit           func(RuntimeEvent)
}

var safeRuntimeCode = regexp.MustCompile(`^[a-z][a-z0-9_]{0,80}$`)

func (f ExecFactory) Start(ctx context.Context, a Account, epoch string, emit func(RuntimeEvent)) (Driver, error) {
	if !filepath.IsAbs(f.Node) || !filepath.IsAbs(f.Entry) || !filepath.IsAbs(f.Shared) {
		return nil, Fail("line_runtime_configuration_invalid")
	}
	if f.Manifest != "" {
		if err := VerifyBundle(f.Manifest, f.Node, f.Entry, f.Shared); err != nil {
			return nil, err
		}
	}
	cmd := exec.Command(f.Node, f.Entry)
	// Child-specific configuration never mutates the Bridge's environment.
	vars := map[string]string{"LINE_ACCOUNT_ID": a.ID, "LINE_RUNTIME_EPOCH": epoch, "LINEJS_DATA_DIR": a.DataDir, "LINEJS_KEYCHAIN_HELPER": a.KeychainHelper, "LINEJS_SHARED_DIR": f.Shared}
	cmd.Env = filteredEnv(os.Environ(), vars)
	input, err := cmd.StdinPipe()
	if err != nil {
		return nil, Fail("line_runtime_pipe_failed")
	}
	output, err := cmd.StdoutPipe()
	if err != nil {
		input.Close()
		return nil, Fail("line_runtime_pipe_failed")
	}
	cmd.Stderr = io.Discard
	d := &execDriver{cmd: cmd, input: input, account: a.ID, epoch: epoch, pending: map[string]chan response{}, ready: make(chan struct{}), done: make(chan struct{}), emit: emit}
	if err = cmd.Start(); err != nil {
		input.Close()
		return nil, Fail("line_runtime_start_failed")
	}
	go d.read(output)
	go func() {
		select {
		case <-ctx.Done():
			d.Close()
		case <-d.done:
		}
	}()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case <-d.ready:
		return d, nil
	case <-d.done:
		return nil, Fail("line_runtime_initialization_failed")
	case <-ctx.Done():
		d.Close()
		return nil, ctx.Err()
	case <-timer.C:
		d.Close()
		return nil, Fail("line_runtime_initialization_timeout")
	}
}
func filteredEnv(input []string, vars map[string]string) []string {
	out := make([]string, 0, len(input)+len(vars))
	allowed := map[string]bool{"PATH": true, "HOME": true, "TMPDIR": true, "TMP": true, "TEMP": true, "LANG": true, "LC_ALL": true, "TZ": true, "SYSTEMROOT": true}
	for _, v := range input {
		key := v
		for i, c := range v {
			if c == '=' {
				key = v[:i]
				break
			}
		}
		if _, found := vars[key]; !found && allowed[key] {
			out = append(out, v)
		}
	}
	for key, value := range vars {
		out = append(out, key+"="+value)
	}
	return out
}
func (d *execDriver) read(output io.ReadCloser) {
	scanner := bufio.NewScanner(output)
	scanner.Buffer(make([]byte, 4096), 16*1024*1024)
	for scanner.Scan() {
		var frame response
		if json.Unmarshal(scanner.Bytes(), &frame) != nil || frame.Account != d.account || frame.Epoch != d.epoch {
			_ = d.cmd.Process.Kill()
			break
		}
		if frame.Type == "ready" {
			if frame.Version != 1 {
				_ = d.cmd.Process.Kill()
				break
			}
			select {
			case <-d.ready:
			default:
				close(d.ready)
			}
			continue
		}
		d.mu.Lock()
		receiver := d.pending[frame.ID]
		delete(d.pending, frame.ID)
		d.mu.Unlock()
		if receiver != nil {
			receiver <- frame
		}
	}
	if scanner.Err() != nil {
		_ = d.cmd.Process.Kill()
	}
	_ = d.cmd.Wait()
	close(d.done)
	if d.emit != nil {
		d.emit(RuntimeEvent{Kind: "runtime_failed", Code: "line_runtime_stopped"})
	}
}
func (d *execDriver) Alive() bool {
	select {
	case <-d.done:
		return false
	default:
		return true
	}
}
func (d *execDriver) Invoke(ctx context.Context, method string, params, out any) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if !d.Alive() {
		return Fail("line_runtime_stopped")
	}
	id := newID("rpc_")
	receiver := make(chan response, 1)
	d.mu.Lock()
	if len(d.pending) >= 32 {
		d.mu.Unlock()
		return Fail("line_runtime_busy")
	}
	d.pending[id] = receiver
	d.mu.Unlock()
	defer func() { d.mu.Lock(); delete(d.pending, id); d.mu.Unlock() }()
	data, err := json.Marshal(map[string]any{"id": id, "account_ref": d.account, "epoch": d.epoch, "method": method, "params": params})
	if err != nil || len(data) > 116*1024*1024 {
		return Fail("line_request_limit")
	}
	d.writer.Lock()
	_, err = d.input.Write(append(data, '\n'))
	d.writer.Unlock()
	if err != nil {
		return &Failure{Code: "line_runtime_pipe_lost"}
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-d.done:
		return &Failure{Code: "line_runtime_stopped"}
	case r := <-receiver:
		if r.Error != nil {
			code := r.Error.Code
			if !safeRuntimeCode.MatchString(code) {
				code = "line_runtime_failed"
			}
			return &Failure{Code: code, BeforeEffect: r.Error.Before}
		}
		if out != nil && json.Unmarshal(r.Result, out) != nil {
			return &Failure{Code: "line_runtime_result_invalid"}
		}
		return nil
	}
}
func (d *execDriver) Close() error {
	d.closeOnce.Do(func() {
		_ = d.input.Close()
		select {
		case <-d.done:
		case <-time.After(4 * time.Second):
			_ = d.cmd.Process.Kill()
			<-d.done
		}
	})
	return nil
}
