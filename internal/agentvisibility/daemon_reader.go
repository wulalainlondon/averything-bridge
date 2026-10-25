package agentvisibility

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/coder/websocket"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

// DaemonMetadataRPC is an owner-only attachment to an existing local provider.
// It has no spawn/resume/turn/account/approval operations or reconnect loop. The
// production provider's already-connected reader remains the preferred path.
// This attachment supplies bounded metadata proof when that owner explicitly
// selects an original session. It cannot enumerate Root collaboration actors.
type DaemonMetadataRPC struct {
	mu          sync.Mutex
	conn        *websocket.Conn
	socket      string
	socketEpoch string
	scope       Scope
	serial      uint64
	closed      bool
}

func nativeSocketEpoch(path string) (string, error) {
	info, err := os.Stat(path)
	if err != nil || info.Mode()&os.ModeSocket == 0 {
		return "", ErrUnsupported
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || st.Uid != uint32(os.Geteuid()) {
		return "", ErrUnsupported
	}
	return fmt.Sprintf("unix:%d:%d:%d", st.Dev, st.Ino, info.ModTime().UnixNano()), nil
}

// OpenExistingDaemonMetadata never runs a CLI or creates a socket/credential.
// Scope identifiers must be read by the original owner from its registry.
func OpenExistingDaemonMetadata(ctx context.Context, socket string, scope Scope, configRevision uint64) (*DaemonMetadataRPC, error) {
	if scope.Authority == "" || scope.SessionID == "" || scope.RootActorID == "" || !filepath.IsAbs(socket) {
		return nil, ErrUnsupported
	}
	epoch, err := nativeSocketEpoch(socket)
	if err != nil {
		return nil, err
	}
	dialer := net.Dialer{}
	tr := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return dialer.DialContext(ctx, "unix", socket)
	}}
	c, _, err := websocket.Dial(ctx, "ws://localhost/", &websocket.DialOptions{HTTPClient: &http.Client{Transport: tr}})
	tr.CloseIdleConnections()
	if err != nil {
		return nil, ErrUnsupported
	}
	c.SetReadLimit(65536)
	scope.Epoch = fmt.Sprintf("%s:config:%d", epoch, configRevision)
	rpc := &DaemonMetadataRPC{conn: c, socket: socket, socketEpoch: epoch, scope: scope}
	_, err = rpc.call(ctx, "initialize", map[string]any{"clientInfo": map[string]string{"name": "bridge-metadata-reader", "version": "metadata-candidate"}, "capabilities": map[string]bool{"experimentalApi": true}})
	if err == nil {
		err = c.Write(ctx, websocket.MessageText, []byte(`{"jsonrpc":"2.0","method":"initialized"}`))
	}
	if err != nil {
		rpc.Close()
		return nil, ErrUnsupported
	}
	return rpc, nil
}
func (r *DaemonMetadataRPC) CurrentScope() Scope { return r.Scope() }
func (r *DaemonMetadataRPC) Scope() Scope {
	r.mu.Lock()
	defer r.mu.Unlock()
	epoch, err := nativeSocketEpoch(r.socket)
	if r.closed || err != nil || epoch != r.socketEpoch {
		return Scope{}
	}
	return r.scope
}
func (r *DaemonMetadataRPC) ChildIDs() []string { return nil }
func (r *DaemonMetadataRPC) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	_ = r.conn.CloseNow()
}
func (r *DaemonMetadataRPC) ReadThread(ctx context.Context, id string) (json.RawMessage, error) {
	if id != r.scope.RootActorID {
		return nil, ErrUnsupported
	}
	return r.call(ctx, "thread/read", map[string]any{"threadId": id, "includeTurns": false})
}
func (r *DaemonMetadataRPC) call(ctx context.Context, method string, params any) (json.RawMessage, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, ErrUnsupported
	}
	epoch, err := nativeSocketEpoch(r.socket)
	if err != nil || epoch != r.socketEpoch {
		return nil, ErrUnsupported
	}
	bounded, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	r.serial++
	id := r.serial
	msg, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params})
	if err = r.conn.Write(bounded, websocket.MessageText, msg); err != nil {
		r.closed = true
		_ = r.conn.CloseNow()
		return nil, ErrUnsupported
	}
	// Notifications are discarded. Provider requests are never approved, and
	// exceeding a fixed frame budget closes the attachment rather than looping.
	for i := 0; i < 32; i++ {
		_, raw, e := r.conn.Read(bounded)
		if e != nil {
			r.closed = true
			_ = r.conn.CloseNow()
			return nil, ErrUnsupported
		}
		var reply struct {
			ID     uint64          `json:"id"`
			Method string          `json:"method"`
			Result json.RawMessage `json:"result"`
			Error  json.RawMessage `json:"error"`
		}
		if json.Unmarshal(raw, &reply) != nil {
			return nil, ErrUnsupported
		}
		if reply.Method != "" {
			continue
		}
		if reply.ID != id {
			continue
		}
		if len(reply.Error) > 0 || len(reply.Result) == 0 {
			return nil, ErrUnsupported
		}
		return reply.Result, nil
	}
	r.closed = true
	_ = r.conn.CloseNow()
	return nil, ErrUnsupported
}

// SelectedDaemonReader makes the narrower coverage of the owner attachment
// explicit. No empty list is presented as full native inventory.
type SelectedDaemonReader struct {
	RPC *DaemonMetadataRPC
	Now func() time.Time
}

func (r SelectedDaemonReader) CurrentScope() Scope {
	if r.RPC == nil {
		return Scope{}
	}
	return r.RPC.Scope()
}
func (r SelectedDaemonReader) Probe(ctx context.Context, s Scope) (Probe, error) {
	if r.RPC == nil {
		return Probe{}, ErrUnsupported
	}
	p, e := (CodexReader{RPC: r.RPC, Now: r.Now}).Probe(ctx, s)
	p.Coverage = "partial_selected_native_root"
	return p, e
}
