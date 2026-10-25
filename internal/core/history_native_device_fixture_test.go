package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/executor/goexec"
	"everything-go/internal/governance"
	"everything-go/internal/history"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
)

type historyDeviceProvider struct {
	inner *goexec.Codex
	reads atomic.Int64
}

func (p *historyDeviceProvider) LoadHistory(id string, o history.Opts) (*history.Result, error) {
	p.reads.Add(1)
	return p.inner.LoadHistory(id, o)
}
func (p *historyDeviceProvider) ResumableSessions(n int) ([]history.ResumableSession, error) {
	return p.inner.ResumableSessions(n)
}
func (p *historyDeviceProvider) HistorySourceGeneration(id string) (string, error) {
	return p.inner.HistorySourceGeneration(id)
}

type historyDeviceExec struct {
	fakeExec
	provider *historyDeviceProvider
	messages atomic.Int64
}

func (e *historyDeviceExec) ProviderFor(*session.Session) (backend.HistoryProvider, bool) {
	return e.provider, true
}
func (e *historyDeviceExec) AllProviders() []backend.HistoryProvider {
	return []backend.HistoryProvider{e.provider}
}

func TestHistoryNativeDeviceFixture(t *testing.T) {
	if os.Getenv("HISTORY_NATIVE_DEVICE_FIXTURE") != "1" {
		t.Skip("explicit isolated native wire fixture only")
	}
	root := os.Getenv("HISTORY_NATIVE_DEVICE_ROOT")
	report := os.Getenv("HISTORY_NATIVE_DEVICE_REPORT")
	if root == "" || report == "" {
		t.Fatal("missing exact fixture outputs")
	}
	os.MkdirAll(root, 0700)
	nativeDir := filepath.Join(root, "native", "sessions")
	os.MkdirAll(nativeDir, 0700)
	t.Setenv("EVERYTHING_GO_CODEX_SESSIONS_DIR", nativeDir)
	t.Setenv("EVERYTHING_GO_HISTORY_LOAD_MAX_BYTES", "65536")
	const thread = "00000000-0000-0000-0000-000000000891"
	path := filepath.Join(nativeDir, "rollout-2026-10-17T00-00-00-"+thread+".jsonl")
	reuse := os.Getenv("HISTORY_NATIVE_DEVICE_REUSE") == "1"
	if !reuse {
		source, e := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if e != nil {
			t.Fatal(e)
		}
		for i := 1; i <= 1000; i++ {
			fmt.Fprintf(source, "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":%q}}\n", fmt.Sprintf("隔離歷史第%04d則：保留原始順序與來源，不是模型請求。這段內容用來驗證跨尾端預算的分頁與閱讀位置。", i))
		}
		source.WriteString("{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"assistant\",\"phase\":\"final_answer\",\"content\":\"隔離舊答案，永久只有一份\"}}\n")
		source.Close()
	} else if info, err := os.Stat(path); err != nil || info.Size() == 0 {
		t.Fatal("exact fixture source unavailable for restart")
	}
	dataDir := filepath.Join(root, "data")
	os.MkdirAll(dataDir, 0700)
	reg := session.NewRegistry()
	pairing := governance.NewPairing(filepath.Join(dataDir, "pairing.json"))
	if e := pairing.Claim("history-fixture-only", "native-ux-fixture-device"); e != nil {
		t.Fatal(e)
	}
	h := NewHub(reg, Config{InstanceID: "history-native-qa", InstanceName: "隔離歷史驗收", DataDir: dataDir, RootDir: root}, pairing, 0)
	sessionObject := reg.Create("history", "歷史整合驗收", root, "codex", "", "read-only", thread)
	h.runtimes.Ensure("history", "idle", 0)
	otherThread := "00000000-0000-0000-0000-000000000892"
	otherPath := filepath.Join(nativeDir, "rollout-2026-10-17T00-00-00-"+otherThread+".jsonl")
	if !reuse {
		os.WriteFile(otherPath, []byte("{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":\"隔離返回位置對話\"}}\n"), 0600)
	}
	reg.Create("other", "返回位置測試", root, "codex", "", "read-only", otherThread)
	h.runtimes.Ensure("other", "idle", 0)
	provider := goexec.NewCodex(h, "disabled-no-model-fixture")
	provider.SetDataDir(filepath.Join(root, "provider-data"))
	hp := &historyDeviceProvider{inner: provider}
	exec := &historyDeviceExec{fakeExec: fakeExec{sink: h}, provider: hp}
	exec.onSend = func(*session.Session, string, string) { exec.messages.Add(1) }
	h.SetExecutor(exec)
	address := "127.0.0.1:0"
	if reuse {
		address = os.Getenv("HISTORY_NATIVE_DEVICE_ADDRESS")
		if address != "127.0.0.1:65015" {
			t.Fatal("restart only the original fixture listener")
		}
	}
	listener, e := net.Listen("tcp", address)
	if e != nil {
		t.Fatal(e)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	stop := make(chan struct{})
	var stopOnce sync.Once
	var grown atomic.Bool
	grown.Store(reuse)
	state := func() map[string]any {
		return map[string]any{"fixture": true, "real_native_parser": true, "paid_model_messages": 0, "unexpected_message_commands": exec.messages.Load(), "history_reads": hp.reads.Load(), "grown": grown.Load(), "port": port, "instance": "history-native-qa"}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", h.ServeWS)
	mux.HandleFunc("/fixture/state", func(w http.ResponseWriter, r *http.Request) { json.NewEncoder(w).Encode(state()) })
	mux.HandleFunc("/fixture/stop", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "POST required", 405)
			return
		}
		stopOnce.Do(func() { close(stop) })
		json.NewEncoder(w).Encode(state())
	})

	var opened, finished atomic.Bool
	mux.HandleFunc("/fixture/open-turn", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "POST required", 405)
			return
		}
		if !opened.CompareAndSwap(false, true) {
			json.NewEncoder(w).Encode(state())
			return
		}
		f, e := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
		if e != nil {
			http.Error(w, "source unavailable", 500)
			return
		}
		f.WriteString("{\"type\":\"turn_context\",\"payload\":{\"turn_id\":\"qa-growth-turn\"}}\n" +
			"{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":\"隔離分段串流驗收，沒有真模型請求\"}}\n" +
			"{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"assistant\",\"phase\":\"commentary\",\"content\":\"隔離尚未完成進度\"}}\n" +
			"{\"type\":\"response_item\",\"payload\":{\"type\":\"function_call\",\"call_id\":\"split-first\",\"name\":\"exec_command\",\"arguments\":\"{}\"}}\n" +
			"{\"type\":\"response_item\",\"payload\":{\"type\":\"function_call\",\"call_id\":\"split-second\",\"name\":\"exec_command\",\"arguments\":\"{}\"}}\n")
		f.Sync()
		f.Close()
		sum := sha256.Sum256([]byte(thread))
		journal := filepath.Join(root, "provider-data", "codex-turn-requests")
		os.MkdirAll(journal, 0700)
		data, _ := json.Marshal(map[string]any{"version": 1, "thread_id": thread, "requests": map[string]string{"qa-growth-turn": "r_fixture_split"}})
		os.WriteFile(filepath.Join(journal, hex.EncodeToString(sum[:])+".json"), data, 0600)
		v, _ := h.runtimes.Update("history", "running", "r_fixture_split", 0, "", "")
		h.broadcastRuntime(v)
		h.Emit(protocol.NewTextChunk(sessionObject.ID, "r_fixture_split", "隔離尚未完成進度"))
		h.Emit(protocol.NewToolStart(sessionObject.ID, "r_fixture_split", "split-first", "exec_command", "first fixture"))
		h.Emit(protocol.NewToolStart(sessionObject.ID, "r_fixture_split", "split-second", "exec_command", "second fixture"))
		json.NewEncoder(w).Encode(state())
	})
	mux.HandleFunc("/fixture/finish-turn", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "POST required", 405)
			return
		}
		if !opened.Load() {
			http.Error(w, "original open required", 409)
			return
		}
		if !finished.CompareAndSwap(false, true) {
			json.NewEncoder(w).Encode(state())
			return
		}
		f, e := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
		if e != nil {
			http.Error(w, "source unavailable", 500)
			return
		}
		f.WriteString("{\"type\":\"response_item\",\"payload\":{\"type\":\"function_call_output\",\"call_id\":\"split-first\",\"output\":\"第一個原工具的完整結果\"}}\n" +
			"{\"type\":\"response_item\",\"payload\":{\"type\":\"function_call_output\",\"call_id\":\"split-second\",\"output\":\"第二個原工具的完整結果\"}}\n" +
			"{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"assistant\",\"phase\":\"final_answer\",\"content\":\"隔離完成答案，只有一份\"}}\n" +
			"{\"type\":\"event_msg\",\"payload\":{\"type\":\"task_complete\",\"turn_id\":\"qa-growth-turn\"}}\n")
		f.Sync()
		f.Close()
		h.Emit(protocol.NewToolResult(sessionObject.ID, "r_fixture_split", "split-first", "第一個原工具的完整結果"))
		h.Emit(protocol.NewToolResult(sessionObject.ID, "r_fixture_split", "split-second", "第二個原工具的完整結果"))
		h.Emit(protocol.NewDone(sessionObject.ID, "r_fixture_split"))
		v, _ := h.runtimes.Update("history", "completed", "r_fixture_split", 0, "completed", "")
		h.broadcastRuntime(v)
		json.NewEncoder(w).Encode(state())
	})
	mux.HandleFunc("/fixture/grow", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "POST required", 405)
			return
		}
		if !grown.CompareAndSwap(false, true) {
			json.NewEncoder(w).Encode(state())
			return
		}
		file, e := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
		if e != nil {
			http.Error(w, "fixture append refused", 500)
			return
		}
		for i := 1001; i <= 1600; i++ {
			fmt.Fprintf(file, "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":%q}}\n", fmt.Sprintf("隔離歷史第%04d則：背景補齊仍保留較早閱讀位置。", i))
		}
		file.WriteString("{\"type\":\"turn_context\",\"payload\":{\"turn_id\":\"qa-growth-turn\"}}\n")
		file.WriteString("{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":\"隔離串流完成驗收，沒有真模型請求\"}}\n")
		file.WriteString("{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"assistant\",\"phase\":\"commentary\",\"content\":\"隔離進度只保留一次\"}}\n")
		file.WriteString("{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"assistant\",\"phase\":\"final_answer\",\"content\":\"隔離完成答案，只有一份\"}}\n")
		file.WriteString("{\"type\":\"event_msg\",\"payload\":{\"type\":\"task_complete\",\"turn_id\":\"qa-growth-turn\"}}\n")
		file.Sync()
		file.Close()
		sum := sha256.Sum256([]byte(thread))
		journal := filepath.Join(root, "provider-data", "codex-turn-requests")
		os.MkdirAll(journal, 0700)
		data, _ := json.Marshal(map[string]any{"version": 1, "thread_id": thread, "requests": map[string]string{"qa-growth-turn": "r_fixture_growth"}})
		os.WriteFile(filepath.Join(journal, hex.EncodeToString(sum[:])+".json"), data, 0600)
		h.runtimes.Update("history", "running", "r_fixture_growth", 0, "", "")
		h.Emit(protocol.NewTextChunk(sessionObject.ID, "r_fixture_growth", "隔離進度只保留一次"))
		h.Emit(protocol.NewDone(sessionObject.ID, "r_fixture_growth"))
		view, _ := h.runtimes.Update("history", "completed", "r_fixture_growth", 0, "completed", "")
		h.broadcastRuntime(view)
		json.NewEncoder(w).Encode(state())
	})
	server := &http.Server{Handler: mux}
	go server.Serve(listener)
	output := state()
	output["state"] = "READY"
	output["ws_url"] = fmt.Sprintf("ws://127.0.0.1:%d/ws", port)
	bytes, _ := json.MarshalIndent(output, "", "  ")
	os.WriteFile(report, bytes, 0600)
	select {
	case <-stop:
	case <-time.After(15 * time.Minute):
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	server.Shutdown(ctx)
	h.mu.RLock()
	clients := make([]*Client, 0, len(h.clients))
	for c := range h.clients {
		clients = append(clients, c)
	}
	h.mu.RUnlock()
	for _, c := range clients {
		c.conn.Close("history fixture complete")
		c.shutdown()
	}
	final := state()
	final["state"] = "STOPPED"
	bytes, _ = json.MarshalIndent(final, "", "  ")
	os.WriteFile(report+".final", bytes, 0600)
	if exec.messages.Load() != 0 {
		t.Fatal("fixture received an unexpected message command; no native executor was called")
	}
}
