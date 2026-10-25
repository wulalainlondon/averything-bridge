package history

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestHistoryWindowGenerationAppendRewriteAndMetadataLoss(t *testing.T) {
	root := t.TempDir()
	t.Setenv("EVERYTHING_GO_HISTORY_GENERATION_DIR", filepath.Join(root, "versions"))
	path := filepath.Join(root, "source.jsonl")
	var raw strings.Builder
	for i := 1; i <= 800; i++ {
		fmt.Fprintf(&raw, "{\"type\":\"user\",\"message\":{\"content\":\"row-%04d %s\"}}\n", i, strings.Repeat("x", 40))
	}
	os.WriteFile(path, []byte(raw.String()), 0600)
	first, e := ReadFileWindow(path, 400, 4096)
	if e != nil {
		t.Fatal(e)
	}
	if !first.MoreBefore || !first.MoreAfter {
		t.Fatal("fixture must straddle bounded middle")
	}
	for _, line := range first.Lines {
		if !strings.Contains(string(line.Data), fmt.Sprintf("row-%04d", line.LineNo)) {
			t.Fatal("absolute line id changed", line.LineNo)
		}
	}
	out, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	out.WriteString(`{"type":"user","message":{"content":"partial`)
	out.Close()
	middle, e := ReadFileWindow(path, 400, 4096)
	if e != nil {
		t.Fatal(e)
	}
	if middle.Generation != first.Generation {
		t.Fatal("append must not invent a new source")
	}
	out, _ = os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	out.WriteString(" completed\"}}\n")
	out.Close()
	finished, e := ReadFileWindow(path, 801, 4096)
	if e != nil {
		t.Fatal(e)
	}
	if finished.Generation != first.Generation {
		t.Fatal("finishing partial physical line must retain source")
	}
	data, _ := os.ReadFile(path)
	at := strings.Index(string(data), "row-0400")
	data[at+4] = '9'
	os.WriteFile(path, data, 0600)
	future := time.Now().Add(time.Second)
	os.Chtimes(path, future, future)
	changed, e := ReadFileWindow(path, 400, 4096)
	if e != nil {
		t.Fatal(e)
	}
	if changed.Generation == first.Generation {
		t.Fatal("same-size source rewrite must retire generation")
	}
	windowFiles.Lock()
	delete(windowFiles.values, path)
	windowFiles.Unlock()
	reopened, e := ReadFileWindow(path, 400, 4096)
	if e != nil {
		t.Fatal(e)
	}
	if reopened.Generation != changed.Generation {
		t.Fatal("index eviction must not reset durable source version")
	}
	sum := sha256.Sum256([]byte(path))
	os.Remove(filepath.Join(root, "versions", hex.EncodeToString(sum[:])+".json"))
	windowFiles.Lock()
	delete(windowFiles.values, path)
	windowFiles.Unlock()
	fresh, e := ReadFileWindow(path, 400, 4096)
	if e != nil {
		t.Fatal(e)
	}
	if fresh.Generation == changed.Generation {
		t.Fatal("missing metadata must not reuse a guessed source version")
	}
}
