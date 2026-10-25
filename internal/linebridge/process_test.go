package linebridge

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestAccountProcessEnvironmentDoesNotInheritUnrelatedCredentials(t *testing.T) {
	values := filteredEnv([]string{"PATH=/bin", "OPENAI_API_KEY=private-fixture", "LINEJS_DATA_DIR=/wrong", "OTHER_AUTH_TOKEN=private-fixture"}, map[string]string{"LINEJS_DATA_DIR": "/exact-account"})
	joined := strings.Join(values, "\n")
	if strings.Contains(joined, "private-fixture") || strings.Contains(joined, "/wrong") {
		t.Fatal("unrelated credential or global account configuration inherited")
	}
	if !strings.Contains(joined, "LINEJS_DATA_DIR=/exact-account") {
		t.Fatal("account configuration missing")
	}
}
func TestActualNodeAccountProcessesAreIsolatedWithoutLogin(t *testing.T) {
	entry, err := filepath.Abs("../../services/line-account/dist/server.js")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(entry); err != nil {
		t.Skip("build pinned LINE service first")
	}
	node, err := exec.LookPath("node")
	if err != nil {
		t.Fatal(err)
	}
	shared, _ := filepath.Abs("../../services/line-voice")
	f := ExecFactory{Node: node, Entry: entry, Shared: shared}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	a := Account{ID: "ln_" + strings.Repeat("1", 32), DataDir: t.TempDir(), KeychainHelper: "/fixture/unused-helper"}
	b := Account{ID: "ln_" + strings.Repeat("2", 32), DataDir: t.TempDir(), KeychainHelper: a.KeychainHelper}
	da, err := f.Start(ctx, a, "epoch-a", func(RuntimeEvent) {})
	if err != nil {
		t.Fatal(err)
	}
	defer da.Close()
	db, err := f.Start(ctx, b, "epoch-b", func(RuntimeEvent) {})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var state PairState
	if err = da.Invoke(ctx, "pair_state", nil, &state); err != nil || state.State != "idle" {
		t.Fatal(state, err)
	}
	if err = da.Close(); err != nil {
		t.Fatal(err)
	}
	if err = db.Invoke(ctx, "pair_state", nil, &state); err != nil || state.State != "idle" {
		t.Fatal("closing A stopped B", err)
	}
	for _, account := range []Account{a, b} {
		if _, err = os.Stat(filepath.Join(account.DataDir, "session.enc")); !os.IsNotExist(err) {
			t.Fatal("process observation created a login")
		}
	}
}
