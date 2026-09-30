package core

import (
	"reflect"
	"testing"

	"everything-go/internal/protocol"
)

func TestGoalWarningPreservesActiveRuntime(t *testing.T) {
	h, _ := newTestHub(t)
	h.registry.Create("s1", "one", t.TempDir(), "codex", "", "", "")
	h.updateRuntime("s1", "running", "r1", 1, "", "")
	before := h.runtimeSnapshot("phone")
	h.Emit(protocol.NewSessionWarning("s1", "goal get failed: timed out after 30s"))
	if after := h.runtimeSnapshot("phone"); !reflect.DeepEqual(before, after) {
		t.Fatalf("Goal warning changed active runtime: before=%+v after=%+v", before, after)
	}
}
