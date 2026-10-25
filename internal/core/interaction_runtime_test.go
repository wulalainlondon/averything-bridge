package core

import (
	"testing"

	"everything-go/internal/protocol"
)

func runtimeQuestion(id string, blocking *bool) protocol.UserInputRequestEvent {
	return protocol.NewUserInputRequest(protocol.UserInputRequestPayload{
		SessionID: "s1", RequestID: id, IsBlocking: blocking, Status: "pending",
	})
}

func TestNonBlockingQuestionDoesNotChangeRuntime(t *testing.T) {
	for _, phase := range []string{"idle", "running", "completed", "stopping"} {
		t.Run(phase, func(t *testing.T) {
			h, _ := newTestHub(t)
			h.registry.Create("s1", "one", t.TempDir(), "codex", "", "", "")
			h.updateRuntime("s1", phase, "actual-turn", 3, "", "")
			before := h.runtimes.Snapshot("phone", []string{"s1"})[0]
			blocking := false
			h.Emit(runtimeQuestion("ui_async_question", &blocking))
			h.Emit(protocol.NewInteractionResolved("ui_async_question", "s1", "resolved"))
			after := h.runtimes.Snapshot("phone", []string{"s1"})[0]
			if after != before {
				t.Fatalf("non-blocking question changed runtime: before=%+v after=%+v", before, after)
			}
		})
	}
}

func TestBlockingQuestionKeepsTurnIdentityAndQueue(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(map[bool]string{false: "explicit", true: "legacy"}[legacy], func(t *testing.T) {
			h, _ := newTestHub(t)
			h.updateRuntime("s1", "running", "actual-turn", 3, "", "")
			blocking := true
			var flag *bool
			if !legacy {
				flag = &blocking
			}
			h.Emit(runtimeQuestion("question", flag))
			waiting := h.runtimes.Snapshot("", []string{"s1"})[0]
			if waiting.Phase != "waiting" || waiting.ActiveRequestID != "actual-turn" || waiting.QueueLength != 3 {
				t.Fatalf("question stole active turn: %+v", waiting)
			}
			h.Emit(protocol.NewInteractionResolved("unrelated", "s1", "resolved"))
			if got := h.runtimes.Snapshot("", []string{"s1"})[0]; got != waiting {
				t.Fatalf("unrelated resolution resumed turn: %+v", got)
			}
			h.Emit(protocol.NewInteractionResolved("question", "s1", "resolved"))
			got := h.runtimes.Snapshot("", []string{"s1"})[0]
			if got.Phase != "running" || got.ActiveRequestID != "actual-turn" || got.QueueLength != 3 || got.Stage != "thinking" {
				t.Fatalf("resolution lost active turn: %+v", got)
			}
		})
	}
}

func TestLateInteractionResolutionCannotReviveTerminalOrNewTurn(t *testing.T) {
	for _, phase := range []string{"completed", "running", "stopping"} {
		t.Run(phase, func(t *testing.T) {
			h, _ := newTestHub(t)
			h.updateRuntime("s1", "running", "old-turn", 0, "", "")
			blocking := true
			h.Emit(runtimeQuestion("old-question", &blocking))
			h.updateRuntime("s1", phase, "new-turn", 2, "", "")
			before := h.runtimes.Snapshot("", []string{"s1"})[0]
			h.Emit(protocol.NewInteractionResolved("old-question", "s1", "resolved"))
			if got := h.runtimes.Snapshot("", []string{"s1"})[0]; got != before {
				t.Fatalf("late resolution changed newer state: %+v", got)
			}
		})
	}
}

func TestResolutionWithoutActiveQuestionDoesNotCreateRunningState(t *testing.T) {
	h, _ := newTestHub(t)
	h.runtimes.Ensure("s1", "idle", 0)
	before := h.runtimes.Snapshot("", []string{"s1"})[0]
	h.Emit(protocol.NewInteractionResolved("unknown", "s1", "resolved"))
	if got := h.runtimes.Snapshot("", []string{"s1"})[0]; got != before {
		t.Fatalf("resolution created fake work: %+v", got)
	}
}
