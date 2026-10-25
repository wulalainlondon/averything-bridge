package executor

import (
	"everything-go/internal/protocol"
	"testing"
)

func TestGoalWarningDoesNotSettleInflightTurn(t *testing.T) {
	sink := NewTerminalSinkWithTimeout(&capSink{}, 0)
	key := sink.Begin("s1", "r1")
	sink.Emit(protocol.NewSessionWarning("s1", "goal get failed: timeout"))
	if sink.Done(key) {
		t.Fatal("Goal warning settled the active turn")
	}
	sink.Emit(protocol.NewDone("s1", "r1"))
	if !sink.Done(key) {
		t.Fatal("real completion did not settle active turn")
	}
}
