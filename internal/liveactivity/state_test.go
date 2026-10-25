package liveactivity

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestStateIdentityOrderingAndPrivacyContract(t *testing.T) {
	state := State{Phase: "running", Stage: "waiting_model", Revision: 8, UpdatedAt: time.Now().UnixMilli()}
	if !state.Valid() || state.Terminal() {
		t.Fatal("valid running state rejected")
	}
	older := state
	older.Revision--
	if state.Accepts(older) || state.Accepts(state) {
		t.Fatal("stale/equal revisions accepted")
	}
	terminal := state
	terminal.Phase = "completed"
	terminal.Revision++
	if !state.Accepts(terminal) || !terminal.Terminal() {
		t.Fatal("terminal transition rejected")
	}
	later := terminal
	later.Phase = "running"
	later.Revision++
	if terminal.Accepts(later) {
		t.Fatal("ended run resurrected")
	}
	body, _ := json.Marshal(terminal)
	for _, field := range []string{"token", "credential", "session_name", "content", "title"} {
		if strings.Contains(string(body), field) {
			t.Fatal("private content in live state")
		}
	}
	invalid := state
	invalid.Phase = "idle"
	if invalid.Valid() {
		t.Fatal("idle state accepted")
	}
	invalid = state
	invalid.Stage = strings.Repeat("x", 81)
	if invalid.Valid() {
		t.Fatal("unbounded stage")
	}
}
func TestDeliveryBoundsAndToken(t *testing.T) {
	now := time.Now().Unix()
	delivery := Delivery{ActivityID: "activity", Timestamp: now, Event: "update", StaleDate: now + 180, State: State{Phase: "running", Revision: 1, UpdatedAt: now * 1000}}
	if !delivery.Valid() || !ValidToken(strings.Repeat("ab", 32)) || ValidToken("../secret") {
		t.Fatal("contract bounds")
	}
	delivery.Event = "start"
	if delivery.Valid() {
		t.Fatal("remote start not supported by MVP")
	}
	delivery.Event = "end"
	if delivery.Valid() {
		t.Fatal("active state marked ended")
	}
	delivery.State.Phase = "completed"
	delivery.DismissalDate = now + 900
	if !delivery.Valid() {
		t.Fatal("valid end rejected")
	}
}
