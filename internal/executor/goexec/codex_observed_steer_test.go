package goexec

import (
	"encoding/json"
	"testing"
)

func TestObservedNativeTurnCanBeSteeredWithoutOwningTheWorker(t *testing.T) {
	c, s, st, w, _ := livenessFixture(t)
	st.turnActive = false
	st.currentTurnID = ""
	st.observedTurnID = "observed"
	st.observedRequestID = "codex_external_observed"
	w.reply = func(method string, raw json.RawMessage) (any, error) {
		var p map[string]any
		_ = json.Unmarshal(raw, &p)
		if method != "turn/steer" || p["threadId"] != "root" || p["expectedTurnId"] != "observed" || p["clientUserMessageId"] != "queued" {
			t.Fatalf("wrong target %s %s", method, raw)
		}
		return map[string]any{"turnId": "observed"}, nil
	}
	result, err := c.steerActiveTurn(s, "queued", "continue", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !result.StrictACK || result.RequestID != "codex_external_observed" || result.TurnID != "observed" || st.turnActive || st.reqID != "request-1" {
		t.Fatal("steer changed worker ownership", result)
	}
}

func TestObservedNativeSteerRejectsStaleBoundaryAndUncertainACK(t *testing.T) {
	for _, name := range []string{"ended", "new-owned-turn", "source-changed", "config-changed", "wrong-ack", "epoch-changed"} {
		t.Run(name, func(t *testing.T) {
			c, s, st, w, _ := livenessFixture(t)
			st.turnActive = false
			st.currentTurnID = ""
			st.observedTurnID = "observed"
			st.observedRequestID = "codex_external_observed"
			w.reply = func(method string, raw json.RawMessage) (any, error) {
				switch name {
				case "ended":
					st.mu.Lock()
					st.observedTurnID = ""
					st.mu.Unlock()
				case "new-owned-turn":
					st.mu.Lock()
					st.turnActive = true
					st.currentTurnID = "new"
					st.mu.Unlock()
				case "source-changed":
					s.SetResumeID("other")
				case "config-changed":
					s.SetConfigRevision(s.Snapshot().ConfigRevision + 1)
				case "epoch-changed":
					c.rpc.transportEpoch.Add(1)
				case "wrong-ack":
					return map[string]any{"turnId": "other"}, nil
				}
				return map[string]any{"turnId": "observed"}, nil
			}
			if r, err := c.steerActiveTurn(s, "queued", "continue", nil, nil); err == nil || r.StrictACK {
				t.Fatal("accepted changed native boundary", r, err)
			}
		})
	}
}
func TestObservedNativeSteerNeedsExactLiveScopeBeforeRPC(t *testing.T) {
	for _, name := range []string{"no-request", "no-turn", "wrong-thread", "compacting", "stopping"} {
		t.Run(name, func(t *testing.T) {
			c, s, st, w, _ := livenessFixture(t)
			st.turnActive = false
			st.currentTurnID = ""
			st.observedTurnID = "observed"
			st.observedRequestID = "codex_external_observed"
			switch name {
			case "no-request":
				st.observedRequestID = ""
			case "no-turn":
				st.observedTurnID = ""
			case "wrong-thread":
				s.SetResumeID("other")
			case "compacting":
				st.compactActive = true
			case "stopping":
				st.stopping = true
			}
			if _, err := c.steerActiveTurn(s, "queued", "continue", nil, nil); err == nil {
				t.Fatal("accepted without native proof")
			}
			if len(w.methods) != 0 {
				t.Fatal("sent RPC without live scope")
			}
		})
	}
}
