package core

import (
	"context"
	"everything-go/internal/callbacks"
	"testing"
)

func TestCallbackClosurePreservesTextEvidenceWithoutInventingPlayout(t *testing.T) {
	for _, outcome := range []string{"ended", "failed"} {
		t.Run(outcome, func(t *testing.T) {
			h, _, _, r := callbackReady(t)
			r, err := h.callbacks.Mutate(context.Background(), "fixture-text-evidence", "fixture-text-evidence", r.ID, r.Revision, func(x *callbacks.Record, _ bool) error {
				x.State = "report_text_verified"
				x.Invite.State = "report_text_verified"
				x.Invite.ContentVerified = true
				x.Invite.Voice = "original-verified-voice"
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			source, ok := h.registry.Get(r.Session)
			if !ok {
				t.Fatal("source missing")
			}
			call := &clientVoiceCall{callbackPolicy: r.ID, session: source, voiceID: r.Invite.Voice}
			h.callbackVoiceOutcome(call, outcome, "")
			got, found, err := h.callbacks.Get(context.Background(), r.ID)
			if err != nil || !found || got.State != "report_text_verified" || !got.Invite.ContentVerified || got.Invite.AudioObserved || got.Invite.State != outcome {
				t.Fatalf("closure erased verified text or invented output: %#v %v", got, err)
			}
		})
	}
}
