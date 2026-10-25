package core

import (
	"everything-go/internal/clientproto"
	"everything-go/internal/session"
	"testing"
	"time"
)

func TestVoicePrepareStartsNoCallAndPermitIsSingleUse(t *testing.T) {
	h, _, _, target := controllerFixture(t)
	c := sharedReadClient(t, h, "prepared-phone")
	cmd := clientproto.Command{SessionID: target.ID, VoiceID: "fresh-voice", RequestID: "prepare-request"}
	h.handleVoicePrepare(c, cmd)
	if len(h.voicePermits) != 1 || len(h.voiceClients) != 1 {
		t.Fatal("preparing changed voice ownership")
	}
	for id, permit := range h.voicePermits {
		if permit.thread != target.ResumeID() || permit.owner != target {
			t.Fatal("wrong source")
		}
		cmd.VoiceStartPermit = id
	}
	if !h.consumeVoicePermit(c, target, cmd) {
		t.Fatal("original permit refused")
	}
	if h.consumeVoicePermit(c, target, cmd) {
		t.Fatal("replayed permit accepted")
	}
}
func TestVoicePrepareRejectsExpiryScopeAndDeviceDrift(t *testing.T) {
	for _, kind := range []string{"expired", "device", "voice", "thread", "config", "closed"} {
		t.Run(kind, func(t *testing.T) {
			h, _, _, target := controllerFixture(t)
			c := sharedReadClient(t, h, "phone")
			cmd := clientproto.Command{SessionID: target.ID, VoiceID: "fresh-voice", RequestID: "prepare-request"}
			h.handleVoicePrepare(c, cmd)
			for id, permit := range h.voicePermits {
				cmd.VoiceStartPermit = id
				if kind == "expired" {
					permit.expires = time.Now().Add(-time.Second)
					h.voicePermits[id] = permit
				}
			}
			switch kind {
			case "device":
				c = sharedReadClient(t, h, "other-phone")
			case "voice":
				cmd.VoiceID = "other-voice"
			case "thread":
				target.SetResumeID("replacement")
			case "config":
				target.SetFutureConfiguration(session.ConfigurationFrom(target.SettingsSnapshot()), nil)
			case "closed":
				target.Close()
			}
			if h.consumeVoicePermit(c, target, cmd) {
				t.Fatal("stale or foreign permit accepted", kind)
			}
		})
	}
}
func TestVoicePrepareRefusesActiveOwnerOnSameDevice(t *testing.T) {
	h, _, caller, target := controllerFixture(t)
	c := sharedReadClient(t, h, "phone")
	h.voiceClients[c] = &clientVoiceCall{session: caller.Parent, voiceID: "old-voice", stopping: true}
	h.handleVoicePrepare(c, clientproto.Command{SessionID: target.ID, VoiceID: "fresh-voice", RequestID: "prepare-request"})
	if len(h.voicePermits) != 0 {
		t.Fatal("permit issued while old owner remains")
	}
}
