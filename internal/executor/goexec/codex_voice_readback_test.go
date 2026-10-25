package goexec

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func voiceReadbackFixture(t *testing.T, extra string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "rollout.jsonl")
	body := `{"type":"session_meta","payload":{"id":"original-thread"}}` + "\n" +
		`{"type":"realtime_item","payload":{"type":"realtime_session_started","realtime_session_id":"old-voice"}}` + "\n" +
		`{"type":"realtime_item","payload":{"type":"transcript_segment","realtime_session_id":"old-voice","role":"assistant","text":"unrelated previous result"}}` + "\n" +
		`{"type":"realtime_item","payload":{"type":"realtime_session_started","realtime_session_id":"current-voice"}}` + "\n" +
		`{"type":"realtime_item","payload":{"type":"transcript_segment","realtime_session_id":"current-voice","role":"user","text":"never read user input as report"}}` + "\n" +
		`{"type":"realtime_item","payload":{"type":"transcript_segment","realtime_session_id":"current-voice","role":"assistant","text":"sealed result"}}` + "\n" + extra
	if e := os.WriteFile(p, []byte(body), 0600); e != nil {
		t.Fatal(e)
	}
	return p
}
func TestVoiceReadbackUsesExactThreadGenerationAndAssistantOnly(t *testing.T) {
	p := voiceReadbackFixture(t, "")
	proof, e := readVoiceRollout(context.Background(), p, "original-thread", "current-voice")
	if e != nil || proof.ThreadID != "original-thread" || proof.VoiceID != "current-voice" || proof.Text != "sealed result" {
		t.Fatal(proof, e)
	}
	if _, e = readVoiceRollout(context.Background(), p, "foreign-thread", "current-voice"); e == nil {
		t.Fatal("foreign thread accepted")
	}
	proof, e = readVoiceRollout(context.Background(), p, "original-thread", "missing-voice")
	if e != nil || proof.Text != "" {
		t.Fatal("latest message substituted", proof, e)
	}
}
func TestVoiceReadbackRefusesSymlinksDuplicateGenerationsAndOversizedRows(t *testing.T) {
	p := voiceReadbackFixture(t, "")
	link := filepath.Join(t.TempDir(), "link.jsonl")
	if e := os.Symlink(p, link); e != nil {
		t.Fatal(e)
	}
	if _, e := readVoiceRollout(context.Background(), link, "original-thread", "current-voice"); e == nil {
		t.Fatal("symlink accepted")
	}
	duplicate := voiceReadbackFixture(t, `{"type":"realtime_item","payload":{"type":"realtime_session_started","realtime_session_id":"current-voice"}}`+"\n")
	if _, e := readVoiceRollout(context.Background(), duplicate, "original-thread", "current-voice"); e == nil {
		t.Fatal("reused generation accepted")
	}
	large := voiceReadbackFixture(t, strings.Repeat("x", 300<<10)+"\n")
	if _, e := readVoiceRollout(context.Background(), large, "original-thread", "current-voice"); e == nil {
		t.Fatal("oversized row read")
	}
}

func TestVoiceReadbackPinsClaimTimeAndStopsBeforeFollowup(t *testing.T) {
	p := filepath.Join(t.TempDir(), "rollout.jsonl")
	rows := []string{
		`{"type":"session_meta","payload":{"id":"original-thread"}}`,
		`{"type":"realtime_item","payload":{"type":"realtime_session_started","realtime_session_id":"current-voice"}}`,
		`{"timestamp":"2026-10-17T14:00:00Z","type":"realtime_item","payload":{"type":"transcript_segment","realtime_session_id":"current-voice","role":"assistant","text":"earlier report"}}`,
		`{"timestamp":"2026-10-17T14:00:02Z","type":"realtime_item","payload":{"type":"transcript_segment","realtime_session_id":"current-voice","role":"assistant","text":"sealed result"}}`,
		`{"timestamp":"2026-10-17T14:00:03Z","type":"realtime_item","payload":{"type":"transcript_segment","realtime_session_id":"current-voice","role":"user","text":"followup question"}}`,
		`{"timestamp":"2026-10-17T14:00:04Z","type":"realtime_item","payload":{"type":"transcript_segment","realtime_session_id":"current-voice","role":"assistant","text":"followup answer"}}`,
	}
	if e := os.WriteFile(p, []byte(strings.Join(rows, "\n")+"\n"), 0600); e != nil {
		t.Fatal(e)
	}
	proof, e := readVoiceRolloutSince(context.Background(), p, "original-thread", "current-voice", 1792245601000)
	if e != nil || proof.Text != "sealed result" {
		t.Fatal(proof, e)
	}
}

func TestVoiceReadbackLargeNativeMetadataUsesSeparateBound(t *testing.T) {
	p := voiceReadbackFixture(t, "")
	raw, e := os.ReadFile(p)
	if e != nil {
		t.Fatal(e)
	}
	split := strings.IndexByte(string(raw), '\n')
	header := `{"type":"session_meta","payload":{"id":"original-thread","instructions":"` + strings.Repeat("x", 600<<10) + `"}}` + "\n"
	if e = os.WriteFile(p, append([]byte(header), raw[split+1:]...), 0600); e != nil {
		t.Fatal(e)
	}
	proof, e := readVoiceRollout(context.Background(), p, "original-thread", "current-voice")
	if e != nil || proof.Text != "sealed result" {
		t.Fatal(proof, e)
	}
	if e = os.WriteFile(p, []byte(strings.Repeat("x", (1<<20)+1)), 0600); e != nil {
		t.Fatal(e)
	}
	if _, e = readVoiceRollout(context.Background(), p, "original-thread", "current-voice"); e == nil {
		t.Fatal("unbounded header accepted")
	}
}
