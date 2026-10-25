package goexec

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const exactFixtureThread = "01a0ec71-65c0-7c71-aad2-1336d7251333"

func exactFixture(t *testing.T) (*Codex, string) {
	t.Helper()
	c := NewCodex(&capSink{}, "codex")
	c.dataDir = t.TempDir()
	c.sessionsRoot = t.TempDir()
	if e := c.rememberTurnRequest(exactFixtureThread, "turn", "request"); e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(c.sessionsRoot, "rollout-2026-09-29T17-14-09-"+exactFixtureThread+".jsonl")
	c.rolloutRoot = c.sessionsRoot
	c.rolloutByID = map[string]string{exactFixtureThread: path}
	return c, path
}
func fixtureHeader() string {
	return fmt.Sprintf("{\"type\":\"session_meta\",\"payload\":{\"id\":%q}}\n", exactFixtureThread)
}
func fixtureTurn(turn, text, terminal string) string {
	data := fmt.Sprintf("{\"type\":\"event_msg\",\"payload\":{\"type\":\"task_started\",\"turn_id\":%q}}\n", turn)
	if text != "" {
		data += fmt.Sprintf("{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"assistant\",\"phase\":\"final_answer\",\"content\":[{\"text\":%q}]}}\n", text)
	}
	if terminal != "" {
		data += fmt.Sprintf("{\"type\":\"event_msg\",\"payload\":{\"type\":%q,\"turn_id\":%q,\"last_agent_message\":%q}}\n", terminal, turn, text)
	}
	return data
}
func exactPoll(t *testing.T, c *Codex) (bool, error) {
	t.Helper()
	for i := 0; i < 40; i++ {
		found, e := c.ExactQueueFinalForReceipt(context.Background(), exactFixtureThread, "request", "turn", strings.Repeat("a", 64))
		if e == nil || !errors.Is(e, exactFinalError("final_scan_pending")) {
			return found, e
		}
	}
	t.Fatal("bounded progress did not finish")
	return false, nil
}
func TestNF01LargeActualGoReaderPast64MiBAndOverMiBNonFinal(t *testing.T) {
	c, path := exactFixture(t)
	f, e := os.Create(path)
	if e != nil {
		t.Fatal(e)
	}
	f.WriteString(fixtureHeader())
	// Actual >64MiB stream, produced with <=1MiB temporary memory; no model/history.
	f.WriteString(`{"type":"response_item","payload":{"type":"reasoning","opaque":"`)
	block := strings.Repeat("x", 1<<20)
	for i := 0; i < 70; i++ {
		f.WriteString(block)
	}
	f.WriteString("\"}}\n")
	f.WriteString(`{"type":"event_msg","payload":{"type":"task_started","turn_id":"turn"}}` + "\n")
	f.WriteString(`{"type":"response_item","payload":{"type":"reasoning","opaque":"`)
	f.WriteString(block)
	f.WriteString(block)
	f.WriteString("\"}}\n")
	f.WriteString(`{"type":"response_item","payload":{"type":"message","role":"assistant","phase":"final_answer","content":[{"text":"exact final"}]}}` + "\n")
	f.WriteString(`{"type":"event_msg","payload":{"type":"task_complete","turn_id":"turn","last_agent_message":"exact final"}}` + "\n")
	f.Close()
	// The previous real ReadSlice/1MiB head algorithm must reproduce its failure.
	old, e := os.Open(path)
	if e != nil {
		t.Fatal(e)
	}
	defer old.Close()
	if _, e = old.Seek(int64(len(fixtureHeader())), 0); e != nil {
		t.Fatal(e)
	}
	oversized := make([]byte, 1<<20)
	n, e := old.Read(oversized)
	if n != len(oversized) || strings.Contains(string(oversized), "\n") {
		t.Fatal("fixture did not reproduce >1MiB boundary", n, e)
	}
	if found, e := exactPoll(t, c); !found || e != nil {
		t.Fatal(found, e)
	}
	var st *exactFinalScan
	for _, v := range c.exactFinalScans {
		st = v
	}
	if st == nil || st.bytesRead > exactFinalWindow {
		t.Fatal("unbounded head scan", st)
	}
	before := st.bytesRead
	for i := 0; i < 5; i++ {
		if found, e := exactPoll(t, c); !found || e != nil {
			t.Fatal(found, e)
		}
	}
	if st.bytesRead != before {
		t.Fatal("cache rescanned history")
	}
}
func TestNF01ExactBoundariesAndSafeErrorSubtypes(t *testing.T) {
	for _, row := range []struct {
		name, data, reason string
		found              bool
	}{
		{"valid", fixtureHeader() + fixtureTurn("turn", "final", "task_complete"), "", true},
		{"wrong_turn", fixtureHeader() + fixtureTurn("foreign", "final", "task_complete"), "", false},
		{"no_final", fixtureHeader() + fixtureTurn("turn", "", "task_complete"), "final_boundary_conflict", false},
		{"no_terminal", fixtureHeader() + fixtureTurn("turn", "final", ""), "final_terminal_missing", false},
		{"aborted", fixtureHeader() + fixtureTurn("turn", "final", "turn_aborted"), "final_boundary_conflict", false},
		{"commentary", fixtureHeader() + strings.ReplaceAll(fixtureTurn("turn", "final", "task_complete"), "final_answer", "commentary"), "final_boundary_conflict", false},
		{"wrong_header", strings.Replace(fixtureHeader(), exactFixtureThread, "foreign", 1) + fixtureTurn("turn", "final", "task_complete"), "final_thread_mismatch", false},
		{"complete_conflict", fixtureHeader() + strings.Replace(fixtureTurn("turn", "final", "task_complete"), `"last_agent_message":"final"`, `"last_agent_message":"wrong"`, 1), "final_boundary_conflict", false},
	} {
		t.Run(row.name, func(t *testing.T) {
			c, p := exactFixture(t)
			os.WriteFile(p, []byte(row.data), 0600)
			found, e := exactPoll(t, c)
			if found != row.found || row.reason != "" && !errors.Is(e, exactFinalError(row.reason)) || row.reason == "" && e != nil {
				t.Fatal(found, e)
			}
		})
	}
}
func TestNF01CacheAppendTruncateReplacementAndScope(t *testing.T) {
	c, p := exactFixture(t)
	original := fixtureHeader() + fixtureTurn("turn", "final", "task_complete")
	os.WriteFile(p, []byte(original), 0600)
	if found, e := exactPoll(t, c); !found || e != nil {
		t.Fatal(found, e)
	}
	f, _ := os.OpenFile(p, os.O_APPEND|os.O_WRONLY, 0600)
	f.WriteString(fixtureTurn("later", "other", "task_complete"))
	f.Close()
	if found, e := exactPoll(t, c); found || !errors.Is(e, exactFinalError("final_source_changed")) {
		t.Fatal("append reused unproven snapshot", found, e)
	}
	if found, e := exactPoll(t, c); !found || e != nil {
		t.Fatal("fresh bounded append view", found, e)
	}
	os.WriteFile(p, []byte(fixtureHeader()), 0600)
	if found, e := exactPoll(t, c); found || !errors.Is(e, exactFinalError("final_source_changed")) {
		t.Fatal("truncation reused", found, e)
	}
	os.WriteFile(p, []byte(original), 0600)
	if found, e := exactPoll(t, c); !found || e != nil {
		t.Fatal(found, e)
	}
	newFile := p + ".new"
	os.WriteFile(newFile, []byte(original), 0600)
	os.Rename(newFile, p)
	if found, e := exactPoll(t, c); found || !errors.Is(e, exactFinalError("final_source_changed")) {
		t.Fatal("new writer reused", found, e)
	}
	c.rolloutRoot = "changed"
	if found, e := exactPoll(t, c); found || !errors.Is(e, exactFinalError("final_scope_changed")) {
		t.Fatal(found, e)
	}
}
func TestNF01NoDiscoveryWrongHashMappingSymlinkAndCancellation(t *testing.T) {
	c, p := exactFixture(t)
	os.WriteFile(p, []byte(fixtureHeader()+fixtureTurn("turn", "final", "task_complete")), 0600)
	c.rolloutByID = nil
	if found, e := exactPoll(t, c); found || !errors.Is(e, exactFinalError("final_index_unavailable")) {
		t.Fatal(found, e)
	}
	c.rolloutByID = map[string]string{exactFixtureThread: p}
	if found, e := c.ExactQueueFinalForReceipt(context.Background(), exactFixtureThread, "request", "turn", "bad"); found || !errors.Is(e, exactFinalError("final_payload_identity_invalid")) {
		t.Fatal(found, e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if found, e := c.ExactQueueFinalForReceipt(ctx, exactFixtureThread, "request", "turn", ""); found || !errors.Is(e, exactFinalError("final_scan_pending")) {
		t.Fatal(found, e)
	}
	linked := p + ".symlink"
	os.Symlink(p, linked)
	c.rolloutByID[exactFixtureThread] = linked
	if found, e := exactPoll(t, c); found || !errors.Is(e, exactFinalError("final_source_scope_mismatch")) {
		t.Fatal(found, e)
	}
	c.rolloutByID[exactFixtureThread] = p
	c.rememberTurnRequest(exactFixtureThread, "duplicate", "request")
	if found, e := exactPoll(t, c); found || !errors.Is(e, exactFinalError("final_mapping_ambiguous")) {
		t.Fatal(found, e)
	}
}
func TestNF01FinalByteBoundAndBudget(t *testing.T) {
	c, p := exactFixture(t)
	os.WriteFile(p, []byte(fixtureHeader()+fixtureTurn("turn", strings.Repeat("z", exactFinalText+1), "task_complete")), 0600)
	if found, e := exactPoll(t, c); found || !errors.Is(e, exactFinalError("final_boundary_conflict")) {
		t.Fatal(found, e)
	}
}

func TestNF01RewriteAppendCannotReuseUnpinnedProtocolAndAuthorityAlias(t *testing.T) {
	c, p := exactFixture(t)
	// Unpinned filler is deliberately the same byte length as a target abort.
	abort := `{"type":"event_msg","payload":{"type":"turn_aborted","turn_id":"turn"}}` + "\n"
	filler := `{"type":"event_msg","payload":{"type":"token_count"}}`
	filler += strings.Repeat(" ", len(abort)-len(filler)-1) + "\n"
	original := fixtureHeader() + fixtureTurn("turn", "final", "task_complete") + filler
	os.WriteFile(p, []byte(original), 0600)
	if found, e := exactPoll(t, c); !found || e != nil {
		t.Fatal(found, e)
	}
	os.WriteFile(p, []byte(strings.Replace(original, filler, abort, 1)+fixtureTurn("later", "other", "task_complete")), 0600)
	if found, e := exactPoll(t, c); found || !errors.Is(e, exactFinalError("final_source_changed")) {
		t.Fatal("rewrite append reused old terminal", found, e)
	}
	if found, e := exactPoll(t, c); found || !errors.Is(e, exactFinalError("final_boundary_conflict")) {
		t.Fatal("changed abort missed", found, e)
	}
}
func TestNF01ConfinedIntermediateSymlinkAndBoundedStep(t *testing.T) {
	c, p := exactFixture(t)
	outside := t.TempDir()
	foreign := filepath.Join(outside, filepath.Base(p))
	os.WriteFile(foreign, []byte(fixtureHeader()+fixtureTurn("turn", "final", "task_complete")), 0600)
	sub := filepath.Join(c.sessionsRoot, "link")
	os.Symlink(outside, sub)
	c.rolloutByID[exactFixtureThread] = filepath.Join(sub, filepath.Base(p))
	if found, e := exactPoll(t, c); found || !errors.Is(e, exactFinalError("final_source_scope_mismatch")) {
		t.Fatal("outside source read", found, e)
	}
	c.rolloutByID[exactFixtureThread] = p
	f, _ := os.Create(p)
	f.WriteString(fixtureHeader())
	f.WriteString(`{"type":"response_item","payload":{"type":"reasoning","opaque":"`)
	f.WriteString(strings.Repeat("x", 10<<20))
	f.WriteString("\"}}\n")
	f.WriteString(fixtureTurn("turn", "final", "task_complete"))
	f.Close()
	found, e := c.ExactQueueFinalForReceipt(context.Background(), exactFixtureThread, "request", "turn", strings.Repeat("a", 64))
	if found || !errors.Is(e, exactFinalError("final_scan_pending")) {
		t.Fatal(found, e)
	}
	for _, st := range c.exactFinalScans {
		if st.bytesRead > exactFinalStep {
			t.Fatal("step budget exceeded", st.bytesRead)
		}
	}
	if found, e := exactPoll(t, c); !found || e != nil {
		t.Fatal(found, e)
	}
}
func TestNF01OutsideTailAndOversizedFinalRemainUnknown(t *testing.T) {
	c, p := exactFixture(t)
	f, _ := os.Create(p)
	f.WriteString(fixtureHeader() + fixtureTurn("turn", "old", "task_complete"))
	f.WriteString(`{"type":"response_item","payload":{"type":"reasoning","opaque":"`)
	block := strings.Repeat("x", 1<<20)
	for i := 0; i < 66; i++ {
		f.WriteString(block)
	}
	f.WriteString("\"}}\n")
	f.Close()
	if found, e := exactPoll(t, c); found || !errors.Is(e, exactFinalError("final_outside_window")) {
		t.Fatal(found, e)
	}
}
func TestNF01ForeignOversizedLaterPublicFinalDoesNotEraseObservedHistory(t *testing.T) {
	c, p := exactFixture(t)
	data := fixtureHeader() + fixtureTurn("turn", "exact", "task_complete") + fixtureTurn("foreign", "", "") + `{"type":"response_item","payload":{"type":"message","role":"assistant","phase":"final_answer","content":[{"text":"` + strings.Repeat("x", 2<<20) + `"}]}}` + "\n" + `{"type":"event_msg","payload":{"type":"task_complete","turn_id":"foreign"}}` + "\n"
	os.WriteFile(p, []byte(data), 0600)
	if found, e := exactPoll(t, c); !found || e != nil {
		t.Fatal("foreign later text erased exact prior public span", found, e)
	}
}
func TestNF01OversizedTargetAbortIsNotIgnoredAfterComplete(t *testing.T) {
	c, p := exactFixture(t)
	data := fixtureHeader() + fixtureTurn("turn", "exact", "task_complete") + `{"type":"event_msg","payload":{"type":"turn_aborted","turn_id":"turn","opaque":"` + strings.Repeat("x", 2<<20) + `"}}` + "\n"
	os.WriteFile(p, []byte(data), 0600)
	if found, e := exactPoll(t, c); found || !errors.Is(e, exactFinalError("final_boundary_conflict")) {
		t.Fatal("oversized target contradiction ignored", found, e)
	}
}
func TestNF01OversizedReorderedBoundaryRemainsUnknownAfterComplete(t *testing.T) {
	for _, kind := range []string{"turn_aborted", "task_started", "task_complete"} {
		t.Run(kind, func(t *testing.T) {
			c, p := exactFixture(t)
			data := fixtureHeader() + fixtureTurn("turn", "exact", "task_complete") + `{"type":"event_msg","payload":{"type":"` + kind + `","opaque":"` + strings.Repeat("x", 2<<20) + `","turn_id":"turn"}}` + "\n"
			os.WriteFile(p, []byte(data), 0600)
			if found, e := exactPoll(t, c); found || !errors.Is(e, exactFinalError("final_boundary_conflict")) {
				t.Fatal("unclassified later boundary accepted", found, e)
			}
		})
	}
}
func TestNF01CompactedAndItemCompletedBodiesDoNotEraseExactPublicReturn(t *testing.T) {
	for _, prefix := range []string{`{"type":"compacted","payload":{"opaque":"`, `{"type":"event_msg","payload":{"type":"item_completed","opaque":"`} {
		c, p := exactFixture(t)
		os.WriteFile(p, []byte(fixtureHeader()+fixtureTurn("turn", "exact", "task_complete")+prefix+strings.Repeat("x", 2<<20)+`"}}`+"\n"), 0600)
		if found, e := exactPoll(t, c); !found || e != nil {
			t.Fatal(found, e)
		}
	}
}
func TestNF01InvalidLongEscapesAndDuplicatePublicBoundaries(t *testing.T) {
	for _, end := range []string{`\q","type":"token_count"}}`, `\u12xz","type":"token_count"}}`, `","TYPE":"token_count"}}`, `","type":"token_count"}}`} {
		c, p := exactFixture(t)
		data := fixtureHeader() + fixtureTurn("turn", "exact", "task_complete") + `{"type":"event_msg","payload":{"type":"turn_aborted","turn_id":"turn","opaque":"` + strings.Repeat("x", 2<<20) + end + "\n"
		os.WriteFile(p, []byte(data), 0600)
		if found, e := exactPoll(t, c); found || !errors.Is(e, exactFinalError("final_boundary_conflict")) {
			t.Fatal("malformed/ambiguous large control survived", found, e)
		}
	}
	for _, data := range []string{`{"type":"event_msg","payload":{"type":"turn_aborted","turn_id":"turn","type":"token_count"}}`, `{"type":"event_msg","payload":{"type":"turn_aborted","turn_id":"turn","TYPE":"token_count"}}`} {
		c, p := exactFixture(t)
		os.WriteFile(p, []byte(fixtureHeader()+fixtureTurn("turn", "exact", "task_complete")+data+"\n"), 0600)
		if found, e := exactPoll(t, c); found || !errors.Is(e, exactFinalError("final_boundary_conflict")) {
			t.Fatal(found, e)
		}
	}
}
func TestNF01DuplicateMetadataAndProofTextAreUnknown(t *testing.T) {
	c, p := exactFixture(t)
	header := `{"type":"session_meta","payload":{"id":"foreign","ID":"` + exactFixtureThread + `"}}` + "\n"
	os.WriteFile(p, []byte(header+fixtureTurn("turn", "exact", "task_complete")), 0600)
	if found, e := exactPoll(t, c); found || !errors.Is(e, exactFinalError("final_thread_mismatch")) {
		t.Fatal(found, e)
	}
	c, p = exactFixture(t)
	data := fixtureHeader() + fixtureTurn("turn", "exact", "task_complete") + `{"type":"event_msg","payload":{"type":"task_complete","turn_id":"turn","last_agent_message":"wrong","LAST_AGENT_MESSAGE":"exact"}}` + "\n"
	os.WriteFile(p, []byte(data), 0600)
	if found, e := exactPoll(t, c); found || !errors.Is(e, exactFinalError("final_boundary_conflict")) {
		t.Fatal(found, e)
	}
}
func TestNF01UnicodeDecoderAliasesCannotOverridePhaseAndLastMessage(t *testing.T) {
	for _, middle := range []string{`{"type":"response_item","payload":{"type":"message","role":"assistant","phase":"commentary","pha\u017fe":"final_answer","content":[{"text":"exact"}]}}`, `{"type":"event_msg","payload":{"type":"task_complete","turn_id":"turn","last_agent_message":"wrong","last_agent_me\u017f\u017fage":"exact"}}`} {
		c, p := exactFixture(t)
		os.WriteFile(p, []byte(fixtureHeader()+fixtureTurn("turn", "exact", "task_complete")+middle+"\n"), 0600)
		if found, e := exactPoll(t, c); found || !errors.Is(e, exactFinalError("final_boundary_conflict")) {
			t.Fatal(found, e)
		}
	}
}

func TestCoordinatorNF01OversizedForeignBoundaryCannotAttachForeignFinal(t *testing.T) {
	c, p := exactFixture(t)
	data := fixtureHeader() + fixtureTurn("turn", "", "") + `{"type":"event_msg","payload":{"type":"task_started","turn_id":"foreign","opaque":"` + strings.Repeat("x", 2<<20) + `"}}` + "\n" + `{"type":"response_item","payload":{"type":"message","role":"assistant","phase":"final_answer","content":[{"text":"foreign final"}]}}` + "\n" + `{"type":"event_msg","payload":{"type":"task_complete","turn_id":"turn"}}` + "\n"
	if e := os.WriteFile(p, []byte(data), 0600); e != nil {
		t.Fatal(e)
	}
	found, e := exactPoll(t, c)
	if found {
		t.Fatalf("foreign final was accepted as original target return: found=%v error=%v", found, e)
	}
}

// Same exact request/map/payload identity throughout: only public control
// ordering and target closure differ. No live provider or queue mutation.
func TestNF02OversizedForeignControlBreaksUnfinishedTargetAssociation(t *testing.T) {
	for _, kind := range []string{"task_started", "turn_context", "task_complete", "turn_aborted"} {
		for _, order := range []string{"before", "between", "after"} {
			for _, state := range []string{"unfinished", "has_final", "completed"} {
				t.Run(kind+"/"+order+"/"+state, func(t *testing.T) {
					c, p := exactFixture(t)
					text, terminal := "", ""
					if state != "unfinished" {
						text = "target final"
					}
					if state == "completed" {
						terminal = "task_complete"
					}
					row, kindField := "event_msg", fmt.Sprintf(`"type":%q,`, kind)
					if kind == "turn_context" {
						row, kindField = "turn_context", ""
					}
					opaque := `"opaque":"` + strings.Repeat("x", 2<<20) + `"`
					fields := kindField + `"turn_id":"foreign",` + opaque
					if order == "before" {
						fields = opaque + `,` + kindField + `"turn_id":"foreign"`
					}
					if order == "between" {
						fields = kindField + opaque + `,"turn_id":"foreign"`
					}
					foreign := fmt.Sprintf(`{"type":%q,"payload":{%s}}`, row, fields) + "\n"
					final := `{"type":"response_item","payload":{"type":"message","role":"assistant","phase":"final_answer","content":[{"text":"foreign final"}]}}` + "\n"
					data := fixtureHeader() + fixtureTurn("turn", text, terminal) + foreign + final
					if state != "completed" {
						data += `{"type":"event_msg","payload":{"type":"task_complete","turn_id":"turn"}}` + "\n"
					}
					if e := os.WriteFile(p, []byte(data), 0600); e != nil {
						t.Fatal(e)
					}
					found, e := exactPoll(t, c)
					if state == "completed" {
						if !found || e != nil {
							t.Fatalf("closed exact target lost: %v %v", found, e)
						}
						// Cached proof must preserve the same result without active foreign turn.
						if found, e = exactPoll(t, c); !found || e != nil {
							t.Fatalf("cached closed target lost: %v %v", found, e)
						}
					} else if found {
						t.Fatalf("foreign final attached to unfinished target: %v", e)
					}
				})
			}
		}
	}
}
