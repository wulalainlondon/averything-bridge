package handoff

import (
    "encoding/json"
    "strings"
    "testing"
)

func TestIndependentRequirementPrepassRejectsExcessiveDepth(t *testing.T) {
    // 64,001 bytes fit the 64KiB startup-config bound, but are not a
    // reasonable nesting level for this fixed schema. The duplicate-key
    // prepass must reject before recursively visiting all 32,000 levels.
    const depth = 32000
    raw := strings.Repeat("[", depth) + "0" + strings.Repeat("]", depth)
    if err := uniqueKeys(json.NewDecoder(strings.NewReader(raw))); err == nil {
        t.Fatal("duplicate-key prepass accepted 32,000 levels without a depth guard; later typed Decode rejection does not bound the earlier recursion")
    }
}

func TestIndependentRequirementOversizeReminderKeepsIdentity(t *testing.T) {
    p, d := fixture(t)
    d.UpdatedAt = "2026-10-10T07:59:00." + strings.Repeat("0", 7000) + "Z"
    writeDoc(t, p, d)
    s := Read(p, p.Workspace, testNow)
    s.DelegationID = "dg_independent"
    s.ResultSHA256 = strings.Repeat("a", 64)
    reminder := s.Bounded().Reminder()
    if len(reminder) > MaxReminderBytes { t.Fatal("6KiB reminder limit violated") }
    if !strings.Contains(reminder, s.DelegationID) || !strings.Contains(reminder, s.ResultSHA256) || !strings.Contains(reminder, p.Path) || !strings.Contains(reminder, "captured_at") {
        t.Fatalf("validated external timestamp caused fallback to discard delegation/result/path/capture identity; read_status=%s timestamp_bytes=%d reminder_bytes=%d", s.Status, len(s.UpdatedAt), len(reminder))
    }
}
