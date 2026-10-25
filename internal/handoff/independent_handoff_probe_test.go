package handoff

import (
    "encoding/json"
    "strings"
    "testing"
)

func TestIndependentHandoffDepthProbe(t *testing.T) {
    // Token() does not use encoding/json's full-value scanner depth guard.
    for _, depth := range []int{8190, 10001, 32000} {
        raw := strings.Repeat("[", depth) + "0" + strings.Repeat("]", depth)
        err := uniqueKeys(json.NewDecoder(strings.NewReader(raw)))
        t.Logf("duplicate-key prepass depth=%d bytes=%d error=%v", depth, len(raw), err)
        if _, err := ReadConfig(strings.NewReader(raw)); err == nil {
            t.Fatal("typed config accepted nested array")
        }
    }
    p, _ := fixture(t)
    raw := `{"schema_version":1,"revision":1,"updated_at":"2026-10-10T07:59:00Z","goal":` + strings.Repeat("[", 8000) + `"x"` + strings.Repeat("]", 8000) + `,"progress":"x","next_step":"x"}`
    if len(raw) >= MaxFileBytes { t.Fatal("probe exceeded file bound") }
    // Public file reader still rejects the wrong typed field within its byte cap.
    if err := writeProbeFile(p, raw); err != nil { t.Fatal(err) }
    if got := Read(p, p.Workspace, testNow); got.Status != "unavailable" { t.Fatal(got) }
}

func TestIndependentHandoffLongTimestampProbe(t *testing.T) {
    p, d := fixture(t)
    d.UpdatedAt = "2026-10-10T07:59:00." + strings.Repeat("0", 7000) + "Z"
    writeDoc(t, p, d)
    s := Read(p, p.Workspace, testNow)
    s.DelegationID = "dg_independent"
    s.ResultSHA256 = strings.Repeat("a", 64)
    b, _ := json.Marshal(s.Bounded())
    t.Logf("public read status=%s timestamp_bytes=%d bounded_snapshot_bytes=%d reminder_bytes=%d reminder_has_result_sha=%t", s.Status, len(s.UpdatedAt), len(b), len(s.Reminder()), strings.Contains(s.Reminder(), s.ResultSHA256))
    if len(s.Reminder()) > MaxReminderBytes { t.Fatal("reminder exceeded 6KiB") }
}
