package callbacks

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestActiveInvitationPagesExcludeLargeHistoryAndPreserveFullReports(t *testing.T) {
	s, err := Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	insert := func(index int, state, device, binding string, enabled bool, expires int64, text string) string {
		id := fmt.Sprintf("cb_%048x", index)
		r := Record{ID: id, Authority: "a", Session: "s", Request: fmt.Sprint(index), Enabled: enabled, Recipient: Recipient{Device: device, Binding: binding}, Report: &Report{ID: "report", Text: text, Hash: Digest(text)}, Invite: &Invitation{ID: "inv_" + fmt.Sprint(index), State: state, Expires: expires}}
		if _, e := s.db.Exec("INSERT INTO callbacks VALUES(?,?,?,?)", id, r.Session, r.Request, encode(r)); e != nil {
			t.Fatal(e)
		}
		return id
	}
	for i := 0; i < 2050; i++ {
		insert(i, "ended", "device", "pair", true, 10000, "history")
	}
	one := insert(2050, "ringing", "device", "pair", true, 10000, strings.Repeat("a", 1<<20))
	two := insert(2051, "delivery_pending", "device", "pair", true, 10000, strings.Repeat("b", 1<<20))
	insert(2052, "ringing", "other-device", "pair", true, 10000, "foreign")
	insert(2053, "ringing", "device", "new-pair", true, 10000, "new identity")
	insert(2054, "ringing", "device", "pair", true, 1, "expired")
	insert(2055, "claimed", "device", "pair", true, 10000, "claimed")
	insert(2056, "ringing", "device", "pair", false, 10000, "disabled")
	first, next, err := s.ActiveInvitationPage(ctx, "device", "pair", "", 1, 500)
	if err != nil || len(first) != 1 || first[0].ID != one || next != one || first[0].Report != nil {
		t.Fatalf("first page: %v %v %v", first, next, err)
	}
	second, next, err := s.ActiveInvitationPage(ctx, "device", "pair", next, 1, 500)
	if err != nil || len(second) != 1 || second[0].ID != two || next != "" || second[0].Report != nil {
		t.Fatalf("second page: %v %v %v", second, next, err)
	}
	raw, _ := json.Marshal(append(first, second...))
	if len(raw) > 4096 || strings.Contains(string(raw), "new-pair") {
		t.Fatal("page contains report bytes or another binding")
	}
	original, found, err := s.Get(ctx, one)
	if err != nil || !found || len(original.Report.Text) != 1<<20 || original.Report.Hash != Digest(original.Report.Text) {
		t.Fatal("read-only page changed original report", err)
	}
	if _, _, err = s.ActiveInvitationPage(ctx, "device", "pair", "", 65, 500); err == nil {
		t.Fatal("unbounded page accepted")
	}
	if _, _, err = s.ActiveInvitationPage(ctx, "", "pair", "", 1, 500); err == nil {
		t.Fatal("unbound page accepted")
	}
}
