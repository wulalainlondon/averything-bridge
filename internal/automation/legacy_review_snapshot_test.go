package automation

import "testing"

func TestSnapshotProjectsLegacyReviewWithoutRewritingRoute(t *testing.T) {
	s, err := Open(t.TempDir(), "i1")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	route, _, err := s.UpsertRoute(t.Context(), Route{ID: "legacy", Name: "Legacy review", Enabled: true, SourcePattern: "judicial.ops.health", KindPattern: "ops.hourly", SessionID: "original-session", HandlingMode: AnalyzeForReview, RunKind: "research", ReviewOnly: true}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.ExecContext(t.Context(), `UPDATE external_event_routes SET handling_mode='review' WHERE id=?`, route.ID); err != nil {
		t.Fatal(err)
	}
	before, err := s.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(before.Routes) != 1 || before.Routes[0].HandlingMode != AnalyzeForReview {
		t.Fatalf("legacy route makes the whole phone snapshot invalid: %+v", before.Routes)
	}
	got := before.Routes[0]
	if got.ID != route.ID || got.SessionID != route.SessionID || got.Version != route.Version || !got.ReviewOnly || got.TrustedInstruction != route.TrustedInstruction {
		t.Fatal("snapshot projection changed original routing or policy")
	}
	var stored string
	if err = s.db.QueryRowContext(t.Context(), `SELECT handling_mode FROM external_event_routes WHERE id=?`, route.ID).Scan(&stored); err != nil || stored != "review" {
		t.Fatalf("original stored route rewritten: %q %v", stored, err)
	}
	if before.InboxVersion != 2 {
		t.Fatal("capability lost")
	}
}

func TestSnapshotKeepsCurrentHandlingModes(t *testing.T) {
	for _, mode := range []HandlingMode{NotifyOnly, AnalyzeForReview, DraftForReview, ApprovedAction} {
		t.Run(string(mode), func(t *testing.T) {
			s, err := Open(t.TempDir(), "i1")
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			_, _, err = s.UpsertRoute(t.Context(), Route{ID: "route", Name: "Current mode", Enabled: true, SourcePattern: "github", KindPattern: "issue", SessionID: "s", HandlingMode: mode, RunKind: "research", ReviewOnly: true}, 0)
			if err != nil {
				t.Fatal(err)
			}
			snap, err := s.Snapshot(t.Context())
			if err != nil || snap.Routes[0].HandlingMode != mode {
				t.Fatalf("current mode changed: %+v %v", snap, err)
			}
		})
	}
}
