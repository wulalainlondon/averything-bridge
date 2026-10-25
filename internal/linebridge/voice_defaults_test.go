package linebridge

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func enableVoiceDefaults(s *Service, a Account, allow func(string) bool) {
	s.config.SubjectAllowed = allow
	s.config.VoiceDispatchSubjectAllowed = allow
	s.config.VoiceDispatchAccounts = []VoiceDispatchAccount{{Account: a.ID, Binding: a.Revision}}
}
func TestVoiceDefaultsUsePinnedAccountAndDoNotCreateIndividualGrants(t *testing.T) {
	ctx := context.Background()
	store, s, _, owner := fixture(t)
	a, target := accountFixture(t, s, owner, "original")
	other, _ := accountFixture(t, s, owner, "not enabled")
	allowed := map[string]bool{owner.Subject: true, "session:voice-one": true, "session:voice-new": true, "device:paired-phone": true}
	enableVoiceDefaults(s, a, func(subject string) bool { return allowed[subject] })
	for _, subject := range []string{"session:voice-one", "session:voice-new", "device:paired-phone"} {
		c := Caller{Subject: subject}
		accounts, err := store.Accounts(ctx, c)
		if err != nil || len(accounts) != 1 || accounts[0].ID != a.ID {
			t.Fatal(accounts, err)
		}
		access, err := s.Access(ctx, c, a.ID)
		if err != nil || !access.VoiceDispatch.Enabled || access.CanManage || access.Role != "shared" || len(access.Scopes) != len(scopes) {
			t.Fatal(access, err)
		}
		for _, scope := range scopes {
			if !store.Allowed(ctx, c, a.ID, scope) {
				t.Fatal(subject, scope)
			}
		}
		if store.Allowed(ctx, c, other.ID, "status") {
			t.Fatal("another account inherited access")
		}
		var count int
		store.db.QueryRow("SELECT count(*) FROM line_grants WHERE account=? AND subject=?", a.ID, subject).Scan(&count)
		if count != 0 {
			t.Fatal("materialized per-subject grants")
		}
		if _, err = s.Grants(ctx, Caller{Subject: subject, Admin: true}, a.ID); err == nil {
			t.Fatal("inherited caller became owner")
		}
		if _, err = s.SetGrants(ctx, Caller{Subject: subject, Admin: true}, Request{Account: a.ID, Subject: owner.Subject, Scopes: scopes, Revision: 1}); err == nil {
			t.Fatal("inherited caller changed grants")
		}
	}
	// A caller can send under its own original subject. Other voices cannot read
	// that operation's private content even though both can use the same account.
	c := Caller{Subject: "session:voice-one"}
	op, err := s.Send(ctx, c, a.ID, target.Ref, Content{Text: "voice fixture"}, "original-voice-intent", 0, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := terminal(t, s, c, op.ID); got.State != "accepted_by_line" {
		t.Fatal(got)
	}
	if _, err = s.Receipt(ctx, Caller{Subject: "session:voice-new"}, op.ID); err == nil {
		t.Fatal("cross-conversation receipt leaked")
	}
}
func TestVoiceDefaultsRevocationBindingAndBlockedScopesFailClosed(t *testing.T) {
	for _, mode := range []string{"revoked-subject", "closed-session", "changed-binding", "disabled-account", "unpaired-account", "operator-disabled", "blocked-call"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			store, s, _, owner := fixture(t)
			a, _ := accountFixture(t, s, owner, "original")
			c := Caller{Subject: "session:voice"}
			enableVoiceDefaults(s, a, func(subject string) bool { return subject == c.Subject || subject == owner.Subject })
			switch mode {
			case "revoked-subject":
				s.config.SubjectAllowed = func(subject string) bool { return subject == owner.Subject }
			case "closed-session":
				s.config.VoiceDispatchSubjectAllowed = func(string) bool { return false }
			case "changed-binding":
				store.db.Exec("UPDATE line_accounts SET revision=revision+1 WHERE id=?", a.ID)
			case "disabled-account":
				store.db.Exec("UPDATE line_accounts SET enabled=0 WHERE id=?", a.ID)
			case "unpaired-account":
				store.db.Exec("UPDATE line_accounts SET principal=NULL WHERE id=?", a.ID)
			case "operator-disabled":
				s.config.VoiceDispatchAccounts = nil
			case "blocked-call":
				s.config.BlockedScopes = map[string]bool{"call": true}
			}
			if store.Allowed(ctx, c, a.ID, "call") {
				t.Fatal("voice default still permits call")
			}
			if mode == "blocked-call" && !store.Allowed(ctx, c, a.ID, "messages.read") {
				t.Fatal("unrelated supported read lost")
			}
			if store.Allowed(ctx, Caller{Subject: "device:unpaired"}, a.ID, "status") {
				t.Fatal("unknown device gained access")
			}
		})
	}
}
func TestVoiceDefaultScheduledSendSurvivesVoiceEndingButNotAccountPolicyRemoval(t *testing.T) {
	for _, revoke := range []bool{false, true} {
		t.Run(map[bool]string{false: "after-voice", true: "policy-revoked"}[revoke], func(t *testing.T) {
			ctx := context.Background()
			_, s, _, owner := fixture(t)
			a, target := accountFixture(t, s, owner, "original")
			c := Caller{Subject: "session:voice"}
			enableVoiceDefaults(s, a, func(subject string) bool { return subject == c.Subject || subject == owner.Subject })
			op, err := s.Send(ctx, c, a.ID, target.Ref, Content{Text: "scheduled"}, "voice-schedule-intent", nowMS()+40, "Asia/Taipei", 5000)
			if err != nil {
				t.Fatal(err)
			}
			// No active voice exists in this store/service. Authorization comes from the
			// user's persistent operator policy, not a short-lived microphone lease.
			time.Sleep(50 * time.Millisecond)
			if revoke {
				s.config.VoiceDispatchAccounts = nil
			}
			s.Tick(ctx, nowMS())
			deadline := time.Now().Add(time.Second)
			for time.Now().Before(deadline) {
				got, _ := s.store.Operation(ctx, op.ID)
				if !revoke && got.State == "accepted_by_line" {
					return
				}
				if revoke && got.State == "held_unavailable" {
					return
				}
				time.Sleep(time.Millisecond)
			}
			got, _ := s.store.Operation(ctx, op.ID)
			t.Fatal(got)
		})
	}
}
func TestVoiceDefaultConfigurationRejectsUnknownDuplicateAndInvalidFields(t *testing.T) {
	account := "ln_" + strings.Repeat("1", 32)
	for _, raw := range []string{`{"account_ref":"` + account + `","binding_revision":28,"all_accounts":true}`, `{"account_ref":"` + account + `","binding_revision":1,"binding_revision":28}`, `null`, `[]`} {
		var v VoiceDispatchAccount
		if json.Unmarshal([]byte(raw), &v) == nil {
			t.Fatal("accepted malformed operator policy", raw)
		}
	}
	for _, items := range [][]VoiceDispatchAccount{{{Account: account, Binding: 0}}, {{Account: account, Binding: 1}, {Account: account, Binding: 1}}, {{Account: "*", Binding: 1}}} {
		if ValidateVoiceDispatchAccounts(items) == nil {
			t.Fatal(items)
		}
	}
	if ValidateVoiceDispatchAccounts([]VoiceDispatchAccount{{Account: account, Binding: 28}}) != nil {
		t.Fatal("valid policy refused")
	}
}
