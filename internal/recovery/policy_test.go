package recovery

import "testing"

func TestRecoveryDecisionNeverAuthorizesReplay(t *testing.T) {
	yes, no := true, false
	for _, tc := range []struct {
		name           string
		f              Failure
		c              Context
		action, reason string
		eligible       bool
	}{
		{"ingress_candidate", Failure{Category: ModelCapacity, Source: "codex_ingress"}, Context{Owned: true, OrdinaryChat: true, Acceptance: Rejected, IngressRejected: true}, "observe_only", "verified_rejection_scheduler_not_enabled", true},
		{"conflicting_native_terminal", Failure{Category: ModelCapacity, Source: "codex_ingress"}, Context{Owned: true, OrdinaryChat: true, Acceptance: Rejected, IngressRejected: true, Terminal: true}, "reconcile", "conflicting_rejection_evidence", false},
		{"text_not_proof", Failure{Category: ModelCapacity, Source: "known_message"}, Context{Owned: true, OrdinaryChat: true, Acceptance: Rejected, IngressRejected: true}, "requires_user", "rejection_contract_not_verified", false},
		{"capacity_accepted", Failure{Category: ModelCapacity}, Context{Owned: true, OrdinaryChat: true, Acceptance: Accepted, Terminal: true}, "requires_user", "accepted_work_requires_reconciliation", false},
		{"timeout", Failure{Category: TransportUncertain}, Context{Owned: true, OrdinaryChat: true, Acceptance: AcceptanceUnknown}, "reconcile", "acceptance_unknown", false},
		{"upstream_retry", Failure{Category: ModelCapacity}, Context{Owned: true, OrdinaryChat: true, Acceptance: Accepted, NativeWillRetry: &yes}, "observe_upstream", "native_retry_in_progress", false},
		{"missing_retry_hint", Failure{Category: ModelCapacity}, Context{Owned: true, OrdinaryChat: true, Acceptance: Accepted}, "reconcile", "accepted_turn_not_terminal", false},
		{"false_is_not_terminal", Failure{Category: ModelCapacity}, Context{Owned: true, OrdinaryChat: true, Acceptance: Accepted, NativeWillRetry: &no}, "reconcile", "accepted_turn_not_terminal", false},
		{"terminal_supersedes_hint", Failure{Category: ModelCapacity}, Context{Owned: true, OrdinaryChat: true, Acceptance: Accepted, Terminal: true, NativeWillRetry: &yes}, "requires_user", "accepted_work_requires_reconciliation", false},
		{"permission_retry_hint", Failure{Category: PermissionRevoked}, Context{NativeWillRetry: &yes}, "requires_user", string(PermissionRevoked), false},
		{"quota", Failure{Category: UsageExhausted}, Context{Owned: true, OrdinaryChat: true, Acceptance: Rejected}, "requires_user", string(UsageExhausted), false},
		{"external", Failure{Category: ModelCapacity, Source: "codex_ingress"}, Context{OrdinaryChat: true, Acceptance: Rejected, IngressRejected: true}, "requires_user", "operation_not_eligible", false},
		{"async_reply", Failure{Category: ModelCapacity, Source: "codex_ingress"}, Context{Owned: true, Acceptance: Rejected, IngressRejected: true}, "requires_user", "operation_not_eligible", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := Decide(tc.f, tc.c)
			if d.Mode != "observe_only" || d.Action != tc.action || d.Reason != tc.reason || d.RetryEligible != tc.eligible {
				t.Fatalf("unsafe/wrong decision: %+v", d)
			}
		})
	}
}
