package recovery

type Acceptance string

const (
	AcceptanceUnknown Acceptance = "unknown"
	Accepted          Acceptance = "accepted"
	Rejected          Acceptance = "definitively_rejected"
)

type Context struct {
	Owned        bool       `json:"owned"`
	OrdinaryChat bool       `json:"ordinary_chat"`
	Acceptance   Acceptance `json:"acceptance"`
	// Only a verified ingress response establishes the P0 rejection contract.
	IngressRejected bool  `json:"ingress_rejected,omitempty"`
	NativeWillRetry *bool `json:"native_will_retry,omitempty"`
	Terminal        bool  `json:"terminal"`
}

type Decision struct {
	Mode          string `json:"mode"`
	Action        string `json:"action"`
	Reason        string `json:"reason"`
	RetryEligible bool   `json:"retry_eligible"`
}

// Decide is deliberately observe-only in P0. RetryEligible diagnoses candidates
// for the later persisted scheduler; it never authorizes a caller to replay.
func Decide(f Failure, c Context) Decision {
	d := Decision{Mode: "observe_only", Action: "requires_user", Reason: "unclassified_failure"}
	switch f.Category {
	case PermissionRevoked, PolicyBlocked, AuthenticationRequired, UsageExhausted, OwnershipConflict, ConfigurationError:
		d.Reason = string(f.Category)
		return d
	}
	if !c.Terminal && c.NativeWillRetry != nil && *c.NativeWillRetry {
		d.Action, d.Reason = "observe_upstream", "native_retry_in_progress"
		return d
	}
	if !c.Owned || !c.OrdinaryChat {
		d.Reason = "operation_not_eligible"
		return d
	}
	if c.Acceptance == AcceptanceUnknown || c.Acceptance == "" {
		d.Action, d.Reason = "reconcile", "acceptance_unknown"
		return d
	}
	if c.Acceptance == Accepted {
		if !c.Terminal {
			d.Action, d.Reason = "reconcile", "accepted_turn_not_terminal"
		} else {
			d.Reason = "accepted_work_requires_reconciliation"
		}
		return d
	}
	if c.Acceptance == Rejected && c.IngressRejected && f.Source == "codex_ingress" && f.Category == ModelCapacity {
		if c.Terminal || (c.NativeWillRetry != nil && *c.NativeWillRetry) {
			d.Action, d.Reason = "reconcile", "conflicting_rejection_evidence"
			return d
		}
		d.Action, d.Reason, d.RetryEligible = "observe_only", "verified_rejection_scheduler_not_enabled", true
		return d
	}
	d.Reason = "rejection_contract_not_verified"
	return d
}
