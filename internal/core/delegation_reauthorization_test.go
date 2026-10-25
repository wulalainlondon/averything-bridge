package core

import (
	"context"
	"everything-go/internal/delegation"
	"everything-go/internal/session"
	"fmt"
	"testing"
)

func TestReauthorizationExplicitPairKeepsOriginalBindingsAndQAHistory(t *testing.T) {
	h, p, _ := continuityFixture(t)
	owner, first := continuityDelegate(t, h, p, "reauth-owner", "first owner")
	qa, failed := continuityDelegate(t, h, p, "reauth-qa", fmt.Sprintf(`{"owner_round_id":%q,"verdict":"FAIL"}`, owner.ID))
	originalThread, originalRevision, _, _ := h.delegations.SourceBinding(context.Background(), owner.ID)
	continuityDeliver(t, h, p, first, failed)
	if _, e := p.SetFutureConfiguration(session.ConfigurationFrom(p.SettingsSnapshot()), nil); e != nil {
		t.Fatal(e)
	}
	continuityParentTurn(t, p, "r_reauthorization")
	bind := map[string]any{"owner_delegation_id": owner.ID, "qa_delegation_id": qa.ID}
	if r := continuityCompatCall(t, h, p, "append", "before-reauth-0001", "bind_qa", bind); r.OK {
		t.Fatal("implicit revision adoption")
	}
	os, _ := h.registry.Get(owner.ChildSessionID)
	qs, _ := h.registry.Get(qa.ChildSessionID)
	in := delegationReauthorization{Owner: owner.ID, QA: qa.ID, Instance: h.cfg.InstanceID, SourceThread: p.ResumeID(), From: originalRevision, To: originalRevision + 1, OwnerThread: os.ResumeID(), QAThread: qs.ResumeID(), OwnerRevision: os.SettingsSnapshot().ConfigRevision, QARevision: qs.SettingsSnapshot().ConfigRevision}
	continuityCompatData(t, continuityCompatCall(t, h, p, "append", "reauth-pair-00001", "reauthorize_pair", in))
	continuityCompatData(t, continuityCompatCall(t, h, p, "append", "reauth-pair-00001", "reauthorize_pair", in))
	continuityCompatData(t, continuityCompatCall(t, h, p, "append", "bind-after-00001", "bind_qa", bind))
	continuityCompatData(t, continuityCompatCall(t, h, p, "append", "fail-after-00001", "record_review", map[string]any{"owner_round_id": owner.ID, "qa_round_id": qa.ID}))
	fixed := continuityContinue(t, h, p, "fix-reauth", continuityInput(first, p, h, "corrected"))
	checked := continuityContinue(t, h, p, "qa-reauth", continuityInput(failed, p, h, fmt.Sprintf(`{"owner_round_id":%q,"verdict":"PASS"}`, fixed.ID)))
	continuityCompatData(t, continuityCompatCall(t, h, p, "append", "pass-after-00001", "record_review", map[string]any{"owner_round_id": fixed.ID, "qa_round_id": checked.ID}))
	if fixed.ChildSessionID != owner.ChildSessionID || checked.ChildSessionID != qa.ChildSessionID || os.ResumeID() != in.OwnerThread || qs.ResumeID() != in.QAThread {
		t.Fatal("context changed")
	}
	for _, id := range []string{owner.ID, qa.ID} {
		thread, revision, _, e := h.delegations.SourceBinding(context.Background(), id)
		if e != nil || thread != originalThread || revision != originalRevision {
			t.Fatal("old binding overwritten", thread, revision, e)
		}
	}
	_, revision, _, _ := h.delegations.SourceBinding(context.Background(), fixed.ID)
	if revision != in.To {
		t.Fatal("new round lacks current epoch")
	}
	if old := capacityRecordFor(t, h, first.ID); old.Result != first.Result || old.ParentRequestID != first.ParentRequestID {
		t.Fatal("old result/return changed")
	}
	for _, v := range []struct{ id, want string }{{first.ID, "FAIL"}, {fixed.ID, "PASS"}} {
		_, got, e := h.delegations.QAForRound(context.Background(), v.id)
		if e != nil || got != v.want {
			t.Fatal("QA history", got, e)
		}
	}
	continuityDeliver(t, h, p, fixed, checked)
	reopened, e := delegation.Open(h.cfg.DataDir)
	if e != nil {
		t.Fatal(e)
	}
	defer reopened.Close()
	epoch, found, e := reopened.SourceEpoch(context.Background(), owner.ID)
	if e != nil || !found || epoch.Epoch != 1 || epoch.ToRevision != in.To {
		t.Fatal("epoch not durable", epoch, e)
	}
	if counts, e := h.delegations.CountsForParent(context.Background(), p.ID); e != nil || counts != (delegation.Counts{}) {
		t.Fatal("reservation leak", counts, e)
	}
	continuityParentTurn(t, p, "r_later_replay")
	continuityCompatData(t, continuityCompatCall(t, h, p, "append", "reauth-pair-00001", "reauthorize_pair", in))
}

func TestReauthorizationRejectsDriftAndPartialPair(t *testing.T) {
	for _, kind := range []string{"wrong_authority", "wrong_source_thread", "wrong_child_thread", "wrong_child_revision", "wrong_previous_revision", "wrong_new_revision", "same_child", "foreign_child", "child_profile", "pending_delivery", "parent_sandbox", "changed_parent_thread"} {
		t.Run(kind, func(t *testing.T) {
			h, p, _ := continuityFixture(t)
			owner, first := continuityDelegate(t, h, p, "deny-owner", "owner")
			qa, failed := continuityDelegate(t, h, p, "deny-qa", "qa")
			old := p.SettingsSnapshot().ConfigRevision
			if kind != "pending_delivery" {
				continuityDeliver(t, h, p, first, failed)
			} else {
				p.EndTurn()
			}
			if _, e := p.SetFutureConfiguration(session.ConfigurationFrom(p.SettingsSnapshot()), nil); e != nil {
				t.Fatal(e)
			}
			continuityParentTurn(t, p, "r_denied_reauth")
			os, _ := h.registry.Get(owner.ChildSessionID)
			qs, _ := h.registry.Get(qa.ChildSessionID)
			in := delegationReauthorization{Owner: owner.ID, QA: qa.ID, Instance: h.cfg.InstanceID, SourceThread: p.ResumeID(), From: old, To: old + 1, OwnerThread: os.ResumeID(), QAThread: qs.ResumeID(), OwnerRevision: os.SettingsSnapshot().ConfigRevision, QARevision: qs.SettingsSnapshot().ConfigRevision}
			switch kind {
			case "wrong_authority":
				in.Instance = "foreign"
			case "wrong_source_thread":
				in.SourceThread = "foreign"
			case "wrong_child_thread":
				in.QAThread = "foreign"
			case "wrong_child_revision":
				in.QARevision++
			case "wrong_previous_revision":
				in.From++
			case "wrong_new_revision":
				in.To++
			case "same_child":
				in.QA = in.Owner
			case "foreign_child":
				in.QA = "dg_foreign"
			case "child_profile":
				qs.SetModel("changed-model")
			case "parent_sandbox":
				snap := p.SettingsSnapshot()
				snap.Sandbox = "read-only"
				p.RestoreFutureConfiguration(snap)
			case "changed_parent_thread":
				p.SetResumeID("new-parent-thread")
				in.SourceThread = p.ResumeID()
			}
			r := continuityCompatCall(t, h, p, "append", "denied-reauth-0001", "reauthorize_pair", in)
			if r.OK {
				t.Fatal("invalid reauthorization accepted")
			}
			for _, id := range []string{owner.ID, qa.ID} {
				if _, found, e := h.delegations.SourceEpoch(context.Background(), id); e != nil || found {
					t.Fatal("partial reauthorization", id, e)
				}
			}
		})
	}
}
