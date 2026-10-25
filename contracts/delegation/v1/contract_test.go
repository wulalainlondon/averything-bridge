package delegationcontract

import "testing"

func TestContinuationWireContractRejectsExpansionAndMalformedReplay(t *testing.T) {
	valid := `{"delegation_id":"dg_1","expected_previous_round_id":"dg_1","instance_id":"i1","expected_thread_id":"thread1","expected_config_revision":0,"content":"fix","mode":"queue"}`
	if e := Validate("continue_delegation", []byte(valid)); e != nil {
		t.Fatal(e)
	}
	for _, raw := range []string{valid + `{}`, `{"delegation_id":"dg_1","expected_previous_round_id":"dg_1","instance_id":"i1","expected_thread_id":"thread1","expected_config_revision":0,"content":"fix","mode":"steer"}`, `{"delegation_id":"dg_1","expected_previous_round_id":"dg_1","instance_id":"i1","expected_thread_id":"thread1","expected_config_revision":0,"content":"fix","mode":"queue","sandbox":"danger-full-access"}`, `{"delegation_id":"dg_1","delegation_id":"dg_foreign"}`} {
		if e := Validate("continue_delegation", []byte(raw)); e == nil {
			t.Fatal("unsafe wire accepted", raw)
		}
	}
}
