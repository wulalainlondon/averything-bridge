package agentvisibility

import (
	"encoding/json"
	observationcontract "everything-go/contracts/internal-agent-observation/v1"
	taskcontract "everything-go/contracts/task-api/v1"
	"testing"
	"time"
)

func TestMetadataActualProjectionMatchesIndependentContract(t *testing.T) {
	for _, s := range []Snapshot{Unsupported(scope), observe(reader(), nil)} {
		raw, _ := json.Marshal(s)
		if err := taskcontract.Validate2020(observationcontract.Schema, raw); err != nil {
			t.Fatal(err)
		}
	}
	s := observe(reader(), nil)
	s.Children[0].State = "authoritative_qa_pass"
	raw, _ := json.Marshal(s)
	if taskcontract.Validate2020(observationcontract.Schema, raw) == nil {
		t.Fatal("unknown product state accepted")
	}
}

func TestMetadataOfflineRetainsTimestampWithoutQA(t *testing.T) {
	c := &Channel{}
	r := reader()
	out := c.Read(t.Context(), scope, r, nil, func() time.Time { return at }, time.Minute)
	offline := c.Offline(scope, at.Add(time.Second))
	if offline.Availability != "unknownstale" || offline.ObservedAt != out.ObservedAt || offline.AttemptedAt == offline.ObservedAt || offline.Children[0].Axes.Review != "unverified" {
		t.Fatal("cache renewed evidence")
	}
	changed := scope
	changed.Epoch = "new-epoch"
	if next := c.Offline(changed, at); next.Availability != "unsupported" || next.ObservedAt != "" || len(next.Children) != 0 {
		t.Fatal("old scope cache reused")
	}
}
