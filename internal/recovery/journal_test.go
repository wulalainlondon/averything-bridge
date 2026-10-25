package recovery

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestObservationJournalIsBoundedPrivateAndRestartReadable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "observations.json")
	j := &Journal{Path: path}
	for i := 0; i < MaxObservations+3; i++ {
		if err := j.Record(Observation{At: int64(i), SessionID: "s1", Failure: Failure{Category: ModelCapacity}, Decision: Decision{Mode: "observe_only"}}); err != nil {
			t.Fatal(err)
		}
	}
	// Simulate a fresh executor; the latest records survive.
	if err := (&Journal{Path: path}).Record(Observation{At: 999, SessionID: "s1"}); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var records []Observation
	if err := json.Unmarshal(b, &records); err != nil {
		t.Fatal(err)
	}
	if len(records) != MaxObservations || records[0].At != 4 || records[len(records)-1].At != 999 {
		t.Fatalf("lost/bad bounded ledger: %d", len(records))
	}
	st, err := os.Stat(path)
	if err != nil || st.Mode().Perm() != 0600 {
		t.Fatalf("journal not private: %v %v", st, err)
	}
}

func TestObservationJournalConcurrentWritesAndCorruptionPreservation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "observations.json")
	j := &Journal{Path: path}
	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func(at int) {
			defer wg.Done()
			if err := j.Record(Observation{At: int64(at)}); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	b, _ := os.ReadFile(path)
	var records []Observation
	if json.Unmarshal(b, &records) != nil || len(records) != 24 {
		t.Fatal("concurrent observations lost")
	}
	if err := os.WriteFile(path, []byte("corrupt evidence"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := j.Record(Observation{}); err == nil {
		t.Fatal("silently replaced corrupt evidence")
	}
	b, _ = os.ReadFile(path)
	if string(b) != "corrupt evidence" {
		t.Fatal("existing evidence overwritten")
	}
}
