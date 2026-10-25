package protocol

import (
	"encoding/json"
	"os"
	"testing"
	"time"
)

func TestSharedNormalizationVectors(t *testing.T) {
	data, err := os.ReadFile("../../contracts/v3/normalization_vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var vectors struct {
		Endpoints []struct{ Input, Canonical string } `json:"endpoints"`
		Paths     []struct {
			Input, Canonical string
			Allowed          bool
		} `json:"paths"`
		Timestamps []struct {
			WireUnixMS int64  `json:"wire_unix_ms"`
			ISO        string `json:"iso"`
		} `json:"timestamps"`
	}
	if err := json.Unmarshal(data, &vectors); err != nil {
		t.Fatal(err)
	}
	for _, vector := range vectors.Endpoints {
		if got, ok := CanonicalEndpoint(vector.Input); !ok || got != vector.Canonical {
			t.Fatalf("endpoint %q got=(%q,%v)", vector.Input, got, ok)
		}
	}
	for _, vector := range vectors.Paths {
		if got, ok := CanonicalLocalPath(vector.Input); ok != vector.Allowed || got != vector.Canonical {
			t.Fatalf("path %q got=(%q,%v)", vector.Input, got, ok)
		}
	}
	for _, vector := range vectors.Timestamps {
		if got := time.UnixMilli(vector.WireUnixMS).UTC().Format("2006-01-02T15:04:05.000Z"); got != vector.ISO {
			t.Fatalf("timestamp got=%s want=%s", got, vector.ISO)
		}
	}
}
