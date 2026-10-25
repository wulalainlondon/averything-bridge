package identity

import (
	"encoding/json"
	"os"
	"testing"
)

func TestSessionKeySharedVectors(t *testing.T) {
	data, err := os.ReadFile("../../contracts/v1/session_key_vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Version int `json:"version"`
		Vectors []struct {
			Authority string `json:"authority_instance_id"`
			SessionID string `json:"session_id"`
			Key       string `json:"session_key"`
		} `json:"vectors"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	if fixture.Version != SessionKeyVersion {
		t.Fatalf("fixture version=%d want=%d", fixture.Version, SessionKeyVersion)
	}
	for _, vector := range fixture.Vectors {
		key, err := MakeSessionKey(vector.Authority, vector.SessionID)
		if err != nil || key != vector.Key {
			t.Fatalf("MakeSessionKey(%q,%q)=%q err=%v want=%q", vector.Authority, vector.SessionID, key, err, vector.Key)
		}
		parsed, ok := ParseSessionKey(key)
		if !ok || parsed.AuthorityInstanceID != vector.Authority || parsed.SessionID != vector.SessionID {
			t.Fatalf("ParseSessionKey(%q)=%+v ok=%v", key, parsed, ok)
		}
	}
}

func TestSessionKeyCollisionAndLegacyWireID(t *testing.T) {
	left, _ := MakeSessionKey("wulala", "same")
	right, _ := MakeSessionKey("morrie", "same")
	if left == right {
		t.Fatal("different authorities collided")
	}
	if WireSessionID(left) != "same" || WireSessionID("legacy") != "legacy" {
		t.Fatal("wire id adapter did not preserve local ids")
	}
	if _, ok := ParseSessionKey("sk1::bad"); ok {
		t.Fatal("accepted malformed key")
	}
}
