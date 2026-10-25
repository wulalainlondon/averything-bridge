package protocol

import "testing"

func TestParseInboundRestoresAuthorityScopedWireSessionIDs(t *testing.T) {
	in, err := ParseInbound([]byte(`{"type":"message","session_id":"sk1:wulala:local%3A1","session_ids":["sk1:wulala:a","sk1:wulala:b"]}`))
	if err != nil {
		t.Fatal(err)
	}
	if in.SessionID != "local:1" {
		t.Fatalf("session id=%q", in.SessionID)
	}
	if len(in.SessionIDs) != 2 || in.SessionIDs[0] != "a" || in.SessionIDs[1] != "b" {
		t.Fatalf("session ids=%v", in.SessionIDs)
	}
}
