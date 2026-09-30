package relay

import (
	"context"
	"strings"
	"testing"
	"time"
)

func validJob() Job {
	return Job{SchemaVersion: 1, ID: "job-1", OriginInstanceID: "wulala", TargetInstanceID: "morrie",
		EventID: "event-1", TargetWorkItemID: "wi-1", TargetSessionID: "session-1",
		Instruction: "analyze", ReviewOnly: true, CreatedAt: 1, ExpiresAt: time.Now().Add(time.Hour).UnixMilli(),
		Attachments: []Attachment{{ID: "attachment-1", Ordinal: 0, MIMEType: "image/png",
			DisplayName: "proof.png", SizeBytes: 10, SHA256: strings.Repeat("a", 64)}}}
}

func TestRelayStoreDeduplicatesRecoversAndCompletesByRequest(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	job := validJob()
	created, err := store.AcceptInbound(context.Background(), job)
	if err != nil || !created {
		t.Fatalf("created=%v err=%v", created, err)
	}
	created, err = store.AcceptInbound(context.Background(), job)
	if err != nil || created {
		t.Fatalf("duplicate created=%v err=%v", created, err)
	}
	record, ok, err := store.Claim(context.Background(), "inbound", "worker", time.Now().UnixMilli(), time.Minute)
	if err != nil || !ok || record.Attempt != 1 {
		t.Fatalf("record=%+v ok=%v err=%v", record, ok, err)
	}
	_ = store.Close()
	reloaded, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reloaded.Close()
	record, ok, err = reloaded.Claim(context.Background(), "inbound", "worker-2", time.Now().UnixMilli(), time.Minute)
	if err != nil || !ok || record.Attempt != 2 {
		t.Fatalf("recovered=%+v ok=%v err=%v", record, ok, err)
	}
	if err := reloaded.Update(context.Background(), job.ID, "inbound", "queued", "", "run-1", job.ID, "", 0); err != nil {
		t.Fatal(err)
	}
	id, changed, err := reloaded.CompleteInboundByRequest(context.Background(), job.ID, "succeeded", "", "recommended handling")
	if err != nil || !changed || id != job.ID {
		t.Fatalf("id=%s changed=%v err=%v", id, changed, err)
	}
	final, _ := reloaded.Get(context.Background(), job.ID, "inbound")
	if final.Status != "review_ready" || final.Result != "recommended handling" {
		t.Fatalf("final=%+v", final)
	}
}

func TestRelaySignatureRejectsTamperingAndExpiredTimestamp(t *testing.T) {
	secret := "relay-secret"
	body := []byte(`{"job":1}`)
	headers := Sign(secret, "wulala", "POST", "/api/relay/v1/jobs", body, time.Unix(1000, 0))
	input := map[string]string{"timestamp": headers["X-Bridge-Relay-Timestamp"], "nonce": headers["X-Bridge-Relay-Nonce"], "signature": headers["X-Bridge-Relay-Signature"]}
	if err := Verify(secret, "POST", "/api/relay/v1/jobs", body, input, time.Unix(1001, 0)); err != nil {
		t.Fatal(err)
	}
	if err := Verify(secret, "POST", "/api/relay/v1/jobs", []byte(`{"job":2}`), input, time.Unix(1001, 0)); err == nil {
		t.Fatal("tampered body passed signature verification")
	}
	if err := Verify(secret, "POST", "/api/relay/v1/jobs", body, input, time.Unix(1400, 0)); err == nil {
		t.Fatal("expired relay signature was accepted")
	}
}

func TestLoadPeersAcceptsOnlyTailscaleEndpoints(t *testing.T) {
	t.Setenv("BRIDGE_RELAY_PEERS_JSON", `[{"instance_id":"morrie","base_url":"http://100.75.252.64:9453","secret_ref":"env:RELAY_SECRET"}]`)
	peers, err := LoadPeers()
	if err != nil || peers["morrie"].BaseURL == "" {
		t.Fatalf("peers=%v err=%v", peers, err)
	}
	t.Setenv("BRIDGE_RELAY_PEERS_JSON", `[{"instance_id":"evil","base_url":"http://example.com:9453","secret_ref":"env:RELAY_SECRET"}]`)
	if _, err := LoadPeers(); err == nil {
		t.Fatal("public relay endpoint was accepted")
	}
}
