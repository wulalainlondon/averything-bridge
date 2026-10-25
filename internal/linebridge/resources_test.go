package linebridge

import (
	"bytes"
	"context"
	"testing"
)

func TestResourcesFrozenEncryptedAndAccountScoped(t *testing.T) {
	store, s, _, owner := fixture(t)
	ctx := context.Background()
	a, _ := accountFixture(t, s, owner, "A")
	b, _ := accountFixture(t, s, owner, "B")
	input := []byte("private fixture file")
	s.config.Resources = func(context.Context, Caller, string) (ResourceInput, error) {
		return ResourceInput{Bytes: input, Name: "fixture.txt", MIME: "text/plain"}, nil
	}
	r, err := s.ImportResource(ctx, owner, a.ID, "known-attachment")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.Resource(ctx, owner, b.ID, r.Ref); err == nil {
		t.Fatal("resource crossed account")
	}
	if _, err = store.Resource(ctx, Caller{Subject: "device:other"}, a.ID, r.Ref); err == nil {
		t.Fatal("resource crossed caller")
	}
	input[0] = 'X'
	saved, err := store.Resource(ctx, owner, a.ID, r.Ref)
	if err != nil || string(saved.Data) != "private fixture file" {
		t.Fatal("resource not frozen", err)
	}
	var encoded []byte
	if err = store.db.QueryRow("SELECT data FROM line_resources WHERE ref=?", r.Ref).Scan(&encoded); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(encoded, []byte("private fixture")) {
		t.Fatal("plaintext resource in database")
	}
	if _, err = store.db.Exec("UPDATE line_resources SET data=? WHERE ref=?", []byte("corrupt"), r.Ref); err != nil {
		t.Fatal(err)
	}
	if _, err = store.Resource(ctx, owner, a.ID, r.Ref); err == nil {
		t.Fatal("corrupt resource accepted")
	}
}
