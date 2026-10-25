package core

import (
	"bytes"
	"context"
	"encoding/json"
	"everything-go/internal/taskapi"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"os"
	"path/filepath"
	"testing"
)

func TestNF01LegacyStrictSnapshotOmissionAndOptInShape(t *testing.T) {
	// This exact old contract is a test fixture exported from sealed7e (not used
	// as an authority or a guessed native origin).
	schema, e := os.ReadFile(filepath.Join("testdata", "nf01-legacy-schema.json"))
	if e != nil {
		t.Fatal(e)
	}
	var raw any
	if json.Unmarshal(schema, &raw) != nil {
		t.Fatal("fixture invalid")
	}
	comp := jsonschema.NewCompiler()
	comp.DefaultDraft(jsonschema.Draft2020)
	if e = comp.AddResource("urn:nf01:legacy", raw); e != nil {
		t.Fatal(e)
	}
	validator, e := comp.Compile("urn:nf01:legacy#/$defs/Snapshot")
	if e != nil {
		t.Fatal(e)
	}
	h, _ := newTestHub(t)
	// The actual adapter strips extension unless the request explicitly opts in.
	for _, opt := range []bool{false, true} {
		input, _ := json.Marshal(map[string]any{"session_id": "s_fixture", "views": []string{"source_children"}, "limit": 10, "include_relation_coverage": opt})
		valueResult, readErr := h.Read(context.Background(), taskapi.AuthorizedCommand{Caller: taskapi.VerifiedContext{Authority: "i1", StableScopeID: "fixture-native-scope", BindingKind: "paired_human"}, Request: taskapi.Request{Operation: "snapshot", Input: input}, Revalidate: func(context.Context) error { return nil }})
		if readErr != nil {
			t.Fatal(readErr)
		}
		candidate := valueResult

		b, _ := json.Marshal(candidate)
		d := json.NewDecoder(bytes.NewReader(b))
		d.UseNumber()
		var value any
		d.Decode(&value)
		err := validator.Validate(value)
		if opt == (err == nil) {
			t.Fatal("legacy strict shape negotiation wrong", opt, err)
		}
	}
}
