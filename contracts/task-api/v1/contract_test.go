package taskcontract

import (
	"encoding/json"
	"testing"
)

func TestSharedFullDialectVectors(t *testing.T) {
	c, err := New()
	if err != nil {
		t.Fatal(err)
	}
	var vectors []struct {
		Name       string          `json:"name"`
		Valid      bool            `json:"valid"`
		Definition string          `json:"definition"`
		Value      json.RawMessage `json:"value"`
		Raw        *string         `json:"raw"`
	}
	if err := json.Unmarshal(Golden, &vectors); err != nil {
		t.Fatal(err)
	}
	for _, v := range vectors {
		t.Run(v.Name, func(t *testing.T) {
			raw := []byte(v.Value)
			if v.Raw != nil {
				raw = []byte(*v.Raw)
			}
			err := c.Validate(raw, v.Definition)
			if (err == nil) != v.Valid {
				t.Fatalf("valid=%v error=%v", v.Valid, err)
			}
		})
	}
}

func TestStrictJSONRejectsDuplicateAndTrailing(t *testing.T) {
	for _, raw := range []string{`{"x":1,"x":2}`, `{"x":{"y":1,"\u0079":2}}`, `[{"x":1,"x":2}]`, `{} {}`, `{"x":NaN}`, `[1,]`} {
		if _, err := Parse([]byte(raw)); err == nil {
			t.Errorf("accepted invalid JSON: %s", raw)
		}
	}
	if _, err := Parse([]byte(`{"x":[1,2],"y":"中文🙂"}`)); err != nil {
		t.Fatal(err)
	}
}

func TestCompilerIsFullDraft2020(t *testing.T) {
	c, err := New()
	if err != nil {
		t.Fatal(err)
	}
	if c.Dialect() != "https://json-schema.org/draft/2020-12/schema" {
		t.Fatal(c.Dialect())
	}
	// Compile and validate a 2020-only vocabulary keyword, not an AJV6 subset.
	if err := Validate2020([]byte(`{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"array","prefixItems":[{"const":1}],"items":false}`), []byte(`[1,2]`)); err == nil {
		t.Fatal("prefixItems/items not enforced")
	}
	if err := Validate2020([]byte(`{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"array","prefixItems":[{"const":1}],"items":false}`), []byte(`[1]`)); err != nil {
		t.Fatal(err)
	}
	if err := Validate2020([]byte(`{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"nonsense"}`), []byte(`{}`)); err == nil {
		t.Fatal("invalid schema not rejected")
	}
}
