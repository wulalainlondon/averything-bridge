// Package taskcontract fixes the runtime compiler and complete JSON Schema dialect.
package taskcontract

import (
	"bytes"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

const Version = "1.0.0-rc.1"
const ID = "urn:bridge:task-api:" + Version
const MaxBytes = 256 * 1024

//go:embed api-contract.schema.json
var Schema []byte

//go:embed golden.json
var Golden []byte

type Contract struct{ validators map[string]*jsonschema.Schema }

func New() (*Contract, error) {
	doc, err := Parse(Schema)
	if err != nil {
		return nil, err
	}
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)
	if err = compiler.AddResource(ID, doc); err != nil {
		return nil, err
	}
	root, err := compiler.Compile(ID)
	if err != nil {
		return nil, err
	}
	c := &Contract{validators: map[string]*jsonschema.Schema{"": root}}
	defs := doc.(map[string]any)["$defs"].(map[string]any)
	for name := range defs {
		validator, err := compiler.Compile(ID + "#/$defs/" + name)
		if err != nil {
			return nil, err
		}
		c.validators[name] = validator
	}
	return c, nil
}
func (c *Contract) Dialect() string { return "https://json-schema.org/draft/2020-12/schema" }
func Hash() string                  { sum := sha256.Sum256(Schema); return hex.EncodeToString(sum[:]) }
func (c *Contract) Validate(raw []byte, definition string) error {
	value, err := Parse(raw)
	if err != nil {
		return err
	}
	return c.ValidateValue(value, definition)
}
func (c *Contract) ValidateValue(value any, definition string) error {
	if err := wireNumbers(value); err != nil {
		return err
	}
	validator, ok := c.validators[definition]
	if !ok {
		return errors.New("unknown contract definition")
	}
	return validator.Validate(value)
}

// Task API wire fields are integers: both runtimes accept only integer JSON
// lexemes in the JS-safe range. This is a transport constraint, not a reduced
// JSON Schema dialect. Generic provider JSON parsing remains lossless.
func wireNumbers(value any) error {
	switch x := value.(type) {
	case json.Number:
		if strings.ContainsAny(string(x), ".eE") {
			return errors.New("noncanonical wire integer")
		}
		n, err := strconv.ParseInt(string(x), 10, 64)
		if err != nil || n > 9007199254740991 || n < -9007199254740991 {
			return errors.New("unsafe wire integer")
		}
	case float64:
		if x != float64(int64(x)) || x > 9007199254740991 || x < -9007199254740991 {
			return errors.New("unsafe wire integer")
		}
	case map[string]any:
		for _, item := range x {
			if err := wireNumbers(item); err != nil {
				return err
			}
		}
	case []any:
		for _, item := range x {
			if err := wireNumbers(item); err != nil {
				return err
			}
		}
	}
	return nil
}
func Validate2020(schema, instance []byte) error {
	doc, err := Parse(schema)
	if err != nil {
		return err
	}
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)
	if err = compiler.AddResource("urn:bridge:test-schema", doc); err != nil {
		return err
	}
	validator, err := compiler.Compile("urn:bridge:test-schema")
	if err != nil {
		return err
	}
	value, err := Parse(instance)
	if err != nil {
		return err
	}
	return validator.Validate(value)
}

// Parse rejects duplicate object keys (including escaped aliases), trailing values,
// oversized frames and excessive nesting before schema validation or normalization.
// UseNumber keeps integer identity lossless. No remote resource loader is installed.
func Parse(raw []byte) (any, error) {
	if len(raw) > MaxBytes {
		return nil, errors.New("frame too large")
	}
	if !json.Valid(raw) {
		return nil, errors.New("invalid JSON")
	}
	if !utf8.Valid(raw) || !pairedSurrogates(raw) {
		return nil, errors.New("invalid Unicode JSON")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	value, err := readValue(dec, 0)
	if err != nil {
		return nil, err
	}
	if _, err = dec.Token(); err != io.EOF {
		return nil, errors.New("trailing JSON")
	}
	return value, nil
}

// Reject invalid surrogate escapes instead of letting Go replace them while
// JS preserves them. This keeps normalized intents equivalent across runtimes.
func pairedSurrogates(raw []byte) bool {
	inString := false
	for i := 0; i < len(raw); i++ {
		if raw[i] == '"' {
			inString = !inString
			continue
		}
		if !inString || raw[i] != '\\' {
			continue
		}
		i++
		if i >= len(raw) {
			return false
		}
		if raw[i] != 'u' {
			continue
		}
		if i+4 >= len(raw) {
			return false
		}
		n, err := strconv.ParseUint(string(raw[i+1:i+5]), 16, 16)
		if err != nil {
			return false
		}
		i += 4
		if n >= 0xd800 && n <= 0xdbff {
			if i+6 >= len(raw) || raw[i+1] != '\\' || raw[i+2] != 'u' {
				return false
			}
			low, err := strconv.ParseUint(string(raw[i+3:i+7]), 16, 16)
			if err != nil || low < 0xdc00 || low > 0xdfff {
				return false
			}
			i += 6
		} else if n >= 0xdc00 && n <= 0xdfff {
			return false
		}
	}
	return true
}
func readValue(dec *json.Decoder, depth int) (any, error) {
	if depth > 64 {
		return nil, errors.New("JSON nesting exceeds limit")
	}
	token, err := dec.Token()
	if err != nil {
		return nil, err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return token, nil
	}
	switch delimiter {
	case '{':
		value := map[string]any{}
		for dec.More() {
			key, err := dec.Token()
			if err != nil {
				return nil, err
			}
			s, ok := key.(string)
			if !ok {
				return nil, errors.New("invalid object key")
			}
			if _, exists := value[s]; exists {
				return nil, errors.New("duplicate JSON key")
			}
			item, err := readValue(dec, depth+1)
			if err != nil {
				return nil, err
			}
			value[s] = item
		}
		_, err := dec.Token()
		return value, err
	case '[':
		value := []any{}
		for dec.More() {
			item, err := readValue(dec, depth+1)
			if err != nil {
				return nil, err
			}
			value = append(value, item)
		}
		_, err := dec.Token()
		return value, err
	default:
		return nil, errors.New("unexpected JSON delimiter")
	}
}

// ToolInputs returns the closed input schemas with local definitions for loading.
// Internal delivery has a contract but is never registered as a model tool.
func ToolInputs() (map[string]any, error) {
	doc, err := Parse(Schema)
	if err != nil {
		return nil, err
	}
	defs := doc.(map[string]any)["$defs"].(map[string]any)
	tools := map[string]any{}
	for name := range defs {
		if !strings.HasPrefix(name, "Request_") {
			continue
		}
		op := strings.TrimPrefix(name, "Request_")
		if op == "deliver_result" || op == "legacy_source" || op == "intake" {
			continue
		}
		tools["task_"+op] = map[string]any{"$schema": "https://json-schema.org/draft/2020-12/schema", "$defs": defs, "$ref": "#/$defs/" + name}
	}
	return tools, nil
}
func CompilerTag() string {
	return fmt.Sprintf("%s; github.com/santhosh-tekuri/jsonschema/v6@v6.0.2", "draft-2020-12")
}

// Shared transport integer constraint, independent of version definitions.
func ValidateWireNumbers(value any) error { return wireNumbers(value) }
