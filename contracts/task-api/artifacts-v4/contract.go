package artifactcontract

import (
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	old "everything-go/contracts/task-api/artifacts-v3"
	base "everything-go/contracts/task-api/v1"
	"strings"
)

const Version = "1.0.0-rc.4"

//go:embed api-contract.schema.json
var Schema []byte

func Hash() string { s := sha256.Sum256(Schema); return hex.EncodeToString(s[:]) }
func Validate(raw []byte, definition string) error {
	// Same closed strict parser/dialect; this version has independent definitions.
	instance, err := base.Parse(raw)
	if err != nil {
		return err
	}
	if err = base.ValidateWireNumbers(instance); err != nil {
		return err
	}
	value, err := base.Parse(Schema)
	if err != nil {
		return err
	}
	doc := value.(map[string]any)
	doc["$ref"] = "#/$defs/" + definition
	encoded, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	return base.Validate2020(encoded, raw)
}
func ToolInputs() (map[string]any, error) {
	v, err := base.Parse(Schema)
	if err != nil {
		return nil, err
	}
	defs := v.(map[string]any)["$defs"]
	return map[string]any{"task_artifact_capabilities": map[string]any{"$schema": "https://json-schema.org/draft/2020-12/schema", "$defs": defs, "$ref": "#/$defs/Request_artifact_capabilities"}, "task_read_artifact_bytes": map[string]any{"$schema": "https://json-schema.org/draft/2020-12/schema", "$defs": defs, "$ref": "#/$defs/Request_read_artifact_bytes"}}, nil
}

func CompatibleToolInputs() (map[string]any, error) {
	definitions := map[string]any{}
	for _, v := range []struct {
		prefix string
		schema []byte
	}{{"v3_", old.Schema}, {"v4_", Schema}} {
		renamed := strings.ReplaceAll(string(v.schema), "#/$defs/", "#/$defs/"+v.prefix)
		raw, e := base.Parse([]byte(renamed))
		if e != nil {
			return nil, e
		}
		for key, value := range raw.(map[string]any)["$defs"].(map[string]any) {
			definitions[v.prefix+key] = value
		}
	}
	out := map[string]any{}
	for _, operation := range []string{"artifact_capabilities", "read_artifact_bytes"} {
		out["task_"+operation] = map[string]any{"$schema": "https://json-schema.org/draft/2020-12/schema", "type": "object", "$defs": definitions, "oneOf": []any{map[string]string{"$ref": "#/$defs/v3_Request_" + operation}, map[string]string{"$ref": "#/$defs/v4_Request_" + operation}}}
	}
	return out, nil
}
