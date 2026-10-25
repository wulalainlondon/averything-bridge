package artifactcontract

import (
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	base "everything-go/contracts/task-api/v1"
)

const Version = "1.0.0-rc.3"

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
