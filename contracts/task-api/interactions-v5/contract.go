package interactioncontract

import (
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	base "everything-go/contracts/task-api/v1"
)

const Version = "1.0.0-rc.5"

//go:embed api-contract.schema.json
var Schema []byte

func Hash() string { s := sha256.Sum256(Schema); return hex.EncodeToString(s[:]) }
func Validate(raw []byte) error {
	v, e := base.Parse(raw)
	if e != nil {
		return e
	}
	if e = base.ValidateWireNumbers(v); e != nil {
		return e
	}
	return base.Validate2020(Schema, raw)
}
func ToolInput() map[string]any {
	var root map[string]any
	_ = json.Unmarshal(Schema, &root)
	root["type"] = "object"
	return root
}

func ValidateResponse(raw []byte) error {
	value, e := base.Parse(raw)
	if e != nil {
		return e
	}
	if e = base.ValidateWireNumbers(value); e != nil {
		return e
	}
	var root map[string]any
	if e = json.Unmarshal(Schema, &root); e != nil {
		return e
	}
	root["$ref"] = "#/$defs/Response"
	schema, e := json.Marshal(root)
	if e != nil {
		return e
	}
	return base.Validate2020(schema, raw)
}
