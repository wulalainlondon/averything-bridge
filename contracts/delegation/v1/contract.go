// Package delegationcontract publishes the parent-owned native tool contract.
package delegationcontract

import (
	_ "embed"
	"encoding/json"
	"errors"
	taskcontract "everything-go/contracts/task-api/v1"
)

//go:embed continuity.schema.json
var Schema []byte

func Tools() []map[string]any {
	var doc struct {
		Tools []map[string]any `json:"tools"`
	}
	if err := json.Unmarshal(Schema, &doc); err != nil {
		panic(err)
	}
	return doc.Tools
}

func Validate(name string, raw []byte) error {
	for _, tool := range Tools() {
		if tool["name"] == name {
			schema, e := json.Marshal(tool["inputSchema"])
			if e != nil {
				return e
			}
			return taskcontract.Validate2020(schema, raw)
		}
	}
	return errors.New("delegation_continuity_tool_unknown")
}
