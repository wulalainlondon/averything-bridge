// Package observationcontract owns the independent metadata wire family.
// It is not an alias for TaskAPI, Continuation or context focus.
package observationcontract

import (
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
)

const Version = "internal-agent-observation.v1"

//go:embed observation.schema.json
var Schema []byte

func Hash() string { sum := sha256.Sum256(Schema); return hex.EncodeToString(sum[:]) }
