// Package metadatacontract owns the bounded Hub status/discovery projection.
package metadatacontract

import _ "embed"

const Version = "plugin-metadata-status.v1"

//go:embed status.schema.json
var Schema []byte
