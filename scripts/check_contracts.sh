#!/bin/sh
set -eu
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
node "$ROOT/tools/generate_protocol_contract.mjs" --check
node "$ROOT/tools/check_protocol_inventory.mjs"
(cd "$ROOT/go" && go run ./cmd/collaboration-contract --root "$ROOT" --check)
(cd "$ROOT/go" && go test ./internal/identity ./internal/protocol)
