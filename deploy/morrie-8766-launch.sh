#!/bin/bash
set -uo pipefail

# Keep Morrie's established Go data directory in place during the 9453 -> 8766
# cutover. The directory name is legacy; moving 2+ GiB of live databases is not
# required to change the listener and would add needless migration risk.
RUNTIME_DIR="/Users/morrie/.everything-go-runtime-9453"
BIN="$RUNTIME_DIR/Everything Go.app/Contents/MacOS/everything-go"
PORT="${EVERYTHING_GO_PORT:-8766}"
SESSION_STORE="${EVERYTHING_GO_SESSION_STORE:-/Users/morrie/.claude-bridge-runtime/saved_sessions.json}"
SERVICE_ACCOUNT="${EVERYTHING_GO_SERVICE_ACCOUNT:-/Users/morrie/.config/claude-bridge/serviceAccountKey.json}"
CLAUDE_BIN="${CLAUDE_BIN:-/Users/morrie/.local/bin/claude-pgwrap}"
CODEX_BIN="${CODEX_BIN:-/usr/local/bin/codex}"

export PATH="/Users/morrie/.bun/bin:/usr/local/bin:/opt/homebrew/bin:/Users/morrie/.local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
export EVERYTHING_GO_CODEX_SESSIONS_DIR="/Users/morrie/.codex/sessions"
export EVERYTHING_GO_CODEX_IGNORE_CWD_GLOBS="/private/tmp/**,/tmp/**"
export EVERYTHING_GO_CODEX_IGNORE_NAME_PREFIXES="<recommended_plugins>,[bridge-eval]"
export EVERYTHING_GO_CODEX_ROLLOVER_ENABLED="true"
export EVERYTHING_GO_CODEX_COLD_RESUME_MAX_BYTES="268435456"
export EVERYTHING_GO_CODEX_CHECKPOINT_MAX_BYTES="131072"
export EVERYTHING_GO_CODEX_APP_SERVER_MODE="daemon"
export EVERYTHING_GO_CODEX_APP_SERVER_SOCKET="/Users/morrie/.codex/app-server-control/app-server-control.sock"
export BRIDGE_RELAY_SECRET="$(<"$RUNTIME_DIR/relay_secret")"
export BRIDGE_RELAY_PEERS_JSON='[{"instance_id":"eg_a0ee076ad8a212fa6f9fbc97838f75f1","base_url":"http://100.69.175.49:8766","secret_ref":"env:BRIDGE_RELAY_SECRET"}]'

log() { echo "[everything-go-morrie-launch] $*"; }

if [[ ! -x "$BIN" ]]; then
  log "ERROR: signed app binary not found or not executable at $BIN"
  exit 1
fi

args=(
  --port "$PORT"
  --executor go
  --data-dir "$RUNTIME_DIR"
  --session-store "$SESSION_STORE"
  --instance-name morrie-everything-go
  --claude-bin "$CLAUDE_BIN"
  --codex-bin "$CODEX_BIN"
  --discovery
  --mdns
  --tunnel
)

if [[ -f "$SERVICE_ACCOUNT" ]]; then
  args+=(--service-account "$SERVICE_ACCOUNT")
fi

log "Starting signed Everything Go on :$PORT (data-dir=$RUNTIME_DIR)"
exec "$BIN" "${args[@]}"
