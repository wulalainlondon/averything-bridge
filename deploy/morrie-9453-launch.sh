#!/bin/bash
set -uo pipefail

# Deployment invariant: Morrie is an Intel Mac (x86_64 / GOARCH=amd64).
# A correctly signed arm64 app still fails here with "Bad CPU type"; always
# verify the packaged executable architecture before replacing this runtime.

RUNTIME_DIR="/Users/morrie/.everything-go-runtime-9453"
BIN="$RUNTIME_DIR/Everything Go.app/Contents/MacOS/everything-go"
PORT="${EVERYTHING_GO_PORT:-9453}"
SESSION_STORE="${EVERYTHING_GO_SESSION_STORE:-/Users/morrie/.claude-bridge-runtime/saved_sessions.json}"
SERVICE_ACCOUNT="${EVERYTHING_GO_SERVICE_ACCOUNT:-/Users/morrie/.config/claude-bridge/serviceAccountKey.json}"
CLAUDE_BIN="${CLAUDE_BIN:-/Users/morrie/.local/bin/claude-pgwrap}"
CODEX_BIN="${CODEX_BIN:-/usr/local/bin/codex}"

export PATH="/Users/morrie/.bun/bin:/usr/local/bin:/opt/homebrew/bin:/Users/morrie/.local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
export EVERYTHING_GO_CODEX_SESSIONS_DIR="/Users/morrie/.codex/sessions"
export EVERYTHING_GO_CODEX_IGNORE_CWD_GLOBS="/private/tmp/**,/tmp/**"
export EVERYTHING_GO_CODEX_IGNORE_NAME_PREFIXES="<recommended_plugins>,[bridge-eval]"
export EVERYTHING_GO_CODEX_APP_SERVER_MODE="daemon"
export EVERYTHING_GO_CODEX_APP_SERVER_SOCKET="/Users/morrie/.codex/app-server-control/app-server-control.sock"

log() { echo "[everything-go-9453-launch] $*"; }

if [[ ! -x "$BIN" ]]; then
  log "ERROR: binary not found/executable at $BIN"
  exit 0
fi

args=(
  --port "$PORT"
  --executor go
  --data-dir "$RUNTIME_DIR"
  --session-store "$SESSION_STORE"
  --instance-name morrie-everything-go-9453
  --claude-bin "$CLAUDE_BIN"
  --codex-bin "$CODEX_BIN"
)

if [[ -f "$SERVICE_ACCOUNT" ]]; then
  args+=(--service-account "$SERVICE_ACCOUNT")
fi

log "Starting everything-go on :$PORT (data-dir=$RUNTIME_DIR)"
exec "$BIN" "${args[@]}"
