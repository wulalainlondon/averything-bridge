#!/bin/bash
# cloudflared_launcher.sh
# Run as a standalone launchd service (com.claude-bridge.cloudflared).
# Keeps cloudflared alive independently of bridge restarts.
# Writes tunnel URL to $BRIDGE_TUNNEL_URL_FILE; clears it on exit.
set -uo pipefail

DATA_DIR="${BRIDGE_DATA_DIR:-$HOME/.claude-bridge-runtime}"
PORT="${BRIDGE_PORT:-8766}"
URL_FILE="${BRIDGE_TUNNEL_URL_FILE:-$DATA_DIR/tunnel_url.txt}"
TIMEOUT_SECS="${BRIDGE_TUNNEL_TIMEOUT:-90}"

log() { echo "[cloudflared-launcher] $*"; }

# ── Clear stale URL so bridge knows there is no valid tunnel yet ────────────
rm -f "$URL_FILE"
log "Cleared stale tunnel_url.txt"

# ── Confirm binary ───────────────────────────────────────────────────────────
CFD="$(command -v cloudflared 2>/dev/null || true)"
if [[ -z "$CFD" ]]; then
  log "ERROR: cloudflared not found. Install: brew install cloudflared"
  # Exit 0 so launchd does not KeepAlive-storm when binary is simply missing.
  exit 0
fi
log "cloudflared: $($CFD --version 2>&1 | head -1)"

# ── Cleanup on exit: remove URL file so bridge stops advertising dead tunnel ─
cleanup() {
  log "Exiting — clearing $URL_FILE"
  rm -f "$URL_FILE"
}
trap cleanup EXIT INT TERM

# ── Launch cloudflared, capture output to a temp log ────────────────────────
TMPLOG="$(mktemp /tmp/cloudflared_launcher.XXXXXX)"
trap 'rm -f "$TMPLOG"; cleanup' EXIT INT TERM

log "Starting tunnel → http://localhost:$PORT (URL timeout: ${TIMEOUT_SECS}s)"
"$CFD" tunnel --url "http://localhost:$PORT" >"$TMPLOG" 2>&1 &
CFD_PID=$!

# ── Wait for URL (or timeout) ────────────────────────────────────────────────
URL_FOUND=0
ELAPSED=0
while [[ $ELAPSED -lt $TIMEOUT_SECS ]]; do
  # If cloudflared died early, no point waiting
  if ! kill -0 "$CFD_PID" 2>/dev/null; then
    log "cloudflared (pid=$CFD_PID) exited before providing URL"
    break
  fi

  MATCH="$(grep -o 'https://[a-zA-Z0-9.-]*\.trycloudflare\.com' "$TMPLOG" 2>/dev/null | head -1 || true)"
  if [[ -n "$MATCH" ]]; then
    WS_URL="${MATCH/https:/wss:}"
    printf '%s\n' "$WS_URL" > "$URL_FILE"
    log "Tunnel ready → $WS_URL"
    URL_FOUND=1
    break
  fi

  sleep 1
  ELAPSED=$((ELAPSED + 1))
done

# ── Drain tmp log to stdout for launchd logging ──────────────────────────────
cat "$TMPLOG" 2>/dev/null || true
rm -f "$TMPLOG"
trap 'cleanup' EXIT INT TERM   # restore simple cleanup (tmplog gone)

if [[ $URL_FOUND -eq 0 ]]; then
  log "No URL within ${TIMEOUT_SECS}s — killing cloudflared, launchd will retry"
  kill "$CFD_PID" 2>/dev/null || true
  wait "$CFD_PID" 2>/dev/null || true
  exit 1
fi

# ── URL obtained — wait for cloudflared to exit (crash/signal) ──────────────
# When it exits, this script exits too → launchd KeepAlive restarts us →
# we clear URL file, get a fresh URL, repeat.
log "Tunnel running (pid=$CFD_PID). Waiting…"
wait "$CFD_PID" || true
log "cloudflared exited — will restart"
