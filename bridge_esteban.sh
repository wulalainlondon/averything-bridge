#!/bin/bash
# Bridge instance on port 9453, no directory jail.
set -euo pipefail

BRIDGE_DIR="$(cd "$(dirname "$0")" && pwd)"
DATA_DIR="${HOME}/.claude-bridge-esteban"
PORT=9453
LOG_FILE="${DATA_DIR}/bridge_esteban.log"

export HOME="${HOME:-$(eval echo ~"$(id -un)")}"
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:${PATH:-}"
export BRIDGE_DISABLE_MDNS=1
export BRIDGE_TUNNEL_URL_FILE="${DATA_DIR}/tunnel_url.txt"

mkdir -p "$DATA_DIR"
ulimit -n 65536 2>/dev/null || ulimit -n 8192 2>/dev/null || true

echo "[esteban-bridge] $(date '+%F %T') starting on port $PORT (no jail)" >> "$LOG_FILE"

exec "$BRIDGE_DIR/venv/bin/python" "$BRIDGE_DIR/bridge_v2.py" \
  --port "$PORT" \
  --no-discovery \
  --data-dir "$DATA_DIR" \
  --root-dir "${HOME}/Desktop/Esteban" \
  --instance-name "esteban" \
  "$@"
