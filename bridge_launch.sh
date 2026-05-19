#!/bin/bash
set -euo pipefail

BRIDGE_DIR="$(cd "$(dirname "$0")" && pwd)"
SUPERVISOR="$BRIDGE_DIR/bridge_supervisor.sh"
LAUNCH_LOG="$BRIDGE_DIR/launchd-wrapper.log"

export HOME="${HOME:-$(eval echo ~"$(id -un)")}"
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:${PATH:-}"
export BRIDGE_PORT="${BRIDGE_PORT:-8766}"
export BRIDGE_DISABLE_MDNS="${BRIDGE_DISABLE_MDNS:-1}"

# Raise fd limit before spawning supervisor/python.  Plist HardResourceLimits
# is set to 65536; this ulimit is a belt-and-suspenders guarantee that the bash
# wrapper and all children inherit a high fd ceiling.
ulimit -n 65536 2>/dev/null || ulimit -n 8192 2>/dev/null || true

cd "$BRIDGE_DIR"

{
  echo "[launch] $(date '+%F %T') start bridge_launch.sh"
  echo "[launch] PATH=$PATH"
  echo "[launch] BRIDGE_PORT=$BRIDGE_PORT BRIDGE_DISABLE_MDNS=$BRIDGE_DISABLE_MDNS"
} >> "$LAUNCH_LOG"

exec "$SUPERVISOR"
