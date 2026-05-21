#!/bin/bash
# Restart agent — started by launchd WatchPaths, NOT a child of bridge.
# Triggered when bridge writes .restart-trigger (via restart_bridge WS command).
# Because this script's parent is launchd (PID 1), install.sh's pre-flight
# check passes and the bridge restarts cleanly.
set -euo pipefail

RUNTIME_DIR="$HOME/.claude-bridge-runtime"
TRIGGER="$RUNTIME_DIR/.restart-trigger"
SERVICE_LABEL_FILE="$RUNTIME_DIR/.service-label"

SERVICE_LABEL="$(cat "$SERVICE_LABEL_FILE" 2>/dev/null || echo 'com.claude-bridge.app')"

# Remove trigger BEFORE restarting to prevent re-trigger on bridge startup.
rm -f "$TRIGGER"

# Brief delay so bridge can flush its response before dying.
sleep 1

echo "[restart-agent] kickstarting $SERVICE_LABEL"
launchctl kickstart -k "gui/$(id -u)/$SERVICE_LABEL"
