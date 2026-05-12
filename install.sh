#!/bin/bash
set -e

BRIDGE_DIR="/Users/wulala/Downloads/Helper/claude-bridge/bridge"
PLIST_NAME="com.wulala.claude-bridge.plist"
LAUNCH_AGENTS="$HOME/Library/LaunchAgents"

echo "==> Setting up venv..."
cd "$BRIDGE_DIR"
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip --quiet
pip install websockets --quiet
echo "    websockets installed: $(python -c 'import websockets; print(websockets.__version__)')"

echo "==> Installing launchd plist..."
mkdir -p "$LAUNCH_AGENTS"
cp "$BRIDGE_DIR/$PLIST_NAME" "$LAUNCH_AGENTS/$PLIST_NAME"

# Unload first in case it was already loaded (ignore error if not loaded)
launchctl unload "$LAUNCH_AGENTS/$PLIST_NAME" 2>/dev/null || true
launchctl load "$LAUNCH_AGENTS/$PLIST_NAME"

echo ""
echo "Bridge plist installed at: $LAUNCH_AGENTS/$PLIST_NAME"
echo "Log: $BRIDGE_DIR/bridge.log"
echo "Err: $BRIDGE_DIR/bridge.err"
echo ""

# Check if launchd started the bridge successfully
sleep 2
STATUS=$(launchctl list | grep "com.wulala.claude-bridge" | awk '{print $1}')
if [ "$STATUS" = "-" ]; then
    echo "NOTE: launchd loaded the plist but the bridge is not running (exit code 78)."
    echo "      This is expected on macOS 26 (Tahoe) — third-party LaunchAgents require"
    echo "      authorization via System Settings > General > Login Items & Extensions."
    echo "      Alternatively, start the bridge manually with:"
    echo "        cd $BRIDGE_DIR && source venv/bin/activate && python claude_bridge.py --port 8765"
    echo ""
    echo "Starting bridge manually for now..."
    nohup "$BRIDGE_DIR/venv/bin/python" "$BRIDGE_DIR/claude_bridge.py" --port 8765 \
        >> "$BRIDGE_DIR/bridge.log" 2>> "$BRIDGE_DIR/bridge.err" &
    echo "Bridge PID: $!"
    sleep 1
    if lsof -iTCP:8765 -sTCP:LISTEN &>/dev/null; then
        echo "Bridge is listening on port 8765"
    else
        echo "WARNING: bridge may not have started. Check bridge.log for details."
    fi
else
    echo "Bridge installed and running (PID $STATUS)"
fi
