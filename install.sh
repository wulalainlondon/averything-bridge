#!/bin/bash
set -euo pipefail

SRC_DIR="/Users/wulala/Downloads/Helper/claude-bridge/bridge"
RUNTIME_DIR="$HOME/.claude-bridge-runtime"
PLIST_NAME="com.wulala.claude-bridge.plist"
LAUNCH_AGENTS="$HOME/Library/LaunchAgents"
PLIST_PATH="$LAUNCH_AGENTS/$PLIST_NAME"

echo "==> Sync runtime files"
mkdir -p "$RUNTIME_DIR"
rsync -a --delete \
  --exclude '.git' \
  --exclude '__pycache__' \
  --exclude 'saved_sessions*.json' \
  --exclude 'session_meta.json' \
  --exclude 'bridge_v2.log' \
  --exclude 'bridge.log' \
  --exclude 'bridge.err' \
  --exclude 'bridge.pid' \
  --exclude 'venv/' \
  "$SRC_DIR/" "$RUNTIME_DIR/"
cd "$RUNTIME_DIR"

echo "==> Setup virtualenv and deps"
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip --quiet
pip install -r requirements.txt --quiet

chmod +x run_bridge.sh bridge_supervisor.sh bridge_healthcheck.py bridge_launch.sh

echo "==> Install launchd agent"
mkdir -p "$LAUNCH_AGENTS"

cat > "$PLIST_PATH" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>com.wulala.claude-bridge</string>
  <key>Program</key><string>/bin/bash</string>
  <key>ProgramArguments</key>
  <array>
    <string>/bin/bash</string>
    <string>-lc</string>
    <string>exec $RUNTIME_DIR/bridge_launch.sh</string>
  </array>
  <key>WorkingDirectory</key><string>$RUNTIME_DIR</string>
  <key>KeepAlive</key><true/>
  <key>RunAtLoad</key><true/>
  <key>StandardOutPath</key><string>/tmp/com.wulala.claude-bridge.stdout.log</string>
  <key>StandardErrorPath</key><string>/tmp/com.wulala.claude-bridge.stderr.log</string>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PATH</key><string>/Users/wulala/.npm-global/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin</string>
    <key>HOME</key><string>$HOME</string>
    <key>BRIDGE_PORT</key><string>8766</string>
    <key>BRIDGE_DISABLE_MDNS</key><string>1</string>
  </dict>
</dict>
</plist>
PLIST

launchctl bootout "gui/$(id -u)" "$PLIST_PATH" 2>/dev/null || true

# 確保舊 bridge 進程完全死亡，避免新 supervisor 搶 port
PORT="${BRIDGE_PORT:-8766}"
PIDS="$(lsof -ti :"$PORT" 2>/dev/null || true)"
if [[ -n "$PIDS" ]]; then
  echo "==> Killing existing process(es) on port $PORT: $PIDS"
  echo "$PIDS" | xargs kill -9 2>/dev/null || true
  sleep 1
fi

launchctl bootstrap "gui/$(id -u)" "$PLIST_PATH"
launchctl enable "gui/$(id -u)/com.wulala.claude-bridge" || true
launchctl kickstart -k "gui/$(id -u)/com.wulala.claude-bridge"

sleep 2

if lsof -nP -iTCP:8766 -sTCP:LISTEN >/dev/null; then
  echo "Bridge is healthy on :8766"
else
  echo "Bridge not listening on :8766 yet."
  echo "Check logs:"
  echo "  /tmp/com.wulala.bridge.stderr.log"
  echo "  /tmp/com.wulala.bridge.stdout.log"
  echo "  $RUNTIME_DIR/bridge_v2.log"
fi

echo "Installed: $PLIST_PATH"
echo "Runtime : $RUNTIME_DIR"
