#!/bin/bash
set -euo pipefail

SRC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNTIME_DIR="$HOME/.claude-bridge-runtime"
SERVICE_LABEL="${BRIDGE_SERVICE_LABEL:-com.claude-bridge.app}"
PLIST_NAME="${SERVICE_LABEL}.plist"
LAUNCH_AGENTS="$HOME/Library/LaunchAgents"
PLIST_PATH="$LAUNCH_AGENTS/$PLIST_NAME"
SERVICE_TARGET="gui/$(id -u)/$SERVICE_LABEL"
DOMAIN_TARGET="gui/$(id -u)"

# ============================================================================
# Pre-flight: refuse to run from inside the bridge's own process tree.
#
# Why: install.sh ultimately restarts the bridge service.  If launched from a
# subprocess of bridge_v2.py (e.g. a Codex / Claude backend exec_command), the
# restart will SIGKILL this very script mid-execution, leaving the deploy
# half-done.  This caused a real outage on 2026-05-18 01:37.
# ============================================================================
check_not_inside_bridge() {
  local pid="$PPID"
  local depth=0
  while [[ "$pid" -gt 1 && "$depth" -lt 20 ]]; do
    local cmd
    cmd="$(ps -o command= -p "$pid" 2>/dev/null || true)"
    if [[ "$cmd" == *"bridge_v2.py"* ]] \
       || [[ "$cmd" == *"bridge_supervisor.sh"* ]] \
       || [[ "$cmd" == *"bridge_launch.sh"* ]]; then
      echo "ERROR: install.sh is running inside the bridge's own process tree." >&2
      echo "       Restarting the bridge here would SIGKILL this script." >&2
      echo "       Offending ancestor: pid=$pid cmd=$cmd" >&2
      echo "       Run install.sh from a separate terminal (not from a bridge" >&2
      echo "       backend's exec_command)." >&2
      exit 2
    fi
    pid="$(ps -o ppid= -p "$pid" 2>/dev/null | tr -d ' ' || echo 1)"
    [[ -z "$pid" ]] && break
    depth=$((depth+1))
  done
}
check_not_inside_bridge

# ============================================================================
# Sync source -> runtime.  Exclude ALL state files so a re-deploy never
# clobbers anything the running bridge owns.
# ============================================================================
echo "==> Sync runtime files"
mkdir -p "$RUNTIME_DIR"
rsync -a --delete \
  --exclude '.git' \
  --exclude '__pycache__' \
  --exclude 'venv/' \
  --exclude 'saved_sessions*.json' \
  --exclude 'session_meta.json' \
  --exclude 'read_cursors.json' \
  --exclude 'path_overrides.json' \
  --exclude 'fcm_token.txt' \
  --exclude 'serviceAccountKey.json' \
  --exclude 'bridge_v2.log' \
  --exclude 'bridge.log' \
  --exclude 'bridge.err' \
  --exclude 'bridge.pid' \
  --exclude 'supervisor.log' \
  --exclude 'supervisor.pid' \
  --exclude '.supervisor.lock' \
  --exclude '.requirements.sha256' \
  --exclude 'search.db' \
  --exclude 'search.db-shm' \
  --exclude 'search.db-wal' \
  --exclude 'launchd-wrapper.log' \
  "$SRC_DIR/" "$RUNTIME_DIR/"
cd "$RUNTIME_DIR"

# ============================================================================
# Setup venv.  Only --force-reinstall when requirements.txt actually changed.
# This avoids burning 30+ seconds on every deploy.
# ============================================================================
echo "==> Setup virtualenv and deps"
if [[ ! -d venv ]]; then
  python3 -m venv venv
fi
# shellcheck disable=SC1091
source venv/bin/activate
pip install --upgrade pip --quiet

REQ_HASH_FILE="$RUNTIME_DIR/.requirements.sha256"
REQ_HASH_NEW="$(shasum -a 256 requirements.txt | awk '{print $1}')"
REQ_HASH_OLD="$(cat "$REQ_HASH_FILE" 2>/dev/null || echo '')"

if [[ "$REQ_HASH_NEW" != "$REQ_HASH_OLD" ]]; then
  echo "    requirements.txt changed, reinstalling deps"
  pip install -r requirements.txt --quiet --force-reinstall
  echo "$REQ_HASH_NEW" > "$REQ_HASH_FILE"
else
  echo "    requirements.txt unchanged, skipping reinstall"
  # Still run a normal install in case venv is missing something
  pip install -r requirements.txt --quiet
fi

chmod +x run_bridge.sh bridge_supervisor.sh bridge_healthcheck.py bridge_launch.sh

# ============================================================================
# Write plist into a temp path; only swap if content actually differs.
# This lets us avoid bootout/bootstrap on every deploy.
# ============================================================================
echo "==> Update launchd agent"
mkdir -p "$LAUNCH_AGENTS"

PLIST_TMP="$(mktemp -t claude-bridge-plist.XXXXXX)"
cat > "$PLIST_TMP" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>$SERVICE_LABEL</string>
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
  <key>ThrottleInterval</key><integer>30</integer>
  <key>StandardOutPath</key><string>/tmp/$SERVICE_LABEL.stdout.log</string>
  <key>StandardErrorPath</key><string>/tmp/$SERVICE_LABEL.stderr.log</string>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PATH</key><string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin</string>
    <key>HOME</key><string>$HOME</string>
    <key>BRIDGE_PORT</key><string>8766</string>
    <key>BRIDGE_DISABLE_MDNS</key><string>1</string>
  </dict>
  <key>SoftResourceLimits</key>
  <dict>
    <key>NumberOfFiles</key>
    <integer>65536</integer>
  </dict>
  <key>HardResourceLimits</key>
  <dict>
    <key>NumberOfFiles</key>
    <integer>65536</integer>
  </dict>
</dict>
</plist>
PLIST

PLIST_CHANGED=false
if [[ ! -f "$PLIST_PATH" ]] || ! cmp -s "$PLIST_TMP" "$PLIST_PATH"; then
  PLIST_CHANGED=true
  mv "$PLIST_TMP" "$PLIST_PATH"
  echo "    plist content changed"
else
  rm -f "$PLIST_TMP"
  echo "    plist unchanged"
fi

# ============================================================================
# Restart service.  Strategy:
#
#   - If plist content changed OR service not loaded: bootout + bootstrap
#     (full re-register; only path that picks up plist changes).
#   - Otherwise:                                       kickstart -k
#     (hot restart; KeepAlive respawns; no caller-tree-kill side-effect).
#
# We never use bootout when not strictly necessary: bootout SIGKILLs the entire
# process tree of the service, which is fine when called externally but
# catastrophic when called from within a bridge backend.  The pre-flight check
# above protects us either way, but minimising bootout reduces blast radius.
# ============================================================================
SERVICE_LOADED=false
if launchctl print "$SERVICE_TARGET" >/dev/null 2>&1; then
  SERVICE_LOADED=true
fi

if [[ "$PLIST_CHANGED" == "true" ]] || [[ "$SERVICE_LOADED" == "false" ]]; then
  echo "==> Re-registering service (plist changed or not loaded)"
  if [[ "$SERVICE_LOADED" == "true" ]]; then
    launchctl bootout "$DOMAIN_TARGET" "$PLIST_PATH" 2>/dev/null || true
    # Give launchd a moment to actually tear down + release port.
    sleep 1
  fi

  # Defensive: clean any stragglers on port 8766 before bootstrap, in case
  # bootout couldn't reach a process or someone started bridge manually.
  PORT="${BRIDGE_PORT:-8766}"
  STRAGGLER_PIDS="$(lsof -ti :"$PORT" 2>/dev/null || true)"
  if [[ -n "$STRAGGLER_PIDS" ]]; then
    echo "    Killing stragglers on port $PORT: $STRAGGLER_PIDS"
    echo "$STRAGGLER_PIDS" | xargs kill -9 2>/dev/null || true
    sleep 1
  fi

  launchctl bootstrap "$DOMAIN_TARGET" "$PLIST_PATH"
  launchctl enable "$SERVICE_TARGET" 2>/dev/null || true
else
  echo "==> Hot-restarting service (kickstart -k)"
  launchctl kickstart -k "$SERVICE_TARGET"
fi

# ============================================================================
# Wait for bridge to come back up.
# ============================================================================
echo "==> Waiting for bridge to become healthy..."
HEALTHY=false
for i in {1..20}; do
  sleep 0.5
  if "$RUNTIME_DIR/venv/bin/python" "$RUNTIME_DIR/bridge_healthcheck.py" \
       --host 127.0.0.1 --port 8766 --timeout 1 2>/dev/null; then
    HEALTHY=true
    break
  fi
done

if [[ "$HEALTHY" == "true" ]]; then
  echo "Bridge is healthy on :8766"
else
  echo "WARNING: Bridge did not pass healthcheck within 10s." >&2
  echo "  Check logs:" >&2
  echo "    /tmp/$SERVICE_LABEL.stderr.log" >&2
  echo "    /tmp/$SERVICE_LABEL.stdout.log" >&2
  echo "    $RUNTIME_DIR/bridge_v2.log" >&2
  exit 1
fi

echo "Installed: $PLIST_PATH"
echo "Runtime  : $RUNTIME_DIR"
