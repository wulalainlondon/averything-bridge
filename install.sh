#!/bin/bash
set -euo pipefail

SRC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNTIME_DIR="$HOME/.claude-bridge-runtime"

if [[ "$SRC_DIR" == "$RUNTIME_DIR" ]]; then
  echo "ERROR: install.sh 從 runtime 目錄執行 — rsync 會是 no-op，程式碼不會更新。" >&2
  echo "       請從 source 目錄執行，例如：" >&2
  echo "         bash ~/Downloads/Helper/claude-bridge/bridge/install.sh" >&2
  exit 1
fi
SERVICE_LABEL="${BRIDGE_SERVICE_LABEL:-com.claude-bridge.app}"
PLIST_NAME="${SERVICE_LABEL}.plist"
LAUNCH_AGENTS="$HOME/Library/LaunchAgents"
PLIST_PATH="$LAUNCH_AGENTS/$PLIST_NAME"
SERVICE_TARGET="gui/$(id -u)/$SERVICE_LABEL"
DOMAIN_TARGET="gui/$(id -u)"

# ============================================================================
# Evict any legacy bridge labels that are NOT the current SERVICE_LABEL.
#
# Why: SERVICE_LABEL is configurable via BRIDGE_SERVICE_LABEL.  When it
# changes (e.g. com.claude-bridge.app → com.wulala.claude-bridge) the new
# plist is created but the old one is never removed.  Both have KeepAlive=true
# so both supervisors stay alive and fight for the same port.  We clean up
# all known historical labels here, before the pre-flight check, so we can
# never end up with two supervisors.
# ============================================================================
_LEGACY_LABELS=(
  "com.claude-bridge.app"
  "com.wulala.claude-bridge"
)
for _legacy in "${_LEGACY_LABELS[@]}"; do
  [[ "$_legacy" == "$SERVICE_LABEL" ]] && continue
  _legacy_target="gui/$(id -u)/$_legacy"
  _legacy_plist="$LAUNCH_AGENTS/$_legacy.plist"
  if launchctl print "$_legacy_target" >/dev/null 2>&1; then
    echo "[install] Evicting legacy bridge service: $_legacy"
    launchctl bootout "$_legacy_target" 2>/dev/null || true
    sleep 1
  fi
  if [[ -f "$_legacy_plist" ]]; then
    echo "[install] Removing legacy plist: $_legacy_plist"
    rm -f "$_legacy_plist"
  fi
done

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

# Dynamically exclude instance data subdirectories defined in instances.json
# so rsync --delete doesn't warn about non-empty dirs it can't remove.
_INSTANCE_EXCLUDES=()
if [[ -f "$RUNTIME_DIR/instances.json" ]]; then
  while IFS= read -r _subdir; do
    [[ -n "$_subdir" ]] && _INSTANCE_EXCLUDES+=("--exclude" "$_subdir/")
  done < <(python3 - "$RUNTIME_DIR" <<'PYEOF'
import json, os, sys
runtime = os.path.realpath(sys.argv[1])
try:
    data = json.load(open(os.path.join(runtime, "instances.json")))
    seen = set()
    for inst in data.get("instances", []):
        dd = os.path.realpath(os.path.expanduser(inst.get("data_dir", "")))
        if dd.startswith(runtime + "/"):
            rel = dd[len(runtime)+1:].split("/")[0]
            if rel and rel not in seen:
                seen.add(rel)
                print(rel)
except Exception as e:
    print(str(e), file=sys.stderr)
PYEOF
)
fi

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
  --exclude 'instances.json' \
  --exclude 'install.sh' \
  --exclude 'tunnel_url.txt' \
  --exclude 'bridge_identity.json' \
  ${_INSTANCE_EXCLUDES[@]+"${_INSTANCE_EXCLUDES[@]}"} \
  "$SRC_DIR/" "$RUNTIME_DIR/"
cd "$RUNTIME_DIR"

# Record the active service label so restart_agent.sh can look it up.
echo "$SERVICE_LABEL" > "$RUNTIME_DIR/.service-label"

# ============================================================================
# Bootstrap instances.json — create on first install/upgrade; never overwrite.
# ============================================================================
INSTANCES_CONFIG="$RUNTIME_DIR/instances.json"
if [[ ! -f "$INSTANCES_CONFIG" ]]; then
  if [[ -f "$RUNTIME_DIR/saved_sessions.json" ]]; then
    # Upgrading from single-instance: auto-generate default instance config
    echo "[install] Detected existing single-instance state. Generating default instances.json..."
    cat > "$INSTANCES_CONFIG" <<EOF
{
  "instances": [
    {
      "name": "default",
      "port": 8766,
      "data_dir": "$RUNTIME_DIR",
      "root_dir": ""
    }
  ]
}
EOF
    echo "[install] Generated $INSTANCES_CONFIG with single default instance (port 8766, data_dir=$RUNTIME_DIR)"
  else
    # Fresh install: copy example
    cp "$RUNTIME_DIR/instances.json.example" "$INSTANCES_CONFIG"
    echo "[install] Created $INSTANCES_CONFIG from example."
    echo "[install]    Edit it to configure your instances, then re-run install.sh."
    echo "[install]    At minimum, update 'root_dir' values to real paths."
  fi
fi

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

chmod +x run_bridge.sh bridge_supervisor.sh supervisor_instance.sh bridge_healthcheck.py bridge_launch.sh restart_agent.sh cloudflared_launcher.sh

# ============================================================================
# Install cloudflared for auto-tunnel support (best-effort).
# ============================================================================
echo "==> Check cloudflared"
if command -v cloudflared >/dev/null 2>&1; then
  echo "    cloudflared already installed: $(cloudflared --version 2>&1 | head -1)"
elif command -v brew >/dev/null 2>&1; then
  echo "    Installing cloudflared via Homebrew..."
  brew install cloudflared --quiet
  echo "    cloudflared installed: $(cloudflared --version 2>&1 | head -1)"
else
  echo "    WARNING: brew not found — cloudflared skipped."
  echo "             Auto-tunnel will not work until cloudflared is installed."
  echo "             Manual install: https://developers.cloudflare.com/cloudflare-one/connections/connect-apps/install-and-setup/installation/"
fi

# ============================================================================
# Check git — required for diff visualization feature.
# ============================================================================
echo "==> Check git"
if command -v git >/dev/null 2>&1; then
  echo "    git already installed: $(git --version)"
elif command -v brew >/dev/null 2>&1; then
  echo "    Installing git via Homebrew..."
  brew install git --quiet
  echo "    git installed: $(git --version)"
else
  echo "    WARNING: git not found and Homebrew is unavailable."
  echo "             To install: xcode-select --install"
  echo "             (This opens a macOS system dialog to install Command Line Tools)"
fi

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
    <key>BRIDGE_INSTANCES_CONFIG</key><string>$RUNTIME_DIR/instances.json</string>
    <key>BRIDGE_DISABLE_MDNS</key><string>0</string>
    <key>BRIDGE_AUTO_TUNNEL</key><string>0</string>
    <key>BRIDGE_TUNNEL_URL_FILE</key><string>$RUNTIME_DIR/tunnel_url.txt</string>
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
  # Kill the running bridge process(es) BEFORE kickstart so the new supervisor
  # starts with a free port and launches fresh code.  Without this, supervisor
  # adopts the healthy orphan and the newly-deployed bridge_v2.py never takes
  # effect — the same bug that affects code-only deploys (plist unchanged).
  PORT="${BRIDGE_PORT:-8766}"
  STRAGGLER_PIDS="$(lsof -ti :"$PORT" 2>/dev/null || true)"
  if [[ -n "$STRAGGLER_PIDS" ]]; then
    echo "    Killing bridge on port $PORT (pid(s): $STRAGGLER_PIDS) so new code takes effect"
    echo "$STRAGGLER_PIDS" | xargs kill -9 2>/dev/null || true
    sleep 1
  fi
  launchctl kickstart -k "$SERVICE_TARGET"
fi

# ============================================================================
# Wait for bridge instances to come back up.
# Reads instances.json for the authoritative port list; falls back to port
# 8766 only if parsing fails (e.g. fresh example config not yet edited).
# ============================================================================
echo "==> Waiting for bridge instances to start..."
UNHEALTHY=0

# Parse instances.json into name|port lines; on failure emit a sentinel.
INSTANCES_LIST="$(python3 -c "
import json, sys
try:
    data = json.load(open('$RUNTIME_DIR/instances.json'))
    for inst in data['instances']:
        print(f\"{inst['name']}|{inst['port']}|\")
except Exception as e:
    print(f'ERROR|0|{e}', file=sys.stderr)
" 2>/dev/null)" || true

if [[ -z "$INSTANCES_LIST" ]]; then
  # Fallback: instances.json unreadable — check port 8766 only
  echo "[install] Could not parse instances.json; falling back to port 8766 healthcheck"
  INSTANCES_LIST="default|8766|"
fi

while IFS='|' read -r name port _rest; do
  [[ -z "$name" || "$name" == "ERROR" ]] && continue
  # Wait up to 10 s (20 x 0.5 s) for this instance to become healthy
  for i in {1..20}; do
    sleep 0.5
    if "$RUNTIME_DIR/venv/bin/python" "$RUNTIME_DIR/bridge_healthcheck.py" \
         --host 127.0.0.1 --port "$port" --timeout 1 2>/dev/null; then
      echo "[install] instance '$name' is healthy on :$port"
      break
    fi
  done
  # Final verdict check
  if ! "$RUNTIME_DIR/venv/bin/python" "$RUNTIME_DIR/bridge_healthcheck.py" \
       --host 127.0.0.1 --port "$port" --timeout 1 2>/dev/null; then
    echo "WARNING: instance '$name' on port $port did not pass healthcheck within 10s." >&2
    echo "  Check logs:" >&2
    echo "    /tmp/$SERVICE_LABEL.stderr.log" >&2
    echo "    /tmp/$SERVICE_LABEL.stdout.log" >&2
    echo "    $RUNTIME_DIR/instances/$name/bridge.log" >&2
    UNHEALTHY=1
  fi
done <<< "$INSTANCES_LIST"

if (( UNHEALTHY )); then
  exit 1
fi

# ============================================================================
# Register the restart-agent launchd service.
#
# This is a separate launchd service whose parent is PID 1, NOT the bridge.
# It watches .restart-trigger via WatchPaths; when bridge writes that file
# (on a restart_bridge WS command), launchd fires restart_agent.sh which
# does a kickstart of the bridge service without being in its process tree.
# ============================================================================
# ============================================================================
# Register cloudflared as an independent launchd service.
#
# Key invariant: if the plist is UNCHANGED, we do NOT restart the service.
# This preserves the running cloudflared process (and its tunnel URL) across
# bridge deploys.  Only a plist change (e.g. port update) forces a restart.
# ============================================================================
echo "==> Register cloudflared service"
CFD_LABEL="com.claude-bridge.cloudflared"
CFD_PLIST_PATH="$LAUNCH_AGENTS/$CFD_LABEL.plist"
CFD_TARGET="gui/$(id -u)/$CFD_LABEL"
CFD_URL_FILE="$RUNTIME_DIR/tunnel_url.txt"

if ! command -v cloudflared >/dev/null 2>&1; then
  echo "    cloudflared not installed — skipping service registration"
  echo "    (install via: brew install cloudflared)"
else
  CFD_TMP="$(mktemp -t claude-bridge-cfd.XXXXXX)"
  cat > "$CFD_TMP" <<CFD_PLIST_EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>$CFD_LABEL</string>
  <key>ProgramArguments</key>
  <array>
    <string>/bin/bash</string>
    <string>-lc</string>
    <string>exec $RUNTIME_DIR/cloudflared_launcher.sh</string>
  </array>
  <key>WorkingDirectory</key><string>$RUNTIME_DIR</string>
  <key>KeepAlive</key><true/>
  <key>RunAtLoad</key><true/>
  <key>ThrottleInterval</key><integer>30</integer>
  <key>StandardOutPath</key><string>/tmp/$CFD_LABEL.stdout.log</string>
  <key>StandardErrorPath</key><string>/tmp/$CFD_LABEL.stderr.log</string>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PATH</key><string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin</string>
    <key>HOME</key><string>$HOME</string>
    <key>BRIDGE_DATA_DIR</key><string>$RUNTIME_DIR</string>
    <key>BRIDGE_PORT</key><string>8766</string>
    <key>BRIDGE_TUNNEL_URL_FILE</key><string>$CFD_URL_FILE</string>
  </dict>
</dict>
</plist>
CFD_PLIST_EOF

  if [[ ! -f "$CFD_PLIST_PATH" ]] || ! cmp -s "$CFD_TMP" "$CFD_PLIST_PATH"; then
    # Plist changed (or first install) — restart service to pick up new config.
    mv "$CFD_TMP" "$CFD_PLIST_PATH"
    echo "    cloudflared plist updated — restarting service"
    if launchctl print "$CFD_TARGET" >/dev/null 2>&1; then
      launchctl bootout "$CFD_TARGET" 2>/dev/null || true
      sleep 1
    fi
    launchctl bootstrap "$DOMAIN_TARGET" "$CFD_PLIST_PATH"
    launchctl enable "$CFD_TARGET" 2>/dev/null || true
    echo "    cloudflared service registered (will acquire new tunnel URL)"
  else
    rm -f "$CFD_TMP"
    echo "    cloudflared plist unchanged — service left running (tunnel URL preserved)"
  fi
fi

echo "==> Register restart-agent"
RAGENT_LABEL="com.claude-bridge.restart-agent"
RAGENT_PLIST_PATH="$LAUNCH_AGENTS/$RAGENT_LABEL.plist"
RAGENT_TRIGGER="$RUNTIME_DIR/.restart-trigger"
RAGENT_TARGET="gui/$(id -u)/$RAGENT_LABEL"

RAGENT_TMP="$(mktemp -t claude-bridge-ragent.XXXXXX)"
cat > "$RAGENT_TMP" <<RAGENT_PLIST_EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>$RAGENT_LABEL</string>
  <key>ProgramArguments</key>
  <array>
    <string>/bin/bash</string>
    <string>-lc</string>
    <string>exec $RUNTIME_DIR/restart_agent.sh</string>
  </array>
  <key>WorkingDirectory</key><string>$RUNTIME_DIR</string>
  <key>WatchPaths</key>
  <array>
    <string>$RAGENT_TRIGGER</string>
  </array>
  <key>KeepAlive</key><false/>
  <key>RunAtLoad</key><false/>
  <key>ThrottleInterval</key><integer>5</integer>
  <key>StandardOutPath</key><string>/tmp/$RAGENT_LABEL.stdout.log</string>
  <key>StandardErrorPath</key><string>/tmp/$RAGENT_LABEL.stderr.log</string>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PATH</key><string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin</string>
    <key>HOME</key><string>$HOME</string>
  </dict>
</dict>
</plist>
RAGENT_PLIST_EOF

if [[ ! -f "$RAGENT_PLIST_PATH" ]] || ! cmp -s "$RAGENT_TMP" "$RAGENT_PLIST_PATH"; then
  mv "$RAGENT_TMP" "$RAGENT_PLIST_PATH"
  if launchctl print "$RAGENT_TARGET" >/dev/null 2>&1; then
    launchctl bootout "$RAGENT_TARGET" 2>/dev/null || true
    sleep 0.5
  fi
  launchctl bootstrap "$DOMAIN_TARGET" "$RAGENT_PLIST_PATH"
  launchctl enable "$RAGENT_TARGET" 2>/dev/null || true
  echo "    restart-agent registered"
else
  rm -f "$RAGENT_TMP"
  echo "    restart-agent plist unchanged"
fi

echo "Installed: $PLIST_PATH"
echo "Runtime  : $RUNTIME_DIR"
