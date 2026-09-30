#!/usr/bin/env bash
#
# everything-go bridge installer
# ------------------------------------------------------------------
# One-liner (paste to your claude/codex CLI, or run in a terminal):
#
#   curl -fsSL https://github.com/wulalainlondon/everything-go/releases/latest/download/install.sh | bash
#
# What it does:
#   1. downloads and verifies the signed, notarized macOS app (or Linux binary)
#   2. makes sure `cloudflared` exists (for off-WiFi remote access)
#   3. installs everything-go as a background service (launchd / systemd)
#      started with UDP discovery + mDNS; the tunnel starts only after a trusted
#      device has paired on the local network
#   4. starts it and prints how the phone app connects
#
# Prerequisite: you already have the `claude` and/or `codex` CLI installed
# and logged in. This bridge spawns *your* CLI with *your* account.
# ------------------------------------------------------------------
set -euo pipefail

REPO="${EVERYTHING_GO_REPO:-wulalainlondon/everything-go}"
PORT="${EVERYTHING_GO_PORT:-8766}"
RUNTIME_DIR="${EVERYTHING_GO_HOME:-$HOME/.everything-go-runtime}"
LABEL="com.everything-go.app"
BIN="$RUNTIME_DIR/everything-go"
APP_DIR="$RUNTIME_DIR/Everything Go.app"
APP_BIN="$APP_DIR/Contents/MacOS/everything-go"
EXPECTED_SIGNING_AUTHORITY="Developer ID Application: YuDi Huang (UPWLTJL6S2)"
EXPECTED_TEAM_ID="UPWLTJL6S2"
LAUNCH="$RUNTIME_DIR/everything_go_launch.sh"
SESSION_STORE="${EVERYTHING_GO_SESSION_STORE:-$HOME/.claude-bridge-runtime/saved_sessions.json}"
SERVICE_BIN="$BIN"
PERMISSION_TARGET="$BIN"

# Preserve generated launch-wrapper settings across upgrades unless the caller
# explicitly supplies an environment override. Replacing these with defaults
# can change source-policy fingerprints, force multi-gigabyte reindexing, or
# silently switch Codex transport behavior.
existing_launch_export() {
  local name="$1"
  [ -f "$LAUNCH" ] || return 0
  sed -n "s/^export ${name}=\"\(.*\)\"$/\1/p" "$LAUNCH" | tail -1
}

resolve_launch_export() {
  local name="$1" fallback="$2" existing
  if printenv "$name" >/dev/null 2>&1; then
    printenv "$name"
    return 0
  fi
  existing=$(existing_launch_export "$name")
  if [ -n "$existing" ]; then
    printf '%s\n' "$existing"
  else
    printf '%s\n' "$fallback"
  fi
}

BROWSER_ORIGIN_MODE=$(resolve_launch_export BRIDGE_BROWSER_ORIGIN_MODE ask)
BROWSER_ALLOWED_ORIGINS=$(resolve_launch_export BRIDGE_BROWSER_ALLOWED_ORIGINS "")
BROWSER_MANAGE_AUTO_REVIEW=$(resolve_launch_export BRIDGE_BROWSER_MANAGE_AUTO_REVIEW 1)
BROWSER_ENABLE_NETWORK=$(resolve_launch_export BRIDGE_BROWSER_ENABLE_NETWORK 1)
CODEX_SESSIONS_DIR=$(resolve_launch_export EVERYTHING_GO_CODEX_SESSIONS_DIR "${CODEX_HOME:-$HOME/.codex}/sessions")
CODEX_IGNORE_CWD_GLOBS=$(resolve_launch_export EVERYTHING_GO_CODEX_IGNORE_CWD_GLOBS "")
CODEX_IGNORE_NAME_PREFIXES=$(resolve_launch_export EVERYTHING_GO_CODEX_IGNORE_NAME_PREFIXES "")
CODEX_ROLLOVER_ENABLED=$(resolve_launch_export EVERYTHING_GO_CODEX_ROLLOVER_ENABLED false)
CODEX_COLD_RESUME_MAX_BYTES=$(resolve_launch_export EVERYTHING_GO_CODEX_COLD_RESUME_MAX_BYTES 268435456)
CODEX_CHECKPOINT_MAX_BYTES=$(resolve_launch_export EVERYTHING_GO_CODEX_CHECKPOINT_MAX_BYTES 131072)
CODEX_APP_SERVER_MODE=$(resolve_launch_export EVERYTHING_GO_CODEX_APP_SERVER_MODE daemon)
CODEX_APP_SERVER_SOCKET=$(resolve_launch_export EVERYTHING_GO_CODEX_APP_SERVER_SOCKET "")

say()  { printf '\033[1;36m[everything-go]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[everything-go]\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m[everything-go]\033[0m %s\n' "$*" >&2; exit 1; }

run_permission_check() {
  local extra_paths="$HOME/Downloads:$HOME/Documents:$HOME/Desktop"
  "$SERVICE_BIN" \
    --permission-check \
    --data-dir "$RUNTIME_DIR" \
    --session-store "$SESSION_STORE" \
    --permission-check-paths "$extra_paths"
}

open_full_disk_access_settings() {
  if command -v open >/dev/null 2>&1; then
    open "x-apple.systempreferences:com.apple.preference.security?Privacy_AllFiles" >/dev/null 2>&1 || true
  fi
}

ensure_macos_permissions() {
  [ "$OS" = darwin ] || return 0
  [ "${EVERYTHING_GO_SKIP_PERMISSION_CHECK:-0}" = "1" ] && {
    warn "skipping macOS permission check because EVERYTHING_GO_SKIP_PERMISSION_CHECK=1"
    return 0
  }

  say "checking macOS file permissions with the installed bridge binary ..."
  if run_permission_check; then
    say "macOS permission check passed"
    return 0
  fi

  warn "macOS blocked the bridge from reading one or more local folders."
  warn "Grant Full Disk Access to: $PERMISSION_TARGET"
  warn "System Settings → Privacy & Security → Full Disk Access"
  open_full_disk_access_settings

  while true; do
    if [ ! -r /dev/tty ]; then
      die "permission approval needs an interactive terminal; re-run install.sh in Terminal after granting Full Disk Access to $SERVICE_BIN"
    fi
    printf "Press Enter after granting Full Disk Access, or Ctrl-C to stop: " >/dev/tty
    read -r _unused </dev/tty
    if run_permission_check; then
      say "macOS permission check passed"
      return 0
    fi
    warn "permission check still failed; confirm Full Disk Access is enabled for $SERVICE_BIN"
    open_full_disk_access_settings
  done
}

install_bridge_binary() {
  mkdir -p "$RUNTIME_DIR"

  # Resolve the latest tag via API to build a direct /releases/download/<tag>/
  # URL, bypassing the /releases/latest/download/ redirect which returns 504
  # for zip assets on some GitHub CDN nodes.
  local latest_tag
  latest_tag=$(curl -fsSL --proto '=https' --tlsv1.2 \
    "https://api.github.com/repos/$REPO/releases/latest" \
    | grep '"tag_name"' | head -1 | sed 's/.*"tag_name": *"\([^"]*\)".*/\1/')
  [ -n "$latest_tag" ] || latest_tag="latest"

  base_url() { # $1 = asset filename
    if [ "$latest_tag" = "latest" ]; then
      echo "https://github.com/$REPO/releases/latest/download/$1"
    else
      echo "https://github.com/$REPO/releases/download/$latest_tag/$1"
    fi
  }

  URL=$(base_url "$ASSET")

  if [ "$OS" = darwin ]; then
    local app_asset="everything-go-darwin-${ARCH}.app.zip"
    local app_url sums_url staging extracted authority team backup expected_sha actual_sha
    app_url=$(base_url "$app_asset")
    sums_url=$(base_url "SHA256SUMS")
    say "downloading $app_asset ..."
    curl -fSL --proto '=https' --tlsv1.2 "$app_url" -o "$RUNTIME_DIR/$app_asset.tmp" \
      || die "signed macOS app download failed: $app_url"
    curl -fsSL --proto '=https' --tlsv1.2 "$sums_url" -o "$RUNTIME_DIR/SHA256SUMS.tmp" \
      || die "checksum download failed: $sums_url"
    expected_sha=$(awk -v asset="$app_asset" '$2 == asset { print $1 }' "$RUNTIME_DIR/SHA256SUMS.tmp")
    [ -n "$expected_sha" ] || die "release checksum does not contain $app_asset"
    actual_sha=$(shasum -a 256 "$RUNTIME_DIR/$app_asset.tmp" | awk '{print $1}')
    [ "$actual_sha" = "$expected_sha" ] || die "checksum mismatch for $app_asset"

    staging=$(mktemp -d "$RUNTIME_DIR/.install.XXXXXX")
    ditto -x -k "$RUNTIME_DIR/$app_asset.tmp" "$staging"
    extracted="$staging/Everything Go.app"
    [ -x "$extracted/Contents/MacOS/everything-go" ] \
      || die "app asset did not contain the expected executable"
    codesign --verify --deep --strict --verbose=2 "$extracted" \
      || die "Developer ID signature verification failed"
    authority=$(codesign -d --verbose=4 "$extracted" 2>&1 | sed -n 's/^Authority=//p' | head -1)
    team=$(codesign -d --verbose=4 "$extracted" 2>&1 | sed -n 's/^TeamIdentifier=//p' | head -1)
    [ "$authority" = "$EXPECTED_SIGNING_AUTHORITY" ] \
      || die "unexpected signing authority: ${authority:-missing}"
    [ "$team" = "$EXPECTED_TEAM_ID" ] || die "unexpected signing team: ${team:-missing}"
    spctl -a -t exec -vv "$extracted" || die "Gatekeeper rejected the app"
    xcrun stapler validate "$extracted" || die "notarization ticket validation failed"

    backup="$RUNTIME_DIR/Everything Go.previous.app"
    rm -rf "$backup"
    if [ -d "$APP_DIR" ]; then mv "$APP_DIR" "$backup"; fi
    if ! mv "$extracted" "$APP_DIR"; then
      [ -d "$backup" ] && mv "$backup" "$APP_DIR"
      die "failed to install the verified app"
    fi
    rm -rf "$staging"
    rm -f "$RUNTIME_DIR/$app_asset.tmp" "$RUNTIME_DIR/SHA256SUMS.tmp"
    SERVICE_BIN="$APP_BIN"
    PERMISSION_TARGET="$APP_DIR"
    say "verified signed app installed: $APP_DIR"
    return 0
  fi

  say "downloading $ASSET ..."
  curl -fSL --proto '=https' --tlsv1.2 "$URL" -o "$BIN.tmp" \
    || die "download failed: $URL"
  chmod +x "$BIN.tmp"
  mv -f "$BIN.tmp" "$BIN"
  SERVICE_BIN="$BIN"
  PERMISSION_TARGET="$BIN"
  say "binary installed: $BIN"
}

# ── 1. platform detection ──────────────────────────────────────────
OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"
case "$ARCH" in
  arm64|aarch64) ARCH=arm64 ;;
  x86_64|amd64)  ARCH=amd64 ;;
  *) die "unsupported architecture: $ARCH" ;;
esac
case "$OS" in
  darwin|linux) ;;
  *) die "unsupported OS: $OS (only macOS and Linux are supported)" ;;
esac
ASSET="everything-go-${OS}-${ARCH}"

# ── 2. prerequisite: claude or codex CLI ───────────────────────────
CLAUDE_BIN="$(command -v claude 2>/dev/null || true)"
CODEX_BIN="$(command -v codex 2>/dev/null || true)"
if [ -z "$CLAUDE_BIN" ] && [ -z "$CODEX_BIN" ]; then
  die "neither 'claude' nor 'codex' CLI found in PATH. Install and log in to at least one first."
fi
say "found CLI: ${CLAUDE_BIN:-—} ${CODEX_BIN:-—}"

# Collect the dirs that hold claude/codex so the background service's PATH
# can find them (launchd/systemd start with a minimal PATH).
CLI_PATHS=""
for b in "$CLAUDE_BIN" "$CODEX_BIN"; do
  [ -n "$b" ] && CLI_PATHS="$CLI_PATHS:$(dirname "$b")"
done

# ── 3. download the bridge binary/app ──────────────────────────────
install_bridge_binary

# ── 4. ensure cloudflared (remote access; optional but recommended) ─
CF_PATH=""
if command -v cloudflared >/dev/null 2>&1; then
  CF_PATH="$(dirname "$(command -v cloudflared)")"
elif [ "$OS" = darwin ] && command -v brew >/dev/null 2>&1; then
  say "installing cloudflared via Homebrew ..."
  brew install cloudflared && CF_PATH="$(dirname "$(command -v cloudflared)")"
else
  # Linux ships a raw binary; macOS only ships .tgz/.pkg, so non-brew mac
  # users get a warning instead.
  if [ "$OS" = linux ]; then
    say "downloading cloudflared ..."
    curl -fSL "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-${ARCH}" \
      -o "$RUNTIME_DIR/cloudflared" && chmod +x "$RUNTIME_DIR/cloudflared" \
      && CF_PATH="$RUNTIME_DIR"
  fi
fi
if [ -z "$CF_PATH" ]; then
  warn "cloudflared not available — same-WiFi will still work via mDNS, but"
  warn "off-WiFi remote access needs cloudflared (install Homebrew then re-run)."
fi

# ── 5. generate the launch wrapper ─────────────────────────────────
# Service-managers start with a bare PATH; bake in the CLI + cloudflared dirs.
SERVICE_PATH="/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin"
if [ -n "$CLI_PATHS" ]; then
  # Prefer the exact CLI locations discovered above.  A stale global Codex
  # installation must not shadow the managed/current CLI for launchd.
  SERVICE_PATH="${CLI_PATHS#:}:$SERVICE_PATH"
fi
[ -n "$CF_PATH" ] && SERVICE_PATH="$SERVICE_PATH:$CF_PATH"
cat > "$LAUNCH" <<EOF
#!/usr/bin/env bash
set -uo pipefail
export PATH="$SERVICE_PATH:\$PATH"
export BRIDGE_BROWSER_ORIGIN_MODE="$BROWSER_ORIGIN_MODE"
export BRIDGE_BROWSER_ALLOWED_ORIGINS="$BROWSER_ALLOWED_ORIGINS"
export BRIDGE_BROWSER_MANAGE_AUTO_REVIEW="$BROWSER_MANAGE_AUTO_REVIEW"
export BRIDGE_BROWSER_ENABLE_NETWORK="$BROWSER_ENABLE_NETWORK"
export EVERYTHING_GO_CODEX_SESSIONS_DIR="$CODEX_SESSIONS_DIR"
export EVERYTHING_GO_CODEX_IGNORE_CWD_GLOBS="$CODEX_IGNORE_CWD_GLOBS"
export EVERYTHING_GO_CODEX_IGNORE_NAME_PREFIXES="$CODEX_IGNORE_NAME_PREFIXES"
export EVERYTHING_GO_CODEX_ROLLOVER_ENABLED="$CODEX_ROLLOVER_ENABLED"
export EVERYTHING_GO_CODEX_COLD_RESUME_MAX_BYTES="$CODEX_COLD_RESUME_MAX_BYTES"
export EVERYTHING_GO_CODEX_CHECKPOINT_MAX_BYTES="$CODEX_CHECKPOINT_MAX_BYTES"
export EVERYTHING_GO_CODEX_APP_SERVER_MODE="$CODEX_APP_SERVER_MODE"
export EVERYTHING_GO_CODEX_APP_SERVER_SOCKET="$CODEX_APP_SERVER_SOCKET"

if [ -r "$RUNTIME_DIR/monitoring_env.sh" ]; then
  . "$RUNTIME_DIR/monitoring_env.sh"
fi
exec "$SERVICE_BIN" \\
  --port "$PORT" \\
  --executor go \\
  --data-dir "$RUNTIME_DIR" \\
  --session-store "$SESSION_STORE" \\
  --discovery \\
  --mdns \\
  --tunnel \\
  --instance-name "\$(hostname -s 2>/dev/null || echo everything-go)"
EOF
chmod +x "$LAUNCH"
say "launch script written: $LAUNCH"
ensure_macos_permissions

# ── 6. install as a background service ─────────────────────────────
install_launchd() {
  local plist="$HOME/Library/LaunchAgents/$LABEL.plist"
  mkdir -p "$HOME/Library/LaunchAgents"
  cat > "$plist" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>$LABEL</string>
  <key>ProgramArguments</key>
  <array>
    <string>/bin/bash</string><string>-lc</string><string>exec $LAUNCH</string>
  </array>
  <key>WorkingDirectory</key><string>$RUNTIME_DIR</string>
  <key>KeepAlive</key><true/>
  <key>RunAtLoad</key><true/>
  <key>ThrottleInterval</key><integer>10</integer>
  <key>StandardOutPath</key><string>/tmp/$LABEL.stdout.log</string>
  <key>StandardErrorPath</key><string>/tmp/$LABEL.stderr.log</string>
</dict>
</plist>
EOF
  local target="gui/$(id -u)/$LABEL"
  if launchctl print "$target" >/dev/null 2>&1; then
    launchctl kickstart -k "$target"
  else
    launchctl bootstrap "gui/$(id -u)" "$plist"
    launchctl kickstart -k "$target"
  fi
}

verify_launchd_health_or_rollback() {
  local target="gui/$(id -u)/$LABEL" attempt failed_app
  for attempt in $(seq 1 20); do
    if nc -z 127.0.0.1 "$PORT" >/dev/null 2>&1; then
      say "launchd health check passed on port $PORT"
      return 0
    fi
    sleep 1
  done

  warn "new bridge failed its launch health check; restoring previous signed app"
  if [ -d "$RUNTIME_DIR/Everything Go.previous.app" ]; then
    failed_app="$RUNTIME_DIR/Everything Go.failed.app"
    rm -rf "$failed_app"
    [ -d "$APP_DIR" ] && mv "$APP_DIR" "$failed_app"
    mv "$RUNTIME_DIR/Everything Go.previous.app" "$APP_DIR"
    launchctl kickstart -k "$target" >/dev/null 2>&1 || true
    die "update rolled back; inspect /tmp/$LABEL.stderr.log"
  fi
  die "bridge did not become healthy; inspect /tmp/$LABEL.stderr.log"
}

install_systemd() {
  local unit_dir="$HOME/.config/systemd/user"
  mkdir -p "$unit_dir"
  cat > "$unit_dir/everything-go.service" <<EOF
[Unit]
Description=everything-go bridge
After=network-online.target

[Service]
ExecStart=$LAUNCH
Restart=always
RestartSec=10

[Install]
WantedBy=default.target
EOF
  systemctl --user daemon-reload
  systemctl --user enable --now everything-go.service
  command -v loginctl >/dev/null 2>&1 && loginctl enable-linger "$(whoami)" 2>/dev/null || true
}

if [ "$OS" = darwin ]; then
  install_launchd
  verify_launchd_health_or_rollback
else
  if command -v systemctl >/dev/null 2>&1; then
    install_systemd
  else
    warn "no systemd — starting in foreground (will stop when you close this shell)."
    exec "$LAUNCH"
  fi
fi

# ── 7. report ──────────────────────────────────────────────────────
LAN_IP="$( (ipconfig getifaddr en0 2>/dev/null) || (hostname -I 2>/dev/null | awk '{print $1}') || true )"
echo
say "✅ everything-go is running on port $PORT"
echo
say "📱 In the phone app:"
say "   • Same WiFi: it auto-discovers this bridge (UDP + mDNS). Just open the app."
[ -n "$LAN_IP" ] && say "     (manual fallback — add bridge IP: $LAN_IP  port: $PORT)"
say "   • Off WiFi: pair once on WiFi. Only then does the Cloudflare tunnel start;"
say "     its URL is pushed to the trusted app automatically."
echo
say "logs:    /tmp/$LABEL.stderr.log"
say "restart: launchctl kickstart -k gui/\$(id -u)/$LABEL   (macOS)"
