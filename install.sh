#!/usr/bin/env bash
# Fresh installation from an explicitly selected, verified release. See docs/DEPLOYMENT.md.
# Existing deployments must follow the preserved-data update procedure instead.
set -euo pipefail
STAGING=''
INSTALL_LOCK=''

say() { printf '[everything-go] %s\n' "$*"; }
die() { printf '[everything-go] %s\n' "$*" >&2; exit 1; }
usage() {
  cat <<'HELP'
Usage: bash install.sh [--check | --help]

Select exactly one source (no latest/default-version fallback):
  EVERYTHING_GO_ARCHIVE=/path/to/reviewed.app.zip
  EVERYTHING_GO_ARCHIVE_SHA256=<independently recorded SHA256>
    OR
  EVERYTHING_GO_TAG=vX.Y.Z  (an already published, reviewed GitHub release)

Optional settings:
  EVERYTHING_GO_REPO=wulalainlondon/everything-go
  EVERYTHING_GO_HOME=$HOME/.everything-go-runtime
  EVERYTHING_GO_LABEL=com.everything-go.app
  EVERYTHING_GO_PORT=8766
  EVERYTHING_GO_INSTANCE_NAME=<computer display name>
  EVERYTHING_GO_SOURCE_REVISION=<expected full Git commit>
  EVERYTHING_GO_TUNNEL=0  (default; 1 explicitly enables an existing cloudflared)

--check validates prerequisites and the selected package, then removes its staging
files. It does not install, start/restart services, pair devices or change permissions.
Normal mode is a FRESH install. Existing data/wrappers/services/port ownership cause
refusal before replacement. See docs/DEPLOYMENT.md for an existing-machine update.
The CLI account must already be installed and logged in on this computer.
HELP
}

cleanup() {
  [ -z "${STAGING:-}" ] || rm -rf "$STAGING"
  [ -z "${INSTALL_LOCK:-}" ] || rmdir "$INSTALL_LOCK"
}
sha256_file() {
  if command -v shasum >/dev/null 2>&1; then shasum -a 256 "$1" | awk '{print $1}'
  else sha256sum "$1" | awk '{print $1}'; fi
}
validate_config() {
  [[ "$LABEL" =~ ^[A-Za-z0-9_.-]+$ ]] || die 'invalid service label'
  [[ "$PORT" =~ ^[0-9]{1,5}$ ]] && [ "$PORT" -ge 1 ] && [ "$PORT" -le 65535 ] || die 'invalid port'
  [[ "$TUNNEL_ENABLED" = 0 || "$TUNNEL_ENABLED" = 1 ]] || die 'EVERYTHING_GO_TUNNEL must be 0 or 1'
  [[ "$RUNTIME_DIR" = /* && "$SESSION_STORE" = /* ]] || die 'runtime and session-store paths must be absolute'
  [[ "$RUNTIME_DIR" != *$'\n'* && "$SESSION_STORE" != *$'\n'* ]] || die 'paths must not contain newlines'
  if [ -n "$LOCAL_ARCHIVE" ]; then
    [ -z "$RELEASE_TAG" ] || die 'choose local archive OR release tag, not both'
    [ -f "$LOCAL_ARCHIVE" ] || die 'local archive not found'
    [[ "$LOCAL_SHA" =~ ^[a-fA-F0-9]{64}$ ]] || die 'local archive requires an independently recorded SHA256'
    [ "$OS" = darwin ] || die 'local .app archives require macOS'
  else
    [ -z "$LOCAL_SHA" ] || die 'archive checksum provided without an archive'
    [[ "$REPO" =~ ^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$ ]] || die 'invalid release repository'
    [[ "$RELEASE_TAG" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] || die 'select an explicit published release tag or a verified local archive; no default release'
  fi
  [ -z "$EXPECTED_SOURCE" ] || [[ "$EXPECTED_SOURCE" =~ ^[a-f0-9]{40}$ ]] || die 'expected source must be a full Git revision'
}
assert_fresh_target() {
  [ ! -e "$LAUNCH" ] && [ ! -e "$APP_DIR" ] && [ ! -e "$BIN" ] && [ ! -e "$SESSION_STORE" ] || die 'existing deployment/data detected; use the preserved-data update procedure'
  if [ -e "$RUNTIME_DIR" ]; then
    [ -d "$RUNTIME_DIR" ] && [ ! -L "$RUNTIME_DIR" ] || die 'runtime must be a new or empty directory'
    [ -z "$(ls -A "$RUNTIME_DIR")" ] || die 'runtime is not empty; refusing to replace existing identity or data'
  fi
  if [ "$OS" = darwin ]; then
    [ ! -e "$PLIST" ] || die 'existing LaunchAgent plist; use the update procedure'
    if launchctl print "gui/$(id -u)/$LABEL" >/dev/null 2>&1; then die 'service label already loaded'; fi
  elif [ -e "$HOME/.config/systemd/user/everything-go.service" ]; then
    die 'existing systemd service; use the update procedure'
  fi
  if lsof -nP -iTCP:"$PORT" -sTCP:LISTEN -t 2>/dev/null | awk 'NF { found=1 } END { exit !found }'; then
    die 'port already in use; no running service was stopped'
  fi
}
require_tools() {
  local tool
  for tool in awk curl lsof mktemp; do command -v "$tool" >/dev/null 2>&1 || die "missing prerequisite: $tool"; done
  if [ "$OS" = darwin ]; then
    for tool in ditto codesign spctl xcrun lipo sw_vers launchctl; do command -v "$tool" >/dev/null 2>&1 || die "missing prerequisite: $tool"; done
  fi
  CLAUDE_BIN="$(command -v claude 2>/dev/null || true)"
  CODEX_BIN="$(command -v codex 2>/dev/null || true)"
  [ -n "$CLAUDE_BIN" ] || [ -n "$CODEX_BIN" ] || die 'install and log in to claude and/or codex first'
  CF_BIN=''
  if [ "$TUNNEL_ENABLED" = 1 ]; then
    CF_BIN="$(command -v cloudflared 2>/dev/null || true)"
    [ -n "$CF_BIN" ] || die 'Tunnel explicitly requested, but cloudflared is not installed'
  fi
}
verify_macos_app() {
  local authority team version source archs minimum current
  SERVICE_BIN="$STAGING/Everything Go.app/Contents/MacOS/everything-go"
  [ -x "$SERVICE_BIN" ] || die 'archive does not contain Everything Go.app executable'
  codesign --verify --deep --strict --verbose=2 "$STAGING/Everything Go.app" || die 'signature verification failed'
  authority=$(codesign -d --verbose=4 "$STAGING/Everything Go.app" 2>&1 | sed -n 's/^Authority=//p' | head -1)
  team=$(codesign -d --verbose=4 "$STAGING/Everything Go.app" 2>&1 | sed -n 's/^TeamIdentifier=//p' | head -1)
  [ "$authority" = 'Developer ID Application: YuDi Huang (UPWLTJL6S2)' ] && [ "$team" = UPWLTJL6S2 ] || die 'unexpected signing identity'
  spctl -a -t exec -vv "$STAGING/Everything Go.app" || die 'Gatekeeper rejected the app'
  xcrun stapler validate "$STAGING/Everything Go.app" || die 'notarization ticket validation failed'
  archs=$(lipo -archs "$SERVICE_BIN")
  case " $archs " in *" $NATIVE_ARCH "*) ;; *) die "package architecture does not match $NATIVE_ARCH" ;; esac
  version=$(/usr/libexec/PlistBuddy -c 'Print :CFBundleShortVersionString' "$STAGING/Everything Go.app/Contents/Info.plist")
  source=$(/usr/libexec/PlistBuddy -c 'Print :BridgeSourceRevision' "$STAGING/Everything Go.app/Contents/Info.plist")
  minimum=$(/usr/libexec/PlistBuddy -c 'Print :LSMinimumSystemVersion' "$STAGING/Everything Go.app/Contents/Info.plist")
  [[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ && "$source" =~ ^[a-f0-9]{40}$ ]] || die 'missing valid release provenance'
  [ -z "$RELEASE_TAG" ] || [ "v$version" = "$RELEASE_TAG" ] || die 'bundle version differs from selected release'
  [ -z "$EXPECTED_SOURCE" ] || [ "$source" = "$EXPECTED_SOURCE" ] || die 'bundle source differs from reviewed revision'
  [[ "$minimum" =~ ^[0-9]+(\.[0-9]+){0,2}$ ]] || die 'invalid minimum macOS provenance'
  current=$(sw_vers -productVersion)
  awk -v current="$current" -v minimum="$minimum" 'BEGIN { split(current,c,"."); split(minimum,m,"."); for(i=1;i<=3;i++){ if(c[i]+0>m[i]+0)exit 0; if(c[i]+0<m[i]+0)exit 1 } exit 0 }' || die "requires macOS $minimum or newer"
  say "verified version=$version source=$source architecture=$NATIVE_ARCH"
}
prepare_package() {
  local asset base expected actual
  STAGING=$(mktemp -d "${TMPDIR:-/tmp}/everything-go-install.XXXXXX")
  if [ -n "$LOCAL_ARCHIVE" ]; then
    expected=$(printf '%s' "$LOCAL_SHA" | tr '[:upper:]' '[:lower:]')
    cp "$LOCAL_ARCHIVE" "$STAGING/package.zip"
  else
    asset="everything-go-${OS}-${ARCH}"
    [ "$OS" != darwin ] || asset="$asset.app.zip"
    base="https://github.com/$REPO/releases/download/$RELEASE_TAG"
    curl -fSL --proto '=https' --tlsv1.2 "$base/$asset" -o "$STAGING/package.zip" || die 'selected release asset unavailable; no version fallback'
    curl -fsSL --proto '=https' --tlsv1.2 "$base/SHA256SUMS" -o "$STAGING/SHA256SUMS" || die 'release checksum download failed'
    expected=$(awk -v asset="$asset" '$2==asset {print $1}' "$STAGING/SHA256SUMS")
    [[ "$expected" =~ ^[a-f0-9]{64}$ ]] || die 'release checksum missing, duplicated or malformed'
  fi
  actual=$(sha256_file "$STAGING/package.zip")
  [ "$actual" = "$expected" ] || die 'archive checksum mismatch'
  if [ "$OS" = darwin ]; then
    ditto -x -k "$STAGING/package.zip" "$STAGING"
    verify_macos_app
  fi
}
write_launch_wrapper() {
  local name value
  {
    printf '#!/usr/bin/env bash\nset -euo pipefail\n'
    printf 'export PATH=%q\n' "$SERVICE_PATH"
    for name in BRIDGE_BROWSER_ORIGIN_MODE BRIDGE_BROWSER_ALLOWED_ORIGINS BRIDGE_BROWSER_MANAGE_AUTO_REVIEW BRIDGE_BROWSER_ENABLE_NETWORK EVERYTHING_GO_CODEX_SESSIONS_DIR EVERYTHING_GO_CODEX_IGNORE_CWD_GLOBS EVERYTHING_GO_CODEX_IGNORE_NAME_PREFIXES EVERYTHING_GO_CODEX_ROLLOVER_ENABLED EVERYTHING_GO_CODEX_COLD_RESUME_MAX_BYTES EVERYTHING_GO_CODEX_CHECKPOINT_MAX_BYTES EVERYTHING_GO_CODEX_APP_SERVER_MODE EVERYTHING_GO_CODEX_APP_SERVER_SOCKET; do
      value="${!name}"
      printf 'export %s=%q\n' "$name" "$value"
    done
    printf 'if [ -r %q ]; then\n  . %q\nfi\n' "$RUNTIME_DIR/monitoring_env.sh" "$RUNTIME_DIR/monitoring_env.sh"
    printf 'exec '
    printf '%q ' "$SERVICE_BIN" --port "$PORT" --executor go --data-dir "$RUNTIME_DIR" --session-store "$SESSION_STORE" --discovery --mdns --instance-name "$INSTANCE_NAME"
    [ -z "$CLAUDE_BIN" ] || printf '%q ' --claude-bin "$CLAUDE_BIN"
    [ -z "$CODEX_BIN" ] || printf '%q ' --codex-bin "$CODEX_BIN"
    [ "$TUNNEL_ENABLED" != 1 ] || printf '%q ' --tunnel
    printf '\n'
  } > "$LAUNCH"
  chmod 700 "$LAUNCH"
}
xml_escape() { printf '%s' "$1" | sed -e 's/\&/\&amp;/g' -e 's/</\&lt;/g' -e 's/>/\&gt;/g' -e 's/"/\&quot;/g' -e "s/'/\&apos;/g"; }
write_launchd_plist() {
  mkdir -p "$HOME/Library/LaunchAgents"
  cat > "$PLIST" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
<key>Label</key><string>$LABEL</string>
<key>ProgramArguments</key><array><string>/bin/bash</string><string>$(xml_escape "$LAUNCH")</string></array>
<key>WorkingDirectory</key><string>$(xml_escape "$RUNTIME_DIR")</string>
<key>KeepAlive</key><true/><key>RunAtLoad</key><true/>
<key>ThrottleInterval</key><integer>10</integer>
<key>StandardOutPath</key><string>$(xml_escape "$RUNTIME_DIR/stdout.log")</string>
<key>StandardErrorPath</key><string>$(xml_escape "$RUNTIME_DIR/stderr.log")</string>
</dict></plist>
EOF
}
check_permissions() {
  [ "$OS" = darwin ] || return 0
  if ! "$SERVICE_BIN" --permission-check --data-dir "$RUNTIME_DIR" --session-store "$SESSION_STORE" --permission-check-paths "$HOME/Downloads:$HOME/Documents:$HOME/Desktop"; then
    say "macOS file access not ready. Grant access to $APP_DIR in System Settings → Privacy & Security → Full Disk Access, then follow docs/DEPLOYMENT.md. Service was not started."
    die 'permission preflight failed; installed files retained for inspection, do not rerun fresh installer'
  fi
}
activate_fresh_service() {
  if [ "$OS" = darwin ]; then
    write_launchd_plist
    launchctl bootstrap "gui/$(id -u)" "$PLIST" || die 'bootstrap failed or uncertain; inspect the original service before any retry'
    local attempt pid listeners inode
    for attempt in $(seq 1 20); do
      pid=$(launchctl print "gui/$(id -u)/$LABEL" 2>/dev/null | awk '/^[[:space:]]*pid = [0-9]+/ {print $3;exit}' || true)
      listeners=$(lsof -nP -iTCP:"$PORT" -sTCP:LISTEN -t 2>/dev/null | sort -u || true)
      inode=$(stat -f '%i' "$SERVICE_BIN" 2>/dev/null || true)
      if [[ "$pid" =~ ^[0-9]+$ ]] && [ "$pid" = "$listeners" ] && lsof -a -p "$pid" -d txt -F in 2>/dev/null | awk -v path="$SERVICE_BIN" -v wanted="$inode" '/^i/{inode=substr($0,2)} /^n/{if(substr($0,2)==path && inode==wanted)found=1} END{exit !found}'; then
        say "LaunchAgent binary/listener verified on port $PORT"
        return 0
      fi
      sleep 1
    done
    die 'service health unconfirmed; inspect logs and original receipt; no automatic old-binary rollback or restart'
  else
    command -v systemctl >/dev/null 2>&1 || die 'systemd user service required; no foreground fallback'
    mkdir -p "$HOME/.config/systemd/user"
    # systemd escaping differs from shell quoting; only simple paths are supported here.
    [[ "$LAUNCH" =~ ^/[A-Za-z0-9_./-]+$ ]] || die 'Linux service path contains unsupported characters'
    cat > "$HOME/.config/systemd/user/everything-go.service" <<EOF
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
    say 'systemd start requested; Linux end-to-end qualification remains separate'
  fi
}
main() {
  local check_only=0
  case "${1:-}" in --help|-h) usage; return 0 ;; --check) check_only=1 ;; '') ;; *) usage >&2; die 'unknown argument' ;; esac
  [ "$#" -le 1 ] || die 'too many arguments'
  umask 077
  REPO="${EVERYTHING_GO_REPO:-wulalainlondon/everything-go}"
  RELEASE_TAG="${EVERYTHING_GO_TAG:-}"
  LOCAL_ARCHIVE="${EVERYTHING_GO_ARCHIVE:-}"
  LOCAL_SHA="${EVERYTHING_GO_ARCHIVE_SHA256:-}"
  EXPECTED_SOURCE="${EVERYTHING_GO_SOURCE_REVISION:-}"
  PORT="${EVERYTHING_GO_PORT:-8766}"
  RUNTIME_DIR="${EVERYTHING_GO_HOME:-$HOME/.everything-go-runtime}"
  LABEL="${EVERYTHING_GO_LABEL:-com.everything-go.app}"
  INSTANCE_NAME="${EVERYTHING_GO_INSTANCE_NAME:-$(hostname -s 2>/dev/null || echo everything-go)}"
  TUNNEL_ENABLED="${EVERYTHING_GO_TUNNEL:-0}"
  SESSION_STORE="${EVERYTHING_GO_SESSION_STORE:-$RUNTIME_DIR/saved_sessions.json}"
  LAUNCH="$RUNTIME_DIR/everything_go_launch.sh"
  APP_DIR="$RUNTIME_DIR/Everything Go.app"
  BIN="$RUNTIME_DIR/everything-go"
  PLIST="$HOME/Library/LaunchAgents/$LABEL.plist"
  OS=$(uname -s | tr '[:upper:]' '[:lower:]');NATIVE_ARCH=$(uname -m)
  case "$NATIVE_ARCH" in arm64|aarch64) ARCH=arm64 ;; x86_64|amd64) ARCH=amd64;NATIVE_ARCH=x86_64 ;; *) die 'unsupported CPU architecture' ;; esac
  case "$OS" in darwin|linux) ;; *) die 'unsupported OS' ;; esac
  validate_config
  require_tools
  assert_fresh_target
  trap cleanup EXIT
  prepare_package
  if [ "$check_only" = 1 ]; then
    say 'CHECK PASSED: package and prerequisites only; no install/service/pairing/permission changes'
    return 0
  fi
  # Serialize first-install writers; acquiring this lock never stops another service.
  mkdir -p "$(dirname "$RUNTIME_DIR")"
  local proposed_lock="$RUNTIME_DIR.install-lock"
  mkdir "$proposed_lock" || die 'another installation or retained install lock exists; inspect before retry'
  INSTALL_LOCK="$proposed_lock"
  # Recheck after potentially slow download/verification, under the install lock.
  assert_fresh_target
  mkdir -p "$RUNTIME_DIR"
  if [ "$OS" = darwin ]; then
    mv "$STAGING/Everything Go.app" "$APP_DIR"
    SERVICE_BIN="$APP_DIR/Contents/MacOS/everything-go"
  else
    mv "$STAGING/package.zip" "$BIN";chmod 700 "$BIN";SERVICE_BIN="$BIN"
  fi
  SERVICE_PATH="/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin"
  local b
  for b in "$CF_BIN" "$CLAUDE_BIN" "$CODEX_BIN"; do [ -z "$b" ] || SERVICE_PATH="$(dirname "$b"):$SERVICE_PATH"; done
  : "${BRIDGE_BROWSER_ORIGIN_MODE:=ask}" "${BRIDGE_BROWSER_ALLOWED_ORIGINS:=}" "${BRIDGE_BROWSER_MANAGE_AUTO_REVIEW:=1}" "${BRIDGE_BROWSER_ENABLE_NETWORK:=1}"
  : "${EVERYTHING_GO_CODEX_SESSIONS_DIR:=${CODEX_HOME:-$HOME/.codex}/sessions}" "${EVERYTHING_GO_CODEX_IGNORE_CWD_GLOBS:=}" "${EVERYTHING_GO_CODEX_IGNORE_NAME_PREFIXES:=}" "${EVERYTHING_GO_CODEX_ROLLOVER_ENABLED:=false}"
  : "${EVERYTHING_GO_CODEX_COLD_RESUME_MAX_BYTES:=268435456}" "${EVERYTHING_GO_CODEX_CHECKPOINT_MAX_BYTES:=131072}" "${EVERYTHING_GO_CODEX_APP_SERVER_MODE:=daemon}" "${EVERYTHING_GO_CODEX_APP_SERVER_SOCKET:=}"
  write_launch_wrapper
  check_permissions
  activate_fresh_service
  say "Connect the phone App on the same LAN and complete pairing. For remote access, use this computer's Tailscale address, port $PORT."
  if [ "$TUNNEL_ENABLED" = 0 ]; then say 'Cloudflare disabled; no cloudflared download/install/start performed.'
  else say 'Tunnel explicitly enabled; it starts only after trusted-device pairing.';fi
  say "Logs and runtime: $RUNTIME_DIR"
  say 'CLI login/quota, phone flows, notifications and audio require their own acceptance; this check does not prove them.'
}
if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then main "$@"; fi
