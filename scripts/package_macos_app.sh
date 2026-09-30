#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 3 ]; then
  echo "usage: $0 <binary> <arch> <output-zip>" >&2
  exit 2
fi

BIN_SRC="$1"
node "$(dirname "$0")/check_release_secrets.mjs" "$BIN_SRC"
ARCH="$2"
OUT_ZIP="$3"
OUT_DIR="$(dirname "$OUT_ZIP")"
OUT_BASE="$(basename "$OUT_ZIP")"
mkdir -p "$OUT_DIR"
OUT_ZIP="$(cd "$OUT_DIR" && pwd)/$OUT_BASE"

APP_NAME="Everything Go"
BUNDLE_ID="${EVERYTHING_GO_BUNDLE_ID:-com.everything-go.app}"
VERSION="${EVERYTHING_GO_VERSION:-0.0.0}"
SIGN_IDENTITY="${EVERYTHING_GO_CODESIGN_IDENTITY:--}"

if [ "$SIGN_IDENTITY" != "Developer ID Application: YuDi Huang (UPWLTJL6S2)" ]; then
  echo "refusing to package release with unexpected signing identity: $SIGN_IDENTITY" >&2
  exit 1
fi

WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT

APP_DIR="$WORK_DIR/$APP_NAME.app"
MACOS_DIR="$APP_DIR/Contents/MacOS"
RES_DIR="$APP_DIR/Contents/Resources"
mkdir -p "$MACOS_DIR" "$RES_DIR"

cp "$BIN_SRC" "$MACOS_DIR/everything-go"
chmod +x "$MACOS_DIR/everything-go"
case "$ARCH" in
  arm64) SWIFT_TARGET="arm64-apple-macosx14.0" ;;
  amd64) SWIFT_TARGET="x86_64-apple-macosx14.0" ;;
  *) echo "unsupported macOS architecture: $ARCH" >&2; exit 2 ;;
esac
xcrun swiftc -parse-as-library -O -target "$SWIFT_TARGET" \
  -o "$MACOS_DIR/bridge-remote-helper" "$(dirname "$0")/../native/bridge-remote-helper.swift"
xcrun swiftc -parse-as-library -O -target "$SWIFT_TARGET" \
  -o "$MACOS_DIR/bridge-remote-stream" "$(dirname "$0")/../native/bridge-remote-stream.swift"
"$(dirname "$0")/write_release_provenance.sh" "$RES_DIR/release-provenance.json"

cat > "$APP_DIR/Contents/Info.plist" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>CFBundleDevelopmentRegion</key><string>en</string>
  <key>CFBundleDisplayName</key><string>$APP_NAME</string>
  <key>CFBundleExecutable</key><string>everything-go</string>
  <key>CFBundleIdentifier</key><string>$BUNDLE_ID</string>
  <key>CFBundleInfoDictionaryVersion</key><string>6.0</string>
  <key>CFBundleName</key><string>$APP_NAME</string>
  <key>CFBundlePackageType</key><string>APPL</string>
  <key>CFBundleShortVersionString</key><string>$VERSION</string>
  <key>CFBundleVersion</key><string>$VERSION</string>
  <key>LSBackgroundOnly</key><true/>
  <key>LSMinimumSystemVersion</key><string>12.0</string>
  <key>LSUIElement</key><true/>
</dict>
</plist>
EOF

command -v codesign >/dev/null 2>&1 || { echo "codesign is required" >&2; exit 1; }
codesign --force --deep --options runtime --timestamp --sign "$SIGN_IDENTITY" "$APP_DIR"
codesign --verify --deep --strict --verbose=2 "$APP_DIR"

(cd "$WORK_DIR" && ditto -c -k --sequesterRsrc --keepParent "$APP_NAME.app" "$OUT_ZIP")
echo "wrote $OUT_ZIP"
