#!/usr/bin/env bash
set -euo pipefail
# Compatibility entry; one canonical fixed-tag reviewed installer.
# Existing runtime configuration is inherited, no credential/profile minted.
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
url="https://github.com/wulalainlondon/everything-go/releases/download/v0.2.86/install.sh"
curl -fsSL --proto '=https' --tlsv1.2 "$url" -o "$work/install.sh"
expected="803c29bf5ef1d9dc02e3f0dc39896fde54a5ef6597c7a9081ba99c33d60b9d5c"
actual="$(shasum -a 256 "$work/install.sh" | awk '{print $1}')"
[ "$actual" = "$expected" ] || { echo "canonical installer checksum mismatch" >&2; exit 1; }
bash "$work/install.sh" "$@"
