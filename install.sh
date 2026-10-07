#!/usr/bin/env bash
set -euo pipefail
# Compatibility entry; one canonical fixed-tag reviewed installer.
# Existing runtime configuration is inherited, no credential/profile minted.
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
url="https://github.com/wulalainlondon/everything-go/releases/download/v0.2.86/install.sh"
curl -fsSL --proto '=https' --tlsv1.2 "$url" -o "$work/install.sh"
expected="fd522bfcc45661dee766e7151a6747d223a1d5788cec6c78919c1bcb44247b11"
actual="$(shasum -a 256 "$work/install.sh" | awk '{print $1}')"
[ "$actual" = "$expected" ] || { echo "canonical installer checksum mismatch" >&2; exit 1; }
bash "$work/install.sh" "$@"
