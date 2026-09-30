#!/bin/sh
set -eu

if [ "$#" -ne 1 ]; then
  printf '%s\n' 'usage: write_release_provenance.sh <output-json>' >&2
  exit 2
fi
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
OUT=$1
COMMIT=$(git -C "$ROOT" rev-parse HEAD)
DIRTY=false
if [ -n "$(git -C "$ROOT" status --porcelain --untracked-files=all)" ]; then DIRTY=true; fi
SOURCE_SHA=$(
  {
    git -C "$ROOT" rev-parse HEAD
    git -C "$ROOT" diff --no-ext-diff --binary HEAD
    git -C "$ROOT" ls-files --others --exclude-standard | LC_ALL=C sort | while IFS= read -r file; do
      shasum -a 256 "$ROOT/$file"
    done
  } | shasum -a 256 | awk '{print $1}'
)
mkdir -p "$(dirname "$OUT")"
printf '{"canonical_repository":"go","commit":"%s","dirty":%s,"source_sha256":"%s"}\n' "$COMMIT" "$DIRTY" "$SOURCE_SHA" > "$OUT"
