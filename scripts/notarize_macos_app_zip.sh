#!/usr/bin/env bash
set -euo pipefail
# Historical disk-P8 path is disabled. This public helper consumes only an
# operator-provisioned notarytool Keychain profile; it never creates credentials.
[ "$#" -eq 1 ] || { echo "usage: $0 <signed-app-zip>" >&2; exit 2; }
[ -n "${NOTARYTOOL_KEYCHAIN_PROFILE:-}" ] || { echo "controlled Keychain profile required; no PEM/env/temp fallback" >&2; exit 1; }
xcrun notarytool submit "$1" --keychain-profile "$NOTARYTOOL_KEYCHAIN_PROFILE" --wait
# No automatic resubmission or archive rewriting. Record the receipt and staple
# only under the independently reviewed exact-artifact promotion procedure.
