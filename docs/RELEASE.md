# Release & Update Flow

## Prerequisites

| Item | Value |
|------|-------|
| Repo | `wulalainlondon/everything-go` |
| Developer ID Team | `UPWLTJL6S2` |
| CI trigger | push tag matching `v*` |
| CI file | `.github/workflows/release.yml` |

The 7 macOS signing/notarization GitHub Secrets are pre-configured — do not modify them:
`MACOS_CERTIFICATE_P12_BASE64`, `MACOS_CERTIFICATE_PASSWORD`, `MACOS_KEYCHAIN_PASSWORD`,
`MACOS_CODESIGN_IDENTITY`, `APPSTORE_CONNECT_API_KEY_ID`, `APPSTORE_CONNECT_API_ISSUER_ID`,
`APPSTORE_CONNECT_API_KEY_P8_BASE64`

FCM service-account credentials are **runtime-only**. The release workflow must
not read or inject `FCM_SERVICE_ACCOUNT_JSON`, and no private key may be embedded
in the application. CI scans every compiled binary before packaging; the local
macOS packager applies the same gate.

Provision a newly issued runtime credential on each host outside the source
checkout, with owner-only permissions. The default location is
`DATA_DIR/fcm_service_account.json`; an explicit `--service-account` argument or
`EVERYTHING_GO_FCM_SERVICE_ACCOUNT` path overrides it. On macOS/Linux the file
must be private (`0600` or read-only `0400`). Missing, invalid, overly large or
non-private credentials disable FCM without stopping other Bridge services.
Provision the runtime credential **before** upgrading an embedded-key release
to avoid interrupting push notifications.

If a key was included in a publicly accessible application, treat it as exposed
and rotate/revoke it; removing an artifact alone cannot invalidate downloaded
copies. See [Google Cloud's service-account key guidance](https://docs.cloud.google.com/iam/docs/best-practices-for-managing-service-account-keys).

---

## Releasing a new version

### 1. Commit and push code changes

```bash
git add .
git commit -m "feat: ..."
git push
```

### 2. Push a version tag

```bash
git tag v0.1.2
git push origin v0.1.2
```

The CI pipeline runs automatically (~3–5 min) and:
1. Cross-compiles for `darwin/arm64`, `darwin/amd64`, `linux/arm64`, `linux/amd64`
2. Signs the macOS `.app` bundle with the Developer ID certificate
3. Submits to Apple for notarization (`notarytool submit --wait`)
4. Staples the notarization ticket back into the `.app.zip`
5. Publishes all assets to GitHub Releases with a `SHA256SUMS` file

Every macOS bundle also contains
`Contents/Resources/release-provenance.json`. Before promotion, verify its
`canonical_repository` is `go`, record the commit, dirty flag and
`source_sha256`, and keep it with the release SHA256 evidence. CI releases must
be clean (`dirty=false`); a local canary may record `dirty=true` but still must
pass the exact Developer ID signing gate.

> **Tag format is required.** The tag must match `v*` (e.g. `v0.1.2`).
> A plain commit push does not trigger the release workflow.

## Deployment architecture matrix

| Target | CPU / release asset | Port | Runtime |
|---|---|---:|---|
| Wulala | Apple Silicon `arm64` / `darwin-arm64` | 8766 | `~/.everything-go-runtime` |
| Morrie | **Intel `x86_64` (`GOARCH=amd64`)** / `darwin-amd64` | 8766 | `/Users/morrie/.everything-go-runtime-9453` (legacy data-dir name retained) |

Morrie's active launchd label is `com.morrie.everything-go-9453`; the legacy name
and data-directory suffix are retained, but the service listens on 8766. The
retired `com.morrie.everything-go` plist and former Python Bridge must remain
disabled. Everything Go owns 8766 and its tunnel directly.

Morrie is an Intel Mac. Never deploy the Wulala `darwin-arm64` bundle to it:
Developer ID verification can still succeed for the wrong architecture, but
launchd will fail with `Bad CPU type in executable`. Before replacing either
installation, verify both the host and extracted signed artifact:

```bash
uname -m
file "Everything Go.app/Contents/MacOS/everything-go"
codesign --verify --deep --strict --verbose=2 "Everything Go.app"
```

For Morrie the first two commands must report `x86_64`, and the signing
authority must remain `Developer ID Application: YuDi Huang (UPWLTJL6S2)`.

---

## Updating the local installation

```bash
EVERYTHING_GO_SKIP_PERMISSION_CHECK=1 \
  bash ~/Downloads/Helper/claude-bridge/go/install.sh
```

`install.sh` will:
- Download the new `everything-go-darwin-arm64.app.zip` from the latest release
- Replace `~/.everything-go-runtime/Everything Go.app/`
- Restart the launchd service (`com.everything-go.app`)

This local installer selects the Wulala `darwin-arm64` asset. Do not use that
asset for Morrie; deploy the signed `darwin-amd64`/`x86_64` app instead.

### Verify the update

```bash
# Service running (exit code must be 0)
launchctl list | grep everything-go

# Port 8766 listening
lsof -Pan -p $(pgrep -x everything-go) -i | grep LISTEN

# Correct Developer ID (not adhoc)
codesign -d --verbose=4 \
  ~/.everything-go-runtime/Everything\ Go.app/Contents/MacOS/everything-go 2>&1 \
  | grep TeamIdentifier
# Expected: TeamIdentifier=UPWLTJL6S2
```

---

## What does NOT need to change between releases

- `~/Library/LaunchAgents/com.everything-go.app.plist`
- `~/.everything-go-runtime/everything_go_launch.sh`
- Any GitHub Secret

---

## Troubleshooting

### Service shows exit `-9` (SIGKILL) after update

Do not ad-hoc re-sign or run a raw replacement binary. Re-run `install.sh`; it
verifies the checksum, exact Developer ID identity, Gatekeeper acceptance and
the stapled notarization ticket before replacing the installed app. If the new
release cannot pass those gates, keep the previous signed app and stop rollout.
