# Averything Bridge — support and compatibility entry

The canonical Go source and new macOS ARM64/Intel release is [everything-go](https://github.com/wulalainlondon/everything-go). Both the App setup link and this support entry use the same fixed reviewed payload; this repository is not a second writable backend.

## Planned v0.2.86 installation

```bash
curl -fsSL https://github.com/wulalainlondon/everything-go/releases/download/v0.2.86/install.sh | bash
```

v0.2.86 remains planned until the controller publishes the independently accepted artifacts and verifies the official link/checksums. The root `install.sh` compatibility shim fetches only that canonical installer and verifies its fixed SHA256 before running it. Existing user paths/configuration are preserved by the canonical installer. No latest-tag guessing or different backend payload is used.

The release adds the currently reviewed authenticated Task API/queue/task-mode projections and Codex realtime handler, preserving Claude policy/context-known-zero, upload2GiB/root scope/storm and device guards. Installed/logged-in compatible CLI/account/voice availability are still required; models/providers are not interchangeable. PM narrower targets, background/steer/active cancellation remain unsupported where scope cannot be enforced. Unknown historical dispatch origins require lawful human confirmation.

[Release/source/signing and rollback gates](https://github.com/wulalainlondon/everything-go/blob/release/public-v0.2.86/docs/RELEASE.md). macOS helpers for remote capture require14; the Go service minimum is12. New Linux/Windows packages are not part of this release preparation.

## Historical releases

[v0.2.1](https://github.com/wulalainlondon/averything-bridge/releases/tag/v0.2.1) and [v0.2.77](https://github.com/wulalainlondon/everything-go/releases/tag/v0.2.77) keep their original bytes. Existing Windows/Linux installers and the historical migration source remain available as history; they do not supply current Task API/realtime capabilities. No tag rewrite or force push is permitted.

Privacy: [policy](docs/privacy.html). Paired runtime credentials/config/data remain on the operator host and never enter public binaries/source. Swift/Mac App Store and iOS packaging belong to their respective owners.
