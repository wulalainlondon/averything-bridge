# 0.2.57 release scope and verification

This release promotes the existing local 0.2.56 backend feature set and repairs
the presentation lifecycle of turns initiated by a native Codex client.

## Preserved baseline

- Human-approved PM/collaboration workflows, scoped worker tools and durable recovery.
- Tool-environment inspection and explicitly confirmed maintenance.
- Session configuration revisions and immutable queued-message configuration.
- Resumable general-file uploads, attachment identity/path/digest validation and PDF admission.
- Read-only widget grants, quota metadata and independently ordered completion pushes.
- Native history/request correlation and inactivity-based Codex liveness handling.

The mobile and native desktop binaries are not part of this release.

## External turn repair

- Native turns have their own request identity, start and terminal lifecycle.
- Observation does not claim the Bridge worker or release an unrelated queue.
- Session summaries and runtime snapshots agree on streaming presentation.
- Correlated live events repair a missed start; retired IDs reject delayed events.
- A read-only watcher of known native transcripts recovers activity after a Bridge restart,
  without resuming, steering, or taking over the native client's thread.
- The watcher reads explicit lifecycle records only. It stats known paths once a second,
  does not retain per-file descriptors and bounds backward reads at 32 MiB. Old unchanged
  files are not replayed as new completions. New transcript discovery retains the existing
  native-discovery interval; this is not a guarantee of immediate discovery of a new session.

## Release checks

- Clean isolated source snapshot, preserving the developer's original worktree.
- 739 Go tests passed; the full suite also passed with the race detector.
- `go vet ./...` and CGO-free command build passed.
- Shared protocol generation/inventory checks passed against the companion contract tree.
- Targeted frontend state/row/handler regression tests and TypeScript checks passed.
- Reviewed authentication and scope checks on new APIs, additive storage changes and attachment confinement.
- Source-only credential scan found no private keys or access tokens; the sole pattern hit was an explicit no-network test fixture.

Prior feature acceptance limitations remain: this does not claim new live Claude authentication,
full desktop drag/drop/accessibility/large-file stress acceptance, or deployment to other hosts.
Production promotion additionally requires published checksums, exact Developer ID signature,
Apple notarization, stapling and Gatekeeper acceptance. Device acceptance is recorded separately
after deployment, not inferred from these tests.
