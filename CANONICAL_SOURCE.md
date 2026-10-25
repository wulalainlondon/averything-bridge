# Canonical Go Bridge source

This repository is the sole writable source for the production Go Bridge.

The sibling `bridge/` checkout is a generated public mirror. Do not implement
or merge product changes there. From the workspace root:

```sh
tools/sync_go_public_mirror.sh --check
tools/sync_go_public_mirror.sh --sync
```

The sync command refuses a dirty mirror. Every signed macOS application embeds
`Contents/Resources/release-provenance.json` with this repository's commit,
dirty flag and content hash.
