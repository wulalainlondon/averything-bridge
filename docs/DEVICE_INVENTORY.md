# Authenticated device version inventory

`everything-go --mode=devices --data-dir=/path/to/runtime` queries the running
Bridge through a local Unix socket. It does not start another Bridge. There is
no public HTTP/WebSocket device-list endpoint. The socket is mode 0600 beneath
a mode-0700 directory; use SSH as the runtime owner for remote administration.

Optional `client_info` on existing `hello` and `ping` frames contains:

```json
{"app_id":"com.morrie.text","platform":"ios","version":"1.2.60","build":"65","channel":"apple-sandbox"}
```

The credential must authenticate and match the pairing registry's exact device
ID. Reports without that binding (including standalone shared environment-token
authentication) are not recorded. Metadata is client-reported, not remote device
attestation. Copies of both a credential and its device ID cannot be distinguished.
`connection_probe:true` on a discovery/pairing hello suppresses observations;
a primary hello promotes that same connection. Old clients need no changes and
remain `not_reported`. These optional fields do not change protocol version 3.

The protected `device_inventory.json` persists version, build, channel, last-seen
time and pairing status. No raw credential is stored; internal credential keys
use a random-salted HMAC. Registered IDs remain private on disk and are omitted,
along with credential hashes and salt, from query output. Public IDs are random.
Names are user-provided labels. Online requires a live connection and an
observation within 90 seconds; restarting clears online state, not version history.
Revocation marks records unpaired. Inventory is capped at 256 retained records.

## Release targets

Install `device_release_targets.json` in the data directory only after verifying
that the intended release is available in its distribution channel:

```json
{
  "schema_version": 1,
  "releases": [
    {"app_id":"com.morrie.text","platform":"ios","version":"1.2.60","build":"65","channel":"apple-sandbox","order":1}
  ],
  "targets": [
    {"app_id":"com.morrie.text","platform":"ios","version":"1.2.60","build":"65","channel":"apple-sandbox"}
  ]
}
```

The catalog is reloaded on each query. Matching requires platform, app ID,
channel, marketing version AND build. Ordering is explicit per platform/app/channel,
not guessed from strings. Unlisted versions are `unrecognized_version`, unknown
channels are `channel_unknown`, and absent targets are `target_not_set`.
Malformed catalogs produce `target_unavailable`, never an up-to-date result.

iOS obtains native bundle metadata and the verified StoreKit AppTransaction
environment. `apple-sandbox` covers TestFlight and other Apple sandbox contexts;
it is deliberately not named `testflight`. DEBUG reports `development`; failures
or older OS versions report `unknown`. Android currently reports native version
with unknown channel. Web clients do not invent a native version. No transaction
payload, receipt, purchase details or Apple account information is transmitted.

Publishing or uploading a build does not prove that a device installed it: only
its next authenticated report can establish an exact target match.
