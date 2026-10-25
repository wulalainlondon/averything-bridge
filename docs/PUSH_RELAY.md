# Managed push relay — first implementation

Status: implemented and locally verified, **not deployed**. Existing iOS/Android
releases and production Bridges are unchanged. No published Firebase key has
been revoked by this change. The previously exposed key must still be retired
before a managed relay can enforce access to the project's push channel.

## Customer experience

1. Install a Bridge configured with the operator's managed relay URL.
2. Pair the existing mobile App with the Bridge normally and allow notifications.
3. On the computer, open `http://127.0.0.1:BRIDGE_PORT/push/setup`.
4. Select the paired phone. Put the phone App in the background before sending
   the verification notification; the released iOS App does not necessarily
   present unknown notifications while foregrounded.
5. Enter the six-digit code shown on that phone. The code expires after five
   minutes and is locked after five incorrect attempts.

After confirmation, pushes use the relay automatically. Customers do not obtain
Google/Firebase service-account keys. A reissued FCM registration token requires
a new ownership confirmation in this first version; automatic token rotation
with independent mobile authentication is future work, not silently trusted.

The operator must publish a stable HTTPS endpoint and include its **public URL**
in the Bridge installer/configuration. This implementation intentionally does
not invent a production domain or make an unapproved deployment. The local
setup page is an initial onboarding surface; no new App Store build is required
by this wire-compatible design, but live App Store-device delivery still needs
validation before rollout.

## Credential separation

- `cmd/push-relay` runs only in the operator's trusted environment. It uses Google
  Application Default Credentials for the mobile App's existing Firebase project.
  Prefer an attached service identity with `cloudmessaging.messages.create`;
  never put a Google key into the customer Bridge or its release image.
- Each customer Bridge generates a random 256-bit broker credential in private
  runtime `push_relay_client.json` (0600). It is not a Google credential.
- The broker persists only the broker-credential digest, not its raw bearer value.
  Every request is scoped to that Bridge; enrollment cannot change an existing
  Bridge identity's authority or resurrect a disabled identity.
- Device ownership is proved through a fixed verification notification, not
  merely asserted by the Bridge. The code is never returned through the broker
  API. Unverified destinations cannot receive client-selected notification text.
- A normal send supplies a local device ID and token fingerprint. The broker
  inserts the verified token itself. Token/topic/condition/project overrides,
  cross-authority payloads, and APNs app-topic overrides are rejected.
- Changing a Bridge's relay URL cannot forward its existing credential to a
  different service. Explicit re-enrollment requires a fresh identity file.

Firebase server authentication and device targeting are described in
[FCM HTTP v1](https://firebase.google.com/docs/cloud-messaging/send/v1-api).
Keep service keys separate from distributed applications, per
[Google's key guidance](https://docs.cloud.google.com/iam/docs/best-practices-for-managing-service-account-keys).

## Run the broker on a single persistent host

From this repository:

```sh
go build -trimpath -o /private/operator/bin/push-relay ./cmd/push-relay
/private/operator/bin/push-relay \
  --firebase-project=averthing \
  --data-dir=/private/operator/push-relay-state \
  --listen=127.0.0.1:8090
```

The paths above are examples; provision owned private paths on the selected
host. The state directory must have mode 0700 and the SQLite file 0600. The
directory/database are not source or release artifacts. Back them up privately.
Use a trusted HTTPS terminator in front of the loopback listener. Do not log
Authorization headers, FCM registration tokens, request bodies, or verification
codes. Registration tokens are stored in the private database; notification
payloads are not retained in delivery receipts.

The service requires operator-provisioned ADC. On a managed Google runtime use
the attached service identity. On a trusted non-Google host, operator-managed
`GOOGLE_APPLICATION_CREDENTIALS` is possible; keep that private file outside the
repository/image. This is never customer setup.

Configure the customer Bridge using its normal data directory:

```sh
EVERYTHING_GO_PUSH_RELAY_URL=https://YOUR_MANAGED_PUSH_HOST \
  everything-go --data-dir=/PRIVATE_BRIDGE_RUNTIME --port=8766
```

`--push-relay-url` overrides the environment setting. Explicit relay mode never
falls back to a Google key if configuration, authorization or delivery fails.
Leaving the relay URL unset preserves the explicit runtime-key self-hosted mode
from the previous implementation, not the insecure embedded-key release mode.

## Cloud Run and cost boundary

The current persistence adapter is SQLite on an owned persistent filesystem.
**Do not deploy this adapter on Cloud Run's ephemeral local filesystem.** Losing
state would lose device approvals, revoked identities, rate counters and receipt
tombstones. Multiple independently stored instances are also not supported.

A Cloud Run production rollout requires a shared durable storage adapter (for
example Firestore) and request-safe maintenance scheduling. That adapter and
cloud resources have not been provisioned in this implementation. Mounting a
bucket or keeping a minimum instance alive is not a substitute for transactional
durable storage.

Cloud Run has usage-based billing and a free allowance; FCM itself is a no-cost
Firebase product. Database operations, images/builds and networking can add
charges. Small initial traffic might fit free allowances, but there is no zero-
cost guarantee. Minimum/maximum instances and budgets are cost controls, not a
hard spending cap. Confirm region, storage backend, budget and deployment
authority before creating resources.

References: [Cloud Run filesystem/lifecycle](https://docs.cloud.google.com/run/docs/container-contract#file_system),
[Cloud Run pricing](https://cloud.google.com/run/pricing),
[Firebase pricing](https://firebase.google.com/pricing).

## Public API

All requests are POST with `Content-Type: application/json` and the private
Bridge broker credential in `Authorization: Bearer …`. Remote clients require
HTTPS; literal loopback HTTP is supported only for isolated local tests.

| Path | Purpose |
| --- | --- |
| `/v1/enroll` | Create/reuse this credential's immutable Bridge identity |
| `/v1/devices/challenge` | Send a fixed ownership-verification notification |
| `/v1/devices/confirm` | Single-use confirmation with the phone's code |
| `/v1/devices/list` | This Bridge's approved IDs/fingerprints only; no raw tokens |
| `/v1/devices/revoke` | Revoke a specific original token binding |
| `/v1/notifications` | Submit an approved device's existing FCM message |
| `/healthz` | Process liveness only; not a proof of Google delivery |

Enrollment does not authorize any destination. Bridge ID, device ID and FCM
registration token are not interchangeable credentials. No public admin endpoint
exists. Operators disable an installation using its opaque broker Bridge ID:

```sh
push-relay --data-dir=/PRIVATE_RELAY_STATE --revoke-bridge=pr_BRIDGE_DIGEST
```

Revocation is read from storage on each request, including immediately before
provider submission. Already in-flight provider delivery cannot be recalled.
Unpairing locally stops token registration/delivery immediately, even if the
broker is offline. Broker revokes are persisted in the Bridge's private identity
file and retried every 30 seconds across restart. A stale queued revoke or an
old `UNREGISTERED` response cannot delete a newly confirmed token binding.

## Delivery, abuse protection and privacy

- All HTTP requests are body bounded. The service has per-source, per-Bridge,
  per-token and aggregate rate limits. Verification text is fixed, with limits
  on retries/attempts and approved devices. The IP limit deliberately ignores
  untrusted forwarded headers; behind a proxy it is an aggregate safety limit.
  Production anti-abuse/WAF and enrollment admission policy still need an
  operator review before opening public signup.
- Notification submissions carry an ID and content digest. Concurrent/repeated
  HTTP attempts do not resubmit an accepted message. Changed content under the
  same ID is rejected. IDs expire after 24 hours; receipt tombstones are kept
  for 30 days, so pruning cannot resurrect old sends.
- Only a confirmed provider rejection such as 429/5xx is retryable. A provider
  network error or crash after reservation is **uncertain**, not automatically
  resent. This is at-most-once submission, not exactly-once delivery and not a
  guarantee that a phone displayed the notification. It favors avoiding duplicate
  notifications over silently resending an ambiguous result.
- Bridge retries recheck local pairing, registration and notification privacy.
  A changed preference or unpair event stops remaining retry attempts.
- Invalid FCM tokens revoke only that exact old binding, not another device.
  Provider error bodies and unrecognized relay error text are never reflected
  into customer logs.
- Chat still connects directly to the user's Bridge. The broker nevertheless
  sees destination metadata and the existing notification payload, including
  excerpts/URLs/reply capabilities if present. This is not end-to-end encrypted
  notification transport. Existing mobile privacy settings and FCM/APNs shaping
  are preserved; do not advertise that the relay sees no user content.
- The computer setup page rejects remote, forwarded/tunnel, foreign-origin and
  DNS-rebinding requests. POSTs also require its per-boot CSRF token. It never
  exposes Google or broker credentials or raw FCM tokens.

## Verification and rollout gates

```sh
go test ./...
go vet ./...
go test -race ./internal/pushrelay ./internal/fcm ./internal/core ./cmd/everything-go
node --test scripts/check_release_secrets.test.mjs
```

Tests cover proof ownership, wrong-code lockout/expiry, cross-Bridge isolation,
payload/target injection, token rotation, revocation across outages/restarts,
concurrent receipts, lost responses, provider ambiguity/rejection, redirect
credential protection and private persistence. A real local WebSocket test uses
the unchanged mobile hello/token/preference frame, the real setup handlers and
durable broker, with only phone delivery/Google mocked. The opt-in browser
harness is in test files only; its fake-code endpoint cannot ship in production.

Before release: select hosting/durable storage; provision the server identity;
retire the exposed Google key; review public-enrollment abuse/cost controls;
then validate actual App Store iOS and Android delivery, background verification,
token rotation, unpairing and outage behavior on a canary Bridge. This source
change alone does not complete those production gates.
