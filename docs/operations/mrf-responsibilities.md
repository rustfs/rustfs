# Retained MRF responsibilities

**Use this when:** a legacy partial-write responsibility has no source bucket identity, or the MRF controller reports an unreadable checkpoint.

**Source of truth:** `crates/heal/src/heal/mrf_queue.rs` (`read_replay_source`, `MrfRuntime::flush`), `crates/heal/src/heal/mrf_queue/partial_write.rs` (`probe_unknown_generation`, `list_unverified_legacy`), and `rustfs/src/admin/handlers/heal.rs` (`MrfLegacyResponsibilitiesHandler`).

## Unknown bucket identity

`GET /rustfs/admin/v4/heal/mrf/responsibilities` lists retained responsibilities on the receiving node. Follow `nextCursor` until it is absent; the maximum page size is 256. Compare responsibility IDs and original pool/set scopes, rather than matching only bucket and object names.

`legacy_generation_unobserved` means the original source identity is unknown and no current identity has been observed. Both incarnation fields remain null. A missing bucket is not evidence that this responsibility can be acknowledged or removed.

These entries stay outside object-heal dispatch. After an initial 60-second delay, the consumer rotates through a bounded probe queue, attempts at most one identity lookup per dispatch round, and limits each lookup to one second. Failed or unresolved attempts use the existing bounded backoff, with a five-second minimum and a 60-second ceiling. The queue examination budget is `RUSTFS_HEAL_MRF_REPLAY_BATCH`.

The listing exposes `lastIdentityProbeResult` (`unresolved`, `unavailable`, or `known`), `lastIdentityProbeAtMs`, and `nextIdentityProbeAtMs` when available. The result and last-probe timestamp are process-local observations. For lifecycle V1, restart resets the probe delay; these timestamps do not establish when a responsibility was first retained.

A lifecycle V2 checkpoint can contain `source_and_current_generation_unknown`. It retains the first-held timestamp, next-probe timestamp, and attempt count. Both incarnation fields remain null, and the listing exposes the first-held timestamp as `heldSinceMs`. Restart restores the remaining delay within five to 60 seconds, including after clock changes or an expired deadline. Identity probes retain their schedule after unsuccessful lookups. Risk acceptance and targeted recheck require a known current incarnation; a guessed identity cannot bypass refresh.

When a same-name bucket becomes visible, its identity is recorded as the observed current incarnation in `legacy_generation_unknown`. The source stays null. This transition does not enqueue object repair or create a repair proof. Targeted recheck remains rejected because the original generation cannot be established.

An explicit refresh distinguishes a successful lookup with no identity from a lookup failure. The former returns an unresolved-identity error; permission, quorum, transport, and invalid-identity failures return service unavailable without exposing internal storage details. Neither case is reported as a proven incarnation change.

## Unreadable checkpoint

A committed lifecycle companion with an unknown state, unsupported version, invalid contents, mismatched owner/sequence, conflicting replicas, or no readable replica must not be treated as an absent legacy companion. A genuinely absent companion remains compatible with older snapshots.

If replay cannot read the committed owner, the consumer retains the original payloads, manifests, lifecycle companions, and compatibility mirrors. It rejects durable admission, lifecycle queries/actions, snapshot publication, and cleanup. It also stops object dispatch. The read failure is sticky for that process: restore a compatible reader or repair the storage fault, then restart and inspect the retained files again.

Do not delete a companion, journal, or commit manifest to clear this condition. An empty API response would be misleading; the API returns service unavailable while the owner is unreadable.

## Format and rollout boundary

The writer is disabled by default. With `RUSTFS_MRF_LIFECYCLE_V2_ENABLE=false`, owners that have not read a V2 checkpoint continue to emit lifecycle format version 1. A source-less `Active` record still means a parked legacy responsibility, and observing a current incarnation uses the existing `LegacyGenerationUnknown` state.

The reader accepts lifecycle formats 1 and 2. Replaying a V2 owner preserves that format in successor checkpoints, even after the current bucket becomes visible; it never writes the new state inside a V1 envelope. A valid V2 unknown-generation state cannot contain a source incarnation. Unknown fields, missing retry fields, unsupported versions, and mismatched envelope/payload versions remain unreadable.

Peers can query a dedicated MRF lifecycle reader capability through the authenticated heal-control RPC. Its response binds the maximum readable lifecycle format, topology member and process epoch to the requested topology and fresh challenge. Old peers that cannot answer this probe return an error; the general heal-control and cross-pool capabilities are not substitutes.

Set `RUSTFS_MRF_LIFECYCLE_V2_ENABLE=true` only after deploying compatible readers to every member and qualifying the rollback binary. For each new unresolved-generation state, the writer requires a fresh authenticated observation of every configured remote member and its own decoder. Missing, unreachable, duplicate, unknown, or older members prevent promotion. Each observation has a five-second deadline. One observation establishes the publication permit; a second, shared by all replicas, rechecks the fleet before the first manifest commit. The permit has a fixed 30-second deadline that later probe refreshes cannot extend. Every manifest commit checks the current permit locally. Local disk workers retain the publication permit through native I/O, including after an async timeout or cancellation, and recheck it before the publishing rename.

The capability probe does not persist a cluster version floor. The minimum rollback reader must support lifecycle format 2 and preserve unknown-generation records on restart and subsequent flush. The initial qualified rollback candidate is [d8fe88ad4cb57c74f5061fccf6ffc1b4932368aa](https://github.com/rustfs/rustfs/commit/d8fe88ad4cb57c74f5061fccf6ffc1b4932368aa), which includes the V2 reader and the dedicated capability response. Pin and verify the deployed binary or image digest; this commit is not a released-version guarantee. Disabling the writer prevents new promotions but does not downgrade an existing V2 owner. Keep a compatible reader on every member and never convert the retained state to `Active` or remove its companion to force a rollback.

## Interrupted writer publication

The consumer keeps the original visible responsibility until a matching payload, lifecycle companion, and manifest have committed. It preserves the responsibility ID, intent digest, scope, and unknown source identity. The first successful unresolved lookup in the current process establishes the first-held timestamp; denied writer admission does not reset it. V1 data cannot establish observations made before a previous restart. A compatibility-mirror failure after that commit leaves the owner in V2 and retries the mirror; it cannot undo the committed state.

An uncertain publication freezes that owner's checkpoint body. Listing remains available, while new durable admissions and lifecycle actions return a persistence error. The consumer retries the same proposal within the existing five-to-60-second backoff. An exact owner/sequence/body readback can confirm a lost successful result without requesting new writer authority. Conflicting or unreadable evidence remains retained. Restore reader connectivity or the storage fault and let this retry path proceed; do not delete journals, companions, or manifests.
