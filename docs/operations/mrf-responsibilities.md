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

Owners that have not read a V2 checkpoint continue to emit lifecycle format version 1. A source-less `Active` record still means a parked legacy responsibility, and observing a current incarnation uses the existing `LegacyGenerationUnknown` state. No new persistent state is activated by admission or identity probing.

The reader accepts lifecycle formats 1 and 2. Replaying a V2 owner preserves that format in successor checkpoints, even after the current bucket becomes visible; it never writes the new state inside a V1 envelope. A valid V2 unknown-generation state cannot contain a source incarnation. Unknown fields, missing retry fields, unsupported versions, and mismatched envelope/payload versions remain unreadable.

Creating the new state requires a separate writer rollout. The existing heal-control and cross-pool protocol capabilities do not prove support for it. New writers require pinned read/restart evidence for every supported reader and an explicit minimum rollback reader. Reader support alone does not qualify writer activation; do not downgrade past that reader or silently convert an unknown state to `Active`.
