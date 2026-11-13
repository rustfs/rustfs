# Scanner Bucket Mutation Watches

**Use this when:** changing scanner reuse across cycles, adding an object mutation producer, moving a physical commit into detached work, or changing the bucket watch RPC and its compatibility fallback.
**Source of truth:** `UsageWatchRegistry` in `crates/ecstore/src/runtime/usage_watch.rs`, `DiskUsageWatchBinding` in `crates/ecstore/src/disk/usage_watch.rs`, `ScannerUsageWatchProof` in `crates/scanner/src/scanner_io/usage_watch.rs`, and the canonical wire encoding in `crates/protos/src/usage_watch.rs`.

## Scope of the proof

A watch proves that one bucket's counted namespace stayed unchanged within the same producer session. It does not acknowledge dirty work, prove durable object contents, replace quorum, or authorize root publication. Bucket incarnation, the complete producer owner set, and the topology and movement identity are separate required inputs.

The registry linearizes watch installation, mutation begin, mutation end, and observation under one short synchronous lock. Every admitted user-bucket mutation is tracked even when no watch exists yet. Installing a watch during an earlier mutation therefore returns an active count. Both begin and the physical tail's end advance the installed revision; logical cancellation cannot expose an idle interval while detached work is still changing storage.

An unchanged interval requires matching owner, process session, watch token, bucket incarnation, revision, coverage version, and coverage epoch, with no active mutation at either endpoint. Missing, expired, malformed, or unsupported evidence is Unknown. Unknown never becomes revision zero or an empty clean set.

Watch renewal keeps the original starting revision. A caller may extend its conservative local monotonic deadline after a successful renewal, but it must not replace the original proof with the renewed revision. A persisted checkpoint stores identity and revision, not a usable `Instant`; after restart, the old producer token must be checked again against the complete current owner set.

## Consumer and persistence order

The scanner combines a bucket watch with the existing structure and publication fences. It binds the original proof to the traversal checkpoint and verifies it again at EOF. An unrelated bucket mutation can leave this proof valid; a mutation in the watched bucket cannot.

The remote worker capability is authenticated independently from durable-progress and completed-observation capabilities. A coordinator sends the optional watch only after this separate handshake succeeds. Old strict readers receive their previous request and response shape. An explicit parser incompatibility removes only the watch extension before retrying the previous capabilities; a network or authentication failure does not authorize downgrade.

The worker validates the request proof before reading a reusable complete cache, verifies it again before returning such a cache, and rechecks the persisted starting proof before classifying a resumed sweep for storage. A failed resumed-sweep EOF proof becomes an observation with a fresh-sweep obligation; it cannot be saved as Complete or as a last-known-good value. The companion, cleared traversal cursor, and retry obligation remain in the same cache object. An ordinary single-pass result keeps the existing CompletePending behavior; its original proof remains attached so coordinator validation marks the changed bucket pending and excludes it from certification and ACK. The root publisher still independently requires all source and epoch fences before certification or ACK.

The coordinator also validates the completed bucket proofs before root publication. A successful bucket watch never bypasses the canonical root CAS, lease ownership, source completeness, or the scoped dirty-generation checks described in [scanner-usage-publication.md](scanner-usage-publication.md).

After publication and ACK processing have finished, completed cohort entries explicitly retire their producer watches. Retirement matches the original owner, producer session, bucket incarnation, and token; an old release cannot remove a replacement watch. It returns Unknown and never changes durable dirty responsibility or active physical mutation ownership. Unfinished checkpoints retain their watches. This releases the producer's bounded slots before lease expiry so earlier completed buckets cannot exclude the rest of a large inventory.

## Producer boundaries

| Mutation family | Proof ownership |
|---|---|
| PUT, lockless PUT, copy, and multipart completion | The bucket-scoped namespace owner and `rename_data_inner` carry the guard through visibility, rollback, and grouped sync work. |
| New versions, delete markers, ordinary metadata rewrites, and healing | `write_metadata_with_namespace_owner` and non-status `update_metadata` retain the owner in the metadata publication queue. |
| Single and batch deletes, rollback, and prepared-delete recovery | The delete mutation owner, settlement owner, and publish/finalize leases cover the physical operation and its detached tail. |
| Raw file publication and deletion | `write_all_internal`, owned rename/remove, and bucket deletion retain the guard inside the actual filesystem task. Durable rename also owns its final directory sync. |
| Existing-version replication status | The explicit status-only allowlist preserves the existing maintenance cadence. Unknown keys, user metadata replacement, and tier reconciliation remain counted mutations. |
| Internal staging and scanner metadata | Internal metadata volumes do not change user-bucket counts. Their eventual publication into a user bucket crosses a covered commit boundary. |

`DiskUsageWatchBinding` attaches a local disk to one explicit instance registry. Work that started before binding holds an untracked owner until its physical tails drain. Conflicting instance bindings permanently revoke coverage for the affected sessions, including queries that checked the binding before awaiting bucket metadata.

## Conservative fallback

Raw streaming writes directly into a live user volume do not yet carry an independently provable completion owner. `create_file` and `append_file` therefore revoke watch coverage for that producer session. Internal multipart and PUT staging do not use this fallback; their user-visible commit is covered separately. A restart is required before that session can advertise a fresh complete proof again.

Recovery operations whose user bucket cannot be derived from a hashed internal scope use an untracked owner. While it is active, every watch is Unknown. Resource overflow also revokes existing tokens and keeps Unknown until untracked work drains. Revision or coverage exhaustion never wraps, and replay invalidates tokens without copying durable dirty generations into watch revisions. ACK does not mutate the watch registry.

The receiver admits at most eight requests, including at most four active operations. Admitted bursts may wait for a worker within the same five-second deadline used for storage validation; overload beyond that bounded queue is rejected. Sender-side watch requests share a per-peer limit across admission, keepalive and root verification, so those paths do not independently exceed the receiver's four active slots.

Registry entry and name-byte limits, RPC batch and byte limits, lease duration, and receiver concurrency are enforced by the owning implementations. Proof encoding is bounded before use; a cluster that cannot provide a complete bounded proof retains the strict global mutation check. None of these fallbacks permits dropping pending work or treating an incomplete companion as a canonical usage value.

## Verification boundaries

Registry tests exercise installation races, isolated bucket revisions, expiry, overflow, replay, session changes, and exhaustion. Local disk tests pause at a real publication barrier after metadata is visible, cancel the logical waiter, and assert that the physical owner still keeps the watch active. ECStore tests use real PUT, object deletion, bucket deletion, and recreation through the public store APIs.

The remote worker test calls the same EOF classifier as production after a real folder scan and a later PUT. It requires a partial result and a persisted-format round trip that preserves the observation while refusing Complete and last-known-good flags. Capability tests exercise the real HTTP negotiation with current, strict old, missing-proof, and stale-proof peers.

`watch_contention_cost_preserves_all_mutation_boundaries` reports the registry's contended begin/end cost and verifies that no revision update is lost. It is a synchronization microbenchmark. S3 throughput, latency, HTTP 503 frequency, and freshness still require matched base and candidate workload runs with recorded binary identities; this microbenchmark does not establish those results.
