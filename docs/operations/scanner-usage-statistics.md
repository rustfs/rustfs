# Interpreting scanner usage statistics

Scanner publication time and bucket measurement time describe different events. A newly published response can contain a carried bucket value or a complete traversal that crossed a mutation boundary. Use the per-bucket provenance together with the enclosing snapshot's coverage and pending state; a recent `last_update` alone does not establish fresh bucket usage.

## Bucket value provenance

| `usage_value_state` | Meaning | `usage_certification` |
|---|---|---|
| `certified` | Every required source contributed to this exact bucket value under the scanner's certification fences. | The certifying epoch, cycle and original `certified_at`. |
| `carried` | The response retained the prior bucket value while newer work remains incomplete. | Preserved only when the exact prior value had a valid certificate. |
| `observed` | The value is useful diagnostic coverage without a complete bucket certificate. | Absent. |
| Missing or `unknown` | This reader cannot establish the value's provenance. | Must not be inferred from the enclosing snapshot. |

Carrying a value does not refresh its certificate. Combining fresh and old source values does not inherit the old whole-bucket certificate. Certification is an accounting result, not an assertion that no write occurred after the certified interval.

## Complete observations and compatibility

A traversal can reach its end after resuming checkpoints from a different mutation interval. Such a mixed traversal can provide a complete observation while still owing a fresh sweep. Its observation, cleared cursor and fresh-sweep debt are committed together. The scanner does not mark that mixed bucket cache complete for legacy readers.

Compatible prior values remain in the primary usage map. A bucket with no eligible prior value remains absent there until the scanner establishes the required coverage; absence is not a measured zero. Complete mixed observations are stored as additive companions with their source identities and original completion times. The administrative read path may project a validated companion into its response. That projection does not become the canonical scanner cache, a quota input, or permission to acknowledge dirty usage.

The admin projection validates bucket incarnations against storage with at most four concurrent reads and one shared two-second budget for that optional stage. Unavailable or timed-out identities keep their canonical values, while successfully validated buckets can still display observations. This bounds the projection's added delay; it does not change the underlying canonical snapshot read timeout or certify an unverified value.

Unsupported or malformed companion entries are ignored without making an otherwise readable primary snapshot unusable. Bucket recreation, source changes and mismatched snapshot identities reject obsolete companions. Old readers continue to consume the compatible primary representation and ignore the additive fields. Rollback does not require deleting scanner state.

## Budgets, publication and pending work

A bucket or source quantum limits how long one work item holds service. The parent cycle retains its own limits. Only validated, persisted forward progress earns another turn; a changed checkpoint identifier, repeated entry counters or an in-memory cursor is insufficient. A source that makes no durable progress waits for a later cycle instead of immediately spinning.

When a cycle work budget stops admission, its owner can finish already-started collection and publication within one bounded drain interval. Later stages cannot restart that interval. Losing the owner, publication lease or movement epoch still rejects publication. The maintenance cycle remains partial when its work budget was exhausted, even if independently complete bucket coverage was durably published during drain.

Acknowledgement requires the exact durable canonical root and actual fresh completion from every expected source for the bucket. A successful observation write, a carried value or completion from only one set cannot clear the remaining responsibility. An empty scoped acknowledgement does not authorize a whole-generation fallback.

See [scanner runtime controls](scanner-runtime-controls.md) for configuration, effective-value diagnostics and quantum rollout requirements. All quantum dimensions default to disabled. Candidate settings require measurements under representative namespace shape and foreground load; these controls do not promise a fixed convergence time for every bucket size.

## Verification and diagnosis

When Console and `rc bucket quota info` disagree, record the client build and capture both responses from the same node and time window. The native quota endpoint (`GET /rustfs/admin/v3/quota/{bucket}`) reports usage in `size`; `/quota-stats/{bucket}` calls it `current_usage`. Distributed quota statistics read the persisted scanner baseline, whereas the administrative data-usage response can include a newer observation. On a single-process deployment the quota report reflects known deletes even while quota admission conservatively retains those bytes. The MinIO-compatible `get-bucket-quota` response uses `size` for the configured limit. None of these display values proves how a durable reservation decided a particular write.

Record the RustFS source revision and binary identity, effective scanner controls, exact S3 object/version inventory, per-bucket value and provenance, original certificate or observation times, pending work, source coverage and relevant request errors. Compare the same workload and configuration before and after a change. Preserve HTTP 503 counts and latency evidence separately from accounting convergence so an improved snapshot timestamp cannot conceal foreground regressions.

The companion [auto-testing scanner usage suite](https://github.com/rustfs/auto-testing) covers public observations and candidate-native race assertions. Its deterministic cases require evidence from the exact candidate test executable. Missing internal proof, an unavailable hook, a failed cleanup or an incomplete source matrix must remain a reported limitation instead of a passing case. Multi-node load and rolling-version acceptance still require execution in the intended test environment.

## Console inventory and scanner status

`GET /rustfs/admin/v3/info?include=inventory` adds an `inventory` object to the usual server information response. `current_buckets` counts the current namespace. `measured_buckets`, `measured_objects`, `measured_bytes`, `snapshot_at` and `converged` describe the available scanner snapshot. An unavailable measurement is unknown. A measured bucket count can lag a new bucket even while that bucket appears in S3 listings. The request uses the existing server information permission.

`GET /rustfs/admin/v3/realtime?scope=cluster&types=1&n=1` returns one NDJSON frame containing `cluster_scanner` and `final: true` under the existing realtime metrics permission. `nodes_responded` counts configured members that supplied a supported sample. Compare it with `nodes_total`. `sampled_at` identifies the observation time, including when concurrent refreshes share the result. Scanning requires an active coordinator whose epoch and cycle match the durable scanner record. Idle requires complete coverage and an identified coordinator. Missing or contradictory evidence gives `unknown`. The optional `current`, `last_attempt` and `last_success_at` fields distinguish work in progress, a completed attempt (which may be partial), and a successful cycle. An interrupted worker records its budget reason, lost leadership or cancellation even when the drain deadline prevents completion. Successful completion time comes only from persisted cycle history. The current cycle's object count measures work performed during that cycle.

Older servers can ignore the query and return their ordinary response. Check for `inventory` or `cluster_scanner` before interpreting the corresponding response as the new contract.

## Console bucket scan progress

`GET /rustfs/admin/v3/accountinfo` may attach `scan_progress` to each authorized bucket. Its `objects` and `size` describe accepted scanned coverage, not the bucket total, a lower bound on current contents, or a percentage. Concurrent changes can invalidate any such comparison. `stage` distinguishes `enumerating`, `scanning`, `awaiting_sources` and `completed`; completion means all logical sources supplied a completed traversal sample, without granting a usage certificate. Enumeration alone leaves both counters absent. `sources_observed` and `sources_total` make missing sets explicit. `updated_at` is the oldest contributing measurement time in Unix milliseconds. Saving an unchanged frontier or retained compacted receipt, including after a cold executor handoff, does not refresh that time. Legacy receipts without measurement metadata remain reusable for scanning but cannot supply a new numeric display sample with a fabricated age.

The scanner saves compact progress companions on their contributing erasure sets after committing bucket checkpoints. Retries replace a sample instead of adding deltas, and physical replicas are alternative views of one logical source. A new executor that has not yet measured another range retains the prior accepted sample and its original timestamp. Reader-local admission epochs and wakeup counters never fence a shared diagnostic sample; its movement scope comes from persisted pool/rebalance metadata. Missing, unsupported or corrupt companions do not affect checkpoint recovery or complete usage. Admin reads bind companions to the current bucket incarnation, set layout and durable movement generation, use bounded concurrent reads, and never list S3 objects or load the full checkpoint graph. The response keeps existing `objects` and `size` fields independent; Console retains their prior values and shows scan progress separately. Optional `usage_updated_at` preserves the original bucket certificate time, independently of the progress timestamp. Older servers and clients can omit or ignore these additive fields.

The two-second Admin read budget reserves its final quarter for movement-scope validation. Each validated source contributes immediately, so a stalled source or another bucket's identity lookup cannot erase completed reads. A bucket with missing sources reports `awaiting_sources`; unavailable final scope validation still omits the diagnostic view.

Progress extraction shares the checkpoint's serialized buffers, including paged payloads, with at most one dedicated CPU worker per process. It releases movement admission after capturing the durable generation and leaves native I/O workers available for primary saves. Waiting for the worker and preparing its input are included in the two-second diagnostic preparation budget; cancellation is checked during decoding. A nonpreemptible validation may finish after its waiter expires, retaining one decoded checkpoint until it exits. Its resource permit remains held, preventing repeated saves from accumulating workers or decoded copies, and its expired result is not published. Primary and companion publication retain their existing epoch and revision checks; scanner cycle budgets are unchanged.

The adaptive best-effort backup budget covers preparation, manifest publication, reconciliation and page reclamation together. Exhausting it stops waiting for the backup without undoing a successful primary checkpoint. An uncertain backup outcome can be recovered from storage on a later turn; it does not extend the current turn's budget.
