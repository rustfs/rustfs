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

Record the RustFS source revision and binary identity, effective scanner controls, exact S3 object/version inventory, per-bucket value and provenance, original certificate or observation times, pending work, source coverage and relevant request errors. Compare the same workload and configuration before and after a change. Preserve HTTP 503 counts and latency evidence separately from accounting convergence so an improved snapshot timestamp cannot conceal foreground regressions.

The companion [auto-testing scanner usage suite](https://github.com/rustfs/auto-testing) covers public observations and candidate-native race assertions. Its deterministic cases require evidence from the exact candidate test executable. Missing internal proof, an unavailable hook, a failed cleanup or an incomplete source matrix must remain a reported limitation instead of a passing case. Multi-node load and rolling-version acceptance still require execution in the intended test environment.
