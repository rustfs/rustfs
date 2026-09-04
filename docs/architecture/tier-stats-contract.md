# Tier Stats Contract

**Use this when:** changing what `GET /rustfs/admin/v3/tier-stats` returns, adding a tier accounting source, or wiring a metric to a remote tier request.
**Source of truth:** `rustfs/src/admin/handlers/tier.rs` (`GetTierInfo`, `tier_stats_body`), `crates/ecstore/src/services/notification_sys.rs` (`ClusterTierDailyStats`), `crates/ecstore/src/bucket/lifecycle/tier_last_day_stats.rs` (`LastDayTierStats`), `crates/ecstore/src/services/tier/warm_backend.rs` (`MeteredWarmBackend`), `crates/obs/src/metrics/schema/tier.rs`.

## Two quantities, never one

A tier carries two unrelated numbers, and reporting either as the other is the defect this contract exists to prevent.

- **Stored inventory** — how many bytes, objects and versions currently live in the tier. It is produced by the scanner, persisted in the data usage snapshot (`DataUsageInfo::tier_stats`), and is already cluster-wide. It is a level, not a rate.
- **Rolling activity** — how many transitions the cluster completed into the tier during the last 24 hours. Each node keeps its own 24-bin ring in memory (`TransitionState::add_lastday_stats`) and counts only the transitions it completed itself. It is a rate window, not a level, and it is lost on restart.

Neither substitutes for the other. An empty rolling window does not mean an empty tier, and a populated tier does not imply recent activity.

## Response contract

`contractVersion` names the body shape.

Version 1 was a bare map of tier name to the answering process's rolling counters, with nothing in the body distinguishing a node from the cluster or a rate from a level. It remains reachable at `?format=legacy` for callers pinned to it; it is not extended.

Version 2 is the default body:

- `inventory.status` is `accounted`, `not-accounted`, or `unavailable`. Per-tier `inventory` values are present only under `accounted`. An absent per-tier accounting means "not accounted", never "zero" — the scanner classifies objects by tier only once a tier exists, so a zero would be indistinguishable from an unscanned cluster.
- `activity.status` is `complete` or `partial`, with `nodesReporting`, `nodesExpected`, and the `unavailableNodes` that could not be asked, timed out, or answered with a ring this build refuses to merge. A peer that predates the `TierDailyStats` RPC answers `UNIMPLEMENTED` and is reported as unavailable rather than as zero activity.
- Each tier entry carries `type` only when the name is a configured remote tier. A name that carries stats without a configuration is a local storage class the scanner accounts for, or a tier removed since the snapshot.

Field names inside the counter objects are `totalSize`, `numVersions`, `numObjects` — the camelCase spelling admin clients expect from the `madmin` tier stats shape, rather than the Rust field names version 1 leaked. This aligns the spelling; it is not a claim that the envelope is byte-compatible with a specific `madmin` release, and a client-side check belongs with whatever client a release wants to support.

## Why merging rings is not summing totals

Nodes are asked concurrently under a per-peer deadline, and each answer is merged with `LastDayTierStats::merge` rather than added. Merging ages the older ring forward to the newer ring's clock first, so a node that stopped transitioning yesterday contributes only the hours still inside the rolling day. Adding raw totals would keep expired hours alive for as long as the node stays up.

Double counting is prevented at the source, not at the aggregator: `add_lastday_stats` runs once per committed transition on the node that committed it, so a transition retried on another node is counted once, by whichever node finally committed it.

## Tier request metrics

`rustfs_tier_requests_success` and `rustfs_tier_requests_failure` are updated at the two seams every remote tier request passes through: `MeteredWarmBackend`, which wraps every backend `new_warm_backend` builds, and `MeteredTransitionCandidateReconciler`, which wraps the separate recovery probe handle `new_transition_candidate_reconciler` builds. A new provider is therefore counted by construction. Two seams are deliberately delegated without a counter: `validate`, whose trait default performs no remote request on every backend but one, and a `probe_transition_candidate` that answers `Unsupported`, which is the same default. Counting either would report requests that were never issued.

The label set is closed by two enums in `crates/scanner-metrics/src/metrics.rs`: `TierRequestOperation` (`put`, `get`, `remove`, `probe`, `in_use`) and `TierRequestOutcome` (`success`, `backend_error`, `timeout`, `cancelled`). Tier names, endpoints and object keys must never become labels — the endpoint carries credentials in its userinfo form and the key is unbounded.

`timeout` and `cancelled` are recognised from `std::io::ErrorKind`, never from an error message, so a message that mentions an endpoint cannot reach a label. Until the transition client grows bounded deadlines (rustfs/backlog#2204), few failures actually carry `TimedOut`, so most land in `backend_error`; the classification is the seam that work extends, not a claim that timeouts are already distinguishable.

The outcome classifies the request only. A transition whose remote PUT succeeded and whose local commit then failed is a `success` here: the remote service did perform the request, and recording it as a tier failure would hide a leaked remote object behind an apparent backend outage. Local commit failure is observable through the ILM task-event metrics instead.
