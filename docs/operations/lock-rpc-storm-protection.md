# Lock RPC storm protection

**Use this when:** a slow lock endpoint turns into cluster-wide `Remote lock RPC timed out`, `Evicting cached remote lock connection`, and `GOAWAY too_many_resets` log floods, or when you tune how the remote lock client reacts to per-request deadlines (rustfs#7363).

## What the client does on a failed lock RPC

Every remote lock call (`lock`, `lock_batch`, `release`, `refresh`, `force_release`, `check_status`, and the readiness `ping`) runs under the deadline from `RUSTFS_OBJECT_LOCK_RPC_TIMEOUT_MS` (readiness uses `RUSTFS_HEALTH_LOCK_ONLINE_TIMEOUT_MS`). A deadline only says that one stream was slow; it says nothing about the shared HTTP/2 channel it ran on. The client therefore keeps a small per-peer history and decides per failure:

| Failure | Verdict | Effect |
| --- | --- | --- |
| Deadline expired, peer completed any lock RPC within two deadlines | `peer_recently_served` | Channel kept. The peer is slow, not gone. |
| Deadline expired, peer quiet for longer than two deadlines | `evict` | Cached channel evicted once, then the next request re-dials. |
| Any failure while the last eviction is younger than the cooldown | `cooling_down` | Channel kept so the fresh dial can prove itself; no re-dial burst. |
| Transport failure (refused, reset, `GOAWAY`) outside the cooldown | `evict` | Cached channel evicted once. |

A timed-out request is no longer cancelled. Cancelling sends `RST_STREAM`, and enough resets against a server that is slow to accept streams make it answer `GOAWAY too_many_resets`, which kills every stream on the connection and restarts the loop. Instead the stream is detached: it keeps running in the background (bounded by the internode RPC timeout), the caller still gets its timeout error, and if the peer grants a lock after the caller gave up the client releases it immediately instead of leaving an orphan for the lease to expire.

Unlocks that fail three quick retries no longer stop there. The background task continues with a deferred schedule (1s, 2s, 4s, 8s, 16s) before it gives up and leaves the entry to the server-side lease.

## Configuration

| Environment variable | Default | Behavior |
| --- | ---: | --- |
| `RUSTFS_OBJECT_LOCK_RPC_TIMEOUT_MS` | `3000` | Per-request deadline for remote lock RPCs. |
| `RUSTFS_OBJECT_LOCK_RPC_EVICTION_COOLDOWN_MS` | `5000` | Minimum interval between channel evictions per peer. `0` restores eviction on every qualifying failure. |
| `RUSTFS_OBJECT_LOCK_RPC_DETACHED_LIMIT` | `256` | How many timed-out lock RPCs per peer may keep running in the background. Beyond the budget a timed-out stream is cancelled as before. |

## Metrics

| Metric | Labels | Meaning |
| --- | --- | --- |
| `rustfs_remote_lock_rpc_timeouts_total` | `peer`, `op` | Remote lock RPCs that exceeded their deadline. |
| `rustfs_remote_lock_channel_evictions_total` | `peer`, `trigger` | Cached channel evictions; `trigger` is `timeout` or `transport`. |
| `rustfs_remote_lock_channel_evictions_suppressed_total` | `peer`, `verdict` | Failures that kept the channel; `verdict` is `peer_recently_served` or `cooling_down`. |
| `rustfs_remote_lock_rpc_detached_total` | `op`, `outcome` | Timed-out RPCs left running (`detached`) or cancelled for budget (`aborted`). |
| `rustfs_remote_lock_rpc_late_completions_total` | `op`, `outcome` | How detached RPCs ended (`success`, `error`, `join_error`). |
| `rustfs_remote_lock_late_releases_total` | `outcome` | Releases of locks granted after their caller timed out (`released`, `partial`, `failed`). |

## Reading an incident

A healthy-but-slow endpoint now shows a rising `rustfs_remote_lock_rpc_timeouts_total{peer}` with `evictions_suppressed_total{verdict="peer_recently_served"}` and at most one eviction per cooldown. A dead endpoint shows `evictions_total{trigger="transport"}` once per cooldown while the connection re-dials. Sustained `GOAWAY too_many_resets` in the server log means detached streams are being cancelled, which only happens once `RUSTFS_OBJECT_LOCK_RPC_DETACHED_LIMIT` is exhausted; raise the limit or fix the slow lock service (`http_request_inflight_slow` on `NodeService/Lock` names the endpoint).

The client code lives in `crates/ecstore/src/cluster/rpc/remote_locker.rs`; the deferred unlock schedule lives in `crates/lock/src/distributed_lock.rs`.
