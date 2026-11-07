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

A timed-out request is no longer cancelled. Cancelling sends `RST_STREAM`, and enough resets against a server that is slow to accept streams make it answer `GOAWAY too_many_resets`, which kills every stream on the connection and restarts the loop. Instead the stream is detached: it keeps running in the background (bounded by the internode RPC timeout), the caller still gets its timeout error, and if the peer grants a lock after the caller gave up the client performs bounded release retries. A per-peer cleanup budget prevents late-release retries from becoming a second request storm; entries beyond that budget remain protected by the server lease.

Three consecutive lock-acquisition timeouts open a request-level per-peer breaker. While open, new `lock` and `lock_batch` calls fail fast as retryable contention; after the backoff window one half-open probe is admitted. The breaker does not block release or refresh traffic, and it is separate from channel eviction. A per-peer in-flight admission limit also bounds requests that were already admitted before the breaker opened.

Single-resource distributed acquisition bounds each peer's contention wait to one second, but keeps pending responses and partial grants in the same quorum round until a response proves that round cannot reach quorum or the caller's remaining acquisition budget expires. A delayed response can still complete a healthy quorum; it does not restart acquisition with a fresh lock ID merely because delivery took longer than the peer's contention wait. Explicit contention failures retain rollback and retry behavior. Grants whose conservative lease deadline has expired before quorum are rolled back. Cancelling acquisition transfers both confirmed grants and pending replies to background cleanup. The caller's total deadline, read/write quorums, and fast quorum return remain unchanged.

Unlock cleanup that remains unconfirmed after three quick retries continues in the background with a deferred schedule (1s, 2s, 4s, 8s, 16s). Exhausting that schedule means cleanup was not confirmed; it does not prove that the peer still holds the lock. A server-side lease bounds any grant that remains.

Cleanup of a previously confirmed grant is idempotent for its exact `LockId`: `Released` and authoritative `AlreadyAbsent` both complete cleanup. The unlock RPCs append a structured `release_status` to each response or batch entry while preserving the existing `success` and `error_info` fields. Ordinary release and force-release still report a missing lock as failure. A new client keeps retrying unknown transport/backend results and absence responses from older peers that omit the structured status; it never infers absence from diagnostic text. Older clients ignore the added field and retain their existing behavior during upgrades or rollback.

An absence response cannot settle an acquisition that may still grant the same ID later. Pending acquisition completion retains its own cleanup owner, and uncertain remote acquisition failures keep their conservative late-release retries. Only a completed, confirmed grant can use authoritative absence to stop retrying.

## Local grant registration

The local guard registry uses nonblocking native-lock attempts for short map operations and async notifications when the table is occupied. An unpolled metadata waiter does not reserve the table, so currently running peers can register compatible grants without inheriting that waiter's scheduling delay. Notification registration precedes the retry, and unlocking precedes waking waiters. Every registry access and contention retry retains cooperative scheduling points; no executor thread waits on a blocking table-lock acquisition.

Physical acquisition and registry contention share the peer's existing acquisition deadline. An expired registration releases its unpublished physical guard and returns retryable contention, including when the table becomes available after the deadline. Replaced, removed, and reclaimed physical guards are dropped outside the registry lock. This does not change object-lock fairness, quorum, lease duration, or caller cancellation handling.

## Configuration

| Environment variable | Default | Behavior |
| --- | ---: | --- |
| `RUSTFS_OBJECT_LOCK_RPC_TIMEOUT_MS` | `3000` | Per-request deadline for remote lock RPCs. |
| `RUSTFS_OBJECT_LOCK_RPC_EVICTION_COOLDOWN_MS` | `5000` | Minimum interval between channel evictions per peer. `0` restores eviction on every qualifying failure. |
| `RUSTFS_OBJECT_LOCK_RPC_DETACHED_LIMIT` | `256` | How many timed-out lock RPCs per peer may keep running in the background. Beyond the budget a timed-out stream is cancelled as before. |
| `RUSTFS_OBJECT_LOCK_RPC_REQUEST_LIMIT` | `128` | Maximum in-flight lock acquisition RPCs admitted to a peer while its request breaker is degraded. Excess calls fail fast as retryable contention; healthy peers are not capped by this guard. |

## Metrics

| Metric | Labels | Meaning |
| --- | --- | --- |
| `rustfs_remote_lock_rpc_timeouts_total` | `peer`, `op` | Remote lock RPCs that exceeded their deadline. |
| `rustfs_remote_lock_channel_evictions_total` | `peer`, `trigger` | Cached channel evictions; `trigger` is `timeout` or `transport`. |
| `rustfs_remote_lock_channel_evictions_suppressed_total` | `peer`, `verdict` | Failures that kept the channel; `verdict` is `peer_recently_served` or `cooling_down`. |
| `rustfs_remote_lock_rpc_detached_total` | `op`, `outcome` | Timed-out RPCs left running (`detached`) or cancelled for budget (`aborted`). |
| `rustfs_remote_lock_rpc_late_completions_total` | `op`, `outcome` | How detached RPCs ended (`success`, `error`, `join_error`). |
| `rustfs_remote_lock_late_releases_total` | `outcome` | Releases of locks granted after their caller timed out (`released`, `partial`, `failed`). |
| `rustfs_remote_lock_request_suppressed_total` | `peer`, `op`, `reason` | Lock acquisitions rejected before dialing because the peer breaker is open or its degraded in-flight limit is reached. |

## Reading an incident

A healthy-but-slow endpoint now shows a rising `rustfs_remote_lock_rpc_timeouts_total{peer}` with `evictions_suppressed_total{verdict="peer_recently_served"}` and at most one eviction per cooldown. A dead endpoint shows `evictions_total{trigger="transport"}` once per cooldown while the connection re-dials. Sustained `GOAWAY too_many_resets` in the server log means detached streams are being cancelled, which only happens once `RUSTFS_OBJECT_LOCK_RPC_DETACHED_LIMIT` is exhausted; raise the limit or fix the slow lock service (`http_request_inflight_slow` on `NodeService/Lock` names the endpoint).

The client code lives in `crates/ecstore/src/cluster/rpc/remote_locker.rs`; the deferred unlock schedule lives in `crates/lock/src/distributed_lock.rs`.
