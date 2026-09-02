# NATS JetStream Operations Guide

**Use this when:** you need at-least-once delivery of notification or audit events to NATS, must pre-provision and size the stream RustFS publishes to, or are diagnosing JetStream delivery failures.
**Source of truth:** `crates/config/src/constants/targets.rs` (`NATS_JETSTREAM_*` keys and the ack-timeout bounds), `crates/config/src/notify/nats.rs` and `crates/config/src/audit/nats.rs` (env names), `crates/targets/src/target/nats/{validation,jetstream}.rs` (stream validation, `retry_lifetime`, outcome classification), `crates/targets/src/runtime/mod.rs` (`REPLAY_MAX_RETRIES`, `REPLAY_BASE_RETRY_DELAY`), `crates/targets/src/store.rs` (`FAILED_STORE_MAX_ENTRIES`, `FAILED_STORE_TTL`).

## What the JetStream path does

By default the NATS notify and audit targets publish with NATS Core, which returns once the message is written to the socket; a failure between that write and the server persisting the message loses the event. With JetStream enabled, each event is first written to the local store-and-forward queue and then published to a JetStream stream with a stable `Nats-Msg-Id` header; the queue entry clears only after the server returns a publish acknowledgement (the stream leader has accepted and sequenced the message) or after a terminal rejection has been recorded in the failed-events store. An unacknowledged event survives a process restart on disk and replays.

The path is opt-in and off by default, and applies to both the notify NATS target and the audit NATS target. RustFS never creates the stream: the operator owns the stream and its retention, storage, and replication policy. RustFS validates that the stream exists and is writable, reports a validation failure otherwise, and a store-backed target keeps queueing events through a failed validation until the stream is repaired.

## Configuration

Each key has a configuration-key form and an environment-variable form per target; the audit target uses the `RUSTFS_AUDIT_NATS_` prefix in place of `RUSTFS_NOTIFY_NATS_`.

| Key | Env (notify) | Required when enabled | Default / range | Meaning |
| --- | --- | --- | --- | --- |
| `jetstream_enable` | `RUSTFS_NOTIFY_NATS_JETSTREAM_ENABLE` | — | off | Turns the JetStream path on for the target |
| `jetstream_stream_name` | `RUSTFS_NOTIFY_NATS_JETSTREAM_STREAM_NAME` | yes | none | Pre-provisioned stream to publish to |
| `jetstream_ack_timeout_secs` | `RUSTFS_NOTIFY_NATS_JETSTREAM_ACK_TIMEOUT_SECS` | no | `NATS_JETSTREAM_ACK_TIMEOUT_DEFAULT_SECS` (30); accepted range `NATS_JETSTREAM_ACK_TIMEOUT_MIN_SECS`..=`NATS_JETSTREAM_ACK_TIMEOUT_MAX_SECS` (10..=120) | How long a publish waits for an acknowledgement before it is treated as timed out and retried. Each attempt, including connection establishment, is bounded by this deadline |
| `queue_dir` | `RUSTFS_NOTIFY_NATS_QUEUE_DIR` | yes | none | Local store-and-forward queue directory; durability needs a local store to replay from |
| `queue_limit` | `RUSTFS_NOTIFY_NATS_QUEUE_LIMIT` | no | target default | Maximum live-queue entries; see sizing below |

```bash
RUSTFS_NOTIFY_NATS_JETSTREAM_ENABLE=true
RUSTFS_NOTIFY_NATS_JETSTREAM_STREAM_NAME=RUSTFS_EVENTS
RUSTFS_NOTIFY_NATS_JETSTREAM_ACK_TIMEOUT_SECS=30
RUSTFS_NOTIFY_NATS_QUEUE_DIR=/var/lib/rustfs/notify-nats
```

Enabling the path without a stream name or without a queue directory, or with an out-of-range acknowledgement timeout, is rejected at startup and in the admin validation path (`validate_jetstream_settings`). The default 30 s timeout suits production: a server replicating to several replicas acknowledges only after replication and the deferred fsync, which legitimately takes well over 100 ms at the tail.

A mistyped key name is not a value error and escapes that check: the key reads as absent and the target silently stays on the NATS Core path. After enabling, confirm the target reports its JetStream fields on startup and logs the stream-validation success line naming the configured stream; absence of that line for a target meant to be enabled indicates an unrecognised key name.

## Stream requirements

RustFS reads the stream once at target init and in the admin validation path, and reports a validation failure rather than publishing into a stream where writes would silently fail. Revalidation runs while the verdict is unset and stops once one validation passes, resuming only after the verdict is reset (reconnect, TLS rotation, an acknowledgement naming an unexpected stream, or a stream-not-found publish outcome). The stream must:

1. Exist and be writable. A missing or unreachable stream fails validation; a stream provisioned after the path is enabled picks up the queued events on the next replay, because stream-not-found is retryable.
2. Capture the configured publish subject in its subject filter, by literal match or NATS wildcard.
3. Acknowledge writes (`no_ack` false); otherwise the queue would never clear.
4. Not be sealed.
5. Set a duplicate window of at least the worst-case retry span (next section).

Choose retention, storage type, and replica count for the durability the deployment needs. Use file storage if a server restart must preserve already-persisted events. For durability across a node failure, provision at least 3 replicas: an acknowledgement returns after the stream leader commits, and a leader that acknowledges and then fails before replicating to a quorum can lose that message on failover, so a single-replica stream is durable only against a clean restart.

## Duplicate window and the retry span

The `Nats-Msg-Id` is minted once when the event is queued and reused on every retry and replay of that entry, so the server collapses duplicates within the stream duplicate window. The window must therefore cover the worst-case span from the first publish attempt of a stored entry to its last within one replay cycle, or a late retry of an already-persisted event is delivered twice.

`retry_lifetime` (`crates/targets/src/target/nats/jetstream.rs`) computes that span, and validation rejects a stream whose duplicate window is below it, naming the configured and required window in the log line:

```text
duplicate_window >= REPLAY_MAX_RETRIES * ack_timeout_secs
                  + inter_attempt_backoff_sum(REPLAY_MAX_RETRIES)
                  + replay_backoff_term(REPLAY_MAX_RETRIES)
```

- `REPLAY_MAX_RETRIES` is the number of publish attempts per replay cycle.
- The sleep before the retry at shift `n` is `replay_backoff_term(n) = REPLAY_BASE_RETRY_DELAY * 2^n`; `inter_attempt_backoff_sum` adds the terms for shifts `1..REPLAY_MAX_RETRIES` (no sleep follows the last attempt).
- The final `replay_backoff_term(REPLAY_MAX_RETRIES)` is a deliberate headroom term above the realized span.

With the constants as shipped (5 attempts, 2 s base) the backoff sum is 60 s and the headroom 64 s, so the default 30 s acknowledgement timeout requires a window of at least 274 s; each extra second of acknowledgement timeout adds `REPLAY_MAX_RETRIES` seconds to the required window. Raise the stream duplicate window in step whenever you raise `jetstream_ack_timeout_secs`.

The validated window covers one retry cycle. An entry that exhausts a cycle without delivering stays on the live queue and is retried on a later cycle, so an entry surviving across cycles can be delivered again — consistent with at-least-once delivery. Throttling `RUSTFS_NOTIFY_TARGET_STREAM_CONCURRENCY` below the number of concurrently backlogged targets inserts untimed waits between the retries of one entry and can push a late retry past the window; keep the default or add window margin when lowering it.

## Retryable versus terminal outcomes

Every publish outcome falls into one of two families, classified once in `crates/targets/src/target/nats/jetstream.rs`. Retryable conditions keep the entry on the live queue and retry until it delivers or an operator intervenes; they never reach the failed store, because a misclassification there would lose an entry once the failed-store retention lapses. Terminal conditions move to the failed store immediately so a poison message cannot block the queue. A failure to establish the connection at all (refused connection, unreadable TLS material) is handled before either family: the entry retries with backoff and stays queued until the connection recovers.

| Outcome family | Examples | Handling |
| --- | --- | --- |
| Connectivity | Connection lost mid-publish, broken pipe, connection dropped during the stream-validation lookup | Retryable |
| Timeouts | No acknowledgement within the timeout, attempt deadline reached | Retryable |
| Cluster in transition | No leader elected, peer membership changing | Retryable |
| Stream offline | Stream or JetStream subsystem temporarily offline, stream not found | Retryable |
| Resource and quota exhaustion | Insufficient server resources, storage, memory, or account quota | Retryable |
| Server errors | Any rejection reporting a 5xx status | Retryable |
| Wrong subject, wrong credentials, no responders, authorization failure | Surface through the validation and connection path | Retryable — they hold events on the live queue until the configuration is fixed |
| Permanent rejections | Payload exceeds the maximum size, wrong expected last message id or last sequence, sealed stream | Terminal — failed store immediately |

A sealed stream is classified at three points: startup and admin validation reject it before the target serves traffic; a stream sealed while the target runs is caught by the next validation on the publish path and treated as retryable; a publish that reaches an already-cached validation pass and is then rejected with the sealed-stream code is terminal.

A process interruption between the failed-record write and the live-entry removal can leave a failed record whose event a later replay still delivers — a diagnostic residue, not a lost event.

## The failed-events store

A terminal rejection is recorded, not silently dropped. The failed store is an on-disk `failed` child directory inside the queue directory, one per target, created lazily on the first terminal write. Each entry preserves the event body, routing metadata, the deduplication identifier, an error class tag of `terminal`, the failure time, and the retry count, and an error-level log line names the bucket, object, event name, and error. Records are diagnostic only and are never republished: a condition repaired later (for example a raised broker payload limit) delivers only events still on the live queue, whereas the NATS Core path would have kept retrying such an event.

The store is bounded by `FAILED_STORE_MAX_ENTRIES` per target and by `FAILED_STORE_TTL` (`crates/targets/src/store.rs`), separately from the live queue limit so failures cannot crowd out new events. When the count bound is reached the oldest failed entry is dropped with a warning naming it; entries past the TTL are removed on the replay maintenance tick. The count is a cached value seeded at startup and reconciled to the directory on each maintenance interval; writes and the maintenance scan share one exclusive guard so the bound holds against concurrent writers, and a change made to the directory outside the store drifts the cached count until the next reconciliation.

## Sizing `queue_limit`

Because retryable failures keep events on the live queue, a long outage grows the queue toward `queue_limit`. At the bound, new events are rejected at ingest with a logged error rather than overwriting queued events. Size it for the longest outage the deployment must survive without rejecting: peak events per second times the outage window in seconds — a target averaging 50 events/s that must ride out a one-hour broker outage needs at least 50 * 3600 = 180000 entries — plus headroom, and provision the queue directory storage accordingly.

## Observability

| Gauge | Meaning |
| --- | --- |
| `queue_length` | Entries on the live queue (existing gauge) |
| `failed_store_length` | Entries in the failed-events store per target; a rising value means terminal rejections are accumulating |
| `failed_messages` | Advances only on terminal and dropped events, never on a retry or an exhaustion — entries that left the queue for good |

All three are emitted on both the notify and audit metric paths (`crates/targets/src/runtime/mod.rs`). An exhaustion warning marks each cycle in which an entry spends its full retry budget without delivering; the entry stays queued and the warning repeats once per cycle, so a persistent delivery problem is visible at warn level without per-attempt noise.

## Troubleshooting

| Symptom | Cause | Action |
| --- | --- | --- |
| Validation fails at startup with a missing-stream error | The stream named in `jetstream_stream_name` does not exist or is unreachable | Provision it, or correct the name |
| Validation fails with a subject, `no_ack`, sealed, or duplicate-window error | The stream violates one of the requirements above | Correct the stream configuration |
| Repeated stream-not-found errors in the log | The stream disappeared or was never created; the publish is retryable | Provision the stream; queued events deliver on the next replay |
| Warning that a publish was acknowledged by an unexpected stream | The configured stream no longer captures the publish subject and another stream does. The publish is rejected as retryable and the validation verdict is reset, so later retries fail validation instead of repeating the warning | Inspect the acknowledging stream named in the warning and remove the stray copies; delivery resumes once the configured stream captures the subject and validation passes |
| Health checks slow while a stream fails validation | Health reflects the last validation verdict; a reset forces a live stream lookup on the next check or publish, bounded by the acknowledgement timeout, so a snapshot right after a reset can take up to that timeout per affected target | Repair the stream. After a silent reconnect, a reconfigured stream is detected on the first publish evidence (wrong-stream acknowledgement or stream-not-found), not immediately |
| Events stay in the queue and do not clear | The server is not acknowledging | Check connectivity, that the subject filter captures the publish subject, and server capacity. Unacknowledged events are retained, not lost |
| Duplicate deliveries observed | Duplicate window shorter than the worst-case retry span, an outage or restart that kept an entry queued longer than the window, or `RUSTFS_NOTIFY_TARGET_STREAM_CONCURRENCY` throttled below the backlogged-target count | Raise the stream duplicate window per the formula above, especially after raising the acknowledgement timeout |
| Failed-store entries accumulating | A terminal rejection is recurring (payload too large, wrong expected sequence or message id, sealed stream) | Fix the cause; the records are diagnostic, are not republished, and expire after `FAILED_STORE_TTL`. Wrong subject or credentials never land here — they hold events on the live queue |

## Disabling the path

Set `jetstream_enable` off. The target reverts to the NATS Core path; events already in the queue are delivered by the standard replay, and no failed-store entries are created while the path is off.
