# NATS JetStream Operations Guide

The guide covers enabling and operating the JetStream publish path for the NATS
notify and audit targets in RustFS. It is written for operators who need
at-least-once delivery of events to NATS, must pre-provision and size the stream
the targets publish to, and need to diagnose delivery failures.

## What the JetStream Path Does

By default the NATS notify and audit targets publish with NATS Core, which
returns once the message is written to the socket. A process or server failure
in the gap between the socket write and the server persisting the message loses
the event. The JetStream path closes that gap. With it enabled, each event is
published to a JetStream stream and the local store-and-forward queue entry
clears only after the server returns a publish acknowledgement, which means the
stream leader has accepted and sequenced the message, or after a terminal
rejection has been recorded in the failed-events store. A server restart
mid-flight loses nothing, because an unacknowledged event stays in the local
queue and replays after the server returns.

The path is opt-in and off by default. With it off, behaviour is the NATS Core
path. It applies to both the notify NATS target and the audit NATS target.

RustFS gets each event to the server without losing it before the
acknowledgement. The operator owns the stream and its policy. RustFS validates
that the stream exists and is writable and reports a validation failure
otherwise. A store-backed target keeps running through a failed validation,
holding queued events until the stream is repaired. RustFS never creates the
stream. Stream retention, storage, and replication policy stay under operator
control.

## Enabling JetStream: Recommended Configuration

Three keys turn the path on and tune it. Each is available on both the notify
NATS target and the audit NATS target, and each has a configuration-key form and
an environment-variable form per target.

```bash
RUSTFS_NOTIFY_NATS_JETSTREAM_ENABLE=true
RUSTFS_NOTIFY_NATS_JETSTREAM_STREAM_NAME=RUSTFS_EVENTS
RUSTFS_NOTIFY_NATS_JETSTREAM_ACK_TIMEOUT_SECS=30
RUSTFS_NOTIFY_NATS_QUEUE_DIR=/var/lib/rustfs/notify-nats
```

The audit target takes the same keys under the RUSTFS_AUDIT_NATS_ prefix.

- jetstream_enable turns the JetStream path on for the target. Off by default.
  Environment forms RUSTFS_NOTIFY_NATS_JETSTREAM_ENABLE and
  RUSTFS_AUDIT_NATS_JETSTREAM_ENABLE.
- jetstream_stream_name is the name of the pre-provisioned stream to publish to.
  Required when enable is on, with no default. Environment forms
  RUSTFS_NOTIFY_NATS_JETSTREAM_STREAM_NAME and
  RUSTFS_AUDIT_NATS_JETSTREAM_STREAM_NAME.
- jetstream_ack_timeout_secs is how long a publish waits for an acknowledgement
  before it is treated as timed out and retried. Range 10 to 120, default 30.
  Environment forms RUSTFS_NOTIFY_NATS_JETSTREAM_ACK_TIMEOUT_SECS and
  RUSTFS_AUDIT_NATS_JETSTREAM_ACK_TIMEOUT_SECS.
- queue_dir is the local store-and-forward queue directory and is required when
  enable is on. Durability is not achievable without a local store to replay
  from. Environment forms RUSTFS_NOTIFY_NATS_QUEUE_DIR and
  RUSTFS_AUDIT_NATS_QUEUE_DIR.

A configuration that enables the path without a stream name or without a queue
directory is rejected at startup and in the admin validation path. An
acknowledgement timeout outside the 10 to 120 second range is rejected the same
way.

The 30 second acknowledgement timeout suits production. A server replicating to
multiple replicas acknowledges only after the replication and the deferred fsync,
which legitimately takes well over 100 milliseconds at the tail, so a short
timeout would spuriously fail a publish mid-replication.

After enabling, confirm the target is on the JetStream path before relying on it.
Value errors, an out-of-range acknowledgement timeout or a missing stream name
while enable is on, are already rejected loudly at startup and in the admin
validation path. A mistyped configuration key name is not a value error, so it
escapes that check: the key reads as absent, and the target stays on the NATS
Core path with no durability. Verify the enable took effect by confirming the
target reports its JetStream fields on startup and logs the stream-validation
success line naming the configured stream. Absence of that line for a target that
was meant to be enabled indicates a key name the server did not recognise.

## How Delivery Works

An event is queued to the local store-and-forward queue first, then published
through JetStream. The publish carries a stable Nats-Msg-Id header and the path
awaits the real publish acknowledgement.

- The queue entry clears only after the durable acknowledgement returns, or
  after a terminal rejection has been recorded in the failed-events store. A
  timeout, or any retryable error, retains the entry for a retry. The entry is
  never cleared without one of those two outcomes and never silently dropped. A
  process interruption between the failed-record write and the live-entry removal
  can leave a record whose event a later replay still delivers, a diagnostic
  residue and not a lost event.
- The Nats-Msg-Id is minted once when the event is queued and stored with it. It
  is identical across every retry and replay of that entry, and unique per
  entry, so the server collapses retries and replays of the same event within
  the stream duplicate window. A retry after a slow acknowledgement reuses the
  same identifier and is not delivered twice.
- A crash before the acknowledgement leaves the entry in the queue. The entry
  replays after restart. Closing the target releases the cached connection and
  context but never the queued entries, so an entry queued before close survives
  on disk and replays on the next start.
- A broker outage detected at connection establishment keeps queued events on
  disk. Replay retries with backoff while the broker is unreachable, and the
  queued events deliver when the connection recovers, keeping entries queued like
  the NATS Core path rather than replicating its mechanics byte for byte. Every
  retryable publish failure on an established connection is treated the same way,
  kept on the live queue and retried until it delivers, which includes a
  connection dropping during the stream-validation lookup. Only a non-retryable
  rejection is moved to the failed-events store.

For durability across a node failure, provision the stream with a replica count
of at least 3. An acknowledgement returns after the stream leader commits. A
leader that acknowledges and then fails before the message replicates to a quorum
can lose that message on failover, so a single-replica stream is durable only
against a clean restart, not against the loss of the node holding the data.

## Pre-provisioning the Stream

The stream is provisioned by the operator before the path is enabled. RustFS
reads the stream once at target init and in the admin validation path, asserts
the requirements below, and reports a validation failure rather than publishing
into a stream where writes would silently fail. A failed validation does not
stop a store-backed target. Events keep queueing to the local store. Revalidation
runs while the validation verdict is unset and stops once one validation passes,
resuming only after the verdict is reset, and the queued events deliver once the
stream is repaired. The stream is not auto-created. A missing stream is an
error.

The stream must:

- Exist and be writable. A missing or unreachable stream fails validation.
- Capture the configured publish subject in its subject filter, by literal match
  or by a NATS wildcard. A stream that does not capture the subject fails
  validation.
- Acknowledge writes (no_ack false). A stream with no_ack set never returns an
  acknowledgement, so the queue would never clear. It fails validation.
- Not be sealed. A sealed stream rejects writes and fails validation.
- Set a duplicate window of at least the retry span (see the next section). A
  window below that span fails validation.

Choose retention, storage type, and replica count to match the durability the
deployment needs. Use file storage if a server restart must preserve
already-persisted events. These are operator decisions and RustFS does not set
them. If the stream is provisioned shortly after the path is enabled, the queued
events deliver on the next replay rather than being lost, because a
stream-not-found error is retryable and keeps the events on the live queue until
the stream exists.

## Setting the Duplicate Window

The acknowledgement timeout and the stream duplicate window are linked. The
duplicate window must cover the worst-case retry span, or a late retry of an
event the server already persisted is delivered a second time instead of being
recognised as a duplicate.

The worst-case retry span is the retry count times the acknowledgement timeout,
plus the realized backoff sleeps across the retries and a final headroom term:

    duplicate_window >= (retry_count * ack_timeout_secs) + realized_backoff + headroom

There are 5 retry attempts. The realized backoff sleeps sum to 60 seconds
(4 + 8 + 16 + 32), and no sleep follows the last attempt, so a 64 second headroom
term is added above the realized span. At the default 30 second acknowledgement
timeout the required window is:

    (5 * 30) + 60 + 64 = 274 seconds

Set the stream duplicate window to at least 274 seconds when the acknowledgement
timeout is at the 30 second default. A configuration whose duplicate window is
below this span is rejected at validation, with the configured and required
window named in the log line.

The span scales with the acknowledgement timeout. Raising
jetstream_ack_timeout_secs requires raising the stream duplicate window in step
using the formula above. The 60 second realized backoff and the 64 second headroom
stay fixed, so each extra second of acknowledgement timeout adds 5 seconds to the
required window. At the 120 second maximum timeout the required window is
(5 * 120) + 60 + 64 = 724 seconds. Each publish attempt, including connection
establishment, is bounded by a single deadline equal to the acknowledgement
timeout. The formula is a conservative upper bound on the retry span, and the
final headroom term holds the required window above the realized span.

The validated window covers one retry cycle: the worst-case span from the first
publish attempt of a stored entry to its last within a single replay cycle. An
entry that exhausts a cycle without delivering stays on the live queue and is
retried on a later cycle, so an entry surviving across cycles can spend longer in
the queue than the duplicate window and be delivered again, consistent with
at-least-once delivery.

## The Failed-events Store

An event that cannot be delivered is recorded, not silently dropped. Only one
case produces a failed event:

- A terminal rejection. A message that exceeds the maximum payload size, a wrong
  expected last message identifier or last sequence, or a sealed stream. These do
  not improve with a retry, so they fail fast.

Every other rejection is retryable and never produces a failed event. A timeout,
backpressure, no responders, a missing stream, a subject the stream does not
capture, or an authorization failure keeps the event on the live queue and
retries it until it delivers or an operator intervenes. A wrong subject and wrong
credentials surface through the validation and connection path as retryable, so
they hold the events on the live queue rather than moving them to the failed
store.

A failed event is moved to an on-disk failed store that sits in a failed
child directory inside the queue directory, one per target. The directory is created
lazily on the first failed write, so a target that never fails terminally leaves
no failed directory. The entry preserves the event body, its routing metadata, the
deduplication identifier, an error class tag of terminal, the failure time, and
the retry count. An error-level log line is written for each failed event, naming
the bucket, object, event name, and the error, so a broken integration is visible
rather than hidden. A record in the failed store is diagnostic only and is never
republished, so a condition repaired later, for example a raised broker payload
limit, delivers only events still on the live queue. The NATS Core path without
JetStream instead retries such an event until the limit allows it.

The failed store is bounded by count (10000 entries per target) and by age (a 72
hour retention), and is kept separate from the live queue limit so an
accumulation of failures cannot crowd out new events. The count is maintained as a
cached value, seeded at startup and reconciled to the directory on each
maintenance interval, so it stays accurate without a directory scan on the hot
path. Failed-store writes and the maintenance scan run under one exclusive guard,
so the at-bound check and the write stay atomic and the bound holds against
concurrent writers. A change made to the directory outside the store can drift the
cached count until the next maintenance interval reconciles it. When the count
bound is reached the oldest failed entry is dropped, with a warning naming the
trimmed entry, so a newer failure is never lost in favour of an older one. Entries
past the retention bound are removed as expired on the replay maintenance tick.

Because a retryable failure keeps events on the live queue, a long outage grows
the queue toward its configured queue_limit bound. Once the queue reaches that
bound, new events are rejected at ingest with a logged error rather than
overwriting queued events, so the backlog is bounded and visible.

Size queue_limit for the longest outage the deployment must survive without
rejecting new events. Multiply the peak event rate in events per second by the
outage window in seconds. A target that averages 50 events per second and must
ride out a one hour broker outage needs a queue_limit of at least 50 * 3600 =
180000 entries. Add headroom above the calculated figure, and provision the
queue directory storage for the resulting entry count.

## Publish Outcome Handling

Every publish outcome falls into one of two families. Transient conditions are
retried until they deliver and the entry stays on the live queue, so a transient
condition never reaches the failed store. Permanent conditions move to the failed
store immediately. Transient conditions classify toward retry because a terminal
misclassification risks losing an entry once the failed store retention lapses.
Permanent conditions move immediately so a poison message cannot block the queue.

Both families cover publish outcomes on an established connection. A failure to
establish the connection at all, a refused connection or unreadable TLS
material, is handled before either family applies: the entry retries with
backoff and stays queued until the connection recovers. A publish-level failure
on an established connection, including a connection that drops during the
stream-validation lookup, is retried on the live queue when it is transient and
moved to the failed store only when it is a permanent rejection.

| Outcome family | Examples | Handling |
| --- | --- | --- |
| Connectivity | connection lost mid-publish, broken pipe | Retried until delivered, live queue |
| Timeouts | no acknowledgement within the timeout, attempt deadline reached | Retried until delivered, live queue |
| Cluster in transition | no leader elected, peer membership changing | Retried until delivered, live queue |
| Stream offline | stream or JetStream subsystem temporarily offline, stream not found | Retried until delivered, live queue |
| Resource and quota exhaustion | insufficient server resources, storage, memory, or account quota reached | Retried until delivered, live queue |
| Server errors | any rejection reporting a 5xx status, with or without a specific error code | Retried until delivered, live queue |
| Permanent rejections | payload too large, wrong expected sequence or message identifier, sealed stream | Failed store immediately |

A sealed stream is classified at three points, with three outcomes. Startup validation and the admin
validation path reject a sealed stream before the target serves traffic, so init fails and no event
is queued against it. A stream sealed while the target runs is caught by the next validation on the
publish path, which classifies it retryable and keeps the entry on the live queue while validation
keeps failing. A publish that reaches an already-cached validation pass and is then rejected with the
sealed-stream code is terminal and moves to the failed store immediately. The permanent-rejections
row lists that last outcome.

## Observability

The failed_store_length gauge reports the number of entries in the failed-events
store per target, next to the existing failed_messages and queue_length gauges,
on both the notify and audit metric paths. A rising failed_store_length points to
terminal rejections accumulating for a target. An exhaustion warning marks each
cycle where an entry spends its full retry budget without delivering, after which
the entry stays queued and retries on the next scan. The warning repeats once per
retry cycle while the entry stays queued, so a persistent delivery problem is
visible at warn level without per-attempt noise. The failed_messages count
advances only on terminal and dropped events, never on a retry or an exhaustion,
so it counts entries that left the queue for good rather than entries still
retrying.

## Troubleshooting

- Validation fails at startup with a missing-stream error. The stream named in
  jetstream_stream_name does not exist on the server, or is not reachable.
  Provision it, or correct the name.
- Validation fails with a subject, no_ack, sealed, or duplicate-window error. The
  pre-provisioned stream does not capture the publish subject, has no_ack set, is
  sealed, or has a duplicate window below the worst-case retry span. Correct the
  stream configuration to satisfy the pre-provisioning requirements above.
- Repeated stream-not-found errors in the log. The stream disappeared or was
  never created. The publish is retryable, so the events stay on the live queue
  and retry. Provision the stream so the queued events deliver on the next
  replay.
- A warning that a publish was acknowledged by an unexpected stream. The
  configured stream no longer captures the publish subject and another stream
  does. The mismatched publish is rejected with a retryable error, the entry
  stays on the live queue, and the mismatch resets the stream-validation
  verdict. Later retries then fail validation before publishing, so the log
  shows the validation failure rather than a repeating mismatch warning. The
  mismatch warning fires again only after a validation pass lets another
  publish through. The acknowledging stream named in the warning has persisted
  a copy of each mismatched publish. Inspect that stream and remove the stray
  copies. Delivery resumes once the configured stream captures the subject
  again and validation passes.
- Health checks slow while a stream fails validation. Health reflects the last
  stream-validation verdict rather than a live lookup on every check. The verdict
  resets on a reconnect, a TLS rotation, an acknowledgment naming an unexpected
  stream, or a stream-not-found publish outcome, and a reset forces a live stream
  lookup on the next check or publish, bounded by the acknowledgement timeout. A
  health snapshot taken right after a reset can take up to that timeout per
  affected target until the stream is repaired. After a broker connection heals
  on its own, publishes and health rely on the last validation verdict until an
  error outcome resets it, a wrong-stream acknowledgment or a stream-not-found
  publish outcome, so a stream reconfigured during a silent reconnect is
  detected on the first publish evidence rather than immediately.
- Events stay in the queue and do not clear. The server is not acknowledging.
  Check connectivity, that the stream subject filter captures the publish
  subject, and that the server has capacity. Unacknowledged events are retained,
  not lost.
- Duplicate deliveries observed. The duplicate window is shorter than the
  worst-case retry span. Raise the stream duplicate window using the formula
  above, especially after raising the acknowledgement timeout. An outage or a
  restart that keeps an unacknowledged entry queued longer than the duplicate
  window can also produce a duplicate on replay, consistent with at-least-once
  delivery. Throttling RUSTFS_NOTIFY_TARGET_STREAM_CONCURRENCY below the number
  of concurrently backlogged targets inserts untimed waits between the retries
  of one entry and can push a late retry past the window. Keep the default or
  add window margin when lowering it.
- Failed-store entries accumulating. A terminal rejection is recurring: a
  message over the maximum payload size, a wrong expected last message identifier
  or last sequence, or a sealed stream. Failed entries are on-disk diagnostic
  records carrying the error class and a fixed diagnostic detail for inspection.
  They are not re-published, and they are removed after the retention period. A
  wrong subject or wrong credentials do not land here. They surface as retryable
  and hold the events on the live queue until the configuration is fixed.

## Disabling the Path

Set jetstream_enable off. The target reverts to the NATS Core path. Events
already in the queue are delivered by the standard replay. No failed-store
entries are created while the path is off.
