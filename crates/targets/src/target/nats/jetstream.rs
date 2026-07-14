// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::NATSTarget;
use super::publish_error::classify_publish_error;
use super::validation::{gate_publish_on_stream_validation, validate_jetstream_stream};
use crate::StoreError;
use crate::arn::TargetID;
use crate::error::TargetError;
use crate::plugin::PluginEvent;
use crate::runtime::{REPLAY_MAX_RETRIES, inter_attempt_backoff_sum, replay_backoff_term};
use crate::store::{FailedEventStore, Key, Store};
use crate::target::{
    FailedErrorClass, QueuedPayload, build_failed_error_detail, delete_stored_payload, encode_failed_entry,
    truncate_to_char_boundary,
};
use async_nats::header::{self, HeaderMap};
use async_nats::jetstream::{self, context::PublishErrorKind};
use sha2::{Digest, Sha256};
use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tracing::{debug, error, warn};

/// A cached JetStream context paired with its stream-validation verdict. The verdict starts
/// unvalidated for every built context, so a context rebuilt after a TLS rotation re-validates
/// before its first publish. A separate flag records whether the first passing validation has been
/// logged, so revalidations after a verdict reset stay at debug. Both start fresh with a rebuilt
/// context, and clones share them through the Arc.
#[derive(Clone)]
pub(crate) struct CachedJetStreamContext {
    pub(crate) context: jetstream::Context,
    pub(crate) stream_validated: Arc<AtomicBool>,
    pub(crate) validation_logged: Arc<AtomicBool>,
}

impl CachedJetStreamContext {
    pub(crate) fn new(context: jetstream::Context) -> Self {
        Self {
            context,
            validation_logged: Arc::new(AtomicBool::new(false)),
            stream_validated: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl<E> NATSTarget<E>
where
    E: PluginEvent,
{
    /// Returns the cached JetStream context, building it from the connected client on first use. The
    /// context owns one background acker and one MaxAckPending semaphore, with its timeout set to the
    /// ack timeout.
    pub(crate) async fn jetstream_context(&self) -> Result<CachedJetStreamContext, TargetError> {
        // get_or_connect runs before the cache read so a detected TLS rotation swaps in a fresh
        // context bound to the new client rather than one bound to the invalidated client.
        let client = self.get_or_connect().await?;

        {
            let guard = self.jetstream_context.lock().await;
            if let Some(cached) = guard.as_ref() {
                return Ok(cached.clone());
            }
        }

        let ack_timeout = self.ack_timeout();

        // Build under the lock so concurrent first callers do not each spawn an acker. new and
        // set_timeout are synchronous, so no await holds the lock.
        let mut guard = self.jetstream_context.lock().await;
        let cached = guard.get_or_insert_with(|| {
            let mut context = jetstream::new(client);
            context.set_timeout(ack_timeout);
            CachedJetStreamContext::new(context)
        });
        Ok(cached.clone())
    }

    /// Returns the cached JetStream context after its configured stream has passed validation on the
    /// bound connection. The verdict is cached with the context, so the steady-state cost is one
    /// atomic load and a rebuilt context re-validates before its first publish. A connection-phase
    /// failure classifies as NotConnected and a validation failure as a retryable JetStream publish
    /// error, so either leaves the entry queued.
    pub(crate) async fn validated_jetstream_context(&self) -> Result<CachedJetStreamContext, TargetError> {
        let cached = self.jetstream_context().await.map_err(jetstream_connect_error)?;
        if !cached.stream_validated.load(Ordering::Acquire) {
            let verdict =
                validate_jetstream_stream(&cached.context, &self.args, &self.id.to_string(), Some(&cached.validation_logged))
                    .await;
            gate_publish_on_stream_validation(verdict, &cached.stream_validated)?;
        }
        Ok(cached)
    }

    /// Publishes the body with the Nats-Msg-Id dedup header and awaits the real PublishAck. The whole
    /// attempt, including connection establishment, stream validation, and the ack await, is bounded
    /// by a single deadline equal to the ack timeout. Dropping a timed-out attempt releases its
    /// resources, the broker suite exercises the drop path.
    pub(crate) async fn publish_jetstream(&self, body: Vec<u8>, dedup_id: &str) -> Result<(), TargetError> {
        attempt_within_deadline(self.ack_timeout(), self.publish_jetstream_attempt(body, dedup_id)).await
    }

    /// Runs one publish attempt with no deadline of its own: acquires the validated context,
    /// publishes with the dedup header, and awaits the PublishAck.
    async fn publish_jetstream_attempt(&self, body: Vec<u8>, dedup_id: &str) -> Result<(), TargetError> {
        let cached = self.validated_jetstream_context().await?;

        let mut headers = HeaderMap::new();
        headers.insert(header::NATS_MESSAGE_ID, dedup_id);

        // A stream-not-found outcome on either await resets the verdict before classification, so
        // the next attempt re-validates instead of publishing blind into a missing stream.
        let started = Instant::now();
        let ack_future = cached
            .context
            .publish_with_headers(self.args.subject.clone(), headers, body.into())
            .await
            .inspect_err(|err| reset_verdict_on_stream_not_found(err, &cached.stream_validated))
            .map_err(|err| classify_publish_error(&err))?;

        let ack = ack_future
            .await
            .inspect_err(|err| reset_verdict_on_stream_not_found(err, &cached.stream_validated))
            .map_err(|err| classify_publish_error(&err))?;
        let latency_ms = started.elapsed().as_millis() as u64;

        self.finish_acknowledged_publish(&ack.stream, ack.sequence, ack.duplicate, latency_ms, dedup_id, &cached.stream_validated)
    }

    /// Completes a publish from the returned acknowledgment: verifies the acknowledging stream
    /// matches the configured name, records the delivery, and logs the outcome. A mismatch resets
    /// the verdict and returns a retryable error, so the entry stays queued and the next attempt
    /// re-validates.
    fn finish_acknowledged_publish(
        &self,
        ack_stream: &str,
        sequence: u64,
        duplicate: bool,
        ack_latency_ms: u64,
        dedup_id: &str,
        stream_validated: &AtomicBool,
    ) -> Result<(), TargetError> {
        let configured_stream = self.args.jetstream_stream_name.as_deref().unwrap_or_default();
        if ack_stream != configured_stream {
            stream_validated.store(false, Ordering::Release);
            // Server-controlled text, length-bounded before logging.
            let mut acknowledged_stream = ack_stream.to_string();
            truncate_to_char_boundary(&mut acknowledged_stream, ACK_STREAM_LOG_MAX_LEN);
            warn!(
                target_id = %self.id,
                expected_stream = %configured_stream,
                acknowledged_stream = %acknowledged_stream,
                dedup_id = %dedup_id,
                "JetStream publish acknowledged by an unexpected stream, verdict reset for re-validation"
            );
            return Err(TargetError::JetStreamPublish {
                retryable: true,
                detail: ACK_STREAM_MISMATCH_DETAIL.to_string(),
            });
        }

        // A first-attempt duplicate would mean two events collided on one id, which the minted uuid
        // prevents, so the flag is logged rather than treated as an error.
        debug!(
            target_id = %self.id,
            stream = %ack_stream,
            sequence,
            duplicate,
            ack_latency_ms,
            "JetStream publish acknowledged"
        );

        self.delivery_counters.record_success();
        Ok(())
    }
}

/// Resolves the dedup id for a stored entry: the minted id when present, otherwise a hash of the
/// entry's on-disk key. Deriving from the key rather than the body keeps two entries with identical
/// bodies on separate ids, so the server does not drop the second as a duplicate.
pub(crate) fn resolve_dedup_id(meta_dedup_id: &str, key: &Key) -> String {
    if !meta_dedup_id.is_empty() {
        return meta_dedup_id.to_string();
    }
    let digest = Sha256::digest(key.to_key_string().as_bytes());
    let mut id = String::with_capacity(digest.len() * 2);
    for byte in digest {
        // Writing a formatted byte into a String cannot fail.
        let _ = write!(id, "{byte:02x}");
    }
    id
}

/// Moves a live queue entry that failed terminally to the failed-events store, then removes it from
/// the live queue. The failed entry is written first and the live entry deleted only after, so a
/// crash between the two leaves a recoverable duplicate rather than a loss.
pub(crate) async fn move_entry_to_failed_store(
    store: &(dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send),
    failed_store: &dyn FailedEventStore,
    target_id: &TargetID,
    key: &Key,
    error: &TargetError,
    retry_count: u32,
) -> Result<(), TargetError> {
    let raw = match store.get_raw(key) {
        Ok(raw) => raw,
        // A definitive ack from a concurrent path already cleared the entry. Nothing to move.
        Err(StoreError::NotFound) => return Ok(()),
        Err(err) => {
            return Err(TargetError::Storage(format!(
                "Failed to read queued payload before failed-store move: {err}"
            )));
        }
    };

    let queued = QueuedPayload::decode(&raw)
        .map_err(|err| TargetError::Storage(format!("Failed to decode queued payload before failed-store move: {err}")))?;
    let meta = queued.meta.clone();
    // Same resolution the publish path applies, so the recorded id matches what the server saw even
    // for a pre-enable entry with an empty stored id.
    let resolved_dedup_id = resolve_dedup_id(&meta.dedup_id, key);
    let encoded = encode_failed_entry(queued, FailedErrorClass::Terminal, error, retry_count, &resolved_dedup_id)?;

    failed_store
        .put_failed_raw(&key.name, &encoded)
        .map_err(|err| TargetError::Storage(format!("Failed to write failed-store entry: {err}")))?;

    // Error level: a terminal failure means a lost notification an operator must act on. Names
    // routing metadata and the redacted detail only, never the event body or a credential.
    error!(
        target_id = %target_id,
        error_class = FailedErrorClass::Terminal.as_str(),
        error_detail = %build_failed_error_detail(error),
        bucket = %meta.bucket_name,
        object = %meta.object_name,
        event_name = %meta.event_name.as_str(),
        nats_msg_id = %resolved_dedup_id,
        retry_count,
        "event moved to the failed-events store"
    );

    delete_stored_payload(store, key)?;
    // The failure count is recorded by the replay hook, not here, so it counts once.
    Ok(())
}

/// Drains a JetStream context and drops it so its background acker exits. async-nats keeps the acker
/// task alive with no Drop on Context, so wait_for_acks drains every in-flight publish before the
/// final drop closes the ack channel and ends the task. None is a no-op.
pub(crate) async fn drain_jetstream_context(context: Option<jetstream::Context>) {
    if let Some(context) = context {
        context.wait_for_acks().await;
    }
}

/// Maps a connectivity or configuration failure from establishing the JetStream connection to
/// NotConnected, so the replay worker retries it and leaves the entry queued rather than parking it
/// in the failed store. Errors already classified at the publish level pass through unchanged.
fn jetstream_connect_error(err: TargetError) -> TargetError {
    match err {
        TargetError::Network(_) | TargetError::Configuration(_) => TargetError::NotConnected,
        other => other,
    }
}

/// Detail carried by the retryable publish error raised when a publish attempt exceeds the ack
/// timeout.
pub(crate) const ATTEMPT_DEADLINE_DETAIL: &str = "attempt deadline exceeded";

/// Bounds one publish attempt by the ack timeout. An attempt that does not resolve within the
/// deadline is dropped and classified as a retryable JetStream publish error, so the replay loop
/// counts it toward the bounded budget. An inner client timeout can resolve first with its own
/// timed-out error, and both paths classify retryable.
async fn attempt_within_deadline<F>(ack_timeout: Duration, attempt: F) -> Result<(), TargetError>
where
    F: std::future::Future<Output = Result<(), TargetError>>,
{
    match tokio::time::timeout(ack_timeout, attempt).await {
        Ok(result) => result,
        Err(_elapsed) => Err(TargetError::JetStreamPublish {
            retryable: true,
            detail: ATTEMPT_DEADLINE_DETAIL.to_string(),
        }),
    }
}

/// Detail carried by the retryable publish error raised when an acknowledgment names a stream other
/// than the configured one.
pub(crate) const ACK_STREAM_MISMATCH_DETAIL: &str = "acknowledged by an unexpected stream";

/// Byte bound applied to the acknowledged stream name before it is logged.
const ACK_STREAM_LOG_MAX_LEN: usize = 64;

/// Detail carried by the retryable publish error raised when the server reports the configured
/// stream does not exist.
pub(crate) const STREAM_NOT_FOUND_DETAIL: &str = "stream not found";

/// Resets the validation verdict when a publish outcome reports the configured stream does not
/// exist, so the next attempt re-validates. Keys off the raw error kind before classification,
/// keeping the classifier pure.
fn reset_verdict_on_stream_not_found(err: &async_nats::jetstream::context::PublishError, stream_validated: &AtomicBool) {
    if matches!(err.kind(), PublishErrorKind::StreamNotFound) {
        stream_validated.store(false, Ordering::Release);
    }
}

/// Conservative upper bound on the wall-clock retry span of a stored entry, used as the minimum
/// acceptable stream duplicate_window. Sums the per-attempt ack ceiling across REPLAY_MAX_RETRIES
/// with the realized backoff schedule plus a headroom term. The headroom is a deliberate
/// conservatism margin that holds the required duplicate_window above the realized retry span. A
/// duplicate_window at or above this keeps a late same-id retry inside the dedup window. Saturating
/// arithmetic keeps it panic-free.
pub(crate) fn retry_lifetime(ack_timeout: Duration) -> Duration {
    let ack_span = ack_timeout.saturating_mul(REPLAY_MAX_RETRIES as u32);
    let realized_backoff = inter_attempt_backoff_sum(REPLAY_MAX_RETRIES);
    let headroom = replay_backoff_term(REPLAY_MAX_RETRIES as u32);
    ack_span.saturating_add(realized_backoff).saturating_add(headroom)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Target;
    use crate::arn::TargetID;
    use crate::store::{FailedEventStore, QueueStore};
    use crate::target::TargetType;
    use crate::target::nats::test_support::*;
    use crate::target::nats::validation::STREAM_VALIDATION_FAILED_DETAIL;
    use crate::target::test_support::{
        MoveTestTarget, failed_store_dir, move_test_target, move_test_target_with_store, sample_queued,
    };
    use crate::target::{build_target_tls_fingerprint, persist_queued_payload_to_store};
    use async_nats::jetstream::context::PublishError;
    use rustfs_config::NATS_JETSTREAM_ACK_TIMEOUT_DEFAULT_SECS;
    use std::sync::atomic::AtomicU64;
    use uuid::Uuid;

    #[test]
    fn enabled_target_mints_dedup_id_stored_in_meta() {
        let target = nats_target(jetstream_args(TargetType::NotifyEvent));
        let queued = target.build_queued_payload(&sample_event()).expect("payload builds");
        assert!(!queued.meta.dedup_id.is_empty(), "an enabled target mints a dedup id");
        // The minted id is a uuid string, distinct from any event field.
        assert!(Uuid::parse_str(&queued.meta.dedup_id).is_ok(), "the dedup id is a uuid");
    }

    #[test]
    fn disabled_target_mints_no_dedup_id() {
        let mut args = jetstream_args(TargetType::NotifyEvent);
        args.jetstream_enable = Some(false);
        let target = nats_target(args);
        let queued = target.build_queued_payload(&sample_event()).expect("payload builds");
        assert!(queued.meta.dedup_id.is_empty(), "a disabled target mints no dedup id");
    }

    #[test]
    fn dedup_id_is_stable_across_reads_of_the_same_stored_entry() {
        for target_type in [TargetType::NotifyEvent, TargetType::AuditLog] {
            let target = nats_target(jetstream_args(target_type));
            let queued = target.build_queued_payload(&sample_event()).expect("payload builds");

            let encoded = queued.encode().expect("payload encodes");
            let first = QueuedPayload::decode(&encoded).expect("payload decodes");
            let second = QueuedPayload::decode(&encoded).expect("payload decodes");

            assert_eq!(
                first.meta.dedup_id, second.meta.dedup_id,
                "reading the same stored entry twice yields the same id for {target_type}"
            );
            assert_eq!(
                first.meta.dedup_id, queued.meta.dedup_id,
                "the stored id is the minted id for {target_type}"
            );
        }
    }

    #[test]
    fn dedup_id_is_distinct_across_entries() {
        for target_type in [TargetType::NotifyEvent, TargetType::AuditLog] {
            let target = nats_target(jetstream_args(target_type));
            let first = target.build_queued_payload(&sample_event()).expect("payload builds");
            let second = target.build_queued_payload(&sample_event()).expect("payload builds");
            assert_ne!(
                first.meta.dedup_id, second.meta.dedup_id,
                "two distinct entries get distinct ids for {target_type}"
            );
        }
    }

    #[test]
    fn resolve_dedup_id_prefers_the_stored_minted_id() {
        let id = resolve_dedup_id("minted-uuid", &sample_stored_key("entry-one"));
        assert_eq!(id, "minted-uuid");
    }

    #[test]
    fn resolve_dedup_id_derives_distinct_ids_for_entries_with_equal_bodies() {
        // Distinct on-disk keys yield distinct fallback ids even for identical bodies.
        let first = resolve_dedup_id("", &sample_stored_key("entry-one"));
        let second = resolve_dedup_id("", &sample_stored_key("entry-two"));
        assert_ne!(first, second, "distinct entries derive distinct ids");
        assert_eq!(first.len(), 64, "the derived id is a sha256 hex digest");
    }

    #[test]
    fn resolve_dedup_id_is_stable_across_replays_of_one_entry() {
        // The same entry keeps its key across replays, so the derived id is identical each time.
        let key = sample_stored_key("entry-one");
        assert_eq!(resolve_dedup_id("", &key), resolve_dedup_id("", &key));
    }

    #[test]
    fn jetstream_connect_error_maps_network_to_not_connected() {
        // A connect-phase connectivity failure classifies as NotConnected so the entry stays queued.
        let mapped = jetstream_connect_error(TargetError::Network("Failed to connect to NATS server: refused".to_string()));
        assert!(
            matches!(mapped, TargetError::NotConnected),
            "a connect-phase connectivity failure classifies as NotConnected, got {mapped:?}"
        );
    }

    #[test]
    fn jetstream_connect_error_maps_configuration_to_not_connected() {
        // A connect-phase configuration failure (unreadable TLS material after start) classifies as NotConnected.
        let mapped = jetstream_connect_error(TargetError::Configuration("Failed to read TLS material".to_string()));
        assert!(
            matches!(mapped, TargetError::NotConnected),
            "a connect-phase configuration failure classifies as NotConnected, got {mapped:?}"
        );
    }

    #[test]
    fn jetstream_connect_error_passes_through_classified_errors() {
        // An already-classified publish error passes through, so the mapping cannot mask a terminal cause.
        let terminal = jetstream_connect_error(TargetError::JetStreamPublish {
            retryable: false,
            detail: "stream sealed".to_string(),
        });
        match terminal {
            TargetError::JetStreamPublish { retryable, .. } => assert!(!retryable, "a terminal publish error stays terminal"),
            other => panic!("expected JetStreamPublish, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn publish_jetstream_maps_a_connect_failure_to_not_connected() {
        // A connect-level failure on the publish path surfaces as NotConnected. Port 1 refuses without a server.
        let queue_dir = tempfile::tempdir().expect("queue dir");
        let mut args = jetstream_args(TargetType::NotifyEvent);
        args.address = "nats://127.0.0.1:1".to_string();
        args.queue_dir = queue_dir.path().to_str().unwrap().to_string();
        let target = nats_target(args);

        let err = target
            .publish_jetstream(b"body".to_vec(), "dedup-connect-fail")
            .await
            .expect_err("an unreachable broker fails the publish");
        assert!(
            matches!(err, TargetError::NotConnected),
            "a connect-level failure on the publish path surfaces as NotConnected, got {err:?}"
        );
    }

    #[test]
    fn a_wrong_stream_ack_resets_the_verdict_and_classifies_retryable() {
        // A wrong-stream ack resets the verdict and returns the retryable mismatch error. A matching ack completes the publish and keeps the verdict.
        let target = nats_target(jetstream_args(TargetType::NotifyEvent));
        let stream_validated = AtomicBool::new(true);
        let err = target
            .finish_acknowledged_publish("SOME_OTHER_STREAM", 7, false, 12, "dedup-mismatch", &stream_validated)
            .expect_err("an unexpected acknowledging stream is rejected");
        match err {
            TargetError::JetStreamPublish { retryable, detail } => {
                assert!(retryable, "a wrong-stream ack is retryable so the entry stays queued");
                assert_eq!(detail, ACK_STREAM_MISMATCH_DETAIL, "the fixed mismatch label is the detail");
            }
            other => panic!("expected a retryable JetStreamPublish, got {other:?}"),
        }
        assert!(
            !stream_validated.load(Ordering::SeqCst),
            "the verdict resets so the next attempt re-validates the stream"
        );

        // A matching acknowledging stream completes the publish and keeps the recorded verdict.
        let validated = AtomicBool::new(true);
        target
            .finish_acknowledged_publish("RUSTFS_EVENTS", 8, false, 12, "dedup-match", &validated)
            .expect("a matching stream ack completes the publish");
        assert!(validated.load(Ordering::SeqCst), "a matching ack keeps the recorded verdict");
    }

    #[test]
    fn stream_not_found_publish_outcome_resets_the_verdict() {
        // A StreamNotFound outcome resets the verdict. Any other kind leaves it untouched.
        let stream_validated = AtomicBool::new(true);
        reset_verdict_on_stream_not_found(&PublishError::new(PublishErrorKind::StreamNotFound), &stream_validated);
        assert!(!stream_validated.load(Ordering::SeqCst), "a stream-not-found outcome resets the verdict");

        for kind in [PublishErrorKind::TimedOut, PublishErrorKind::MaxPayloadExceeded] {
            let verdict = AtomicBool::new(true);
            reset_verdict_on_stream_not_found(&PublishError::new(kind), &verdict);
            assert!(verdict.load(Ordering::SeqCst), "{kind} leaves the verdict intact");
        }
    }

    #[tokio::test]
    async fn a_failed_publish_attempt_leaves_the_queued_entry_intact() {
        // A publish that fails before any send leaves the queued entry, since only a returned PublishAck clears it.
        let queue_dir = tempfile::tempdir().expect("queue dir");
        let mut args = jetstream_args(TargetType::NotifyEvent);
        args.address = "nats://127.0.0.1:1".to_string();
        let target = nats_target_with_store(args, queue_dir.path().to_str().unwrap());

        let queued = target.build_queued_payload(&sample_event()).expect("payload builds");
        let store = target.store().expect("a store is configured");
        persist_queued_payload_to_store(store, &queued).expect("entry persists");
        let key = store.list().pop().expect("the entry is listed");

        let err = target.send_from_store(key.clone()).await.expect_err("the attempt fails");
        assert!(
            matches!(err, TargetError::NotConnected),
            "the failed connect surfaces as a connectivity error, got {err:?}"
        );
        assert_eq!(store.len(), 1, "the queued entry survives the failed attempt");
        assert!(store.get_raw(&key).is_ok(), "the surviving entry is still readable");
    }

    #[tokio::test(start_paused = true)]
    async fn publish_path_consults_the_stream_gate_before_publishing() {
        // The publish path runs stream validation first, so an unvalidated stream fails with the validation label rather than the publish call.
        let client = async_nats::ConnectOptions::new()
            .retry_on_initial_connect()
            .connect("nats://127.0.0.1:1")
            .await
            .expect("retry_on_initial_connect returns without a live server");

        let target = nats_target(jetstream_args(TargetType::NotifyEvent));
        let fingerprint = build_target_tls_fingerprint("", "", "").await.expect("fingerprint builds");
        target.tls_state.lock().refresh(fingerprint);
        *target.client.lock().await = Some(client.clone());

        let mut context = jetstream::new(client);
        // Shorter than the 30s attempt deadline, so the validation lookup elapses first.
        context.set_timeout(Duration::from_secs(5));
        let cached = CachedJetStreamContext::new(context);
        let verdict = Arc::clone(&cached.stream_validated);
        *target.jetstream_context.lock().await = Some(cached);

        let err = target
            .publish_jetstream(b"body".to_vec(), "dedup-gate-wiring")
            .await
            .expect_err("the unvalidated stream blocks the publish");
        match err {
            TargetError::JetStreamPublish { retryable, detail } => {
                assert!(retryable, "the gate failure is retryable");
                assert_eq!(
                    detail, STREAM_VALIDATION_FAILED_DETAIL,
                    "the failure names the stream validation, not the publish"
                );
            }
            other => panic!("expected a retryable JetStreamPublish, got {other:?}"),
        }
        assert!(!verdict.load(Ordering::SeqCst), "the verdict stays closed after the failed validation");
    }

    #[tokio::test(start_paused = true)]
    async fn is_active_reports_a_failing_stream_instead_of_healthy() {
        // is_active consults the stream-validation gate, so a reachable broker with a failing stream reports the error, not healthy.
        let client = async_nats::ConnectOptions::new()
            .retry_on_initial_connect()
            .connect("nats://127.0.0.1:1")
            .await
            .expect("retry_on_initial_connect returns without a live server");

        let target = nats_target(jetstream_args(TargetType::NotifyEvent));
        let fingerprint = build_target_tls_fingerprint("", "", "").await.expect("fingerprint builds");
        target.tls_state.lock().refresh(fingerprint);
        *target.client.lock().await = Some(client.clone());

        let mut context = jetstream::new(client);
        context.set_timeout(Duration::from_secs(5));
        let cached = CachedJetStreamContext::new(context);
        let verdict = Arc::clone(&cached.stream_validated);
        *target.jetstream_context.lock().await = Some(cached);

        let err = target.is_active().await.expect_err("a failing stream fails the health check");
        match err {
            TargetError::JetStreamPublish { retryable, detail } => {
                assert!(retryable, "the health failure carries the gate classification");
                assert_eq!(
                    detail, STREAM_VALIDATION_FAILED_DETAIL,
                    "the health check reports the stream validation failure"
                );
            }
            other => panic!("expected the gate classification, got {other:?}"),
        }

        // A reset verdict forces the next check to re-run validation.
        verdict.store(true, Ordering::SeqCst);
        verdict.store(false, Ordering::SeqCst);
        let err = target
            .is_active()
            .await
            .expect_err("a freshly reset verdict re-runs validation");
        match err {
            TargetError::JetStreamPublish { retryable, detail } => {
                assert!(retryable, "the re-validation failure carries the gate classification");
                assert_eq!(
                    detail, STREAM_VALIDATION_FAILED_DETAIL,
                    "the reset verdict forces the health check to re-validate the stream"
                );
            }
            other => panic!("expected the gate classification after a reset, got {other:?}"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn attempt_deadline_fires_against_a_pending_publish_and_classifies_retryable() {
        // A publish that never resolves is cut off at the ack timeout and classified retryable.
        let result = attempt_within_deadline(Duration::from_secs(30), std::future::pending::<Result<(), TargetError>>()).await;
        match result {
            Err(TargetError::JetStreamPublish { retryable, detail }) => {
                assert!(retryable, "an elapsed attempt deadline is retryable");
                assert_eq!(detail, ATTEMPT_DEADLINE_DETAIL);
            }
            other => panic!("expected a retryable JetStreamPublish, got {other:?}"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn attempt_deadline_passes_through_a_result_within_the_deadline() {
        attempt_within_deadline(Duration::from_secs(30), async { Ok(()) })
            .await
            .expect("a resolved attempt passes its result through");

        // An inner client timeout can resolve first and keeps its own retryable classification.
        let inner = TargetError::JetStreamPublish {
            retryable: true,
            detail: "timed out".to_string(),
        };
        let err = attempt_within_deadline(Duration::from_secs(30), async { Err(inner) })
            .await
            .expect_err("an inner error passes through");
        match err {
            TargetError::JetStreamPublish { retryable, detail } => {
                assert!(retryable, "the inner timed-out classification is retryable");
                assert_eq!(detail, "timed out", "the inner detail is preserved");
            }
            other => panic!("expected a retryable JetStreamPublish, got {other:?}"),
        }
    }

    #[test]
    fn ack_timeout_uses_the_configured_value() {
        let target = nats_target(jetstream_args(TargetType::NotifyEvent));
        assert_eq!(target.ack_timeout(), Duration::from_secs(30));

        let mut args = jetstream_args(TargetType::NotifyEvent);
        args.jetstream_ack_timeout_secs = None;
        let target = nats_target(args);
        assert_eq!(
            target.ack_timeout(),
            Duration::from_secs(NATS_JETSTREAM_ACK_TIMEOUT_DEFAULT_SECS),
            "an absent ack timeout uses the default"
        );
    }

    #[test]
    fn retry_lifetime_covers_every_attempt_ack_wait_plus_full_backoff() {
        // The realized backoff schedule at the default base is 4 + 8 + 16 + 32 = 60s, plus a 64s headroom term = 124s. At the 30s ack timeout the worst case is 5 * 30 + 124 = 274s, the floor a duplicate_window must clear.
        assert_eq!(retry_lifetime(Duration::from_secs(30)), Duration::from_secs(274));
        // With a zero ack timeout only the backoff schedule and the headroom term remain.
        assert_eq!(retry_lifetime(Duration::from_secs(0)), Duration::from_secs(124));
    }

    #[test]
    fn retry_lifetime_backoff_equals_the_realized_schedule_plus_headroom() {
        // The duplicate-window backoff span must equal the worker's realized sleep schedule plus one final-shift headroom term.
        let mut realized = Duration::ZERO;
        for shift in 1..REPLAY_MAX_RETRIES as u32 {
            realized += replay_backoff_term(shift);
        }
        assert_eq!(
            realized,
            inter_attempt_backoff_sum(REPLAY_MAX_RETRIES),
            "the shared sum matches the worker sleep schedule"
        );
        let headroom = replay_backoff_term(REPLAY_MAX_RETRIES as u32);
        assert_eq!(retry_lifetime(Duration::ZERO), realized + headroom);
        assert_eq!(retry_lifetime(Duration::ZERO), Duration::from_secs(124));
    }

    #[test]
    fn clone_box_shares_one_cached_context_handle() {
        // One acker and one backpressure semaphore serve every clone of the target.
        let target = nats_target(jetstream_args(TargetType::NotifyEvent));
        let before = Arc::strong_count(&target.jetstream_context);
        let _clone = target.clone_box();
        assert_eq!(
            Arc::strong_count(&target.jetstream_context),
            before + 1,
            "clone_box shares the one context handle rather than building a new one"
        );
    }

    #[tokio::test]
    async fn close_keeps_a_queued_entry_for_replay() {
        // Close releases cached handles but never durable entries, so a queued entry survives and replays.
        let queue_dir = tempfile::tempdir().expect("queue dir");
        let target = nats_target_with_store(jetstream_args(TargetType::NotifyEvent), queue_dir.path().to_str().unwrap());
        let queued = target.build_queued_payload(&sample_event()).expect("payload builds");
        let store = target.store().expect("a store is configured");
        persist_queued_payload_to_store(store, &queued).expect("entry persists");
        assert_eq!(store.len(), 1, "the entry is queued before close");

        target.close().await.expect("close succeeds");

        let store_after = target.store().expect("the store handle is still held");
        assert_eq!(store_after.len(), 1, "the queued entry survives close");
        assert_eq!(store_after.list().len(), 1, "the surviving entry is listable for replay");
    }

    #[tokio::test]
    async fn flag_off_close_matches_the_baseline() {
        // With JetStream off no context is cached and close drains only the client. A queued entry still survives.
        let queue_dir = tempfile::tempdir().expect("queue dir");
        let target = nats_target_with_store(base_args(), queue_dir.path().to_str().unwrap());
        assert!(!target.jetstream_enabled(), "the flag is off");
        assert!(target.jetstream_context.lock().await.is_none(), "no context is cached with the flag off");

        let queued = target.build_queued_payload(&sample_event()).expect("payload builds");
        let store = target.store().expect("a store is configured");
        persist_queued_payload_to_store(store, &queued).expect("entry persists");

        target.close().await.expect("flag-off close succeeds");

        assert!(
            target.jetstream_context.lock().await.is_none(),
            "the flag-off close leaves the context field empty"
        );
        assert_eq!(target.store().expect("store held").len(), 1, "the queued entry survives a flag-off close");
    }

    #[tokio::test]
    async fn drain_jetstream_context_handles_none() {
        // A None context (nothing cached) is a no-op.
        drain_jetstream_context(None).await;
    }

    #[tokio::test]
    #[ignore]
    async fn tls_change_rebuilds_the_context_and_drains_the_old_acker() {
        // A TLS fingerprint change on the publish path rebuilds the cached context from the new client and drains the old acker.
        let subject = format!("rustfs.tlsrebuild.{}", Uuid::new_v4().simple());
        let stream_name = format!("RUSTFS_TLSREBUILD_{}", Uuid::new_v4().simple());

        let provision_client = async_nats::connect(broker_url()).await.expect("connect to provision");
        let provision_context = jetstream::new(provision_client);
        provision_context
            .create_stream(jetstream::stream::Config {
                name: stream_name.clone(),
                subjects: vec![subject.clone()],
                duplicate_window: Duration::from_secs(300),
                ..Default::default()
            })
            .await
            .expect("provision the stream");

        let queue_dir = tempfile::tempdir().expect("queue dir");
        let mut args = jetstream_args(TargetType::NotifyEvent);
        args.address = broker_url();
        args.subject = subject.clone();
        args.jetstream_stream_name = Some(stream_name.clone());
        args.queue_dir = queue_dir.path().to_str().unwrap().to_string();
        args.tls_required = false;

        let target = nats_target(args);

        // First publish builds and caches context A bound to the first connection.
        target
            .publish_jetstream(b"first".to_vec(), "dedup-first")
            .await
            .expect("publish A");
        let context_a = target.jetstream_context().await.expect("context A is cached").context;
        // Read the client_id from the cache without get_or_connect, so the read never triggers a reconnect.
        let cached_client_id = || async {
            target
                .client
                .lock()
                .await
                .as_ref()
                .expect("a client is cached")
                .server_info()
                .client_id
        };
        let client_id_a = cached_client_id().await;

        // Reset the fingerprint so the next publish detects a change, as a rotated cert would.
        target.tls_state.lock().reset();

        // The next publish detects the change, reconnects, rebuilds the context, and drains context A.
        target
            .publish_jetstream(b"second".to_vec(), "dedup-second")
            .await
            .expect("publish B on the rebuilt context");
        let client_id_b = cached_client_id().await;

        // The cached client id changed, so only the publish path could have reconnected onto a new connection.
        assert_ne!(
            client_id_a, client_id_b,
            "the production publish path rebuilt onto a fresh connection after the material change"
        );

        // A further publish on the rebuilt context acks.
        target
            .publish_jetstream(b"third".to_vec(), "dedup-third")
            .await
            .expect("third publish on the rebuilt context acks");

        // Context A was drained in the swap, so a fresh wait returns promptly.
        tokio::time::timeout(Duration::from_secs(2), context_a.wait_for_acks())
            .await
            .expect("the old context is fully drained, its acker has nothing parked");

        // Best-effort teardown.
        let _ = provision_context.delete_stream(&stream_name).await;
    }

    #[tokio::test]
    #[ignore]
    async fn tls_change_after_a_failed_reconnect_still_rebuilds_the_context() {
        // A rotation detected while the broker is unreachable does not orphan the cached context: a failed reconnect followed by a successful one ends bound to the rebuilt context.
        let subject = format!("rustfs.tlsfail.{}", Uuid::new_v4().simple());
        let stream_name = format!("RUSTFS_TLSFAIL_{}", Uuid::new_v4().simple());

        let provision_client = async_nats::connect(broker_url()).await.expect("connect to provision");
        let provision_context = jetstream::new(provision_client);
        provision_context
            .create_stream(jetstream::stream::Config {
                name: stream_name.clone(),
                subjects: vec![subject.clone()],
                duplicate_window: Duration::from_secs(300),
                ..Default::default()
            })
            .await
            .expect("provision the stream");

        let queue_dir = tempfile::tempdir().expect("queue dir");
        let mut args = jetstream_args(TargetType::NotifyEvent);
        args.address = broker_url();
        args.subject = subject.clone();
        args.jetstream_stream_name = Some(stream_name.clone());
        args.queue_dir = queue_dir.path().to_str().unwrap().to_string();
        args.tls_required = false;

        let mut target = nats_target(args);

        // First production publish builds and caches context A bound to the first connection.
        target
            .publish_jetstream(b"first".to_vec(), "dedup-first")
            .await
            .expect("publish A");
        let context_a = target.jetstream_context().await.expect("context A is cached").context;
        let client_id_a = context_a.client().server_info().client_id;

        // Signal a rotation, then point at a dead port so the first reconnect fails.
        target.tls_state.lock().reset();
        let good_address = target.args.address.clone();
        target.args.address = "nats://127.0.0.1:1".to_string();

        let failed = target.publish_jetstream(b"second".to_vec(), "dedup-second").await;
        assert!(
            failed.is_err(),
            "the first reconnect after the change fails while the broker is unreachable"
        );

        // The broker returns. A later publish reconnects, rebuilds the context, and drains the old one.
        target.args.address = good_address;
        target
            .publish_jetstream(b"third".to_vec(), "dedup-third")
            .await
            .expect("publish on the rebuilt context");

        // The cached context is bound to a fresh connection, not the pre-rotation one.
        let cached_context = target
            .jetstream_context
            .lock()
            .await
            .as_ref()
            .expect("a context is cached")
            .clone()
            .context;
        let cached_context_client_id = cached_context.client().server_info().client_id;
        assert_ne!(
            cached_context_client_id, client_id_a,
            "the cached context was rebuilt onto a fresh connection after the failed-then-successful reconnect"
        );

        // Context A was drained in the swap, so a fresh wait returns promptly.
        tokio::time::timeout(Duration::from_secs(2), context_a.wait_for_acks())
            .await
            .expect("the pre-rotation context is drained, its acker has nothing parked");

        let _ = provision_context.delete_stream(&stream_name).await;
    }

    #[tokio::test]
    #[ignore]
    async fn publish_gate_rejects_an_unsafe_stream_and_heals_after_the_stream_is_fixed() {
        // The gate rejects every publish while the stream's duplicate window is below the retry lifetime, and starts publishing once the operator widens it, without a restart.
        let subject = format!("rustfs.gate.{}", Uuid::new_v4().simple());
        let stream_name = format!("RUSTFS_GATE_{}", Uuid::new_v4().simple());

        let provision_client = async_nats::connect(broker_url()).await.expect("connect to provision");
        let provision_context = jetstream::new(provision_client);
        provision_context
            .create_stream(jetstream::stream::Config {
                name: stream_name.clone(),
                subjects: vec![subject.clone()],
                // Below the 274s required at the default 30s ack timeout.
                duplicate_window: Duration::from_secs(60),
                ..Default::default()
            })
            .await
            .expect("provision the unsafe stream");

        let queue_dir = tempfile::tempdir().expect("queue dir");
        let mut args = jetstream_args(TargetType::NotifyEvent);
        args.address = broker_url();
        args.subject = subject.clone();
        args.jetstream_stream_name = Some(stream_name.clone());
        args.queue_dir = queue_dir.path().to_str().unwrap().to_string();
        args.tls_required = false;

        let target = nats_target(args);

        let err = target
            .publish_jetstream(b"first".to_vec(), "dedup-gate-first")
            .await
            .expect_err("no publish proceeds against a stream that fails validation");
        assert!(
            matches!(err, TargetError::JetStreamPublish { retryable: true, .. }),
            "the gate failure is retryable, got {err:?}"
        );

        // The operator widens the window past the retry lifetime, and the next attempt re-validates and publishes.
        provision_context
            .update_stream(jetstream::stream::Config {
                name: stream_name.clone(),
                subjects: vec![subject.clone()],
                duplicate_window: Duration::from_secs(300),
                ..Default::default()
            })
            .await
            .expect("widen the duplicate window");

        target
            .publish_jetstream(b"second".to_vec(), "dedup-gate-second")
            .await
            .expect("the repaired stream accepts the publish without a restart");

        let _ = provision_context.delete_stream(&stream_name).await;
    }

    #[tokio::test]
    #[ignore]
    async fn a_remapped_subject_is_rejected_by_the_ack_stream_check_and_the_entry_stays_queued() {
        // After a subject remap the takeover stream acknowledges, so the ack-stream check rejects it with the mismatch detail, keeps the entry queued, and resets the verdict for re-validation.
        let subject = format!("rustfs.remap.{}", Uuid::new_v4().simple());
        let configured_stream = format!("RUSTFS_REMAP_A_{}", Uuid::new_v4().simple());
        let takeover_stream = format!("RUSTFS_REMAP_B_{}", Uuid::new_v4().simple());
        let parked_subject = format!("rustfs.remapparked.{}", Uuid::new_v4().simple());

        let provision_client = async_nats::connect(broker_url()).await.expect("connect to provision");
        let provision_context = jetstream::new(provision_client);
        provision_context
            .create_stream(jetstream::stream::Config {
                name: configured_stream.clone(),
                subjects: vec![subject.clone()],
                duplicate_window: Duration::from_secs(300),
                ..Default::default()
            })
            .await
            .expect("provision the configured stream");

        let queue_dir = tempfile::tempdir().expect("queue dir");
        let mut args = jetstream_args(TargetType::NotifyEvent);
        args.address = broker_url();
        args.subject = subject.clone();
        args.jetstream_stream_name = Some(configured_stream.clone());
        let target = nats_target_with_store(args, queue_dir.path().to_str().unwrap());

        // The pre-remap publish validates the stream and acks from it, recording a passing verdict.
        target
            .publish_jetstream(b"pre-remap".to_vec(), "dedup-remap-first")
            .await
            .expect("the configured stream acknowledges before the remap");

        // The remap: the configured stream stops capturing the subject and the takeover stream starts.
        provision_context
            .update_stream(jetstream::stream::Config {
                name: configured_stream.clone(),
                subjects: vec![parked_subject],
                duplicate_window: Duration::from_secs(300),
                ..Default::default()
            })
            .await
            .expect("remap the configured stream away from the subject");
        provision_context
            .create_stream(jetstream::stream::Config {
                name: takeover_stream.clone(),
                subjects: vec![subject.clone()],
                duplicate_window: Duration::from_secs(300),
                ..Default::default()
            })
            .await
            .expect("provision the takeover stream");

        let queued = target.build_queued_payload(&sample_event()).expect("payload builds");
        let store = target.store().expect("a store is configured");
        persist_queued_payload_to_store(store, &queued).expect("entry persists");
        let key = store.list().pop().expect("the entry is listed");

        let log = CapturedLog::default();
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::WARN)
            .with_writer(log.clone())
            .finish();
        let _log_guard = tracing::subscriber::set_default(subscriber);

        let err = target
            .send_from_store(key.clone())
            .await
            .expect_err("the takeover stream's acknowledgment is rejected");
        match err {
            TargetError::JetStreamPublish { retryable, detail } => {
                assert!(retryable, "the mismatch keeps the entry on the live queue");
                assert_eq!(detail, ACK_STREAM_MISMATCH_DETAIL, "the fixed mismatch label is the detail");
            }
            other => panic!("expected the mismatch classification, got {other:?}"),
        }
        assert_eq!(store.len(), 1, "the queued entry survives the rejected acknowledgment");

        let warn_line = log.contents();
        assert!(
            warn_line.contains("acknowledged by an unexpected stream"),
            "the mismatch warn is logged: {warn_line}"
        );
        assert!(
            warn_line.contains(&takeover_stream),
            "the warn names the acknowledging stream: {warn_line}"
        );

        // The reset verdict forces re-validation on the next attempt, which reports the configured
        // stream no longer captures the subject. The entry stays queued through that failure too.
        let err = target
            .send_from_store(key)
            .await
            .expect_err("re-validation fails while the subject is remapped away");
        match err {
            TargetError::JetStreamPublish { detail, .. } => {
                assert_eq!(detail, STREAM_VALIDATION_FAILED_DETAIL, "the retry re-validates the stream");
            }
            other => panic!("expected the validation classification, got {other:?}"),
        }
        assert_eq!(store.len(), 1, "the entry stays queued while the remap persists");

        let _ = provision_context.delete_stream(&configured_stream).await;
        let _ = provision_context.delete_stream(&takeover_stream).await;
    }

    // A successful move deletes the live entry and leaves exactly one failed entry.
    #[tokio::test]
    async fn move_entry_writes_failed_then_deletes_live() {
        let dir = failed_store_dir("move-order");
        let store = QueueStore::<QueuedPayload>::new_with_compression(&dir, 8, ".test", false);
        store.open().unwrap();

        let key = store.put_raw(&sample_queued("minted-id").encode().unwrap()).unwrap();
        assert_eq!(store.len(), 1);

        let target = move_test_target();
        let error = TargetError::JetStreamPublish {
            retryable: false,
            detail: "max payload exceeded".to_string(),
        };
        move_entry_to_failed_store(&store, &store, &target.id(), &key, &error, 0)
            .await
            .unwrap();

        assert_eq!(store.len(), 0, "the live entry is deleted after the failed write");
        assert_eq!(store.failed_len(), 1, "the failed entry is written");
        assert!(matches!(store.get_raw(&key), Err(StoreError::NotFound)));

        let _ = store.delete();
    }

    // A move of an entry the queue no longer holds is a no-op, not an error.
    #[tokio::test]
    async fn move_entry_is_a_noop_when_the_live_entry_is_already_cleared() {
        let dir = failed_store_dir("move-missing");
        let store = QueueStore::<QueuedPayload>::new_with_compression(&dir, 8, ".test", false);
        store.open().unwrap();

        let absent_key = Key {
            name: "gone".to_string(),
            extension: ".test".to_string(),
            item_count: 1,
            compress: false,
        };
        let target = move_test_target();
        let error = TargetError::JetStreamPublish {
            retryable: false,
            detail: "max payload exceeded".to_string(),
        };
        move_entry_to_failed_store(&store, &store, &target.id(), &absent_key, &error, 0)
            .await
            .unwrap();

        assert_eq!(store.failed_len(), 0, "no failed entry is written for an absent live entry");

        let _ = store.delete();
    }

    // The delivery snapshot reports the failed-store depth read from the store.
    #[tokio::test]
    async fn delivery_snapshot_reports_failed_store_depth() {
        let dir = failed_store_dir("snapshot-depth");
        let store = Arc::new(QueueStore::<QueuedPayload>::new_with_compression(&dir, 8, ".test", false));
        store.open().unwrap();

        let target = move_test_target_with_store(store.clone());
        assert_eq!(
            target.delivery_snapshot().failed_store_length,
            0,
            "an empty failed store reports depth zero"
        );

        let key = store.put_raw(&sample_queued("minted-id").encode().unwrap()).unwrap();
        let error = TargetError::JetStreamPublish {
            retryable: false,
            detail: "max payload exceeded".to_string(),
        };
        move_entry_to_failed_store(&*store, &*store, &target.id(), &key, &error, 0)
            .await
            .unwrap();

        assert_eq!(
            target.delivery_snapshot().failed_store_length,
            1,
            "a terminal failure lands one entry and the snapshot reports depth one"
        );

        let _ = store.delete();
    }

    // For a pre-enable entry with an empty stored id, the failed record carries the id derived from the entry key.
    #[tokio::test]
    async fn move_entry_records_the_resolved_dedup_id_for_a_pre_enable_entry() {
        let dir = failed_store_dir("resolved-id");
        let store = QueueStore::<QueuedPayload>::new_with_compression(&dir, 8, ".test", false);
        store.open().unwrap();

        let key = store.put_raw(&sample_queued("").encode().unwrap()).unwrap();
        let expected = resolve_dedup_id("", &key);
        assert!(!expected.is_empty(), "the resolved id is derived from the entry key");

        let target = move_test_target();
        let error = TargetError::JetStreamPublish {
            retryable: false,
            detail: "max payload exceeded".to_string(),
        };
        move_entry_to_failed_store(&store, &store, &target.id(), &key, &error, 0)
            .await
            .unwrap();

        let failed_dir = dir.join("failed");
        let failed_file = std::fs::read_dir(&failed_dir)
            .unwrap()
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .find(|path| path.is_file())
            .expect("a failed entry is written");
        let decoded = QueuedPayload::decode(&std::fs::read(&failed_file).unwrap()).unwrap();
        assert_eq!(
            decoded.meta.failure.unwrap().nats_msg_id,
            expected,
            "the failed record carries the resolved id, not the empty stored id"
        );

        let _ = store.delete();
    }

    // The move does not count the failure, so a move followed by one replay hook emit counts it exactly once.
    #[tokio::test]
    async fn a_single_failed_delivery_counts_once() {
        let dir = failed_store_dir("count-once");
        let store = Arc::new(QueueStore::<QueuedPayload>::new_with_compression(&dir, 8, ".test", false));
        store.open().unwrap();

        let failed = Arc::new(AtomicU64::new(0));
        let target: Arc<dyn Target<String> + Send + Sync> = Arc::new(MoveTestTarget {
            id: TargetID::new("target-a".to_string(), "nats".to_string()),
            store: Some(store.clone()),
            failed: failed.clone(),
        });

        let key = store.put_raw(&sample_queued("minted-id").encode().unwrap()).unwrap();
        let error = TargetError::JetStreamPublish {
            retryable: false,
            detail: "max payload exceeded".to_string(),
        };
        move_entry_to_failed_store(&*store, &*store, &target.id(), &key, &error, 0)
            .await
            .unwrap();
        assert_eq!(failed.load(Ordering::Relaxed), 0, "the move itself does not count the failure");

        // The replay worker follows every move with one hook emit that records the failure.
        target.record_final_failure();
        assert_eq!(failed.load(Ordering::Relaxed), 1, "one failed delivery counts exactly once");

        let _ = store.delete();
    }
}
