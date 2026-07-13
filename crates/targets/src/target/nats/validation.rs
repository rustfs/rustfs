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

use super::NATSArgs;
use super::jetstream::retry_lifetime;
use crate::error::TargetError;
use async_nats::jetstream;
use rustfs_config::{
    NATS_JETSTREAM_ACK_TIMEOUT_DEFAULT_SECS, NATS_JETSTREAM_ACK_TIMEOUT_MAX_SECS, NATS_JETSTREAM_ACK_TIMEOUT_MIN_SECS,
    NATS_JETSTREAM_ACK_TIMEOUT_SECS, NATS_JETSTREAM_ENABLE, NATS_JETSTREAM_STREAM_NAME, NATS_QUEUE_DIR,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tracing::{debug, error, info};

/// Validates the JetStream publish settings shared by the config-time and connect-time validators,
/// so both reject the same combinations.
pub(crate) fn validate_jetstream_settings(
    enable: bool,
    stream_name: &str,
    queue_dir: &str,
    ack_timeout_secs: Option<u64>,
) -> Result<(), TargetError> {
    if let Some(secs) = ack_timeout_secs
        && !(NATS_JETSTREAM_ACK_TIMEOUT_MIN_SECS..=NATS_JETSTREAM_ACK_TIMEOUT_MAX_SECS).contains(&secs)
    {
        return Err(TargetError::Configuration(format!(
            "{NATS_JETSTREAM_ACK_TIMEOUT_SECS} must be between {NATS_JETSTREAM_ACK_TIMEOUT_MIN_SECS} and {NATS_JETSTREAM_ACK_TIMEOUT_MAX_SECS} seconds"
        )));
    }

    if !enable {
        return Ok(());
    }

    if stream_name.trim().is_empty() {
        return Err(TargetError::Configuration(format!(
            "{NATS_JETSTREAM_STREAM_NAME} is required when {NATS_JETSTREAM_ENABLE} is on"
        )));
    }

    if queue_dir.trim().is_empty() {
        return Err(TargetError::Configuration(format!(
            "{NATS_QUEUE_DIR} is required when {NATS_JETSTREAM_ENABLE} is on"
        )));
    }

    Ok(())
}

/// Detail carried by the retryable publish error raised when the stream-validation gate closes. The
/// full validation cause is logged at error level inside the validator.
pub(crate) const STREAM_VALIDATION_FAILED_DETAIL: &str = "stream validation failed";

/// Applies a stream-validation verdict on the publish path. A pass records the verdict so later
/// publishes skip re-validation. A failure returns a retryable JetStream publish error carrying the
/// fixed label and leaves the verdict unvalidated.
pub(crate) fn gate_publish_on_stream_validation(
    verdict: Result<(), TargetError>,
    stream_validated: &AtomicBool,
) -> Result<(), TargetError> {
    match verdict {
        Ok(()) => {
            stream_validated.store(true, Ordering::Release);
            Ok(())
        }
        Err(_) => Err(TargetError::JetStreamPublish {
            retryable: true,
            detail: STREAM_VALIDATION_FAILED_DETAIL.to_string(),
        }),
    }
}

/// Reports whether a publish subject is captured by a stream subject filter, honouring the NATS
/// wildcard tokens: a single-token wildcard matches one subject token, a trailing multi-token
/// wildcard matches one or more remaining tokens, any other token matches literally.
fn subject_filter_captures(filter: &str, subject: &str) -> bool {
    let mut filter_tokens = filter.split('.');
    let mut subject_tokens = subject.split('.');

    loop {
        match (filter_tokens.next(), subject_tokens.next()) {
            (Some(">"), Some(_)) => return true,
            (Some("*"), Some(_)) => continue,
            (Some(filter_token), Some(subject_token)) if filter_token == subject_token => continue,
            (None, None) => return true,
            _ => return false,
        }
    }
}

/// Validates the configured JetStream stream from a single get_stream lookup when enabled. Asserts
/// the stream exists, captures the subject, returns acks, accepts writes, and keeps a duplicate
/// window that covers one retry cycle. The client-facing error is generic and every concrete cause
/// is logged server-side. The stream is never created, a missing stream is an error.
///
/// When validation_logged is set, the first passing validation logs at info and later revalidations
/// of the same context log at debug, so a persistent wrong-stream loop does not flood the log.
pub(crate) async fn validate_jetstream_stream(
    context: &jetstream::Context,
    args: &NATSArgs,
    target_label: &str,
    validation_logged: Option<&AtomicBool>,
) -> Result<(), TargetError> {
    if !args.jetstream_enable.unwrap_or(false) {
        return Ok(());
    }

    let stream_name = args.jetstream_stream_name.as_deref().unwrap_or_default();
    if stream_name.is_empty() {
        error!(target = %target_label, "JetStream enabled without a configured stream name");
        return Err(TargetError::Configuration("JetStream stream is not configured".to_string()));
    }

    let stream = context.get_stream(stream_name).await.map_err(|err| {
        error!(
            target = %target_label,
            stream = %stream_name,
            error = %err,
            "JetStream stream lookup failed, the stream must be pre-provisioned"
        );
        TargetError::Configuration("configured JetStream stream is unavailable".to_string())
    })?;

    assert_stream_writable(&stream.cached_info().config, args, target_label)?;
    // Info on a context's first passing validation, debug on later revalidations of the same context.
    let first_pass = validation_logged
        .map(|logged| !logged.swap(true, Ordering::AcqRel))
        .unwrap_or(true);
    if first_pass {
        info!(
            target = %target_label,
            stream = %stream_name,
            "JetStream stream validation succeeded"
        );
    } else {
        debug!(
            target = %target_label,
            stream = %stream_name,
            "JetStream stream revalidation succeeded"
        );
    }
    Ok(())
}

/// Asserts the retrieved stream configuration captures the subject, returns acks, accepts writes, and
/// keeps a duplicate window that covers one retry cycle. The client-facing error is generic, the
/// concrete cause logged server-side.
fn assert_stream_writable(config: &jetstream::stream::Config, args: &NATSArgs, target_label: &str) -> Result<(), TargetError> {
    let stream_name = args.jetstream_stream_name.as_deref().unwrap_or_default();
    let subject = &args.subject;
    let subject_bound = config.subjects.iter().any(|filter| subject_filter_captures(filter, subject));
    if !subject_bound {
        error!(
            target = %target_label,
            stream = %stream_name,
            subject = %subject,
            "configured subject is not captured by the JetStream stream subjects"
        );
        return Err(TargetError::Configuration(
            "configured JetStream stream does not capture the subject".to_string(),
        ));
    }

    if config.no_ack {
        error!(
            target = %target_label,
            stream = %stream_name,
            "JetStream stream has no_ack set, publishes would never acknowledge"
        );
        return Err(TargetError::Configuration(
            "configured JetStream stream does not acknowledge writes".to_string(),
        ));
    }

    if config.sealed {
        error!(
            target = %target_label,
            stream = %stream_name,
            "JetStream stream is sealed, writes are rejected"
        );
        return Err(TargetError::Configuration("configured JetStream stream is sealed".to_string()));
    }

    let ack_timeout = Duration::from_secs(
        args.jetstream_ack_timeout_secs
            .unwrap_or(NATS_JETSTREAM_ACK_TIMEOUT_DEFAULT_SECS),
    );
    let required_window = retry_lifetime(ack_timeout);
    if config.duplicate_window < required_window {
        error!(
            target = %target_label,
            stream = %stream_name,
            configured_window_secs = config.duplicate_window.as_secs(),
            required_window_secs = required_window.as_secs(),
            "JetStream stream duplicate_window is below the retry lifetime, a late retry would be delivered twice"
        );
        return Err(TargetError::Configuration(
            "configured JetStream stream duplicate window is too small for the retry lifetime".to_string(),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::target::TargetType;
    use crate::target::nats::test_support::*;

    #[test]
    fn jetstream_settings_disabled_passes() {
        validate_jetstream_settings(false, "", "", None).expect("disabled jetstream validates");
        validate_jetstream_settings(false, "", "", Some(30)).expect("disabled jetstream ignores stream and queue");
    }

    #[test]
    fn jetstream_settings_ack_timeout_range() {
        for secs in [
            NATS_JETSTREAM_ACK_TIMEOUT_MIN_SECS,
            NATS_JETSTREAM_ACK_TIMEOUT_DEFAULT_SECS,
            NATS_JETSTREAM_ACK_TIMEOUT_MAX_SECS,
        ] {
            validate_jetstream_settings(false, "", "", Some(secs)).expect("in-range ack timeout accepted");
        }
        for secs in [
            NATS_JETSTREAM_ACK_TIMEOUT_MIN_SECS - 1,
            NATS_JETSTREAM_ACK_TIMEOUT_MAX_SECS + 1,
        ] {
            let err = validate_jetstream_settings(false, "", "", Some(secs)).expect_err("out-of-range ack timeout rejected");
            assert!(err.to_string().contains("between"));
        }
    }

    #[test]
    fn jetstream_settings_requires_stream_name() {
        let err = validate_jetstream_settings(true, "  ", &nats_queue_dir(), Some(30))
            .expect_err("empty stream name rejected when enabled");
        assert!(err.to_string().contains(NATS_JETSTREAM_STREAM_NAME));
    }

    #[test]
    fn jetstream_settings_requires_queue_dir() {
        let err =
            validate_jetstream_settings(true, "RUSTFS_EVENTS", "", Some(30)).expect_err("empty queue_dir rejected when enabled");
        assert!(err.to_string().contains(NATS_QUEUE_DIR));
    }

    #[test]
    fn jetstream_settings_enabled_with_stream_and_queue_passes() {
        validate_jetstream_settings(true, "RUSTFS_EVENTS", &nats_queue_dir(), Some(30))
            .expect("a stream name and queue dir accept enabling jetstream");
    }

    #[test]
    fn natsargs_validate_rejects_enabled_jetstream_without_stream() {
        let args = NATSArgs {
            queue_dir: nats_queue_dir(),
            jetstream_enable: Some(true),
            jetstream_stream_name: None,
            ..base_args()
        };
        let err = args.validate().expect_err("enabled jetstream without a stream is rejected");
        assert!(err.to_string().contains(NATS_JETSTREAM_STREAM_NAME));
    }

    #[test]
    fn subject_filter_matches_literal_and_wildcards() {
        assert!(subject_filter_captures("rustfs.events", "rustfs.events"));
        assert!(subject_filter_captures("rustfs.*", "rustfs.events"));
        assert!(subject_filter_captures("rustfs.>", "rustfs.events.created"));
        assert!(subject_filter_captures("rustfs.*.created", "rustfs.events.created"));
        assert!(!subject_filter_captures("rustfs.events", "rustfs.audit"));
        assert!(!subject_filter_captures("rustfs.*", "rustfs.events.created"));
        assert!(!subject_filter_captures("rustfs.events", "rustfs.events.created"));
        assert!(!subject_filter_captures("rustfs.events.created", "rustfs.events"));
    }

    #[test]
    fn assert_stream_writable_enforces_the_worst_case_window_at_the_default_ack_timeout() {
        // The default 30s ack timeout requires a 274s window. One second below is rejected, exactly at it is accepted.
        let args = jetstream_args(TargetType::NotifyEvent);
        assert_eq!(retry_lifetime(Duration::from_secs(30)), Duration::from_secs(274));

        let mut below = writable_stream_config(&args.subject);
        below.duplicate_window = Duration::from_secs(273);
        let err = assert_stream_writable(&below, &args, "test").expect_err("a window below the worst case is rejected");
        assert!(matches!(err, TargetError::Configuration(_)));

        let mut at = writable_stream_config(&args.subject);
        at.duplicate_window = Duration::from_secs(274);
        assert_stream_writable(&at, &args, "test").expect("a window at the worst case is accepted");
    }

    #[test]
    fn assert_stream_writable_accepts_a_valid_stream() {
        for target_type in [TargetType::NotifyEvent, TargetType::AuditLog] {
            let args = jetstream_args(target_type);
            let config = writable_stream_config(&args.subject);
            assert_stream_writable(&config, &args, "test")
                .unwrap_or_else(|err| panic!("a writable stream passes for {target_type:?}: {err}"));
        }
    }

    #[test]
    fn assert_stream_writable_passes_when_subject_is_bound_by_wildcard() {
        let args = jetstream_args(TargetType::NotifyEvent);
        let mut config = writable_stream_config(&args.subject);
        config.subjects = vec!["rustfs.>".to_string()];
        // The configured subject rustfs.events is captured by the rustfs.> filter.
        assert_stream_writable(&config, &args, "test").expect("a wildcard-bound subject passes");
    }

    #[test]
    fn assert_stream_writable_rejects_a_subject_not_bound() {
        let args = jetstream_args(TargetType::NotifyEvent);
        let mut config = writable_stream_config(&args.subject);
        config.subjects = vec!["some.other.subject".to_string()];
        let err = assert_stream_writable(&config, &args, "test").expect_err("an unbound subject is rejected");
        assert!(matches!(err, TargetError::Configuration(_)), "the error is a generic configuration error");
        assert!(
            !err.to_string().contains("some.other.subject"),
            "the client message omits the stream subjects"
        );
    }

    #[test]
    fn assert_stream_writable_rejects_no_ack() {
        let args = jetstream_args(TargetType::NotifyEvent);
        let mut config = writable_stream_config(&args.subject);
        config.no_ack = true;
        let err = assert_stream_writable(&config, &args, "test").expect_err("a no_ack stream is rejected");
        assert!(matches!(err, TargetError::Configuration(_)));
    }

    #[test]
    fn assert_stream_writable_rejects_sealed() {
        let args = jetstream_args(TargetType::NotifyEvent);
        let mut config = writable_stream_config(&args.subject);
        config.sealed = true;
        let err = assert_stream_writable(&config, &args, "test").expect_err("a sealed stream is rejected");
        assert!(matches!(err, TargetError::Configuration(_)));
    }

    #[test]
    fn assert_stream_writable_rejects_a_too_small_duplicate_window() {
        let args = jetstream_args(TargetType::NotifyEvent);
        let mut config = writable_stream_config(&args.subject);
        // Below the worst-case retry lifetime at the default ack timeout, so a late retry would deliver twice.
        config.duplicate_window = Duration::from_secs(90);
        let err = assert_stream_writable(&config, &args, "test").expect_err("a too-small window is rejected");
        assert!(matches!(err, TargetError::Configuration(_)));
    }

    #[test]
    fn assert_stream_writable_rejects_a_zero_duplicate_window() {
        let args = jetstream_args(TargetType::NotifyEvent);
        let mut config = writable_stream_config(&args.subject);
        // The window defaults to zero (dedup disabled) when the operator leaves it unset.
        config.duplicate_window = Duration::ZERO;
        let err = assert_stream_writable(&config, &args, "test").expect_err("a zero window is rejected");
        assert!(matches!(err, TargetError::Configuration(_)));
    }

    #[test]
    fn stream_validation_gate_blocks_the_publish_and_classifies_retryable() {
        // A failed validation returns before any publish call, classified retryable so the entry is never lost.
        let stream_validated = AtomicBool::new(false);
        let verdict = Err(TargetError::Configuration("configured JetStream stream is unavailable".to_string()));
        let err = gate_publish_on_stream_validation(verdict, &stream_validated).expect_err("a failed validation closes the gate");
        match err {
            TargetError::JetStreamPublish { retryable, detail } => {
                assert!(retryable, "a validation failure is retryable");
                assert_eq!(detail, STREAM_VALIDATION_FAILED_DETAIL, "the fixed label is the detail");
            }
            other => panic!("expected a retryable JetStreamPublish, got {other:?}"),
        }
        assert!(
            !stream_validated.load(Ordering::SeqCst),
            "the verdict stays closed so the next attempt re-validates"
        );
    }

    #[test]
    fn stream_validation_gate_pass_is_recorded_with_the_context() {
        let stream_validated = AtomicBool::new(false);
        gate_publish_on_stream_validation(Ok(()), &stream_validated).expect("a passing validation opens the gate");
        assert!(
            stream_validated.load(Ordering::SeqCst),
            "the recorded verdict lets later publishes skip re-validation"
        );
    }
}
