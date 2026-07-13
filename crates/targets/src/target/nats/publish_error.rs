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

use super::jetstream::STREAM_NOT_FOUND_DETAIL;
use crate::error::TargetError;
use async_nats::jetstream::context::PublishErrorKind;

/// Maps an async-nats publish error to a typed TargetError with the retryable-versus-terminal
/// classification, so the replay loop retries recoverable errors instead of dropping them. The
/// detail comes from the fixed vocabulary in publish_error_detail, never from raw error text.
pub(crate) fn classify_publish_error(err: &async_nats::jetstream::context::PublishError) -> TargetError {
    let retryable = match err.kind() {
        PublishErrorKind::TimedOut
        | PublishErrorKind::BrokenPipe
        | PublishErrorKind::MaxAckPending
        | PublishErrorKind::StreamNotFound => true,
        PublishErrorKind::MaxPayloadExceeded | PublishErrorKind::WrongLastMessageId | PublishErrorKind::WrongLastSequence => {
            false
        }
        PublishErrorKind::Other => !other_error_is_terminal(err),
    };
    TargetError::JetStreamPublish {
        retryable,
        detail: publish_error_detail(err),
    }
}

/// Fixed diagnostic detail for a publish failure. Each error kind maps to a constant label, and a
/// server rejection carries the numeric JetStream error code and status. Never the library rendering
/// or any server-returned text, so the detail is safe to persist in a failed-store entry.
fn publish_error_detail(err: &async_nats::jetstream::context::PublishError) -> String {
    use async_nats::jetstream::Error as JetStreamApiError;
    match err.kind() {
        PublishErrorKind::StreamNotFound => STREAM_NOT_FOUND_DETAIL.to_string(),
        PublishErrorKind::TimedOut => "publish timed out".to_string(),
        PublishErrorKind::BrokenPipe => "broken pipe".to_string(),
        PublishErrorKind::MaxAckPending => "max ack pending reached".to_string(),
        PublishErrorKind::MaxPayloadExceeded => "max payload exceeded".to_string(),
        PublishErrorKind::WrongLastMessageId => "wrong last message id".to_string(),
        PublishErrorKind::WrongLastSequence => "wrong last sequence".to_string(),
        PublishErrorKind::Other => {
            match std::error::Error::source(err).and_then(|source| source.downcast_ref::<JetStreamApiError>()) {
                Some(api_error) => format!("server error code {} status {}", api_error.error_code().0, api_error.code()),
                None => "publish failed".to_string(),
            }
        }
    }
}

/// HTTP-style 5xx status range. A rejection reporting a 5xx status is a server-side condition and
/// stays retryable regardless of its error code.
const SERVER_ERROR_STATUS_RANGE: std::ops::Range<usize> = 500..600;

/// Classifies an Other publish error against a terminal allowlist. Terminal only for an explicit
/// permanent error code, STREAM_SEALED being the sole current member. Everything else is retryable,
/// so only a recognized permanent rejection enters the failed store.
fn other_error_is_terminal(err: &async_nats::jetstream::context::PublishError) -> bool {
    use async_nats::jetstream::{Error as JetStreamApiError, ErrorCode};

    let Some(api_error) = std::error::Error::source(err).and_then(|source| source.downcast_ref::<JetStreamApiError>()) else {
        return false;
    };

    // A 5xx status is a server-side condition, so it stays retryable even when the error code is a
    // permanent one.
    if SERVER_ERROR_STATUS_RANGE.contains(&api_error.code()) {
        return false;
    }

    matches!(api_error.error_code(), ErrorCode::STREAM_SEALED)
}

/// Classifies a NATS publish failure by its typed error kind. Send means the outbound channel is
/// closed and is retriable. Payload and subject violations are permanent request-level errors
/// (backlog#971, backlog#973).
pub(crate) fn classify_nats_publish_error(err: &async_nats::PublishError) -> TargetError {
    use async_nats::PublishErrorKind;
    match err.kind() {
        PublishErrorKind::Send => TargetError::NotConnected,
        PublishErrorKind::MaxPayloadExceeded | PublishErrorKind::InvalidSubject => {
            TargetError::Request(format!("Failed to publish NATS message: {err}"))
        }
    }
}

/// Classifies a NATS flush failure. Both kinds mean the message was not confirmed on the wire, so
/// the event is kept for replay (backlog#971, backlog#973).
pub(crate) fn classify_nats_flush_error(err: &async_nats::client::FlushError) -> TargetError {
    use async_nats::client::FlushErrorKind;
    match err.kind() {
        FlushErrorKind::SendError | FlushErrorKind::FlushError => TargetError::NotConnected,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::target::nats::jetstream::{ACK_STREAM_MISMATCH_DETAIL, ATTEMPT_DEADLINE_DETAIL};
    use crate::target::nats::validation::gate_publish_on_stream_validation;
    use async_nats::jetstream::context::{PublishError, PublishErrorKind};
    use std::sync::atomic::AtomicBool;

    #[test]
    fn nats_publish_error_classification() {
        use async_nats::PublishErrorKind;
        // Send means the outbound channel is gone: retriable connectivity error.
        let send_err = async_nats::PublishError::new(PublishErrorKind::Send);
        assert!(matches!(classify_nats_publish_error(&send_err), TargetError::NotConnected));

        // Payload and subject violations are permanent request-level errors.
        let payload_err = async_nats::PublishError::new(PublishErrorKind::MaxPayloadExceeded);
        assert!(matches!(classify_nats_publish_error(&payload_err), TargetError::Request(_)));
        let subject_err = async_nats::PublishError::new(PublishErrorKind::InvalidSubject);
        assert!(matches!(classify_nats_publish_error(&subject_err), TargetError::Request(_)));
    }

    #[test]
    fn nats_flush_error_classification() {
        use async_nats::client::{FlushError, FlushErrorKind};
        for kind in [FlushErrorKind::SendError, FlushErrorKind::FlushError] {
            assert!(matches!(classify_nats_flush_error(&FlushError::new(kind)), TargetError::NotConnected));
        }
    }

    #[test]
    fn classify_publish_error_marks_recoverable_kinds_retryable() {
        for kind in [
            PublishErrorKind::TimedOut,
            PublishErrorKind::BrokenPipe,
            PublishErrorKind::MaxAckPending,
            PublishErrorKind::StreamNotFound,
        ] {
            let classified = classify_publish_error(&PublishError::new(kind));
            match classified {
                TargetError::JetStreamPublish { retryable, .. } => assert!(retryable, "{kind} is retryable"),
                other => panic!("expected JetStreamPublish, got {other:?}"),
            }
        }
    }

    #[test]
    fn classify_publish_error_marks_terminal_kinds_terminal() {
        for kind in [
            PublishErrorKind::MaxPayloadExceeded,
            PublishErrorKind::WrongLastMessageId,
            PublishErrorKind::WrongLastSequence,
        ] {
            let classified = classify_publish_error(&PublishError::new(kind));
            match classified {
                TargetError::JetStreamPublish { retryable, .. } => assert!(!retryable, "{kind} is terminal"),
                other => panic!("expected JetStreamPublish, got {other:?}"),
            }
        }
    }

    #[test]
    fn classify_publish_error_other_without_source_is_retryable() {
        // An undecodable Other carries no code to recognize as permanent, so it stays on the retry path.
        let classified = classify_publish_error(&PublishError::new(PublishErrorKind::Other));
        match classified {
            TargetError::JetStreamPublish { retryable, .. } => assert!(retryable, "an undecodable Other is retryable"),
            other => panic!("expected JetStreamPublish, got {other:?}"),
        }
    }

    #[test]
    fn classify_publish_error_unknown_code_is_retryable() {
        // A decoded rejection carrying a code outside the terminal allowlist is retryable.
        let api_error: async_nats::jetstream::Error =
            serde_json::from_value(serde_json::json!({"code": 400, "err_code": 10999, "description": "an unrecognized code"}))
                .expect("api error deserializes");
        let publish_error = PublishError::with_source(PublishErrorKind::Other, api_error);
        match classify_publish_error(&publish_error) {
            TargetError::JetStreamPublish { retryable, .. } => assert!(retryable, "an unknown error code is retryable"),
            other => panic!("expected JetStreamPublish, got {other:?}"),
        }
    }

    #[test]
    fn classify_publish_error_other_with_transient_code_is_retryable() {
        // Server conditions that each clear on their own. None is in the terminal allowlist, so each
        // is retryable. The status is 400 in every case, so retryability comes from the allowlist
        // policy rather than the 5xx status range.
        for (err_code, condition) in [
            (10002, "account resources exceeded"),
            (10008, "cluster not available"),
            (10009, "cluster not leader"),
            (10028, "memory resources exceeded"),
            (10039, "jetstream not enabled for account"),
            (10040, "cluster peer not a member"),
            (10041, "raft general error"),
            (10076, "jetstream not enabled"),
            (10118, "stream offline"),
            (10194, "stream offline with a reason"),
            (10202, "server member change in flight"),
        ] {
            let api_error: async_nats::jetstream::Error =
                serde_json::from_value(serde_json::json!({"code": 400, "err_code": err_code, "description": condition}))
                    .expect("api error deserializes");
            let publish_error = PublishError::with_source(PublishErrorKind::Other, api_error);
            let classified = classify_publish_error(&publish_error);
            match classified {
                TargetError::JetStreamPublish { retryable, .. } => {
                    assert!(retryable, "transient code {err_code} ({condition}) is retryable")
                }
                other => panic!("expected JetStreamPublish for code {err_code}, got {other:?}"),
            }
        }
    }

    #[test]
    fn classify_publish_error_uncoded_server_error_status_is_retryable() {
        // A rejection with no err_code and a 5xx status is a server-side condition, kept on the retry path.
        for status in [500, 503] {
            let api_error: async_nats::jetstream::Error =
                serde_json::from_value(serde_json::json!({"code": status, "description": "insufficient resources"}))
                    .expect("api error deserializes");
            let publish_error = PublishError::with_source(PublishErrorKind::Other, api_error);
            let classified = classify_publish_error(&publish_error);
            match classified {
                TargetError::JetStreamPublish { retryable, .. } => {
                    assert!(retryable, "an uncoded {status} status is retryable")
                }
                other => panic!("expected JetStreamPublish for status {status}, got {other:?}"),
            }
        }
    }

    #[test]
    fn classify_publish_error_other_with_terminal_code_is_terminal() {
        // STREAM_SEALED (10109) is the sole terminal-allowlist member and its 400 status keeps it terminal.
        let api_error: async_nats::jetstream::Error =
            serde_json::from_value(serde_json::json!({"code": 400, "err_code": 10109, "description": "stream sealed"}))
                .expect("api error deserializes");
        let publish_error = PublishError::with_source(PublishErrorKind::Other, api_error);
        match classify_publish_error(&publish_error) {
            TargetError::JetStreamPublish { retryable, .. } => assert!(!retryable, "a sealed stream is terminal"),
            other => panic!("expected JetStreamPublish, got {other:?}"),
        }
    }

    #[test]
    fn classify_publish_error_uncoded_4xx_is_retryable() {
        // An uncoded 4xx rejection carries no permanent code, so the allowlist policy leaves it on the retry path.
        let api_error: async_nats::jetstream::Error =
            serde_json::from_value(serde_json::json!({"code": 400, "description": "bad request without an error code"}))
                .expect("api error deserializes");
        let publish_error = PublishError::with_source(PublishErrorKind::Other, api_error);
        match classify_publish_error(&publish_error) {
            TargetError::JetStreamPublish { retryable, .. } => assert!(retryable, "an uncoded 4xx rejection is retryable"),
            other => panic!("expected JetStreamPublish, got {other:?}"),
        }
    }

    #[test]
    fn classify_publish_error_terminal_code_with_server_error_status_is_retryable() {
        // Deliberate policy: a 5xx status classifies retryable even when the error code is terminal.
        let api_error: async_nats::jetstream::Error =
            serde_json::from_value(serde_json::json!({"code": 503, "err_code": 10109, "description": "stream sealed"}))
                .expect("api error deserializes");
        let publish_error = PublishError::with_source(PublishErrorKind::Other, api_error);
        let classified = classify_publish_error(&publish_error);
        match classified {
            TargetError::JetStreamPublish { retryable, .. } => {
                assert!(retryable, "a 5xx status keeps the STREAM_SEALED code on the retry path")
            }
            other => panic!("expected JetStreamPublish, got {other:?}"),
        }
    }

    #[test]
    fn classify_publish_error_detail_omits_server_body() {
        // A server rejection contributes its numeric code and status, never the server-returned description.
        let api_error: async_nats::jetstream::Error = serde_json::from_value(
            serde_json::json!({"code": 503, "err_code": 10008, "description": "secret-token-abc123 in description"}),
        )
        .expect("api error deserializes");
        let classified = classify_publish_error(&PublishError::with_source(PublishErrorKind::Other, api_error));
        match classified {
            TargetError::JetStreamPublish { detail, .. } => {
                assert_eq!(detail, "server error code 10008 status 503");
            }
            other => panic!("expected JetStreamPublish, got {other:?}"),
        }
    }

    #[test]
    fn classified_publish_details_use_the_fixed_vocabulary() {
        // Every classification path draws its detail from fixed labels and numeric fields. The pattern
        // (lowercase letters, digits, spaces, underscores, colons) rejects raw error text.
        let is_fixed = |detail: &str| {
            !detail.is_empty()
                && detail
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, ' ' | '_' | ':'))
        };

        let mut details = vec![ATTEMPT_DEADLINE_DETAIL.to_string(), ACK_STREAM_MISMATCH_DETAIL.to_string()];
        for kind in [
            PublishErrorKind::StreamNotFound,
            PublishErrorKind::TimedOut,
            PublishErrorKind::BrokenPipe,
            PublishErrorKind::MaxAckPending,
            PublishErrorKind::MaxPayloadExceeded,
            PublishErrorKind::WrongLastMessageId,
            PublishErrorKind::WrongLastSequence,
            PublishErrorKind::Other,
        ] {
            match classify_publish_error(&PublishError::new(kind)) {
                TargetError::JetStreamPublish { detail, .. } => details.push(detail),
                other => panic!("expected JetStreamPublish, got {other:?}"),
            }
        }

        let api_error: async_nats::jetstream::Error =
            serde_json::from_value(serde_json::json!({"code": 503, "err_code": 10077, "description": "raw server text"}))
                .expect("api error deserializes");
        match classify_publish_error(&PublishError::with_source(PublishErrorKind::Other, api_error)) {
            TargetError::JetStreamPublish { detail, .. } => details.push(detail),
            other => panic!("expected JetStreamPublish, got {other:?}"),
        }

        // The validation gate detail must stay fixed even when the verdict error carries text outside the vocabulary.
        let stream_validated = AtomicBool::new(false);
        let verdict = Err(TargetError::Configuration("Raw Display TEXT with (punctuation)".to_string()));
        match gate_publish_on_stream_validation(verdict, &stream_validated) {
            Err(TargetError::JetStreamPublish { detail, .. }) => details.push(detail),
            other => panic!("expected a JetStreamPublish gate error, got {other:?}"),
        }

        for detail in details {
            assert!(is_fixed(&detail), "detail falls outside the fixed vocabulary: {detail:?}");
        }
    }
}
