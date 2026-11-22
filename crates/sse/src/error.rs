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

//! The error every SSE operation reports, already classified for the S3 data path.

use rustfs_kms::{KmsError, KmsUnavailableError};
use rustfs_s3_types::S3ErrorCode;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// S3 error code a KMS key that does not exist is reported under.
pub const KMS_KEY_NOT_FOUND_ERROR_CODE: &str = "KMS.NotFoundException";

pub(crate) const INTERNAL_ERROR_MESSAGE: &str = "We encountered an internal error, please try again.";
const SERVICE_UNAVAILABLE_MESSAGE: &str = "The service is unavailable. Please retry.";
const ACCESS_DENIED_MESSAGE: &str = "Access Denied.";

/// A failed SSE operation, carrying the S3 error code and message the caller answers with.
///
/// `source` keeps the underlying failure for logs and for classification by
/// callers; it is never part of the response.
#[derive(Debug)]
pub struct SseError {
    pub code: S3ErrorCode,
    pub message: String,
    pub source: Option<BoxError>,
}

impl SseError {
    /// Classify a failure that did not originate from the request itself.
    ///
    /// A [`KmsUnavailableError`] is a retryable `ServiceUnavailable`, a
    /// [`KmsError`] follows [`data_plane_kms_error`], and everything else is an
    /// `InternalError` whose detail stays in `source` and out of the response.
    pub fn internal(error: impl Into<BoxError>) -> Self {
        let source = error.into();
        let (code, message) = if source.downcast_ref::<KmsUnavailableError>().is_some() {
            (S3ErrorCode::ServiceUnavailable, SERVICE_UNAVAILABLE_MESSAGE.to_string())
        } else if let Some(classified) = source.downcast_ref::<KmsError>().and_then(data_plane_kms_error) {
            classified
        } else {
            (S3ErrorCode::InternalError, INTERNAL_ERROR_MESSAGE.to_string())
        };
        Self {
            code,
            message,
            source: Some(source),
        }
    }
}

impl std::fmt::Display for SseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for SseError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_deref().map(|source| source as _)
    }
}

/// Map a KMS failure that surfaced on the S3 data path to its S3 error code.
///
/// The contract deliberately differs from the admin key-lifecycle handlers:
/// there a key id is the resource being addressed, so a missing key is `404`.
/// Here the key id arrives inside a request header or a bucket default, so a
/// key that is missing, disabled, or otherwise unusable is configuration the
/// caller has to correct — AWS answers `400`, and reporting `500` instead both
/// misfiles the failure as a server fault and makes SDKs back off and retry a
/// request that cannot succeed.
///
/// `None` keeps the caller's fallthrough, which is the `500` that integrity
/// faults — damaged, unreadable, or unknown-format key material — must keep.
///
/// Messages either name what the caller asked for or stay generic; detail that
/// belongs to the deployment rather than the request stays in `source`, which
/// the caller attaches and the logs retain.
pub fn data_plane_kms_error(error: &KmsError) -> Option<(S3ErrorCode, String)> {
    use KmsError as Kms;

    match error {
        Kms::KeyNotFound { key_id } => Some((
            S3ErrorCode::Custom(KMS_KEY_NOT_FOUND_ERROR_CODE.into()),
            format!("KMS key not found: {key_id}"),
        )),
        Kms::KeyVersionNotFound { key_id, version } => Some((
            S3ErrorCode::Custom(KMS_KEY_NOT_FOUND_ERROR_CODE.into()),
            format!("KMS key version {version} not found for key {key_id}"),
        )),
        // The key exists but its state forbids the operation (disabled,
        // pending deletion). AWS treats an invalid key state as a request
        // error, not a server fault.
        Kms::InvalidOperation { .. } => Some((S3ErrorCode::InvalidRequest, error.to_string())),
        // A policy decision needs a human, so it must be distinguishable from
        // the transient classes an SDK retries.
        Kms::AccessDenied { .. } => Some((S3ErrorCode::AccessDenied, ACCESS_DENIED_MESSAGE.to_string())),
        // Request-side faults: what was asked for cannot be served as asked.
        Kms::ContextMismatch { .. }
        | Kms::InvalidKey { .. }
        | Kms::ValidationError { .. }
        | Kms::UnsupportedAlgorithm { .. }
        | Kms::InvalidKeySize { .. } => Some((S3ErrorCode::InvalidRequest, error.to_string())),
        // A deployment whose KMS configuration cannot serve the request (for
        // example no key named and no default). Actionable, but the detail
        // describes the deployment, so it stays out of the response body.
        Kms::ConfigurationError { .. } => {
            Some((S3ErrorCode::InvalidRequest, "The KMS configuration cannot serve this request".to_string()))
        }
        // Transient: worth retrying, and must be counted against availability
        // rather than against the caller. `IoError` belongs here because it is
        // how a backend reports that its key store itself was unreachable —
        // rustfs/rustfs#7470 separated that from a missing key precisely so the
        // two stop looking alike, and leaving it on the 500 fallthrough would
        // erase that distinction again at the S3 boundary.
        Kms::BackendError { .. }
        | Kms::IoError { .. }
        | Kms::OperationTimedOut { .. }
        | Kms::OperationCancelled { .. }
        | Kms::CredentialsUnavailable { .. }
        | Kms::CacheError { .. } => Some((S3ErrorCode::ServiceUnavailable, SERVICE_UNAVAILABLE_MESSAGE.to_string())),
        // A permanent gap in the configured backend, never a missing resource.
        Kms::UnsupportedCapability { .. } => Some((S3ErrorCode::NotImplemented, error.to_string())),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn internal_keeps_request_free_failures_generic() {
        let error = SseError::internal("Failed to decode IV: bad base64");

        assert_eq!(error.code, S3ErrorCode::InternalError);
        assert_eq!(error.message, INTERNAL_ERROR_MESSAGE);
        assert!(
            error
                .source
                .as_ref()
                .is_some_and(|source| source.to_string().contains("Failed to decode IV"))
        );
    }

    #[test]
    fn internal_reports_an_unavailable_kms_as_retryable() {
        let error = SseError::internal(KmsUnavailableError);

        assert_eq!(error.code, S3ErrorCode::ServiceUnavailable);
        assert_eq!(error.message, SERVICE_UNAVAILABLE_MESSAGE);
    }

    #[test]
    fn internal_classifies_kms_errors_for_the_data_path() {
        let missing = SseError::internal(KmsError::key_not_found("absent-key"));
        assert_eq!(missing.code, S3ErrorCode::Custom(KMS_KEY_NOT_FOUND_ERROR_CODE.into()));
        assert_eq!(missing.message, "KMS key not found: absent-key");

        let damaged = SseError::internal(KmsError::cryptographic_error("decrypt", "authentication failed"));
        assert_eq!(damaged.code, S3ErrorCode::InternalError);
        assert_eq!(damaged.message, INTERNAL_ERROR_MESSAGE);
    }
}
