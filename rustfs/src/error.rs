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

use crate::storage_api::error::contract::{StorageErrorCode, range::HTTPRangeError};
use crate::storage_api::error::{PoolMetadataError, QuotaError, StorageError, unreadable_config_refusal};
use http::StatusCode;
use rustfs_kms::KmsUnavailableError;
use rustfs_lock::LockError;
use s3s::stream::{aws_chunked_stream::AwsChunkedStreamError, upload_stream::UploadStreamError};
use s3s::{S3Error, S3ErrorCode};

const CONTENT_SHA256_MISMATCH_CODE: &str = "XAmzContentSHA256Mismatch";
const CONTENT_SHA256_MISMATCH_MESSAGE: &str = "The provided 'x-amz-content-sha256' header does not match what was computed.";

const MAX_VERSIONS_EXCEEDED_CODE: &str = "MaxVersionsExceeded";
const MAX_VERSIONS_EXCEEDED_MESSAGE: &str = "You've exceeded the limit on the number of versions you can create on this object";
const SLOW_DOWN_READ_CODE: &str = "SlowDownRead";
const SLOW_DOWN_READ_MESSAGE: &str = "Resource requested is unreadable, please reduce your request rate";
const NAMESPACE_LOCK_TIMEOUT_CAUSE: &str = "namespace_lock_timeout";
/// Public message for `StorageError::UnsupportedDisk` on a request path. The
/// only request-time producer is the delete-transaction protocol when a
/// participant answers Unimplemented (a peer still on a release without the
/// recovery RPC, or a protocol-version mismatch); the variant's Display text
/// ("drive does not support O_DIRECT") describes the startup drive probe and
/// would send an operator mid-upgrade after the wrong cause.
const UNSUPPORTED_PARTICIPANT_MESSAGE: &str = "A storage participant does not support this operation's protocol version. If the cluster is being upgraded, finish upgrading every node and retry.";

/// S3 error code for a request that names a KMS key the KMS does not hold.
pub const KMS_KEY_NOT_FOUND_ERROR_CODE: &str = "KMS.NotFoundException";

fn lock_resource_class(resource: Option<&str>) -> &'static str {
    let Some(resource) = resource else {
        return "unknown";
    };
    if resource == ".rustfs.sys/multipart"
        || resource.starts_with(".rustfs.sys/multipart/")
        || resource == ".minio.sys/multipart"
        || resource.starts_with(".minio.sys/multipart/")
    {
        return "multipart_metadata";
    }
    if resource.starts_with(".rustfs.sys/") || resource.starts_with(".minio.sys/") {
        return "system_metadata";
    }
    let Some((bucket, object)) = resource.split_once('/') else {
        return "unknown";
    };
    let object = object.split('@').next().unwrap_or(object);
    if object.is_empty() || object == bucket {
        "bucket_namespace"
    } else {
        "object_namespace"
    }
}

fn record_s3_lock_failure(err: &StorageError) {
    let record = |resource: Option<&str>, mode: &'static str, reason: &'static str| {
        rustfs_io_metrics::s3_http_metrics::record_s3_http_lock_failure(lock_resource_class(resource), mode, reason);
    };
    match err {
        StorageError::Lock(lock_error) => match lock_error {
            LockError::Timeout { resource, .. } => record(Some(resource), "unknown", "timeout"),
            LockError::AlreadyLocked { resource, .. } => record(Some(resource), "unknown", "contention"),
            LockError::QuorumNotReached { .. } | LockError::InsufficientNodes { .. } => record(None, "unknown", "quorum"),
            LockError::Network { .. } | LockError::Internal { .. } => record(None, "unknown", "transport"),
            _ => record(None, "unknown", "other"),
        },
        StorageError::NamespaceLockQuorumUnavailable {
            mode, bucket, object, ..
        } => {
            let resource = format!("{bucket}/{object}");
            let mode = match *mode {
                "read" => "read",
                "write" => "write",
                _ => "other",
            };
            record(Some(&resource), mode, "quorum");
        }
        _ => {}
    }
}

/// Map a KMS failure that surfaced on the S3 data path to its S3 error code.
///
/// The contract deliberately differs from the admin lifecycle handlers
/// (`kms_key_lifecycle::lifecycle_error_status`): there a key id is the
/// resource being addressed, so a missing key is `404`. Here the key id
/// arrives inside a request header or a bucket default, so a key that is
/// missing, disabled, or otherwise unusable is configuration the caller has to
/// correct — AWS answers `400`, and reporting `500` instead both misfiles the
/// failure as a server fault and makes SDKs back off and retry a request that
/// cannot succeed.
///
/// `None` keeps the caller's fallthrough, which is the `500` that integrity
/// faults — damaged, unreadable, or unknown-format key material — must keep.
///
/// Messages either name what the caller asked for or stay generic; detail that
/// belongs to the deployment rather than the request stays in `source`, which
/// the caller attaches and the logs retain.
fn data_plane_kms_error(error: &rustfs_kms::KmsError) -> Option<(S3ErrorCode, String)> {
    use rustfs_kms::KmsError as Kms;

    let generic = |code: S3ErrorCode| {
        let message = ApiError::error_code_to_message(&code);
        Some((code, message))
    };

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
        Kms::AccessDenied { .. } => generic(S3ErrorCode::AccessDenied),
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
        | Kms::CacheError { .. } => generic(S3ErrorCode::ServiceUnavailable),
        // A permanent gap in the configured backend, never a missing resource.
        Kms::UnsupportedCapability { .. } => Some((S3ErrorCode::NotImplemented, error.to_string())),
        _ => None,
    }
}

/// HTTP status of the error codes s3s cannot derive on its own.
///
/// s3s answers `None` for every `Custom` code, which the response layer turns
/// into a 500; a code that means "your request named something that does not
/// exist" has to say so itself.
fn custom_error_status(code: &S3ErrorCode) -> Option<StatusCode> {
    match code {
        S3ErrorCode::Custom(custom)
            if matches!(
                &**custom,
                KMS_KEY_NOT_FOUND_ERROR_CODE | MAX_VERSIONS_EXCEEDED_CODE | CONTENT_SHA256_MISMATCH_CODE
            ) =>
        {
            Some(StatusCode::BAD_REQUEST)
        }
        S3ErrorCode::Custom(custom) if &**custom == SLOW_DOWN_READ_CODE => Some(StatusCode::SERVICE_UNAVAILABLE),
        _ => None,
    }
}

pub(crate) fn slow_down_read_api_error(err: StorageError) -> ApiError {
    ApiError {
        code: S3ErrorCode::Custom(SLOW_DOWN_READ_CODE.into()),
        message: SLOW_DOWN_READ_MESSAGE.to_string(),
        source: Some(Box::new(err)),
    }
}

/// Normalize a storage read-quorum failure at PUT and LIST boundaries.
///
/// Storage-level read quorum failures keep their `SlowDownRead` code for GET
/// compatibility. PUT and LIST expose storage unavailability instead of
/// throttling, while retaining the typed source for diagnostics and audit consumers.
pub(crate) fn normalize_read_quorum_error(mut err: S3Error) -> S3Error {
    let is_slow_down_read = matches!(
        err.code(),
        S3ErrorCode::Custom(code) if &**code == SLOW_DOWN_READ_CODE
    );
    if !is_slow_down_read {
        return err;
    }
    let has_read_quorum_source = err.source().is_some_and(|source| {
        error_chain_find(source, |error| {
            matches!(
                error.downcast_ref::<StorageError>(),
                Some(StorageError::ErasureReadQuorum | StorageError::InsufficientReadQuorum(_, _))
            )
            .then_some(())
        })
        .is_some()
    });

    if has_read_quorum_source {
        err.set_code(S3ErrorCode::ServiceUnavailable);
        err.set_message(ApiError::error_code_to_message(&S3ErrorCode::ServiceUnavailable));
        err.set_status_code(StatusCode::SERVICE_UNAVAILABLE);
    }

    err
}

/// Marks a request body that exceeded a presigned upload size capability.
///
/// This marker must survive the body-reader and storage layers so the client
/// receives `EntityTooLarge` instead of a generic internal error.
#[derive(Debug, Clone, Copy)]
pub(crate) struct UploadLimitExceeded {
    pub limit: u64,
}

impl std::fmt::Display for UploadLimitExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "upload exceeds the maximum content length of {} bytes", self.limit)
    }
}

impl std::error::Error for UploadLimitExceeded {}

/// Identifies inactivity of an external client body, rather than a storage timeout.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ClientBodyReadTimeout {
    pub timeout: std::time::Duration,
    pub raw_bytes_received: u64,
}

impl std::fmt::Display for ClientBodyReadTimeout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "request body made no progress for {} seconds after {} raw bytes",
            self.timeout.as_secs(),
            self.raw_bytes_received
        )
    }
}

impl std::error::Error for ClientBodyReadTimeout {}

/// Marks a server-side object/source reader failure that must not be reported as
/// a malformed client request body.
#[derive(Debug)]
pub(crate) struct ServerSideSourceReadError {
    operation: &'static str,
    source: std::io::Error,
}

impl ServerSideSourceReadError {
    pub(crate) const fn new(operation: &'static str, source: std::io::Error) -> Self {
        Self { operation, source }
    }
}

impl std::fmt::Display for ServerSideSourceReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} source read failed: {}", self.operation, self.source)
    }
}

impl std::error::Error for ServerSideSourceReadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

#[derive(Debug)]
pub struct ApiError {
    pub code: S3ErrorCode,
    pub message: String,
    pub source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for ApiError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_deref().map(|source| source as _)
    }
}

/// Only bounded classifications are safe to include in routine diagnostics.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct ApiErrorDiagnostic {
    pub storage_code: Option<StorageErrorCode>,
    pub io_kind: Option<std::io::ErrorKind>,
    pub rpc_code: Option<tonic::Code>,
    pub pool_metadata: Option<PoolMetadataDiagnostic>,
    pub truncated: bool,
}

/// Safe projection of local metadata failure context, without operation text or sources.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PoolMetadataDiagnostic {
    pub reason: &'static str,
    pub phase: &'static str,
    pub since_unix_secs: i64,
}

impl From<&PoolMetadataError> for PoolMetadataDiagnostic {
    fn from(context: &PoolMetadataError) -> Self {
        Self {
            reason: context.kind.as_str(),
            phase: context.phase,
            since_unix_secs: context.since.unix_timestamp(),
        }
    }
}

impl ApiError {
    pub(crate) fn diagnostic(&self) -> ApiErrorDiagnostic {
        let mut diagnostic = ApiErrorDiagnostic::default();
        let mut current = std::error::Error::source(self);
        for _ in 0..16 {
            let Some(error) = current else {
                return diagnostic;
            };
            if let Some(storage) = error.downcast_ref::<StorageError>() {
                diagnostic.storage_code = Some(storage.code());
            }
            if let Some(status) = error.downcast_ref::<tonic::Status>() {
                diagnostic.rpc_code = Some(status.code());
            }
            if diagnostic.pool_metadata.is_none()
                && let Some(context) = error.downcast_ref::<PoolMetadataError>()
            {
                diagnostic.pool_metadata = Some(context.into());
            }
            current = if let Some(io) = error.downcast_ref::<std::io::Error>() {
                diagnostic.io_kind = Some(io.kind());
                // io::Error::source can skip the wrapped error itself.
                io.get_ref().map(|source| source as &(dyn std::error::Error + 'static))
            } else {
                error.source()
            };
        }
        diagnostic.truncated = current.is_some();
        diagnostic
    }

    /// Access-denied error with the exact message emitted by the authorization
    /// paths in `storage::access`; callers there match on the code only.
    pub fn access_denied() -> Self {
        ApiError {
            code: S3ErrorCode::AccessDenied,
            message: "Access Denied".to_string(),
            source: None,
        }
    }

    pub fn service_unavailable() -> Self {
        ApiError {
            code: S3ErrorCode::ServiceUnavailable,
            message: Self::error_code_to_message(&S3ErrorCode::ServiceUnavailable),
            source: None,
        }
    }

    pub fn invalid_request(message: impl std::fmt::Display) -> Self {
        ApiError {
            code: S3ErrorCode::InvalidRequest,
            message: message.to_string(),
            source: None,
        }
    }

    pub fn other<E>(error: E) -> Self
    where
        E: std::fmt::Display + Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        ApiError {
            code: S3ErrorCode::InternalError,
            message: error.to_string(),
            source: Some(error.into()),
        }
    }

    pub fn error_code_to_message(code: &S3ErrorCode) -> String {
        match code {
            S3ErrorCode::InvalidRequest => "Invalid Request".to_string(),
            S3ErrorCode::InvalidArgument => "Invalid argument".to_string(),
            S3ErrorCode::InvalidStorageClass => "Invalid storage class.".to_string(),
            S3ErrorCode::AccessDenied => "Access Denied.".to_string(),
            S3ErrorCode::Custom(code) if &**code == CONTENT_SHA256_MISMATCH_CODE => {
                CONTENT_SHA256_MISMATCH_MESSAGE.to_string()
            }
            S3ErrorCode::BadDigest => "The Content-Md5 you specified did not match what we received.".to_string(),
            S3ErrorCode::EntityTooSmall => "Your proposed upload is smaller than the minimum allowed object size.".to_string(),
            S3ErrorCode::EntityTooLarge => "Your proposed upload exceeds the maximum allowed object size.".to_string(),
            S3ErrorCode::InternalError => "We encountered an internal error, please try again.".to_string(),
            S3ErrorCode::InvalidAccessKeyId => "The Access Key Id you provided does not exist in our records.".to_string(),
            S3ErrorCode::InvalidBucketName => "The specified bucket is not valid.".to_string(),
            S3ErrorCode::InvalidDigest => "The Content-Md5 you specified is not valid.".to_string(),
            S3ErrorCode::InvalidRange => "The requested range is not satisfiable".to_string(),
            S3ErrorCode::MalformedXML => "The XML you provided was not well-formed or did not validate against our published schema.".to_string(),
            S3ErrorCode::MissingContentLength => "You must provide the Content-Length HTTP header.".to_string(),
            S3ErrorCode::MissingSecurityHeader => "Your request was missing a required header".to_string(),
            S3ErrorCode::MissingRequestBodyError => "Request body is empty.".to_string(),
            S3ErrorCode::NoSuchBucket => "The specified bucket does not exist".to_string(),
            S3ErrorCode::NoSuchBucketPolicy => "The bucket policy does not exist".to_string(),
            S3ErrorCode::NoSuchLifecycleConfiguration => "The lifecycle configuration does not exist".to_string(),
            S3ErrorCode::NoSuchKey => "The specified key does not exist.".to_string(),
            S3ErrorCode::NoSuchUpload => "The specified multipart upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed.".to_string(),
            S3ErrorCode::NoSuchVersion => "The specified version does not exist.".to_string(),
            S3ErrorCode::NotImplemented => "A header you provided implies functionality that is not implemented".to_string(),
            S3ErrorCode::PreconditionFailed => "At least one of the pre-conditions you specified did not hold".to_string(),
            S3ErrorCode::SignatureDoesNotMatch => "The request signature we calculated does not match the signature you provided. Check your key and signing method.".to_string(),
            S3ErrorCode::MethodNotAllowed => "The specified method is not allowed against this resource.".to_string(),
            S3ErrorCode::InvalidPart => "One or more of the specified parts could not be found.  The part may not have been uploaded, or the specified entity tag may not match the part's entity tag.".to_string(),
            S3ErrorCode::InvalidPartOrder => "The list of parts was not in ascending order. The parts list must be specified in order by part number.".to_string(),
            S3ErrorCode::InvalidObjectState => "The operation is not valid for the current state of the object.".to_string(),
            S3ErrorCode::AuthorizationHeaderMalformed => "The authorization header is malformed; the region is wrong; expecting 'us-east-1'.".to_string(),
            S3ErrorCode::MalformedPOSTRequest => "The body of your POST request is not well-formed multipart/form-data.".to_string(),
            S3ErrorCode::BucketNotEmpty => "The bucket you tried to delete is not empty".to_string(),
            S3ErrorCode::BucketAlreadyExists => "The requested bucket name is not available. The bucket namespace is shared by all users of the system. Please select a different name and try again.".to_string(),
            S3ErrorCode::BucketAlreadyOwnedByYou => "Your previous request to create the named bucket succeeded and you already own it.".to_string(),
            S3ErrorCode::AllAccessDisabled => "All access to this resource has been disabled.".to_string(),
            S3ErrorCode::InvalidPolicyDocument => "The content of the form does not meet the conditions specified in the policy document.".to_string(),
            S3ErrorCode::IncompleteBody => "You did not provide the number of bytes specified by the Content-Length HTTP header.".to_string(),
            S3ErrorCode::RequestTimeTooSkewed => "The difference between the request time and the server's time is too large.".to_string(),
            S3ErrorCode::InvalidRegion => "Region does not match.".to_string(),
            S3ErrorCode::SlowDown => "Resource requested is unreadable, please reduce your request rate".to_string(),
            S3ErrorCode::KeyTooLongError => "Your key is too long".to_string(),
            S3ErrorCode::NoSuchTagSet => "The TagSet does not exist".to_string(),
            S3ErrorCode::ObjectLockConfigurationNotFoundError => "Object Lock configuration does not exist for this bucket".to_string(),
            S3ErrorCode::InvalidBucketState => "Object Lock configuration cannot be enabled on existing buckets".to_string(),
            S3ErrorCode::NoSuchCORSConfiguration => "The CORS configuration does not exist".to_string(),
            S3ErrorCode::NoSuchWebsiteConfiguration => "The specified bucket does not have a website configuration".to_string(),
            S3ErrorCode::NoSuchObjectLockConfiguration => "The specified object does not have a ObjectLock configuration".to_string(),
            S3ErrorCode::MetadataTooLarge => "Your metadata headers exceed the maximum allowed metadata size.".to_string(),
            S3ErrorCode::ServiceUnavailable => "The service is unavailable. Please retry.".to_string(),
            S3ErrorCode::Busy => "The service is unavailable. Please retry.".to_string(),
            S3ErrorCode::EmptyRequestBody => "Request body cannot be empty.".to_string(),
            S3ErrorCode::UnauthorizedAccess => "You are not authorized to perform this operation".to_string(),
            S3ErrorCode::ExpressionTooLong => "The SQL expression is too long: The maximum byte-length for the SQL expression is 256 KB.".to_string(),
            S3ErrorCode::IllegalSqlFunctionArgument => "Illegal argument was used in the SQL function.".to_string(),
            S3ErrorCode::InvalidKeyPath => "Key path in the SQL expression is invalid.".to_string(),
            S3ErrorCode::InvalidCompressionFormat => "The file is not in a supported compression format. Only GZIP is supported at this time.".to_string(),
            S3ErrorCode::InvalidFileHeaderInfo => "The FileHeaderInfo is invalid. Only NONE, USE, and IGNORE are supported.".to_string(),
            S3ErrorCode::InvalidJsonType => "The JsonType is invalid. Only DOCUMENT and LINES are supported at this time.".to_string(),
            S3ErrorCode::InvalidQuoteFields => "The QuoteFields is invalid. Only ALWAYS and ASNEEDED are supported.".to_string(),
            S3ErrorCode::InvalidRequestParameter => "The value of a parameter in SelectRequest element is invalid. Check the service API documentation and try again.".to_string(),
            S3ErrorCode::InvalidDataSource => "Invalid data source type. Only CSV and JSON are supported at this time.".to_string(),
            S3ErrorCode::InvalidExpressionType => "The ExpressionType is invalid. Only SQL expressions are supported at this time.".to_string(),
            S3ErrorCode::InvalidDataType => "The SQL expression contains an invalid data type.".to_string(),
            S3ErrorCode::InvalidTextEncoding => "Invalid encoding type. Only UTF-8 encoding is supported at this time.".to_string(),
            S3ErrorCode::InvalidTableAlias => "The SQL expression contains an invalid table alias.".to_string(),
            S3ErrorCode::MissingRequiredParameter => "The SelectRequest entity is missing a required parameter. Check the service documentation and try again.".to_string(),
            S3ErrorCode::ObjectSerializationConflict => "The SelectRequest entity can only contain one of CSV or JSON. Check the service documentation and try again.".to_string(),
            S3ErrorCode::UnsupportedSqlOperation => "Encountered an unsupported SQL operation.".to_string(),
            S3ErrorCode::UnsupportedSqlStructure => "Encountered an unsupported SQL structure. Check the SQL Reference.".to_string(),
            S3ErrorCode::UnsupportedSyntax => "Encountered invalid syntax.".to_string(),
            S3ErrorCode::UnsupportedRangeHeader => "Range header is not supported for this operation.".to_string(),
            S3ErrorCode::LexerInvalidChar => "The SQL expression contains an invalid character.".to_string(),
            S3ErrorCode::LexerInvalidOperator => "The SQL expression contains an invalid literal.".to_string(),
            S3ErrorCode::LexerInvalidLiteral => "The SQL expression contains an invalid operator.".to_string(),
            S3ErrorCode::LexerInvalidIONLiteral => "The SQL expression contains an invalid operator.".to_string(),
            S3ErrorCode::ParseExpectedDatePart => "Did not find the expected date part in the SQL expression.".to_string(),
            S3ErrorCode::ParseExpectedKeyword => "Did not find the expected keyword in the SQL expression.".to_string(),
            S3ErrorCode::ParseExpectedTokenType => "Did not find the expected token in the SQL expression.".to_string(),
            S3ErrorCode::ParseExpected2TokenTypes => "Did not find the expected token in the SQL expression.".to_string(),
            S3ErrorCode::ParseExpectedNumber => "Did not find the expected number in the SQL expression.".to_string(),
            S3ErrorCode::ParseExpectedRightParenBuiltinFunctionCall => "Did not find the expected right parenthesis character in the SQL expression.".to_string(),
            S3ErrorCode::ParseExpectedTypeName => "Did not find the expected type name in the SQL expression.".to_string(),
            S3ErrorCode::ParseExpectedWhenClause => "Did not find the expected WHEN clause in the SQL expression. CASE is not supported.".to_string(),
            S3ErrorCode::ParseUnsupportedToken => "The SQL expression contains an unsupported token.".to_string(),
            S3ErrorCode::ParseUnsupportedLiteralsGroupBy => "The SQL expression contains an unsupported use of GROUP BY.".to_string(),
            S3ErrorCode::ParseExpectedMember => "The SQL expression contains an unsupported use of MEMBER.".to_string(),
            S3ErrorCode::ParseUnsupportedSelect => "The SQL expression contains an unsupported use of SELECT.".to_string(),
            S3ErrorCode::ParseUnsupportedCase => "The SQL expression contains an unsupported use of CASE.".to_string(),
            S3ErrorCode::ParseUnsupportedCaseClause => "The SQL expression contains an unsupported use of CASE.".to_string(),
            S3ErrorCode::ParseUnsupportedAlias => "The SQL expression contains an unsupported use of ALIAS.".to_string(),
            S3ErrorCode::ParseUnsupportedSyntax => "The SQL expression contains unsupported syntax.".to_string(),
            S3ErrorCode::ParseUnknownOperator => "The SQL expression contains an invalid operator.".to_string(),
            S3ErrorCode::ParseMissingIdentAfterAt => "Did not find the expected identifier after the @ symbol in the SQL expression.".to_string(),
            S3ErrorCode::ParseUnexpectedOperator => "The SQL expression contains an unexpected operator.".to_string(),
            S3ErrorCode::ParseUnexpectedTerm => "The SQL expression contains an unexpected term.".to_string(),
            S3ErrorCode::ParseUnexpectedToken => "The SQL expression contains an unexpected token.".to_string(),
            S3ErrorCode::ParseExpectedExpression => "Did not find the expected SQL expression.".to_string(),
            S3ErrorCode::ParseExpectedLeftParenAfterCast => "Did not find expected the left parenthesis in the SQL expression.".to_string(),
            S3ErrorCode::ParseExpectedLeftParenValueConstructor => "Did not find expected the left parenthesis in the SQL expression.".to_string(),
            S3ErrorCode::ParseExpectedLeftParenBuiltinFunctionCall => "Did not find the expected left parenthesis in the SQL expression.".to_string(),
            S3ErrorCode::ParseExpectedArgumentDelimiter => "Did not find the expected argument delimiter in the SQL expression.".to_string(),
            S3ErrorCode::ParseCastArity => "The SQL expression CAST has incorrect arity.".to_string(),
            S3ErrorCode::ParseInvalidTypeParam => "The SQL expression contains an invalid parameter value.".to_string(),
            S3ErrorCode::ParseEmptySelect => "The SQL expression contains an empty SELECT.".to_string(),
            S3ErrorCode::ParseSelectMissingFrom => "GROUP is not supported in the SQL expression.".to_string(),
            S3ErrorCode::ParseExpectedIdentForGroupName => "GROUP is not supported in the SQL expression.".to_string(),
            S3ErrorCode::ParseExpectedIdentForAlias => "Did not find the expected identifier for the alias in the SQL expression.".to_string(),
            S3ErrorCode::ParseUnsupportedCallWithStar => "Only COUNT with (*) as a parameter is supported in the SQL expression.".to_string(),
            S3ErrorCode::ParseNonUnaryAgregateFunctionCall => "Only one argument is supported for aggregate functions in the SQL expression.".to_string(),
            S3ErrorCode::ParseMalformedJoin => "JOIN is not supported in the SQL expression.".to_string(),
            S3ErrorCode::ParseExpectedIdentForAt => "Did not find the expected identifier for AT name in the SQL expression.".to_string(),
            S3ErrorCode::ParseAsteriskIsNotAloneInSelectList => "Other expressions are not allowed in the SELECT list when '*' is used without dot notation in the SQL expression.".to_string(),
            S3ErrorCode::ParseCannotMixSqbAndWildcardInSelectList => "Cannot mix [] and * in the same expression in a SELECT list in SQL expression.".to_string(),
            S3ErrorCode::ParseInvalidContextForWildcardInSelectList => "Invalid use of * in SELECT list in the SQL expression.".to_string(),
            S3ErrorCode::IncorrectSqlFunctionArgumentType => "Incorrect type of arguments in function call in the SQL expression.".to_string(),
            S3ErrorCode::ValueParseFailure => "Time stamp parse failure in the SQL expression.".to_string(),
            S3ErrorCode::EvaluatorInvalidArguments => "Incorrect number of arguments in the function call in the SQL expression.".to_string(),
            S3ErrorCode::IntegerOverflow => "Int overflow or underflow in the SQL expression.".to_string(),
            S3ErrorCode::LikeInvalidInputs => "Invalid argument given to the LIKE clause in the SQL expression.".to_string(),
            S3ErrorCode::CastFailed => "Attempt to convert from one data type to another using CAST failed in the SQL expression.".to_string(),
            S3ErrorCode::InvalidCast => "Attempt to convert from one data type to another using CAST failed in the SQL expression.".to_string(),
            S3ErrorCode::EvaluatorInvalidTimestampFormatPattern => "Time stamp format pattern requires additional fields in the SQL expression.".to_string(),
            S3ErrorCode::EvaluatorInvalidTimestampFormatPatternSymbolForParsing => "Time stamp format pattern contains a valid format symbol that cannot be applied to time stamp parsing in the SQL expression.".to_string(),
            S3ErrorCode::EvaluatorTimestampFormatPatternDuplicateFields => "Time stamp format pattern contains multiple format specifiers representing the time stamp field in the SQL expression.".to_string(),
            S3ErrorCode::EvaluatorTimestampFormatPatternHourClockAmPmMismatch => "Time stamp format pattern contains unterminated token in the SQL expression.".to_string(),
            S3ErrorCode::EvaluatorUnterminatedTimestampFormatPatternToken => "Time stamp format pattern contains an invalid token in the SQL expression.".to_string(),
            S3ErrorCode::EvaluatorInvalidTimestampFormatPatternToken => "Time stamp format pattern contains an invalid token in the SQL expression.".to_string(),
            S3ErrorCode::EvaluatorInvalidTimestampFormatPatternSymbol => "Time stamp format pattern contains an invalid symbol in the SQL expression.".to_string(),
            S3ErrorCode::EvaluatorBindingDoesNotExist => "A column name or a path provided does not exist in the SQL expression".to_string(),
            S3ErrorCode::InvalidColumnIndex => "The column index is invalid. Please check the service documentation and try again.".to_string(),
            S3ErrorCode::UnsupportedFunction => "Encountered an unsupported SQL function.".to_string(),
            S3ErrorCode::Custom(code) if &**code == MAX_VERSIONS_EXCEEDED_CODE => {
                MAX_VERSIONS_EXCEEDED_MESSAGE.to_string()
            }
            S3ErrorCode::Custom(code) if &**code == SLOW_DOWN_READ_CODE => SLOW_DOWN_READ_MESSAGE.to_string(),
            _ => code.as_str().to_string(),
        }
    }
}

/// Classify body failures in one bounded pass, retaining precedence across wrappers.
/// A digest failure wins immediately; other markers must wait for deeper causes.
fn body_read_error_code(err: &(dyn std::error::Error + 'static)) -> Option<S3ErrorCode> {
    let mut upload_limit_exceeded = false;
    let mut server_source_failed = false;
    let mut client_body_timed_out = false;
    let mut body_size_exceeded = false;
    let mut aws_chunked_error = None;
    let mut incomplete_body = false;
    let digest = error_chain_find(err, |error| {
        let upload_error = error.downcast_ref::<UploadStreamError>();
        if matches!(upload_error, Some(UploadStreamError::Sha256Mismatch)) || error.is::<rustfs_rio::Sha256Mismatch>() {
            return Some(S3ErrorCode::Custom(CONTENT_SHA256_MISMATCH_CODE.into()));
        }
        if error.is::<rustfs_rio::ChecksumMismatch>() || error.is::<rustfs_rio::BadDigest>() {
            return Some(S3ErrorCode::BadDigest);
        }

        upload_limit_exceeded |= error.is::<UploadLimitExceeded>();
        server_source_failed |= error.is::<ServerSideSourceReadError>();
        client_body_timed_out |= error.is::<ClientBodyReadTimeout>();
        body_size_exceeded |= error.is::<s3s::BodySizeLimitExceeded>();
        if let Some(chunked) = error.downcast_ref::<AwsChunkedStreamError>()
            && !matches!(chunked, AwsChunkedStreamError::Underlying(_))
        {
            aws_chunked_error = Some(chunked.to_s3_error_code());
        }
        incomplete_body |= error.is::<rustfs_rio::IncompleteBody>()
            || matches!(upload_error, Some(UploadStreamError::LengthMismatch | UploadStreamError::Incomplete))
            || is_hyper_body_eof(error);
        None
    });
    digest.or_else(|| {
        if upload_limit_exceeded {
            Some(S3ErrorCode::EntityTooLarge)
        } else if server_source_failed {
            Some(S3ErrorCode::ServiceUnavailable)
        } else if client_body_timed_out {
            Some(S3ErrorCode::RequestTimeout)
        } else if body_size_exceeded {
            Some(S3ErrorCode::EntityTooLarge)
        } else if let Some(code) = aws_chunked_error {
            Some(code)
        } else if incomplete_body {
            Some(S3ErrorCode::IncompleteBody)
        } else {
            None
        }
    })
}

/// `io::Error::source` skips its custom payload itself. Visit that payload
/// explicitly at every level, then follow its source exactly once. The bound
/// also makes cyclic or excessively deep foreign error chains safe.
fn error_chain_find<T>(
    err: &(dyn std::error::Error + 'static),
    mut classify: impl FnMut(&(dyn std::error::Error + 'static)) -> Option<T>,
) -> Option<T> {
    let mut current = Some(err);
    for _ in 0..64 {
        let error = current?;
        if let Some(classified) = classify(error) {
            return Some(classified);
        }
        current = error
            .downcast_ref::<std::io::Error>()
            .and_then(std::io::Error::get_ref)
            .map(|inner| inner as &(dyn std::error::Error + 'static))
            .or_else(|| error.source());
    }
    None
}

/// hyper reports a request body whose connection hit EOF before
/// `Content-Length` bytes arrived as a `Kind::Body` error carrying an
/// `UnexpectedEof` `io::Error` (its `IncompleteBody` marker is private).
/// That is a client-side short body, not a server fault.
fn is_hyper_body_eof(err: &(dyn std::error::Error + 'static)) -> bool {
    err.downcast_ref::<hyper::Error>()
        .and_then(|hyper_err| std::error::Error::source(hyper_err))
        .and_then(|cause| cause.downcast_ref::<std::io::Error>())
        .is_some_and(|io_err| io_err.kind() == std::io::ErrorKind::UnexpectedEof)
}

/// A nested lock timeout retains its retry advice even when an outer lock
/// failure supplies a less specific cause. Share one bounded source walk.
fn service_unavailable_cause(source: &(dyn std::error::Error + 'static)) -> Option<&'static str> {
    let mut cause = None;
    let timeout = error_chain_find(source, |error| match error.downcast_ref::<LockError>() {
        Some(LockError::Timeout { .. }) => Some(NAMESPACE_LOCK_TIMEOUT_CAUSE),
        Some(_) => {
            cause.get_or_insert("namespace_lock_unavailable");
            None
        }
        None => {
            if cause.is_none() {
                cause = match error.downcast_ref::<StorageError>() {
                    Some(StorageError::NamespaceLockQuorumUnavailable { .. }) => Some("namespace_lock_unavailable"),
                    Some(StorageError::ErasureWriteQuorum | StorageError::InsufficientWriteQuorum(_, _)) => {
                        Some("storage_write_quorum")
                    }
                    _ => None,
                };
            }
            None
        }
    });
    timeout.or(cause)
}

#[derive(Clone, Copy)]
pub(crate) enum MultipartCreateBoundary {
    Access,
    Execute,
}

struct MultipartCreateTimeout {
    resource_class: &'static str,
    source_kind: &'static str,
    timeout_ms: u64,
}

fn multipart_timeout_resource_class(resource: &str, bucket: &str, key: &str) -> &'static str {
    if bucket.is_empty() {
        return "unknown";
    }

    let request_resource = resource
        .strip_suffix("@latest")
        .and_then(|path| path.strip_prefix(bucket))
        .and_then(|path| path.strip_prefix('/'));
    let lifecycle = request_resource == Some("");
    let transaction = resource
        .strip_prefix(".rustfs.sys/bucket-targets/")
        .and_then(|path| path.strip_suffix("/transaction.lock@latest"))
        == Some(bucket);
    let object = if key.is_empty() {
        false
    } else {
        let encoded = rustfs_utils::path::encode_dir_object_ref(key);
        let identity = rustfs_utils::path::object_name_disk_identity(encoded.as_ref());
        request_resource == Some(identity.as_ref())
    };

    // A formatted key may be ambiguous; do not choose a class in that case.
    match (lifecycle, transaction, object) {
        (true, false, false) => "bucket_lifecycle",
        (false, true, false) => "bucket_transaction",
        (false, false, true) => "request_object",
        _ => "unknown",
    }
}

fn multipart_create_timeout_diagnostic(error: &S3Error, bucket: &str, key: &str) -> Option<MultipartCreateTimeout> {
    if error.code() != &S3ErrorCode::ServiceUnavailable || error.status_code() != Some(StatusCode::SERVICE_UNAVAILABLE) {
        return None;
    }
    let source = error.source()?;
    let direct = matches!(source.downcast_ref::<StorageError>(), Some(StorageError::Lock(LockError::Timeout { .. })))
        || matches!(source.downcast_ref::<LockError>(), Some(LockError::Timeout { .. }));
    error_chain_find(source, |source| match source.downcast_ref::<LockError>() {
        Some(LockError::Timeout { resource, timeout }) => Some(MultipartCreateTimeout {
            resource_class: multipart_timeout_resource_class(resource, bucket, key),
            source_kind: if direct { "direct" } else { "nested" },
            timeout_ms: u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX),
        }),
        _ => None,
    })
}

pub(crate) fn log_multipart_create_timeout(
    error: &S3Error,
    boundary: MultipartCreateBoundary,
    request_id: Option<&str>,
    bucket: &str,
    key: &str,
) {
    let Some(diagnostic) = multipart_create_timeout_diagnostic(error, bucket, key) else {
        return;
    };
    let request_id = request_id
        .filter(|value| uuid::Uuid::parse_str(value).is_ok())
        .unwrap_or("unknown");
    let boundary = match boundary {
        MultipartCreateBoundary::Access => "access",
        MultipartCreateBoundary::Execute => "execute",
    };
    tracing::error!(
        target: rustfs_obs::MULTIPART_CREATE_TIMEOUT_LOG_TARGET,
        parent: None,
        event = "multipart_create_lock_timeout",
        component = "storage",
        subsystem = "object_lock",
        state = "failed",
        boundary,
        request_id,
        resource_class = diagnostic.resource_class,
        timeout_source = diagnostic.source_kind,
        timeout_budget_ms = diagnostic.timeout_ms,
        "Multipart creation returned a lock timeout source"
    );
}

impl From<ApiError> for S3Error {
    fn from(err: ApiError) -> Self {
        let status = custom_error_status(&err.code);
        let cause = if err.code == S3ErrorCode::ServiceUnavailable {
            err.source.as_deref().and_then(|source| service_unavailable_cause(source))
        } else {
            None
        };
        let mut s3e = S3Error::with_message(err.code, err.message);
        if let Some(cause) = cause {
            let mut headers = http::HeaderMap::new();
            // s3s replaces the generated headers when a custom map is supplied.
            headers.insert(http::header::CONTENT_TYPE, http::HeaderValue::from_static("application/xml"));
            headers.insert("x-rustfs-error-cause", http::HeaderValue::from_static(cause));
            if cause == NAMESPACE_LOCK_TIMEOUT_CAUSE {
                headers.insert(http::header::RETRY_AFTER, http::HeaderValue::from_static("1"));
            }
            s3e.set_headers(headers);
        }
        if let Some(status) = status {
            s3e.set_status_code(status);
        }
        if let Some(source) = err.source {
            s3e.set_source(source);
        }
        s3e
    }
}

impl From<crate::table_catalog::TableCatalogStoreError> for ApiError {
    fn from(err: crate::table_catalog::TableCatalogStoreError) -> Self {
        if let crate::table_catalog::TableCatalogStoreError::LockUnavailable { source, .. } = err {
            // Use the storage boundary once so typed lock failures share its
            // bounded metrics, public message, and retry advice.
            return Self::from(StorageError::Lock(*source));
        }
        let code = if err.is_unavailable() {
            S3ErrorCode::ServiceUnavailable
        } else {
            S3ErrorCode::InternalError
        };
        Self {
            message: Self::error_code_to_message(&code),
            code,
            source: Some(Box::new(err)),
        }
    }
}

impl From<StorageError> for ApiError {
    fn from(err: StorageError) -> Self {
        record_s3_lock_failure(&err);
        if err.pool_metadata_failure().is_some() {
            return ApiError {
                code: S3ErrorCode::ServiceUnavailable,
                message: ApiError::error_code_to_message(&S3ErrorCode::ServiceUnavailable),
                source: Some(Box::new(err)),
            };
        }
        // A stored bucket config that cannot be parsed stays refused until an
        // operator repairs it, so it is a recoverable 503 that names what to
        // repair rather than a generic 500 (rustfs/backlog#1734).
        if let Some(message) = unreadable_config_refusal(&err).map(ToString::to_string) {
            return ApiError {
                code: S3ErrorCode::ServiceUnavailable,
                message,
                source: Some(Box::new(err)),
            };
        }
        if let StorageError::Io(ref io_err) = err
            && let Some(inner) = io_err.get_ref()
        {
            if let Some(code) = body_read_error_code(inner) {
                return ApiError {
                    message: ApiError::error_code_to_message(&code),
                    code,
                    source: Some(Box::new(err)),
                };
            }

            if inner.downcast_ref::<KmsUnavailableError>().is_some() {
                return ApiError {
                    code: S3ErrorCode::ServiceUnavailable,
                    message: ApiError::error_code_to_message(&S3ErrorCode::ServiceUnavailable),
                    source: Some(Box::new(err)),
                };
            }

            if let Some(kms_error) = inner.downcast_ref::<rustfs_kms::KmsError>()
                && let Some((code, message)) = data_plane_kms_error(kms_error)
            {
                return ApiError {
                    code,
                    message,
                    source: Some(Box::new(err)),
                };
            }
        }

        let code = match &err {
            StorageError::NotImplemented => S3ErrorCode::NotImplemented,
            StorageError::InvalidArgument(_, _, _) => S3ErrorCode::InvalidArgument,
            StorageError::MethodNotAllowed => S3ErrorCode::MethodNotAllowed,
            StorageError::BucketNotFound(_) => S3ErrorCode::NoSuchBucket,
            StorageError::BucketNotEmpty(_) | StorageError::BucketNotEmptyWithDetails { .. } => S3ErrorCode::BucketNotEmpty,
            StorageError::BucketNameInvalid(_) => S3ErrorCode::InvalidBucketName,
            StorageError::ObjectNameInvalid(_, _) => S3ErrorCode::InvalidArgument,
            StorageError::BucketExists(_) => S3ErrorCode::BucketAlreadyOwnedByYou,
            StorageError::DiskFull | StorageError::StorageFull => S3ErrorCode::ServiceUnavailable,
            // Retryable on an upgraded node (or once the cluster upgrade
            // completes); see UNSUPPORTED_PARTICIPANT_MESSAGE.
            StorageError::UnsupportedDisk => S3ErrorCode::ServiceUnavailable,
            StorageError::SlowDown => S3ErrorCode::SlowDown,
            StorageError::FaultyDisk
            | StorageError::FaultyRemoteDisk
            | StorageError::DiskNotFound
            | StorageError::Timeout
            | StorageError::TooManyOpenFiles => S3ErrorCode::ServiceUnavailable,
            StorageError::ErasureReadQuorum | StorageError::InsufficientReadQuorum(_, _) => {
                S3ErrorCode::Custom(SLOW_DOWN_READ_CODE.into())
            }
            StorageError::ErasureWriteQuorum | StorageError::InsufficientWriteQuorum(_, _) => S3ErrorCode::ServiceUnavailable,
            StorageError::NamespaceLockQuorumUnavailable { .. } => S3ErrorCode::ServiceUnavailable,
            StorageError::QuotaExceeded { .. } => S3ErrorCode::InvalidRequest,
            StorageError::MaxVersionsExceeded => S3ErrorCode::Custom(MAX_VERSIONS_EXCEEDED_CODE.into()),
            StorageError::Lock(_) => S3ErrorCode::ServiceUnavailable,
            StorageError::DecommissionNotStarted => S3ErrorCode::InvalidRequest,
            StorageError::DecommissionAlreadyRunning => S3ErrorCode::InvalidRequest,
            StorageError::RebalanceAlreadyRunning => S3ErrorCode::InvalidRequest,
            StorageError::PrefixAccessDenied(_, _) => S3ErrorCode::AccessDenied,
            StorageError::InvalidUploadIDKeyCombination(_, _) => S3ErrorCode::InvalidArgument,
            StorageError::MalformedUploadID(_) => S3ErrorCode::InvalidArgument,
            StorageError::ObjectNameTooLong(_, _) => S3ErrorCode::InvalidArgument,
            StorageError::ObjectNamePrefixAsSlash(_, _) => S3ErrorCode::InvalidArgument,
            StorageError::ObjectNotFound(_, _) => S3ErrorCode::NoSuchKey,
            StorageError::ConfigNotFound => S3ErrorCode::NoSuchKey,
            StorageError::VolumeNotFound => S3ErrorCode::NoSuchBucket,
            StorageError::FileNotFound => S3ErrorCode::NoSuchKey,
            StorageError::FileVersionNotFound => S3ErrorCode::NoSuchVersion,
            StorageError::VersionNotFound(_, _, _) => S3ErrorCode::NoSuchVersion,
            StorageError::InvalidUploadID(_, _, _) => S3ErrorCode::NoSuchUpload,
            StorageError::InvalidVersionID(_, _, _) => S3ErrorCode::InvalidArgument,
            StorageError::DataMovementOverwriteErr(_, _, _) => S3ErrorCode::InvalidArgument,
            StorageError::ObjectExistsAsDirectory(_, _) => S3ErrorCode::InvalidArgument,
            StorageError::InvalidPart(_, _, _) => S3ErrorCode::InvalidPart,
            StorageError::EntityTooSmall(_, _, _) => S3ErrorCode::EntityTooSmall,
            StorageError::EntityTooLarge(_, _) => S3ErrorCode::EntityTooLarge,
            StorageError::PreconditionFailed => S3ErrorCode::PreconditionFailed,
            StorageError::ConditionalDeleteConflict => S3ErrorCode::OperationAborted,
            StorageError::GuardedDeleteUnsupported => S3ErrorCode::NotImplemented,
            StorageError::NotModified => S3ErrorCode::NotModified,
            StorageError::InvalidRangeSpec(_) => S3ErrorCode::InvalidRange,
            _ => S3ErrorCode::InternalError,
        };

        let message = if matches!(&err, StorageError::QuotaExceeded { .. }) {
            err.to_string()
        } else if matches!(&err, StorageError::UnsupportedDisk) {
            UNSUPPORTED_PARTICIPANT_MESSAGE.to_string()
        } else if matches!(&err, StorageError::MaxVersionsExceeded)
            || (code == S3ErrorCode::InternalError && matches!(&err, StorageError::Io(_)))
        {
            ApiError::error_code_to_message(&code)
        } else if code == S3ErrorCode::InternalError {
            err.to_string()
        } else if let StorageError::InvalidArgument(_, _, reason) = &err
            && !reason.is_empty()
        {
            // Argument-validation rejections carry a client-actionable reason
            // (e.g. object keys unsupported on Windows hosts, issue #3299);
            // a bare "Invalid argument" gives the caller nothing to fix.
            format!("Invalid argument: {reason}")
        } else {
            ApiError::error_code_to_message(&code)
        };
        ApiError {
            code,
            message,
            source: Some(Box::new(err)),
        }
    }
}

impl From<HTTPRangeError> for ApiError {
    fn from(err: HTTPRangeError) -> Self {
        ApiError {
            code: S3ErrorCode::InvalidRange,
            message: err.to_string(),
            source: Some(Box::new(err)),
        }
    }
}

impl From<std::io::Error> for ApiError {
    fn from(err: std::io::Error) -> Self {
        if let Some(inner) = err.get_ref()
            && let Some(code) = body_read_error_code(inner)
        {
            return ApiError {
                message: ApiError::error_code_to_message(&code),
                code,
                source: Some(Box::new(err)),
            };
        }
        ApiError {
            code: S3ErrorCode::InternalError,
            message: err.to_string(),
            source: Some(Box::new(err)),
        }
    }
}

impl From<rustfs_iam::error::Error> for ApiError {
    fn from(err: rustfs_iam::error::Error) -> Self {
        let serr: StorageError = err.into();
        serr.into()
    }
}

impl From<QuotaError> for ApiError {
    fn from(err: QuotaError) -> Self {
        let code = match &err {
            QuotaError::QuotaExceeded { .. } => S3ErrorCode::InvalidRequest,
            QuotaError::ConfigNotFound { .. } => S3ErrorCode::NoSuchBucket,
            QuotaError::UsageUnavailable { .. } => S3ErrorCode::ServiceUnavailable,
            QuotaError::InvalidConfig { .. } => S3ErrorCode::InvalidArgument,
            QuotaError::StorageError(_) => S3ErrorCode::InternalError,
        };

        let message = if code == S3ErrorCode::InternalError {
            err.to_string()
        } else {
            ApiError::error_code_to_message(&code)
        };

        ApiError {
            code,
            message,
            source: Some(Box::new(err)),
        }
    }
}

#[cfg(test)]
pub(crate) mod multipart_timeout_test_log {
    use std::{
        io,
        io::Write,
        sync::{Arc, Mutex},
    };
    use tracing_subscriber::fmt::MakeWriter;

    #[derive(Clone, Default)]
    pub(crate) struct Capture(Arc<Mutex<Vec<u8>>>);

    pub(crate) struct Writer(Arc<Mutex<Vec<u8>>>);

    impl Write for Writer {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().expect("log capture").extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for Capture {
        type Writer = Writer;

        fn make_writer(&'a self) -> Self::Writer {
            Writer(self.0.clone())
        }
    }

    impl Capture {
        pub(crate) fn rows(&self) -> Vec<serde_json::Value> {
            let data = self.0.lock().expect("log capture").clone();
            String::from_utf8(data)
                .expect("UTF-8 logs")
                .lines()
                .map(|line| serde_json::from_str(line).expect("JSON log"))
                .collect()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics::with_local_recorder;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use rustfs_io_metrics::s3_http_metrics::S3HttpRequestGuard;
    use rustfs_s3_ops::S3Operation;
    use s3s::stream::{aws_chunked_stream::AwsChunkedStreamError, upload_stream::UploadStreamError};
    use s3s::{S3Error, S3ErrorCode};
    use std::io::{Error as IoError, ErrorKind};

    fn multipart_timeout_error(resource: &str) -> S3Error {
        ApiError::from(StorageError::Lock(LockError::timeout(resource, std::time::Duration::from_secs(5)))).into()
    }

    #[test]
    fn multipart_create_timeout_classifies_only_the_exact_known_resources() {
        for (resource, key, expected) in [
            ("bucket/@latest", "private-key", "bucket_lifecycle"),
            ("bucket/private-key@latest", "private-key", "request_object"),
            ("bucket/dir__XLDIR__@latest", "dir/", "request_object"),
            (
                ".rustfs.sys/bucket-targets/bucket/transaction.lock@latest",
                "private-key",
                "bucket_transaction",
            ),
            (".rustfs.sys/bucket-targets/other/transaction.lock@latest", "private-key", "unknown"),
            ("bucket/private-key@older", "private-key", "unknown"),
            ("bucket/other@latest", "private-key", "unknown"),
        ] {
            let error = multipart_timeout_error(resource);
            let diagnostic = multipart_create_timeout_diagnostic(&error, "bucket", key).expect("typed timeout");
            assert_eq!(diagnostic.resource_class, expected, "{resource}");
            assert_eq!(diagnostic.source_kind, "direct");
            assert_eq!(diagnostic.timeout_ms, 5_000);
        }

        let key = "a\\b";
        let encoded = rustfs_utils::path::object_name_disk_identity(key);
        let error = multipart_timeout_error(&format!("bucket/{encoded}@latest"));
        assert_eq!(
            multipart_create_timeout_diagnostic(&error, "bucket", key)
                .unwrap()
                .resource_class,
            "request_object"
        );

        let error = multipart_timeout_error(".rustfs.sys/bucket-targets/bucket/transaction.lock@latest");
        assert_eq!(
            multipart_create_timeout_diagnostic(&error, ".rustfs.sys", "bucket-targets/bucket/transaction.lock")
                .unwrap()
                .resource_class,
            "request_object"
        );
        let ambiguous = multipart_timeout_error(".rustfs.sys/bucket-targets/.rustfs.sys/transaction.lock@latest");
        assert_eq!(
            multipart_create_timeout_diagnostic(&ambiguous, ".rustfs.sys", "bucket-targets/.rustfs.sys/transaction.lock")
                .unwrap()
                .resource_class,
            "unknown"
        );
    }

    #[test]
    fn multipart_create_timeout_keeps_nested_and_untrusted_sources_distinct() {
        let nested = S3Error::from(ApiError {
            code: S3ErrorCode::ServiceUnavailable,
            message: "unavailable".into(),
            source: Some(Box::new(IoError::other(LockError::Network {
                message: "LEAK-network-detail".into(),
                source: Box::new(LockError::timeout("bucket/private-key@latest", std::time::Duration::from_secs(5))),
            }))),
        });
        let diagnostic = multipart_create_timeout_diagnostic(&nested, "bucket", "private-key").expect("nested timeout");
        assert_eq!(diagnostic.resource_class, "request_object");
        assert_eq!(diagnostic.source_kind, "nested");

        let forged = S3Error::from(ApiError {
            code: S3ErrorCode::ServiceUnavailable,
            message: "unavailable".into(),
            source: Some(Box::new(IoError::other(
                "Lock acquisition timeout for resource 'bucket/private-key@latest'",
            ))),
        });
        assert!(multipart_create_timeout_diagnostic(&forged, "bucket", "private-key").is_none());
        let mut denied = multipart_timeout_error("bucket/private-key@latest");
        denied.set_code(S3ErrorCode::AccessDenied);
        assert!(multipart_create_timeout_diagnostic(&denied, "bucket", "private-key").is_none());
    }

    #[test]
    fn multipart_create_timeout_event_is_visible_at_warn_and_does_not_expose_source() {
        use crate::error::multipart_timeout_test_log::Capture;

        let capture = Capture::default();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .flatten_event(true)
            .with_current_span(false)
            .with_span_list(false)
            .with_max_level(tracing::Level::WARN)
            .with_writer(capture.clone())
            .finish();
        let error = multipart_timeout_error("private-bucket/LEAK-private-key@latest");
        let request_id = "b8172a15-a428-4b42-9cef-1f4a7b96874e";
        tracing::subscriber::with_default(subscriber, || {
            log_multipart_create_timeout(
                &error,
                MultipartCreateBoundary::Execute,
                Some(request_id),
                "private-bucket",
                "LEAK-private-key",
            );
            log_multipart_create_timeout(
                &error,
                MultipartCreateBoundary::Access,
                Some("LEAK-invalid-request-id"),
                "private-bucket",
                "LEAK-private-key",
            );
        });
        let rows = capture.rows();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["event"], "multipart_create_lock_timeout");
        assert_eq!(rows[0]["boundary"], "execute");
        assert_eq!(rows[0]["request_id"], request_id);
        assert_eq!(rows[0]["resource_class"], "request_object");
        assert_eq!(rows[0]["timeout_budget_ms"], 5_000);
        let output = rows[0].to_string();
        assert!(!output.contains("private-bucket"));
        assert!(!output.contains("LEAK-private-key"));
        assert!(!output.contains("LEAK-parent-span"));
        assert_eq!(rows[1]["request_id"], "unknown");
        assert!(!rows[1].to_string().contains("LEAK-invalid-request-id"));
        assert_eq!(error.headers().unwrap()["x-rustfs-error-cause"], "namespace_lock_timeout");
        assert_eq!(error.headers().unwrap()[http::header::RETRY_AFTER], "1");
    }

    fn error_chain_has_type<T>(err: &(dyn std::error::Error + 'static)) -> bool
    where
        T: std::error::Error + 'static,
    {
        error_chain_find(err, |error| error.is::<T>().then_some(())).is_some()
    }

    #[tokio::test]
    async fn lock_timeout_response_advises_retry_without_exposing_resource() {
        use http_body_util::BodyExt as _;

        let storage_error =
            StorageError::Lock(LockError::timeout("private-bucket/private-key@latest", std::time::Duration::from_secs(5)));
        let error = S3Error::from(ApiError::from(storage_error));
        assert_eq!(error.code(), &S3ErrorCode::ServiceUnavailable);
        let source = error.source().expect("retain the typed storage cause");
        assert!(source.downcast_ref::<StorageError>().is_some());
        assert!(error_chain_has_type::<LockError>(source));
        let response = error.to_http_response().expect("serialize lock timeout response");
        assert_eq!(response.status(), http::StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()[http::header::CONTENT_TYPE], "application/xml");
        assert_eq!(response.headers()[http::header::RETRY_AFTER], "1");
        assert_eq!(response.headers()["x-rustfs-error-cause"], "namespace_lock_timeout");
        let body = response
            .into_body()
            .collect()
            .await
            .expect("read timeout error XML")
            .to_bytes();
        let body = String::from_utf8(body.to_vec()).expect("S3 XML is UTF-8");
        assert!(body.contains("<Code>ServiceUnavailable</Code>"));
        assert!(!body.contains("private-bucket"));
        assert!(!body.contains("private-key"));
    }

    #[tokio::test]
    async fn unsupported_participant_is_retryable_with_an_actionable_message() {
        use http_body_util::BodyExt as _;

        let error = S3Error::from(ApiError::from(StorageError::UnsupportedDisk));
        assert_eq!(error.code(), &S3ErrorCode::ServiceUnavailable);
        assert_eq!(error.message(), Some(UNSUPPORTED_PARTICIPANT_MESSAGE));
        assert!(matches!(
            error.source().and_then(|source| source.downcast_ref::<StorageError>()),
            Some(StorageError::UnsupportedDisk)
        ));

        let response = error
            .to_http_response()
            .expect("unsupported participant error must serialize");
        assert_eq!(response.status(), http::StatusCode::SERVICE_UNAVAILABLE);
        assert!(
            !response.headers().contains_key(http::header::RETRY_AFTER),
            "retrying the same node does not help until it or its peers are upgraded"
        );
        let body = response
            .into_body()
            .collect()
            .await
            .expect("error XML must be readable")
            .to_bytes();
        let xml = std::str::from_utf8(&body).expect("error XML must be UTF-8");
        assert!(xml.contains("<Code>ServiceUnavailable</Code>"));
        assert!(
            !xml.contains("O_DIRECT"),
            "the drive-probe Display text must not leak as the public message"
        );
    }

    #[tokio::test]
    async fn capacity_failures_share_service_unavailable_without_lock_retry_advice() {
        use crate::storage_api::error::DiskError;
        use http_body_util::BodyExt as _;

        for failure in [
            StorageError::DiskFull,
            StorageError::StorageFull,
            StorageError::from(DiskError::DiskFull),
        ] {
            let expected_code = failure.code();
            let error = S3Error::from(ApiError::from(failure));
            assert_eq!(error.code(), &S3ErrorCode::ServiceUnavailable);
            assert_eq!(error.message(), Some("The service is unavailable. Please retry."));
            let source = error
                .source()
                .and_then(|source| source.downcast_ref::<StorageError>())
                .expect("retain the original capacity failure");
            assert_eq!(source.code(), expected_code);
            let response = error.to_http_response().expect("serialize capacity refusal");
            assert_eq!(response.status(), http::StatusCode::SERVICE_UNAVAILABLE);
            assert!(!response.headers().contains_key(http::header::RETRY_AFTER));
            let body = response.into_body().collect().await.expect("read S3 error XML").to_bytes();
            let xml = std::str::from_utf8(&body).expect("S3 XML is UTF-8");
            assert!(xml.contains("<Code>ServiceUnavailable</Code>"));
            assert!(!xml.contains("InternalError"));
        }
    }

    #[test]
    fn lock_timeout_response_preserves_wrapped_source_and_requires_503() {
        for code in [S3ErrorCode::ServiceUnavailable, S3ErrorCode::RequestTimeout] {
            let should_advise_retry = code == S3ErrorCode::ServiceUnavailable;
            for source in [
                IoError::other(LockError::timeout("bucket/object@latest", std::time::Duration::from_secs(5))),
                IoError::other(LockError::Network {
                    message: "private-peer".to_owned(),
                    source: Box::new(LockError::timeout("bucket/object@latest", std::time::Duration::from_secs(5))),
                }),
            ] {
                let error = S3Error::from(ApiError {
                    message: ApiError::error_code_to_message(&code),
                    code: code.clone(),
                    source: Some(Box::new(source)),
                });
                assert_eq!(error.code(), &code);
                let source = error.source().expect("preserve wrapped lock timeout");
                assert!(error_chain_has_type::<LockError>(source));
                let response = error.to_http_response().expect("serialize wrapped timeout");
                assert_eq!(response.headers()[http::header::CONTENT_TYPE], "application/xml");
                assert_eq!(response.headers().contains_key(http::header::RETRY_AFTER), should_advise_retry);
                assert_eq!(response.headers().contains_key("x-rustfs-error-cause"), should_advise_retry);
                if should_advise_retry {
                    assert_eq!(response.headers()["x-rustfs-error-cause"], "namespace_lock_timeout");
                }
            }
        }
    }

    #[tokio::test]
    async fn unavailable_response_attributes_typed_causes_without_exposing_resources() {
        use http_body_util::BodyExt as _;

        for (failure, cause) in [
            (
                StorageError::NamespaceLockQuorumUnavailable {
                    mode: "quota_capability",
                    bucket: "private-bucket".to_owned(),
                    object: "private-object".to_owned(),
                    required: 3,
                    achieved: 2,
                },
                "namespace_lock_unavailable",
            ),
            (
                StorageError::Lock(LockError::QuorumNotReached {
                    required: 3,
                    achieved: 2,
                }),
                "namespace_lock_unavailable",
            ),
            (StorageError::ErasureWriteQuorum, "storage_write_quorum"),
            (
                StorageError::InsufficientWriteQuorum("private-bucket".to_owned(), "private-object".to_owned()),
                "storage_write_quorum",
            ),
        ] {
            let expected_code = failure.code();
            let error = S3Error::from(ApiError::from(failure));
            assert_eq!(error.code(), &S3ErrorCode::ServiceUnavailable);
            assert_eq!(error.message(), Some("The service is unavailable. Please retry."));
            let source = error
                .source()
                .and_then(|source| source.downcast_ref::<StorageError>())
                .expect("retain the original storage failure");
            assert_eq!(source.code(), expected_code);
            let response = error.to_http_response().expect("serialize typed unavailable response");
            assert_eq!(response.status(), http::StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(response.headers()[http::header::CONTENT_TYPE], "application/xml");
            assert_eq!(response.headers()["x-rustfs-error-cause"], cause);
            assert!(!response.headers().contains_key(http::header::RETRY_AFTER));
            let body = response.into_body().collect().await.expect("read S3 error XML").to_bytes();
            let xml = std::str::from_utf8(&body).expect("S3 XML is UTF-8");
            assert!(xml.contains("<Code>ServiceUnavailable</Code>"));
            assert!(!xml.contains("private-bucket"));
            assert!(!xml.contains("private-object"));
        }
    }

    #[test]
    fn unavailable_response_does_not_infer_a_cause_from_timeout_text() {
        for failure in [
            StorageError::Timeout,
            StorageError::DiskFull,
            StorageError::DiskNotFound,
            StorageError::Io(IoError::other("Lock acquisition timeout for private-bucket/private-object after 5s")),
            StorageError::Io(IoError::new(std::io::ErrorKind::TimedOut, "body stalled")),
        ] {
            let error = S3Error::from(ApiError::from(failure));
            let response = error.to_http_response().expect("serialize unattributed failure");
            assert!(!response.headers().contains_key("x-rustfs-error-cause"));
        }
    }

    #[test]
    fn unrelated_unavailable_responses_do_not_inherit_lock_retry_advice() {
        for failure in [
            StorageError::Timeout,
            StorageError::DiskNotFound,
            StorageError::FileCorrupt,
            StorageError::NamespaceLockQuorumUnavailable {
                mode: "read",
                bucket: "bucket".to_owned(),
                object: "object".to_owned(),
                required: 2,
                achieved: 1,
            },
            StorageError::Io(IoError::other("Lock acquisition timeout for bucket/object after 5s")),
            StorageError::SlowDown,
        ] {
            let error = S3Error::from(ApiError::from(failure));
            let response = error.to_http_response().expect("serialize unrelated storage failure");
            assert!(!response.headers().contains_key(http::header::RETRY_AFTER));
        }
    }

    #[test]
    fn lock_503_mapping_records_operation_and_resource_class_without_resource_name() {
        let recorder = DebuggingRecorder::new();
        with_local_recorder(&recorder, || {
            let mut request = S3HttpRequestGuard::new("PUT");
            request.in_scope(|| {
                rustfs_io_metrics::record_s3_op(S3Operation::PutObject);
                let error = ApiError::from(StorageError::Lock(LockError::timeout(
                    ".rustfs.sys/scanner/durable-dirty-producer-replay.json",
                    std::time::Duration::from_secs(5),
                )));
                assert_eq!(error.code, S3ErrorCode::ServiceUnavailable);
            });
        });

        let metrics = recorder.snapshotter().snapshot().into_vec();
        let (key, _, _, value) = metrics
            .iter()
            .find(|(key, _, _, _)| key.key().name() == "rustfs_s3_lock_failures_total")
            .expect("S3 lock failures should be observable");
        assert!(matches!(value, DebugValue::Counter(1)));
        let labels = key
            .key()
            .labels()
            .map(|label| (label.key(), label.value()))
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(labels["op"], S3Operation::PutObject.as_str());
        assert_eq!(labels["mode"], "unknown");
        assert_eq!(labels["reason"], "timeout");
        assert_eq!(labels["resource_class"], "system_metadata");
        assert!(!labels.values().any(|value| value.contains("durable-dirty-producer-replay")));
    }

    #[test]
    fn catalog_lock_failure_uses_canonical_metrics_and_retry_advice_once() {
        let recorder = DebuggingRecorder::new();
        let error = with_local_recorder(&recorder, || {
            let mut request = S3HttpRequestGuard::new("PUT");
            request.in_scope(|| {
                rustfs_io_metrics::record_s3_op(S3Operation::PutObject);
                S3Error::from(ApiError::from(crate::table_catalog::TableCatalogStoreError::LockUnavailable {
                    message: "private catalog operation".to_owned(),
                    source: Box::new(LockError::timeout(
                        "private-bucket/.rustfs-table/warehouses/default/publication.lock@latest",
                        std::time::Duration::from_secs(5),
                    )),
                }))
            })
        });
        let response = error.to_http_response().expect("serialize catalog lock timeout");
        assert_eq!(response.status(), http::StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()[http::header::RETRY_AFTER], "1");
        let metrics = recorder.snapshotter().snapshot().into_vec();
        let (key, _, _, value) = metrics
            .iter()
            .find(|(key, _, _, _)| key.key().name() == "rustfs_s3_lock_failures_total")
            .expect("catalog lock failures should use the canonical counter");
        assert!(matches!(value, DebugValue::Counter(1)));
        let labels = key
            .key()
            .labels()
            .map(|label| (label.key(), label.value()))
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(labels["op"], S3Operation::PutObject.as_str());
        assert_eq!(labels["reason"], "timeout");
        assert_eq!(labels["resource_class"], "object_namespace");
        assert!(!labels.values().any(|value| value.contains("private")));
    }

    #[test]
    fn multipart_lock_resources_have_a_distinct_bounded_class() {
        assert_eq!(lock_resource_class(Some(".rustfs.sys/multipart/upload-1")), "multipart_metadata");
        assert_eq!(lock_resource_class(Some(".minio.sys/multipart/upload-1")), "multipart_metadata");
    }

    /// rustfs/backlog#1734: a refusal to act on a stored bucket config that
    /// cannot be parsed is recoverable once an operator repairs the bytes, so
    /// it answers 503 naming the bucket, config and stored length, not a
    /// generic 500. It must survive the error being cloned on the way.
    #[test]
    fn unreadable_bucket_config_maps_to_service_unavailable_naming_the_config() {
        let err = StorageError::other(crate::storage_api::error::UnreadableBucketConfig {
            bucket: "photos".to_string(),
            config_file: "versioning.xml".to_string(),
            raw_len: 42,
        });
        for api_error in [ApiError::from(err.clone()), ApiError::from(err)] {
            assert_eq!(api_error.code, S3ErrorCode::ServiceUnavailable);
            for needle in ["photos", "versioning.xml", "42"] {
                assert!(api_error.message.contains(needle), "{needle} missing from {:?}", api_error.message);
            }
        }
    }

    #[test]
    fn api_error_diagnostic_preserves_typed_cause_without_sensitive_payload() {
        let error = ApiError::from(StorageError::Io(IoError::new(ErrorKind::TimedOut, "secret=do-not-log")));
        let diagnostic = error.diagnostic();
        assert_eq!(diagnostic.storage_code, Some(StorageErrorCode::Io));
        assert_eq!(diagnostic.io_kind, Some(ErrorKind::TimedOut));
        assert!(!diagnostic.truncated);
        assert!(!format!("{diagnostic:?}").contains("do-not-log"));
        assert!(std::error::Error::source(&error).is_some());

        let error = ApiError::from(StorageError::Io(IoError::other(StorageError::ErasureWriteQuorum)));
        assert_eq!(error.diagnostic().storage_code, Some(StorageErrorCode::ErasureWriteQuorum));

        let mut status = tonic::Status::unavailable("secret RPC message");
        status
            .metadata_mut()
            .insert("authorization", "secret-token".parse().expect("metadata value"));
        let error = ApiError::from(StorageError::from(status));
        assert_eq!(error.diagnostic().rpc_code, Some(tonic::Code::Unavailable));
        assert!(!format!("{:?}", error.diagnostic()).contains("secret"));
    }

    #[test]
    fn api_error_diagnostic_bounds_cyclic_error_chains() {
        #[derive(Debug)]
        struct Cycle;
        impl std::fmt::Display for Cycle {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("secret cycle")
            }
        }
        impl std::error::Error for Cycle {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                Some(self)
            }
        }
        let error = ApiError {
            code: S3ErrorCode::InternalError,
            message: "safe".into(),
            source: Some(Box::new(Cycle)),
        };
        assert!(error.diagnostic().truncated);
        assert!(!format!("{:?}", error.diagnostic()).contains("secret"));
        let error = S3Error::from(ApiError {
            code: S3ErrorCode::ServiceUnavailable,
            message: "safe".into(),
            source: Some(Box::new(Cycle)),
        });
        let response = error.to_http_response().expect("serialize cyclic unavailable source");
        assert!(!response.headers().contains_key("x-rustfs-error-cause"));
        assert!(!response.headers().contains_key(http::header::RETRY_AFTER));
    }

    #[derive(Debug)]
    struct MockS3sBodyStreamError(&'static str);

    impl std::fmt::Display for MockS3sBodyStreamError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(self.0)
        }
    }

    impl std::error::Error for MockS3sBodyStreamError {}

    #[test]
    fn test_api_error_from_io_error() {
        let io_error = IoError::new(ErrorKind::PermissionDenied, "permission denied");
        let api_error: ApiError = io_error.into();

        assert_eq!(api_error.code, S3ErrorCode::InternalError);
        assert!(api_error.message.contains("permission denied"));
        assert!(api_error.source.is_some());
    }

    #[test]
    fn test_api_error_from_io_error_different_kinds() {
        let test_cases = vec![
            (ErrorKind::NotFound, "not found"),
            (ErrorKind::InvalidInput, "invalid input"),
            (ErrorKind::TimedOut, "timed out"),
            (ErrorKind::WriteZero, "write zero"),
            (ErrorKind::Other, "other error"),
        ];

        for (kind, message) in test_cases {
            let io_error = IoError::new(kind, message);
            let api_error: ApiError = io_error.into();

            assert_eq!(api_error.code, S3ErrorCode::InternalError);
            assert!(api_error.message.contains(message));
            assert!(api_error.source.is_some());

            // Test that source can be downcast back to io::Error
            let source = api_error.source.as_ref().unwrap();
            let downcast_io_error = source.downcast_ref::<IoError>();
            assert!(downcast_io_error.is_some());
            assert_eq!(downcast_io_error.unwrap().kind(), kind);
        }
    }

    #[test]
    fn payload_sha256_mismatch_io_errors_map_to_content_sha256_mismatch() {
        for storage in [false, true] {
            for nested in [false, true] {
                for error in [
                    IoError::other(rustfs_rio::Sha256Mismatch {
                        expected_sha256: "expected".to_string(),
                        calculated_sha256: "calculated".to_string(),
                    }),
                    IoError::other(UploadStreamError::Sha256Mismatch),
                ] {
                    let error = if nested { IoError::other(error) } else { error };
                    let api_error = if storage {
                        ApiError::from(StorageError::Io(error))
                    } else {
                        ApiError::from(error)
                    };
                    assert_eq!(api_error.code.as_str(), "XAmzContentSHA256Mismatch");
                    assert_eq!(
                        api_error.message,
                        "The provided 'x-amz-content-sha256' header does not match what was computed."
                    );
                    assert!(
                        error_chain_has_type::<rustfs_rio::Sha256Mismatch>(&api_error)
                            || error_chain_has_type::<UploadStreamError>(&api_error)
                    );
                    let s3_error = S3Error::from(api_error);
                    assert_eq!(s3_error.status_code(), Some(StatusCode::BAD_REQUEST));
                    assert!(s3_error.source().is_some());
                }
            }
        }
    }

    #[test]
    fn content_md5_and_additional_checksum_mismatches_map_to_bad_digest() {
        for storage in [false, true] {
            for nested in [false, true] {
                for error in [
                    IoError::other(rustfs_rio::BadDigest {
                        expected_md5: "expected".to_string(),
                        calculated_md5: "calculated".to_string(),
                    }),
                    IoError::other(rustfs_rio::ChecksumMismatch {
                        want: "expected".into(),
                        got: "calculated".into(),
                    }),
                ] {
                    let error = if nested { IoError::other(error) } else { error };
                    let api_error = if storage {
                        ApiError::from(StorageError::Io(error))
                    } else {
                        ApiError::from(error)
                    };
                    assert_eq!(api_error.code, S3ErrorCode::BadDigest);
                    assert_eq!(api_error.message, ApiError::error_code_to_message(&S3ErrorCode::BadDigest));
                    assert_eq!(S3Error::from(api_error).status_code(), Some(StatusCode::BAD_REQUEST));
                }
            }
        }
    }

    #[test]
    fn underlying_upload_stream_errors_remain_internal() {
        let api_error = ApiError::from(IoError::other(UploadStreamError::Underlying(Box::new(IoError::other(
            "underlying body error",
        )))));
        assert_eq!(api_error.code, S3ErrorCode::InternalError);

        let api_error = ApiError::from(StorageError::Io(IoError::other(UploadStreamError::Underlying(Box::new(
            IoError::other("underlying body error"),
        )))));
        assert_eq!(api_error.code, S3ErrorCode::InternalError);
    }

    #[test]
    fn s3s_incomplete_body_stream_errors_map_to_incomplete_body() {
        for error in [UploadStreamError::LengthMismatch, UploadStreamError::Incomplete] {
            let api_error = ApiError::from(IoError::other(error));
            assert_eq!(api_error.code, S3ErrorCode::IncompleteBody);
            assert_eq!(api_error.message, ApiError::error_code_to_message(&S3ErrorCode::IncompleteBody));
        }

        for error in [UploadStreamError::LengthMismatch, UploadStreamError::Incomplete] {
            let api_error = ApiError::from(StorageError::Io(IoError::other(error)));
            assert_eq!(api_error.code, S3ErrorCode::IncompleteBody);
            assert_eq!(api_error.message, ApiError::error_code_to_message(&S3ErrorCode::IncompleteBody));
        }

        for storage in [false, true] {
            for nested in [false, true] {
                for error in [AwsChunkedStreamError::LengthMismatch, AwsChunkedStreamError::Incomplete] {
                    let error = IoError::other(error);
                    let error = if nested { IoError::other(error) } else { error };
                    let api_error = if storage {
                        ApiError::from(StorageError::Io(error))
                    } else {
                        ApiError::from(error)
                    };
                    assert_eq!(api_error.code, S3ErrorCode::IncompleteBody);
                    assert_eq!(api_error.message, ApiError::error_code_to_message(&S3ErrorCode::IncompleteBody));
                }
            }
        }
    }

    #[test]
    fn body_error_display_text_does_not_impersonate_typed_failures() {
        for message in [
            "UploadStreamError: Sha256Mismatch",
            "UploadStreamError: Incomplete",
            "AwsChunkedStreamError: LengthMismatch",
        ] {
            for error in [
                ApiError::from(IoError::other(MockS3sBodyStreamError(message))),
                ApiError::from(StorageError::Io(IoError::other(MockS3sBodyStreamError(message)))),
            ] {
                assert_eq!(error.code, S3ErrorCode::InternalError);
            }
        }
    }

    #[tokio::test]
    async fn aws_chunked_named_errors_preserve_http_contract_across_io_boundaries() {
        use http_body_util::BodyExt as _;

        for storage in [false, true] {
            for nested in [false, true] {
                for error in [
                    AwsChunkedStreamError::SignatureMismatch,
                    AwsChunkedStreamError::FormatError,
                    AwsChunkedStreamError::Incomplete,
                    AwsChunkedStreamError::LengthMismatch,
                    AwsChunkedStreamError::ChunkMetaTooLarge(1025, 1024),
                    AwsChunkedStreamError::ChunkDataTooLarge(257, 256),
                    AwsChunkedStreamError::TrailersTooLarge(16385, 16384),
                    AwsChunkedStreamError::TooManyTrailerHeaders(101, 100),
                ] {
                    let expected = error.to_s3_error_code();
                    let expected_status = if expected == S3ErrorCode::SignatureDoesNotMatch {
                        StatusCode::FORBIDDEN
                    } else {
                        StatusCode::BAD_REQUEST
                    };
                    let error = IoError::other(error);
                    let error = if nested {
                        IoError::other(AwsChunkedStreamError::Underlying(Box::new(IoError::other(error))))
                    } else {
                        error
                    };
                    let api_error = if storage {
                        ApiError::from(StorageError::Io(error))
                    } else {
                        ApiError::from(error)
                    };
                    assert_eq!(api_error.code, expected);
                    assert_eq!(api_error.message, ApiError::error_code_to_message(&expected));
                    assert!(error_chain_has_type::<AwsChunkedStreamError>(&api_error));
                    let response = S3Error::from(api_error).to_http_response().expect("serialize chunked error");
                    assert_eq!(response.status(), expected_status);
                    let body = response
                        .into_body()
                        .collect()
                        .await
                        .expect("read chunked error XML")
                        .to_bytes();
                    let xml = std::str::from_utf8(&body).expect("error XML is UTF-8");
                    assert!(xml.contains(&format!("<Code>{}</Code>", expected.as_str())));
                    assert!(!xml.contains("AwsChunkedStreamError"));
                    assert!(!xml.contains("16385"));
                }
            }
        }
    }

    #[test]
    fn aws_chunked_underlying_errors_retain_typed_marker_precedence() {
        for storage in [false, true] {
            for (inner, expected) in [
                (IoError::other(UploadStreamError::Sha256Mismatch), CONTENT_SHA256_MISMATCH_CODE),
                (IoError::other(UploadLimitExceeded { limit: 5 }), "EntityTooLarge"),
                (
                    IoError::other(ClientBodyReadTimeout {
                        timeout: std::time::Duration::from_secs(300),
                        raw_bytes_received: 0,
                    }),
                    "RequestTimeout",
                ),
                (IoError::other(s3s::BodySizeLimitExceeded { size: 6, limit: 5 }), "EntityTooLarge"),
                (IoError::other("private underlying body diagnostic"), "InternalError"),
            ] {
                let error = IoError::other(AwsChunkedStreamError::Underlying(Box::new(inner)));
                let error = if storage {
                    ApiError::from(StorageError::Io(error))
                } else {
                    ApiError::from(error)
                };
                assert_eq!(error.code.as_str(), expected);
            }
            for chunked in [
                AwsChunkedStreamError::SignatureMismatch,
                AwsChunkedStreamError::FormatError,
                AwsChunkedStreamError::ChunkDataTooLarge(257, 256),
            ] {
                let error = IoError::other(ServerSideSourceReadError::new("CopyObject", IoError::other(chunked)));
                let error = if storage {
                    ApiError::from(StorageError::Io(error))
                } else {
                    ApiError::from(error)
                };
                assert_eq!(error.code, S3ErrorCode::ServiceUnavailable);
            }
        }
    }

    #[test]
    fn aws_chunked_classification_does_not_format_private_diagnostics() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };

        #[derive(Debug)]
        struct Diagnostic {
            displays: Arc<AtomicUsize>,
            source: AwsChunkedStreamError,
        }
        impl std::fmt::Display for Diagnostic {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.displays.fetch_add(1, Ordering::Relaxed);
                f.write_str("private chunk signature and body diagnostic")
            }
        }
        impl std::error::Error for Diagnostic {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                Some(&self.source)
            }
        }
        let displays = Arc::new(AtomicUsize::new(0));
        let error = ApiError::from(IoError::other(Diagnostic {
            displays: Arc::clone(&displays),
            source: AwsChunkedStreamError::SignatureMismatch,
        }));
        assert_eq!(
            displays.load(Ordering::Relaxed),
            0,
            "classified failures must not format discarded diagnostics"
        );
        assert_eq!(error.code, S3ErrorCode::SignatureDoesNotMatch);
        assert!(!error.message.contains("private"));
        assert!(error_chain_has_type::<AwsChunkedStreamError>(&error));
    }

    #[test]
    fn body_size_limit_exceeded_maps_to_entity_too_large_across_io_boundaries() {
        // Shape observed in production (issue #7596):
        // Custom { UnexpectedEof, Custom { Other, BodySizeLimitExceeded { size, limit } } }
        let nested = || {
            IoError::new(
                ErrorKind::UnexpectedEof,
                IoError::other(s3s::BodySizeLimitExceeded {
                    size: 16384,
                    limit: 6389,
                }),
            )
        };

        let direct: ApiError = nested().into();
        assert_eq!(direct.code, S3ErrorCode::EntityTooLarge);
        assert_eq!(direct.message, ApiError::error_code_to_message(&S3ErrorCode::EntityTooLarge));

        let storage: ApiError = StorageError::Io(nested()).into();
        assert_eq!(storage.code, S3ErrorCode::EntityTooLarge);
        assert!(storage.source.is_some());

        // An unrelated message that merely mentions a limit stays internal.
        let other: ApiError = IoError::other(MockS3sBodyStreamError("limit exceeded for something else")).into();
        assert_eq!(other.code, S3ErrorCode::InternalError);
        let impostor: ApiError = IoError::other(MockS3sBodyStreamError("body size 16384 exceeds limit 6389")).into();
        assert_eq!(impostor.code, S3ErrorCode::InternalError);
    }

    #[test]
    fn client_body_timeout_survives_intermediate_io_and_storage_errors() {
        #[derive(Debug)]
        struct DecoderError(IoError);
        impl std::fmt::Display for DecoderError {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("decoder source failed")
            }
        }
        impl std::error::Error for DecoderError {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                Some(&self.0)
            }
        }
        let nested = || {
            IoError::other(DecoderError(IoError::other(IoError::new(
                ErrorKind::TimedOut,
                ClientBodyReadTimeout {
                    timeout: std::time::Duration::from_secs(300),
                    raw_bytes_received: 8192,
                },
            ))))
        };
        for error in [ApiError::from(nested()), ApiError::from(StorageError::Io(nested()))] {
            assert_eq!(error.code, S3ErrorCode::RequestTimeout);
            assert!(error_chain_has_type::<ClientBodyReadTimeout>(&error));
            let s3_error = S3Error::from(error);
            assert_eq!(s3_error.status_code(), Some(StatusCode::BAD_REQUEST));
        }
        for error in [
            ApiError::from(IoError::new(ErrorKind::TimedOut, "disk read timeout")),
            ApiError::from(StorageError::Io(IoError::new(ErrorKind::TimedOut, "peer timeout"))),
        ] {
            assert_eq!(error.code, S3ErrorCode::InternalError);
        }
        // A server-side source wrapper has precedence even if a remote source
        // has carried its own client-body marker across an I/O boundary.
        let source = ServerSideSourceReadError::new("CopyObject", nested());
        assert_eq!(ApiError::from(IoError::other(source)).code, S3ErrorCode::ServiceUnavailable);
    }

    #[test]
    fn body_error_classification_bounds_cyclic_source_chains() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        #[derive(Debug)]
        struct Cycle(Arc<AtomicUsize>);
        impl std::fmt::Display for Cycle {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("cycle")
            }
        }
        impl std::error::Error for Cycle {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                self.0.fetch_add(1, Ordering::Relaxed);
                Some(self)
            }
        }
        let visits = Arc::new(AtomicUsize::new(0));
        let error = ApiError::from(IoError::other(Cycle(Arc::clone(&visits))));
        assert_eq!(error.code, S3ErrorCode::InternalError);
        assert!(visits.load(Ordering::Relaxed) <= 64, "body classification must share one chain budget");
    }

    #[test]
    fn body_error_classification_preserves_nested_marker_precedence() {
        for storage in [false, true] {
            for (inner, expected) in [
                (IoError::other(UploadStreamError::Sha256Mismatch), CONTENT_SHA256_MISMATCH_CODE),
                (
                    IoError::other(rustfs_rio::BadDigest {
                        expected_md5: "expected".into(),
                        calculated_md5: "calculated".into(),
                    }),
                    "BadDigest",
                ),
                (IoError::other(UploadLimitExceeded { limit: 5 }), "EntityTooLarge"),
                (
                    IoError::other(ClientBodyReadTimeout {
                        timeout: std::time::Duration::from_secs(300),
                        raw_bytes_received: 0,
                    }),
                    "ServiceUnavailable",
                ),
                (IoError::other(s3s::BodySizeLimitExceeded { size: 6, limit: 5 }), "ServiceUnavailable"),
            ] {
                let error = IoError::other(ServerSideSourceReadError::new("CopyObject", inner));
                let error = if storage {
                    ApiError::from(StorageError::Io(error))
                } else {
                    ApiError::from(error)
                };
                assert_eq!(error.code.as_str(), expected);
                assert!(error.source.is_some());
            }
        }
    }

    #[tokio::test]
    async fn short_hash_reader_body_maps_to_incomplete_body_across_io_boundaries() {
        use tokio::io::AsyncReadExt;
        for storage in [false, true] {
            let mut reader =
                rustfs_rio::HashReader::from_stream(std::io::Cursor::new(b"short".to_vec()), 10, 10, None, None, false)
                    .expect("valid declared body length");
            let error = reader
                .read_to_end(&mut Vec::new())
                .await
                .expect_err("five bytes cannot satisfy declared ten");
            let error = if storage {
                ApiError::from(StorageError::Io(error))
            } else {
                ApiError::from(error)
            };
            assert_eq!(error.code, S3ErrorCode::IncompleteBody);
            assert!(error_chain_has_type::<rustfs_rio::IncompleteBody>(&error));
            assert_eq!(S3Error::from(error).status_code(), Some(StatusCode::BAD_REQUEST));
        }
    }

    /// Exercise the public error type through the actual body budget.
    #[tokio::test]
    async fn real_s3s_body_size_limit_error_maps_to_entity_too_large() {
        use futures::StreamExt;

        let real_error = || async {
            let mut body = s3s::Body::from(bytes::Bytes::from_static(b"hello"));
            body.set_limit(Some(4));
            body.next()
                .await
                .expect("one frame")
                .expect_err("five bytes must exceed a four-byte budget")
        };

        let err = real_error().await;
        assert!(err.is::<s3s::BodySizeLimitExceeded>(), "unexpected body error: {err}");

        let err = real_error().await;
        let storage: ApiError = StorageError::Io(IoError::new(ErrorKind::UnexpectedEof, IoError::other(err))).into();
        assert_eq!(storage.code, S3ErrorCode::EntityTooLarge);
        assert_eq!(storage.message, ApiError::error_code_to_message(&S3ErrorCode::EntityTooLarge));

        let err = real_error().await;
        let direct: ApiError = IoError::other(err).into();
        assert_eq!(direct.code, S3ErrorCode::EntityTooLarge);
    }

    /// Drive a real hyper HTTP/1 server so the test sees hyper's own body EOF
    /// error (`hyper::Error(Body, UnexpectedEof, IncompleteBody)`), which has no
    /// public constructor.
    async fn capture_hyper_body_eof_error() -> hyper::Error {
        use http_body_util::BodyExt;
        use hyper::service::service_fn;
        use hyper_util::rt::TokioIo;
        use std::sync::{Arc, Mutex};
        use tokio::io::AsyncWriteExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let captured: Arc<Mutex<Option<hyper::Error>>> = Arc::new(Mutex::new(None));
        let server_slot = Arc::clone(&captured);
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let slot = server_slot;
            let service = service_fn(move |req: hyper::Request<hyper::body::Incoming>| {
                let slot = Arc::clone(&slot);
                async move {
                    let err = req.into_body().collect().await.expect_err("short body must fail");
                    *slot.lock().expect("slot") = Some(err);
                    Ok::<_, std::convert::Infallible>(hyper::Response::new(String::new()))
                }
            });
            let _ = hyper::server::conn::http1::Builder::new()
                .serve_connection(TokioIo::new(stream), service)
                .await;
        });

        let mut client = tokio::net::TcpStream::connect(addr).await.expect("connect");
        client
            .write_all(b"PUT /bucket/key HTTP/1.1\r\nHost: localhost\r\nContent-Length: 100\r\n\r\nabc")
            .await
            .expect("write partial body");
        client.shutdown().await.expect("shutdown write side");
        let _ = tokio::time::timeout(std::time::Duration::from_secs(10), server).await;
        let captured = captured.lock().expect("slot").take();
        captured.expect("hyper body error captured")
    }

    #[tokio::test]
    async fn hyper_body_eof_maps_to_incomplete_body_across_io_boundaries() {
        let hyper_err = capture_hyper_body_eof_error().await;
        assert!(is_hyper_body_eof(&hyper_err), "unexpected hyper error shape: {hyper_err:?}");

        // Shape observed in production (issue #7596):
        // Custom { UnexpectedEof, Custom { Other, hyper::Error(Body, UnexpectedEof, IncompleteBody) } }
        let nested = IoError::new(ErrorKind::UnexpectedEof, IoError::other(hyper_err));
        let storage: ApiError = StorageError::Io(nested).into();
        assert_eq!(storage.code, S3ErrorCode::IncompleteBody);
        assert_eq!(storage.message, ApiError::error_code_to_message(&S3ErrorCode::IncompleteBody));

        let hyper_err = capture_hyper_body_eof_error().await;
        let direct: ApiError = IoError::other(hyper_err).into();
        assert_eq!(direct.code, S3ErrorCode::IncompleteBody);
    }

    #[test]
    fn server_side_source_read_error_maps_to_service_unavailable_before_incomplete_body() {
        let short_source = IoError::new(ErrorKind::UnexpectedEof, rustfs_rio::IncompleteBody { remaining: 17 });
        let marker = ServerSideSourceReadError::new("CopyObject", short_source);

        let api_error = ApiError::from(IoError::new(ErrorKind::UnexpectedEof, marker));
        assert_eq!(api_error.code, S3ErrorCode::ServiceUnavailable);
        assert_ne!(api_error.code, S3ErrorCode::IncompleteBody);

        let short_source = IoError::new(ErrorKind::UnexpectedEof, rustfs_rio::IncompleteBody { remaining: 17 });
        let marker = ServerSideSourceReadError::new("CopyObject", short_source);
        let api_error = ApiError::from(StorageError::Io(IoError::new(ErrorKind::UnexpectedEof, marker)));
        assert_eq!(api_error.code, S3ErrorCode::ServiceUnavailable);
        assert_ne!(api_error.code, S3ErrorCode::IncompleteBody);
    }

    #[test]
    fn test_api_error_surfaces_invalid_argument_reason() {
        let err = StorageError::InvalidArgument(
            "bucket".into(),
            "a:b*c".into(),
            "object key contains characters unsupported on Windows hosts".into(),
        );
        let api_error: ApiError = err.into();
        assert_eq!(api_error.code, S3ErrorCode::InvalidArgument);
        assert_eq!(
            api_error.message,
            "Invalid argument: object key contains characters unsupported on Windows hosts"
        );

        // An empty reason falls back to the generic message.
        let err = StorageError::InvalidArgument("bucket".into(), "object".into(), String::new());
        let api_error: ApiError = err.into();
        assert_eq!(api_error.message, "Invalid argument");
    }

    #[test]
    fn test_api_error_other_function() {
        let custom_error = "Custom API error";
        let api_error = ApiError::other(custom_error);

        assert_eq!(api_error.code, S3ErrorCode::InternalError);
        assert_eq!(api_error.message, custom_error);
        assert!(api_error.source.is_some());
    }

    #[test]
    fn test_api_error_other_function_with_complex_error() {
        let io_error = IoError::new(ErrorKind::InvalidData, "complex error");
        let api_error = ApiError::other(io_error);

        assert_eq!(api_error.code, S3ErrorCode::InternalError);
        assert!(api_error.message.contains("complex error"));
        assert!(api_error.source.is_some());

        // Test that source can be downcast back to io::Error
        let source = api_error.source.as_ref().unwrap();
        let downcast_io_error = source.downcast_ref::<IoError>();
        assert!(downcast_io_error.is_some());
        assert_eq!(downcast_io_error.unwrap().kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn test_api_error_from_storage_error() {
        let storage_error = StorageError::BucketNotFound("test-bucket".to_string());
        let api_error: ApiError = storage_error.into();

        assert_eq!(api_error.code, S3ErrorCode::NoSuchBucket);
        assert!(api_error.source.is_some());

        // Test that source can be downcast back to StorageError
        let source = api_error.source.as_ref().unwrap();
        let downcast_storage_error = source.downcast_ref::<StorageError>();
        assert!(downcast_storage_error.is_some());
    }

    #[test]
    fn policy_metadata_read_failure_maps_to_internal_error() {
        let api_error = ApiError::from(StorageError::Io(IoError::other("policy read failed")));

        assert_eq!(api_error.code, S3ErrorCode::InternalError);
        assert!(api_error.source.is_some());
    }

    #[test]
    fn storage_io_internal_error_redacts_public_message_and_retains_source() {
        let sensitive_path = "/sensitive/storage/path";
        let api_error = ApiError::from(StorageError::Io(IoError::new(
            ErrorKind::PermissionDenied,
            format!("permission denied: {sensitive_path}"),
        )));

        assert_eq!(api_error.code, S3ErrorCode::InternalError);
        assert_eq!(api_error.message, ApiError::error_code_to_message(&S3ErrorCode::InternalError));
        assert!(!api_error.message.contains(sensitive_path));
        let source = api_error
            .source
            .as_deref()
            .and_then(|source| source.downcast_ref::<StorageError>())
            .expect("API error should retain the storage error source");
        assert!(matches!(source, StorageError::Io(io_error) if io_error.to_string().contains(sensitive_path)));
    }

    #[test]
    fn test_kms_service_unavailable_maps_to_retryable_error() {
        let api_error = ApiError::from(StorageError::other(KmsUnavailableError));

        assert_eq!(api_error.code, S3ErrorCode::ServiceUnavailable);
        assert_eq!(api_error.message, "The service is unavailable. Please retry.");
    }

    #[test]
    fn test_kms_backend_unavailable_maps_to_retryable_error() {
        let api_error = ApiError::from(StorageError::other(rustfs_kms::KmsError::backend_error("Vault connection refused")));

        assert_eq!(api_error.code, S3ErrorCode::ServiceUnavailable);
        assert_eq!(api_error.message, "The service is unavailable. Please retry.");
    }

    #[test]
    fn pool_metadata_failures_map_to_503_and_preserve_typed_private_context() {
        use crate::storage_api::error::{PoolMetadataError, PoolMetadataFailure};
        let since = time::OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("fixed failure time");
        for kind in [
            PoolMetadataFailure::ReadUnavailable,
            PoolMetadataFailure::RecoveryRequired,
            PoolMetadataFailure::TransactionUnknown,
            PoolMetadataFailure::FenceLost,
        ] {
            let error = StorageError::other(PoolMetadataError {
                kind,
                operation: "private operation path".to_owned(),
                phase: "prepare_cas",
                since,
                source: Some(std::sync::Arc::new(StorageError::other("private disk failure"))),
            });
            let error = StorageError::Io(std::io::Error::new(std::io::ErrorKind::TimedOut, error));
            let cloned = error.clone();
            assert_eq!(cloned, error, "cloning must preserve the outer I/O kind and message");
            assert_eq!(cloned.pool_metadata_failure().unwrap().kind, kind);
            let api = ApiError::from(cloned);
            assert_eq!(api.code, S3ErrorCode::ServiceUnavailable);
            assert_eq!(api.message, "The service is unavailable. Please retry.");
            assert!(!api.message.contains("private"));
            let source = api.source.as_ref().unwrap().downcast_ref::<StorageError>().unwrap();
            assert_eq!(source.pool_metadata_failure().unwrap().kind, kind);
            let diagnostic = api.diagnostic();
            assert_eq!(
                diagnostic.pool_metadata,
                Some(PoolMetadataDiagnostic {
                    reason: kind.as_str(),
                    phase: "prepare_cas",
                    since_unix_secs: since.unix_timestamp(),
                })
            );
            assert!(!diagnostic.truncated);
            assert!(!format!("{diagnostic:?}").contains("private"));
            assert_eq!(api.diagnostic(), diagnostic, "inspection must preserve the original failure time");
        }
    }

    #[test]
    fn test_kms_key_not_found_maps_to_bad_request_kms_not_found_exception() {
        let api_error = ApiError::from(StorageError::other(rustfs_kms::KmsError::key_not_found("no-such-key")));

        assert_eq!(api_error.code, S3ErrorCode::Custom(KMS_KEY_NOT_FOUND_ERROR_CODE.into()));
        assert_eq!(api_error.message, "KMS key not found: no-such-key");

        // s3s knows no status for a custom code; the conversion has to supply it.
        let s3_error = S3Error::from(api_error);
        assert_eq!(s3_error.status_code(), Some(StatusCode::BAD_REQUEST));
    }

    /// backlog#2368 B6: every KMS failure class that is not an integrity fault
    /// carries a status that says whether retrying, fixing the request, or
    /// calling a human is the right response. Collapsing them onto 500 made
    /// SDKs back off on unfixable configuration errors and filed every one of
    /// them as a server fault.
    #[test]
    fn kms_data_plane_errors_are_classified_by_what_the_caller_should_do() {
        let cases: Vec<(rustfs_kms::KmsError, S3ErrorCode)> = vec![
            // Names something the KMS does not hold.
            (
                rustfs_kms::KmsError::key_version_not_found("finance-key", 7),
                S3ErrorCode::Custom(KMS_KEY_NOT_FOUND_ERROR_CODE.into()),
            ),
            // The key exists but its state forbids the operation.
            (rustfs_kms::KmsError::invalid_key_state("disabled"), S3ErrorCode::InvalidRequest),
            // Request-side faults.
            (rustfs_kms::KmsError::context_mismatch("bucket differs"), S3ErrorCode::InvalidRequest),
            (rustfs_kms::KmsError::invalid_key("malformed key id"), S3ErrorCode::InvalidRequest),
            (rustfs_kms::KmsError::validation_error("empty key id"), S3ErrorCode::InvalidRequest),
            (rustfs_kms::KmsError::unsupported_algorithm("aes-999"), S3ErrorCode::InvalidRequest),
            (rustfs_kms::KmsError::invalid_key_size(32, 16), S3ErrorCode::InvalidRequest),
            (
                rustfs_kms::KmsError::configuration_error("no default key configured"),
                S3ErrorCode::InvalidRequest,
            ),
            // A policy decision that needs a human, never retried by an SDK.
            (rustfs_kms::KmsError::access_denied("no kms:Decrypt grant"), S3ErrorCode::AccessDenied),
            // Transient, worth retrying, counted against availability.
            (rustfs_kms::KmsError::backend_error("vault refused"), S3ErrorCode::ServiceUnavailable),
            (
                rustfs_kms::KmsError::operation_timed_out("attempt deadline"),
                S3ErrorCode::ServiceUnavailable,
            ),
            (
                rustfs_kms::KmsError::operation_cancelled("shutting down"),
                S3ErrorCode::ServiceUnavailable,
            ),
            (
                rustfs_kms::KmsError::credentials_unavailable("approle login failed"),
                S3ErrorCode::ServiceUnavailable,
            ),
            (rustfs_kms::KmsError::cache_error("poisoned"), S3ErrorCode::ServiceUnavailable),
            // A key store that cannot be read is an outage, not a missing key:
            // rustfs/rustfs#7470 made the backend say so, and the S3 boundary
            // has to keep the two apart.
            (
                rustfs_kms::KmsError::io_error("No such file or directory (os error 2)"),
                S3ErrorCode::ServiceUnavailable,
            ),
            // A permanent gap in the configured backend, never a missing resource.
            (
                rustfs_kms::KmsError::unsupported_capability("local", "rewrap"),
                S3ErrorCode::NotImplemented,
            ),
        ];

        for (error, expected) in cases {
            let description = error.to_string();
            let api_error = ApiError::from(StorageError::other(error));
            assert_eq!(api_error.code, expected, "wrong code for: {description}");
            assert_ne!(api_error.code, S3ErrorCode::InternalError, "must not be a server fault: {description}");
        }
    }

    /// A deployment-side configuration message describes the server, not the
    /// request, so it stays in `source` the way the storage-IO mapping does.
    #[test]
    fn kms_configuration_errors_do_not_echo_deployment_detail() {
        let detail = "vault mount /secret/rustfs-prod has no default key";
        let api_error = ApiError::from(StorageError::other(rustfs_kms::KmsError::configuration_error(detail)));

        assert_eq!(api_error.code, S3ErrorCode::InvalidRequest);
        assert!(
            !api_error.message.contains(detail),
            "message leaked deployment detail: {}",
            api_error.message
        );
        let source = api_error
            .source
            .as_deref()
            .and_then(|source| source.downcast_ref::<StorageError>())
            .expect("API error should retain the storage error source");
        assert!(source.to_string().contains(detail), "the detail must survive on the source");
    }

    #[test]
    fn test_generated_error_codes_keep_their_own_status() {
        let s3_error = S3Error::from(ApiError::from(StorageError::other(rustfs_kms::KmsError::backend_error("down"))));

        assert_eq!(s3_error.status_code(), Some(StatusCode::SERVICE_UNAVAILABLE));
    }

    #[test]
    fn test_slow_down_read_api_error_maps_to_retryable_status() {
        let api_error = slow_down_read_api_error(StorageError::PartMissingOrCorrupt);

        assert_eq!(api_error.code, S3ErrorCode::Custom(SLOW_DOWN_READ_CODE.into()));
        assert_eq!(S3Error::from(api_error).status_code(), Some(StatusCode::SERVICE_UNAVAILABLE));
    }

    #[test]
    fn put_read_quorum_error_preserves_context_and_is_idempotent() {
        for source in [
            StorageError::ErasureReadQuorum,
            StorageError::InsufficientReadQuorum("bucket".into(), "object".into()),
        ] {
            for nested in [false, true] {
                let mut err: S3Error = ApiError::from(source.clone()).into();
                if nested {
                    err.set_source(Box::new(IoError::other(source.clone())));
                }
                err.set_request_id("put-quorum-request");
                let mut headers = http::HeaderMap::new();
                headers.insert("retry-after", http::HeaderValue::from_static("2"));
                err.set_headers(headers);

                let err = normalize_read_quorum_error(normalize_read_quorum_error(err));
                assert_eq!(err.code(), &S3ErrorCode::ServiceUnavailable);
                assert_eq!(err.status_code(), Some(StatusCode::SERVICE_UNAVAILABLE));
                assert_eq!(err.message(), Some("The service is unavailable. Please retry."));
                assert_eq!(err.request_id(), Some("put-quorum-request"));
                assert_eq!(err.headers().expect("retry headers must survive")["retry-after"], "2");
                let retained = error_chain_find(err.source().expect("typed source must survive"), |error| {
                    error.downcast_ref::<StorageError>().cloned()
                });
                assert_eq!(retained, Some(source.clone()));
            }
        }
    }

    #[test]
    fn put_read_quorum_error_preserves_other_error_contracts() {
        for code in [
            S3ErrorCode::SlowDown,
            S3ErrorCode::AccessDenied,
            S3ErrorCode::InvalidRequest,
            S3ErrorCode::PreconditionFailed,
            S3ErrorCode::BadDigest,
            S3ErrorCode::IncompleteBody,
            S3ErrorCode::NoSuchKey,
            S3ErrorCode::NoSuchBucket,
            S3ErrorCode::InternalError,
            S3ErrorCode::ServiceUnavailable,
        ] {
            let mut err = S3Error::with_message(code.clone(), "original message");
            err.set_source(Box::new(StorageError::ErasureReadQuorum));
            let original_status = err.status_code();
            let err = normalize_read_quorum_error(err);
            assert_eq!(err.code(), &code);
            assert_eq!(err.message(), Some("original message"));
            assert_eq!(err.status_code(), original_status);
        }
        for source in [
            None,
            Some(StorageError::ErasureWriteQuorum),
            Some(StorageError::PartMissingOrCorrupt),
        ] {
            let mut err = S3Error::with_message(S3ErrorCode::Custom(SLOW_DOWN_READ_CODE.into()), "original message");
            if let Some(source) = source {
                err.set_source(Box::new(source));
            }
            let err = normalize_read_quorum_error(err);
            assert_eq!(err.code(), &S3ErrorCode::Custom(SLOW_DOWN_READ_CODE.into()));
            assert_eq!(err.message(), Some("original message"));
        }
    }

    #[test]
    fn conditional_delete_storage_errors_preserve_api_contract() {
        for (source, expected) in [
            (StorageError::ConditionalDeleteConflict, S3ErrorCode::OperationAborted),
            (StorageError::GuardedDeleteUnsupported, S3ErrorCode::NotImplemented),
        ] {
            let error = ApiError::from(source.clone());
            assert_eq!(error.code, expected);
            assert!(
                error
                    .source
                    .as_ref()
                    .is_some_and(|value| value.downcast_ref::<StorageError>() == Some(&source))
            );
        }
    }

    #[test]
    fn test_unknown_authoritative_quota_usage_maps_to_retryable_error() {
        let api_error = ApiError::from(QuotaError::UsageUnavailable {
            bucket: "bucket".to_string(),
        });

        assert_eq!(api_error.code, S3ErrorCode::ServiceUnavailable);
        assert_eq!(api_error.message, "The service is unavailable. Please retry.");
    }

    #[test]
    fn test_kms_cryptographic_error_is_not_retryable() {
        let api_error = ApiError::from(StorageError::other(rustfs_kms::KmsError::cryptographic_error(
            "decrypt",
            "authentication failed",
        )));

        assert_eq!(api_error.code, S3ErrorCode::InternalError);
    }

    #[test]
    fn test_kms_material_faults_map_to_internal_error_not_retryable_or_not_found() {
        // Missing/corrupt key material is a persistent integrity fault of an existing key.
        // It must not be disguised as a retryable backend outage (503 invites pointless
        // retries) nor as NoSuchKey/404 (which suggests the key can be recreated —
        // recreating it would orphan every DEK wrapped by the original material).
        let material_faults = [
            rustfs_kms::KmsError::material_missing("key-a"),
            rustfs_kms::KmsError::material_corrupt("key-a", "truncated record"),
            rustfs_kms::KmsError::material_authentication_failed("key-a"),
            rustfs_kms::KmsError::unsupported_format_version("key-a", "v99"),
        ];

        for fault in material_faults {
            let description = fault.to_string();
            let api_error = ApiError::from(StorageError::other(fault));

            assert_eq!(api_error.code, S3ErrorCode::InternalError, "wrong code for: {description}");
            assert_ne!(api_error.code, S3ErrorCode::ServiceUnavailable, "must not be retryable: {description}");
            assert_ne!(api_error.code, S3ErrorCode::NoSuchKey, "must not report key-not-found: {description}");
        }
    }

    #[test]
    fn test_api_error_from_storage_error_mappings() {
        let test_cases = vec![
            (StorageError::NotImplemented, S3ErrorCode::NotImplemented),
            (
                StorageError::InvalidArgument("test".into(), "test".into(), "test".into()),
                S3ErrorCode::InvalidArgument,
            ),
            (StorageError::MethodNotAllowed, S3ErrorCode::MethodNotAllowed),
            (StorageError::BucketNotFound("test".into()), S3ErrorCode::NoSuchBucket),
            (StorageError::BucketNotEmpty("test".into()), S3ErrorCode::BucketNotEmpty),
            (
                StorageError::BucketNotEmptyWithDetails {
                    bucket: "test".into(),
                    details: "metadata-less residue".into(),
                },
                S3ErrorCode::BucketNotEmpty,
            ),
            (StorageError::BucketNameInvalid("test".into()), S3ErrorCode::InvalidBucketName),
            (
                StorageError::ObjectNameInvalid("test".into(), "test".into()),
                S3ErrorCode::InvalidArgument,
            ),
            (StorageError::BucketExists("test".into()), S3ErrorCode::BucketAlreadyOwnedByYou),
            (StorageError::DiskFull, S3ErrorCode::ServiceUnavailable),
            (StorageError::StorageFull, S3ErrorCode::ServiceUnavailable),
            (StorageError::UnsupportedDisk, S3ErrorCode::ServiceUnavailable),
            (StorageError::SlowDown, S3ErrorCode::SlowDown),
            (StorageError::FaultyDisk, S3ErrorCode::ServiceUnavailable),
            (StorageError::FaultyRemoteDisk, S3ErrorCode::ServiceUnavailable),
            (StorageError::DiskNotFound, S3ErrorCode::ServiceUnavailable),
            (StorageError::TooManyOpenFiles, S3ErrorCode::ServiceUnavailable),
            (StorageError::ErasureReadQuorum, S3ErrorCode::Custom(SLOW_DOWN_READ_CODE.into())),
            (
                StorageError::InsufficientReadQuorum("test".into(), "test".into()),
                S3ErrorCode::Custom(SLOW_DOWN_READ_CODE.into()),
            ),
            (StorageError::ErasureWriteQuorum, S3ErrorCode::ServiceUnavailable),
            (
                StorageError::InsufficientWriteQuorum("test".into(), "test".into()),
                S3ErrorCode::ServiceUnavailable,
            ),
            (
                StorageError::NamespaceLockQuorumUnavailable {
                    mode: "write",
                    bucket: "bucket".into(),
                    object: "object".into(),
                    required: 3,
                    achieved: 2,
                },
                S3ErrorCode::ServiceUnavailable,
            ),
            (StorageError::DecommissionNotStarted, S3ErrorCode::InvalidRequest),
            (StorageError::DecommissionAlreadyRunning, S3ErrorCode::InvalidRequest),
            (StorageError::RebalanceAlreadyRunning, S3ErrorCode::InvalidRequest),
            (StorageError::QuotaExceeded { current: 5, limit: 10 }, S3ErrorCode::InvalidRequest),
            (StorageError::PrefixAccessDenied("test".into(), "test".into()), S3ErrorCode::AccessDenied),
            (StorageError::ObjectNotFound("test".into(), "test".into()), S3ErrorCode::NoSuchKey),
            (StorageError::ConfigNotFound, S3ErrorCode::NoSuchKey),
            (StorageError::VolumeNotFound, S3ErrorCode::NoSuchBucket),
            (StorageError::FileNotFound, S3ErrorCode::NoSuchKey),
            (StorageError::FileVersionNotFound, S3ErrorCode::NoSuchVersion),
            (StorageError::NotModified, S3ErrorCode::NotModified),
            (StorageError::MalformedUploadID("test".into()), S3ErrorCode::InvalidArgument),
            (
                StorageError::InvalidUploadID("bucket".into(), "object".into(), "uploadid".into()),
                S3ErrorCode::NoSuchUpload,
            ),
        ];

        for (storage_error, expected_code) in test_cases {
            let api_error: ApiError = storage_error.into();
            assert_eq!(api_error.code, expected_code);
            assert!(api_error.source.is_some());
        }
    }

    #[test]
    fn disk_quorum_timeouts_map_to_retryable_service_unavailable() {
        use crate::storage_api::error::{DiskError, OBJECT_OP_IGNORED_ERRS, reduce_read_quorum_errs, reduce_write_quorum_errs};

        let errors = [
            Some(DiskError::Timeout),
            Some(DiskError::Timeout),
            Some(DiskError::Timeout),
            None,
        ];
        for reduced in [
            reduce_read_quorum_errs(&errors, OBJECT_OP_IGNORED_ERRS, 2),
            reduce_write_quorum_errs(&errors, OBJECT_OP_IGNORED_ERRS, 3),
        ] {
            let reduced = reduced.expect("quorum of timeouts must retain its typed cause");
            assert_eq!(reduced, DiskError::Timeout);
            let error = ApiError::from(StorageError::from(reduced));
            assert_eq!(error.code, S3ErrorCode::ServiceUnavailable);
            let response = S3Error::from(error);
            assert_eq!(response.status_code(), Some(http::StatusCode::SERVICE_UNAVAILABLE));
        }
        let degraded = [None, None, None, Some(DiskError::Timeout)];
        assert!(reduce_write_quorum_errs(&degraded, OBJECT_OP_IGNORED_ERRS, 3).is_none());
        for failure in [DiskError::FileCorrupt, DiskError::Io(IoError::other("unknown disk failure"))] {
            let errors = [Some(failure.clone()), Some(failure.clone()), Some(failure), None];
            let reduced =
                reduce_write_quorum_errs(&errors, OBJECT_OP_IGNORED_ERRS, 3).expect("permanent failures must remain errors");
            assert_eq!(ApiError::from(StorageError::from(reduced)).code, S3ErrorCode::InternalError);
        }
    }

    #[test]
    fn test_api_error_from_unexpected_eof_maps_to_incomplete_body() {
        let io_error = IoError::new(ErrorKind::UnexpectedEof, rustfs_rio::IncompleteBody { remaining: 7 });
        let api_error: ApiError = io_error.into();

        assert_eq!(api_error.code, S3ErrorCode::IncompleteBody);
        assert_eq!(api_error.message, ApiError::error_code_to_message(&S3ErrorCode::IncompleteBody));
        assert!(api_error.source.is_some());
    }

    #[test]
    fn test_api_error_from_nested_unexpected_eof_maps_to_incomplete_body() {
        let nested = IoError::new(
            ErrorKind::UnexpectedEof,
            IoError::new(ErrorKind::UnexpectedEof, rustfs_rio::IncompleteBody { remaining: 7 }),
        );
        let api_error: ApiError = nested.into();

        assert_eq!(api_error.code, S3ErrorCode::IncompleteBody);
        assert_eq!(api_error.message, ApiError::error_code_to_message(&S3ErrorCode::IncompleteBody));
        assert!(api_error.source.is_some());
    }

    #[test]
    fn upload_limit_marker_maps_to_entity_too_large_across_io_boundaries() {
        let direct: ApiError = IoError::other(UploadLimitExceeded { limit: 5 }).into();
        assert_eq!(direct.code, S3ErrorCode::EntityTooLarge);

        let storage: ApiError = StorageError::Io(IoError::other(IoError::other(UploadLimitExceeded { limit: 5 }))).into();
        assert_eq!(storage.code, S3ErrorCode::EntityTooLarge);
        assert_eq!(storage.message, ApiError::error_code_to_message(&S3ErrorCode::EntityTooLarge));
    }

    #[test]
    fn test_api_error_from_storage_io_copy_object_terminal_error_stays_internal() {
        let io_error = IoError::other(StorageError::FileCorrupt);
        let storage_error: StorageError = io_error.into();
        assert!(matches!(storage_error, StorageError::FileCorrupt));

        let api_error: ApiError = storage_error.into();

        assert_eq!(api_error.code, S3ErrorCode::InternalError);
        let source = api_error
            .source
            .as_deref()
            .and_then(|source| source.downcast_ref::<StorageError>())
            .expect("API error should retain the storage error source");
        assert!(matches!(source, StorageError::FileCorrupt));
    }

    #[test]
    fn test_api_error_from_iam_error() {
        let iam_error = rustfs_iam::error::Error::other("IAM test error");
        let api_error: ApiError = iam_error.into();

        assert_eq!(api_error.code, S3ErrorCode::InternalError);
        assert_eq!(api_error.message, ApiError::error_code_to_message(&S3ErrorCode::InternalError));
        let source = api_error
            .source
            .as_deref()
            .and_then(|source| source.downcast_ref::<StorageError>())
            .expect("API error should retain the storage error source");
        assert!(matches!(source, StorageError::Io(io_error) if io_error.to_string().contains("IAM test error")));
    }

    #[test]
    fn test_api_error_to_s3_error() {
        let api_error = ApiError {
            code: S3ErrorCode::NoSuchBucket,
            message: "Bucket not found".to_string(),
            source: Some(Box::new(IoError::new(ErrorKind::NotFound, "not found"))),
        };

        let s3_error: S3Error = api_error.into();
        assert_eq!(*s3_error.code(), S3ErrorCode::NoSuchBucket);
        assert!(s3_error.message().unwrap_or("").contains("Bucket not found"));
        assert!(s3_error.source().is_some());
    }

    #[test]
    fn test_namespace_lock_quorum_failure_maps_to_service_unavailable_status() {
        let api_error: ApiError = StorageError::NamespaceLockQuorumUnavailable {
            mode: "write",
            bucket: "bucket".into(),
            object: "object".into(),
            required: 3,
            achieved: 2,
        }
        .into();

        let s3_error: S3Error = api_error.into();

        assert_eq!(*s3_error.code(), S3ErrorCode::ServiceUnavailable);
        assert_eq!(s3_error.status_code(), Some(http::StatusCode::SERVICE_UNAVAILABLE));
    }

    #[test]
    fn read_quorum_failure_matches_minio_slow_down_read_response() {
        for error in [
            StorageError::ErasureReadQuorum,
            StorageError::InsufficientReadQuorum("bucket".into(), "object".into()),
        ] {
            let api_error = ApiError::from(error);
            assert_eq!(api_error.code, S3ErrorCode::Custom(SLOW_DOWN_READ_CODE.into()));
            assert_eq!(api_error.message, SLOW_DOWN_READ_MESSAGE);

            let s3_error: S3Error = api_error.into();
            assert_eq!(s3_error.status_code(), Some(StatusCode::SERVICE_UNAVAILABLE));
        }
    }

    #[test]
    fn quota_exceeded_preserves_existing_s3_error_contract() {
        let api_error: ApiError = StorageError::QuotaExceeded { current: 5, limit: 10 }.into();

        assert_eq!(api_error.code, S3ErrorCode::InvalidRequest);
        assert_eq!(api_error.message, "Bucket quota exceeded. Current usage: 5 bytes, limit: 10 bytes");
    }

    #[test]
    fn max_versions_exceeded_maps_to_minio_compatible_s3_error() {
        let api_error: ApiError = StorageError::MaxVersionsExceeded.into();

        assert_eq!(api_error.code, S3ErrorCode::Custom(MAX_VERSIONS_EXCEEDED_CODE.into()));
        assert_eq!(api_error.message, MAX_VERSIONS_EXCEEDED_MESSAGE);

        let s3_error: S3Error = api_error.into();
        assert_eq!(s3_error.code(), &S3ErrorCode::Custom(MAX_VERSIONS_EXCEEDED_CODE.into()));
        assert_eq!(s3_error.message(), Some(MAX_VERSIONS_EXCEEDED_MESSAGE));
        assert_eq!(s3_error.status_code(), Some(StatusCode::BAD_REQUEST));
    }

    #[test]
    fn test_api_error_to_s3_error_without_source() {
        let api_error = ApiError {
            code: S3ErrorCode::InvalidArgument,
            message: "Invalid argument".to_string(),
            source: None,
        };

        let s3_error: S3Error = api_error.into();
        assert_eq!(*s3_error.code(), S3ErrorCode::InvalidArgument);
        assert!(s3_error.message().unwrap_or("").contains("Invalid argument"));
    }

    #[test]
    fn test_api_error_display() {
        let api_error = ApiError {
            code: S3ErrorCode::InternalError,
            message: "Test error message".to_string(),
            source: None,
        };

        assert_eq!(api_error.to_string(), "Test error message");
    }

    #[test]
    fn test_api_error_debug() {
        let api_error = ApiError {
            code: S3ErrorCode::NoSuchKey,
            message: "Object not found".to_string(),
            source: Some(Box::new(IoError::new(ErrorKind::NotFound, "file not found"))),
        };

        let debug_str = format!("{api_error:?}");
        assert!(debug_str.contains("NoSuchKey"));
        assert!(debug_str.contains("Object not found"));
    }

    #[test]
    fn test_api_error_roundtrip_through_io_error() {
        let original_io_error = IoError::new(ErrorKind::PermissionDenied, "original permission error");

        // Convert to ApiError
        let api_error: ApiError = original_io_error.into();

        // Verify the conversion preserved the information
        assert_eq!(api_error.code, S3ErrorCode::InternalError);
        assert!(api_error.message.contains("original permission error"));
        assert!(api_error.source.is_some());

        // Test that we can downcast back to the original io::Error
        let source = api_error.source.as_ref().unwrap();
        let downcast_io_error = source.downcast_ref::<IoError>();
        assert!(downcast_io_error.is_some());
        assert_eq!(downcast_io_error.unwrap().kind(), ErrorKind::PermissionDenied);
        assert!(downcast_io_error.unwrap().to_string().contains("original permission error"));
    }

    #[test]
    fn test_api_error_chain_conversion() {
        // Start with an io::Error
        let io_error = IoError::new(ErrorKind::InvalidData, "invalid data");

        // Convert to StorageError (simulating what happens in the codebase)
        let storage_error = StorageError::other(io_error);

        // Convert to ApiError
        let api_error: ApiError = storage_error.into();

        // Verify the chain is preserved
        assert!(api_error.source.is_some());

        // Check that we can still access the original error information
        let source = api_error.source.as_ref().unwrap();
        let downcast_storage_error = source.downcast_ref::<StorageError>();
        assert!(downcast_storage_error.is_some());
    }

    #[test]
    fn test_api_error_error_trait_implementation() {
        let api_error = ApiError {
            code: S3ErrorCode::InternalError,
            message: "Test error".to_string(),
            source: Some(Box::new(IoError::other("source error"))),
        };

        // Test that it implements std::error::Error
        let error: &dyn std::error::Error = &api_error;
        assert_eq!(error.to_string(), "Test error");
        let source = error
            .source()
            .expect("typed source must remain reachable through the error trait");
        let source = source.downcast_ref::<IoError>().expect("original I/O error");
        assert_eq!(source.kind(), ErrorKind::Other);
        assert_eq!(source.to_string(), "source error");
        assert!(std::error::Error::source(&ApiError::access_denied()).is_none());
    }
}
