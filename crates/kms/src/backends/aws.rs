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

//! AWS KMS backend.
//!
//! AWS is the cryptographic source of truth: key material never leaves KMS,
//! and AWS owns key state, backing-key rotation, and the pending-deletion
//! window. This backend is a thin mapping onto the native API — no key state
//! is mirrored locally, so there is no metadata store to keep consistent.
//!
//! Deliberate deviations from the RustFS backend contract, all forced by AWS
//! semantics and reflected in [`AwsKmsBackend::capabilities`]:
//!
//! - No physical deletion. `ScheduleKeyDeletion` is the only removal path AWS
//!   offers, and AWS itself destroys the material when the window elapses, so
//!   `physical_delete` is false and [`KmsBackend::remove_expired_key`] never
//!   destroys anything — it only reports what AWS has already done.
//! - AWS rejects *decryption* with `Disabled` and `PendingDeletion` keys,
//!   whereas every RustFS-managed backend keeps it working (see
//!   `backends::ensure_key_state_permits`). Objects encrypted under a key that
//!   is later disabled in AWS therefore become unreadable until it is
//!   re-enabled. The shared contract-test driver is not applicable here for
//!   exactly this reason.
//! - `CancelKeyDeletion` leaves the key `Disabled` in AWS, not `Enabled`.
//! - Key identifiers are assigned by AWS. Every other backend treats
//!   `CreateKeyRequest::key_name` as the identifier the key will answer to, and
//!   alias management is out of scope here, so a named create is *refused*
//!   rather than silently creating a key the caller cannot address. Honouring
//!   it silently would make each caller-by-name flow — SSE-S3 auto-creation
//!   and the synthetic probe both describe-then-create — recreate an
//!   unreachable key on every attempt. Consequently SSE-S3 key auto-creation
//!   and the probe are unavailable on this backend: keys must be pre-created
//!   and referenced by AWS key id or ARN.
//! - Versions are opaque: AWS addresses backing keys internally and decrypts
//!   with the right one automatically, so `key_version` is reported as 1.
//!
//! Credentials come from the standard `aws-config` provider chain
//! (environment, shared profile, container/IMDS role). This backend never
//! stores or refreshes AWS credential material itself.
//!
//! Every call goes through [`crate::policy::execute`], which owns the
//! per-attempt timeout, the total operation deadline, and the retry decision;
//! the SDK's own retry loop is disabled so the configured budget is not
//! multiplied. Throttled reads are replayed, while mutations (create, rotate,
//! schedule/cancel deletion, enable/disable) are executed at most once because
//! a lost response could otherwise be replayed into a second side effect.

use std::collections::HashMap;
use std::future::Future;

use async_trait::async_trait;
use aws_sdk_kms::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_kms::types::{DataKeySpec, KeySpec as AwsKeySpec, KeyState as AwsKeyState, KeyUsageType, Tag};
use aws_smithy_runtime_api::client::orchestrator::HttpResponse;
use aws_smithy_types::Blob;
use jiff::Zoned;
use tokio_util::sync::CancellationToken;

use super::{BackendCapabilities, ExpiredKeyRemoval, KmsBackend, empty_key_page, list_keys_page_size};
use crate::config::{BackendConfig, KmsConfig};
use crate::error::{KmsError, Result};
use crate::policy::{self, AttemptError, ErrorClass, OpClass, RetryPolicy, classify_status};
use crate::types::*;

/// Backend name reported in `UnsupportedCapability` errors.
const BACKEND_NAME: &str = "aws";

/// Reported as `KeyMetadata::key_manager`: the material is managed by AWS KMS.
const KEY_MANAGER: &str = "AWS_KMS";

/// AWS accepts a pending deletion window of 7 to 30 days.
const MIN_PENDING_WINDOW_DAYS: u32 = 7;
const MAX_PENDING_WINDOW_DAYS: u32 = 30;

/// Default window applied when the caller does not pick one, matching AWS's
/// own default.
const DEFAULT_PENDING_WINDOW_DAYS: u32 = 30;

/// AWS error codes describing a transient, server-side condition that a retry
/// can clear. Everything else — access denial, invalid state, not found,
/// malformed input — is deterministic and must not be replayed.
fn is_retryable_error_code(code: &str) -> bool {
    matches!(
        code,
        "ThrottlingException"
            | "ThrottledException"
            | "TooManyRequestsException"
            | "RequestThrottled"
            | "SlowDown"
            | "KMSInternalException"
            | "DependencyTimeoutException"
            | "KeyUnavailableException"
            | "InternalFailure"
            | "ServiceUnavailable"
    )
}

/// Classify an AWS SDK failure for the operation policy's retry decision.
///
/// Transport-level failures are connection-class: the request may or may not
/// have reached AWS, which is why non-idempotent mutations are executed once
/// regardless. Service errors are classified by AWS error code first and by
/// HTTP status second, so a throttling response retries even when the service
/// returns it as an unmodeled error.
fn classify_sdk_error<E: ProvideErrorMetadata>(error: &SdkError<E, HttpResponse>) -> ErrorClass {
    match error {
        SdkError::ConstructionFailure(_) => ErrorClass::Fatal,
        SdkError::TimeoutError(_) | SdkError::DispatchFailure(_) | SdkError::ResponseError(_) => ErrorClass::RetryableConn,
        SdkError::ServiceError(context) => {
            if context.err().code().is_some_and(is_retryable_error_code) {
                return ErrorClass::RetryableStatus;
            }
            classify_status(context.raw().status().as_u16())
        }
        // `SdkError` is non-exhaustive: an unrecognized failure mode is treated
        // as deterministic rather than replayed blind.
        _ => ErrorClass::Fatal,
    }
}

/// Human-readable failure detail that never carries key material: only the AWS
/// error code and its own message are surfaced.
fn error_detail<E: ProvideErrorMetadata>(error: &SdkError<E, HttpResponse>) -> String {
    match error {
        SdkError::ServiceError(context) => format!(
            "{} ({})",
            context.err().message().unwrap_or("no message"),
            context.err().code().unwrap_or("unknown code")
        ),
        SdkError::TimeoutError(_) => "the request timed out".to_string(),
        SdkError::DispatchFailure(_) => "the request could not be dispatched".to_string(),
        SdkError::ResponseError(_) => "the response could not be parsed".to_string(),
        SdkError::ConstructionFailure(_) => "the request could not be constructed".to_string(),
        _ => "an unrecognized failure occurred".to_string(),
    }
}

/// Map an AWS SDK failure onto the typed KMS error surface.
///
/// `key_id` names the key the operation addressed, when it had one, so
/// not-found stays reportable as [`KmsError::KeyNotFound`] with the identifier
/// the caller passed.
fn map_sdk_error<E: ProvideErrorMetadata>(operation: &str, key_id: Option<&str>, error: SdkError<E, HttpResponse>) -> KmsError {
    let detail = error_detail(&error);
    let subject = key_id.map(|id| format!(" for key {id}")).unwrap_or_default();
    let code = match &error {
        SdkError::ServiceError(context) => context.err().code().unwrap_or_default().to_string(),
        SdkError::TimeoutError(_) => {
            return KmsError::operation_timed_out(format!("AWS KMS {operation}{subject} timed out"));
        }
        SdkError::ConstructionFailure(_) => {
            return KmsError::configuration_error(format!(
                "AWS KMS {operation}{subject} could not be built; check the configured region and credential chain: {detail}"
            ));
        }
        _ => String::new(),
    };

    match code.as_str() {
        "NotFoundException" => KmsError::key_not_found(key_id.unwrap_or("unknown")),
        "AlreadyExistsException" => KmsError::key_already_exists(key_id.unwrap_or("unknown")),
        "AccessDeniedException" => KmsError::access_denied(format!("AWS KMS denied {operation}{subject}: {detail}")),
        // Credential problems surfaced by SigV4/STS: fail closed instead of
        // reporting a generic backend error, so operators see the real cause.
        "UnrecognizedClientException" | "InvalidClientTokenId" | "ExpiredTokenException" | "InvalidSignatureException" => {
            KmsError::credentials_unavailable(format!(
                "AWS KMS rejected the request credentials for {operation}{subject}: {detail}"
            ))
        }
        "KMSInvalidStateException" | "DisabledException" | "InvalidKeyUsageException" | "InvalidGrantTokenException" => {
            KmsError::invalid_key_state(format!("AWS KMS rejected {operation}{subject}: {detail}"))
        }
        "InvalidCiphertextException" | "IncorrectKeyException" | "IncorrectKeyMaterialException" => {
            KmsError::cryptographic_error(operation.to_string(), detail)
        }
        // Malformed input — most often a key identifier that is neither an AWS
        // key id, an ARN, nor an alias. Reported as a parameter problem so
        // callers stop rather than treating it as a backend outage to retry.
        "ValidationException" | "InvalidArnException" | "InvalidMarkerException" => {
            KmsError::invalid_parameter(format!("AWS KMS rejected {operation}{subject} as invalid: {detail}"))
        }
        _ => KmsError::backend_error(format!("AWS KMS {operation}{subject} failed: {detail}")),
    }
}

/// Translate an AWS key state onto the shared state machine.
///
/// States this build does not recognize — including `Creating`, `Updating`,
/// and any state added to AWS later — map to `Unavailable`, which every state
/// gate rejects, so an unknown state fails closed rather than being treated as
/// usable.
fn map_key_state(state: Option<&AwsKeyState>) -> KeyState {
    match state {
        Some(AwsKeyState::Enabled) => KeyState::Enabled,
        Some(AwsKeyState::Disabled) => KeyState::Disabled,
        Some(AwsKeyState::PendingDeletion | AwsKeyState::PendingReplicaDeletion) => KeyState::PendingDeletion,
        Some(AwsKeyState::PendingImport) => KeyState::PendingImport,
        _ => KeyState::Unavailable,
    }
}

/// Project a key state onto the coarser [`KeyStatus`] used by key listings.
fn key_status_of(state: &KeyState) -> KeyStatus {
    match state {
        KeyState::Enabled => KeyStatus::Active,
        KeyState::PendingDeletion => KeyStatus::PendingDeletion,
        // `Unavailable`/`PendingImport` keys are not deleted, only unusable;
        // reporting them as `Deleted` would invite the deletion sweep to act
        // on keys AWS still owns.
        KeyState::Disabled | KeyState::PendingImport | KeyState::Unavailable => KeyStatus::Disabled,
    }
}

/// AWS models `GENERATE_VERIFY_MAC` and `KEY_AGREEMENT` usages that RustFS has
/// no equivalent for; only `SIGN_VERIFY` maps across, everything else is
/// reported as encrypt/decrypt.
fn map_key_usage(usage: Option<&KeyUsageType>) -> KeyUsage {
    match usage {
        Some(KeyUsageType::SignVerify) => KeyUsage::SignVerify,
        _ => KeyUsage::EncryptDecrypt,
    }
}

fn to_zoned(value: Option<&aws_smithy_types::DateTime>) -> Option<Zoned> {
    let value = value?;
    let nanos = i32::try_from(value.subsec_nanos()).ok()?;
    jiff::Timestamp::new(value.secs(), nanos)
        .ok()
        .map(|timestamp| timestamp.to_zoned(jiff::tz::TimeZone::UTC))
}

/// `None` for an empty context so the request omits the field entirely rather
/// than sending an empty map, which AWS treats as a distinct context.
fn encryption_context(context: &HashMap<String, String>) -> Option<HashMap<String, String>> {
    (!context.is_empty()).then(|| context.clone())
}

fn grant_tokens(tokens: &[String]) -> Option<Vec<String>> {
    (!tokens.is_empty()).then(|| tokens.to_vec())
}

/// Build the RustFS key metadata view of an AWS key.
///
/// `tags` are supplied by the caller: reading them back requires a separate
/// `ListResourceTags` call (and the matching IAM permission), so describe
/// paths report an empty tag set rather than paying for it.
fn key_metadata_from_aws(metadata: &aws_sdk_kms::types::KeyMetadata, tags: HashMap<String, String>) -> Result<KeyMetadata> {
    let creation_date = to_zoned(metadata.creation_date()).ok_or_else(|| {
        KmsError::backend_error(format!("AWS KMS returned key {} without a usable creation date", metadata.key_id()))
    })?;

    Ok(KeyMetadata {
        key_id: metadata.key_id().to_string(),
        key_state: map_key_state(metadata.key_state()),
        key_usage: map_key_usage(metadata.key_usage()),
        description: metadata.description().map(str::to_string),
        creation_date,
        deletion_date: to_zoned(metadata.deletion_date()),
        origin: metadata
            .origin()
            .map(|origin| origin.as_str().to_string())
            .unwrap_or_else(|| KEY_MANAGER.to_string()),
        key_manager: KEY_MANAGER.to_string(),
        tags,
    })
}

/// KMS backend backed by AWS KMS.
pub struct AwsKmsBackend {
    client: aws_sdk_kms::Client,
    /// Budgets wrapping every outbound AWS call (see [`crate::policy`]).
    retry: RetryPolicy,
    /// Cancellation point for the operation executor: aborts in-flight
    /// attempts and backoff sleeps. Owned by the backend and currently never
    /// triggered — shutdown drops the whole backend — but kept as the single
    /// hook a future lifecycle owner can cancel through.
    cancel: CancellationToken,
}

impl AwsKmsBackend {
    /// Build the backend from a KMS configuration.
    ///
    /// Resolving credentials and the region is delegated to `aws-config`. A
    /// configuration that cannot produce a region fails here rather than at
    /// the first cryptographic operation.
    pub async fn new(config: KmsConfig) -> Result<Self> {
        config.validate()?;

        let aws_backend_config = match &config.backend_config {
            BackendConfig::Aws(aws_config) => (**aws_config).clone(),
            BackendConfig::Local(_) | BackendConfig::VaultKv2(_) | BackendConfig::VaultTransit(_) | BackendConfig::Static(_) => {
                return Err(KmsError::configuration_error("Expected AWS KMS backend configuration"));
            }
        };

        let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
        if let Some(region) = &aws_backend_config.region {
            loader = loader.region(aws_sdk_kms::config::Region::new(region.clone()));
        }
        let sdk_config = loader.load().await;
        let Some(region) = sdk_config.region() else {
            return Err(KmsError::configuration_error(
                "AWS KMS backend could not resolve a region; set the backend region or AWS_REGION",
            ));
        };

        let mut builder = aws_sdk_kms::config::Builder::from(&sdk_config)
            // `crate::policy` owns retries and timeouts; leaving the SDK's own
            // retry loop enabled would multiply the configured attempt budget
            // and replay mutations the policy deliberately runs once.
            .retry_config(aws_sdk_kms::config::retry::RetryConfig::disabled());
        if let Some(endpoint_url) = &aws_backend_config.endpoint_url {
            builder = builder.endpoint_url(endpoint_url);
        }

        Ok(Self::with_client(
            aws_sdk_kms::Client::from_conf(builder.build()),
            &config,
            aws_backend_config.endpoint_url.as_deref().unwrap_or_default(),
            region.as_ref(),
        ))
    }

    fn with_client(client: aws_sdk_kms::Client, config: &KmsConfig, endpoint: &str, region: &str) -> Self {
        Self {
            client,
            retry: RetryPolicy::for_backend(config, "aws", endpoint, Some(region), "operations"),
            cancel: CancellationToken::new(),
        }
    }

    /// Run one AWS call under the operation policy.
    async fn run<T, F, Fut>(&self, operation: &'static str, class: OpClass, attempt: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = std::result::Result<T, AttemptError>>,
    {
        policy::execute(operation, class, &self.retry, &self.cancel, attempt).await
    }

    /// Fetch a key's metadata from AWS. Tags are not read back; see
    /// [`key_metadata_from_aws`].
    async fn describe(&self, key_id: &str) -> Result<KeyMetadata> {
        let output = self
            .run("aws_kms_describe_key", OpClass::ReadIdempotent, move || async move {
                self.client
                    .describe_key()
                    .key_id(key_id)
                    .send()
                    .await
                    .map_err(|error| AttemptError {
                        class: classify_sdk_error(&error),
                        error: map_sdk_error("describe_key", Some(key_id), error),
                    })
            })
            .await?;

        let metadata = output
            .key_metadata()
            .ok_or_else(|| KmsError::backend_error(format!("AWS KMS DescribeKey returned no metadata for key {key_id}")))?;
        key_metadata_from_aws(metadata, HashMap::new())
    }
}

#[async_trait]
impl KmsBackend for AwsKmsBackend {
    async fn create_key(&self, request: CreateKeyRequest) -> Result<CreateKeyResponse> {
        if request.key_usage != KeyUsage::EncryptDecrypt {
            return Err(KmsError::unsupported_capability(BACKEND_NAME, "create_key with SIGN_VERIFY usage"));
        }
        // AWS assigns the identifier itself and this backend does not manage
        // aliases, so a key created here can never answer to the requested
        // name. Reporting the gap keeps describe-then-create callers from
        // creating a fresh, unreachable key on every attempt.
        if request.key_name.is_some() {
            return Err(KmsError::unsupported_capability(
                BACKEND_NAME,
                "create_key with a caller-assigned key name; AWS assigns key identifiers",
            ));
        }

        let description = request.description.clone();
        // Sorted so the request AWS receives is deterministic for a given tag set.
        let mut tag_pairs: Vec<_> = request.tags.iter().collect();
        tag_pairs.sort_by_key(|(key, _)| *key);
        let tags = tag_pairs
            .into_iter()
            .map(|(key, value)| Tag::builder().tag_key(key).tag_value(value).build())
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| KmsError::invalid_parameter(format!("AWS KMS rejected the requested key tags: {error}")))?;

        // Single attempt: CreateKey has no idempotency key, so replaying a lost
        // response would leave an orphaned second key behind.
        let output = self
            .run("aws_kms_create_key", OpClass::MutatingNonIdempotent, || {
                let description = description.clone();
                let tags = tags.clone();
                async move {
                    let mut builder = self
                        .client
                        .create_key()
                        .key_usage(KeyUsageType::EncryptDecrypt)
                        .key_spec(AwsKeySpec::SymmetricDefault);
                    if let Some(description) = description {
                        builder = builder.description(description);
                    }
                    if !tags.is_empty() {
                        builder = builder.set_tags(Some(tags));
                    }
                    builder.send().await.map_err(|error| AttemptError {
                        class: classify_sdk_error(&error),
                        error: map_sdk_error("create_key", None, error),
                    })
                }
            })
            .await?;

        let metadata = output
            .key_metadata()
            .ok_or_else(|| KmsError::backend_error("AWS KMS CreateKey returned no key metadata"))?;
        let key_metadata = key_metadata_from_aws(metadata, request.tags)?;
        Ok(CreateKeyResponse {
            key_id: key_metadata.key_id.clone(),
            key_metadata,
        })
    }

    async fn encrypt(&self, request: EncryptRequest) -> Result<EncryptResponse> {
        let key_id = request.key_id.as_str();
        let plaintext = request.plaintext.as_slice();
        let context = encryption_context(&request.encryption_context);
        let tokens = grant_tokens(&request.grant_tokens);

        let output = self
            .run("aws_kms_encrypt", OpClass::ReadIdempotent, || {
                let context = context.clone();
                let tokens = tokens.clone();
                async move {
                    self.client
                        .encrypt()
                        .key_id(key_id)
                        .plaintext(Blob::new(plaintext.to_vec()))
                        .set_encryption_context(context)
                        .set_grant_tokens(tokens)
                        .send()
                        .await
                        .map_err(|error| AttemptError {
                            class: classify_sdk_error(&error),
                            error: map_sdk_error("encrypt", Some(key_id), error),
                        })
                }
            })
            .await?;

        let ciphertext = output
            .ciphertext_blob
            .ok_or_else(|| KmsError::backend_error(format!("AWS KMS Encrypt returned no ciphertext for key {key_id}")))?;
        Ok(EncryptResponse {
            ciphertext: ciphertext.into_inner(),
            key_id: output.key_id.unwrap_or_else(|| request.key_id.clone()),
            // AWS addresses backing keys internally and never exposes a
            // version to the caller.
            key_version: 1,
            algorithm: output
                .encryption_algorithm
                .map(|algorithm| algorithm.as_str().to_string())
                .unwrap_or_else(|| AwsKeySpec::SymmetricDefault.as_str().to_string()),
        })
    }

    async fn decrypt(&self, request: DecryptRequest) -> Result<DecryptResponse> {
        // AWS ciphertext blobs identify their own key, so no key id is needed
        // here — and AWS refuses to decrypt with a disabled or pending-deletion
        // key, unlike the RustFS-managed backends.
        let ciphertext = request.ciphertext.as_slice();
        let context = encryption_context(&request.encryption_context);
        let tokens = grant_tokens(&request.grant_tokens);

        let output = self
            .run("aws_kms_decrypt", OpClass::ReadIdempotent, || {
                let context = context.clone();
                let tokens = tokens.clone();
                async move {
                    self.client
                        .decrypt()
                        .ciphertext_blob(Blob::new(ciphertext.to_vec()))
                        .set_encryption_context(context)
                        .set_grant_tokens(tokens)
                        .send()
                        .await
                        .map_err(|error| AttemptError {
                            class: classify_sdk_error(&error),
                            error: map_sdk_error("decrypt", None, error),
                        })
                }
            })
            .await?;

        let plaintext = output
            .plaintext
            .ok_or_else(|| KmsError::backend_error("AWS KMS Decrypt returned no plaintext"))?;
        Ok(DecryptResponse {
            plaintext: plaintext.into_inner(),
            key_id: output.key_id.unwrap_or_default(),
            encryption_algorithm: output.encryption_algorithm.map(|algorithm| algorithm.as_str().to_string()),
        })
    }

    async fn generate_data_key(&self, request: GenerateDataKeyRequest) -> Result<GenerateDataKeyResponse> {
        let key_id = request.key_id.as_str();
        let context = encryption_context(&request.encryption_context);
        // AWS names the AES specs directly; anything else is requested by
        // length so the caller still gets material of the size it asked for.
        let key_spec = match request.key_spec {
            KeySpec::Aes256 => Some(DataKeySpec::Aes256),
            KeySpec::Aes128 => Some(DataKeySpec::Aes128),
            KeySpec::ChaCha20 => None,
        };
        let number_of_bytes = match key_spec {
            Some(_) => None,
            None => Some(
                i32::try_from(request.key_spec.key_size())
                    .map_err(|_| KmsError::invalid_parameter("Requested data key size exceeds the AWS KMS limit"))?,
            ),
        };

        let output = self
            .run("aws_kms_generate_data_key", OpClass::ReadIdempotent, || {
                let context = context.clone();
                let key_spec = key_spec.clone();
                async move {
                    self.client
                        .generate_data_key()
                        .key_id(key_id)
                        .set_key_spec(key_spec)
                        .set_number_of_bytes(number_of_bytes)
                        .set_encryption_context(context)
                        .send()
                        .await
                        .map_err(|error| AttemptError {
                            class: classify_sdk_error(&error),
                            error: map_sdk_error("generate_data_key", Some(key_id), error),
                        })
                }
            })
            .await?;

        let plaintext = output
            .plaintext
            .ok_or_else(|| KmsError::backend_error(format!("AWS KMS GenerateDataKey returned no plaintext for key {key_id}")))?;
        let ciphertext = output
            .ciphertext_blob
            .ok_or_else(|| KmsError::backend_error(format!("AWS KMS GenerateDataKey returned no ciphertext for key {key_id}")))?;

        Ok(GenerateDataKeyResponse {
            key_id: output.key_id.unwrap_or_else(|| request.key_id.clone()),
            plaintext_key: plaintext.into_inner(),
            ciphertext_blob: ciphertext.into_inner(),
        })
    }

    async fn describe_key(&self, request: DescribeKeyRequest) -> Result<DescribeKeyResponse> {
        Ok(DescribeKeyResponse {
            key_metadata: self.describe(&request.key_id).await?,
        })
    }

    /// List keys, describing each entry.
    ///
    /// AWS `ListKeys` returns identifiers only, so filling in the state, usage
    /// and creation date every caller relies on costs one `DescribeKey` per
    /// listed key. The page size is bounded by the caller's `limit`.
    async fn list_keys(&self, request: ListKeysRequest) -> Result<ListKeysResponse> {
        // AWS rejects a `Limit` of zero, and clamping it up to one would return
        // a key to a caller that asked for none; the empty page is answered
        // here instead.
        let Some(page_size) = list_keys_page_size(request.limit) else {
            return Ok(empty_key_page());
        };

        // Taking the remote page size from the shared resolver keeps the AWS
        // request under the same ceiling every other backend obeys; the AWS API
        // maximum is the same 1000, so this never widens the remote page.
        let limit = request
            .limit
            .map(|_| i32::try_from(page_size).unwrap_or(i32::MAX).clamp(1, 1000));
        let marker = request.marker.clone();

        let output = self
            .run("aws_kms_list_keys", OpClass::ReadIdempotent, || {
                let marker = marker.clone();
                async move {
                    self.client
                        .list_keys()
                        .set_limit(limit)
                        .set_marker(marker)
                        .send()
                        .await
                        .map_err(|error| AttemptError {
                            class: classify_sdk_error(&error),
                            error: map_sdk_error("list_keys", None, error),
                        })
                }
            })
            .await?;

        let mut keys = Vec::new();
        for entry in output.keys() {
            let Some(key_id) = entry.key_id() else {
                continue;
            };
            let metadata = match self.describe(key_id).await {
                Ok(metadata) => metadata,
                // AWS `ListKeys` is eventually consistent, so a key destroyed
                // between the listing and the describe is routine: it is
                // dropped and the remote cursor still advances past it. There
                // is no local record to be damaged here — key state lives in
                // AWS — so `unreadable_key_ids` stays empty on this backend and
                // every other failure fails the listing.
                Err(KmsError::KeyNotFound { .. }) => continue,
                Err(error) => return Err(error),
            };
            if request
                .usage_filter
                .as_ref()
                .is_some_and(|filter| *filter != metadata.key_usage)
            {
                continue;
            }
            let status = key_status_of(&metadata.key_state);
            if request.status_filter.as_ref().is_some_and(|filter| *filter != status) {
                continue;
            }
            keys.push(KeyInfo {
                key_id: metadata.key_id,
                description: metadata.description,
                algorithm: AwsKeySpec::SymmetricDefault.as_str().to_string(),
                usage: metadata.key_usage,
                status,
                // AWS never exposes a backing-key version to the caller.
                version: 1,
                metadata: HashMap::new(),
                tags: metadata.tags,
                created_at: metadata.creation_date,
                rotated_at: None,
                created_by: None,
                rotation_due: false,
                rotation_due_reason: None,
                wrap_budget_reserved: None,
            });
        }

        Ok(ListKeysResponse {
            keys,
            next_marker: output.next_marker.clone(),
            truncated: output.truncated,
            // AWS owns key state; nothing here can be present-but-unreadable.
            unreadable_key_ids: Vec::new(),
        })
    }

    /// Schedule deletion through the native `ScheduleKeyDeletion`.
    ///
    /// AWS owns the window and destroys the material itself when it elapses;
    /// there is no immediate-deletion path, so `force_immediate` is rejected.
    async fn delete_key(&self, request: DeleteKeyRequest) -> Result<DeleteKeyResponse> {
        if request.force_immediate.unwrap_or(false) {
            return Err(KmsError::unsupported_capability(BACKEND_NAME, "immediate key deletion"));
        }

        let days = request.pending_window_in_days.unwrap_or(DEFAULT_PENDING_WINDOW_DAYS);
        if !(MIN_PENDING_WINDOW_DAYS..=MAX_PENDING_WINDOW_DAYS).contains(&days) {
            return Err(KmsError::invalid_parameter(format!(
                "pending_window_in_days must be between {MIN_PENDING_WINDOW_DAYS} and {MAX_PENDING_WINDOW_DAYS}"
            )));
        }
        let window = i32::try_from(days).map_err(|_| KmsError::invalid_parameter("pending_window_in_days is out of range"))?;

        let key_id = request.key_id.as_str();
        // Single attempt: AWS rejects a repeated schedule with an invalid-state
        // error, so a replayed lost response would surface as a spurious
        // failure rather than a completed deletion.
        let output = self
            .run("aws_kms_schedule_key_deletion", OpClass::MutatingNonIdempotent, move || async move {
                self.client
                    .schedule_key_deletion()
                    .key_id(key_id)
                    .pending_window_in_days(window)
                    .send()
                    .await
                    .map_err(|error| AttemptError {
                        class: classify_sdk_error(&error),
                        error: map_sdk_error("schedule_key_deletion", Some(key_id), error),
                    })
            })
            .await?;

        let deletion_date = to_zoned(output.deletion_date()).map(|date| date.to_string());
        Ok(DeleteKeyResponse {
            key_id: request.key_id.clone(),
            deletion_date,
            key_metadata: self.describe(&request.key_id).await?,
        })
    }

    /// Cancel a scheduled deletion.
    ///
    /// AWS leaves the key `Disabled` afterwards; callers that need it usable
    /// again must enable it explicitly.
    async fn cancel_key_deletion(&self, request: CancelKeyDeletionRequest) -> Result<CancelKeyDeletionResponse> {
        let key_id = request.key_id.as_str();
        self.run("aws_kms_cancel_key_deletion", OpClass::MutatingNonIdempotent, move || async move {
            self.client
                .cancel_key_deletion()
                .key_id(key_id)
                .send()
                .await
                .map_err(|error| AttemptError {
                    class: classify_sdk_error(&error),
                    error: map_sdk_error("cancel_key_deletion", Some(key_id), error),
                })
        })
        .await?;

        Ok(CancelKeyDeletionResponse {
            key_id: request.key_id.clone(),
            key_metadata: self.describe(&request.key_id).await?,
        })
    }

    async fn enable_key(&self, key_id: &str) -> Result<()> {
        self.run("aws_kms_enable_key", OpClass::MutatingNonIdempotent, move || async move {
            self.client
                .enable_key()
                .key_id(key_id)
                .send()
                .await
                .map(|_| ())
                .map_err(|error| AttemptError {
                    class: classify_sdk_error(&error),
                    error: map_sdk_error("enable_key", Some(key_id), error),
                })
        })
        .await
    }

    async fn disable_key(&self, key_id: &str) -> Result<()> {
        self.run("aws_kms_disable_key", OpClass::MutatingNonIdempotent, move || async move {
            self.client
                .disable_key()
                .key_id(key_id)
                .send()
                .await
                .map(|_| ())
                .map_err(|error| AttemptError {
                    class: classify_sdk_error(&error),
                    error: map_sdk_error("disable_key", Some(key_id), error),
                })
        })
        .await
    }

    /// Rotate the key's backing material through `RotateKeyOnDemand`.
    ///
    /// This is AWS's *manual* rotation: it creates a new backing key while
    /// every prior one stays available for decryption, which is exactly the
    /// version-retaining rotation the trait requires. AWS's automatic
    /// (yearly) rotation is a separate, independently configured mechanism
    /// that this backend neither enables nor reports on.
    async fn rotate_key(&self, key_id: &str) -> Result<()> {
        self.run("aws_kms_rotate_key_on_demand", OpClass::MutatingNonIdempotent, move || async move {
            self.client
                .rotate_key_on_demand()
                .key_id(key_id)
                .send()
                .await
                .map(|_| ())
                .map_err(|error| AttemptError {
                    class: classify_sdk_error(&error),
                    error: map_sdk_error("rotate_key_on_demand", Some(key_id), error),
                })
        })
        .await
    }

    async fn health_check(&self) -> Result<bool> {
        self.run("aws_kms_health_check", OpClass::ReadIdempotent, move || async move {
            self.client
                .list_keys()
                .limit(1)
                .send()
                .await
                .map(|_| true)
                .map_err(|error| AttemptError {
                    class: classify_sdk_error(&error),
                    error: map_sdk_error("health_check", None, error),
                })
        })
        .await
    }

    fn capabilities(&self) -> BackendCapabilities {
        // AWS supports on-demand rotation with retained backing keys,
        // enable/disable, and a 7-30 day deletion window. It offers no
        // immediate physical deletion: material is destroyed by AWS when the
        // window elapses, never by RustFS.
        BackendCapabilities::minimal()
            .with_rotate(true)
            .with_enable_disable(true)
            .with_schedule_deletion(true)
            .with_versioning(true)
            .with_physical_delete(false)
    }

    /// Observe, never destroy.
    ///
    /// AWS runs the deletion window itself, so the sweep's job here is only to
    /// report what AWS has already done: a key that is gone counts as removed,
    /// a key that left `PendingDeletion` counts as a state change, and a key
    /// still pending is never expired by RustFS regardless of the deadline.
    async fn remove_expired_key(&self, key_id: &str, _now: &Zoned) -> Result<ExpiredKeyRemoval> {
        match self.describe(key_id).await {
            Ok(metadata) => Ok(match metadata.key_state {
                KeyState::PendingDeletion => ExpiredKeyRemoval::NotExpired,
                _ => ExpiredKeyRemoval::StateChanged,
            }),
            Err(KmsError::KeyNotFound { .. }) => Ok(ExpiredKeyRemoval::Removed),
            Err(error) => Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_kms::config::{BehaviorVersion, Credentials, Region};
    use aws_smithy_http_client::test_util::{NeverClient, ReplayEvent, StaticReplayClient};
    use aws_smithy_types::body::SdkBody;
    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD as BASE64;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// AWS KMS speaks awsJson1_1; every request goes to `/` on the regional
    /// endpoint, so the replayed request side carries no useful assertion.
    fn any_request() -> http::Request<SdkBody> {
        http::Request::builder()
            .method("POST")
            .uri("https://kms.us-east-1.amazonaws.com/")
            .body(SdkBody::from("{}"))
            .expect("replay request should build")
    }

    fn response(status: u16, body: String) -> http::Response<SdkBody> {
        http::Response::builder()
            .status(status)
            .header("content-type", "application/x-amz-json-1.1")
            .body(SdkBody::from(body))
            .expect("replay response should build")
    }

    fn ok_event(body: serde_json::Value) -> ReplayEvent {
        ReplayEvent::new(any_request(), response(200, body.to_string()))
    }

    /// An AWS error response in the awsJson1_1 shape.
    fn error_event(status: u16, code: &str, message: &str) -> ReplayEvent {
        ReplayEvent::new(
            any_request(),
            response(status, serde_json::json!({ "__type": code, "message": message }).to_string()),
        )
    }

    fn scripted_endpoint() -> String {
        static NEXT_SCRIPTED_BACKEND_ID: AtomicU64 = AtomicU64::new(0);

        format!(
            "https://scripted-{}.example.invalid",
            NEXT_SCRIPTED_BACKEND_ID.fetch_add(1, Ordering::Relaxed)
        )
    }

    fn scripted_backend(events: Vec<ReplayEvent>) -> (StaticReplayClient, AwsKmsBackend) {
        let http_client = StaticReplayClient::new(events);
        let sdk_config = aws_sdk_kms::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::new("AKIDTEST", "secret", None, None, "scripted"))
            .http_client(http_client.clone())
            .retry_config(aws_sdk_kms::config::retry::RetryConfig::disabled())
            .build();
        let kms_config = KmsConfig::aws(Some("us-east-1".to_string()));
        let endpoint = scripted_endpoint();
        let backend = AwsKmsBackend::with_client(aws_sdk_kms::Client::from_conf(sdk_config), &kms_config, &endpoint, "us-east-1");
        (http_client, backend)
    }

    fn aws_config(endpoint: &str, region: &str) -> KmsConfig {
        let mut config = KmsConfig::aws(Some(region.to_owned()));
        let BackendConfig::Aws(aws) = &mut config.backend_config else {
            panic!("AWS constructor must create an AWS backend configuration");
        };
        aws.endpoint_url = Some(endpoint.to_owned());
        config
    }

    fn key_metadata_json(key_id: &str, state: &str) -> serde_json::Value {
        serde_json::json!({
            "KeyMetadata": {
                "KeyId": key_id,
                "Arn": format!("arn:aws:kms:us-east-1:111122223333:key/{key_id}"),
                "CreationDate": 1_700_000_000,
                "Enabled": state == "Enabled",
                "Description": "contract key",
                "KeyUsage": "ENCRYPT_DECRYPT",
                "KeyState": state,
                "Origin": "AWS_KMS",
                "KeyManager": "CUSTOMER",
            }
        })
    }

    fn generate_request() -> GenerateDataKeyRequest {
        GenerateDataKeyRequest {
            key_id: "test-key".to_string(),
            key_spec: KeySpec::Aes256,
            encryption_context: HashMap::from([("bucket".to_string(), "aws".to_string())]),
        }
    }

    fn capabilities_snapshot(capabilities: BackendCapabilities) -> std::collections::BTreeMap<String, bool> {
        serde_json::from_value(serde_json::to_value(capabilities).expect("capabilities should serialize"))
            .expect("capabilities should deserialize into a flat bool map")
    }

    #[tokio::test]
    async fn aws_backend_new_identity_includes_endpoint_and_region() {
        let endpoint = scripted_endpoint();
        let first = AwsKmsBackend::new(aws_config(&endpoint, "us-east-1"))
            .await
            .expect("first AWS backend");
        let matching = AwsKmsBackend::new(aws_config(&endpoint, "us-east-1"))
            .await
            .expect("matching AWS backend");
        let other_region = AwsKmsBackend::new(aws_config(&endpoint, "us-west-2"))
            .await
            .expect("other-region AWS backend");
        let other_endpoint = AwsKmsBackend::new(aws_config(&scripted_endpoint(), "us-east-1"))
            .await
            .expect("other-endpoint AWS backend");

        assert!(first.retry.shares_active_capacity_with(&matching.retry));
        assert!(!first.retry.shares_active_capacity_with(&other_region.retry));
        assert!(!first.retry.shares_active_capacity_with(&other_endpoint.retry));
    }

    #[tokio::test]
    async fn aws_backend_capabilities_golden() {
        let (_http, backend) = scripted_backend(Vec::new());
        insta::assert_json_snapshot!("aws_backend_capabilities", capabilities_snapshot(backend.capabilities()));
    }

    #[tokio::test]
    async fn generate_data_key_maps_the_native_response() {
        let plaintext = vec![7u8; 32];
        let ciphertext = b"encrypted-data-key".to_vec();
        let (http_client, backend) = scripted_backend(vec![ok_event(serde_json::json!({
            "KeyId": "arn:aws:kms:us-east-1:111122223333:key/test-key",
            "Plaintext": BASE64.encode(&plaintext),
            "CiphertextBlob": BASE64.encode(&ciphertext),
        }))]);

        let response = backend
            .generate_data_key(generate_request())
            .await
            .expect("generate_data_key should map the AWS response");

        assert_eq!(response.plaintext_key, plaintext);
        assert_eq!(response.ciphertext_blob, ciphertext);
        assert_eq!(response.key_id, "arn:aws:kms:us-east-1:111122223333:key/test-key");
        assert_eq!(http_client.actual_requests().count(), 1);
    }

    #[tokio::test]
    async fn decrypt_recovers_the_plaintext_without_a_key_id() {
        let plaintext = b"recovered-data-key".to_vec();
        let (_http, backend) = scripted_backend(vec![ok_event(serde_json::json!({
            "KeyId": "arn:aws:kms:us-east-1:111122223333:key/test-key",
            "Plaintext": BASE64.encode(&plaintext),
            "EncryptionAlgorithm": "SYMMETRIC_DEFAULT",
        }))]);

        let response = backend
            .decrypt(DecryptRequest {
                ciphertext: b"blob".to_vec(),
                encryption_context: HashMap::new(),
                grant_tokens: Vec::new(),
            })
            .await
            .expect("decrypt should map the AWS response");

        assert_eq!(response.plaintext, plaintext);
        assert_eq!(response.encryption_algorithm.as_deref(), Some("SYMMETRIC_DEFAULT"));
    }

    /// Asserts that a mapped failure landed on the intended `KmsError` variant.
    type ErrorPredicate = fn(&KmsError) -> bool;

    /// Every AWS error code the backend classifies must land on the intended
    /// typed error, so callers can keep reacting to categories rather than
    /// parsing messages.
    #[tokio::test]
    async fn aws_error_codes_map_to_typed_errors() {
        let cases: Vec<(u16, &str, ErrorPredicate)> = vec![
            (400, "NotFoundException", |error| matches!(error, KmsError::KeyNotFound { .. })),
            (400, "AccessDeniedException", |error| matches!(error, KmsError::AccessDenied { .. })),
            (400, "KMSInvalidStateException", |error| {
                matches!(error, KmsError::InvalidOperation { .. })
            }),
            (400, "DisabledException", |error| matches!(error, KmsError::InvalidOperation { .. })),
            (400, "InvalidCiphertextException", |error| {
                matches!(error, KmsError::CryptographicError { .. })
            }),
            (400, "ValidationException", |error| matches!(error, KmsError::InvalidOperation { .. })),
            (403, "UnrecognizedClientException", |error| {
                matches!(error, KmsError::CredentialsUnavailable { .. })
            }),
            (500, "KMSInternalException", |error| matches!(error, KmsError::BackendError { .. })),
        ];

        for (status, code, expected) in cases {
            // One event per configured attempt so retryable codes exhaust the
            // budget and still surface their mapped error.
            let events = (0..4).map(|_| error_event(status, code, "scripted failure")).collect();
            let (_http, backend) = scripted_backend(events);
            let error = backend
                .generate_data_key(generate_request())
                .await
                .expect_err("scripted AWS failure should surface");
            assert!(expected(&error), "unexpected mapping for {code}: {error:?}");
        }
    }

    /// Throttling is replayed by the policy executor for read-shaped
    /// operations, and the eventual success is returned to the caller.
    #[tokio::test(start_paused = true)]
    async fn throttled_reads_are_retried_until_they_succeed() {
        let (http_client, backend) = scripted_backend(vec![
            error_event(400, "ThrottlingException", "rate exceeded"),
            error_event(400, "ThrottlingException", "rate exceeded"),
            ok_event(serde_json::json!({
                "KeyId": "test-key",
                "Plaintext": BASE64.encode([1u8; 32]),
                "CiphertextBlob": BASE64.encode(b"blob"),
            })),
        ]);

        backend
            .generate_data_key(generate_request())
            .await
            .expect("a throttled read must be retried");
        assert_eq!(http_client.actual_requests().count(), 3, "both throttled attempts should be replayed");
    }

    /// A connector that never responds must be cut off by the backend's
    /// per-attempt timeout rather than hanging the KMS operation indefinitely.
    #[tokio::test(start_paused = true)]
    async fn stalled_aws_request_is_cut_off_by_the_attempt_timeout() {
        let never_client = NeverClient::new();
        let sdk_config = aws_sdk_kms::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::new("AKIDTEST", "secret", None, None, "scripted"))
            .http_client(never_client.clone())
            .retry_config(aws_sdk_kms::config::retry::RetryConfig::disabled())
            .build();
        let mut kms_config = KmsConfig::aws(Some("us-east-1".to_string()));
        kms_config.timeout = std::time::Duration::from_millis(5_000);
        kms_config.retry_attempts = 1;
        let backend = AwsKmsBackend::with_client(aws_sdk_kms::Client::from_conf(sdk_config), &kms_config, "", "us-east-1");

        let error = backend
            .describe_key(DescribeKeyRequest {
                key_id: "stalled-key".to_string(),
            })
            .await
            .expect_err("a stalled AWS request must be cut off by the attempt timeout");

        assert!(matches!(error, KmsError::OperationTimedOut { .. }), "unexpected error: {error:?}");
        assert_eq!(never_client.num_calls(), 1, "one configured attempt must reach the connector");
    }

    /// Access denial is deterministic: replaying it cannot help and would only
    /// multiply the audit trail of denied calls.
    #[tokio::test(start_paused = true)]
    async fn access_denied_is_not_retried() {
        let (http_client, backend) = scripted_backend(vec![
            error_event(400, "AccessDeniedException", "not authorized"),
            error_event(400, "AccessDeniedException", "not authorized"),
        ]);

        let error = backend
            .generate_data_key(generate_request())
            .await
            .expect_err("access denial should surface");
        assert!(matches!(error, KmsError::AccessDenied { .. }), "unexpected error: {error:?}");
        assert_eq!(http_client.actual_requests().count(), 1, "access denial must not be replayed");
    }

    /// Rotation carries an external side effect and has no idempotency key, so
    /// even a throttled attempt is never replayed.
    #[tokio::test(start_paused = true)]
    async fn rotation_is_never_retried() {
        let (http_client, backend) = scripted_backend(vec![
            error_event(400, "ThrottlingException", "rate exceeded"),
            error_event(400, "ThrottlingException", "rate exceeded"),
        ]);

        backend
            .rotate_key("test-key")
            .await
            .expect_err("the scripted throttling should surface");
        assert_eq!(http_client.actual_requests().count(), 1, "a mutation must run at most once");
    }

    #[tokio::test]
    async fn describe_key_maps_aws_states() {
        for (aws_state, expected) in [
            ("Enabled", KeyState::Enabled),
            ("Disabled", KeyState::Disabled),
            ("PendingDeletion", KeyState::PendingDeletion),
            ("PendingReplicaDeletion", KeyState::PendingDeletion),
            ("PendingImport", KeyState::PendingImport),
            // Transient and unknown AWS states must fail closed.
            ("Creating", KeyState::Unavailable),
            ("Updating", KeyState::Unavailable),
        ] {
            let (_http, backend) = scripted_backend(vec![ok_event(key_metadata_json("test-key", aws_state))]);
            let described = backend
                .describe_key(DescribeKeyRequest {
                    key_id: "test-key".to_string(),
                })
                .await
                .expect("describe_key should map the AWS response");
            assert_eq!(described.key_metadata.key_state, expected, "unexpected mapping for {aws_state}");
            assert_eq!(described.key_metadata.key_manager, KEY_MANAGER);
        }
    }

    /// AWS's window bounds are enforced before the request leaves the process.
    #[tokio::test]
    async fn deletion_window_is_validated_before_the_call() {
        for days in [0, 6, 31] {
            let (http_client, backend) = scripted_backend(Vec::new());
            let error = backend
                .delete_key(DeleteKeyRequest {
                    key_id: "test-key".to_string(),
                    pending_window_in_days: Some(days),
                    force_immediate: None,
                    confirm_key_id: None,
                })
                .await
                .expect_err("an out-of-range window must be rejected");
            assert!(matches!(error, KmsError::InvalidOperation { .. }), "unexpected error: {error:?}");
            assert_eq!(http_client.actual_requests().count(), 0, "no request should reach AWS");
        }
    }

    /// AWS has no immediate-deletion path, so the capability gap is reported
    /// instead of silently degrading to a scheduled deletion.
    #[tokio::test]
    async fn immediate_deletion_is_unsupported() {
        let (http_client, backend) = scripted_backend(Vec::new());
        let error = backend
            .delete_key(DeleteKeyRequest {
                key_id: "test-key".to_string(),
                pending_window_in_days: None,
                force_immediate: Some(true),
                confirm_key_id: None,
            })
            .await
            .expect_err("immediate deletion must be rejected");
        assert!(matches!(error, KmsError::UnsupportedCapability { .. }), "unexpected error: {error:?}");
        assert_eq!(http_client.actual_requests().count(), 0);
        assert!(!backend.capabilities().physical_delete);
    }

    /// A caller-assigned key name cannot be honoured by AWS. Refusing it keeps
    /// describe-then-create callers (SSE-S3 auto-creation, the synthetic
    /// probe) from creating a fresh, unreachable key on every attempt.
    #[tokio::test]
    async fn caller_assigned_key_names_are_unsupported() {
        let (http_client, backend) = scripted_backend(Vec::new());
        let error = backend
            .create_key(CreateKeyRequest {
                key_name: Some("rustfs-internal-kms-probe".to_string()),
                ..Default::default()
            })
            .await
            .expect_err("a named create must be rejected");
        assert!(matches!(error, KmsError::UnsupportedCapability { .. }), "unexpected error: {error:?}");
        assert_eq!(http_client.actual_requests().count(), 0, "no key may be created in AWS");
    }

    /// AWS intentionally does not use the shared lifecycle contract driver.
    /// The driver requires disabled/pending keys to decrypt, expects cancelling
    /// deletion to re-enable a key, and creates keys by caller-assigned name;
    /// AWS rejects the decryption assumption, leaves a cancelled key
    /// disabled, and cannot honour the third.
    #[tokio::test]
    async fn aws_backend_shared_contract_exemption_is_pinned() {
        let (_http, backend) = scripted_backend(vec![error_event(400, "DisabledException", "key is disabled")]);
        let error = backend
            .decrypt(DecryptRequest {
                ciphertext: b"blob".to_vec(),
                encryption_context: HashMap::new(),
                grant_tokens: Vec::new(),
            })
            .await
            .expect_err("AWS must reject decrypt with a disabled key");
        assert!(matches!(error, KmsError::InvalidOperation { .. }), "unexpected error: {error:?}");

        let (_http, backend) = scripted_backend(vec![error_event(400, "KMSInvalidStateException", "key is pending deletion")]);
        let error = backend
            .decrypt(DecryptRequest {
                ciphertext: b"blob".to_vec(),
                encryption_context: HashMap::new(),
                grant_tokens: Vec::new(),
            })
            .await
            .expect_err("AWS must reject decrypt with a pending-deletion key");
        assert!(matches!(error, KmsError::InvalidOperation { .. }), "unexpected error: {error:?}");

        let (_http, backend) = scripted_backend(vec![
            ok_event(serde_json::json!({})),
            ok_event(key_metadata_json("test-key", "Disabled")),
        ]);
        let response = backend
            .cancel_key_deletion(CancelKeyDeletionRequest {
                key_id: "test-key".to_string(),
            })
            .await
            .expect("AWS cancellation should complete");
        assert_eq!(response.key_metadata.key_state, KeyState::Disabled);

        let (_http, backend) = scripted_backend(Vec::new());
        let error = backend
            .create_key(CreateKeyRequest {
                key_name: Some("contract-key".to_string()),
                ..Default::default()
            })
            .await
            .expect_err("AWS cannot create a key under a caller-assigned name");
        assert!(matches!(error, KmsError::UnsupportedCapability { .. }), "unexpected error: {error:?}");
    }

    /// AWS rejects `Limit: 0` outright, so the request cannot be forwarded as
    /// written; clamping it up to one would answer a caller that asked for no
    /// keys with a key. The empty page is served locally instead.
    #[tokio::test]
    async fn zero_limit_list_returns_an_empty_page_without_calling_aws() {
        let (http_client, backend) = scripted_backend(Vec::new());
        let response = backend
            .list_keys(ListKeysRequest {
                limit: Some(0),
                ..Default::default()
            })
            .await
            .expect("a zero-limit list must succeed");

        assert!(response.keys.is_empty());
        assert!(!response.truncated);
        assert!(response.next_marker.is_none());
        assert_eq!(http_client.actual_requests().count(), 0, "no request should reach AWS");
    }

    /// Signing keys are outside the envelope-encryption surface this backend
    /// serves; creating one is refused rather than silently downgraded.
    #[tokio::test]
    async fn sign_verify_keys_are_unsupported() {
        let (http_client, backend) = scripted_backend(Vec::new());
        let error = backend
            .create_key(CreateKeyRequest {
                key_usage: KeyUsage::SignVerify,
                ..Default::default()
            })
            .await
            .expect_err("SIGN_VERIFY keys must be rejected");
        assert!(matches!(error, KmsError::UnsupportedCapability { .. }), "unexpected error: {error:?}");
        assert_eq!(http_client.actual_requests().count(), 0);
    }

    /// The deletion sweep must never destroy AWS-held material: a key still
    /// inside its window is reported as not expired, and one AWS has already
    /// removed is reported as removed.
    #[tokio::test]
    async fn expired_key_removal_only_observes_aws() {
        let (http_client, backend) = scripted_backend(vec![ok_event(key_metadata_json("test-key", "PendingDeletion"))]);
        assert_eq!(
            backend
                .remove_expired_key("test-key", &Zoned::now())
                .await
                .expect("observation should succeed"),
            ExpiredKeyRemoval::NotExpired
        );
        assert_eq!(http_client.actual_requests().count(), 1, "no deletion call may be issued");

        let (_http, backend) = scripted_backend(vec![error_event(400, "NotFoundException", "key does not exist")]);
        assert_eq!(
            backend
                .remove_expired_key("test-key", &Zoned::now())
                .await
                .expect("a key AWS already removed is a completed removal"),
            ExpiredKeyRemoval::Removed
        );

        let (_http, backend) = scripted_backend(vec![ok_event(key_metadata_json("test-key", "Enabled"))]);
        assert_eq!(
            backend
                .remove_expired_key("test-key", &Zoned::now())
                .await
                .expect("observation should succeed"),
            ExpiredKeyRemoval::StateChanged
        );
    }

    /// End-to-end lifecycle against a real AWS account.
    ///
    /// Not part of the shared contract-test driver: AWS refuses to decrypt
    /// with a `Disabled` or `PendingDeletion` key, and leaves a cancelled key
    /// disabled, both of which the shared matrix asserts the other way around.
    /// Requires credentials with `kms:CreateKey`, `kms:GenerateDataKey`,
    /// `kms:Decrypt`, `kms:DescribeKey`, `kms:EnableKey`, `kms:DisableKey`,
    /// `kms:RotateKeyOnDemand`, `kms:ScheduleKeyDeletion` and
    /// `kms:CancelKeyDeletion`.
    #[tokio::test]
    #[ignore] // Requires real AWS credentials and creates a billable KMS key
    async fn aws_backend_lifecycle_end_to_end() {
        let region = std::env::var("RUSTFS_KMS_AWS_REGION").ok();
        let backend = AwsKmsBackend::new(KmsConfig::aws(region))
            .await
            .expect("aws backend should build");

        let created = backend
            .create_key(CreateKeyRequest {
                description: Some("rustfs aws backend contract".to_string()),
                tags: HashMap::from([("rustfs-test".to_string(), "contract".to_string())]),
                ..Default::default()
            })
            .await
            .expect("key should be created");
        let key_id = created.key_id;

        let data_key = backend
            .generate_data_key(generate_request_for(&key_id))
            .await
            .expect("an enabled key must generate data keys");
        let decrypted = backend
            .decrypt(DecryptRequest {
                ciphertext: data_key.ciphertext_blob.clone(),
                encryption_context: HashMap::from([("bucket".to_string(), "aws".to_string())]),
                grant_tokens: Vec::new(),
            })
            .await
            .expect("the data key must decrypt");
        assert_eq!(decrypted.plaintext, data_key.plaintext_key);

        backend.rotate_key(&key_id).await.expect("on-demand rotation must succeed");
        backend
            .generate_data_key(generate_request_for(&key_id))
            .await
            .expect("a rotated key must stay usable");
        backend
            .decrypt(DecryptRequest {
                ciphertext: data_key.ciphertext_blob.clone(),
                encryption_context: HashMap::from([("bucket".to_string(), "aws".to_string())]),
                grant_tokens: Vec::new(),
            })
            .await
            .expect("rotation must retain prior backing keys for decryption");

        backend.disable_key(&key_id).await.expect("disable must succeed");
        assert_eq!(
            backend.describe(&key_id).await.expect("describe must succeed").key_state,
            KeyState::Disabled
        );
        backend.enable_key(&key_id).await.expect("enable must succeed");

        backend
            .delete_key(DeleteKeyRequest {
                key_id: key_id.clone(),
                pending_window_in_days: Some(7),
                force_immediate: None,
                confirm_key_id: None,
            })
            .await
            .expect("scheduling deletion must succeed");
        assert_eq!(
            backend.describe(&key_id).await.expect("describe must succeed").key_state,
            KeyState::PendingDeletion
        );
        // Leave the key scheduled for deletion so repeated runs stay tidy; the
        // cancel below is only there to prove the transition works.
        backend
            .cancel_key_deletion(CancelKeyDeletionRequest { key_id: key_id.clone() })
            .await
            .expect("cancelling deletion must succeed");
        backend
            .delete_key(DeleteKeyRequest {
                key_id,
                pending_window_in_days: Some(7),
                force_immediate: None,
                confirm_key_id: None,
            })
            .await
            .expect("re-scheduling deletion must succeed");
    }

    fn generate_request_for(key_id: &str) -> GenerateDataKeyRequest {
        GenerateDataKeyRequest {
            key_id: key_id.to_string(),
            ..generate_request()
        }
    }
}
