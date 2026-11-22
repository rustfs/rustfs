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

//! S3 server-side encryption: which key encrypts an object, and what is stored to find it again.
//!
//! Covers the three S3 schemes:
//! - SSE-S3 (`AES256`): a data key wrapped by a key the server owns
//! - SSE-KMS (`aws:kms`): a data key wrapped by a KMS key the caller names
//! - SSE-C: a key the client supplies on every request and the server never stores
//!
//! ## Where this crate sits
//!
//! Between the S3 layer and the storage engine, depending on neither. It decides
//! the effective scheme of a write, obtains and unwraps data keys, authorizes the
//! use of a KMS key, and encodes and reads the encryption metadata of an object.
//! It does not transform object bytes: it hands back key material
//! ([`EncryptionMaterial`], [`DecryptionMaterial`]) and the caller applies it.
//!
//! What it needs from the running server — the KMS service, a bucket's default
//! encryption, a key-permission decision — arrives through [`SseHost`], passed
//! to every entry point that consults the server. Failures are reported as
//! [`SseError`], already classified with the S3 error code the caller answers with.
//!
//! ## Entry points
//!
//! - [`sse_encryption`] and [`sse_prepare_encryption`] resolve the material of a write;
//!   [`encryption_material_to_metadata`] turns it into the metadata to store.
//! - [`sse_decryption`] resolves the material of a read from stored metadata;
//!   [`classify_sse_read_response`] derives the response headers without unwrapping a key.
//! - [`rewrap_object_encryption_metadata`] moves a stored data key onto the current
//!   version of its KMS key.
//!
//! ```rust,ignore
//! let request = EncryptionRequest {
//!     bucket: &bucket,
//!     key: &key,
//!     server_side_encryption: effective_sse,
//!     ssekms_key_id: effective_kms_key_id,
//!     ssekms_context: None,
//!     sse_customer_algorithm: None,
//!     sse_customer_key: None,
//!     sse_customer_key_md5: None,
//!     content_size: actual_size,
//!     principal: principal.as_ref(),
//! };
//!
//! if let Some(material) = sse_encryption(host, request).await? {
//!     metadata.extend(encryption_material_to_metadata(&material)?);
//! }
//! ```

mod core;
mod error;
mod host;
mod kms;
#[cfg(any(test, feature = "test-util"))]
pub mod testing;
#[cfg(test)]
#[allow(unused_imports)]
mod tests;

pub use crate::core::audit::KmsRequestAuditScope;
pub use crate::core::keying::material::{DecryptionMaterial, EncryptionKeyKind, EncryptionMaterial, ManagedSealedKey};
pub use crate::core::keying::metadata::{
    ConflictingEncryptionMetadata, encryption_material_to_metadata, mark_encrypted_multipart_metadata,
    normalize_encryption_metadata_case, stored_ssec_metadata, strip_managed_encryption_metadata,
};
pub use crate::core::read::{DecryptionRequest, classify_sse_read_response, sse_decryption};
pub use crate::core::request::SseKmsPrincipal;
#[cfg(any(test, feature = "test-util"))]
pub use crate::core::request::TestAuthorizationHooks;
pub use crate::core::rewrap::{ObjectDekRewrapOutcome, rewrap_object_encryption_metadata};
pub use crate::core::s3::authorization::{
    authorize_sse_kms_multipart_write, authorize_sse_kms_object_read, log_sse_kms_key_policy_mode,
};
pub use crate::core::s3::headers::{
    SseReadResponseHeaders, SsecParamsFromHeaders, build_ssec_read_headers, extract_server_side_encryption_from_headers,
    extract_ssec_params_from_headers, extract_ssekms_context_from_headers, project_sse_read_response_headers,
    validate_sse_headers_for_read, validate_sse_headers_for_write,
};
pub use crate::core::s3::scheme::{DEFAULT_SSE_ALGORITHM, SseTypeV2, bucket_default_write_sse};
pub use crate::core::s3::ssec::{ssec_invalid_request, validate_ssec_for_read};
pub use crate::core::write::{EncryptionRequest, PrepareEncryptionRequest, sse_encryption, sse_prepare_encryption};
pub use crate::error::{KMS_KEY_NOT_FOUND_ERROR_CODE, SseError, data_plane_kms_error};
pub use crate::host::{BucketDefaultEncryption, BucketEncryption, KmsKeyAuthorizer, SseHost};
pub use crate::kms::local::LocalSseDekProvider;
#[cfg(any(test, feature = "test-util"))]
pub use crate::kms::local::TestSseDekProvider;
pub use crate::kms::provider::SseDekProvider;
#[cfg(any(test, feature = "test-util"))]
pub use crate::kms::provider::{reset_sse_dek_provider, set_sse_dek_provider_for_test};
// The managed-SSE classifier lives in the shared encryption-keys module so the
// scanner can reuse it; re-exported here so SSE callers find it beside the rest.
pub use rustfs_utils::http::object_encryption_keys::SSEType;
pub use rustfs_utils::http::object_encryption_keys::contains_managed_encryption_metadata;

pub(crate) const LOG_COMPONENT_STORAGE: &str = "storage";
pub(crate) const LOG_SUBSYSTEM_SSE: &str = "sse";
