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

//! What SSE needs from the server it runs in.
//!
//! This crate reaches the running server only through [`SseHost`]: it holds no
//! handle to the storage engine, to IAM, or to the bucket metadata system. Every
//! entry point that consults the server takes the host as its first argument, so
//! a caller cannot reach key selection without saying which server it speaks for.

use crate::{SseError, SseKmsPrincipal};
use async_trait::async_trait;
use rustfs_kms::KmsServiceManager;
use rustfs_kms::service::ObjectEncryptionService;
use rustfs_policy::policy::action::KmsAction;
use std::sync::Arc;

/// The server capabilities SSE key resolution depends on.
#[async_trait]
pub trait SseHost: Send + Sync {
    /// The KMS object-encryption service currently serving, if one is.
    ///
    /// Resolved on every call: the KMS backend can be reconfigured while the
    /// server runs, so a service handle must not outlive the operation it serves.
    async fn encryption_service(&self) -> Option<Arc<ObjectEncryptionService>>;

    /// The KMS service manager, when the server has one.
    ///
    /// Only its status is read, to tell a configured KMS that is not serving
    /// (retryable) from a deployment that never configured one (a request error).
    fn kms_service_manager(&self) -> Option<Arc<KmsServiceManager>>;

    /// The bucket's default-encryption configuration.
    ///
    /// `Ok(None)` means the bucket has no encryption configuration.
    ///
    /// # Errors
    ///
    /// Any failure to read the configuration, already classified for the S3
    /// response. A write must not proceed as if the bucket had no default when
    /// the configuration could not be read.
    async fn bucket_encryption(&self, bucket: &str) -> Result<Option<BucketEncryption>, SseError>;

    /// The authorizer deciding per-key KMS permissions on the data path.
    fn kms_key_authorizer(&self) -> &dyn KmsKeyAuthorizer;
}

/// A bucket's stored encryption configuration, reduced to what a write consults.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BucketEncryption {
    /// The default-encryption rule of the first configuration rule, if it has one.
    pub default_rule: Option<BucketDefaultEncryption>,
}

/// The `ApplyServerSideEncryptionByDefault` element of a bucket encryption rule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketDefaultEncryption {
    /// The stored algorithm, exactly as persisted (`AES256`, `aws:kms`, or an unknown value).
    pub sse_algorithm: String,
    pub kms_master_key_id: Option<String>,
}

/// Decides whether a principal may use a specific KMS key on the data path.
///
/// Behind a trait so tests can pin the allow/deny contract of the SSE call sites
/// without a running IAM subsystem.
#[async_trait]
pub trait KmsKeyAuthorizer: Send + Sync {
    /// # Errors
    ///
    /// When the decision itself could not be made, for example because the
    /// policy subsystem is not ready. A denial is `Ok(false)`.
    async fn is_allowed(&self, principal: &SseKmsPrincipal, action: KmsAction, key_id: &str) -> Result<bool, SseError>;
}
