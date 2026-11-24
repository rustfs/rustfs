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

//! The crate's process-startup step.
//!
//! [`init`] settles the two things that cannot change without a restart, both
//! read from the environment, and reports them once:
//!
//! - whether SSE-KMS requests are authorized against the key they name;
//! - whether the local SSE-S3 master key is usable.
//!
//! Nothing that follows the KMS service is decided here. The KMS can be
//! configured, started, restarted, and stopped while the server runs, so the
//! choice between the KMS and the local master key is made per request, and no
//! KMS state is cached by this crate.
//!
//! Calling [`init`] is optional. A process that never calls it reads the same
//! switch and loads the same master key on first use, with the same errors; it
//! only goes without the startup report.

use crate::error::SseError;
use crate::kms::local::{ENV_SSE_S3_MASTER_KEY, local_master_key_configured};
use crate::kms::provider::get_local_sse_dek_provider;
use crate::{LOG_COMPONENT_STORAGE, LOG_SUBSYSTEM_SSE};
use rustfs_config::{DEFAULT_KMS_ENFORCE_SSE_KEY_POLICY, ENV_RUSTFS_KMS_ENFORCE_SSE_KEY_POLICY};
use rustfs_utils::get_env_bool;
use std::sync::{LazyLock, OnceLock};

const EVENT_SSE_KMS_KEY_POLICY_MODE: &str = "sse_kms_key_policy_mode";
const EVENT_SSE_LOCAL_MASTER_KEY: &str = "sse_local_master_key";

/// State of the local SSE-S3 master key at startup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalMasterKeyStatus {
    /// Configured and usable; SSE-S3 works without a KMS.
    Loaded,
    /// Not configured; SSE-S3 requires a running KMS.
    NotSet,
    /// Configured but unusable; SSE-S3 requests that need it are rejected.
    Invalid,
}

impl LocalMasterKeyStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Loaded => "loaded",
            Self::NotSet => "not_set",
            Self::Invalid => "invalid",
        }
    }
}

/// What [`init`] settled for this process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StartupReport {
    /// Whether SSE-KMS requests are authorized against the KMS key they name.
    pub key_policy_enforced: bool,
    /// State of the local SSE-S3 master key.
    pub local_master_key: LocalMasterKeyStatus,
}

/// Settle the process-wide SSE configuration and report it.
///
/// Idempotent: the first call does the work and logs; later calls, such as a
/// second embedded server in the same process, return the same report silently.
/// An unusable master key does not fail startup, because a server whose KMS is
/// running never needs it.
pub fn init() -> StartupReport {
    static REPORT: OnceLock<StartupReport> = OnceLock::new();
    *REPORT.get_or_init(|| {
        let key_policy_enforced = key_policy_enforced();
        log_key_policy_mode(key_policy_enforced);

        let local_master_key = load_local_master_key();
        log_local_master_key(&local_master_key);

        StartupReport {
            key_policy_enforced,
            local_master_key: local_master_key.unwrap_or(LocalMasterKeyStatus::Invalid),
        }
    })
}

/// Whether per-key KMS authorization is enforced on the SSE-KMS data path.
///
/// Frozen on first read, by [`init`] or by the first request, so one process
/// never answers the same question two ways.
pub(crate) fn key_policy_enforced() -> bool {
    static ENFORCED: LazyLock<bool> =
        LazyLock::new(|| get_env_bool(ENV_RUSTFS_KMS_ENFORCE_SSE_KEY_POLICY, DEFAULT_KMS_ENFORCE_SSE_KEY_POLICY));
    *ENFORCED
}

/// Load the local master key through the loader the request path uses.
///
/// An absent key is a state, not a failure: only a configured key is loaded, so
/// the error case is exactly "configured but unusable".
fn load_local_master_key() -> Result<LocalMasterKeyStatus, SseError> {
    if !local_master_key_configured() {
        return Ok(LocalMasterKeyStatus::NotSet);
    }
    get_local_sse_dek_provider().map(|_| LocalMasterKeyStatus::Loaded)
}

/// Report the configured SSE-KMS authorization mode.
///
/// The disabled case warns rather than logs: while enforcement is off, any identity
/// allowed to write an object can encrypt it under any key, and operators should hear
/// about that even though disabled is the long-term default.
fn log_key_policy_mode(enforced: bool) {
    if enforced {
        tracing::info!(
            event = EVENT_SSE_KMS_KEY_POLICY_MODE,
            component = LOG_COMPONENT_STORAGE,
            subsystem = LOG_SUBSYSTEM_SSE,
            enforced = true,
            "SSE-KMS requests are authorized against the resolved KMS key (kms:GenerateDataKey / kms:Decrypt)"
        );
        return;
    }

    tracing::warn!(
        event = EVENT_SSE_KMS_KEY_POLICY_MODE,
        component = LOG_COMPONENT_STORAGE,
        subsystem = LOG_SUBSYSTEM_SSE,
        enforced = false,
        "SSE-KMS requests are not authorized against the KMS key they name; any identity allowed to \
         write an object may encrypt it under any key, and any identity allowed to read it may have it \
         decrypted. Grant kms:GenerateDataKey and kms:Decrypt on the keys your workloads use, then set \
         {ENV_RUSTFS_KMS_ENFORCE_SSE_KEY_POLICY}=true."
    );
}

/// Report the local master key's state.
///
/// The failure text is the one a request is answered with. It describes the
/// configuration problem and never carries the configured value.
fn log_local_master_key(outcome: &Result<LocalMasterKeyStatus, SseError>) {
    match outcome {
        Ok(status @ LocalMasterKeyStatus::Loaded) => tracing::info!(
            event = EVENT_SSE_LOCAL_MASTER_KEY,
            component = LOG_COMPONENT_STORAGE,
            subsystem = LOG_SUBSYSTEM_SSE,
            state = status.as_str(),
            "{ENV_SSE_S3_MASTER_KEY} is loaded; SSE-S3 uses it whenever no KMS is running"
        ),
        Ok(status) => tracing::info!(
            event = EVENT_SSE_LOCAL_MASTER_KEY,
            component = LOG_COMPONENT_STORAGE,
            subsystem = LOG_SUBSYSTEM_SSE,
            state = status.as_str(),
            "{ENV_SSE_S3_MASTER_KEY} is not set; SSE-S3 requires a running KMS"
        ),
        Err(error) => tracing::error!(
            event = EVENT_SSE_LOCAL_MASTER_KEY,
            component = LOG_COMPONENT_STORAGE,
            subsystem = LOG_SUBSYSTEM_SSE,
            state = LocalMasterKeyStatus::Invalid.as_str(),
            reason = %error.message,
            "{ENV_SSE_S3_MASTER_KEY} is set but unusable; SSE-S3 requests are rejected while no KMS is running"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kms::provider::reset_sse_dek_provider;
    use crate::tests::lock_sse_test_state;
    use base64_simd::STANDARD as BASE64_STANDARD;
    use rustfs_s3_types::S3ErrorCode;
    use temp_env::with_vars;

    /// Run `check` with the local master key set to `value` and nothing cached or injected.
    async fn with_master_key<T>(value: Option<&str>, check: impl FnOnce() -> T) -> T {
        let _guard = lock_sse_test_state().await;
        reset_sse_dek_provider();
        let outcome = with_vars([(ENV_SSE_S3_MASTER_KEY, value), ("__RUSTFS_SSE_SIMPLE_CMK", None)], check);
        reset_sse_dek_provider();
        outcome
    }

    #[tokio::test]
    async fn an_absent_master_key_is_reported_as_not_set() {
        let status = with_master_key(None, load_local_master_key)
            .await
            .expect("an absent key is a state, not a failure");
        assert_eq!(status, LocalMasterKeyStatus::NotSet);

        let blank = with_master_key(Some("   "), load_local_master_key)
            .await
            .expect("a blank value counts as absent");
        assert_eq!(blank, LocalMasterKeyStatus::NotSet);
    }

    #[tokio::test]
    async fn a_valid_master_key_is_loaded_and_cached_for_requests() {
        let encoded = BASE64_STANDARD.encode_to_string([7u8; 32]);
        let cached = with_master_key(Some(&encoded), || {
            let status = load_local_master_key().expect("a 32-byte key should load");
            assert_eq!(status, LocalMasterKeyStatus::Loaded);
            // The request path must find the provider startup loaded, not load a second one.
            crate::kms::provider::GLOBAL_SSE_DEK_PROVIDER
                .read()
                .expect("provider cache lock")
                .is_some()
        })
        .await;
        assert!(cached, "startup should leave the provider cached for the request path");
    }

    #[tokio::test]
    async fn an_unusable_master_key_fails_the_way_a_request_would() {
        let short = BASE64_STANDARD.encode_to_string([7u8; 16]);
        for unusable in ["not base64 at all!", short.as_str()] {
            let error = with_master_key(Some(unusable), load_local_master_key)
                .await
                .expect_err("an unusable key must not load");
            assert_eq!(error.code, S3ErrorCode::InvalidRequest);
            assert!(
                !error.message.contains(unusable),
                "the failure text must not carry the configured value: {}",
                error.message
            );
        }
    }

    #[tokio::test]
    async fn init_reports_once_and_repeats_the_same_report() {
        let encoded = BASE64_STANDARD.encode_to_string([9u8; 32]);
        let (first, second) = with_master_key(Some(&encoded), || (init(), init())).await;

        assert_eq!(first, second, "a second call must return the first report");
        assert_eq!(first.key_policy_enforced, key_policy_enforced());
        // The report is settled once per process, so this holds whether this test
        // or another caller of `init` ran first.
        assert_eq!(init(), first);
    }
}
