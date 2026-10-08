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

use crate::error::{Error, Result};
use manager::IamCache;
use oidc::{OidcExtraRootCaProvider, OidcSys};
use std::sync::{Arc, OnceLock};
use store::object::ObjectStore;
use sys::IamSys;
use tracing::{debug, error, info, instrument, warn};

pub(crate) const LOG_COMPONENT_IAM: &str = "iam";
const LOG_SUBSYSTEM_RUNTIME: &str = "runtime";
pub(crate) const LOG_SUBSYSTEM_OIDC: &str = "oidc";
const EVENT_IAM_STATE: &str = "iam_state";
pub(crate) const EVENT_OIDC_DIAGNOSTICS: &str = "oidc_diagnostics";
const EVENT_OIDC_STATE: &str = "oidc_state";

pub mod cache;
pub mod error;
pub mod federation;
pub mod keyring;
pub mod manager;
pub mod mfa;
pub mod oidc;
pub mod oidc_state;
mod root_credentials;
mod runtime_sources;
mod server_config;
mod storage_api;
pub mod store;
pub mod sys;
pub mod utils;
pub(crate) use storage_api::crate_boundary::{
    IAM_CONFIG_ROOT_PREFIX, IamEcstoreError, IamStorageError, IamStore, classify_iam_system_path_failure_reason,
    delete_iam_config, is_iam_first_cluster_node_local, read_iam_config_no_lock, read_iam_config_with_metadata, save_iam_config,
    save_iam_config_with_opts,
};

pub fn is_root_access_key(access_key: &str) -> bool {
    root_credentials::is_root_access_key(access_key)
}

/// Decrypts an at-rest IAM config blob using the same key sources as the IAM load
/// path (RustFS master keys and MinIO-compatible legacy keys derived from the root
/// credentials). Used by the MinIO -> RustFS migration path. See
/// [`store::object::try_decrypt_iam_blob`].
pub use store::object::try_decrypt_iam_blob;

pub(crate) struct IamNotificationPeerErr {
    pub(crate) err: Option<IamEcstoreError>,
}

impl From<storage_api::crate_boundary::IamEcstoreNotificationPeerErr> for IamNotificationPeerErr {
    fn from(value: storage_api::crate_boundary::IamEcstoreNotificationPeerErr) -> Self {
        Self { err: value.err }
    }
}

pub(crate) async fn notify_iam_delete_policy(policy_name: &str) -> Vec<IamNotificationPeerErr> {
    match runtime_sources::notification_sys() {
        Some(notification_sys) => notification_sys
            .delete_policy(policy_name)
            .await
            .into_iter()
            .map(Into::into)
            .collect(),
        None => Vec::new(),
    }
}

pub(crate) async fn notify_iam_load_policy(policy_name: &str) -> Vec<IamNotificationPeerErr> {
    match runtime_sources::notification_sys() {
        Some(notification_sys) => notification_sys
            .load_policy(policy_name)
            .await
            .into_iter()
            .map(Into::into)
            .collect(),
        None => Vec::new(),
    }
}

pub(crate) async fn notify_iam_delete_user(access_key: &str) -> Vec<IamNotificationPeerErr> {
    match runtime_sources::notification_sys() {
        Some(notification_sys) => notification_sys
            .delete_user(access_key)
            .await
            .into_iter()
            .map(Into::into)
            .collect(),
        None => Vec::new(),
    }
}

#[cfg(test)]
pub(crate) struct LoadUserNotificationProbe {
    pub(crate) observed: std::sync::Mutex<Option<(String, bool)>>,
    pub(crate) remaining_failures: std::sync::atomic::AtomicUsize,
    pub(crate) attempts: std::sync::atomic::AtomicUsize,
    pub(crate) panic: bool,
    pub(crate) started: tokio::sync::Notify,
    pub(crate) release: Option<tokio::sync::Notify>,
    pub(crate) completed: tokio::sync::Notify,
}

#[cfg(test)]
tokio::task_local! {
    pub(crate) static LOAD_USER_NOTIFICATION_PROBE: std::sync::Arc<LoadUserNotificationProbe>;
}

pub(crate) async fn notify_iam_load_user(access_key: &str, temp: bool) -> Vec<IamNotificationPeerErr> {
    #[cfg(test)]
    if let Ok(probe) = LOAD_USER_NOTIFICATION_PROBE.try_with(std::sync::Arc::clone) {
        probe.attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        *probe.observed.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some((access_key.to_string(), temp));
        probe.started.notify_one();
        if let Some(release) = &probe.release {
            release.notified().await;
        }
        assert!(!probe.panic, "notification probe panic");
        let should_fail = probe
            .remaining_failures
            .try_update(std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok();
        let result = if should_fail {
            vec![IamNotificationPeerErr {
                err: Some(IamEcstoreError::other("peer notification failed")),
            }]
        } else {
            Vec::new()
        };
        probe.completed.notify_one();
        return result;
    }

    match runtime_sources::notification_sys() {
        Some(notification_sys) => notification_sys
            .load_user(access_key, temp)
            .await
            .into_iter()
            .map(Into::into)
            .collect(),
        None => Vec::new(),
    }
}

pub(crate) async fn notify_iam_load_service_account(access_key: &str) -> Vec<IamNotificationPeerErr> {
    match runtime_sources::notification_sys() {
        Some(notification_sys) => notification_sys
            .load_service_account(access_key)
            .await
            .into_iter()
            .map(Into::into)
            .collect(),
        None => Vec::new(),
    }
}

pub(crate) async fn notify_iam_load_group(group: &str) -> Vec<IamNotificationPeerErr> {
    match runtime_sources::notification_sys() {
        Some(notification_sys) => notification_sys.load_group(group).await.into_iter().map(Into::into).collect(),
        None => Vec::new(),
    }
}

pub(crate) async fn notify_iam_load_policy_mapping(
    user_or_group: &str,
    user_type: u64,
    is_group: bool,
) -> Vec<IamNotificationPeerErr> {
    match runtime_sources::notification_sys() {
        Some(notification_sys) => notification_sys
            .load_policy_mapping(user_or_group, user_type, is_group)
            .await
            .into_iter()
            .map(Into::into)
            .collect(),
        None => Vec::new(),
    }
}

static IAM_SYS: OnceLock<Arc<IamSys<ObjectStore>>> = OnceLock::new();

/// Build an IAM system bound to the given store without touching the process
/// singleton (backlog#1052 S3): a per-server context can own the returned
/// handle while the singleton keeps serving ambient readers.
#[instrument(skip(ecstore))]
pub async fn build_iam_sys(ecstore: Arc<IamStore>) -> Result<Arc<IamSys<ObjectStore>>> {
    // 1. Create the persistent storage adapter
    let storage_adapter = ObjectStore::new(ecstore);

    // 2. Create the cache manager.
    // The `new` method now performs a blocking initial load from disk.
    let cache_manager = IamCache::new(storage_adapter).await?;

    // 3. Construct the system interface
    Ok(Arc::new(IamSys::new(cache_manager)))
}

/// Build an IAM system for an application context and publish the first one
/// as the ambient compatibility default.
#[instrument(skip(ecstore))]
pub async fn init_iam_sys_for_context(ecstore: Arc<IamStore>) -> Result<Arc<IamSys<ObjectStore>>> {
    let iam_instance = build_iam_sys(ecstore).await?;
    let _ = IAM_SYS.set(iam_instance.clone());
    Ok(iam_instance)
}

#[instrument(skip(ecstore))]
pub async fn init_iam_sys(ecstore: Arc<IamStore>) -> Result<Arc<IamSys<ObjectStore>>> {
    if let Some(existing) = IAM_SYS.get() {
        info!(
            event = EVENT_IAM_STATE,
            component = LOG_COMPONENT_IAM,
            subsystem = LOG_SUBSYSTEM_RUNTIME,
            state = "already_initialized",
            "IAM runtime already initialized"
        );
        return Ok(existing.clone());
    }

    info!(
        event = EVENT_IAM_STATE,
        component = LOG_COMPONENT_IAM,
        subsystem = LOG_SUBSYSTEM_RUNTIME,
        state = "starting",
        "IAM runtime starting"
    );

    let iam_instance = build_iam_sys(ecstore).await?;

    // Securely set the global singleton
    if IAM_SYS.set(iam_instance.clone()).is_err() {
        error!(
            event = EVENT_IAM_STATE,
            component = LOG_COMPONENT_IAM,
            subsystem = LOG_SUBSYSTEM_RUNTIME,
            state = "singleton_set_failed",
            "IAM runtime singleton set failed"
        );
        return Err(Error::IamSysAlreadyInitialized);
    }

    info!(
        event = EVENT_IAM_STATE,
        component = LOG_COMPONENT_IAM,
        subsystem = LOG_SUBSYSTEM_RUNTIME,
        state = "ready",
        "IAM runtime ready"
    );
    Ok(iam_instance)
}

#[inline]
pub fn get() -> Result<Arc<IamSys<ObjectStore>>> {
    let sys = IAM_SYS.get().map(Arc::clone).ok_or(Error::IamSysNotInitialized)?;

    // Double-check the internal readiness state. The OnceLock is only set
    // after initialization and data loading complete, so this is a defensive
    // guard to ensure callers never operate on a partially initialized system.
    if !sys.is_ready() {
        return Err(Error::IamSysNotInitialized);
    }

    Ok(sys)
}

pub fn get_global_iam_sys() -> Option<Arc<IamSys<ObjectStore>>> {
    IAM_SYS.get().cloned()
}

/// Build the OIDC system with reload-aware outbound root CA material.
///
/// Invalid OIDC setup remains non-fatal: the returned system has no providers,
/// matching the server startup behavior when OIDC cannot be initialized.
pub async fn build_oidc_sys_with_extra_root_ca_provider(extra_root_ca_provider: OidcExtraRootCaProvider) -> Result<Arc<OidcSys>> {
    debug!(
        event = EVENT_OIDC_STATE,
        component = LOG_COMPONENT_IAM,
        subsystem = LOG_SUBSYSTEM_OIDC,
        state = "starting",
        "OIDC runtime starting"
    );

    finish_oidc_sys_build(OidcSys::new_with_extra_root_ca_provider(extra_root_ca_provider).await)
}

fn finish_oidc_sys_build(oidc_sys_result: std::result::Result<OidcSys, String>) -> Result<Arc<OidcSys>> {
    let oidc_sys = match oidc_sys_result {
        Ok(sys) => {
            if sys.has_providers() {
                debug!(
                    event = EVENT_OIDC_STATE,
                    component = LOG_COMPONENT_IAM,
                    subsystem = LOG_SUBSYSTEM_OIDC,
                    provider_count = sys.provider_configs().count(),
                    state = "ready",
                    "OIDC runtime ready"
                );
            } else {
                debug!(
                    event = EVENT_OIDC_STATE,
                    component = LOG_COMPONENT_IAM,
                    subsystem = LOG_SUBSYSTEM_OIDC,
                    state = "empty",
                    "OIDC runtime has no providers"
                );
            }
            sys
        }
        Err(e) => {
            warn!(
                event = EVENT_OIDC_STATE,
                component = LOG_COMPONENT_IAM,
                subsystem = LOG_SUBSYSTEM_OIDC,
                state = "init_failed_non_fatal",
                error = %e,
                "OIDC runtime initialization failed"
            );
            OidcSys::empty().map_err(Error::StringError)?
        }
    };

    Ok(Arc::new(oidc_sys))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        io::{self, Write},
        sync::{
            Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };
    use tracing_subscriber::{Registry, fmt::MakeWriter, layer::SubscriberExt};

    #[derive(Clone, Default)]
    struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

    struct CapturedLogWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for CapturedLogWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0
                .lock()
                .expect("captured log lock should be available")
                .extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'writer> MakeWriter<'writer> for CapturedLogs {
        type Writer = CapturedLogWriter;

        fn make_writer(&'writer self) -> Self::Writer {
            CapturedLogWriter(Arc::clone(&self.0))
        }
    }

    fn capture_oidc_build(result: std::result::Result<OidcSys, String>) -> (Arc<OidcSys>, String) {
        let logs = CapturedLogs::default();
        let captured = Arc::clone(&logs.0);
        let subscriber = Registry::default().with(
            tracing_subscriber::fmt::layer()
                .without_time()
                .with_target(false)
                .with_level(true)
                .with_ansi(false)
                .with_writer(logs),
        );

        let oidc = tracing::subscriber::with_default(subscriber, || {
            finish_oidc_sys_build(result).expect("OIDC build completion should succeed")
        });
        let output = String::from_utf8(captured.lock().expect("captured log lock should be available").clone())
            .expect("captured logs should be UTF-8");
        (oidc, output)
    }

    #[test]
    fn oidc_build_reports_empty_configuration() {
        let (oidc, output) = capture_oidc_build(OidcSys::empty());

        assert!(!oidc.has_providers());
        assert!(output.contains(EVENT_OIDC_STATE), "{output}");
        assert!(output.contains("state=\"empty\""), "{output}");
    }

    #[test]
    fn oidc_build_reports_ready_configuration() {
        let oidc = crate::oidc::make_test_sys(vec![crate::oidc::test_config("default")]);
        let (oidc, output) = capture_oidc_build(Ok(oidc));

        assert!(oidc.has_providers());
        assert_eq!(oidc.config_snapshot().providers().len(), 1);
        assert!(output.contains(EVENT_OIDC_STATE), "{output}");
        assert!(output.contains("state=\"ready\""), "{output}");
        assert!(output.contains("provider_count=1"), "{output}");
    }

    #[test]
    fn oidc_build_reports_error_and_returns_empty_runtime() {
        let (oidc, output) = capture_oidc_build(Err("configured OIDC root CA is invalid".to_string()));

        assert!(!oidc.has_providers());
        assert!(output.contains("WARN"), "{output}");
        assert!(output.contains(EVENT_OIDC_STATE), "{output}");
        assert!(output.contains("state=\"init_failed_non_fatal\""), "{output}");
        assert!(output.contains("configured OIDC root CA is invalid"), "{output}");
    }

    #[tokio::test]
    async fn oidc_builder_loads_root_ca_provider_and_keeps_failure_non_fatal() {
        let load_count = Arc::new(AtomicUsize::new(0));
        let observed_load_count = Arc::clone(&load_count);
        let provider = OidcExtraRootCaProvider::new(move || {
            let observed_load_count = Arc::clone(&observed_load_count);
            async move {
                observed_load_count.fetch_add(1, Ordering::SeqCst);
                Err("test root CA load failure".to_string())
            }
        });

        let oidc = build_oidc_sys_with_extra_root_ca_provider(provider)
            .await
            .expect("root CA load failure should produce an empty OIDC runtime");

        assert_eq!(load_count.load(Ordering::SeqCst), 1);
        assert!(!oidc.has_providers());
    }
}
