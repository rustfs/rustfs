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

//! KMS service manager for dynamic configuration and runtime management

use crate::audit::KmsAuditSink;
use crate::backends::vault_credentials::CredentialTaskHandle;
use crate::backends::{KmsBackend, local::LocalKmsBackend};
use crate::config::{BackendConfig, KmsConfig};
use crate::deletion_worker::{DeletionReferenceChecker, DeletionWorker};
use crate::error::{KmsError, Result};
use crate::manager::KmsManager;
use crate::service::ObjectEncryptionService;
use arc_swap::ArcSwap;
use sha2::{Digest, Sha256};
use std::future::Future;
use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicU64, Ordering},
};
use subtle::ConstantTimeEq;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

const LOG_COMPONENT_KMS: &str = "kms";
const LOG_SUBSYSTEM_SERVICE: &str = "service";
const EVENT_KMS_SERVICE_STATE: &str = "kms_service_state";

fn local_master_key_fingerprint(master_key: Option<&str>) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update([u8::from(master_key.is_some())]);
    if let Some(master_key) = master_key {
        digest.update(master_key.as_bytes());
    }
    digest.finalize().into()
}

fn validate_local_transition(current: Option<&KmsConfig>, new: &KmsConfig) -> Result<()> {
    let Some(current) = current else {
        return Ok(());
    };
    let BackendConfig::Local(current_local) = &current.backend_config else {
        return Ok(());
    };
    let BackendConfig::Local(new_local) = &new.backend_config else {
        return Err(KmsError::configuration_error("Local KMS backend cannot be changed after configuration"));
    };

    if current_local.key_dir != new_local.key_dir {
        return Err(KmsError::configuration_error(
            "Local KMS key directory cannot be changed after configuration",
        ));
    }
    if current_local.file_permissions != new_local.file_permissions {
        return Err(KmsError::configuration_error(
            "Local KMS file permissions cannot be changed after configuration",
        ));
    }
    if current.allow_insecure_dev_defaults != new.allow_insecure_dev_defaults {
        return Err(KmsError::configuration_error(
            "Local KMS development mode cannot be changed after configuration",
        ));
    }

    let current_master_key = local_master_key_fingerprint(current_local.master_key.as_deref());
    let new_master_key = local_master_key_fingerprint(new_local.master_key.as_deref());
    if !bool::from(current_master_key.ct_eq(&new_master_key)) {
        return Err(KmsError::configuration_error(
            "Local KMS master key cannot be changed after configuration",
        ));
    }

    Ok(())
}

/// KMS service status
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum KmsServiceStatus {
    /// KMS is not configured
    NotConfigured,
    /// KMS is configured but not running
    Configured,
    /// KMS is running
    Running,
    /// KMS encountered an error
    Error(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KmsStartOutcome {
    Started,
    Restarted,
    AlreadyRunning,
}

/// Service version information for zero-downtime reconfiguration
#[derive(Clone)]
struct ServiceVersion {
    /// Service version number (monotonically increasing)
    version: u64,
    /// The encryption service instance
    service: Arc<ObjectEncryptionService>,
    /// The KMS manager instance
    manager: Arc<KmsManager>,
    /// Owner of the backend's credential renewal task, if the backend needs
    /// one. Stop shuts it down explicitly; reconfigure recycles it through
    /// the handle's cancel-on-drop behavior when the old version is discarded.
    credential_task: Option<Arc<CredentialTaskHandle>>,
    /// Background deletion worker owned by this service version, if the
    /// backend supports deletion scheduling
    deletion_worker: Option<Arc<DeletionWorkerHandle>>,
}

impl ServiceVersion {
    fn shutdown_deletion_worker(&self) {
        if let Some(worker) = &self.deletion_worker {
            worker.shutdown();
        }
    }
}

/// Cancellation handle for one service version's deletion worker.
struct DeletionWorkerHandle {
    cancel: CancellationToken,
    task: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl DeletionWorkerHandle {
    fn shutdown(&self) {
        self.cancel.cancel();
        if let Ok(mut task) = self.task.lock() {
            // Detach: the task observes the cancelled token on its next poll.
            drop(task.take());
        }
    }
}

impl Drop for DeletionWorkerHandle {
    fn drop(&mut self) {
        // Safety net for versions that are replaced without an explicit stop.
        self.cancel.cancel();
    }
}

#[derive(Clone)]
struct RuntimeState {
    config: Option<KmsConfig>,
    status: KmsServiceStatus,
    current_service: Option<ServiceVersion>,
}

/// Dynamic KMS service manager with versioned services for zero-downtime reconfiguration
pub struct KmsServiceManager {
    /// Atomically published configuration, status, and current service.
    state: ArcSwap<RuntimeState>,
    /// Version counter (monotonically increasing)
    version_counter: Arc<AtomicU64>,
    /// Mutex to protect lifecycle operations (start, stop, reconfigure)
    /// This ensures only one lifecycle operation happens at a time
    lifecycle_mutex: Arc<Mutex<()>>,
    /// External reference checker consulted before expired keys are removed
    deletion_reference_checker: std::sync::RwLock<Option<Arc<dyn DeletionReferenceChecker>>>,
    /// External sink receiving audit records for management operations
    audit_sink: std::sync::RwLock<Option<Arc<dyn KmsAuditSink>>>,
}

impl KmsServiceManager {
    /// Create a new KMS service manager (not configured)
    pub fn new() -> Self {
        Self {
            state: ArcSwap::from_pointee(RuntimeState {
                config: None,
                status: KmsServiceStatus::NotConfigured,
                current_service: None,
            }),
            version_counter: Arc::new(AtomicU64::new(0)),
            lifecycle_mutex: Arc::new(Mutex::new(())),
            deletion_reference_checker: std::sync::RwLock::new(None),
            audit_sink: std::sync::RwLock::new(None),
        }
    }

    /// Install the sink that receives an audit record for every KMS
    /// management operation. Takes effect for services built by the next
    /// start or reconfigure.
    ///
    /// Without a sink no records are built, so KMS operations are unaffected
    /// when auditing is not configured.
    pub fn set_audit_sink(&self, sink: Arc<dyn KmsAuditSink>) {
        if let Ok(mut slot) = self.audit_sink.write() {
            *slot = Some(sink);
        }
    }

    fn audit_sink(&self) -> Option<Arc<dyn KmsAuditSink>> {
        self.audit_sink.read().ok().and_then(|slot| slot.clone())
    }

    /// Install the reference checker consulted before the deletion worker
    /// removes an expired key. Takes effect for workers spawned by the next
    /// start or reconfigure.
    pub fn set_deletion_reference_checker(&self, checker: Arc<dyn DeletionReferenceChecker>) {
        if let Ok(mut slot) = self.deletion_reference_checker.write() {
            *slot = Some(checker);
        }
    }

    fn deletion_reference_checker(&self) -> Option<Arc<dyn DeletionReferenceChecker>> {
        self.deletion_reference_checker.read().ok().and_then(|slot| slot.clone())
    }

    /// Get current service status
    pub async fn get_status(&self) -> KmsServiceStatus {
        self.state.load().status.clone()
    }

    /// Get current configuration (if any)
    pub async fn get_config(&self) -> Option<KmsConfig> {
        self.state.load().config.clone()
    }

    /// Get configuration for status and management responses without static key material.
    pub async fn get_redacted_config(&self) -> Option<KmsConfig> {
        let mut config = self.state.load().config.clone()?;
        Self::redact_config(&mut config);
        Some(config)
    }

    /// Get status and redacted configuration from the same published snapshot.
    pub async fn get_redacted_state(&self) -> (KmsServiceStatus, Option<KmsConfig>) {
        let state = self.state.load();
        let mut config = state.config.clone();
        if let Some(config) = &mut config {
            Self::redact_config(config);
        }
        (state.status.clone(), config)
    }

    fn redact_config(config: &mut KmsConfig) {
        if let BackendConfig::Static(static_config) = &mut config.backend_config {
            use zeroize::Zeroize;
            static_config.secret_key.zeroize();
        }
    }

    /// Configure KMS with new configuration
    pub async fn configure(&self, new_config: KmsConfig) -> Result<()> {
        self.configure_with_persistence(new_config, || async { Ok(()) }).await
    }

    /// Configure KMS and publish the in-memory state only after persistence succeeds.
    ///
    /// The persistence callback runs under the lifecycle lock and must not call
    /// another lifecycle method on this manager.
    pub async fn configure_with_persistence<Persist, PersistFuture>(&self, new_config: KmsConfig, persist: Persist) -> Result<()>
    where
        Persist: FnOnce() -> PersistFuture,
        PersistFuture: Future<Output = Result<()>>,
    {
        new_config.validate()?;
        let _guard = self.lifecycle_mutex.lock().await;
        let current = self.state.load_full();
        validate_local_transition(current.config.as_ref(), &new_config)?;
        if current.current_service.is_some() {
            return Err(KmsError::configuration_error(
                "Cannot configure KMS while it is running; use reconfigure instead",
            ));
        }
        persist().await?;
        self.state.store(Arc::new(RuntimeState {
            config: Some(new_config),
            status: KmsServiceStatus::Configured,
            current_service: None,
        }));

        debug!(
            event = EVENT_KMS_SERVICE_STATE,
            component = LOG_COMPONENT_KMS,
            subsystem = LOG_SUBSYSTEM_SERVICE,
            state = "configured",
            "KMS service configured"
        );
        Ok(())
    }

    /// Start KMS service with current configuration
    pub async fn start(&self) -> Result<()> {
        let _guard = self.lifecycle_mutex.lock().await;
        self.start_internal().await
    }

    /// Start or restart KMS with the running-state decision serialized with the lifecycle action.
    pub async fn start_or_restart(&self, force: bool) -> Result<KmsStartOutcome> {
        let _guard = self.lifecycle_mutex.lock().await;
        let running = self.state.load().current_service.is_some();
        if running && !force {
            return Ok(KmsStartOutcome::AlreadyRunning);
        }
        self.start_internal().await?;
        Ok(if running {
            KmsStartOutcome::Restarted
        } else {
            KmsStartOutcome::Started
        })
    }

    /// Internal start implementation (called within lifecycle mutex)
    async fn start_internal(&self) -> Result<()> {
        let state = self.state.load_full();
        let config = match state.config.as_ref() {
            Some(config) => config.clone(),
            None => {
                let err_msg = "Cannot start KMS: no configuration provided";
                error!("{}", err_msg);
                self.state.store(Arc::new(RuntimeState {
                    config: None,
                    status: KmsServiceStatus::Error(err_msg.to_string()),
                    current_service: None,
                }));
                return Err(KmsError::configuration_error(err_msg));
            }
        };

        info!(
            event = EVENT_KMS_SERVICE_STATE,
            component = LOG_COMPONENT_KMS,
            subsystem = LOG_SUBSYSTEM_SERVICE,
            backend = ?config.backend,
            state = "starting",
            "KMS service starting"
        );

        match self.create_healthy_service_version(&config).await {
            Ok(service_version) => {
                self.publish_running(config, service_version);

                debug!(
                    event = EVENT_KMS_SERVICE_STATE,
                    component = LOG_COMPONENT_KMS,
                    subsystem = LOG_SUBSYSTEM_SERVICE,
                    state = "running",
                    "KMS service running"
                );
                Ok(())
            }
            Err(e) => {
                let err_msg = format!("Failed to create KMS backend: {e}");
                error!("{}", err_msg);
                if state.current_service.is_none() {
                    self.state.store(Arc::new(RuntimeState {
                        config: state.config.clone(),
                        status: KmsServiceStatus::Error(err_msg.clone()),
                        current_service: None,
                    }));
                }
                Err(KmsError::backend_error(&err_msg))
            }
        }
    }

    /// Replace the running service without exposing a stopped interval.
    pub async fn restart(&self) -> Result<()> {
        let _guard = self.lifecycle_mutex.lock().await;
        self.start_internal().await
    }

    /// Stop KMS service
    ///
    /// Note: This stops accepting new operations, but existing operations using
    /// the service will continue until they complete (due to Arc reference counting).
    pub async fn stop(&self) -> Result<()> {
        let _guard = self.lifecycle_mutex.lock().await;
        self.stop_internal().await
    }

    /// Internal stop implementation (called within lifecycle mutex)
    async fn stop_internal(&self) -> Result<()> {
        debug!(
            event = EVENT_KMS_SERVICE_STATE,
            component = LOG_COMPONENT_KMS,
            subsystem = LOG_SUBSYSTEM_SERVICE,
            state = "stopping",
            "KMS service stopping"
        );

        // Atomically clear current service version (lock-free, instant)
        // Note: Existing Arc references will keep the service alive until operations complete
        let state = self.state.load_full();
        if let Some(current) = state.current_service.as_ref() {
            current.shutdown_deletion_worker();
        }
        self.state.store(Arc::new(RuntimeState {
            config: state.config.clone(),
            status: if state.config.is_some() {
                KmsServiceStatus::Configured
            } else {
                KmsServiceStatus::NotConfigured
            },
            current_service: None,
        }));

        // Shut down the stopped version's credential renewal task before
        // reporting stopped, so stop deterministically recycles the background
        // task even while in-flight operations still hold the old service Arc.
        if let Some(task) = state.current_service.as_ref().and_then(|sv| sv.credential_task.clone()) {
            task.shutdown().await;
        }

        debug!(
            event = EVENT_KMS_SERVICE_STATE,
            component = LOG_COMPONENT_KMS,
            subsystem = LOG_SUBSYSTEM_SERVICE,
            state = "configured",
            "KMS service stopped"
        );
        Ok(())
    }

    /// Reconfigure and restart KMS service with zero-downtime
    ///
    /// This method implements versioned service switching:
    /// 1. Creates a new service version without stopping the old one
    /// 2. Atomically switches to the new version
    /// 3. Old operations continue using the old service (via Arc reference counting)
    /// 4. New operations automatically use the new service
    ///
    /// This ensures zero downtime during reconfiguration, even for long-running
    /// operations like encrypting large files.
    pub async fn reconfigure(&self, new_config: KmsConfig) -> Result<()> {
        self.reconfigure_with_persistence(new_config, || async { Ok(()) }).await
    }

    /// Reconfigure KMS after the candidate is healthy and persistence succeeds.
    ///
    /// The persistence callback runs under the lifecycle lock and must not call
    /// another lifecycle method on this manager.
    pub async fn reconfigure_with_persistence<Persist, PersistFuture>(
        &self,
        new_config: KmsConfig,
        persist: Persist,
    ) -> Result<()>
    where
        Persist: FnOnce() -> PersistFuture,
        PersistFuture: Future<Output = Result<()>>,
    {
        let _guard = self.lifecycle_mutex.lock().await;

        debug!(
            event = EVENT_KMS_SERVICE_STATE,
            component = LOG_COMPONENT_KMS,
            subsystem = LOG_SUBSYSTEM_SERVICE,
            state = "reconfiguring",
            "KMS service reconfiguring"
        );
        new_config.validate()?;
        validate_local_transition(self.state.load().config.as_ref(), &new_config)?;

        // Create new service version without stopping old one
        // This allows existing operations to continue while new operations use new service
        match self.create_healthy_service_version(&new_config).await {
            Ok(new_service_version) => {
                // Get old version for logging (lock-free read)
                let old_version = self.state.load().current_service.as_ref().map(|sv| sv.version);

                persist().await?;

                self.publish_running(new_config, new_service_version.clone());

                if let Some(old_ver) = old_version {
                    info!(
                        event = EVENT_KMS_SERVICE_STATE,
                        component = LOG_COMPONENT_KMS,
                        subsystem = LOG_SUBSYSTEM_SERVICE,
                        old_version = old_ver,
                        new_version = new_service_version.version,
                        state = "running",
                        "KMS service reconfigured"
                    );
                } else {
                    info!(
                        event = EVENT_KMS_SERVICE_STATE,
                        component = LOG_COMPONENT_KMS,
                        subsystem = LOG_SUBSYSTEM_SERVICE,
                        new_version = new_service_version.version,
                        state = "running",
                        "KMS service started from reconfigure"
                    );
                }
                Ok(())
            }
            Err(e) => {
                let err_msg = format!("Failed to reconfigure KMS: {e}");
                error!("{}", err_msg);
                Err(KmsError::backend_error(&err_msg))
            }
        }
    }

    /// Get KMS manager (if running)
    ///
    /// Returns the manager from the current service version.
    /// Uses lock-free atomic load for optimal performance.
    pub async fn get_manager(&self) -> Option<Arc<KmsManager>> {
        self.state.load().current_service.as_ref().map(|sv| sv.manager.clone())
    }

    /// Get encryption service (if running)
    ///
    /// Returns the service from the current service version.
    /// Uses lock-free atomic load - no blocking, instant access.
    /// This ensures new operations always use the latest service version,
    /// while existing operations continue using their Arc references.
    pub async fn get_encryption_service(&self) -> Option<Arc<ObjectEncryptionService>> {
        self.state.load().current_service.as_ref().map(|sv| sv.service.clone())
    }

    /// Get current service version number
    ///
    /// Useful for monitoring and debugging.
    /// Uses lock-free atomic load.
    pub async fn get_service_version(&self) -> Option<u64> {
        self.state.load().current_service.as_ref().map(|sv| sv.version)
    }

    /// Health check for the KMS service
    pub async fn health_check(&self) -> Result<bool> {
        let checked_state = self.state.load_full();
        match checked_state.current_service.as_ref() {
            Some(service_version) => {
                let manager = service_version.manager.clone();
                let checked_version = service_version.version;
                // Perform health check on the backend
                match manager.health_check().await {
                    Ok(healthy) => {
                        if !healthy {
                            warn!("KMS backend health check failed");
                        }
                        Ok(healthy)
                    }
                    Err(e) => {
                        error!("KMS health check error: {}", e);
                        let _guard = self.lifecycle_mutex.lock().await;
                        self.mark_health_error_if_current(checked_version, &e);
                        Err(e)
                    }
                }
            }
            None => {
                warn!("Cannot perform health check: KMS service not running");
                Ok(false)
            }
        }
    }

    /// Create a new service version from configuration
    ///
    /// This creates a new backend, manager, and service, and assigns it a new version number.
    async fn create_service_version(&self, config: &KmsConfig) -> Result<ServiceVersion> {
        config.validate()?;

        // Increment version counter
        let version = self.version_counter.fetch_add(1, Ordering::Relaxed) + 1;

        info!("Creating KMS service version {} with backend: {:?}", version, config.backend);

        // Create backend. Vault backends may also spawn a background
        // credential renewal task whose owner handle lives on the service
        // version, so replacing the version recycles the task.
        let mut credential_task = None;
        let backend = match &config.backend_config {
            BackendConfig::Local(_) => {
                info!("Creating Local KMS backend for version {}", version);
                let backend = LocalKmsBackend::new(config.clone()).await?;
                Arc::new(backend) as Arc<dyn KmsBackend>
            }
            BackendConfig::VaultKv2(_) => {
                info!("Creating Vault KV2 KMS backend for version {}", version);
                let backend = crate::backends::vault::VaultKmsBackend::new(config.clone()).await?;
                credential_task = backend.spawn_credential_renewal().map(Arc::new);
                Arc::new(backend) as Arc<dyn KmsBackend>
            }
            BackendConfig::VaultTransit(_) => {
                info!("Creating Vault Transit KMS backend for version {}", version);
                let backend = crate::backends::vault_transit::VaultTransitKmsBackend::new(config.clone()).await?;
                credential_task = backend.spawn_credential_renewal().map(Arc::new);
                Arc::new(backend) as Arc<dyn KmsBackend>
            }
            BackendConfig::Static(_) => {
                info!("Creating Static KMS backend for version {}", version);
                let backend = crate::backends::static_kms::StaticKmsBackend::new(config.clone()).await?;
                Arc::new(backend) as Arc<dyn KmsBackend>
            }
        };

        // Create KMS manager
        let mut kms_manager = KmsManager::new(backend, config.clone());
        if let Some(sink) = self.audit_sink() {
            kms_manager = kms_manager.with_audit_sink(sink);
        }
        let kms_manager = Arc::new(kms_manager);

        // Create encryption service
        let encryption_service = Arc::new(ObjectEncryptionService::new((*kms_manager).clone()));

        Ok(ServiceVersion {
            version,
            service: encryption_service,
            manager: kms_manager,
            credential_task,
            deletion_worker: None,
        })
    }

    async fn create_healthy_service_version(&self, config: &KmsConfig) -> Result<ServiceVersion> {
        let service_version = self.create_service_version(config).await?;
        if !service_version.manager.health_check().await? {
            return Err(KmsError::backend_error("KMS backend health check failed"));
        }
        Ok(service_version)
    }

    fn publish_running(&self, config: KmsConfig, mut service_version: ServiceVersion) {
        if let Some(previous) = self.state.load().current_service.as_ref() {
            previous.shutdown_deletion_worker();
        }
        service_version.deletion_worker = self.spawn_deletion_worker(&config, &service_version);
        self.state.store(Arc::new(RuntimeState {
            config: Some(config),
            status: KmsServiceStatus::Running,
            current_service: Some(service_version),
        }));
    }

    /// Spawn the background deletion worker for a service version about to be
    /// published, if its backend supports deletion scheduling. The worker is
    /// only started at publish time so failed start/reconfigure candidates
    /// never leak a running task.
    fn spawn_deletion_worker(&self, config: &KmsConfig, service_version: &ServiceVersion) -> Option<Arc<DeletionWorkerHandle>> {
        let backend = service_version.manager.backend();
        if !backend.capabilities().schedule_deletion {
            return None;
        }
        let cancel = CancellationToken::new();
        let worker = DeletionWorker::new(
            backend,
            config.default_key_id.clone(),
            self.deletion_reference_checker(),
            config.backend.as_str(),
        )
        .with_audit_sink(self.audit_sink());
        let task = worker.spawn(cancel.clone());
        Some(Arc::new(DeletionWorkerHandle {
            cancel,
            task: std::sync::Mutex::new(Some(task)),
        }))
    }

    fn mark_health_error_if_current(&self, checked_version: u64, error: &KmsError) {
        let current = self.state.load_full();
        if current.current_service.as_ref().map(|version| version.version) == Some(checked_version) {
            self.state.store(Arc::new(RuntimeState {
                config: current.config.clone(),
                status: KmsServiceStatus::Error(format!("Health check failed: {error}")),
                current_service: current.current_service.clone(),
            }));
        }
    }
}

impl Default for KmsServiceManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Global KMS service manager instance
static GLOBAL_KMS_SERVICE_MANAGER: OnceLock<Arc<KmsServiceManager>> = OnceLock::new();

/// Initialize global KMS service manager
pub fn init_global_kms_service_manager() -> Arc<KmsServiceManager> {
    GLOBAL_KMS_SERVICE_MANAGER
        .get_or_init(|| Arc::new(KmsServiceManager::new()))
        .clone()
}

/// Get global KMS service manager
pub fn get_global_kms_service_manager() -> Option<Arc<KmsServiceManager>> {
    GLOBAL_KMS_SERVICE_MANAGER.get().cloned()
}

/// Get global encryption service (if KMS is running)
pub async fn get_global_encryption_service() -> Option<Arc<ObjectEncryptionService>> {
    let manager = get_global_kms_service_manager().unwrap_or_else(init_global_kms_service_manager);
    manager.get_encryption_service().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};

    fn static_config(key_id: &str, fill: u8) -> KmsConfig {
        KmsConfig::static_kms(key_id.to_string(), BASE64_STANDARD.encode([fill; 32]))
    }

    #[tokio::test]
    async fn configure_rejects_insecure_development_defaults_before_state_update() {
        let manager = KmsServiceManager::new();

        let error = manager
            .configure(KmsConfig::default())
            .await
            .expect_err("unsafe local defaults should fail validation");

        assert!(error.to_string().contains(crate::config::ENV_KMS_ALLOW_INSECURE_DEV_DEFAULTS));
        assert_eq!(manager.get_status().await, KmsServiceStatus::NotConfigured);
        assert!(manager.get_config().await.is_none());
    }

    #[tokio::test]
    async fn redacted_config_omits_static_key_material() {
        let manager = KmsServiceManager::new();
        let encoded_key = base64::engine::general_purpose::STANDARD.encode([0x5au8; 32]);
        manager
            .configure(KmsConfig::static_kms("static-key".to_string(), encoded_key))
            .await
            .expect("configure static KMS");

        let config = manager.get_redacted_config().await.expect("redacted config");
        let BackendConfig::Static(static_config) = config.backend_config else {
            panic!("expected static config");
        };
        assert!(static_config.secret_key.is_empty());
    }

    #[tokio::test]
    async fn configure_persistence_failure_leaves_state_unchanged() {
        let manager = KmsServiceManager::new();

        let result = manager
            .configure_with_persistence(static_config("key-a", 0x11), || async { Err(KmsError::backend_error("persist failed")) })
            .await;

        assert!(result.is_err());
        assert_eq!(manager.get_status().await, KmsServiceStatus::NotConfigured);
        assert!(manager.get_config().await.is_none());
        assert!(manager.get_encryption_service().await.is_none());
    }

    #[tokio::test]
    async fn configure_rejects_running_service_without_changing_snapshot() {
        let manager = KmsServiceManager::new();
        manager.configure(static_config("key-a", 0x11)).await.expect("configure");
        manager.start().await.expect("start");
        let version = manager.get_service_version().await;

        let result = manager.configure(static_config("key-b", 0x22)).await;

        assert!(result.is_err());
        assert_eq!(manager.get_status().await, KmsServiceStatus::Running);
        assert_eq!(manager.get_service_version().await, version);
        assert_eq!(
            manager.get_config().await.and_then(|config| config.default_key_id),
            Some("key-a".to_string())
        );
    }

    #[tokio::test]
    async fn reconfigure_persistence_failure_keeps_old_running_snapshot() {
        let manager = KmsServiceManager::new();
        manager.configure(static_config("key-a", 0x11)).await.expect("configure");
        manager.start().await.expect("start");
        let old_version = manager.get_service_version().await;
        let old_service = manager.get_encryption_service().await.expect("old service");

        let result = manager
            .reconfigure_with_persistence(static_config("key-b", 0x22), || async {
                Err(KmsError::backend_error("persist failed"))
            })
            .await;

        assert!(result.is_err());
        assert_eq!(manager.get_status().await, KmsServiceStatus::Running);
        assert_eq!(manager.get_service_version().await, old_version);
        assert_eq!(
            manager.get_config().await.and_then(|config| config.default_key_id),
            Some("key-a".to_string())
        );
        assert!(Arc::ptr_eq(
            &old_service,
            &manager.get_encryption_service().await.expect("old service remains")
        ));
    }

    #[tokio::test]
    async fn reconfigure_candidate_failure_keeps_old_running_snapshot() {
        let manager = KmsServiceManager::new();
        manager.configure(static_config("key-a", 0x11)).await.expect("configure");
        manager.start().await.expect("start");
        let old_version = manager.get_service_version().await;
        let invalid_parent = tempfile::NamedTempFile::new().expect("temporary file");
        let invalid_config = KmsConfig::local(invalid_parent.path().join("keys")).with_insecure_development_defaults();

        let result = manager.reconfigure(invalid_config).await;

        assert!(result.is_err());
        assert_eq!(manager.get_status().await, KmsServiceStatus::Running);
        assert_eq!(manager.get_service_version().await, old_version);
        assert_eq!(
            manager.get_config().await.and_then(|config| config.default_key_id),
            Some("key-a".to_string())
        );
    }

    #[tokio::test]
    async fn restart_never_unpublishes_the_running_service() {
        let manager = Arc::new(KmsServiceManager::new());
        manager.configure(static_config("key-a", 0x11)).await.expect("configure");
        manager.start().await.expect("start");
        let old_version = manager.get_service_version().await.expect("old version");
        let restarting = {
            let manager = manager.clone();
            tokio::spawn(async move { manager.restart().await })
        };

        while !restarting.is_finished() {
            assert!(manager.get_encryption_service().await.is_some());
            tokio::task::yield_now().await;
        }
        restarting.await.expect("restart task").expect("restart");

        assert!(manager.get_encryption_service().await.is_some());
        assert!(manager.get_service_version().await.expect("new version") > old_version);
        assert_eq!(manager.get_status().await, KmsServiceStatus::Running);
    }

    #[tokio::test]
    async fn start_or_restart_decides_under_the_lifecycle_lock() {
        let manager = KmsServiceManager::new();
        manager.configure(static_config("key-a", 0x11)).await.expect("configure");

        assert_eq!(manager.start_or_restart(false).await.expect("initial start"), KmsStartOutcome::Started);
        let first_version = manager.get_service_version().await.expect("first version");
        assert_eq!(
            manager.start_or_restart(false).await.expect("already running"),
            KmsStartOutcome::AlreadyRunning
        );
        assert_eq!(manager.get_service_version().await, Some(first_version));
        assert_eq!(manager.start_or_restart(true).await.expect("forced restart"), KmsStartOutcome::Restarted);
        assert!(manager.get_service_version().await.expect("restarted version") > first_version);
    }

    #[tokio::test]
    async fn stale_health_failure_cannot_poison_new_service_status() {
        let manager = KmsServiceManager::new();
        manager.configure(static_config("key-a", 0x11)).await.expect("configure");
        manager.start().await.expect("start");
        let old_version = manager.get_service_version().await.expect("old version");
        manager.restart().await.expect("restart");

        manager.mark_health_error_if_current(old_version, &KmsError::backend_error("stale failure"));

        assert_eq!(manager.get_status().await, KmsServiceStatus::Running);
    }

    #[tokio::test]
    async fn forbidden_local_master_key_change_preserves_running_config_and_service() {
        use crate::types::{CreateKeyRequest, KeyUsage};
        use std::collections::HashMap;
        use tempfile::TempDir;

        let key_dir = TempDir::new().expect("create local KMS directory");
        let config = |master_key: &str| {
            let mut config = KmsConfig::local(key_dir.path().to_path_buf());
            let BackendConfig::Local(local) = &mut config.backend_config else {
                panic!("local constructor must create local backend config");
            };
            local.master_key = Some(master_key.to_string());
            config.allow_insecure_dev_defaults = true;
            config
        };
        let manager = KmsServiceManager::new();
        manager
            .configure(config("working-master-key"))
            .await
            .expect("configure local KMS");
        manager.start().await.expect("start local KMS");
        manager
            .get_manager()
            .await
            .expect("running KMS manager")
            .create_key(CreateKeyRequest {
                key_name: Some("existing-key".to_string()),
                key_usage: KeyUsage::EncryptDecrypt,
                description: None,
                policy: None,
                tags: HashMap::new(),
                origin: None,
            })
            .await
            .expect("create encrypted key");
        let service_version = manager.get_service_version().await;

        let error = manager
            .reconfigure(config("wrong-master-key"))
            .await
            .expect_err("local master key change must be rejected");

        assert!(error.to_string().contains("master key cannot be changed"));
        assert_eq!(manager.get_status().await, KmsServiceStatus::Running);
        assert_eq!(manager.get_service_version().await, service_version);
        let current = manager.get_config().await.expect("working config must remain");
        let BackendConfig::Local(local) = current.backend_config else {
            panic!("working config must remain local");
        };
        assert_eq!(local.master_key.as_deref(), Some("working-master-key"));
    }

    #[tokio::test]
    async fn configure_cannot_replace_existing_local_backend() {
        use base64::Engine as _;
        use tempfile::TempDir;

        let key_dir = TempDir::new().expect("create local KMS directory");
        let mut local = KmsConfig::local(key_dir.path().to_path_buf());
        local.allow_insecure_dev_defaults = true;
        let manager = KmsServiceManager::new();
        manager.configure(local.clone()).await.expect("configure local KMS");

        let encoded_key = base64::engine::general_purpose::STANDARD.encode([0x5au8; 32]);
        let error = manager
            .configure(KmsConfig::static_kms("static-key".to_string(), encoded_key))
            .await
            .expect_err("existing local backend must be immutable");

        assert!(error.to_string().contains("backend cannot be changed"));
        let current = manager.get_config().await.expect("local config must remain");
        assert!(matches!(current.backend_config, BackendConfig::Local(_)));
    }

    #[tokio::test]
    async fn deletion_worker_follows_the_service_lifecycle() {
        use tempfile::TempDir;

        let key_dir = TempDir::new().expect("create local KMS directory");
        let mut config = KmsConfig::local(key_dir.path().to_path_buf());
        config.allow_insecure_dev_defaults = true;
        let manager = KmsServiceManager::new();
        manager.configure(config).await.expect("configure local KMS");
        manager.start().await.expect("start local KMS");

        let first_worker = manager
            .state
            .load()
            .current_service
            .as_ref()
            .expect("running service")
            .deletion_worker
            .clone()
            .expect("local backend must run a deletion worker");
        assert!(!first_worker.cancel.is_cancelled());

        // Replacing the service version replaces (and cancels) its worker.
        manager.restart().await.expect("restart");
        assert!(first_worker.cancel.is_cancelled(), "replaced version's worker must be cancelled");
        let second_worker = manager
            .state
            .load()
            .current_service
            .as_ref()
            .expect("running service")
            .deletion_worker
            .clone()
            .expect("restarted service must run a fresh worker");
        assert!(!second_worker.cancel.is_cancelled());

        // Stopping the service stops its worker.
        manager.stop().await.expect("stop");
        assert!(second_worker.cancel.is_cancelled(), "stop must cancel the deletion worker");
    }

    #[tokio::test]
    async fn static_backend_runs_no_deletion_worker() {
        let manager = KmsServiceManager::new();
        manager.configure(static_config("key-a", 0x11)).await.expect("configure");
        manager.start().await.expect("start");

        assert!(
            manager
                .state
                .load()
                .current_service
                .as_ref()
                .expect("running service")
                .deletion_worker
                .is_none(),
            "a backend without deletion scheduling must not run a worker"
        );
    }

    #[tokio::test]
    async fn reconfigure_allows_safe_local_runtime_settings_only() {
        use tempfile::TempDir;

        let key_dir = TempDir::new().expect("create local KMS directory");
        let mut initial = KmsConfig::local(key_dir.path().to_path_buf());
        initial.allow_insecure_dev_defaults = true;
        let manager = KmsServiceManager::new();
        manager.configure(initial.clone()).await.expect("configure local KMS");
        manager.start().await.expect("start local KMS");

        let mut updated = initial;
        updated.default_key_id = Some("evaluation-key".to_string());
        updated.timeout = std::time::Duration::from_secs(45);
        updated.enable_cache = false;
        manager
            .reconfigure(updated.clone())
            .await
            .expect("update safe local settings");

        let current = manager.get_config().await.expect("updated config");
        assert_eq!(current.default_key_id, updated.default_key_id);
        assert_eq!(current.timeout, updated.timeout);
        assert!(!current.enable_cache);
    }
}
