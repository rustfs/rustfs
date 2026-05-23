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

use crate::{
    StoreError, Target,
    arn::TargetID,
    error::TargetError,
    runtime::tls::{
        ReloadableTargetTls, TargetTlsGeneration, TargetTlsInputSet, TargetTlsReloadCoordinator, TargetTlsRuntimeState,
        config::ReloadApplyMode, validate_tls_material,
    },
    store::{Key, Store},
    target::{
        ChannelTargetType, EntityTarget, QueuedPayload, QueuedPayloadMeta, TargetDeliveryCounters, TargetDeliverySnapshot,
        TargetTlsState, TargetType, build_queued_payload_with_records, build_target_tls_fingerprint, open_target_queue_store,
        persist_queued_payload_to_store,
    },
};
use async_trait::async_trait;
use rustfs_config::{NATS_CREDENTIALS_FILE, NATS_TLS_CA, NATS_TLS_CLIENT_CERT, NATS_TLS_CLIENT_KEY};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{info, instrument, warn};

#[derive(Debug, Clone)]
pub struct NATSArgs {
    pub enable: bool,
    pub address: String,
    pub subject: String,
    pub username: String,
    pub password: String,
    pub token: String,
    pub credentials_file: String,
    pub tls_ca: String,
    pub tls_client_cert: String,
    pub tls_client_key: String,
    pub tls_required: bool,
    pub queue_dir: String,
    pub queue_limit: u64,
    pub target_type: TargetType,
}

impl NATSArgs {
    pub fn validate(&self) -> Result<(), TargetError> {
        if !self.enable {
            return Ok(());
        }

        validate_nats_address(&self.address)?;
        validate_nats_auth(self)?;

        if self.subject.trim().is_empty() || self.subject.chars().any(char::is_whitespace) {
            return Err(TargetError::Configuration(
                "NATS subject cannot be empty or contain whitespace".to_string(),
            ));
        }

        if !self.credentials_file.is_empty() && !Path::new(&self.credentials_file).is_absolute() {
            return Err(TargetError::Configuration(format!("{NATS_CREDENTIALS_FILE} must be an absolute path")));
        }
        if !self.tls_ca.is_empty() && !Path::new(&self.tls_ca).is_absolute() {
            return Err(TargetError::Configuration(format!("{NATS_TLS_CA} must be an absolute path")));
        }
        if !self.tls_client_cert.is_empty() && !Path::new(&self.tls_client_cert).is_absolute() {
            return Err(TargetError::Configuration(format!("{NATS_TLS_CLIENT_CERT} must be an absolute path")));
        }
        if !self.tls_client_key.is_empty() && !Path::new(&self.tls_client_key).is_absolute() {
            return Err(TargetError::Configuration(format!("{NATS_TLS_CLIENT_KEY} must be an absolute path")));
        }
        if self.tls_client_cert.is_empty() != self.tls_client_key.is_empty() {
            return Err(TargetError::Configuration(
                "NATS tls_client_cert and tls_client_key must be specified together".to_string(),
            ));
        }

        if !self.queue_dir.is_empty() && !Path::new(&self.queue_dir).is_absolute() {
            return Err(TargetError::Configuration("NATS queue directory must be an absolute path".to_string()));
        }

        Ok(())
    }
}

pub fn validate_nats_address(address: &str) -> Result<async_nats::ServerAddr, TargetError> {
    let server = async_nats::ServerAddr::from_str(address)
        .map_err(|e| TargetError::Configuration(format!("Invalid NATS address: {e}")))?;

    if server.has_user_pass() {
        return Err(TargetError::Configuration("NATS address must not embed username or password".to_string()));
    }

    Ok(server)
}

fn validate_nats_auth(args: &NATSArgs) -> Result<(), TargetError> {
    let mut auth_methods = 0usize;

    if !args.token.is_empty() {
        auth_methods += 1;
    }

    if !args.credentials_file.is_empty() {
        auth_methods += 1;
    }

    let has_user = !args.username.is_empty();
    let has_password = !args.password.is_empty();
    if has_user || has_password {
        if has_user != has_password {
            return Err(TargetError::Configuration(
                "NATS username and password must be specified together".to_string(),
            ));
        }
        auth_methods += 1;
    }

    if auth_methods > 1 {
        return Err(TargetError::Configuration(
            "NATS supports only one auth method at a time: token, username/password, or credentials_file".to_string(),
        ));
    }

    Ok(())
}

pub async fn connect_nats(args: &NATSArgs) -> Result<async_nats::Client, TargetError> {
    args.validate()?;

    let mut options = async_nats::ConnectOptions::new().require_tls(args.tls_required);

    if !args.token.is_empty() {
        options = options.token(args.token.clone());
    } else if !args.username.is_empty() {
        options = options.user_and_password(args.username.clone(), args.password.clone());
    } else if !args.credentials_file.is_empty() {
        options = options
            .credentials_file(&args.credentials_file)
            .await
            .map_err(|e| TargetError::Configuration(format!("Failed to load NATS credentials file: {e}")))?;
    }

    if !args.tls_ca.is_empty() {
        options = options.add_root_certificates(PathBuf::from(&args.tls_ca));
    }
    if !args.tls_client_cert.is_empty() {
        options = options.add_client_certificate(PathBuf::from(&args.tls_client_cert), PathBuf::from(&args.tls_client_key));
    }

    options
        .connect(args.address.clone())
        .await
        .map_err(|e| TargetError::Network(format!("Failed to connect to NATS server: {e}")))
}

pub struct NATSTarget<E>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
{
    id: TargetID,
    args: NATSArgs,
    client: Arc<Mutex<Option<async_nats::Client>>>,
    tls_state: Arc<parking_lot::Mutex<TargetTlsState>>,
    /// Optional coordinated TLS reload runtime state. When present, the target
    /// uses the coordinator's material directly instead of the inline fingerprint check.
    tls_runtime_state: Option<Arc<TargetTlsRuntimeState<async_nats::Client>>>,
    store: Option<Box<dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send + Sync>>,
    connected: AtomicBool,
    delivery_counters: Arc<TargetDeliveryCounters>,
    _phantom: std::marker::PhantomData<E>,
}

impl<E> NATSTarget<E>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
{
    pub fn clone_box(&self) -> Box<dyn Target<E> + Send + Sync> {
        Box::new(NATSTarget::<E> {
            id: self.id.clone(),
            args: self.args.clone(),
            client: Arc::clone(&self.client),
            tls_state: Arc::clone(&self.tls_state),
            tls_runtime_state: self.tls_runtime_state.clone(),
            store: self.store.as_ref().map(|s| s.boxed_clone()),
            connected: AtomicBool::new(self.connected.load(Ordering::SeqCst)),
            delivery_counters: Arc::clone(&self.delivery_counters),
            _phantom: std::marker::PhantomData,
        })
    }

    #[instrument(skip(args), fields(target_id_as_string = %id))]
    pub fn new(id: String, args: NATSArgs) -> Result<Self, TargetError> {
        args.validate()?;
        let target_id = TargetID::new(id, ChannelTargetType::Nats.as_str().to_string());
        let queue_store = open_target_queue_store(
            &args.queue_dir,
            args.queue_limit,
            args.target_type,
            ChannelTargetType::Nats.as_str(),
            &target_id,
            "Failed to open store for NATS target",
        )?;

        Ok(Self {
            id: target_id,
            args,
            client: Arc::new(Mutex::new(None)),
            tls_state: Arc::new(parking_lot::Mutex::new(TargetTlsState::default())),
            tls_runtime_state: None,
            store: queue_store,
            connected: AtomicBool::new(false),
            delivery_counters: Arc::new(TargetDeliveryCounters::default()),
            _phantom: std::marker::PhantomData,
        })
    }

    async fn invalidate_cached_client_connection(&self) {
        *self.client.lock().await = None;
    }

    async fn get_or_connect(&self) -> Result<async_nats::Client, TargetError> {
        // Coordinator-managed path: use the material directly from the runtime state.
        if let Some(runtime_state) = &self.tls_runtime_state {
            let published = runtime_state.current.load();
            let client: async_nats::Client = (*published.material).clone();

            // Ensure the client is also stored locally so that close() can drain it.
            {
                let mut guard = self.client.lock().await;
                *guard = Some(client.clone());
            }
            return Ok(client);
        }

        // Inline fingerprint fallback path (no coordinator).
        let next_fingerprint =
            build_target_tls_fingerprint(&self.args.tls_ca, &self.args.tls_client_cert, &self.args.tls_client_key).await?;
        let tls_changed = {
            let mut tls_state_guard = self.tls_state.lock();
            tls_state_guard.refresh(next_fingerprint)
        };
        if tls_changed {
            self.invalidate_cached_client_connection().await;
        }

        {
            let guard = self.client.lock().await;
            if let Some(client) = guard.as_ref() {
                return Ok(client.clone());
            }
        }

        let client = connect_nats(&self.args).await?;
        client
            .flush()
            .await
            .map_err(|e| TargetError::Network(format!("Failed to flush NATS connection: {e}")))?;
        self.connected.store(true, Ordering::SeqCst);

        let mut guard = self.client.lock().await;
        let shared = guard.get_or_insert_with(|| client.clone()).clone();
        Ok(shared)
    }

    fn build_queued_payload(&self, event: &EntityTarget<E>) -> Result<QueuedPayload, TargetError> {
        build_queued_payload_with_records(event, vec![event.clone()])
    }

    async fn send_body(&self, body: Vec<u8>) -> Result<(), TargetError> {
        let client = self.get_or_connect().await?;
        client
            .publish(self.args.subject.clone(), body.into())
            .await
            .map_err(|e| TargetError::Request(format!("Failed to publish NATS message: {e}")))?;
        self.delivery_counters.record_success();
        Ok(())
    }

    /// Registers this target with a TLS reload coordinator.
    ///
    /// When TLS files are configured, the coordinator takes over certificate
    /// hot-reload via background polling. When TLS is not configured or the
    /// coordinator fails to register, the target falls back to inline
    /// fingerprint-based change detection.
    ///
    /// This method is async and must be called from a tokio runtime context.
    pub async fn register_tls_reload(&mut self, coordinator: Arc<TargetTlsReloadCoordinator>) {
        let has_tls_config =
            !self.args.tls_ca.is_empty() || !self.args.tls_client_cert.is_empty() || !self.args.tls_client_key.is_empty();

        if !has_tls_config {
            return;
        }

        let shell = Arc::new(Self {
            id: self.id.clone(),
            args: self.args.clone(),
            client: Arc::new(Mutex::new(None)),
            tls_state: Arc::new(parking_lot::Mutex::new(TargetTlsState::default())),
            tls_runtime_state: None,
            store: None,
            connected: AtomicBool::new(false),
            delivery_counters: Arc::new(TargetDeliveryCounters::default()),
            _phantom: std::marker::PhantomData,
        });

        let options = rustfs_tls_runtime::config::TlsReloadOptions::default();
        match coordinator.register(shell, options).await {
            Ok(state) => {
                info!(target_id = %self.id.id, "NATS target registered with TLS reload coordinator");
                self.tls_runtime_state = Some(state);
            }
            Err(err) => {
                warn!(target_id = %self.id.id, error = %err, "Failed to register NATS target with TLS reload coordinator; falling back to inline fingerprint");
            }
        }
    }
}

#[async_trait]
impl<E> Target<E> for NATSTarget<E>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
{
    fn id(&self) -> TargetID {
        self.id.clone()
    }

    async fn is_active(&self) -> Result<bool, TargetError> {
        let client = self.get_or_connect().await?;
        client
            .flush()
            .await
            .map_err(|e| TargetError::Network(format!("NATS health check failed: {e}")))?;
        Ok(true)
    }

    async fn save(&self, event: Arc<EntityTarget<E>>) -> Result<(), TargetError> {
        let queued = match self.build_queued_payload(&event) {
            Ok(queued) => queued,
            Err(err) => {
                self.delivery_counters.record_final_failure();
                return Err(err);
            }
        };

        if let Some(store) = &self.store {
            if let Err(e) = persist_queued_payload_to_store(store.as_ref(), &queued) {
                self.delivery_counters.record_final_failure();
                return Err(e);
            }
            Ok(())
        } else {
            if let Err(err) = self.send_body(queued.body).await {
                self.delivery_counters.record_final_failure();
                return Err(err);
            }
            Ok(())
        }
    }

    async fn send_raw_from_store(&self, _key: Key, body: Vec<u8>, _meta: QueuedPayloadMeta) -> Result<(), TargetError> {
        self.send_body(body).await
    }

    async fn close(&self) -> Result<(), TargetError> {
        let client = {
            let mut guard = self.client.lock().await;
            guard.take()
        };
        self.tls_state.lock().reset();
        // If a coordinator runtime state is attached, reset its error tracking
        // so that a future re-init does not inherit stale failure state.
        if let Some(runtime_state) = &self.tls_runtime_state {
            *runtime_state.last_error.write() = None;
        }
        self.connected.store(false, Ordering::SeqCst);
        if let Some(client) = client {
            client
                .drain()
                .await
                .map_err(|e| TargetError::Network(format!("Failed to drain NATS client: {e}")))?;
        }
        info!(target_id = %self.id, "NATS target closed");
        Ok(())
    }

    fn store(&self) -> Option<&(dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send + Sync)> {
        self.store.as_deref()
    }

    fn clone_dyn(&self) -> Box<dyn Target<E> + Send + Sync> {
        self.clone_box()
    }

    async fn init(&self) -> Result<(), TargetError> {
        if !self.is_enabled() {
            return Ok(());
        }
        let _ = self.get_or_connect().await?;
        Ok(())
    }

    fn is_enabled(&self) -> bool {
        self.args.enable
    }

    fn delivery_snapshot(&self) -> TargetDeliverySnapshot {
        self.delivery_counters
            .snapshot(self.store.as_deref().map_or(0, |store| store.len() as u64))
    }

    fn record_final_failure(&self) {
        self.delivery_counters.record_final_failure();
    }
}

/// Coordinated TLS hot-reload implementation for NATS targets.
///
/// The coordinator calls these methods on a background poll loop to detect
/// TLS file changes and rebuild the NATS client without restarting.
#[async_trait]
impl<E> ReloadableTargetTls for NATSTarget<E>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
{
    type Material = async_nats::Client;

    fn tls_input_set(&self) -> TargetTlsInputSet {
        TargetTlsInputSet {
            ca_path: self.args.tls_ca.clone(),
            client_cert_path: self.args.tls_client_cert.clone(),
            client_key_path: self.args.tls_client_key.clone(),
            target_label: format!("nats:{}", self.id.id),
        }
    }

    async fn build_tls_material(&self) -> Result<Self::Material, TargetError> {
        connect_nats(&self.args).await
    }

    async fn apply_tls_material(
        &self,
        _generation: TargetTlsGeneration,
        material: Arc<Self::Material>,
        _mode: ReloadApplyMode,
    ) -> Result<(), TargetError> {
        let mut guard = self.client.lock().await;
        *guard = Some((*material).clone());
        Ok(())
    }

    async fn validate_tls_files(&self) -> Result<(), TargetError> {
        validate_tls_material(&self.args.tls_ca, &self.args.tls_client_cert, &self.args.tls_client_key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_args() -> NATSArgs {
        NATSArgs {
            enable: true,
            address: "nats://127.0.0.1:4222".to_string(),
            subject: "rustfs.events".to_string(),
            username: String::new(),
            password: String::new(),
            token: String::new(),
            credentials_file: String::new(),
            tls_ca: String::new(),
            tls_client_cert: String::new(),
            tls_client_key: String::new(),
            tls_required: false,
            queue_dir: String::new(),
            queue_limit: 0,
            target_type: TargetType::NotifyEvent,
        }
    }

    #[test]
    fn validate_nats_rejects_multiple_auth_methods() {
        let args = NATSArgs {
            token: "abc".to_string(),
            username: "user".to_string(),
            password: "pass".to_string(),
            ..base_args()
        };
        assert!(args.validate().is_err());
    }

    #[test]
    fn validate_nats_rejects_relative_queue_dir() {
        let args = NATSArgs {
            queue_dir: "relative/path".to_string(),
            ..base_args()
        };
        assert!(args.validate().is_err());
    }
}
