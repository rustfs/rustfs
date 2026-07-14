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

use crate::plugin::PluginEvent;
use crate::{
    StoreError, Target,
    arn::TargetID,
    error::TargetError,
    runtime::tls::{
        ReloadableTargetTls, TargetTlsGeneration, TargetTlsInputSet, TlsReloadAdapter, config::ReloadApplyMode,
        validate_tls_material,
    },
    store::{FailedEventStore, Key, QueueStore, Store},
    target::{
        ChannelTargetType, EntityTarget, QueuedPayload, QueuedPayloadMeta, TargetDeliveryCounters, TargetDeliverySnapshot,
        TargetTlsState, TargetType, build_queued_payload_with_records, build_target_tls_fingerprint, is_connectivity_error,
        open_target_queue_store_typed, persist_queued_payload_to_store, redacted_secret,
    },
};
use async_trait::async_trait;
use rustfs_config::{
    NATS_CREDENTIALS_FILE, NATS_JETSTREAM_ACK_TIMEOUT_DEFAULT_SECS, NATS_TLS_CA, NATS_TLS_CLIENT_CERT, NATS_TLS_CLIENT_KEY,
};
use std::fmt;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{error, info, instrument, warn};
use uuid::Uuid;

mod jetstream;
mod publish_error;
mod validation;

use jetstream::{CachedJetStreamContext, drain_jetstream_context};
use publish_error::{classify_nats_flush_error, classify_nats_publish_error};

pub(crate) use jetstream::resolve_dedup_id;
pub(crate) use validation::{validate_jetstream_settings, validate_jetstream_stream};

#[derive(Clone)]
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
    // JetStream publish settings. Absent maps to off and the defaults.
    pub jetstream_enable: Option<bool>,
    pub jetstream_stream_name: Option<String>,
    pub jetstream_ack_timeout_secs: Option<u64>,
    pub target_type: TargetType,
}

impl fmt::Debug for NATSArgs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NATSArgs")
            .field("enable", &self.enable)
            .field("address", &self.address)
            .field("subject", &self.subject)
            .field("username", &self.username)
            .field("password", &redacted_secret(&self.password))
            .field("token", &redacted_secret(&self.token))
            .field("credentials_file", &redacted_secret(&self.credentials_file))
            .field("tls_ca", &self.tls_ca)
            .field("tls_client_cert", &self.tls_client_cert)
            .field("tls_client_key", &redacted_secret(&self.tls_client_key))
            .field("tls_required", &self.tls_required)
            .field("queue_dir", &self.queue_dir)
            .field("queue_limit", &self.queue_limit)
            .field("jetstream_enable", &self.jetstream_enable)
            .field("jetstream_stream_name", &self.jetstream_stream_name)
            .field("jetstream_ack_timeout_secs", &self.jetstream_ack_timeout_secs)
            .field("target_type", &self.target_type)
            .finish()
    }
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

        validate_jetstream_settings(
            self.jetstream_enable.unwrap_or(false),
            self.jetstream_stream_name.as_deref().unwrap_or_default(),
            &self.queue_dir,
            self.jetstream_ack_timeout_secs,
        )?;

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

/// Returns true when the target sends credentials over a connection that does not require TLS, which
/// would transmit the secrets in cleartext (backlog#983). TLS is active when tls_required is set or
/// the address uses the tls:// scheme.
fn nats_sends_credentials_without_tls(args: &NATSArgs) -> bool {
    let has_auth = !args.token.is_empty() || !args.credentials_file.is_empty() || !args.username.is_empty();
    if !has_auth {
        return false;
    }
    let scheme_is_tls = args.address.trim_start().to_ascii_lowercase().starts_with("tls://");
    !(args.tls_required || scheme_is_tls)
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
    E: PluginEvent,
{
    id: TargetID,
    args: NATSArgs,
    client: Arc<Mutex<Option<async_nats::Client>>>,
    /// Cached JetStream context, one per target shared across clones so a single acker and semaphore
    /// serve every clone. Read out under the lock before each publish await, never held across it.
    /// Carries the stream-validation verdict for the bound connection.
    jetstream_context: Arc<Mutex<Option<CachedJetStreamContext>>>,
    tls_state: Arc<parking_lot::Mutex<TargetTlsState>>,
    /// When set, the coordinator drives TLS reload, otherwise inline fingerprint change detection.
    tls_adapter: Option<TlsReloadAdapter<async_nats::Client>>,
    /// The concrete queue store, held typed so the target projects both the generic Store handle and
    /// its own failed-events capability from a single store. Shared across clones through the Arc.
    store: Option<Arc<QueueStore<QueuedPayload>>>,
    connected: AtomicBool,
    delivery_counters: Arc<TargetDeliveryCounters>,
    _phantom: std::marker::PhantomData<E>,
}

impl<E> NATSTarget<E>
where
    E: PluginEvent,
{
    pub fn clone_box(&self) -> Box<dyn Target<E> + Send + Sync> {
        Box::new(NATSTarget::<E> {
            id: self.id.clone(),
            args: self.args.clone(),
            client: Arc::clone(&self.client),
            jetstream_context: Arc::clone(&self.jetstream_context),
            tls_state: Arc::clone(&self.tls_state),
            tls_adapter: self.tls_adapter.clone(),
            store: self.store.clone(),
            connected: AtomicBool::new(self.connected.load(Ordering::SeqCst)),
            delivery_counters: Arc::clone(&self.delivery_counters),
            _phantom: std::marker::PhantomData,
        })
    }

    #[instrument(skip(args), fields(target_id_as_string = %id))]
    pub fn new(id: String, args: NATSArgs) -> Result<Self, TargetError> {
        args.validate()?;
        if args.enable && nats_sends_credentials_without_tls(&args) {
            warn!(
                target_id = %id,
                address = %args.address,
                "NATS target sends authentication credentials without TLS; secrets are transmitted in cleartext. Enable tls_required or use a tls:// address."
            );
        }
        let target_id = TargetID::new(id, ChannelTargetType::Nats.as_str().to_string());
        let queue_store = open_target_queue_store_typed(
            &args.queue_dir,
            args.queue_limit,
            args.target_type,
            ChannelTargetType::Nats.as_str(),
            &target_id,
            "Failed to open store for NATS target",
        )?
        .map(Arc::new);

        Ok(Self {
            id: target_id,
            args,
            client: Arc::new(Mutex::new(None)),
            jetstream_context: Arc::new(Mutex::new(None)),
            tls_state: Arc::new(parking_lot::Mutex::new(TargetTlsState::default())),
            tls_adapter: None,
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
        // Adapter-managed path: use the material directly from the TLS reload adapter.
        if let Some(adapter) = &self.tls_adapter {
            let client: async_nats::Client = (*adapter.current_material()).clone();

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
            let tls_state_guard = self.tls_state.lock();
            tls_state_guard.needs_update(&next_fingerprint)
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

        let mut client_guard = self.client.lock().await;
        let shared = client_guard.get_or_insert_with(|| client.clone()).clone();

        // Swap in a context built from the winning client while the client lock is held, so client
        // and context swap as a unit and no clone reads a context bound to the old client. Only the
        // JetStream path caches a context. The previous context is drained after the locks release.
        let previous_context = if tls_changed && self.jetstream_enabled() {
            let ack_timeout = self.ack_timeout();
            let mut context_guard = self.jetstream_context.lock().await;
            let mut context = async_nats::jetstream::new(shared.clone());
            context.set_timeout(ack_timeout);
            context_guard.replace(CachedJetStreamContext::new(context))
        } else {
            None
        };
        drop(client_guard);

        // Advance the recorded fingerprint only after the reconnect and flush succeed, so a rotation
        // whose first reconnect fails stays detected and the next success still rebuilds the context.
        if tls_changed {
            self.tls_state.lock().refresh(next_fingerprint);
        }

        drain_jetstream_context(previous_context.map(|cached| cached.context)).await;
        Ok(shared)
    }

    fn jetstream_enabled(&self) -> bool {
        self.args.jetstream_enable.unwrap_or(false)
    }

    fn ack_timeout(&self) -> Duration {
        Duration::from_secs(
            self.args
                .jetstream_ack_timeout_secs
                .unwrap_or(NATS_JETSTREAM_ACK_TIMEOUT_DEFAULT_SECS),
        )
    }

    fn build_queued_payload(&self, event: &EntityTarget<E>) -> Result<QueuedPayload, TargetError> {
        let mut queued = build_queued_payload_with_records(event, vec![event.clone()])?;
        if self.jetstream_enabled() {
            // Mint the dedup id once at enqueue so it is identical across every retry and replay and
            // distinct across entries. The body-hash fallback only covers entries queued before enable.
            queued.meta.dedup_id = Uuid::new_v4().to_string();
        }
        Ok(queued)
    }

    async fn send_body(&self, body: Vec<u8>) -> Result<(), TargetError> {
        let client = self.get_or_connect().await?;
        if let Err(e) = client.publish(self.args.subject.clone(), body.into()).await {
            let err = classify_nats_publish_error(&e);
            if is_connectivity_error(&err) {
                self.invalidate_cached_client_connection().await;
                self.connected.store(false, Ordering::SeqCst);
            }
            return Err(err);
        }

        // publish only enqueues the message on the client's outbound channel. Flush to confirm the
        // message reached the server before delivery is treated as successful (backlog#971).
        if let Err(e) = client.flush().await {
            let err = classify_nats_flush_error(&e);
            self.invalidate_cached_client_connection().await;
            self.connected.store(false, Ordering::SeqCst);
            return Err(err);
        }

        self.delivery_counters.record_success();
        Ok(())
    }
}

#[async_trait]
impl<E> Target<E> for NATSTarget<E>
where
    E: PluginEvent,
{
    fn id(&self) -> TargetID {
        self.id.clone()
    }

    async fn is_active(&self) -> Result<bool, TargetError> {
        // With JetStream enabled the health answer covers the stream as well as the connection, so a
        // reachable broker with a failing stream reports the validation error. The verdict is cached
        // and reset on a reconnect, TLS rotation, wrong-stream ack, or stream-not-found outcome, so a
        // reset forces a live lookup on the next check.
        if self.jetstream_enabled() {
            self.validated_jetstream_context().await?;
        }
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

    async fn send_raw_from_store(&self, key: Key, body: Vec<u8>, meta: QueuedPayloadMeta) -> Result<(), TargetError> {
        if self.jetstream_enabled() {
            let dedup_id = resolve_dedup_id(&meta.dedup_id, &key);
            self.publish_jetstream(body, &dedup_id).await
        } else {
            self.send_body(body).await
        }
    }

    async fn close(&self) -> Result<(), TargetError> {
        // Drain the cached context's in-flight acks before dropping it so the async-nats acker exits.
        // Drain the context before the client, since the acker rides the client connection.
        let context = {
            let mut guard = self.jetstream_context.lock().await;
            guard.take()
        };
        // Bound the drain by the ack timeout so an unresponsive broker cannot stall close. On elapse
        // the client drain below still runs.
        if tokio::time::timeout(self.ack_timeout(), drain_jetstream_context(context.map(|cached| cached.context)))
            .await
            .is_err()
        {
            warn!(target_id = %self.id, "Timed out draining JetStream acks on close, proceeding to close the client");
        }

        let client = {
            let mut guard = self.client.lock().await;
            guard.take()
        };
        self.tls_state.lock().reset();
        self.connected.store(false, Ordering::SeqCst);
        if let Some(client) = client {
            client
                .drain()
                .await
                .map_err(|e| TargetError::Network(format!("Failed to drain NATS client: {e}")))?;
        }
        // The durable queue store stays on disk, so a queued entry survives close and replays. Close
        // releases the cached handles, never the entries.
        info!(target_id = %self.id, "NATS target closed");
        Ok(())
    }

    fn store(&self) -> Option<&(dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send + Sync)> {
        self.store
            .as_deref()
            .map(|store| store as &(dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send + Sync))
    }

    fn failed_store(&self) -> Option<&dyn FailedEventStore> {
        self.store.as_deref().map(|store| store as &dyn FailedEventStore)
    }

    async fn handle_terminal_failure(
        &self,
        store: &(dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send),
        key: &Key,
        error: &TargetError,
        retry_count: u32,
    ) -> bool {
        let Some(failed_store) = self.failed_store() else {
            error!(
                target_id = %self.id,
                replay_key = %key,
                "NATS target has no failed-events store for the terminal move"
            );
            return false;
        };
        match jetstream::move_entry_to_failed_store(store, failed_store, &self.id, key, error, retry_count).await {
            Ok(()) => true,
            Err(move_err) => {
                error!(
                    target_id = %self.id,
                    error = %move_err,
                    replay_key = %key,
                    "Failed to move event to the failed-events store"
                );
                false
            }
        }
    }

    fn clone_dyn(&self) -> Box<dyn Target<E> + Send + Sync> {
        self.clone_box()
    }

    async fn init(&self) -> Result<(), TargetError> {
        if !self.is_enabled() {
            return Ok(());
        }
        let _ = self.get_or_connect().await?;
        if self.jetstream_enabled() {
            let cached = self.jetstream_context().await?;
            validate_jetstream_stream(&cached.context, &self.args, &self.id.to_string(), Some(&cached.validation_logged)).await?;
            // Record the verdict on the context so the first publish does not repeat the stream
            // lookup that just passed.
            cached.stream_validated.store(true, Ordering::Release);
        }
        Ok(())
    }

    fn is_enabled(&self) -> bool {
        self.args.enable
    }

    fn delivery_snapshot(&self) -> TargetDeliverySnapshot {
        self.delivery_counters.snapshot(
            self.store.as_deref().map_or(0, |store| store.len() as u64),
            self.failed_store().map_or(0, |failed_store| failed_store.failed_len() as u64),
        )
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
    E: PluginEvent,
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
pub(crate) mod test_support {
    use super::*;
    use async_nats::jetstream;
    use rustfs_s3_types::EventName;

    // Absolute on Linux, macOS, and Windows. temp_dir needs no filesystem to exist for a
    // validation-only test, and Path::is_absolute stays true across platforms.
    pub(crate) fn nats_queue_dir() -> String {
        std::env::temp_dir().join("rustfs-nats-queue").to_string_lossy().into_owned()
    }

    pub(crate) fn base_args() -> NATSArgs {
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
            jetstream_enable: None,
            jetstream_stream_name: None,
            jetstream_ack_timeout_secs: None,
            target_type: TargetType::NotifyEvent,
        }
    }

    pub(crate) fn jetstream_args(target_type: TargetType) -> NATSArgs {
        NATSArgs {
            jetstream_enable: Some(true),
            jetstream_stream_name: Some("RUSTFS_EVENTS".to_string()),
            jetstream_ack_timeout_secs: Some(30),
            target_type,
            ..base_args()
        }
    }

    pub(crate) fn nats_target(args: NATSArgs) -> NATSTarget<String> {
        NATSTarget::<String> {
            id: TargetID::new("test-target".to_string(), ChannelTargetType::Nats.as_str().to_string()),
            args,
            client: Arc::new(Mutex::new(None)),
            jetstream_context: Arc::new(Mutex::new(None)),
            tls_state: Arc::new(parking_lot::Mutex::new(TargetTlsState::default())),
            tls_adapter: None,
            store: None,
            connected: AtomicBool::new(false),
            delivery_counters: Arc::new(TargetDeliveryCounters::default()),
            _phantom: std::marker::PhantomData,
        }
    }

    pub(crate) fn sample_event() -> EntityTarget<String> {
        EntityTarget {
            object_name: "folder/object.txt".to_string(),
            bucket_name: "bucket-a".to_string(),
            event_name: EventName::ObjectCreatedPut,
            data: "payload-data".to_string(),
        }
    }

    pub(crate) fn sample_stored_key(name: &str) -> Key {
        Key {
            name: name.to_string(),
            extension: ".event".to_string(),
            item_count: 1,
            compress: false,
        }
    }

    pub(crate) fn nats_target_with_store(args: NATSArgs, queue_dir: &str) -> NATSTarget<String> {
        let mut configured = args;
        configured.queue_dir = queue_dir.to_string();
        NATSTarget::<String>::new("store-target".to_string(), configured).expect("target with store builds")
    }

    /// A stream configuration that passes every assertion: it captures the configured subject, returns
    /// acks, accepts writes, and deduplicates well beyond the retry lifetime.
    pub(crate) fn writable_stream_config(subject: &str) -> jetstream::stream::Config {
        jetstream::stream::Config {
            name: "RUSTFS_EVENTS".to_string(),
            subjects: vec![subject.to_string()],
            no_ack: false,
            sealed: false,
            duplicate_window: Duration::from_secs(600),
            ..Default::default()
        }
    }

    // Live-broker behaviour tests. They require a NATS server with JetStream enabled and are ignored by
    // default. To run locally:
    //
    //     docker run -d --name rustfs-nats-test -p 4222:4222 nats:2 -js
    //     cargo test -p rustfs-targets --lib -- --ignored nats::tests::tls_change
    //
    // Override the server URL with RUSTFS_TEST_NATS_URL.
    pub(crate) fn broker_url() -> String {
        std::env::var("RUSTFS_TEST_NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".to_string())
    }

    /// Log sink for asserting a specific line is written. The subscriber writes into a shared
    /// buffer the assertion reads back.
    #[derive(Clone, Default)]
    pub(crate) struct CapturedLog(Arc<parking_lot::Mutex<Vec<u8>>>);

    impl CapturedLog {
        pub(crate) fn contents(&self) -> String {
            String::from_utf8_lossy(&self.0.lock()).into_owned()
        }
    }

    impl std::io::Write for CapturedLog {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturedLog {
        type Writer = CapturedLog;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::target::REDACTED_SECRET;
    use crate::target::nats::test_support::*;

    #[test]
    fn debug_redacts_nats_secret_fields() {
        let args = NATSArgs {
            password: "nats-password".to_string(),
            token: "nats-token".to_string(),
            credentials_file: "/etc/rustfs/nats.creds".to_string(),
            tls_client_key: "/etc/rustfs/nats.key".to_string(),
            ..base_args()
        };

        let rendered = format!("{args:?}");

        assert!(!rendered.contains("nats-password"));
        assert!(!rendered.contains("nats-token"));
        assert!(!rendered.contains("/etc/rustfs/nats.creds"));
        assert!(!rendered.contains("/etc/rustfs/nats.key"));
        assert!(rendered.contains(REDACTED_SECRET));
        assert!(rendered.contains("rustfs.events"));
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

    #[test]
    fn nats_credentials_without_tls_is_detected() {
        // Token auth over a plaintext nats:// address without tls_required leaks the credential (backlog#983).
        let insecure = NATSArgs {
            token: "secret-token".to_string(),
            tls_required: false,
            ..base_args()
        };
        assert!(nats_sends_credentials_without_tls(&insecure));

        // tls_required protects the credentials.
        let with_tls = NATSArgs {
            tls_required: true,
            ..insecure.clone()
        };
        assert!(!nats_sends_credentials_without_tls(&with_tls));

        // A tls:// address also counts as protected.
        let tls_scheme = NATSArgs {
            address: "tls://127.0.0.1:4222".to_string(),
            ..insecure
        };
        assert!(!nats_sends_credentials_without_tls(&tls_scheme));

        // No credentials configured: nothing to leak.
        let no_auth = base_args();
        assert!(!nats_sends_credentials_without_tls(&no_auth));
    }

    #[tokio::test]
    async fn flag_off_stored_meta_is_byte_identical_to_pre_feature() {
        // With the flag off the payload carries no dedup id, so its encoded meta matches a pre-feature entry.
        let disabled = nats_target(base_args());
        let off_payload = disabled.build_queued_payload(&sample_event()).expect("payload builds");
        assert!(off_payload.meta.dedup_id.is_empty(), "no dedup id is minted with the flag off");

        let meta_json = serde_json::to_string(&off_payload.meta).expect("meta serializes");
        assert!(!meta_json.contains("dedup_id"), "the absent dedup id is skipped on serialization");

        // No JetStream context is built on the flag-off path.
        assert!(disabled.jetstream_context.lock().await.is_none(), "no context is built with the flag off");
    }
}
