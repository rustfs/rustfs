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
        ReloadableTargetTls, TargetTlsInputSet, TlsReloadAdapter, config::ReloadApplyMode, fingerprint::TargetTlsGeneration,
        validate::validate_tls_material,
    },
    store::{Key, Store},
    target::{
        ChannelTargetType, EntityTarget, QueuedPayload, QueuedPayloadMeta, TargetDeliveryCounters, TargetDeliverySnapshot,
        TargetHealth, TargetHealthReason, TargetHealthState, TargetTlsState, TargetType, build_queued_payload,
        build_target_tls_fingerprint, open_target_queue_store, persist_queued_payload_to_store, redacted_secret,
    },
};
use async_trait::async_trait;
use parking_lot::Mutex;
use reqwest::{Client, StatusCode, Url};
use rustfs_tls_runtime::load_cert_bundle_der_bytes;
use rustfs_utils::egress::validate_outbound_url;
use std::{
    error::Error as StdError,
    fmt,
    marker::PhantomData,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::mpsc;
use tracing::{debug, error, info, instrument, warn};

const LOG_COMPONENT_TARGETS: &str = "targets";
const LOG_SUBSYSTEM_WEBHOOK: &str = "webhook";
const EVENT_WEBHOOK_TARGET_STATE: &str = "webhook_target_state";
const EVENT_WEBHOOK_DELIVERY_STATE: &str = "webhook_delivery_state";
const WEBHOOK_HEALTH_TIMEOUT: Duration = Duration::from_secs(5);

fn classify_probe_error(err: &reqwest::Error) -> TargetHealthReason {
    if err.is_timeout() {
        return TargetHealthReason::TimedOut;
    }

    let mut source = err.source();
    while let Some(cause) = source {
        if cause.downcast_ref::<rustls::Error>().is_some() {
            return TargetHealthReason::TlsFailure;
        }
        if let Some(io_error) = cause.downcast_ref::<std::io::Error>() {
            match io_error.kind() {
                std::io::ErrorKind::ConnectionRefused => return TargetHealthReason::ConnectionRefused,
                std::io::ErrorKind::NotFound | std::io::ErrorKind::AddrNotAvailable => {
                    return TargetHealthReason::DnsFailure;
                }
                _ => {}
            }
        }

        let label = cause.to_string().to_ascii_lowercase();
        if label.contains("dns error") || label.contains("failed to lookup") || label.contains("name or service not known") {
            return TargetHealthReason::DnsFailure;
        }
        if label.contains("certificate") || label.contains("tls") {
            return TargetHealthReason::TlsFailure;
        }
        source = cause.source();
    }

    TargetHealthReason::Unreachable
}

async fn probe_health_url(client: &Client, health_check_url: &Url) -> TargetHealth {
    match tokio::time::timeout(WEBHOOK_HEALTH_TIMEOUT, client.head(health_check_url.as_str()).send()).await {
        Ok(Ok(_)) => TargetHealth::online(TargetHealthReason::Reachable),
        Ok(Err(err)) => TargetHealth::error(classify_probe_error(&err)),
        Err(_) => TargetHealth::error(TargetHealthReason::TimedOut),
    }
}

fn classify_delivery_status(status: StatusCode) -> Result<(), TargetError> {
    if status.is_success() {
        Ok(())
    } else if status.is_redirection() {
        Err(TargetError::Request(format!(
            "Webhook endpoint returned redirect '{}'; redirects are not followed for webhook delivery",
            status
        )))
    } else if status == StatusCode::FORBIDDEN || status == StatusCode::UNAUTHORIZED {
        Err(TargetError::Authentication(format!(
            "Webhook endpoint returned '{}', please check if your auth token is correctly set",
            status
        )))
    } else {
        Err(TargetError::Request(format!(
            "Webhook endpoint returned '{}', please check your endpoint configuration",
            status
        )))
    }
}

/// Arguments for configuring a Webhook target
#[derive(Clone)]
pub struct WebhookArgs {
    /// Whether the target is enabled
    pub enable: bool,
    /// The endpoint URL to send events to
    pub endpoint: Url,
    /// The authorization token for the endpoint
    pub auth_token: String,
    /// The directory to store events in case of failure
    pub queue_dir: String,
    /// The maximum number of events to store
    pub queue_limit: u64,
    /// The client certificate for TLS (PEM format)
    pub client_cert: String,
    /// The client key for TLS (PEM format)
    pub client_key: String,
    /// The path to a custom client root CA certificate file (PEM format) to trust the server.
    pub client_ca: String,
    /// Skip TLS certificate verification. DANGEROUS: for testing only.
    pub skip_tls_verify: bool,
    /// the target type
    pub target_type: TargetType,
}

impl fmt::Debug for WebhookArgs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WebhookArgs")
            .field("enable", &self.enable)
            .field("endpoint_origin", &self.endpoint.origin().ascii_serialization())
            .field("auth_token", &redacted_secret(&self.auth_token))
            .field("queue_dir", &self.queue_dir)
            .field("queue_limit", &self.queue_limit)
            .field("client_cert", &self.client_cert)
            .field("client_key", &redacted_secret(&self.client_key))
            .field("client_ca", &self.client_ca)
            .field("skip_tls_verify", &self.skip_tls_verify)
            .field("target_type", &self.target_type)
            .finish()
    }
}

impl WebhookArgs {
    /// WebhookArgs verification method
    pub fn validate(&self) -> Result<(), TargetError> {
        if !self.enable {
            return Ok(());
        }

        if self.endpoint.as_str().is_empty() {
            return Err(TargetError::Configuration("endpoint empty".to_string()));
        }
        validate_outbound_url(&self.endpoint)
            .map_err(|err| TargetError::Configuration(format!("webhook endpoint is not allowed: {err}")))?;

        if !self.queue_dir.is_empty() {
            let path = std::path::Path::new(&self.queue_dir);
            if !path.is_absolute() {
                return Err(TargetError::Configuration("webhook queue_dir path should be absolute".to_string()));
            }
        }

        if !self.client_cert.is_empty() && self.client_key.is_empty()
            || self.client_cert.is_empty() && !self.client_key.is_empty()
        {
            return Err(TargetError::Configuration("cert and key must be specified as a pair".to_string()));
        }

        if self.skip_tls_verify && !self.client_ca.is_empty() {
            return Err(TargetError::Configuration(
                "skip_tls_verify and client_ca are mutually exclusive; remove client_ca or disable skip_tls_verify".to_string(),
            ));
        }

        Ok(())
    }
}

/// A target that sends events to a webhook
pub struct WebhookTarget<E>
where
    E: PluginEvent,
{
    id: TargetID,
    args: WebhookArgs,
    health_check_url: Option<Url>,
    http_client: Arc<Mutex<Client>>,
    tls_state: Arc<Mutex<TargetTlsState>>,
    /// When present, the adapter provides coordinator-managed TLS material;
    /// otherwise the inline fingerprint path is used as a fallback.
    tls_adapter: Option<TlsReloadAdapter<Client>>,
    // Add Send + Sync constraints to ensure thread safety
    store: Option<Box<dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send + Sync>>,
    initialized: AtomicBool,
    cancel_sender: mpsc::Sender<()>,
    delivery_counters: Arc<TargetDeliveryCounters>,
    _phantom: PhantomData<E>,
}

impl<E> WebhookTarget<E>
where
    E: PluginEvent,
{
    /// Clones the WebhookTarget, creating a new instance with the same configuration
    pub fn clone_box(&self) -> Box<dyn Target<E> + Send + Sync> {
        Box::new(WebhookTarget::<E> {
            id: self.id.clone(),
            args: self.args.clone(),
            health_check_url: self.health_check_url.clone(),
            http_client: Arc::clone(&self.http_client),
            tls_state: Arc::clone(&self.tls_state),
            tls_adapter: self.tls_adapter.clone(),
            store: self.store.as_ref().map(|s| s.boxed_clone()),
            initialized: AtomicBool::new(self.initialized.load(Ordering::SeqCst)),
            cancel_sender: self.cancel_sender.clone(),
            delivery_counters: Arc::clone(&self.delivery_counters),
            _phantom: PhantomData,
        })
    }

    /// Creates a new WebhookTarget
    #[instrument(skip(args), fields(target_id = %id))]
    pub fn new(id: String, args: WebhookArgs) -> Result<Self, TargetError> {
        // First verify the parameters
        args.validate()?;
        // Create a TargetID
        let target_id = TargetID::new(id, ChannelTargetType::Webhook.as_str().to_string());
        let health_check_url = if args.enable {
            Some(Self::health_check_url(&args.endpoint)?)
        } else {
            None
        };

        // Build HTTP client using the helper function
        let http_client = Arc::new(Mutex::new(Self::build_http_client(&args)?));

        let queue_store = open_target_queue_store(
            &args.queue_dir,
            args.queue_limit,
            args.target_type,
            ChannelTargetType::Webhook.as_str(),
            &target_id,
            "Failed to open store for Webhook target",
        )?;

        // Create a cancel channel
        let (cancel_sender, _) = mpsc::channel(1);
        info!(
            event = EVENT_WEBHOOK_TARGET_STATE,
            component = LOG_COMPONENT_TARGETS,
            subsystem = LOG_SUBSYSTEM_WEBHOOK,
            target_id = %target_id.id,
            state = "created",
            "webhook target state"
        );
        Ok(WebhookTarget::<E> {
            id: target_id,
            args,
            health_check_url,
            http_client,
            tls_state: Arc::new(Mutex::new(TargetTlsState::default())),
            tls_adapter: None,
            store: queue_store,
            initialized: AtomicBool::new(false),
            cancel_sender,
            delivery_counters: Arc::new(TargetDeliveryCounters::default()),
            _phantom: PhantomData,
        })
    }

    fn build_http_client(args: &WebhookArgs) -> Result<Client, TargetError> {
        let mut client_builder = Client::builder()
            .timeout(Duration::from_secs(30))
            // SSRF hardening (backlog#974): never follow HTTP redirects on webhook delivery.
            // reqwest follows up to 10 redirects by default, which lets a malicious or
            // compromised endpoint use a 3xx response to bounce the outbound request to an
            // internal address (e.g. the cloud metadata service at 169.254.169.254),
            // bypassing the outbound-endpoint validation performed on the configured URL.
            .redirect(reqwest::redirect::Policy::none())
            .user_agent(crate::get_user_agent(crate::ServiceType::Basis));
        #[cfg(test)]
        {
            client_builder = client_builder.no_proxy();
        }

        // 1. Configure server certificate verification
        if args.skip_tls_verify {
            // DANGEROUS: For testing only, skip all certificate verification
            client_builder = client_builder.danger_accept_invalid_certs(true);
            warn!(
                event = EVENT_WEBHOOK_TARGET_STATE,
                component = LOG_COMPONENT_TARGETS,
                subsystem = LOG_SUBSYSTEM_WEBHOOK,
                state = "tls_verification_skipped",
                fallback = "danger_accept_invalid_certs",
                "webhook target state"
            );
        } else if !args.client_ca.is_empty() {
            // Use user-provided custom CA certificate
            let certs_der = load_cert_bundle_der_bytes(&args.client_ca)
                .map_err(|e| TargetError::Configuration(format!("Failed to parse root CA cert: {e}")))?;
            if certs_der.is_empty() {
                return Err(TargetError::Configuration(
                    "Webhook client_ca did not contain any parsable certificates".to_string(),
                ));
            }
            for cert_der in certs_der {
                let ca_cert = reqwest::Certificate::from_der(&cert_der)
                    .map_err(|e| TargetError::Configuration(format!("Failed to load root CA cert: {e}")))?;
                client_builder = client_builder.add_root_certificate(ca_cert);
            }
        }
        // If neither is set, use the system's default trust store

        // 2. Configure client certificate (mTLS)
        if !args.client_cert.is_empty() && !args.client_key.is_empty() {
            let cert = std::fs::read(&args.client_cert)
                .map_err(|e| TargetError::Configuration(format!("Failed to read client cert: {e}")))?;
            let key = std::fs::read(&args.client_key)
                .map_err(|e| TargetError::Configuration(format!("Failed to read client key: {e}")))?;

            let identity = reqwest::Identity::from_pem(&[cert, key].concat())
                .map_err(|e| TargetError::Configuration(format!("Failed to create identity for mTLS: {e}")))?;
            client_builder = client_builder.identity(identity);
        }

        client_builder
            .build()
            .map_err(|e| TargetError::Configuration(format!("Failed to build HTTP client: {e}")))
    }

    async fn refresh_tls(&self) -> Result<(), TargetError> {
        let next_fingerprint =
            build_target_tls_fingerprint(&self.args.client_ca, &self.args.client_cert, &self.args.client_key).await?;
        let tls_changed = {
            let tls_state_guard = self.tls_state.lock();
            tls_state_guard.fingerprint.as_ref() != Some(&next_fingerprint)
        };
        if !tls_changed {
            return Ok(());
        }

        let new_client = Self::build_http_client(&self.args)?;
        {
            let mut tls_state_guard = self.tls_state.lock();
            if tls_state_guard.fingerprint.as_ref() == Some(&next_fingerprint) {
                return Ok(());
            }
            *self.http_client.lock() = new_client;
            tls_state_guard.refresh(next_fingerprint);
        }
        Ok(())
    }

    fn health_check_url(endpoint: &Url) -> Result<Url, TargetError> {
        endpoint
            .host()
            .ok_or_else(|| TargetError::Configuration("Webhook endpoint is missing a host".to_string()))?;
        let mut health_check_url = endpoint.clone();
        health_check_url
            .set_username("")
            .map_err(|_| TargetError::Configuration("Webhook endpoint contains invalid user information".to_string()))?;
        health_check_url
            .set_password(None)
            .map_err(|_| TargetError::Configuration("Webhook endpoint contains invalid user information".to_string()))?;
        health_check_url.set_path("/");
        health_check_url.set_query(None);
        health_check_url.set_fragment(None);

        Ok(health_check_url)
    }

    async fn probe_health(&self) -> TargetHealth {
        let Some(health_check_url) = self.health_check_url.as_ref() else {
            return TargetHealth::offline(TargetHealthReason::Unreachable);
        };
        let client = self.http_client.lock().clone();
        let health = probe_health_url(&client, health_check_url).await;
        if health.state == TargetHealthState::Online {
            debug!(
                event = EVENT_WEBHOOK_TARGET_STATE,
                component = LOG_COMPONENT_TARGETS,
                subsystem = LOG_SUBSYSTEM_WEBHOOK,
                target_id = %self.id,
                state = "reachability_probe_succeeded",
                "webhook target state"
            );
        }
        health
    }

    async fn probe_reachability(&self) -> Result<bool, TargetError> {
        let health = self.probe_health().await;
        match health.state {
            TargetHealthState::Online => Ok(true),
            TargetHealthState::Offline | TargetHealthState::Disabled => Ok(false),
            TargetHealthState::Error => match health.reason {
                TargetHealthReason::TimedOut => Err(TargetError::Timeout("Webhook health check timed out".to_string())),
                _ => Err(TargetError::Network(format!("Webhook health check failed: {}", health.reason.as_str()))),
            },
        }
    }

    async fn init_inner(&self) -> Result<(), TargetError> {
        if self.initialized.load(Ordering::SeqCst) {
            return Ok(());
        }

        if !self.args.enable {
            return Ok(());
        }

        // Use the configured reqwest client against the origin URL so proxy and TLS
        // behavior matches real delivery while avoiding path-specific false negatives.
        match self.probe_reachability().await {
            Ok(true) => {
                debug!(
                    event = EVENT_WEBHOOK_TARGET_STATE,
                    component = LOG_COMPONENT_TARGETS,
                    subsystem = LOG_SUBSYSTEM_WEBHOOK,
                    target_id = %self.id,
                    state = "reachable",
                    "webhook target state"
                );
            }
            Ok(false) => {
                return Err(TargetError::NotConnected);
            }
            Err(err) => {
                return Err(err);
            }
        }

        self.initialized.store(true, Ordering::SeqCst);
        info!(
            event = EVENT_WEBHOOK_TARGET_STATE,
            component = LOG_COMPONENT_TARGETS,
            subsystem = LOG_SUBSYSTEM_WEBHOOK,
            target_id = %self.id,
            state = "initialized",
            "webhook target state"
        );
        Ok(())
    }

    fn build_queued_payload(&self, event: &EntityTarget<E>) -> Result<QueuedPayload, TargetError> {
        build_queued_payload(event)
    }

    async fn send_body(&self, body: Vec<u8>, meta: &QueuedPayloadMeta) -> Result<(), TargetError> {
        debug!(
            event = EVENT_WEBHOOK_DELIVERY_STATE,
            component = LOG_COMPONENT_TARGETS,
            subsystem = LOG_SUBSYSTEM_WEBHOOK,
            target_id = %self.id,
            bucket = %meta.bucket_name,
            object = %meta.object_name,
            payload_event = %meta.event_name,
            payload_len = body.len(),
            state = "sending",
            "webhook delivery state"
        );

        // When a TLS reload adapter is attached, it drives client rebuilds in
        // the background. The inline per-send fingerprint check is skipped.
        if self.tls_adapter.is_none() {
            self.refresh_tls().await?;
        }

        let client = self.http_client.lock().clone();
        let mut req_builder = client
            .post(self.args.endpoint.as_str())
            .header("Content-Type", meta.content_type.as_str());

        if !self.args.auth_token.is_empty() {
            // Split auth_token string to check if the authentication type is included
            match self.args.auth_token.split_whitespace().count() {
                2 => {
                    // Already include authentication type and token, such as "Bearer token123"
                    req_builder = req_builder.header("Authorization", &self.args.auth_token);
                }
                1 => {
                    // Only tokens, need to add "Bearer" prefix
                    req_builder = req_builder.header("Authorization", format!("Bearer {}", self.args.auth_token));
                }
                _ => {
                    // Empty string or other situations, no authentication header is added
                }
            }
        }

        // Send a request
        let resp = req_builder.body(body).send().await.map_err(|e| {
            if e.is_timeout() || e.is_connect() {
                TargetError::NotConnected
            } else {
                TargetError::Request("Webhook delivery request failed".to_string())
            }
        })?;

        let status = resp.status();
        // Drain the response body so the underlying connection is returned to the
        // pool and can be reused (keep-alive) instead of being closed mid-stream
        // (backlog#983). The body content is not needed for delivery accounting.
        let _ = resp.bytes().await;
        let result = classify_delivery_status(status);
        if result.is_ok() {
            debug!(
                event = EVENT_WEBHOOK_DELIVERY_STATE,
                component = LOG_COMPONENT_TARGETS,
                subsystem = LOG_SUBSYSTEM_WEBHOOK,
                target_id = %self.id,
                status = %status,
                state = "sent",
                "webhook delivery state"
            );
            self.delivery_counters.record_success();
        }
        result
    }
}

#[async_trait]
impl<E> Target<E> for WebhookTarget<E>
where
    E: PluginEvent,
{
    fn id(&self) -> TargetID {
        self.id.clone()
    }

    async fn is_active(&self) -> Result<bool, TargetError> {
        if !self.args.enable {
            return Ok(false);
        }

        self.probe_reachability().await
    }

    async fn health(&self) -> TargetHealth {
        if !self.args.enable {
            return TargetHealth::disabled();
        }
        self.probe_health().await
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
            debug!(
                event = EVENT_WEBHOOK_DELIVERY_STATE,
                component = LOG_COMPONENT_TARGETS,
                subsystem = LOG_SUBSYSTEM_WEBHOOK,
                target_id = %self.id,
                state = "store_enqueued",
                "webhook delivery state"
            );
            Ok(())
        } else {
            match self.init().await {
                Ok(_) => (),
                Err(e) => {
                    error!(
                        event = EVENT_WEBHOOK_TARGET_STATE,
                        component = LOG_COMPONENT_TARGETS,
                        subsystem = LOG_SUBSYSTEM_WEBHOOK,
                        target_id = %self.id.id,
                        state = "init_failed",
                        error = %e,
                        "webhook target state"
                    );
                    self.delivery_counters.record_final_failure();
                    return Err(TargetError::NotConnected);
                }
            }
            if let Err(err) = self.send_body(queued.body, &queued.meta).await {
                self.delivery_counters.record_final_failure();
                return Err(err);
            }
            Ok(())
        }
    }

    async fn send_raw_from_store(&self, key: Key, body: Vec<u8>, meta: QueuedPayloadMeta) -> Result<(), TargetError> {
        debug!(
            event = EVENT_WEBHOOK_DELIVERY_STATE,
            component = LOG_COMPONENT_TARGETS,
            subsystem = LOG_SUBSYSTEM_WEBHOOK,
            target_id = %self.id,
            key = %key,
            state = "store_replay_started",
            "webhook delivery state"
        );
        match self.init().await {
            Ok(_) => {}
            Err(e) => {
                error!(
                    event = EVENT_WEBHOOK_TARGET_STATE,
                    component = LOG_COMPONENT_TARGETS,
                    subsystem = LOG_SUBSYSTEM_WEBHOOK,
                    target_id = %self.id.id,
                    state = "init_failed",
                    error = %e,
                    "webhook target state"
                );
                return Err(TargetError::NotConnected);
            }
        }

        if let Err(e) = self.send_body(body, &meta).await {
            if let TargetError::NotConnected = e {
                return Err(TargetError::NotConnected);
            }
            return Err(e);
        }

        debug!(
            event = EVENT_WEBHOOK_DELIVERY_STATE,
            component = LOG_COMPONENT_TARGETS,
            subsystem = LOG_SUBSYSTEM_WEBHOOK,
            target_id = %self.id,
            key = %key,
            state = "store_replay_sent",
            "webhook delivery state"
        );
        Ok(())
    }

    async fn close(&self) -> Result<(), TargetError> {
        // Send cancel signal to background tasks
        let _ = self.cancel_sender.try_send(());
        // Adapter cleanup is done by the coordinator; no local state to reset.
        info!(
            event = EVENT_WEBHOOK_TARGET_STATE,
            component = LOG_COMPONENT_TARGETS,
            subsystem = LOG_SUBSYSTEM_WEBHOOK,
            target_id = %self.id,
            state = "closed",
            "webhook target state"
        );
        Ok(())
    }

    fn store(&self) -> Option<&(dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send + Sync)> {
        // Returns the reference to the internal store
        self.store.as_deref()
    }

    fn clone_dyn(&self) -> Box<dyn Target<E> + Send + Sync> {
        self.clone_box()
    }

    async fn init(&self) -> Result<(), TargetError> {
        if !self.is_enabled() {
            debug!(
                event = EVENT_WEBHOOK_TARGET_STATE,
                component = LOG_COMPONENT_TARGETS,
                subsystem = LOG_SUBSYSTEM_WEBHOOK,
                target_id = %self.id,
                state = "disabled",
                "webhook target state"
            );
            return Ok(());
        }
        self.init_inner().await
    }

    fn is_enabled(&self) -> bool {
        self.args.enable
    }

    fn delivery_snapshot(&self) -> TargetDeliverySnapshot {
        self.delivery_counters.snapshot(
            self.store.as_deref().map_or(0, |store| store.len() as u64),
            // Webhook targets record no terminal failures and keep no failed store.
            0,
        )
    }

    fn record_final_failure(&self) {
        self.delivery_counters.record_final_failure();
    }
}

/// Coordinated TLS hot-reload implementation for Webhook targets.
///
/// The coordinator calls these methods on a background poll loop to detect
/// TLS file changes and rebuild the HTTP client without restarting.
#[async_trait]
impl<E> ReloadableTargetTls for WebhookTarget<E>
where
    E: PluginEvent,
{
    type Material = Client;

    fn tls_input_set(&self) -> TargetTlsInputSet {
        TargetTlsInputSet {
            ca_path: self.args.client_ca.clone(),
            client_cert_path: self.args.client_cert.clone(),
            client_key_path: self.args.client_key.clone(),
            target_label: format!("webhook:{}", self.id.id),
        }
    }

    async fn build_tls_material(&self) -> Result<Self::Material, TargetError> {
        // build_http_client is synchronous (reads files + configures reqwest).
        // The coordinator already runs this in a background task, so the
        // synchronous file I/O does not block the send path.
        Self::build_http_client(&self.args)
    }

    async fn apply_tls_material(
        &self,
        _generation: TargetTlsGeneration,
        material: Arc<Self::Material>,
        _mode: ReloadApplyMode,
    ) -> Result<(), TargetError> {
        *self.http_client.lock() = (*material).clone();
        Ok(())
    }

    async fn validate_tls_files(&self) -> Result<(), TargetError> {
        validate_tls_material(&self.args.client_ca, &self.args.client_cert, &self.args.client_key)
    }
}

#[cfg(test)]
mod tests {
    use super::{WebhookArgs, WebhookTarget, classify_delivery_status, probe_health_url};
    use crate::target::{REDACTED_SECRET, Target, TargetHealthReason, TargetHealthState, TargetType, decode_object_name};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use url::Url;
    use url::form_urlencoded;

    fn base_args() -> WebhookArgs {
        WebhookArgs {
            enable: true,
            endpoint: Url::parse("https://example.com/hook").unwrap(),
            auth_token: String::new(),
            queue_dir: String::new(),
            queue_limit: 0,
            client_cert: String::new(),
            client_key: String::new(),
            client_ca: String::new(),
            skip_tls_verify: false,
            target_type: TargetType::NotifyEvent,
        }
    }

    async fn http_status_url(status: u16) -> Url {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind test server");
        let address = listener.local_addr().expect("test server address");
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept health probe");
            let mut request = [0u8; 1024];
            let _ = stream.read(&mut request).await;
            stream
                .write_all(format!("HTTP/1.1 {status} Test\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").as_bytes())
                .await
                .expect("write health response");
        });
        Url::parse(&format!("http://{address}/")).expect("health probe URL")
    }

    #[test]
    fn debug_redacts_webhook_secret_fields() {
        let args = WebhookArgs {
            endpoint: Url::parse("https://user:password@example.com/private?token=query-secret").expect("debug URL"),
            auth_token: "webhook-token".to_string(),
            client_key: "/etc/rustfs/webhook.key".to_string(),
            ..base_args()
        };

        let rendered = format!("{args:?}");

        assert!(!rendered.contains("webhook-token"));
        assert!(!rendered.contains("/etc/rustfs/webhook.key"));
        assert!(!rendered.contains("password"));
        assert!(!rendered.contains("/private"));
        assert!(!rendered.contains("query-secret"));
        assert!(rendered.contains(REDACTED_SECRET));
        assert!(rendered.contains("https://example.com"));
        assert!(rendered.contains("WebhookArgs"));
    }

    #[test]
    fn test_validate_skip_tls_verify_and_client_ca_mutually_exclusive() {
        let args = WebhookArgs {
            skip_tls_verify: true,
            client_ca: "/path/to/ca.pem".to_string(),
            ..base_args()
        };
        let result = args.validate();
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("skip_tls_verify") && err_msg.contains("client_ca"),
            "Error message should mention both fields, got: {err_msg}"
        );
    }

    #[test]
    fn test_validate_skip_tls_verify_without_client_ca_is_ok() {
        let args = WebhookArgs {
            skip_tls_verify: true,
            ..base_args()
        };
        assert!(args.validate().is_ok());
    }

    #[test]
    fn test_validate_client_ca_without_skip_tls_verify_is_ok() {
        let args = WebhookArgs {
            client_ca: "/path/to/ca.pem".to_string(),
            ..base_args()
        };
        assert!(args.validate().is_ok());
    }

    #[test]
    fn test_validate_rejects_loopback_endpoint() {
        let args = WebhookArgs {
            endpoint: Url::parse("https://127.0.0.1/hook").expect("loopback endpoint should parse"),
            ..base_args()
        };
        let err = args.validate().expect_err("loopback endpoint should be rejected");
        assert!(err.to_string().contains("not allowed"));
    }

    #[test]
    fn test_decode_object_name_with_spaces() {
        // Test case from the issue: "greeting file (2).csv"
        let object_name = "greeting file (2).csv";

        // Simulate what event.rs does: form-urlencoded encoding (spaces become +)
        let form_encoded = form_urlencoded::byte_serialize(object_name.as_bytes()).collect::<String>();
        assert_eq!(form_encoded, "greeting+file+%282%29.csv");

        // Test the decode_object_name helper function
        let decoded = decode_object_name(&form_encoded).unwrap();
        assert_eq!(decoded, object_name);
        assert!(!decoded.contains('+'), "Decoded string should not contain + symbols");
    }

    #[test]
    fn test_decode_object_name_with_special_chars() {
        // Test with various special characters
        let test_cases = vec![
            ("folder/greeting file (2).csv", "folder%2Fgreeting+file+%282%29.csv"),
            ("test file.txt", "test+file.txt"),
            ("my file (copy).pdf", "my+file+%28copy%29.pdf"),
            ("file with spaces and (parentheses).doc", "file+with+spaces+and+%28parentheses%29.doc"),
        ];

        for (original, form_encoded) in test_cases {
            // Test the decode_object_name helper function
            let decoded = decode_object_name(form_encoded).unwrap();
            assert_eq!(decoded, original, "Failed to decode: {}", form_encoded);
        }
    }

    #[test]
    fn test_decode_object_name_without_spaces() {
        // Test that files without spaces still work correctly
        let object_name = "simple-file.txt";
        let form_encoded = form_urlencoded::byte_serialize(object_name.as_bytes()).collect::<String>();

        let decoded = decode_object_name(&form_encoded).unwrap();
        assert_eq!(decoded, object_name);
    }

    #[test]
    fn test_health_check_url_ignores_endpoint_path() {
        let endpoint = Url::parse("https://user:password@example.com:9443/hook/path?token=secret").expect("webhook endpoint URL");
        let health_check_url = WebhookTarget::<serde_json::Value>::health_check_url(&endpoint).expect("webhook health-check URL");

        assert_eq!(health_check_url.as_str(), "https://example.com:9443/");
    }

    #[tokio::test]
    async fn head_http_responses_only_measure_reachability() {
        let client = WebhookTarget::<serde_json::Value>::build_http_client(&base_args()).expect("build client");
        for status in [401, 404, 500] {
            let health = probe_health_url(&client, &http_status_url(status).await).await;
            assert_eq!(health.state, TargetHealthState::Online, "HEAD {status} is reachable");
            assert_eq!(health.reason, TargetHealthReason::Reachable);
        }
    }

    #[tokio::test]
    async fn refused_connection_has_stable_health_reason() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve refused port");
        let address = listener.local_addr().expect("refused address");
        drop(listener);
        let url = Url::parse(&format!("http://{address}/")).expect("refused URL");
        let client = WebhookTarget::<serde_json::Value>::build_http_client(&base_args()).expect("build client");

        let health = probe_health_url(&client, &url).await;

        assert_eq!(health.state, TargetHealthState::Error);
        assert_eq!(health.reason, TargetHealthReason::ConnectionRefused);
    }

    #[tokio::test]
    async fn dns_failure_has_stable_health_reason() {
        let url = Url::parse("http://rustfs-health-check.invalid/").expect("invalid test domain URL");
        let client = WebhookTarget::<serde_json::Value>::build_http_client(&base_args()).expect("build client");

        let health = probe_health_url(&client, &url).await;

        assert_eq!(health.state, TargetHealthState::Error);
        assert_eq!(health.reason, TargetHealthReason::DnsFailure);
    }

    #[tokio::test(start_paused = true)]
    async fn webhook_health_probe_has_a_five_second_total_budget() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind stalled server");
        let address = listener.local_addr().expect("stalled server address");
        let server = tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.expect("accept stalled probe");
            std::future::pending::<()>().await;
        });
        let url = Url::parse(&format!("http://{address}/")).expect("stalled URL");
        let client = WebhookTarget::<serde_json::Value>::build_http_client(&base_args()).expect("build client");
        let started = tokio::time::Instant::now();

        let health = probe_health_url(&client, &url).await;

        assert_eq!(started.elapsed(), super::WEBHOOK_HEALTH_TIMEOUT);
        assert_eq!(health.state, TargetHealthState::Error);
        assert_eq!(health.reason, TargetHealthReason::TimedOut);
        server.abort();
    }

    #[tokio::test]
    async fn tls_failure_has_stable_health_reason() {
        use rustls::{
            ServerConfig, ServerConnection, StreamOwned,
            pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer},
        };
        use std::io::Read;
        use std::sync::{Arc, Once};

        static INSTALL_CRYPTO_PROVIDER: Once = Once::new();
        INSTALL_CRYPTO_PROVIDER.call_once(|| {
            let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        });
        let rcgen::CertifiedKey { cert, signing_key } =
            rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).expect("cert should generate");
        let server_config = Arc::new(
            ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(
                    vec![cert.der().clone()],
                    PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(signing_key.serialize_der())),
                )
                .expect("server cert should be valid"),
        );
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind TLS server");
        let address = listener.local_addr().expect("TLS server address");
        let server = std::thread::spawn(move || {
            let (stream, _) = listener.accept().expect("accept TLS client");
            let connection = ServerConnection::new(server_config).expect("server connection");
            let mut tls_stream = StreamOwned::new(connection, stream);
            let mut request = [0u8; 1024];
            let _ = tls_stream.read(&mut request);
        });
        let url = Url::parse(&format!("https://localhost:{}/", address.port())).expect("TLS health URL");
        let client = WebhookTarget::<serde_json::Value>::build_http_client(&base_args()).expect("build client");

        let health = probe_health_url(&client, &url).await;

        assert_eq!(health.state, TargetHealthState::Error);
        assert_eq!(health.reason, TargetHealthReason::TlsFailure);
        server.join().expect("TLS server thread");
    }

    #[test]
    fn post_status_classification_requires_success() {
        assert!(classify_delivery_status(reqwest::StatusCode::NO_CONTENT).is_ok());
        for status in [
            reqwest::StatusCode::MOVED_PERMANENTLY,
            reqwest::StatusCode::UNAUTHORIZED,
            reqwest::StatusCode::INTERNAL_SERVER_ERROR,
        ] {
            assert!(classify_delivery_status(status).is_err(), "POST {status} must fail");
        }
    }

    #[tokio::test]
    async fn test_disabled_target_can_be_constructed_without_origin_probe() {
        let args = WebhookArgs {
            enable: false,
            endpoint: Url::parse("about:blank").unwrap(),
            ..base_args()
        };
        let target = WebhookTarget::<serde_json::Value>::new("disabled-target".to_string(), args).unwrap();

        assert!(!target.is_active().await.unwrap());
    }

    #[test]
    fn test_origin_reachability_probe_requires_non_local_endpoint() {
        let args = WebhookArgs {
            endpoint: Url::parse("http://127.0.0.1/hook").unwrap(),
            ..base_args()
        };
        let err = match WebhookTarget::<serde_json::Value>::new("path-probe".to_string(), args) {
            Ok(_) => panic!("loopback origin probes should now be rejected at construction time"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("not allowed"));
    }

    // SSRF hardening regression (backlog#974): the delivery client must not follow HTTP
    // redirects, otherwise a 3xx from the endpoint could bounce the outbound request to an
    // internal address (e.g. the cloud metadata service) and bypass endpoint validation.
    #[tokio::test]
    async fn test_webhook_client_does_not_follow_redirects() {
        use std::io::{Read, Write};
        use std::net::TcpListener;

        // Minimal HTTP server on an ephemeral loopback port.
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock server");
        let addr = listener.local_addr().expect("local addr");

        // Serve exactly one request with a 302 pointing at an internal metadata address.
        // If the client followed the redirect it would issue a second request to that
        // (unreachable) target instead of returning the 3xx status.
        let handle = std::thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buf = [0u8; 1024];
                let _ = stream.read(&mut buf);
                let response =
                    "HTTP/1.1 302 Found\r\nLocation: http://169.254.169.254/latest/meta-data/\r\nContent-Length: 0\r\n\r\n";
                let _ = stream.write_all(response.as_bytes());
                let _ = stream.flush();
            }
        });

        let client = WebhookTarget::<serde_json::Value>::build_http_client(&base_args()).expect("build client");

        let resp = client
            .post(format!("http://{addr}/hook"))
            .body("{}")
            .send()
            .await
            .expect("request should complete without following the redirect");

        // Redirects are disabled, so the 3xx is surfaced as-is rather than chased to the
        // internal Location target.
        assert_eq!(resp.status().as_u16(), 302, "webhook client must not follow redirects");

        handle.join().expect("mock server thread");
    }

    #[tokio::test]
    async fn test_webhook_client_reaches_https_origin_with_custom_ca() {
        use rustls::{
            ServerConfig, ServerConnection, StreamOwned,
            pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer},
        };
        use std::io::{Read, Write};
        use std::net::TcpListener;
        use std::sync::{Arc, Once};

        static INSTALL_CRYPTO_PROVIDER: Once = Once::new();
        INSTALL_CRYPTO_PROVIDER.call_once(|| {
            let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        });

        let rcgen::CertifiedKey { cert, signing_key } =
            rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).expect("cert should generate");
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let ca_path = temp_dir.path().join("webhook-ca.pem");
        std::fs::write(&ca_path, cert.pem()).expect("write ca pem");

        let cert_chain = vec![cert.der().clone()];
        let key_der = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(signing_key.serialize_der()));
        let server_config = Arc::new(
            ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(cert_chain, key_der)
                .expect("server cert should be valid"),
        );

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind tls server");
        let addr = listener.local_addr().expect("local addr");
        let handle = std::thread::spawn(move || {
            let (stream, _) = listener.accept().expect("accept tls client");
            let connection = ServerConnection::new(server_config).expect("server connection");
            let mut tls_stream = StreamOwned::new(connection, stream);
            let mut buf = [0u8; 1024];
            let _ = tls_stream.read(&mut buf);
            let response = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok";
            tls_stream.write_all(response.as_bytes()).expect("write response");
            tls_stream.flush().expect("flush response");
        });

        let args = WebhookArgs {
            client_ca: ca_path.to_string_lossy().into_owned(),
            ..base_args()
        };
        let client = WebhookTarget::<serde_json::Value>::build_http_client(&args).expect("build https client");
        let resp = client
            .head(format!("https://localhost:{}/hook", addr.port()))
            .send()
            .await
            .expect("https webhook probe should trust configured ca");

        assert_eq!(resp.status(), reqwest::StatusCode::OK);
        assert!(resp.bytes().await.expect("read response body").is_empty());
        handle.join().expect("tls server thread");
    }
}
