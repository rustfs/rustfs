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
    StoreError, Target, TargetLog,
    arn::TargetID,
    error::TargetError,
    store::{Key, QueueStore, Store},
    target::{
        ChannelTargetType, EntityTarget, QueuedPayload, QueuedPayloadMeta, TargetDeliveryCounters, TargetDeliverySnapshot,
        TargetType, queue_store_subdir_name,
    },
};
use async_trait::async_trait;
use reqwest::{Client, StatusCode, Url};
use rustfs_config::audit::AUDIT_STORE_EXTENSION;
use rustfs_config::notify::NOTIFY_STORE_EXTENSION;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::{
    marker::PhantomData,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::mpsc;
use tracing::{debug, error, info, instrument, warn};

/// Arguments for configuring a Webhook target
#[derive(Debug, Clone)]
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

impl WebhookArgs {
    /// WebhookArgs verification method
    pub fn validate(&self) -> Result<(), TargetError> {
        if !self.enable {
            return Ok(());
        }

        if self.endpoint.as_str().is_empty() {
            return Err(TargetError::Configuration("endpoint empty".to_string()));
        }

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
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
{
    id: TargetID,
    args: WebhookArgs,
    health_check_url: Option<Url>,
    http_client: Arc<Client>,
    // Add Send + Sync constraints to ensure thread safety
    store: Option<Box<dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send + Sync>>,
    initialized: AtomicBool,
    cancel_sender: mpsc::Sender<()>,
    delivery_counters: Arc<TargetDeliveryCounters>,
    _phantom: PhantomData<E>,
}

impl<E> WebhookTarget<E>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
{
    /// Clones the WebhookTarget, creating a new instance with the same configuration
    pub fn clone_box(&self) -> Box<dyn Target<E> + Send + Sync> {
        Box::new(WebhookTarget::<E> {
            id: self.id.clone(),
            args: self.args.clone(),
            health_check_url: self.health_check_url.clone(),
            http_client: Arc::clone(&self.http_client),
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
        let http_client = Arc::new(Self::build_http_client(&args)?);

        // Build storage
        let queue_store = if !args.queue_dir.is_empty() {
            let queue_dir =
                PathBuf::from(&args.queue_dir).join(queue_store_subdir_name(ChannelTargetType::Webhook.as_str(), &target_id.id));

            let extension = match args.target_type {
                TargetType::AuditLog => AUDIT_STORE_EXTENSION,
                TargetType::NotifyEvent => NOTIFY_STORE_EXTENSION,
            };

            let store = QueueStore::<QueuedPayload>::new(queue_dir, args.queue_limit, extension);

            if let Err(e) = store.open() {
                error!("Failed to open store for Webhook target {}: {}", target_id.id, e);
                return Err(TargetError::Storage(format!("{e}")));
            }

            // Make sure that the Store trait implemented by QueueStore matches the expected error type
            Some(Box::new(store) as Box<dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send + Sync>)
        } else {
            None
        };

        // Create a cancel channel
        let (cancel_sender, _) = mpsc::channel(1);
        info!(target_id = %target_id.id, "Webhook target created");
        Ok(WebhookTarget::<E> {
            id: target_id,
            args,
            health_check_url,
            http_client,
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
            .user_agent(crate::get_user_agent(crate::ServiceType::Basis));

        // 1. Configure server certificate verification
        if args.skip_tls_verify {
            // DANGEROUS: For testing only, skip all certificate verification
            client_builder = client_builder.danger_accept_invalid_certs(true);
            warn!(
                "Webhook target '{}' is configured to skip TLS verification. This is insecure and should not be used in production.",
                args.endpoint
            );
        } else if !args.client_ca.is_empty() {
            // Use user-provided custom CA certificate
            let ca_cert_pem = std::fs::read(&args.client_ca)
                .map_err(|e| TargetError::Configuration(format!("Failed to read root CA cert: {e}")))?;
            let ca_cert = reqwest::Certificate::from_pem(&ca_cert_pem)
                .map_err(|e| TargetError::Configuration(format!("Failed to parse root CA cert: {e}")))?;
            client_builder = client_builder.add_root_certificate(ca_cert);
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

    fn health_check_url(endpoint: &Url) -> Result<Url, TargetError> {
        endpoint
            .host()
            .ok_or_else(|| TargetError::Configuration(format!("Webhook endpoint '{}' is missing a host", endpoint)))?;
        let mut health_check_url = endpoint.clone();
        health_check_url.set_path("/");
        health_check_url.set_query(None);
        health_check_url.set_fragment(None);

        Ok(health_check_url)
    }

    async fn probe_reachability(&self) -> Result<bool, TargetError> {
        let Some(health_check_url) = self.health_check_url.as_ref() else {
            return Ok(false);
        };

        match tokio::time::timeout(Duration::from_secs(5), self.http_client.head(health_check_url.as_str()).send()).await {
            Ok(Ok(resp)) => {
                debug!(
                    target = %self.id,
                    status = %resp.status(),
                    health_check_url = %health_check_url,
                    "Webhook health check request succeeded"
                );
                Ok(true)
            }
            Ok(Err(err)) if err.is_timeout() => Err(TargetError::Timeout(format!(
                "Webhook health check request to {} timed out",
                health_check_url
            ))),
            Ok(Err(err)) if err.is_connect() => Ok(false),
            Ok(Err(err)) => Err(TargetError::Network(format!(
                "Webhook health check request to {} failed: {}",
                health_check_url, err
            ))),
            Err(_) => Err(TargetError::Timeout(format!(
                "Webhook health check request to {} timed out",
                health_check_url
            ))),
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
                debug!("Webhook target {} reachability probe succeeded via {:?}", self.id, self.health_check_url);
            }
            Ok(false) => {
                return Err(TargetError::NotConnected);
            }
            Err(err) => {
                return Err(err);
            }
        }

        self.initialized.store(true, Ordering::SeqCst);
        info!("Webhook target {} initialized", self.id);
        Ok(())
    }

    fn build_queued_payload(&self, event: &EntityTarget<E>) -> Result<QueuedPayload, TargetError> {
        let object_name = crate::target::decode_object_name(&event.object_name)?;
        let key = format!("{}/{}", event.bucket_name, object_name);
        let log = TargetLog {
            event_name: event.event_name,
            key,
            records: vec![event.data.clone()],
        };
        let body = serde_json::to_vec(&log).map_err(|e| TargetError::Serialization(format!("Failed to serialize event: {e}")))?;
        let meta = QueuedPayloadMeta::new(
            event.event_name,
            event.bucket_name.clone(),
            event.object_name.clone(),
            "application/json",
            body.len(),
        );
        Ok(QueuedPayload::new(meta, body))
    }

    async fn send_body(&self, body: Vec<u8>, meta: &QueuedPayloadMeta) -> Result<(), TargetError> {
        info!("Webhook sending queued payload to target: {}", self.id);
        debug!(
            target = %self.id,
            bucket = %meta.bucket_name,
            object = %meta.object_name,
            event = %meta.event_name,
            payload_len = body.len(),
            "Sending webhook payload"
        );

        let mut req_builder = self
            .http_client
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
                TargetError::Request(format!("Failed to send request: {e}"))
            }
        })?;

        let status = resp.status();
        if status.is_success() {
            debug!("Event sent to webhook target: {}", self.id);
            self.delivery_counters.record_success();
            Ok(())
        } else if status == StatusCode::FORBIDDEN {
            Err(TargetError::Authentication(format!(
                "{} returned '{}', please check if your auth token is correctly set",
                self.args.endpoint, status
            )))
        } else {
            Err(TargetError::Request(format!(
                "{} returned '{}', please check your endpoint configuration",
                self.args.endpoint, status
            )))
        }
    }
}

#[async_trait]
impl<E> Target<E> for WebhookTarget<E>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
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

    async fn save(&self, event: Arc<EntityTarget<E>>) -> Result<(), TargetError> {
        let queued = match self.build_queued_payload(&event) {
            Ok(queued) => queued,
            Err(err) => {
                self.delivery_counters.record_final_failure();
                return Err(err);
            }
        };

        if let Some(store) = &self.store {
            let encoded = match queued.encode() {
                Ok(encoded) => encoded,
                Err(err) => {
                    self.delivery_counters.record_final_failure();
                    return Err(TargetError::Storage(format!("Failed to encode queued payload: {err}")));
                }
            };
            if let Err(e) = store.put_raw(&encoded) {
                self.delivery_counters.record_final_failure();
                return Err(TargetError::Storage(format!("Failed to save event to store: {e}")));
            }
            debug!("Event saved to store for target: {}", self.id);
            Ok(())
        } else {
            match self.init().await {
                Ok(_) => (),
                Err(e) => {
                    error!("Failed to initialize Webhook target {}: {}", self.id.id, e);
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
        debug!("Sending queued payload from store for target: {}, key: {}", self.id, key);
        match self.init().await {
            Ok(_) => {
                debug!("Event sent to store for target: {}", self.name());
            }
            Err(e) => {
                error!("Failed to initialize Webhook target {}: {}", self.id.id, e);
                return Err(TargetError::NotConnected);
            }
        }

        if let Err(e) = self.send_body(body, &meta).await {
            if let TargetError::NotConnected = e {
                return Err(TargetError::NotConnected);
            }
            return Err(e);
        }

        debug!("Event sent from store and deleted for target: {}", self.id);
        Ok(())
    }

    async fn close(&self) -> Result<(), TargetError> {
        // Send cancel signal to background tasks
        let _ = self.cancel_sender.try_send(());
        info!("Webhook target closed: {}", self.id);
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
            debug!("Webhook target {} is disabled, skipping initialization", self.id);
            return Ok(());
        }
        self.init_inner().await
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

#[cfg(test)]
mod tests {
    use super::{WebhookArgs, WebhookTarget};
    use crate::target::{Target, TargetType, decode_object_name};
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;
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
        let endpoint = Url::parse("https://example.com:9443/hook/path").unwrap();
        let health_check_url = WebhookTarget::<serde_json::Value>::health_check_url(&endpoint).unwrap();

        assert_eq!(health_check_url.as_str(), "https://example.com:9443/");
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

    #[tokio::test]
    async fn test_is_active_uses_origin_reachability_for_path_endpoints() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (path_tx, mut path_rx) = mpsc::channel(1);
        let accept_task = tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let path_tx = path_tx.clone();
                tokio::spawn(async move {
                    use tokio::io::{AsyncReadExt, AsyncWriteExt};

                    let mut request = Vec::new();
                    let mut buf = [0u8; 1024];
                    loop {
                        let read = stream.read(&mut buf).await.unwrap();
                        if read == 0 {
                            break;
                        }
                        request.extend_from_slice(&buf[..read]);
                        if request.windows(4).any(|window| window == b"\r\n\r\n") {
                            break;
                        }
                    }

                    let request_line = request
                        .split(|byte| *byte == b'\n')
                        .next()
                        .and_then(|line| std::str::from_utf8(line).ok())
                        .unwrap_or_default()
                        .trim();
                    let path = request_line.split_whitespace().nth(1).unwrap_or_default().to_string();
                    let _ = path_tx.send(path.clone()).await;

                    if path == "/" {
                        let response = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                        let _ = stream.write_all(response).await;
                    }
                });
            }
        });

        let args = WebhookArgs {
            endpoint: Url::parse(&format!("http://{address}/hook")).unwrap(),
            ..base_args()
        };
        let target = WebhookTarget::<serde_json::Value>::new("path-probe".to_string(), args).unwrap();

        assert!(target.is_active().await.unwrap());
        assert_eq!(path_rx.recv().await.unwrap(), "/");
        accept_task.abort();
    }
}
