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

use crate::target::{ChannelTargetType, EntityTarget, TargetType};
use crate::{
    StoreError, Target, TargetLog,
    arn::TargetID,
    error::TargetError,
    store::{Key, Store},
};
use async_trait::async_trait;
use reqwest::{Client, StatusCode, Url};
use rustfs_config::notify::STORE_EXTENSION;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::net::lookup_host;
use tokio::sync::mpsc;
use tracing::{debug, error, info, instrument};
use urlencoding;

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
                return Err(TargetError::Configuration("webhook queueDir path should be absolute".to_string()));
            }
        }

        if !self.client_cert.is_empty() && self.client_key.is_empty()
            || self.client_cert.is_empty() && !self.client_key.is_empty()
        {
            return Err(TargetError::Configuration("cert and key must be specified as a pair".to_string()));
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
    http_client: Arc<Client>,
    // Add Send + Sync constraints to ensure thread safety
    store: Option<Box<dyn Store<EntityTarget<E>, Error = StoreError, Key = Key> + Send + Sync>>,
    initialized: AtomicBool,
    addr: String,
    cancel_sender: mpsc::Sender<()>,
}

impl<E> WebhookTarget<E>
where
    E: Send + Sync + 'static + Clone + Serialize + DeserializeOwned,
{
    /// Clones the WebhookTarget, creating a new instance with the same configuration
    pub fn clone_box(&self) -> Box<dyn Target<E> + Send + Sync> {
        Box::new(WebhookTarget {
            id: self.id.clone(),
            args: self.args.clone(),
            http_client: Arc::clone(&self.http_client),
            store: self.store.as_ref().map(|s| s.boxed_clone()),
            initialized: AtomicBool::new(self.initialized.load(Ordering::SeqCst)),
            addr: self.addr.clone(),
            cancel_sender: self.cancel_sender.clone(),
        })
    }

    /// Creates a new WebhookTarget
    #[instrument(skip(args), fields(target_id = %id))]
    pub fn new(id: String, args: WebhookArgs) -> Result<Self, TargetError> {
        // First verify the parameters
        args.validate()?;
        // Create a TargetID
        let target_id = TargetID::new(id, ChannelTargetType::Webhook.as_str().to_string());
        // Build HTTP client
        let mut client_builder = Client::builder()
            .timeout(Duration::from_secs(30))
            .user_agent(rustfs_utils::sys::get_user_agent(rustfs_utils::sys::ServiceType::Basis));

        // Supplementary certificate processing logic
        if !args.client_cert.is_empty() && !args.client_key.is_empty() {
            // Add client certificate
            let cert = std::fs::read(&args.client_cert)
                .map_err(|e| TargetError::Configuration(format!("Failed to read client cert: {e}")))?;
            let key = std::fs::read(&args.client_key)
                .map_err(|e| TargetError::Configuration(format!("Failed to read client key: {e}")))?;

            let identity = reqwest::Identity::from_pem(&[cert, key].concat())
                .map_err(|e| TargetError::Configuration(format!("Failed to create identity: {e}")))?;
            client_builder = client_builder.identity(identity);
        }

        let http_client = Arc::new(
            client_builder
                .build()
                .map_err(|e| TargetError::Configuration(format!("Failed to build HTTP client: {e}")))?,
        );

        // Build storage
        let queue_store = if !args.queue_dir.is_empty() {
            let queue_dir =
                PathBuf::from(&args.queue_dir).join(format!("rustfs-{}-{}", ChannelTargetType::Webhook.as_str(), target_id.id));
            let store = crate::store::QueueStore::<EntityTarget<E>>::new(queue_dir, args.queue_limit, STORE_EXTENSION);

            if let Err(e) = store.open() {
                error!("Failed to open store for Webhook target {}: {}", target_id.id, e);
                return Err(TargetError::Storage(format!("{e}")));
            }

            // Make sure that the Store trait implemented by QueueStore matches the expected error type
            Some(Box::new(store) as Box<dyn Store<EntityTarget<E>, Error = StoreError, Key = Key> + Send + Sync>)
        } else {
            None
        };

        // resolved address
        let addr = {
            let host = args.endpoint.host_str().unwrap_or("localhost");
            let port = args
                .endpoint
                .port()
                .unwrap_or_else(|| if args.endpoint.scheme() == "https" { 443 } else { 80 });
            format!("{host}:{port}")
        };

        // Create a cancel channel
        let (cancel_sender, _) = mpsc::channel(1);
        info!(target_id = %target_id.id, "Webhook target created");
        Ok(WebhookTarget {
            id: target_id,
            args,
            http_client,
            store: queue_store,
            initialized: AtomicBool::new(false),
            addr,
            cancel_sender,
        })
    }

    async fn init(&self) -> Result<(), TargetError> {
        // Use CAS operations to ensure thread-safe initialization
        if !self.initialized.load(Ordering::SeqCst) {
            // Check the connection
            match self.is_active().await {
                Ok(true) => {
                    info!("Webhook target {} is active", self.id);
                }
                Ok(false) => {
                    return Err(TargetError::NotConnected);
                }
                Err(e) => {
                    error!("Failed to check if Webhook target {} is active: {}", self.id, e);
                    return Err(e);
                }
            }
            self.initialized.store(true, Ordering::SeqCst);
            info!("Webhook target {} initialized", self.id);
        }
        Ok(())
    }

    async fn send(&self, event: &EntityTarget<E>) -> Result<(), TargetError> {
        info!("Webhook Sending event to webhook target: {}", self.id);
        let object_name = urlencoding::decode(&event.object_name)
            .map_err(|e| TargetError::Encoding(format!("Failed to decode object key: {e}")))?;

        let key = format!("{}/{}", event.bucket_name, object_name);

        let log = TargetLog {
            event_name: event.event_name,
            key,
            records: vec![event.data.clone()],
        };

        let data = serde_json::to_vec(&log).map_err(|e| TargetError::Serialization(format!("Failed to serialize event: {e}")))?;

        // Vec<u8> Convert to String
        let data_string = String::from_utf8(data.clone())
            .map_err(|e| TargetError::Encoding(format!("Failed to convert event data to UTF-8: {e}")))?;
        debug!("Sending event to webhook target: {}, event log: {}", self.id, data_string);

        // build request
        let mut req_builder = self
            .http_client
            .post(self.args.endpoint.as_str())
            .header("Content-Type", "application/json");

        if !self.args.auth_token.is_empty() {
            // Split auth_token string to check if the authentication type is included
            let tokens: Vec<&str> = self.args.auth_token.split_whitespace().collect();
            match tokens.len() {
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
        let resp = req_builder.body(data).send().await.map_err(|e| {
            if e.is_timeout() || e.is_connect() {
                TargetError::NotConnected
            } else {
                TargetError::Request(format!("Failed to send request: {e}"))
            }
        })?;

        let status = resp.status();
        if status.is_success() {
            debug!("Event sent to webhook target: {}", self.id);
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
        let socket_addr = lookup_host(&self.addr)
            .await
            .map_err(|e| TargetError::Network(format!("Failed to resolve host: {e}")))?
            .next()
            .ok_or_else(|| TargetError::Network("No address found".to_string()))?;
        debug!("is_active socket addr: {},target id:{}", socket_addr, self.id.id);
        match tokio::time::timeout(Duration::from_secs(5), tokio::net::TcpStream::connect(socket_addr)).await {
            Ok(Ok(_)) => {
                debug!("Connection to {} is active", self.addr);
                Ok(true)
            }
            Ok(Err(e)) => {
                debug!("Connection to {} failed: {}", self.addr, e);
                if e.kind() == std::io::ErrorKind::ConnectionRefused {
                    Err(TargetError::NotConnected)
                } else {
                    Err(TargetError::Network(format!("Connection failed: {e}")))
                }
            }
            Err(_) => Err(TargetError::Timeout("Connection timed out".to_string())),
        }
    }

    async fn save(&self, event: Arc<EntityTarget<E>>) -> Result<(), TargetError> {
        if let Some(store) = &self.store {
            // Call the store method directly, no longer need to acquire the lock
            store
                .put(event)
                .map_err(|e| TargetError::Storage(format!("Failed to save event to store: {e}")))?;
            debug!("Event saved to store for target: {}", self.id);
            Ok(())
        } else {
            match self.init().await {
                Ok(_) => (),
                Err(e) => {
                    error!("Failed to initialize Webhook target {}: {}", self.id.id, e);
                    return Err(TargetError::NotConnected);
                }
            }
            self.send(&event).await
        }
    }

    async fn send_from_store(&self, key: Key) -> Result<(), TargetError> {
        debug!("Sending event from store for target: {}", self.id);
        match self.init().await {
            Ok(_) => {
                debug!("Event sent to store for target: {}", self.name());
            }
            Err(e) => {
                error!("Failed to initialize Webhook target {}: {}", self.id.id, e);
                return Err(TargetError::NotConnected);
            }
        }

        let store = self
            .store
            .as_ref()
            .ok_or_else(|| TargetError::Configuration("No store configured".to_string()))?;

        // Get events directly from the store, no longer need to acquire locks
        let event = match store.get(&key) {
            Ok(event) => event,
            Err(StoreError::NotFound) => return Ok(()),
            Err(e) => {
                return Err(TargetError::Storage(format!("Failed to get event from store: {e}")));
            }
        };

        if let Err(e) = self.send(&event).await {
            if let TargetError::NotConnected = e {
                return Err(TargetError::NotConnected);
            }
            return Err(e);
        }

        // Use the immutable reference of the store to delete the event content corresponding to the key
        debug!("Deleting event from store for target: {}, key:{}, start", self.id, key.to_string());
        match store.del(&key) {
            Ok(_) => debug!("Event deleted from store for target: {}, key:{}, end", self.id, key.to_string()),
            Err(e) => {
                error!("Failed to delete event from store: {}", e);
                return Err(TargetError::Storage(format!("Failed to delete event from store: {e}")));
            }
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

    fn store(&self) -> Option<&(dyn Store<EntityTarget<E>, Error = StoreError, Key = Key> + Send + Sync)> {
        // Returns the reference to the internal store
        self.store.as_deref()
    }

    fn clone_dyn(&self) -> Box<dyn Target<E> + Send + Sync> {
        self.clone_box()
    }

    async fn init(&self) -> Result<(), TargetError> {
        // If the target is disabled, return to success directly
        if !self.is_enabled() {
            debug!("Webhook target {} is disabled, skipping initialization", self.id);
            return Ok(());
        }

        // Use existing initialization logic
        WebhookTarget::init(self).await
    }

    fn is_enabled(&self) -> bool {
        self.args.enable
    }
}
