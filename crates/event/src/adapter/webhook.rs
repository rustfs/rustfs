use crate::config::webhook::WebhookConfig;
use crate::config::STORE_PREFIX;
use crate::store::queue::Store;
use crate::{ChannelAdapter, ChannelAdapterType};
use crate::{Error, QueueStore};
use crate::{Event, DEFAULT_RETRY_INTERVAL};
use async_trait::async_trait;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::{self, Client, Identity, RequestBuilder};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use ChannelAdapterType::Webhook;

// Webhook constants
pub const WEBHOOK_ENDPOINT: &str = "endpoint";
pub const WEBHOOK_AUTH_TOKEN: &str = "auth_token";
pub const WEBHOOK_QUEUE_DIR: &str = "queue_dir";
pub const WEBHOOK_QUEUE_LIMIT: &str = "queue_limit";
pub const WEBHOOK_CLIENT_CERT: &str = "client_cert";
pub const WEBHOOK_CLIENT_KEY: &str = "client_key";

pub const ENV_WEBHOOK_ENABLE: &str = "RUSTFS_NOTIFY_WEBHOOK_ENABLE";
pub const ENV_WEBHOOK_ENDPOINT: &str = "RUSTFS_NOTIFY_WEBHOOK_ENDPOINT";
pub const ENV_WEBHOOK_AUTH_TOKEN: &str = "RUSTFS_NOTIFY_WEBHOOK_AUTH_TOKEN";
pub const ENV_WEBHOOK_QUEUE_DIR: &str = "RUSTFS_NOTIFY_WEBHOOK_QUEUE_DIR";
pub const ENV_WEBHOOK_QUEUE_LIMIT: &str = "RUSTFS_NOTIFY_WEBHOOK_QUEUE_LIMIT";
pub const ENV_WEBHOOK_CLIENT_CERT: &str = "RUSTFS_NOTIFY_WEBHOOK_CLIENT_CERT";
pub const ENV_WEBHOOK_CLIENT_KEY: &str = "RUSTFS_NOTIFY_WEBHOOK_CLIENT_KEY";

/// Webhook adapter for sending events to a webhook endpoint.
pub struct WebhookAdapter {
    /// Configuration information
    config: WebhookConfig,
    /// Event storage queues
    store: Option<Arc<QueueStore<Event>>>,
    /// HTTP client
    client: Client,
}

impl WebhookAdapter {
    /// Creates a new Webhook adapter.
    pub fn new(config: WebhookConfig) -> Self {
        let mut builder = Client::builder();
        if config.timeout.is_some() {
            // Set the timeout for the client
            match config.timeout {
                Some(t) => builder = builder.timeout(Duration::from_secs(t)),
                None => tracing::warn!("Timeout is not set, using default timeout"),
            }
        }
        let client = if let (Some(cert_path), Some(key_path)) = (&config.client_cert, &config.client_key) {
            let cert_path = PathBuf::from(cert_path);
            let key_path = PathBuf::from(key_path);

            // Check if the certificate file exists
            if !cert_path.exists() || !key_path.exists() {
                tracing::warn!("Certificate files not found, falling back to default client");
                builder.build()
            } else {
                // Try to read and load the certificate
                match (fs::read(&cert_path), fs::read(&key_path)) {
                    (Ok(cert_data), Ok(key_data)) => {
                        // Create an identity
                        let mut pem_data = cert_data;
                        pem_data.extend_from_slice(&key_data);

                        match Identity::from_pem(&pem_data) {
                            Ok(identity) => {
                                tracing::info!("Successfully loaded client certificate");
                                builder.identity(identity).build()
                            }
                            Err(e) => {
                                tracing::warn!("Failed to create identity from PEM: {}, falling back to default client", e);
                                builder.build()
                            }
                        }
                    }
                    _ => {
                        tracing::warn!("Failed to read certificate files, falling back to default client");
                        builder.build()
                    }
                }
            }
        } else {
            builder.build()
        }
        .unwrap_or_else(|e| {
            tracing::error!("Failed to create HTTP client: {}", e);
            reqwest::Client::new()
        });

        // create a queue store if enabled
        let store = if !config.common.queue_dir.len() > 0 {
            let store_path = PathBuf::from(&config.common.queue_dir).join(format!(
                "{}-{}-{}",
                STORE_PREFIX,
                Webhook.as_str(),
                config.common.identifier
            ));
            let queue_limit = if config.common.queue_limit > 0 {
                config.common.queue_limit
            } else {
                crate::config::default_queue_limit()
            };
            let store = QueueStore::new(store_path, queue_limit, Some(".event".to_string()));
            if let Err(e) = store.open() {
                tracing::error!("Unable to open queue storage: {}", e);
                None
            } else {
                Some(Arc::new(store))
            }
        } else {
            None
        };

        Self { config, store, client }
    }

    /// Handle backlog events in storage
    pub async fn process_backlog(&self) -> Result<(), Error> {
        if let Some(store) = &self.store {
            let keys = store.list();
            for key in keys {
                match store.get_multiple(&key) {
                    Ok(events) => {
                        for event in events {
                            if let Err(e) = self.send_with_retry(&event).await {
                                tracing::error!("Processing of backlog events failed: {}", e);
                                // If it still fails, we remain in the queue
                                break;
                            }
                        }
                        // Deleted after successful processing
                        if let Err(e) = store.del(&key) {
                            tracing::error!("Failed to delete a handled event: {}", e);
                        }
                    }
                    Err(e) => {
                        tracing::error!("Failed to read events from storage: {}", e);
                        // delete the broken entries
                        // If the event cannot be read, it may be corrupted, delete it
                        if let Err(del_err) = store.del(&key) {
                            tracing::error!("Failed to delete a corrupted event: {}", del_err);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    ///Send events to the webhook endpoint with retry logic
    async fn send_with_retry(&self, event: &Event) -> Result<(), Error> {
        let retry_interval = match self.config.retry_interval {
            Some(t) => Duration::from_secs(t),
            None => Duration::from_secs(DEFAULT_RETRY_INTERVAL), // Default to 3 seconds if not set
        };
        let mut attempts = 0;

        loop {
            attempts += 1;
            match self.send_request(event).await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    if attempts <= self.config.max_retries {
                        tracing::warn!("Send to webhook fails and will be retried after 3 seconds:{}", e);
                        sleep(retry_interval).await;
                    } else if let Some(store) = &self.store {
                        // store in a queue for later processing
                        tracing::warn!("The maximum number of retries is reached, and the event is stored in a queue:{}", e);
                        if let Err(store_err) = store.put(event.clone()) {
                            tracing::error!("Events cannot be stored to a queue:{}", store_err);
                        }
                        return Err(e);
                    } else {
                        return Err(e);
                    }
                }
            }
        }
    }

    /// Send a single HTTP request
    async fn send_request(&self, event: &Event) -> Result<(), Error> {
        // Send a request
        let response = self
            .build_request(event)
            .send()
            .await
            .map_err(|e| Error::Custom(format!("Sending a webhook request failed:{}", e)))?;

        // Check the response status
        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "Unable to read response body".to_string());
            return Err(Error::Custom(format!("Webhook request failed, status code:{},response:{}", status, body)));
        }

        Ok(())
    }

    /// Builds the request to send the event.
    fn build_request(&self, event: &Event) -> RequestBuilder {
        let mut request = self
            .client
            .post(&self.config.endpoint)
            .json(event)
            .header("Content-Type", "application/json");
        if let Some(token) = &self.config.auth_token {
            let tokens: Vec<&str> = token.split_whitespace().collect();
            match tokens.len() {
                2 => request = request.header("Authorization", token),
                1 => request = request.header("Authorization", format!("Bearer {}", token)),
                _ => tracing::warn!("Invalid auth token format, skipping Authorization header"),
            }
        }
        if let Some(headers) = &self.config.custom_headers {
            let mut header_map = HeaderMap::new();
            for (key, value) in headers {
                if let (Ok(name), Ok(val)) = (HeaderName::from_bytes(key.as_bytes()), HeaderValue::from_str(value)) {
                    header_map.insert(name, val);
                }
            }
            request = request.headers(header_map);
        }
        request
    }

    /// Save the event to the queue
    async fn save_to_queue(&self, event: &Event) -> Result<(), Error> {
        if let Some(store) = &self.store {
            store
                .put(event.clone())
                .map_err(|e| Error::Custom(format!("Saving events to queue failed: {}", e)))?;
        }
        Ok(())
    }
}

#[async_trait]
impl ChannelAdapter for WebhookAdapter {
    fn name(&self) -> String {
        Webhook.to_string()
    }

    async fn send(&self, event: &Event) -> Result<(), Error> {
        // Deal with the backlog of events first
        let _ = self.process_backlog().await;

        // Send the current event
        match self.send_with_retry(event).await {
            Ok(_) => Ok(()),
            Err(e) => {
                // If the send fails and the queue is enabled, save to the queue
                if let Some(_) = &self.store {
                    tracing::warn!("Failed to send the event and saved to the queue: {}", e);
                    self.save_to_queue(event).await?;
                    return Ok(());
                }
                Err(e)
            }
        }
    }
}
