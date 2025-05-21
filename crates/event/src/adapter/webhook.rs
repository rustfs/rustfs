use crate::Error;
use crate::Event;
use crate::WebhookConfig;
use crate::{ChannelAdapter, ChannelAdapterType};
use async_trait::async_trait;
use reqwest::{Client, RequestBuilder};
use std::time::Duration;
use tokio::time::sleep;

/// Webhook adapter for sending events to a webhook endpoint.
pub struct WebhookAdapter {
    config: WebhookConfig,
    client: Client,
}

impl WebhookAdapter {
    /// Creates a new Webhook adapter.
    pub fn new(config: WebhookConfig) -> Self {
        let mut builder = Client::builder();
        if config.timeout > 0 {
            builder = builder.timeout(Duration::from_secs(config.timeout));
        }
        let client = builder.build().expect("Failed to build reqwest client");
        Self { config, client }
    }
    /// Builds the request to send the event.
    fn build_request(&self, event: &Event) -> RequestBuilder {
        let mut request = self.client.post(&self.config.endpoint).json(event);
        if let Some(token) = &self.config.auth_token {
            request = request.header("Authorization", format!("Bearer {}", token));
        }
        if let Some(headers) = &self.config.custom_headers {
            for (key, value) in headers {
                request = request.header(key, value);
            }
        }
        request
    }
}

#[async_trait]
impl ChannelAdapter for WebhookAdapter {
    fn name(&self) -> String {
        ChannelAdapterType::Webhook.to_string()
    }

    async fn send(&self, event: &Event) -> Result<(), Error> {
        let mut attempt = 0;
        tracing::info!("Attempting to send webhook request: {:?}", event);
        loop {
            match self.build_request(event).send().await {
                Ok(response) => {
                    response.error_for_status().map_err(Error::Http)?;
                    return Ok(());
                }
                Err(e) if attempt < self.config.max_retries => {
                    attempt += 1;
                    tracing::warn!("Webhook attempt {} failed: {}. Retrying...", attempt, e);
                    sleep(Duration::from_secs(2u64.pow(attempt))).await;
                }
                Err(e) => return Err(Error::Http(e)),
            }
        }
    }
}
