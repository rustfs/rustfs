use crate::sinks::Sink;
use crate::UnifiedLogEntry;
use async_trait::async_trait;

/// Webhook Sink Implementation
pub struct WebhookSink {
    endpoint: String,
    auth_token: String,
    client: reqwest::Client,
    max_retries: usize,
    retry_delay_ms: u64,
}

impl WebhookSink {
    pub fn new(endpoint: String, auth_token: String, max_retries: usize, retry_delay_ms: u64) -> Self {
        WebhookSink {
            endpoint,
            auth_token,
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .unwrap_or_else(|_| reqwest::Client::new()),
            max_retries,
            retry_delay_ms,
        }
    }
}

#[async_trait]
impl Sink for WebhookSink {
    async fn write(&self, entry: &UnifiedLogEntry) {
        let mut retries = 0;
        let url = self.endpoint.clone();
        let entry_clone = entry.clone();
        let auth_value = reqwest::header::HeaderValue::from_str(format!("Bearer {}", self.auth_token.clone()).as_str()).unwrap();
        while retries < self.max_retries {
            match self
                .client
                .post(&url)
                .header(reqwest::header::AUTHORIZATION, auth_value.clone())
                .json(&entry_clone)
                .send()
                .await
            {
                Ok(response) if response.status().is_success() => {
                    return;
                }
                _ => {
                    retries += 1;
                    if retries < self.max_retries {
                        tokio::time::sleep(tokio::time::Duration::from_millis(
                            self.retry_delay_ms * (1 << retries), // Exponential backoff
                        ))
                        .await;
                    }
                }
            }
        }

        eprintln!("Failed to send log to webhook after {} retries", self.max_retries);
    }
}

impl Drop for WebhookSink {
    fn drop(&mut self) {
        // Perform any necessary cleanup here
        // For example, you might want to log that the sink is being dropped
        eprintln!("Dropping WebhookSink with URL: {}", self.endpoint);
    }
}
