//  Copyright 2024 RustFS Team
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use super::{Loggable, Target};
use crate::logger::config::HttpConfig;
use async_trait::async_trait;
use reqwest::Client;
use std::error::Error;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;
use tokio::time::{Duration, sleep};

pub struct HttpTarget {
    name: String,
    config: HttpConfig,
    client: Client,
    sender: mpsc::Sender<Box<dyn Loggable>>,
    shutdown_signal: Arc<AtomicBool>,
    worker_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl HttpTarget {
    pub fn new(name: String, config: HttpConfig) -> Self {
        let (sender, receiver) = mpsc::channel(config.queue_size);
        let shutdown_signal = Arc::new(AtomicBool::new(false));

        let target = Self {
            name,
            config,
            client: Client::new(),
            sender,
            shutdown_signal,
            worker_handle: Arc::new(Mutex::new(None)),
        };

        target.start_worker(receiver);
        target
    }

    fn start_worker(&self, mut receiver: mpsc::Receiver<Box<dyn Loggable>>) {
        let client = self.client.clone();
        let config = self.config.clone();
        let endpoint = self.config.endpoint.clone();
        let shutdown_signal = self.shutdown_signal.clone();
        let name = self.name.clone();

        let handle = tokio::spawn(async move {
            let mut buffer: Vec<Box<dyn Loggable>> = Vec::with_capacity(config.batch_size);
            let batch_timeout = Duration::from_secs(1);

            loop {
                let should_shutdown = shutdown_signal.load(Ordering::SeqCst);

                match tokio::time::timeout(batch_timeout, receiver.recv()).await {
                    Ok(Some(entry)) => {
                        buffer.push(entry);
                        if buffer.len() >= config.batch_size {
                            Self::send_batch(&client, &endpoint, &config.auth_token, &mut buffer, &name).await;
                        }
                    }
                    Ok(None) => {
                        // Channel closed
                        break;
                    }
                    Err(_) => {
                        // Timeout
                        if !buffer.is_empty() {
                            Self::send_batch(&client, &endpoint, &config.auth_token, &mut buffer, &name).await;
                        }
                    }
                }

                if should_shutdown && buffer.is_empty() {
                    break;
                }
            }

            // 发送剩余的日志
            if !buffer.is_empty() {
                Self::send_batch(&client, &endpoint, &config.auth_token, &mut buffer, &name).await;
            }
            println!("Worker for target '{}' has shut down.", name);
        });

        // Store the handle so we can await it later
        let worker_handle = self.worker_handle.clone();
        tokio::spawn(async move {
            *worker_handle.lock().await = Some(handle);
        });
    }

    async fn send_batch(client: &Client, endpoint: &url::Url, token: &str, buffer: &mut Vec<Box<dyn Loggable>>, name: &str) {
        if buffer.is_empty() {
            return;
        }

        let entries_as_json: Vec<_> = buffer.iter().map(|e| e.to_json().unwrap()).collect();
        let body = format!("[{}]", entries_as_json.join(","));

        let mut retries = 0;
        loop {
            let mut request = client
                .post(endpoint.clone())
                .body(body.clone())
                .header("Content-Type", "application/json");
            if !token.is_empty() {
                request = request.bearer_auth(token);
            }

            match request.send().await {
                Ok(resp) if resp.status().is_success() => {
                    // println!("Successfully sent batch of {} logs to {}", buffer.len(), name);
                    buffer.clear();
                    return;
                }
                Ok(resp) => {
                    eprintln!("Error sending logs to {}: HTTP Status {}", name, resp.status());
                }
                Err(e) => {
                    eprintln!("Network error sending logs to {}: {}", name, e);
                }
            }

            retries += 1;
            if retries > default_max_retry() {
                eprintln!("Failed to send batch to {} after {} retries. Dropping logs.", name, retries - 1);
                buffer.clear();
                return;
            }
            sleep(default_retry_interval()).await;
        }
    }
}

#[async_trait]
impl Target for HttpTarget {
    async fn send(&self, entry: Box<dyn Loggable>) -> Result<(), Box<dyn Error + Send>> {
        if self.shutdown_signal.load(Ordering::SeqCst) {
            return Err("Target is shutting down".into());
        }

        match self.sender.try_send(entry) {
            Ok(_) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => {
                eprintln!("Log queue for target '{}' is full. Dropping log.", self.name);
                Err("Queue full".into())
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                eprintln!("Log channel for target '{}' is closed.", self.name);
                Err("Channel closed".into())
            }
        }
    }

    fn name(&self) -> &str {
        &self.name
    }

    async fn shutdown(&self) {
        println!("Initiating shutdown for target '{}'...", self.name);
        self.shutdown_signal.store(true, Ordering::SeqCst);

        // 克隆 sender 并关闭它，这将导致 worker 中的 recv() 返回 None
        let sender_clone = self.sender.clone();
        sender_clone.closed().await;

        if let Some(handle) = self.worker_handle.lock().await.take() {
            if let Err(e) = handle.await {
                eprintln!("Error waiting for worker of target '{}' to shut down: {}", self.name, e);
            }
        }
    }
}
