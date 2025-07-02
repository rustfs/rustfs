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

use crate::sinks::Sink;
use crate::{LogRecord, UnifiedLogEntry};
use async_trait::async_trait;
use std::sync::Arc;

/// Kafka Sink Implementation
pub struct KafkaSink {
    producer: rdkafka::producer::FutureProducer,
    topic: String,
    batch_size: usize,
    batch_timeout_ms: u64,
    entries: Arc<tokio::sync::Mutex<Vec<UnifiedLogEntry>>>,
    last_flush: Arc<std::sync::atomic::AtomicU64>,
}

impl KafkaSink {
    /// Create a new KafkaSink instance
    pub fn new(producer: rdkafka::producer::FutureProducer, topic: String, batch_size: usize, batch_timeout_ms: u64) -> Self {
        // Create Arc-wrapped values first
        let entries = Arc::new(tokio::sync::Mutex::new(Vec::with_capacity(batch_size)));
        let last_flush = Arc::new(std::sync::atomic::AtomicU64::new(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        ));
        let sink = KafkaSink {
            producer: producer.clone(),
            topic: topic.clone(),
            batch_size,
            batch_timeout_ms,
            entries: entries.clone(),
            last_flush: last_flush.clone(),
        };

        // Start background flusher
        tokio::spawn(Self::periodic_flush(producer, topic, entries, last_flush, batch_timeout_ms));

        sink
    }

    /// Add a getter method to read the batch_timeout_ms field
    #[allow(dead_code)]
    pub fn batch_timeout(&self) -> u64 {
        self.batch_timeout_ms
    }

    /// Add a method to dynamically adjust the timeout if needed
    #[allow(dead_code)]
    pub fn set_batch_timeout(&mut self, new_timeout_ms: u64) {
        self.batch_timeout_ms = new_timeout_ms;
    }

    async fn periodic_flush(
        producer: rdkafka::producer::FutureProducer,
        topic: String,
        entries: Arc<tokio::sync::Mutex<Vec<UnifiedLogEntry>>>,
        last_flush: Arc<std::sync::atomic::AtomicU64>,
        timeout_ms: u64,
    ) {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_millis(timeout_ms / 2)).await;

            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

            let last = last_flush.load(std::sync::atomic::Ordering::Relaxed);

            if now - last >= timeout_ms {
                let mut batch = entries.lock().await;
                if !batch.is_empty() {
                    Self::send_batch(&producer, &topic, batch.drain(..).collect()).await;
                    last_flush.store(now, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }
    }

    async fn send_batch(producer: &rdkafka::producer::FutureProducer, topic: &str, entries: Vec<UnifiedLogEntry>) {
        for entry in entries {
            let payload = match serde_json::to_string(&entry) {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("Failed to serialize log entry: {}", e);
                    continue;
                }
            };

            let span_id = entry.get_timestamp().to_rfc3339();

            let _ = producer
                .send(
                    rdkafka::producer::FutureRecord::to(topic).payload(&payload).key(&span_id),
                    std::time::Duration::from_secs(5),
                )
                .await;
        }
    }
}

#[async_trait]
impl Sink for KafkaSink {
    async fn write(&self, entry: &UnifiedLogEntry) {
        let mut batch = self.entries.lock().await;
        batch.push(entry.clone());

        let should_flush_by_size = batch.len() >= self.batch_size;
        let should_flush_by_time = {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            let last = self.last_flush.load(std::sync::atomic::Ordering::Relaxed);
            now - last >= self.batch_timeout_ms
        };

        if should_flush_by_size || should_flush_by_time {
            // Existing flush logic
            let entries_to_send: Vec<UnifiedLogEntry> = batch.drain(..).collect();
            let producer = self.producer.clone();
            let topic = self.topic.clone();

            self.last_flush.store(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
                std::sync::atomic::Ordering::Relaxed,
            );

            tokio::spawn(async move {
                KafkaSink::send_batch(&producer, &topic, entries_to_send).await;
            });
        }
    }
}

impl Drop for KafkaSink {
    fn drop(&mut self) {
        // Perform any necessary cleanup here
        // For example, you might want to flush any remaining entries
        let producer = self.producer.clone();
        let topic = self.topic.clone();
        let entries = self.entries.clone();
        let last_flush = self.last_flush.clone();

        tokio::spawn(async move {
            let mut batch = entries.lock().await;
            if !batch.is_empty() {
                KafkaSink::send_batch(&producer, &topic, batch.drain(..).collect()).await;
                last_flush.store(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
            }
        });

        eprintln!("Dropping KafkaSink with topic: {}", self.topic);
    }
}
