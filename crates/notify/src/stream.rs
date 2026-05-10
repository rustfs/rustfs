#![allow(dead_code)]
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

use crate::{Event, integration::NotificationMetrics};
use rustfs_targets::{
    StoreError, Target, TargetError,
    store::{Key, Store, ensure_store_entry_raw_readable},
    target::QueuedPayload,
};
use rustfs_utils::get_env_usize;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Semaphore, mpsc};
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

/// Legacy compatibility shim for historical notify replay helpers.
///
/// New runtime orchestration should prefer `rustfs_targets::start_replay_worker`
/// and the shared runtime helpers in `rustfs_targets::runtime`.
///
/// This module is kept temporarily to preserve local call compatibility while
/// Phase 3 is being completed, but it is no longer the primary runtime path.
///
/// Streams events from the store to the target with retry logic
///
/// # Arguments
/// - `store`: The event store
/// - `target`: The target to send events to
/// - `cancel_rx`: Receiver to listen for cancellation signals
pub async fn stream_events(
    store: &mut (dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send),
    target: &dyn Target<Event>,
    mut cancel_rx: mpsc::Receiver<()>,
) {
    info!("Starting event stream for target: {}", target.name());

    // Retry configuration
    const MAX_RETRIES: usize = 5;
    const RETRY_DELAY: Duration = Duration::from_secs(5);

    loop {
        // Check for cancellation signal
        if cancel_rx.try_recv().is_ok() {
            info!("Cancellation received for target: {}", target.name());
            return;
        }

        // Get list of events in the store
        let keys = store.list();
        if keys.is_empty() {
            // No events, wait before checking again
            sleep(Duration::from_secs(1)).await;
            continue;
        }

        // Process each event
        for key in keys {
            // Check for cancellation before processing each event
            if cancel_rx.try_recv().is_ok() {
                info!("Cancellation received during processing for target: {}", target.name());
                return;
            }

            let mut retry_count = 0;
            let mut success = false;

            // Retry logic
            while retry_count < MAX_RETRIES && !success {
                match target.send_from_store(key.clone()).await {
                    Ok(_) => {
                        info!("Successfully sent event for target: {}", target.name());
                        // send_from_store deletes the event from store on success
                        success = true;
                    }
                    Err(e) => {
                        // Handle specific errors
                        match &e {
                            TargetError::NotConnected => {
                                warn!("Target {} not connected, retrying...", target.name());
                                retry_count += 1;
                                sleep(RETRY_DELAY).await;
                            }
                            TargetError::Timeout(_) => {
                                warn!("Timeout for target {}, retrying...", target.name());
                                retry_count += 1;
                                sleep(Duration::from_secs((retry_count * 5) as u64)).await; // Exponential backoff
                            }
                            _ => {
                                // Permanent error, skip this event
                                error!("Permanent error for target {}: {}", target.name(), e);
                                break;
                            }
                        }
                    }
                }
            }

            // Remove event from store if successfully sent
            if retry_count >= MAX_RETRIES && !success {
                warn!("Max retries exceeded for event {}, target: {}, skipping", key.to_string(), target.name());
            }
        }

        // Small delay before next iteration
        sleep(Duration::from_millis(100)).await;
    }
}

/// Starts the event streaming process for a target
///
/// # Arguments
/// - `store`: The event store
/// - `target`: The target to send events to
///
/// # Returns
/// A sender to signal cancellation of the event stream
pub fn start_event_stream(
    mut store: Box<dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send>,
    target: Arc<dyn Target<Event> + Send + Sync>,
) -> mpsc::Sender<()> {
    let (cancel_tx, cancel_rx) = mpsc::channel(1);

    tokio::spawn(async move {
        stream_events(&mut *store, &*target, cancel_rx).await;
        info!("Event stream stopped for target: {}", target.name());
    });

    cancel_tx
}

/// Start event stream with batch processing
///
/// # Arguments
/// - `store`: The event store
/// - `target`: The target to send events to clients
/// - `metrics`: Metrics for monitoring
/// - `semaphore`: Semaphore to limit concurrency
///
/// # Returns
/// A sender to signal cancellation of the event stream
pub fn start_event_stream_with_batching(
    mut store: Box<dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send>,
    target: Arc<dyn Target<Event> + Send + Sync>,
    metrics: Arc<NotificationMetrics>,
    semaphore: Arc<Semaphore>,
) -> mpsc::Sender<()> {
    let (cancel_tx, cancel_rx) = mpsc::channel(1);
    debug!("Starting event stream with batching for target: {}", target.name());
    tokio::spawn(async move {
        stream_events_with_batching(&mut *store, &*target, cancel_rx, metrics, semaphore).await;
        info!("Event stream stopped for target: {}", target.name());
    });

    cancel_tx
}

/// Event stream processing with batch processing
///
/// # Arguments
/// - `store`: The event store
/// - `target`: The target to send events to clients
/// - `cancel_rx`: Receiver to listen for cancellation signals
/// - `metrics`: Metrics for monitoring
/// - `semaphore`: Semaphore to limit concurrency
///
/// # Notes
/// This function processes events in batches to improve efficiency.
pub async fn stream_events_with_batching(
    store: &mut (dyn Store<QueuedPayload, Error = StoreError, Key = Key> + Send),
    target: &dyn Target<Event>,
    mut cancel_rx: mpsc::Receiver<()>,
    metrics: Arc<NotificationMetrics>,
    semaphore: Arc<Semaphore>,
) {
    info!("Starting event stream with batching for target: {}", target.name());

    // Configuration parameters
    const DEFAULT_BATCH_SIZE: usize = 1;
    let batch_size = get_env_usize("RUSTFS_EVENT_BATCH_SIZE", DEFAULT_BATCH_SIZE);
    const BATCH_TIMEOUT: Duration = Duration::from_secs(5);
    const MAX_RETRIES: usize = 5;
    const BASE_RETRY_DELAY: Duration = Duration::from_secs(2);

    let mut batch_keys = Vec::with_capacity(batch_size);
    let mut last_flush = Instant::now();

    loop {
        // Check the cancel signal
        if cancel_rx.try_recv().is_ok() {
            info!("Cancellation received for target: {}", target.name());
            return;
        }

        // Get a list of events in storage
        let keys = store.list();
        debug!("Found {} keys in store for target: {}", keys.len(), target.name());
        if keys.is_empty() {
            // If there is data in the batch and timeout, refresh the batch
            if !batch_keys.is_empty() && last_flush.elapsed() >= BATCH_TIMEOUT {
                process_batch(&mut batch_keys, target, MAX_RETRIES, BASE_RETRY_DELAY, &metrics, &semaphore).await;
                last_flush = Instant::now();
            }

            // No event, wait before checking
            tokio::time::sleep(Duration::from_millis(500)).await;
            continue;
        }

        // Handle each event
        for key in keys {
            // Check the cancel signal again
            if cancel_rx.try_recv().is_ok() {
                info!("Cancellation received during processing for target: {}", target.name());

                // Processing collected batches before exiting
                if !batch_keys.is_empty() {
                    process_batch(&mut batch_keys, target, MAX_RETRIES, BASE_RETRY_DELAY, &metrics, &semaphore).await;
                }
                return;
            }

            // Skip unreadable entries so a single corrupt file cannot stall the stream.
            // ensure_store_entry_raw_readable attempts get_raw; on I/O error it calls del() to
            // remove the corrupt entry before returning Err, so no cleanup is needed here.
            match ensure_store_entry_raw_readable(&*store, &key) {
                Ok(true) => {}         // entry is readable, proceed
                Ok(false) => continue, // entry not found (already removed), skip
                Err(err) => {
                    warn!("Skipping unreadable store entry {} for target {}: {}", key, target.name(), err);
                    continue; // corrupt entry was already deleted by ensure_store_entry_raw_readable
                }
            }

            batch_keys.push(key);
            metrics.increment_processing();

            // If the batch is full or enough time has passed since the last refresh, the batch will be processed
            if batch_keys.len() >= batch_size || last_flush.elapsed() >= BATCH_TIMEOUT {
                process_batch(&mut batch_keys, target, MAX_RETRIES, BASE_RETRY_DELAY, &metrics, &semaphore).await;
                last_flush = Instant::now();
            }
        }

        // A small delay will be conducted to check the next round
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Processing event batches for targets
/// # Arguments
/// - `batch`: The batch of events to process
/// - `batch_keys`: The corresponding keys of the events in the batch
/// - `target`: The target to send events to clients
/// - `max_retries`: Maximum number of retries for sending an event
/// - `base_delay`: Base delay duration for retries
/// - `metrics`: Metrics for monitoring
/// - `semaphore`: Semaphore to limit concurrency
/// # Notes
/// This function processes a batch of events, sending each event to the target with retry
async fn process_batch(
    batch_keys: &mut Vec<Key>,
    target: &dyn Target<Event>,
    max_retries: usize,
    base_delay: Duration,
    metrics: &Arc<NotificationMetrics>,
    semaphore: &Arc<Semaphore>,
) {
    debug!("Processing batch of {} events for target: {}", batch_keys.len(), target.name());
    if batch_keys.is_empty() {
        return;
    }

    // Obtain semaphore permission to limit concurrency
    let permit = match semaphore.clone().acquire_owned().await {
        Ok(permit) => permit,
        Err(e) => {
            error!("Failed to acquire semaphore permit: {}", e);
            return;
        }
    };

    // Handle every event in the batch
    for key in batch_keys.iter() {
        let mut retry_count = 0;
        let mut success = false;

        // Retry logic
        while retry_count < max_retries && !success {
            match target.send_from_store(key.clone()).await {
                Ok(_) => {
                    debug!("Successfully sent event for target: {}, Key: {}", target.name(), key.to_string());
                    success = true;
                    metrics.increment_processed();
                }
                Err(e) => match &e {
                    TargetError::NotConnected => {
                        warn!("Target {} not connected, retrying...", target.name());
                        retry_count += 1;
                        let jitter = Duration::from_millis(key.to_string().len() as u64 % 500);
                        let backoff = 1u32 << retry_count as u32;
                        tokio::time::sleep(base_delay * backoff + jitter).await;
                    }
                    TargetError::Timeout(_) => {
                        warn!("Timeout for target {}, retrying...", target.name());
                        retry_count += 1;
                        let jitter = Duration::from_millis(key.to_string().len() as u64 % 500);
                        let backoff = 1u32 << retry_count as u32;
                        tokio::time::sleep(base_delay * backoff + jitter).await;
                    }
                    TargetError::Dropped(reason) => {
                        warn!("Dropped queued payload for target {}: {}", target.name(), reason);
                        metrics.increment_failed();
                        break;
                    }
                    _ => {
                        error!("Permanent error for target {}: {}", target.name(), e);
                        target.record_final_failure();
                        metrics.increment_failed();
                        break;
                    }
                },
            }
        }

        if retry_count >= max_retries && !success {
            warn!("Max retries exceeded for event {}, target: {}, skipping", key.to_string(), target.name());
            target.record_final_failure();
            metrics.increment_failed();
        }
    }

    // Clear processed batches
    batch_keys.clear();

    // Release semaphore permission (via drop)
    drop(permit);
}
