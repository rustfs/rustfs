use crate::{
    error::TargetError, integration::NotificationMetrics,
    store::{Key, Store},
    target::Target,
    Event,
    StoreError,
};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Semaphore};
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

/// Streams events from the store to the target
pub async fn stream_events(
    store: &mut (dyn Store<crate::event::Event, Error = StoreError, Key = Key> + Send),
    target: &dyn Target,
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
                info!(
                    "Cancellation received during processing for target: {}",
                    target.name()
                );
                return;
            }

            let mut retry_count = 0;
            let mut success = false;

            // Retry logic
            while retry_count < MAX_RETRIES && !success {
                match target.send_from_store(key.clone()).await {
                    Ok(_) => {
                        info!("Successfully sent event for target: {}", target.name());
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
                                sleep(Duration::from_secs((retry_count * 5) as u64)).await; // 指数退避
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
                warn!(
                    "Max retries exceeded for event {}, target: {}, skipping",
                    key.to_string(),
                    target.name()
                );
            }
        }

        // Small delay before next iteration
        sleep(Duration::from_millis(100)).await;
    }
}

/// Starts the event streaming process for a target
pub fn start_event_stream(
    mut store: Box<dyn Store<Event, Error = StoreError, Key = Key> + Send>,
    target: Arc<dyn Target + Send + Sync>,
) -> mpsc::Sender<()> {
    let (cancel_tx, cancel_rx) = mpsc::channel(1);

    tokio::spawn(async move {
        stream_events(&mut *store, &*target, cancel_rx).await;
        info!("Event stream stopped for target: {}", target.name());
    });

    cancel_tx
}

/// Start event stream with batch processing
pub fn start_event_stream_with_batching(
    mut store: Box<dyn Store<Event, Error = StoreError, Key = Key> + Send>,
    target: Arc<dyn Target + Send + Sync>,
    metrics: Arc<NotificationMetrics>,
    semaphore: Arc<Semaphore>,
) -> mpsc::Sender<()> {
    let (cancel_tx, cancel_rx) = mpsc::channel(1);
    debug!(
        "Starting event stream with batching for target: {}",
        target.name()
    );
    tokio::spawn(async move {
        stream_events_with_batching(&mut *store, &*target, cancel_rx, metrics, semaphore).await;
        info!("Event stream stopped for target: {}", target.name());
    });

    cancel_tx
}

/// 带批处理的事件流处理
pub async fn stream_events_with_batching(
    store: &mut (dyn Store<Event, Error = StoreError, Key = Key> + Send),
    target: &dyn Target,
    mut cancel_rx: mpsc::Receiver<()>,
    metrics: Arc<NotificationMetrics>,
    semaphore: Arc<Semaphore>,
) {
    info!(
        "Starting event stream with batching for target: {}",
        target.name()
    );

    // Configuration parameters
    const DEFAULT_BATCH_SIZE: usize = 1;
    let batch_size = std::env::var("RUSTFS_EVENT_BATCH_SIZE")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(DEFAULT_BATCH_SIZE);
    const BATCH_TIMEOUT: Duration = Duration::from_secs(5);
    const MAX_RETRIES: usize = 5;
    const BASE_RETRY_DELAY: Duration = Duration::from_secs(2);

    let mut batch = Vec::with_capacity(batch_size);
    let mut batch_keys = Vec::with_capacity(batch_size);
    let mut last_flush = Instant::now();

    loop {
        // 检查取消信号
        if cancel_rx.try_recv().is_ok() {
            info!("Cancellation received for target: {}", target.name());
            return;
        }

        // 获取存储中的事件列表
        let keys = store.list();
        debug!(
            "Found {} keys in store for target: {}",
            keys.len(),
            target.name()
        );
        if keys.is_empty() {
            // 如果批处理中有数据且超时，则刷新批处理
            if !batch.is_empty() && last_flush.elapsed() >= BATCH_TIMEOUT {
                process_batch(
                    &mut batch,
                    &mut batch_keys,
                    target,
                    MAX_RETRIES,
                    BASE_RETRY_DELAY,
                    &metrics,
                    &semaphore,
                )
                .await;
                last_flush = Instant::now();
            }

            // 无事件，等待后再检查
            tokio::time::sleep(Duration::from_millis(500)).await;
            continue;
        }

        // 处理每个事件
        for key in keys {
            // 再次检查取消信号
            if cancel_rx.try_recv().is_ok() {
                info!(
                    "Cancellation received during processing for target: {}",
                    target.name()
                );

                // 在退出前处理已收集的批次
                if !batch.is_empty() {
                    process_batch(
                        &mut batch,
                        &mut batch_keys,
                        target,
                        MAX_RETRIES,
                        BASE_RETRY_DELAY,
                        &metrics,
                        &semaphore,
                    )
                    .await;
                }
                return;
            }

            // 尝试从存储中获取事件
            match store.get(&key) {
                Ok(event) => {
                    // 添加到批处理
                    batch.push(event);
                    batch_keys.push(key);
                    metrics.increment_processing();

                    // 如果批次已满或距离上次刷新已经过了足够时间，则处理批次
                    if batch.len() >= batch_size || last_flush.elapsed() >= BATCH_TIMEOUT {
                        process_batch(
                            &mut batch,
                            &mut batch_keys,
                            target,
                            MAX_RETRIES,
                            BASE_RETRY_DELAY,
                            &metrics,
                            &semaphore,
                        )
                        .await;
                        last_flush = Instant::now();
                    }
                }
                Err(e) => {
                    error!(
                        "Failed to target: {}, get event {} from store: {}",
                        target.name(),
                        key.to_string(),
                        e
                    );
                    // 可以考虑删除无法读取的事件，防止无限循环尝试读取
                    match store.del(&key) {
                        Ok(_) => {
                            info!("Deleted corrupted event {} from store", key.to_string());
                        }
                        Err(del_err) => {
                            error!(
                                "Failed to delete corrupted event {}: {}",
                                key.to_string(),
                                del_err
                            );
                        }
                    }

                    metrics.increment_failed();
                }
            }
        }

        // 小延迟再进行下一轮检查
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// 处理事件批次
async fn process_batch(
    batch: &mut Vec<Event>,
    batch_keys: &mut Vec<Key>,
    target: &dyn Target,
    max_retries: usize,
    base_delay: Duration,
    metrics: &Arc<NotificationMetrics>,
    semaphore: &Arc<Semaphore>,
) {
    debug!(
        "Processing batch of {} events for target: {}",
        batch.len(),
        target.name()
    );
    if batch.is_empty() {
        return;
    }

    // 获取信号量许可，限制并发
    let permit = match semaphore.clone().acquire_owned().await {
        Ok(permit) => permit,
        Err(e) => {
            error!("Failed to acquire semaphore permit: {}", e);
            return;
        }
    };

    // 处理批次中的每个事件
    for (_event, key) in batch.iter().zip(batch_keys.iter()) {
        let mut retry_count = 0;
        let mut success = false;

        // 重试逻辑
        while retry_count < max_retries && !success {
            match target.send_from_store(key.clone()).await {
                Ok(_) => {
                    info!(
                        "Successfully sent event for target: {}, Key: {}",
                        target.name(),
                        key.to_string()
                    );
                    success = true;
                    metrics.increment_processed();
                }
                Err(e) => {
                    // 根据错误类型采用不同的重试策略
                    match &e {
                        TargetError::NotConnected => {
                            warn!("Target {} not connected, retrying...", target.name());
                            retry_count += 1;
                            tokio::time::sleep(base_delay * (1 << retry_count)).await; // 指数退避
                        }
                        TargetError::Timeout(_) => {
                            warn!("Timeout for target {}, retrying...", target.name());
                            retry_count += 1;
                            tokio::time::sleep(base_delay * (1 << retry_count)).await;
                        }
                        _ => {
                            // 永久性错误，跳过此事件
                            error!("Permanent error for target {}: {}", target.name(), e);
                            metrics.increment_failed();
                            break;
                        }
                    }
                }
            }
        }

        // 处理最大重试次数耗尽的情况
        if retry_count >= max_retries && !success {
            warn!(
                "Max retries exceeded for event {}, target: {}, skipping",
                key.to_string(),
                target.name()
            );
            metrics.increment_failed();
        }
    }

    // 清空已处理的批次
    batch.clear();
    batch_keys.clear();

    // 释放信号量许可（通过 drop）
    drop(permit);
}
