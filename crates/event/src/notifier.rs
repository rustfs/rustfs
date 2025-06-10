use crate::config::EventNotifierConfig;
use crate::event::Event;
use common::error::{Error, Result};
use ecstore::store::ECStore;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn};

/// Event Notifier
pub struct EventNotifier {
    /// The event sending channel
    sender: mpsc::Sender<Event>,
    /// Receiver task handle
    task_handle: Option<tokio::task::JoinHandle<()>>,
    /// Configuration information
    config: EventNotifierConfig,
    /// Turn off tagging
    shutdown: CancellationToken,
    /// Close the notification channel
    shutdown_complete_tx: Option<broadcast::Sender<()>>,
}

impl EventNotifier {
    /// Create a new event notifier
    #[instrument(skip_all)]
    pub async fn new(store: Arc<ECStore>) -> Result<Self> {
        let manager = crate::store::manager::EventManager::new(store);

        let manager = Arc::new(manager.await);

        // Initialize the configuration
        let config = manager.clone().init().await?;

        // Create adapters
        let adapters = manager.clone().create_adapters().await?;
        info!("Created {} adapters", adapters.len());

        // Create a close marker
        let shutdown = CancellationToken::new();
        let (shutdown_complete_tx, _) = broadcast::channel(1);

        // 创建事件通道 - 使用默认容量，因为每个适配器都有自己的队列
        // 这里使用较小的通道容量，因为事件会被快速分发到适配器
        let (sender, mut receiver) = mpsc::channel::<Event>(100);

        let shutdown_clone = shutdown.clone();
        let shutdown_complete_tx_clone = shutdown_complete_tx.clone();
        let adapters_clone = adapters.clone();

        // Start the event processing task
        let task_handle = tokio::spawn(async move {
            debug!("The event processing task starts");

            loop {
                tokio::select! {
                    Some(event) = receiver.recv() => {
                        debug!("The event is received:{}", event.id);

                        // Distribute to all adapters
                        for adapter in &adapters_clone {
                            let adapter_name = adapter.name();
                            match adapter.send(&event).await {
                                Ok(_) => {
                                    debug!("Event {} Successfully sent to the adapter {}", event.id, adapter_name);
                                }
                                Err(e) => {
                                    error!("Event {} send to adapter {} failed:{}", event.id, adapter_name, e);
                                }
                            }
                        }
                    }

                    _ = shutdown_clone.cancelled() => {
                        info!("A shutdown signal is received, and the event processing task is stopped");
                        let _ = shutdown_complete_tx_clone.send(());
                        break;
                    }
                }
            }

            debug!("The event processing task has been stopped");
        });

        Ok(Self {
            sender,
            task_handle: Some(task_handle),
            config,
            shutdown,
            shutdown_complete_tx: Some(shutdown_complete_tx),
        })
    }

    /// Turn off the event notifier
    pub async fn shutdown(&mut self) -> Result<()> {
        info!("Turn off the event notifier");
        self.shutdown.cancel();

        if let Some(shutdown_tx) = self.shutdown_complete_tx.take() {
            let mut rx = shutdown_tx.subscribe();

            // Wait for the shutdown to complete the signal or time out
            tokio::select! {
                _ = rx.recv() => {
                    debug!("A shutdown completion signal is received");
                }
                _ = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                    warn!("Shutdown timeout and forced termination");
                }
            }
        }

        if let Some(handle) = self.task_handle.take() {
            handle.abort();
            match handle.await {
                Ok(_) => debug!("The event processing task has been terminated gracefully"),
                Err(e) => {
                    if e.is_cancelled() {
                        debug!("The event processing task has been canceled");
                    } else {
                        error!("An error occurred while waiting for the event processing task to terminate:{}", e);
                    }
                }
            }
        }

        info!("The event notifier is completely turned off");
        Ok(())
    }

    /// Send events
    pub async fn send(&self, event: Event) -> Result<()> {
        self.sender
            .send(event)
            .await
            .map_err(|e| Error::msg(format!("Failed to send events to channel:{}", e)))
    }

    /// Get the current configuration
    pub fn config(&self) -> &EventNotifierConfig {
        &self.config
    }
}
