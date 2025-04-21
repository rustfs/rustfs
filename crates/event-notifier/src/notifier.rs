use crate::{event_bus, ChannelAdapter, Error, Event, EventStore, NotificationConfig};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// The `NotificationSystem` struct represents the notification system.
/// It manages the event bus and the adapters.
/// It is responsible for sending and receiving events.
/// It also handles the shutdown process.
pub struct NotificationSystem {
    tx: mpsc::Sender<Event>,
    rx: Option<mpsc::Receiver<Event>>,
    store: Arc<EventStore>,
    shutdown: CancellationToken,
}

impl NotificationSystem {
    /// Creates a new `NotificationSystem` instance.
    pub async fn new(config: NotificationConfig) -> Result<Self, Error> {
        let (tx, rx) = mpsc::channel::<Event>(config.channel_capacity);
        let store = Arc::new(EventStore::new(&config.store_path).await?);
        let shutdown = CancellationToken::new();

        let restored_logs = store.load_logs().await?;
        for log in restored_logs {
            for event in log.records {
                // For example, where the send method may return a SendError when calling it
                tx.send(event).await.map_err(|e| Error::ChannelSend(Box::new(e)))?;
            }
        }

        Ok(Self {
            tx,
            rx: Some(rx),
            store,
            shutdown,
        })
    }

    /// Starts the notification system.
    /// It initializes the event bus and the producer.
    pub async fn start(&mut self, adapters: Vec<Arc<dyn ChannelAdapter>>) -> Result<(), Error> {
        let rx = self.rx.take().ok_or_else(|| Error::EventBusStarted)?;

        let shutdown_clone = self.shutdown.clone();
        let store_clone = self.store.clone();
        tokio::spawn(async move {
            if let Err(e) = event_bus(rx, adapters, store_clone, shutdown_clone).await {
                tracing::error!("Event bus failed: {}", e);
            }
        });

        Ok(())
    }

    /// Sends an event to the notification system.
    /// This method is used to send events to the event bus.
    pub async fn send_event(&self, event: Event) -> Result<(), Error> {
        self.tx.send(event).await.map_err(|e| Error::ChannelSend(Box::new(e)))?;
        Ok(())
    }

    /// Shuts down the notification system.
    /// This method is used to cancel the event bus and producer tasks.
    pub fn shutdown(&self) {
        tracing::info!("Shutting down the notification system");
        self.shutdown.cancel();
    }
}
