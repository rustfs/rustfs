use crate::{ChannelAdapter, Error, Event, EventStore, NotifierConfig, event_bus};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::instrument;

/// The `NotificationSystem` struct represents the notification system.
/// It manages the event bus and the adapters.
/// It is responsible for sending and receiving events.
/// It also handles the shutdown process.
pub struct NotifierSystem {
    tx: mpsc::Sender<Event>,
    rx: Option<mpsc::Receiver<Event>>,
    store: Arc<EventStore>,
    shutdown: CancellationToken,
    shutdown_complete: Option<tokio::sync::oneshot::Sender<()>>,
    shutdown_receiver: Option<tokio::sync::oneshot::Receiver<()>>,
}

impl NotifierSystem {
    /// Creates a new `NotificationSystem` instance.
    #[instrument(skip(config))]
    pub async fn new(config: NotifierConfig) -> Result<Self, Error> {
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
        // Initialize shutdown_complete to Some(tx)
        let (complete_tx, complete_rx) = tokio::sync::oneshot::channel();
        Ok(Self {
            tx,
            rx: Some(rx),
            store,
            shutdown,
            shutdown_complete: Some(complete_tx),
            shutdown_receiver: Some(complete_rx),
        })
    }

    /// Starts the notification system.
    /// It initializes the event bus and the producer.
    #[instrument(skip_all)]
    pub async fn start(&mut self, adapters: Vec<Arc<dyn ChannelAdapter>>) -> Result<(), Error> {
        if self.shutdown.is_cancelled() {
            let error = Error::custom("System is shutting down");
            self.handle_error("start", &error);
            return Err(error);
        }
        self.log(tracing::Level::INFO, "start", "Starting the notification system");
        let rx = self.rx.take().ok_or_else(|| Error::EventBusStarted)?;
        let shutdown_clone = self.shutdown.clone();
        let store_clone = self.store.clone();
        let shutdown_complete = self.shutdown_complete.take();

        tokio::spawn(async move {
            if let Err(e) = event_bus(rx, adapters, store_clone, shutdown_clone, shutdown_complete).await {
                tracing::error!("Event bus failed: {}", e);
            }
        });
        self.log(tracing::Level::INFO, "start", "Notification system started successfully");
        Ok(())
    }

    /// Sends an event to the notification system.
    /// This method is used to send events to the event bus.
    #[instrument(skip(self))]
    pub async fn send_event(&self, event: Event) -> Result<(), Error> {
        self.log(tracing::Level::DEBUG, "send_event", &format!("Sending event: {:?}", event));
        if self.shutdown.is_cancelled() {
            let error = Error::custom("System is shutting down");
            self.handle_error("send_event", &error);
            return Err(error);
        }
        if let Err(e) = self.tx.send(event).await {
            let error = Error::ChannelSend(Box::new(e));
            self.handle_error("send_event", &error);
            return Err(error);
        }
        self.log(tracing::Level::INFO, "send_event", "Event sent successfully");
        Ok(())
    }

    /// Shuts down the notification system.
    /// This method is used to cancel the event bus and producer tasks.
    #[instrument(skip(self))]
    pub async fn shutdown(&mut self) -> Result<(), Error> {
        tracing::info!("Shutting down the notification system");
        self.shutdown.cancel();
        // wait for the event bus to be completely closed
        if let Some(receiver) = self.shutdown_receiver.take() {
            match receiver.await {
                Ok(_) => {
                    tracing::info!("Event bus shutdown completed successfully");
                    Ok(())
                }
                Err(e) => {
                    let error = Error::custom(format!("Failed to receive shutdown completion: {}", e).as_str());
                    self.handle_error("shutdown", &error);
                    Err(error)
                }
            }
        } else {
            tracing::warn!("Shutdown receiver not available, the event bus might still be running");
            Err(Error::custom("Shutdown receiver not available"))
        }
    }

    /// shutdown state  
    pub fn shutdown_cancelled(&self) -> bool {
        self.shutdown.is_cancelled()
    }

    #[instrument(skip(self))]
    pub fn handle_error(&self, context: &str, error: &Error) {
        self.log(tracing::Level::ERROR, context, &format!("{:?}", error));
        // TODO Can be extended to record to files or send to monitoring systems
    }

    #[instrument(skip(self))]
    fn log(&self, level: tracing::Level, context: &str, message: &str) {
        match level {
            tracing::Level::ERROR => tracing::error!("[{}] {}", context, message),
            tracing::Level::WARN => tracing::warn!("[{}] {}", context, message),
            tracing::Level::INFO => tracing::info!("[{}] {}", context, message),
            tracing::Level::DEBUG => tracing::debug!("[{}] {}", context, message),
            tracing::Level::TRACE => tracing::trace!("[{}] {}", context, message),
        }
    }
}
