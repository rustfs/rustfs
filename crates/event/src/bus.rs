use crate::ChannelAdapter;
use crate::Error;
use crate::EventStore;
use crate::{Event, Log};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::instrument;

/// Handles incoming events from the producer.
///
/// This function is responsible for receiving events from the producer and sending them to the appropriate adapters.
/// It also handles the shutdown process and saves any pending logs to the event store.
#[instrument(skip_all)]
pub async fn event_bus(
    mut rx: mpsc::Receiver<Event>,
    adapters: Vec<Arc<dyn ChannelAdapter>>,
    store: Arc<EventStore>,
    shutdown: CancellationToken,
    shutdown_complete: Option<tokio::sync::oneshot::Sender<()>>,
) -> Result<(), Error> {
    let mut unprocessed_events = Vec::new();
    loop {
        tokio::select! {
            Some(event) = rx.recv() => {
                let mut send_tasks = Vec::new();
                for adapter in &adapters {
                    if event.channels.contains(&adapter.name()) {
                        let adapter = adapter.clone();
                        let event = event.clone();
                        send_tasks.push(tokio::spawn(async move {
                            if let Err(e) = adapter.send(&event).await {
                                tracing::error!("Failed to send event to {}: {}", adapter.name(), e);
                                Err(e)
                            } else {
                                Ok(())
                            }
                        }));
                    }
                }
                for task in send_tasks {
                    if task.await?.is_err() {
                        // If sending fails, add the event to the unprocessed list
                        let failed_event = event.clone();
                        unprocessed_events.push(failed_event);
                    }
                }
            }
            _ = shutdown.cancelled() => {
                tracing::info!("Shutting down event bus, saving pending logs...");
                // Check if there are still unprocessed messages in the channel
                while let Ok(Some(event)) = tokio::time::timeout(
                    Duration::from_millis(100),
                    rx.recv()
                ).await {
                    unprocessed_events.push(event);
                }

                // save only if there are unprocessed events
                if !unprocessed_events.is_empty() {
                    tracing::info!("Save {} unhandled events", unprocessed_events.len());
                    // create and save logging
                    let shutdown_log = Log {
                        event_name: crate::event::Name::Everything,
                        key: format!("shutdown_{}", SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()),
                        records: unprocessed_events,
                    };

                    store.save_logs(&[shutdown_log]).await?;
                } else {
                    tracing::info!("no unhandled events need to be saved");
                }
                 tracing::debug!("shutdown_complete is Some: {}", shutdown_complete.is_some());

                if let Some(complete_sender) = shutdown_complete {
                    // send a completion signal
                    let result = complete_sender.send(());
                    match result {
                        Ok(_) => tracing::info!("Event bus shutdown signal sent"),
                        Err(e) => tracing::error!("Failed to send event bus shutdown signal: {:?}", e),
                    }
                    tracing::info!("Shutting down event bus");
                }
                tracing::info!("Event bus shutdown complete");
                break;
            }
        }
    }
    Ok(())
}
