use crate::ChannelAdapter;
use crate::Error;
use crate::EventStore;
use crate::{Event, Log};
use chrono::Utc;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// Handles incoming events from the producer.
///
/// This function is responsible for receiving events from the producer and sending them to the appropriate adapters.
/// It also handles the shutdown process and saves any pending logs to the event store.
pub async fn event_bus(
    mut rx: mpsc::Receiver<Event>,
    adapters: Vec<Arc<dyn ChannelAdapter>>,
    store: Arc<EventStore>,
    shutdown: CancellationToken,
) -> Result<(), Error> {
    let mut pending_logs = Vec::new();
    let mut current_log = Log {
        event_name: crate::event::Name::Everything,
        key: Utc::now().timestamp().to_string(),
        records: Vec::new(),
    };

    loop {
        tokio::select! {
            Some(event) = rx.recv() => {
                current_log.records.push(event.clone());
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
                        current_log.records.retain(|e| e.id != event.id);
                    }
                }
                if !current_log.records.is_empty() {
                    pending_logs.push(current_log.clone());
                }
                current_log.records.clear();
            }
            _ = shutdown.cancelled() => {
                tracing::info!("Shutting down event bus, saving pending logs...");
                if !current_log.records.is_empty() {
                    pending_logs.push(current_log);
                }
                store.save_logs(&pending_logs).await?;
                break;
            }
            else => break,
        }
    }
    Ok(())
}
