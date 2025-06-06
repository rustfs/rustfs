use crate::config::notifier::EventNotifierConfig;
use crate::notifier::EventNotifier;
use common::error::Result;
use ecstore::store::ECStore;
use once_cell::sync::OnceCell;
use std::sync::{Arc, Mutex};
use tracing::{debug, error, info};

/// Global event system
pub struct EventSystem {
    /// Event Notifier
    notifier: Mutex<Option<EventNotifier>>,
}

impl EventSystem {
    /// Create a new event system
    pub fn new() -> Self {
        Self {
            notifier: Mutex::new(None),
        }
    }

    /// Initialize the event system
    pub async fn init(&self, store: Arc<ECStore>) -> Result<EventNotifierConfig> {
        info!("Initialize the event system");
        let notifier = EventNotifier::new(store).await?;
        let config = notifier.config().clone();

        let mut guard = self
            .notifier
            .lock()
            .map_err(|e| common::error::Error::msg(format!("Failed to acquire locks:{}", e)))?;

        *guard = Some(notifier);
        debug!("The event system initialization is complete");

        Ok(config)
    }

    /// Send events
    pub async fn send_event(&self, event: crate::Event) -> Result<()> {
        let guard = self
            .notifier
            .lock()
            .map_err(|e| common::error::Error::msg(format!("Failed to acquire locks:{}", e)))?;

        if let Some(notifier) = &*guard {
            notifier.send(event).await
        } else {
            error!("The event system is not initialized");
            Err(common::error::Error::msg("The event system is not initialized"))
        }
    }

    /// Shut down the event system
    pub async fn shutdown(&self) -> Result<()> {
        info!("Shut down the event system");
        let mut guard = self
            .notifier
            .lock()
            .map_err(|e| common::error::Error::msg(format!("Failed to acquire locks:{}", e)))?;

        if let Some(ref mut notifier) = *guard {
            notifier.shutdown().await?;
            *guard = None;
            info!("The event system is down");
            Ok(())
        } else {
            debug!("The event system has been shut down");
            Ok(())
        }
    }
}

/// A global event system instance
pub static GLOBAL_EVENT_SYS: OnceCell<EventSystem> = OnceCell::new();

/// Initialize the global event system
pub fn init_global_event_system() -> &'static EventSystem {
    GLOBAL_EVENT_SYS.get_or_init(EventSystem::new)
}
