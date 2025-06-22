use crate::{Event, EventArgs, NotificationError, NotificationSystem};
use ecstore::config::Config;
use once_cell::sync::Lazy;
use std::sync::{Arc, OnceLock};

static NOTIFICATION_SYSTEM: OnceLock<Arc<NotificationSystem>> = OnceLock::new();
// Create a globally unique Notifier instance
pub static GLOBAL_NOTIFIER: Lazy<Notifier> = Lazy::new(|| Notifier {});

/// Initialize the global notification system with the given configuration.
/// This function should only be called once throughout the application life cycle.
pub async fn initialize(config: Config) -> Result<(), NotificationError> {
    // `new` is synchronous and responsible for creating instances
    let system = NotificationSystem::new(config);
    // `init` is asynchronous and responsible for performing I/O-intensive initialization
    system.init().await?;

    match NOTIFICATION_SYSTEM.set(Arc::new(system)) {
        Ok(_) => Ok(()),
        Err(_) => Err(NotificationError::AlreadyInitialized),
    }
}

/// Returns a handle to the global NotificationSystem instance.
/// Return None if the system has not been initialized.
pub fn notification_system() -> Option<Arc<NotificationSystem>> {
    NOTIFICATION_SYSTEM.get().cloned()
}

pub struct Notifier {
    // Notifier can hold state, but in this design we make it stateless,
    // Rely on getting an instance of NotificationSystem from the outside.
}

impl crate::notifier::Notifier {
    /// Notify an event asynchronously.
    /// This is the only entry point for all event notifications in the system.
    pub async fn notify(&self, args: EventArgs) {
        // Dependency injection or service positioning mode obtain NotificationSystem instance
        let notification_sys = match notification_system() {
            // If the notification system itself cannot be retrieved, it will be returned directly
            Some(sys) => sys,
            None => {
                tracing::error!("Notification system is not initialized.");
                return;
            }
        };

        // Avoid generating notifications for replica creation events
        if args.is_replication_request() {
            return;
        }

        // Create an event and send it
        let event = Event::new(args.clone());
        notification_sys
            .send_event(&args.bucket_name, &args.event_name.as_str(), &args.object.name.clone(), event)
            .await;
    }
}
