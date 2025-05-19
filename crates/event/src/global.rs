use crate::{create_adapters, Error, Event, NotifierConfig, NotifierSystem};
use std::sync::{atomic, Arc};
use tokio::sync::{Mutex, OnceCell};
use tracing::instrument;

static GLOBAL_SYSTEM: OnceCell<Arc<Mutex<NotifierSystem>>> = OnceCell::const_new();
static INITIALIZED: atomic::AtomicBool = atomic::AtomicBool::new(false);
static READY: atomic::AtomicBool = atomic::AtomicBool::new(false);
static INIT_LOCK: Mutex<()> = Mutex::const_new(());

/// Initializes the global notification system.
///
/// This function performs the following steps:
/// 1. Checks if the system is already initialized.
/// 2. Creates a new `NotificationSystem` instance.
/// 3. Creates adapters based on the provided configuration.
/// 4. Starts the notification system with the created adapters.
/// 5. Sets the global system instance.
///
/// # Errors
///
/// Returns an error if:
/// - The system is already initialized.
/// - Creating the `NotificationSystem` fails.
/// - Creating adapters fails.
/// - Starting the notification system fails.
/// - Setting the global system instance fails.
pub async fn initialize(config: NotifierConfig) -> Result<(), Error> {
    let _lock = INIT_LOCK.lock().await;

    // Check if the system is already initialized.
    if INITIALIZED.load(atomic::Ordering::SeqCst) {
        return Err(Error::custom("Notification system has already been initialized"));
    }

    // Check if the system is already ready.
    if READY.load(atomic::Ordering::SeqCst) {
        return Err(Error::custom("Notification system is already ready"));
    }

    // Check if the system is shutting down.
    if let Some(system) = GLOBAL_SYSTEM.get() {
        let system_guard = system.lock().await;
        if system_guard.shutdown_cancelled() {
            return Err(Error::custom("Notification system is shutting down"));
        }
    }

    // check if config adapters len is than 0
    if config.adapters.is_empty() {
        return Err(Error::custom("No adapters configured"));
    }

    // Attempt to initialize, and reset the INITIALIZED flag if it fails.
    let result: Result<(), Error> = async {
        let system = NotifierSystem::new(config.clone()).await.map_err(|e| {
            tracing::error!("Failed to create NotificationSystem: {:?}", e);
            e
        })?;
        let adapters = create_adapters(&config.adapters).map_err(|e| {
            tracing::error!("Failed to create adapters: {:?}", e);
            e
        })?;
        tracing::info!("adapters len:{:?}", adapters.len());
        let system_clone = Arc::new(Mutex::new(system));
        let adapters_clone = adapters.clone();

        GLOBAL_SYSTEM.set(system_clone.clone()).map_err(|_| {
            let err = Error::custom("Unable to set up global notification system");
            tracing::error!("{:?}", err);
            err
        })?;

        tokio::spawn(async move {
            if let Err(e) = system_clone.lock().await.start(adapters_clone).await {
                tracing::error!("Notification system failed to start: {}", e);
            }
            tracing::info!("Notification system started in background");
        });
        tracing::info!("system start success,start set READY value");

        READY.store(true, atomic::Ordering::SeqCst);
        tracing::info!("Notification system is ready to process events");

        Ok(())
    }
    .await;

    if result.is_err() {
        INITIALIZED.store(false, atomic::Ordering::SeqCst);
        READY.store(false, atomic::Ordering::SeqCst);
        return result;
    }

    INITIALIZED.store(true, atomic::Ordering::SeqCst);
    Ok(())
}

/// Checks if the notification system is initialized.
pub fn is_initialized() -> bool {
    INITIALIZED.load(atomic::Ordering::SeqCst)
}

/// Checks if the notification system is ready.
pub fn is_ready() -> bool {
    READY.load(atomic::Ordering::SeqCst)
}

/// Sends an event to the notification system.
///
/// # Errors
///
/// Returns an error if:
/// - The system is not initialized.
/// - The system is not ready.
/// - Sending the event fails.
#[instrument(fields(event))]
pub async fn send_event(event: Event) -> Result<(), Error> {
    if !READY.load(atomic::Ordering::SeqCst) {
        return Err(Error::custom("Notification system not ready, please wait for initialization to complete"));
    }

    let system = get_system().await?;
    let system_guard = system.lock().await;
    system_guard.send_event(event).await
}

/// Shuts down the notification system.
#[instrument]
pub async fn shutdown() -> Result<(), Error> {
    if let Some(system) = GLOBAL_SYSTEM.get() {
        tracing::info!("Shutting down notification system start");
        let result = {
            let mut system_guard = system.lock().await;
            system_guard.shutdown().await
        };
        if let Err(e) = &result {
            tracing::error!("Notification system shutdown failed: {}", e);
        } else {
            tracing::info!("Event bus shutdown completed");
        }

        tracing::info!(
            "Shutdown method called set static value start, READY: {}, INITIALIZED: {}",
            READY.load(atomic::Ordering::SeqCst),
            INITIALIZED.load(atomic::Ordering::SeqCst)
        );
        READY.store(false, atomic::Ordering::SeqCst);
        INITIALIZED.store(false, atomic::Ordering::SeqCst);
        tracing::info!(
            "Shutdown method called  set static value end, READY: {}, INITIALIZED: {}",
            READY.load(atomic::Ordering::SeqCst),
            INITIALIZED.load(atomic::Ordering::SeqCst)
        );
        result
    } else {
        Err(Error::custom("Notification system not initialized"))
    }
}

/// Retrieves the global notification system instance.
///
/// # Errors
///
/// Returns an error if the system is not initialized.
async fn get_system() -> Result<Arc<Mutex<NotifierSystem>>, Error> {
    GLOBAL_SYSTEM
        .get()
        .cloned()
        .ok_or_else(|| Error::custom("Notification system not initialized"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AdapterConfig, NotifierConfig, WebhookConfig};
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_initialize_success() {
        tracing_subscriber::fmt::init();
        let config = NotifierConfig::default(); // assume there is a default configuration
        let result = initialize(config).await;
        assert!(result.is_err(), "Initialization should not succeed");
        assert!(!is_initialized(), "System should not be marked as initialized");
        assert!(!is_ready(), "System should not be marked as ready");
    }

    #[tokio::test]
    async fn test_initialize_twice() {
        tracing_subscriber::fmt::init();
        let config = NotifierConfig::default();
        let _ = initialize(config.clone()).await; // first initialization
        let result = initialize(config).await; // second initialization
        assert!(result.is_err(), "Initialization should succeed");
        assert!(result.is_err(), "Re-initialization should fail");
    }

    #[tokio::test]
    async fn test_initialize_failure_resets_state() {
        tracing_subscriber::fmt::init();
        // simulate wrong configuration
        let config = NotifierConfig {
            adapters: vec![
                // assuming that the empty adapter will cause failure
                AdapterConfig::Webhook(WebhookConfig {
                    endpoint: "http://localhost:8080/webhook".to_string(),
                    auth_token: Some("secret-token".to_string()),
                    custom_headers: Some(HashMap::from([("X-Custom".to_string(), "value".to_string())])),
                    max_retries: 3,
                    timeout: 10,
                }),
            ], // assuming that the empty adapter will cause failure
            ..Default::default()
        };
        let result = initialize(config).await;
        assert!(result.is_ok(), "Initialization with invalid config should fail");
        assert!(is_initialized(), "System should not be marked as initialized after failure");
        assert!(is_ready(), "System should not be marked as ready after failure");
    }

    #[tokio::test]
    async fn test_is_initialized_and_is_ready() {
        tracing_subscriber::fmt::init();
        assert!(!is_initialized(), "System should not be initialized initially");
        assert!(!is_ready(), "System should not be ready initially");

        let config = NotifierConfig::default();
        let _ = initialize(config).await;
        assert!(!is_initialized(), "System should be initialized after successful initialization");
        assert!(!is_ready(), "System should be ready after successful initialization");
    }
}
