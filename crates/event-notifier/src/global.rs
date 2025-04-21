use crate::{create_adapters, Error, Event, NotificationConfig, NotificationSystem};
use std::sync::{atomic, Arc};
use tokio::sync::{Mutex, OnceCell};

static GLOBAL_SYSTEM: OnceCell<Arc<Mutex<NotificationSystem>>> = OnceCell::const_new();
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
pub async fn initialize(config: NotificationConfig) -> Result<(), Error> {
    let _lock = INIT_LOCK.lock().await;

    if INITIALIZED.load(atomic::Ordering::SeqCst) {
        return Err(Error::custom("Notification system has already been initialized"));
    }

    // Attempt to initialize, and reset the INITIALIZED flag if it fails.
    let result: Result<(), Error> = async {
        let system = NotificationSystem::new(config.clone()).await?;
        let adapters = create_adapters(&config.adapters)?;
        tracing::info!("adapters len:{:?}", adapters.len());
        let system_clone = Arc::new(Mutex::new(system));
        let adapters_clone = adapters.clone();

        GLOBAL_SYSTEM
            .set(system_clone.clone())
            .map_err(|_| Error::custom("Unable to set up global notification system"))?;

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
pub async fn send_event(event: Event) -> Result<(), Error> {
    if !READY.load(atomic::Ordering::SeqCst) {
        return Err(Error::custom("Notification system not ready, please wait for initialization to complete"));
    }

    let system = get_system().await?;
    let system_guard = system.lock().await;
    system_guard.send_event(event).await
}

/// Shuts down the notification system.
pub fn shutdown() -> Result<(), Error> {
    if let Some(system) = GLOBAL_SYSTEM.get() {
        let system_guard = system.blocking_lock();
        system_guard.shutdown();
        Ok(())
    } else {
        Err(Error::custom("Notification system not initialized"))
    }
}

/// Retrieves the global notification system instance.
///
/// # Errors
///
/// Returns an error if the system is not initialized.
async fn get_system() -> Result<Arc<Mutex<NotificationSystem>>, Error> {
    GLOBAL_SYSTEM
        .get()
        .cloned()
        .ok_or_else(|| Error::custom("Notification system not initialized"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NotificationConfig;

    #[tokio::test]
    async fn test_initialize_success() {
        tracing_subscriber::fmt::init();
        let config = NotificationConfig::default(); // 假设有默认配置
        println!("config: {:?}", config);
        let result = initialize(config).await;
        assert!(result.is_ok(), "Initialization should succeed");
        assert!(is_initialized(), "System should be marked as initialized");
        assert!(is_ready(), "System should be marked as ready");
    }

    #[tokio::test]
    async fn test_initialize_twice() {
        tracing_subscriber::fmt::init();
        let config = NotificationConfig::default();
        println!("config: {:?}", config);
        let _ = initialize(config.clone()).await; // 第一次初始化
        let result = initialize(config).await; // 第二次初始化
        assert!(result.is_err(), "Re-initialization should fail");
    }

    #[tokio::test]
    async fn test_initialize_failure_resets_state() {
        tracing_subscriber::fmt::init();
        // 模拟错误配置
        let config = NotificationConfig {
            adapters: vec![], // 假设空适配器会导致失败
            ..Default::default()
        };

        let result = initialize(config).await;
        assert!(result.is_err(), "Initialization with invalid config should fail");
        assert!(!is_initialized(), "System should not be marked as initialized after failure");
        assert!(!is_ready(), "System should not be marked as ready after failure");
    }

    #[tokio::test]
    async fn test_is_initialized_and_is_ready() {
        tracing_subscriber::fmt::init();
        assert!(!is_initialized(), "System should not be initialized initially");
        assert!(!is_ready(), "System should not be ready initially");

        let config = NotificationConfig::default();
        let _ = initialize(config).await;

        assert!(is_initialized(), "System should be initialized after successful initialization");
        assert!(is_ready(), "System should be ready after successful initialization");
    }
}
