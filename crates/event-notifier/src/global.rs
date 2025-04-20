use crate::{ChannelAdapter, Error, Event, NotificationConfig, NotificationSystem};
use std::sync::Arc;
use tokio::sync::{Mutex, OnceCell};

static GLOBAL_SYSTEM: OnceCell<Arc<Mutex<NotificationSystem>>> = OnceCell::const_new();
static INITIALIZED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// initialize the global notification system
pub async fn initialize(config: NotificationConfig) -> Result<(), Error> {
    if INITIALIZED.swap(true, std::sync::atomic::Ordering::SeqCst) {
        return Err(Error::custom("notify the system has been initialized"));
    }

    let system = Arc::new(Mutex::new(NotificationSystem::new(config).await?));
    GLOBAL_SYSTEM
        .set(system)
        .map_err(|_| Error::custom("unable to set up global notification system"))?;
    Ok(())
}

/// start the global notification system
pub async fn start(adapters: Vec<Arc<dyn ChannelAdapter>>) -> Result<(), Error> {
    let system = get_system().await?;

    // create a new task to run the system
    let system_clone = Arc::clone(&system);
    tokio::spawn(async move {
        let mut system_guard = system_clone.lock().await;
        if let Err(e) = system_guard.start(adapters).await {
            tracing::error!("notify the system to start failed: {}", e);
        }
    });

    Ok(())
}

/// Initialize and start the global notification system
///
/// This method combines the functions of `initialize` and `start` to provide one-step setup:
/// - initialize system configuration
/// - create an adapter
/// - start event listening
///
/// # Example
///
/// ```rust
///  use rustfs_event_notifier::{initialize_and_start, NotificationConfig};
///
/// #[tokio::main]
/// async fn main() -> Result<(), rustfs_event_notifier::Error> {
/// let config = NotificationConfig {
///     store_path: "./events".to_string(),
///     channel_capacity: 100,
///     adapters: vec![/* 适配器配置 */],
///     http: Default::default(),
/// };
///
/// // complete initialization and startup in one step
/// initialize_and_start(config).await?;
/// Ok(())
/// }
/// ```
pub async fn initialize_and_start(config: NotificationConfig) -> Result<(), Error> {
    // initialize the system first
    initialize(config.clone()).await?;

    // create an adapter
    let adapters = crate::create_adapters(&config.adapters).expect("failed to create adapters");

    // start the system
    start(adapters).await?;

    Ok(())
}

/// send events to notification system
pub async fn send_event(event: Event) -> Result<(), Error> {
    let system = get_system().await?;
    let system_guard = system.lock().await;
    system_guard.send_event(event).await
}

/// turn off the notification system
pub fn shutdown() -> Result<(), Error> {
    if let Some(system) = GLOBAL_SYSTEM.get() {
        let system_guard = system.blocking_lock();
        system_guard.shutdown();
        Ok(())
    } else {
        Err(Error::custom("notification system not initialized"))
    }
}

/// get system instance
async fn get_system() -> Result<Arc<Mutex<NotificationSystem>>, Error> {
    GLOBAL_SYSTEM
        .get()
        .cloned()
        .ok_or_else(|| Error::custom("notification system not initialized"))
}
