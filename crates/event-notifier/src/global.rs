use crate::{ChannelAdapter, Error, Event, NotificationConfig, NotificationSystem};
use std::sync::{atomic, Arc};
use std::time;
use std::time::Duration;
use tokio::sync::{Mutex, OnceCell};

static GLOBAL_SYSTEM: OnceCell<Arc<Mutex<NotificationSystem>>> = OnceCell::const_new();
static INITIALIZED: atomic::AtomicBool = atomic::AtomicBool::new(false);
static READY: atomic::AtomicBool = atomic::AtomicBool::new(false);
static INIT_LOCK: Mutex<()> = Mutex::const_new(());

/// initialize the global notification system
pub async fn initialize(config: NotificationConfig) -> Result<(), Error> {
    // use lock to protect the initialization process
    let _lock = INIT_LOCK.lock().await;

    if INITIALIZED.swap(true, atomic::Ordering::SeqCst) {
        return Err(Error::custom("notify the system has been initialized"));
    }

    match Arc::new(Mutex::new(NotificationSystem::new(config).await?)) {
        system => {
            if let Err(_) = GLOBAL_SYSTEM.set(system.clone()) {
                INITIALIZED.store(false, atomic::Ordering::SeqCst);
                return Err(Error::custom("unable to set up global notification system"));
            }
            system
        }
    };
    Ok(())
}

/// securely initialize the global notification system
pub async fn initialize_safe(config: NotificationConfig) -> Result<(), Error> {
    // use-lock-to-protect-the-initialization-process
    let _lock = INIT_LOCK.lock().await;

    if INITIALIZED.load(atomic::Ordering::SeqCst) {
        return Err(Error::custom("notify the system has been initialized"));
    }

    // set-initialization-flag
    INITIALIZED.store(true, atomic::Ordering::SeqCst);

    match Arc::new(Mutex::new(NotificationSystem::new(config).await?)) {
        system => {
            if let Err(_) = GLOBAL_SYSTEM.set(system.clone()) {
                INITIALIZED.store(false, atomic::Ordering::SeqCst);
                return Err(Error::custom("unable to set up global notification system"));
            }
            system
        }
    };
    Ok(())
}

/// securely start the global notification system
pub async fn start_safe(adapters: Vec<Arc<dyn ChannelAdapter>>) -> Result<(), Error> {
    // start process with lock protection
    let _lock = INIT_LOCK.lock().await;

    if !INITIALIZED.load(atomic::Ordering::SeqCst) {
        return Err(Error::custom("notification system not initialized"));
    }

    if READY.load(atomic::Ordering::SeqCst) {
        return Err(Error::custom("notification system already started"));
    }

    let system = get_system().await?;

    // Execute startup operations directly on the current thread, rather than generating new tasks
    let mut system_guard = system.lock().await;
    match system_guard.start(adapters).await {
        Ok(_) => {
            READY.store(true, atomic::Ordering::SeqCst);
            tracing::info!("Notification system is ready to process events");
            Ok(())
        }
        Err(e) => {
            tracing::error!("Notify system start failed: {}", e);
            INITIALIZED.store(false, atomic::Ordering::SeqCst);
            Err(e)
        }
    }
}

/// start the global notification system
pub async fn start(adapters: Vec<Arc<dyn ChannelAdapter>>) -> Result<(), Error> {
    let system = get_system().await?;

    // create a new task to run the system
    let system_clone = Arc::clone(&system);
    tokio::spawn(async move {
        let mut system_guard = system_clone.lock().await;
        match system_guard.start(adapters).await {
            Ok(_) => {
                // The system is started and runs normally, set the ready flag
                READY.store(true, atomic::Ordering::SeqCst);
                tracing::info!("Notification system is ready to process events");
            }
            Err(e) => {
                tracing::error!("Notify system start failed: {}", e);
                INITIALIZED.store(false, atomic::Ordering::SeqCst);
            }
        }
    });

    // Wait for a while to ensure the system has a chance to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    Ok(())
}

/// waiting for notification system to be fully ready
async fn wait_until_ready(timeout: Duration) -> Result<(), Error> {
    let start = time::Instant::now();

    while !READY.load(atomic::Ordering::SeqCst) {
        if start.elapsed() > timeout {
            return Err(Error::custom("timeout waiting for notification system to become ready"));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

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
///     timeout: 0,
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

/// Initialize and start the global notification system and wait until it's ready
pub async fn initialize_and_start_with_ready_check(config: NotificationConfig, timeout: Duration) -> Result<(), Error> {
    // initialize the system
    initialize(config.clone()).await?;

    // create an adapter
    let adapters = crate::create_adapters(&config.adapters).expect("failed to create adapters");

    // start the system
    start(adapters).await?;

    // wait for the system to be ready
    wait_until_ready(timeout).await?;

    Ok(())
}

/// send events to notification system
pub async fn send_event(event: Event) -> Result<(), Error> {
    // check if the system is ready to receive events
    if !READY.load(atomic::Ordering::SeqCst) {
        return Err(Error::custom("notification system not ready, please wait for initialization to complete"));
    }
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
