use crate::NotificationSystem;
use once_cell::sync::Lazy;
use std::sync::Arc;

static NOTIFICATION_SYSTEM: Lazy<Arc<NotificationSystem>> =
    Lazy::new(|| Arc::new(NotificationSystem::new()));

/// Returns the handle to the global NotificationSystem instance.
/// This function can be called anywhere you need to interact with the notification system。
pub fn notification_system() -> Arc<NotificationSystem> {
    NOTIFICATION_SYSTEM.clone()
}
