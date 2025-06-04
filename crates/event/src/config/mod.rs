use std::env;

pub mod adapter;
pub mod kafka;
pub mod mqtt;
pub mod notifier;
pub mod webhook;

/// The default configuration file name
const DEFAULT_CONFIG_FILE: &str = "event";

/// The prefix for the configuration file
pub const STORE_PREFIX: &str = "rustfs";

/// The default retry interval for the webhook adapter
pub const DEFAULT_RETRY_INTERVAL: u64 = 3;

/// The default maximum retry count for the webhook adapter
pub const DEFAULT_MAX_RETRIES: u32 = 3;

/// The default notification queue limit
pub const DEFAULT_NOTIFY_QUEUE_LIMIT: u64 = 10000;

/// Provide temporary directories as default storage paths
pub(crate) fn default_queue_dir() -> String {
    env::var("EVENT_QUEUE_DIR").unwrap_or_else(|e| {
        tracing::info!("Failed to get `EVENT_QUEUE_DIR` failed err: {}", e.to_string());
        env::temp_dir().join(DEFAULT_CONFIG_FILE).to_string_lossy().to_string()
    })
}

/// Provides the recommended default channel capacity for high concurrency systems
pub(crate) fn default_queue_limit() -> u64 {
    env::var("EVENT_CHANNEL_CAPACITY")
        .unwrap_or_else(|_| DEFAULT_NOTIFY_QUEUE_LIMIT.to_string())
        .parse()
        .unwrap_or(DEFAULT_NOTIFY_QUEUE_LIMIT) // Default to 10000 if parsing fails
}
