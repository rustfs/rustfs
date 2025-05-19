use rustfs_event::NotifierConfig;
use tracing::{error, info, instrument};

#[instrument]
pub(crate) async fn init_event_notifier(notifier_config: Option<String>) {
    // Initialize event notifier
    if notifier_config.is_some() {
        info!("event_config is not empty");
        tokio::spawn(async move {
            let config = NotifierConfig::event_load_config(notifier_config);
            let result = rustfs_event::initialize(config).await;
            if let Err(e) = result {
                error!("Failed to initialize event notifier: {}", e);
            } else {
                info!("Event notifier initialized successfully");
            }
        });
    } else {
        info!("event_config is empty");
    }
}
