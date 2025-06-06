use rustfs_notify::EventNotifierConfig;
use tracing::{info, instrument};

#[instrument]
pub(crate) async fn init_event_notifier(notifier_config: Option<String>) {
    info!("Initializing event notifier...");
    let notifier_config_present = notifier_config.is_some();
    let config = if notifier_config_present {
        info!("event_config is not empty, path: {:?}", notifier_config);
        EventNotifierConfig::event_load_config(notifier_config)
    } else {
        info!("event_config is empty");
        // rustfs_notify::get_event_notifier_config().clone()
        EventNotifierConfig::default()
    };

    info!("using event_config: {:?}", config);
    tokio::spawn(async move {
        // let result = rustfs_notify::initialize(&config).await;
        // match result {
        //     Ok(_) => info!(
        //         "event notifier initialized successfully {}",
        //         if notifier_config_present {
        //             "by config file"
        //         } else {
        //             "by sys config"
        //         }
        //     ),
        //     Err(e) => error!("Failed to initialize event notifier: {}", e),
        // }
    });
}
