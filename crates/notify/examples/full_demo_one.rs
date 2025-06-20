// Using Global Accessories
use rustfs_config::notify;
use rustfs_notify::arn::TargetID;
use rustfs_notify::global::notification_system;
use rustfs_notify::{init_logger, BucketNotificationConfig, Event, EventName, LogLevel, NotificationError, KVS};
use std::time::Duration;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), NotificationError> {
    init_logger(LogLevel::Debug);

    // Get global NotificationSystem instance
    let system = notification_system();

    // --- Initial configuration ---
    let mut config = rustfs_notify::Config::new();
    let current_root = rustfs_utils::dirs::get_project_root().expect("failed to get project root");
    // Webhook target
    let mut webhook_kvs = KVS::new();
    webhook_kvs.set("enable", "on");
    webhook_kvs.set("endpoint", "http://127.0.0.1:3020/webhook");
    // webhook_kvs.set("queue_dir", "./logs/webhook");
    webhook_kvs.set(
        "queue_dir",
        current_root
            .clone()
            .join("/deploy/logs/notify/webhook")
            .to_str()
            .unwrap()
            .to_string(),
    );
    let mut webhook_targets = std::collections::HashMap::new();
    webhook_targets.insert("1".to_string(), webhook_kvs);
    config.insert("notify_webhook".to_string(), webhook_targets);

    // Load the initial configuration and initialize the system
    *system.config.write().await = config;
    system.init().await?;
    info!("✅ System initialized with Webhook target.");

    tokio::time::sleep(Duration::from_secs(1)).await;

    // --- Dynamically update system configuration: Add an MQTT Target ---
    info!("\n---> Dynamically adding MQTT target...");
    let mut mqtt_kvs = KVS::new();
    mqtt_kvs.set("enable", "on");
    mqtt_kvs.set("broker", "mqtt://localhost:1883");
    mqtt_kvs.set("topic", "rustfs/events");
    mqtt_kvs.set("qos", "1");
    mqtt_kvs.set("username", "test");
    mqtt_kvs.set("password", "123456");
    mqtt_kvs.set("queue_limit", "10000");
    // mqtt_kvs.set("queue_dir", "./logs/mqtt");
    mqtt_kvs.set("queue_dir", current_root.join("/deploy/logs/notify/mqtt").to_str().unwrap().to_string());
    system.set_target_config("notify_mqtt", "1", mqtt_kvs).await?;
    info!("✅ MQTT target added and system reloaded.");

    tokio::time::sleep(Duration::from_secs(1)).await;

    // --- Loading and managing Bucket configurations ---
    info!("\n---> Loading bucket notification config...");
    let mut bucket_config = BucketNotificationConfig::new("us-east-1");
    bucket_config.add_rule(
        &[EventName::ObjectCreatedPut],
        "*".to_string(),
        TargetID::new("1".to_string(), "webhook".to_string()),
    );
    bucket_config.add_rule(
        &[EventName::ObjectCreatedPut],
        "*".to_string(),
        TargetID::new("1".to_string(), "mqtt".to_string()),
    );
    system.load_bucket_notification_config("my-bucket", &bucket_config).await?;
    info!("✅ Bucket 'my-bucket' config loaded.");

    // --- Send events ---
    info!("\n---> Sending an event...");
    let event = Event::new_test_event("my-bucket", "document.pdf", EventName::ObjectCreatedPut);
    system
        .send_event("my-bucket", "s3:ObjectCreated:Put", "document.pdf", event)
        .await;
    info!("✅ Event sent. Both Webhook and MQTT targets should receive it.");

    tokio::time::sleep(Duration::from_secs(2)).await;

    // --- Dynamically remove configuration ---
    info!("\n---> Dynamically removing Webhook target...");
    system.remove_target_config("notify_webhook", "1").await?;
    info!("✅ Webhook target removed and system reloaded.");

    info!("\n---> Removing bucket notification config...");
    system.remove_bucket_notification_config("my-bucket").await;
    info!("✅ Bucket 'my-bucket' config removed.");

    info!("\nDemo completed successfully");
    Ok(())
}
