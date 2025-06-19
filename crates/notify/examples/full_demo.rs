use notify::arn::TargetID;
use notify::global::notification_system;
use notify::{
    init_logger, BucketNotificationConfig, Event, EventName, LogLevel, NotificationError,
};
use std::time::Duration;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), NotificationError> {
    init_logger(LogLevel::Debug);

    let system = notification_system();

    // --- 初始配置 (Webhook 和 MQTT) ---
    let mut config = notify::Config::new();

    // Webhook target configuration
    let mut webhook_kvs = notify::KVS::new();
    webhook_kvs.set("enable", "on");
    webhook_kvs.set("endpoint", "http://127.0.0.1:3020/webhook");
    webhook_kvs.set("auth_token", "secret-token");
    // webhook_kvs.set("queue_dir", "/tmp/data/webhook");
    webhook_kvs.set(
        "queue_dir",
        "/Users/qun/Documents/rust/rustfs/notify/logs/webhook",
    );
    webhook_kvs.set("queue_limit", "10000");
    let mut webhook_targets = std::collections::HashMap::new();
    webhook_targets.insert("1".to_string(), webhook_kvs);
    config.insert("notify_webhook".to_string(), webhook_targets);

    // MQTT target configuration
    let mut mqtt_kvs = notify::KVS::new();
    mqtt_kvs.set("enable", "on");
    mqtt_kvs.set("broker", "mqtt://localhost:1883");
    mqtt_kvs.set("topic", "rustfs/events");
    mqtt_kvs.set("qos", "1"); // AtLeastOnce
    mqtt_kvs.set("username", "test");
    mqtt_kvs.set("password", "123456");
    // webhook_kvs.set("queue_dir", "/tmp/data/mqtt");
    mqtt_kvs.set(
        "queue_dir",
        "/Users/qun/Documents/rust/rustfs/notify/logs/mqtt",
    );
    mqtt_kvs.set("queue_limit", "10000");

    let mut mqtt_targets = std::collections::HashMap::new();
    mqtt_targets.insert("1".to_string(), mqtt_kvs);
    config.insert("notify_mqtt".to_string(), mqtt_targets);

    // 加载配置并初始化系统
    *system.config.write().await = config;
    system.init().await?;
    info!("✅ System initialized with Webhook and MQTT targets.");

    // --- 1. 查询当前活动的 Target ---
    let active_targets = system.get_active_targets().await;
    info!("\n---> Currently active targets: {:?}", active_targets);
    assert_eq!(active_targets.len(), 2);

    tokio::time::sleep(Duration::from_secs(1)).await;

    // --- 2. 精确删除一个 Target (例如 MQTT) ---
    info!("\n---> Removing MQTT target...");
    let mqtt_target_id = TargetID::new("1".to_string(), "mqtt".to_string());
    system.remove_target(&mqtt_target_id, "notify_mqtt").await?;
    info!("✅ MQTT target removed.");

    // --- 3. 再次查询活动的 Target ---
    let active_targets_after_removal = system.get_active_targets().await;
    info!(
        "\n---> Active targets after removal: {:?}",
        active_targets_after_removal
    );
    assert_eq!(active_targets_after_removal.len(), 1);
    assert_eq!(active_targets_after_removal[0].id, "1".to_string());

    // --- 4. 发送事件进行验证 ---
    // 配置一个规则，指向 Webhook 和已删除的 MQTT
    let mut bucket_config = BucketNotificationConfig::new("us-east-1");
    bucket_config.add_rule(
        &[EventName::ObjectCreatedPut],
        "*".to_string(),
        TargetID::new("1".to_string(), "webhook".to_string()),
    );
    bucket_config.add_rule(
        &[EventName::ObjectCreatedPut],
        "*".to_string(),
        TargetID::new("1".to_string(), "mqtt".to_string()), // 这个规则会匹配，但找不到 Target
    );
    system
        .load_bucket_notification_config("my-bucket", &bucket_config)
        .await?;

    info!("\n---> Sending an event...");
    let event = Event::new_test_event("my-bucket", "document.pdf", EventName::ObjectCreatedPut);
    system
        .send_event("my-bucket", "s3:ObjectCreated:Put", "document.pdf", event)
        .await;
    info!(
        "✅ Event sent. Only the Webhook target should receive it. Check logs for warnings about the missing MQTT target."
    );

    tokio::time::sleep(Duration::from_secs(2)).await;

    info!("\nDemo completed successfully");
    Ok(())
}
