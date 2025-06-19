use notify::arn::TargetID;
use notify::global::notification_system;
// 1. 使用全局访问器
use notify::{
    init_logger, BucketNotificationConfig, Event, EventName, LogLevel, NotificationError, KVS,
};
use std::time::Duration;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), NotificationError> {
    init_logger(LogLevel::Debug);

    // 获取全局 NotificationSystem 实例
    let system = notification_system();

    // --- 初始配置 ---
    let mut config = notify::Config::new();
    // Webhook target
    let mut webhook_kvs = KVS::new();
    webhook_kvs.set("enable", "on");
    webhook_kvs.set("endpoint", "http://127.0.0.1:3020/webhook");
    // webhook_kvs.set("queue_dir", "./logs/webhook");
    webhook_kvs.set(
        "queue_dir",
        "/Users/qun/Documents/rust/rustfs/notify/logs/webhook",
    );
    let mut webhook_targets = std::collections::HashMap::new();
    webhook_targets.insert("1".to_string(), webhook_kvs);
    config.insert("notify_webhook".to_string(), webhook_targets);

    // 加载初始配置并初始化系统
    *system.config.write().await = config;
    system.init().await?;
    info!("✅ System initialized with Webhook target.");

    tokio::time::sleep(Duration::from_secs(1)).await;

    // --- 2. 动态更新系统配置：添加一个 MQTT Target ---
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
    mqtt_kvs.set(
        "queue_dir",
        "/Users/qun/Documents/rust/rustfs/notify/logs/mqtt",
    );
    system
        .set_target_config("notify_mqtt", "1", mqtt_kvs)
        .await?;
    info!("✅ MQTT target added and system reloaded.");

    tokio::time::sleep(Duration::from_secs(1)).await;

    // --- 3. 加载和管理 Bucket 配置 ---
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
    system
        .load_bucket_notification_config("my-bucket", &bucket_config)
        .await?;
    info!("✅ Bucket 'my-bucket' config loaded.");

    // --- 发送事件 ---
    info!("\n---> Sending an event...");
    let event = Event::new_test_event("my-bucket", "document.pdf", EventName::ObjectCreatedPut);
    system
        .send_event("my-bucket", "s3:ObjectCreated:Put", "document.pdf", event)
        .await;
    info!("✅ Event sent. Both Webhook and MQTT targets should receive it.");

    tokio::time::sleep(Duration::from_secs(2)).await;

    // --- 动态移除配置 ---
    info!("\n---> Dynamically removing Webhook target...");
    system.remove_target_config("notify_webhook", "1").await?;
    info!("✅ Webhook target removed and system reloaded.");

    info!("\n---> Removing bucket notification config...");
    system.remove_bucket_notification_config("my-bucket").await;
    info!("✅ Bucket 'my-bucket' config removed.");

    info!("\nDemo completed successfully");
    Ok(())
}
