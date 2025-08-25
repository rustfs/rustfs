// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod base;

use base::{LogLevel, init_logger};
use rustfs_config::EnableState::On;
use rustfs_config::notify::{
    DEFAULT_TARGET, MQTT_BROKER, MQTT_PASSWORD, MQTT_QOS, MQTT_QUEUE_DIR, MQTT_QUEUE_LIMIT, MQTT_TOPIC, MQTT_USERNAME,
    NOTIFY_MQTT_SUB_SYS, NOTIFY_WEBHOOK_SUB_SYS, WEBHOOK_AUTH_TOKEN, WEBHOOK_ENDPOINT, WEBHOOK_QUEUE_DIR, WEBHOOK_QUEUE_LIMIT,
};
use rustfs_config::{DEFAULT_LIMIT, ENABLE_KEY};
use rustfs_ecstore::config::{Config, KV, KVS};
use rustfs_notify::{BucketNotificationConfig, Event, NotificationError};
use rustfs_notify::{initialize, notification_system};
use rustfs_targets::EventName;
use rustfs_targets::arn::TargetID;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), NotificationError> {
    init_logger(LogLevel::Debug);

    // Get global NotificationSystem instance
    let system = match notification_system() {
        Some(sys) => sys,
        None => {
            let config = Config::new();
            initialize(config).await?;
            notification_system().expect("Failed to initialize notification system")
        }
    };

    // --- Initial configuration ---
    let mut config = Config::new();
    let current_root = rustfs_utils::dirs::get_project_root().expect("failed to get project root");
    // Webhook target
    let webhook_kvs_vec = vec![
        KV {
            key: ENABLE_KEY.to_string(),
            value: On.to_string(),
            hidden_if_empty: false,
        },
        KV {
            key: WEBHOOK_ENDPOINT.to_string(),
            value: "http://127.0.0.1:3020/webhook".to_string(),
            hidden_if_empty: false,
        },
        KV {
            key: WEBHOOK_AUTH_TOKEN.to_string(),
            value: "secret-token".to_string(),
            hidden_if_empty: false,
        },
        KV {
            key: WEBHOOK_QUEUE_DIR.to_string(),
            value: current_root
                .clone()
                .join("../../deploy/logs/notify/webhook")
                .to_str()
                .unwrap()
                .to_string(),
            hidden_if_empty: false,
        },
        KV {
            key: WEBHOOK_QUEUE_LIMIT.to_string(),
            value: DEFAULT_LIMIT.to_string(),
            hidden_if_empty: false,
        },
    ];
    let webhook_kvs = KVS(webhook_kvs_vec);

    let mut webhook_targets = std::collections::HashMap::new();
    webhook_targets.insert(DEFAULT_TARGET.to_string(), webhook_kvs);
    config.0.insert(NOTIFY_WEBHOOK_SUB_SYS.to_string(), webhook_targets);

    // Load the initial configuration and initialize the system
    *system.config.write().await = config;
    system.init().await?;
    info!("✅ System initialized with Webhook target.");

    tokio::time::sleep(Duration::from_secs(1)).await;

    // --- Dynamically update system configuration: Add an MQTT Target ---
    info!("\n---> Dynamically adding MQTT target...");

    let mqtt_kvs_vec = vec![
        KV {
            key: ENABLE_KEY.to_string(),
            value: On.to_string(),
            hidden_if_empty: false,
        },
        KV {
            key: MQTT_BROKER.to_string(),
            value: "mqtt://localhost:1883".to_string(),
            hidden_if_empty: false,
        },
        KV {
            key: MQTT_TOPIC.to_string(),
            value: "rustfs/events".to_string(),
            hidden_if_empty: false,
        },
        KV {
            key: MQTT_QOS.to_string(),
            value: "1".to_string(), // AtLeastOnce
            hidden_if_empty: false,
        },
        KV {
            key: MQTT_USERNAME.to_string(),
            value: "test".to_string(),
            hidden_if_empty: false,
        },
        KV {
            key: MQTT_PASSWORD.to_string(),
            value: "123456".to_string(),
            hidden_if_empty: false,
        },
        KV {
            key: MQTT_QUEUE_DIR.to_string(),
            value: current_root
                .join("../../deploy/logs/notify/mqtt")
                .to_str()
                .unwrap()
                .to_string(),
            hidden_if_empty: false,
        },
        KV {
            key: MQTT_QUEUE_LIMIT.to_string(),
            value: DEFAULT_LIMIT.to_string(),
            hidden_if_empty: false,
        },
    ];

    let mqtt_kvs = KVS(mqtt_kvs_vec);
    // let mut mqtt_targets = std::collections::HashMap::new();
    // mqtt_targets.insert(DEFAULT_TARGET.to_string(), mqtt_kvs.clone());

    system
        .set_target_config(NOTIFY_MQTT_SUB_SYS, DEFAULT_TARGET, mqtt_kvs)
        .await?;
    info!("✅ MQTT target added and system reloaded.");

    tokio::time::sleep(Duration::from_secs(1)).await;

    // --- Loading and managing Bucket configurations ---
    info!("\n---> Loading bucket notification config...");
    let mut bucket_config = BucketNotificationConfig::new("us-east-1");
    bucket_config.add_rule(
        &[EventName::ObjectCreatedPut],
        "*".to_string(),
        TargetID::new(DEFAULT_TARGET.to_string(), "webhook".to_string()),
    );
    bucket_config.add_rule(
        &[EventName::ObjectCreatedPut],
        "*".to_string(),
        TargetID::new(DEFAULT_TARGET.to_string(), "mqtt".to_string()),
    );
    system.load_bucket_notification_config("my-bucket", &bucket_config).await?;
    info!("✅ Bucket 'my-bucket' config loaded.");

    // --- Send events ---
    info!("\n---> Sending an event...");
    let event = Arc::new(Event::new_test_event("my-bucket", "document.pdf", EventName::ObjectCreatedPut));
    system.send_event(event).await;
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
