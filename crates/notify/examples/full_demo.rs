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

    let system = match notification_system() {
        Some(sys) => sys,
        None => {
            let config = Config::new();
            initialize(config).await?;
            notification_system().expect("Failed to initialize notification system")
        }
    };

    // --- Initial configuration (Webhook and MQTT) ---
    let mut config = Config::new();
    let current_root = rustfs_utils::dirs::get_project_root().expect("failed to get project root");
    println!("Current project root: {}", current_root.display());

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

    // MQTT target configuration
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
    let mut mqtt_targets = std::collections::HashMap::new();
    mqtt_targets.insert(DEFAULT_TARGET.to_string(), mqtt_kvs);
    config.0.insert(NOTIFY_MQTT_SUB_SYS.to_string(), mqtt_targets);

    // Load the configuration and initialize the system
    *system.config.write().await = config;
    info!("---> Initializing notification system with Webhook and MQTT targets...");
    info!("Webhook Endpoint: {}", WEBHOOK_ENDPOINT);
    info!("MQTT Broker: {}", MQTT_BROKER);
    info!("system.init config: {:?}", system.config.read().await);
    system.init().await?;
    info!("✅ System initialized with Webhook and MQTT targets.");

    // --- Query the currently active Target ---
    let active_targets = system.get_active_targets().await;
    info!("\n---> Currently active targets: {:?}", active_targets);
    assert_eq!(active_targets.len(), 2);

    tokio::time::sleep(Duration::from_secs(1)).await;

    // --- Exactly delete a Target (e.g. MQTT) ---
    info!("\n---> Removing MQTT target...");
    let mqtt_target_id = TargetID::new(DEFAULT_TARGET.to_string(), "mqtt".to_string());
    system.remove_target(&mqtt_target_id, NOTIFY_MQTT_SUB_SYS).await?;
    info!("✅ MQTT target removed.");

    // --- Query the activity's Target again ---
    let active_targets_after_removal = system.get_active_targets().await;
    info!("\n---> Active targets after removal: {:?}", active_targets_after_removal);
    assert_eq!(active_targets_after_removal.len(), 1);
    assert_eq!(active_targets_after_removal[0].id, DEFAULT_TARGET.to_string());

    // --- Send events for verification ---
    // Configure a rule to point to the Webhook and deleted MQTT
    let mut bucket_config = BucketNotificationConfig::new("us-east-1");
    bucket_config.add_rule(
        &[EventName::ObjectCreatedPut],
        "*".to_string(),
        TargetID::new(DEFAULT_TARGET.to_string(), "webhook".to_string()),
    );
    bucket_config.add_rule(
        &[EventName::ObjectCreatedPut],
        "*".to_string(),
        TargetID::new(DEFAULT_TARGET.to_string(), "mqtt".to_string()), // This rule will match, but the Target cannot be found
    );
    system.load_bucket_notification_config("my-bucket", &bucket_config).await?;

    info!("\n---> Sending an event...");
    let event = Arc::new(Event::new_test_event("my-bucket", "document.pdf", EventName::ObjectCreatedPut));
    system.send_event(event).await;
    info!("✅ Event sent. Only the Webhook target should receive it. Check logs for warnings about the missing MQTT target.");

    tokio::time::sleep(Duration::from_secs(2)).await;

    info!("\nDemo completed successfully");
    Ok(())
}
