use crate::config::{ENABLE_KEY, ENABLE_OFF, KV, KVS};
use lazy_static::lazy_static;
use rustfs_config::notify::{
    DEFAULT_DIR, DEFAULT_LIMIT, MQTT_BROKER, MQTT_KEEP_ALIVE_INTERVAL, MQTT_PASSWORD, MQTT_QOS, MQTT_QUEUE_DIR, MQTT_QUEUE_LIMIT,
    MQTT_RECONNECT_INTERVAL, MQTT_TOPIC, MQTT_USERNAME, WEBHOOK_AUTH_TOKEN, WEBHOOK_CLIENT_CERT, WEBHOOK_CLIENT_KEY,
    WEBHOOK_ENDPOINT, WEBHOOK_QUEUE_DIR, WEBHOOK_QUEUE_LIMIT,
};

lazy_static! {
    /// The default configuration collection of webhooks，
    /// Use lazy_static! to ensure that these configurations are initialized only once during the program life cycle, enabling high-performance lazy loading.
    pub static ref DefaultWebhookKVS: KVS = KVS(vec![
        KV { key: ENABLE_KEY.to_owned(), value: ENABLE_OFF.to_owned(), hidden_if_empty: false },
        KV { key: WEBHOOK_ENDPOINT.to_owned(), value: "".to_owned(), hidden_if_empty: false },
        // Sensitive information such as authentication tokens is hidden when the value is empty, enhancing security
        KV { key: WEBHOOK_AUTH_TOKEN.to_owned(), value: "".to_owned(), hidden_if_empty: true },
        KV { key: WEBHOOK_QUEUE_LIMIT.to_owned(), value: DEFAULT_LIMIT.to_string().to_owned(), hidden_if_empty: false },
        KV { key: WEBHOOK_QUEUE_DIR.to_owned(), value: DEFAULT_DIR.to_owned(), hidden_if_empty: false },
        KV { key: WEBHOOK_CLIENT_CERT.to_owned(), value: "".to_owned(), hidden_if_empty: false },
        KV { key: WEBHOOK_CLIENT_KEY.to_owned(), value: "".to_owned(), hidden_if_empty: false },
    ]);

    /// MQTT's default configuration collection
    pub static ref DefaultMqttKVS: KVS = KVS(vec![
        KV { key: ENABLE_KEY.to_owned(), value: ENABLE_OFF.to_owned(), hidden_if_empty: false },
        KV { key: MQTT_BROKER.to_owned(), value: "".to_owned(), hidden_if_empty: false },
        KV { key: MQTT_TOPIC.to_owned(), value: "".to_owned(), hidden_if_empty: false },
        // Sensitive information such as passwords are hidden when the value is empty
        KV { key: MQTT_PASSWORD.to_owned(), value: "".to_owned(), hidden_if_empty: true },
        KV { key: MQTT_USERNAME.to_owned(), value: "".to_owned(), hidden_if_empty: false },
        KV { key: MQTT_QOS.to_owned(), value: "0".to_owned(), hidden_if_empty: false },
        KV { key: MQTT_KEEP_ALIVE_INTERVAL.to_owned(), value: "0s".to_owned(), hidden_if_empty: false },
        KV { key: MQTT_RECONNECT_INTERVAL.to_owned(), value: "0s".to_owned(), hidden_if_empty: false },
        KV { key: MQTT_QUEUE_DIR.to_owned(), value: DEFAULT_DIR.to_owned(), hidden_if_empty: false },
        KV { key: MQTT_QUEUE_LIMIT.to_owned(), value: DEFAULT_LIMIT.to_string().to_owned(), hidden_if_empty: false },
    ]);
}
