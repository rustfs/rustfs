#[allow(dead_code)]
const NOTIFY_KAFKA_SUB_SYS: &str = "notify_kafka";
#[allow(dead_code)]
const NOTIFY_MQTT_SUB_SYS: &str = "notify_mqtt";
#[allow(dead_code)]
const NOTIFY_MY_SQL_SUB_SYS: &str = "notify_mysql";
#[allow(dead_code)]
const NOTIFY_NATS_SUB_SYS: &str = "notify_nats";
#[allow(dead_code)]
const NOTIFY_NSQ_SUB_SYS: &str = "notify_nsq";
#[allow(dead_code)]
const NOTIFY_ES_SUB_SYS: &str = "notify_elasticsearch";
#[allow(dead_code)]
const NOTIFY_AMQP_SUB_SYS: &str = "notify_amqp";
#[allow(dead_code)]
const NOTIFY_POSTGRES_SUB_SYS: &str = "notify_postgres";
#[allow(dead_code)]
const NOTIFY_REDIS_SUB_SYS: &str = "notify_redis";
const NOTIFY_WEBHOOK_SUB_SYS: &str = "notify_webhook";

// Webhook constants
pub const WEBHOOK_ENDPOINT: &str = "endpoint";
pub const WEBHOOK_AUTH_TOKEN: &str = "auth_token";
pub const WEBHOOK_QUEUE_DIR: &str = "queue_dir";
pub const WEBHOOK_QUEUE_LIMIT: &str = "queue_limit";
pub const WEBHOOK_CLIENT_CERT: &str = "client_cert";
pub const WEBHOOK_CLIENT_KEY: &str = "client_key";

pub const ENV_WEBHOOK_ENABLE: &str = "RUSTFS_NOTIFY_WEBHOOK_ENABLE";
pub const ENV_WEBHOOK_ENDPOINT: &str = "RUSTFS_NOTIFY_WEBHOOK_ENDPOINT";
pub const ENV_WEBHOOK_AUTH_TOKEN: &str = "RUSTFS_NOTIFY_WEBHOOK_AUTH_TOKEN";
pub const ENV_WEBHOOK_QUEUE_DIR: &str = "RUSTFS_NOTIFY_WEBHOOK_QUEUE_DIR";
pub const ENV_WEBHOOK_QUEUE_LIMIT: &str = "RUSTFS_NOTIFY_WEBHOOK_QUEUE_LIMIT";
pub const ENV_WEBHOOK_CLIENT_CERT: &str = "RUSTFS_NOTIFY_WEBHOOK_CLIENT_CERT";
pub const ENV_WEBHOOK_CLIENT_KEY: &str = "RUSTFS_NOTIFY_WEBHOOK_CLIENT_KEY";
