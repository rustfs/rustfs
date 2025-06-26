mod arn;
mod mqtt;
mod store;
mod webhook;

pub use arn::*;
pub use mqtt::*;
pub use store::*;
pub use webhook::*;

// --- Configuration Constants ---
pub const DEFAULT_TARGET: &str = "1";

pub const NOTIFY_PREFIX: &str = "notify";

pub const NOTIFY_ROUTE_PREFIX: &str = "notify_";

#[allow(dead_code)]
pub const NOTIFY_SUB_SYSTEMS: &[&str] = &[NOTIFY_MQTT_SUB_SYS, NOTIFY_WEBHOOK_SUB_SYS];

#[allow(dead_code)]
pub const NOTIFY_KAFKA_SUB_SYS: &str = "notify_kafka";
pub const NOTIFY_MQTT_SUB_SYS: &str = "notify_mqtt";
#[allow(dead_code)]
pub const NOTIFY_MY_SQL_SUB_SYS: &str = "notify_mysql";
#[allow(dead_code)]
pub const NOTIFY_NATS_SUB_SYS: &str = "notify_nats";
#[allow(dead_code)]
pub const NOTIFY_NSQ_SUB_SYS: &str = "notify_nsq";
#[allow(dead_code)]
pub const NOTIFY_ES_SUB_SYS: &str = "notify_elasticsearch";
#[allow(dead_code)]
pub const NOTIFY_AMQP_SUB_SYS: &str = "notify_amqp";
#[allow(dead_code)]
pub const NOTIFY_POSTGRES_SUB_SYS: &str = "notify_postgres";
#[allow(dead_code)]
pub const NOTIFY_REDIS_SUB_SYS: &str = "notify_redis";
pub const NOTIFY_WEBHOOK_SUB_SYS: &str = "notify_webhook";
