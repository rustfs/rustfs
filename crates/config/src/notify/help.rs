/// Help text for Webhook configuration.
pub const HELP_WEBHOOK: &str = r#"
Webhook configuration:
- enable: Enable or disable the webhook target (true/false)
- endpoint: Webhook server endpoint (e.g., http://localhost:8080/rustfs/events)
- auth_token: Opaque string or JWT authorization token (optional)
- queue_dir: Absolute path for persistent event queue (optional)
- queue_limit: Maximum number of events to queue (optional, default: 0)
- client_cert: Path to client certificate file (optional)
- client_key: Path to client private key file (optional)
"#;

/// Help text for MQTT configuration.
pub const HELP_MQTT: &str = r#"
MQTT configuration:
- enable: Enable or disable the MQTT target (true/false)
- broker: MQTT broker address (e.g., tcp://localhost:1883)
- topic: MQTT topic (e.g., rustfs/events)
- qos: Quality of Service level (0, 1, or 2)
- username: Username for MQTT authentication (optional)
- password: Password for MQTT authentication (optional)
- reconnect_interval: Reconnect interval in milliseconds (optional)
- keep_alive_interval: Keep alive interval in milliseconds (optional)
- queue_dir: Absolute path for persistent event queue (optional)
- queue_limit: Maximum number of events to queue (optional, default: 0)
"#; 