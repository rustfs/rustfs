use serde::{Deserialize, Serialize};

/// MQTTArgs - MQTT target arguments.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MQTTArgs {
    pub enable: bool,
    pub broker: String,
    pub topic: String,
    pub qos: u8,
    pub username: String,
    pub password: String,
    pub reconnect_interval: u64,
    pub keep_alive_interval: u64,
    #[serde(skip)]
    pub root_cas: Option<()>, // Placeholder for *x509.CertPool
    pub queue_dir: String,
    pub queue_limit: u64,
}

impl MQTTArgs {
    /// Create a new configuration with default values.
    pub fn new() -> Self {
        Self {
            enable: false,
            broker: "".to_string(),
            topic: "".to_string(),
            qos: 0,
            username: "".to_string(),
            password: "".to_string(),
            reconnect_interval: 0,
            keep_alive_interval: 0,
            root_cas: None,
            queue_dir: "".to_string(),
            queue_limit: 0,
        }
    }

    /// Validate MQTTArgs fields
    pub fn validate(&self) -> Result<(), String> {
        if !self.enable {
            return Ok(());
        }
        if self.broker.trim().is_empty() {
            return Err("MQTT broker cannot be empty".to_string());
        }
        if self.topic.trim().is_empty() {
            return Err("MQTT topic cannot be empty".to_string());
        }
        if self.queue_dir != "" && !self.queue_dir.starts_with('/') {
            return Err("queueDir path should be absolute".to_string());
        }
        if self.qos == 0 && self.queue_dir != "" {
            return Err("qos should be set to 1 or 2 if queueDir is set".to_string());
        }
        Ok(())
    }
}

impl Default for MQTTArgs {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mqtt_args_new() {
        let args = MQTTArgs::new();
        assert_eq!(args.broker, "");
        assert_eq!(args.topic, "");
        assert_eq!(args.qos, 0);
        assert_eq!(args.username, "");
        assert_eq!(args.password, "");
        assert_eq!(args.reconnect_interval, 0);
        assert_eq!(args.keep_alive_interval, 0);
        assert!(args.root_cas.is_none());
        assert_eq!(args.queue_dir, "");
        assert_eq!(args.queue_limit, 0);
        assert!(!args.enable);
    }

    #[test]
    fn test_mqtt_args_validate() {
        let mut args = MQTTArgs::new();
        assert!(args.validate().is_ok());
        args.broker = "".to_string();
        assert!(args.validate().is_err());
        args.broker = "localhost".to_string();
        args.topic = "".to_string();
        assert!(args.validate().is_err());
        args.topic = "mqtt_topic".to_string();
        args.reconnect_interval = 10001;
        assert!(args.validate().is_err());
        args.reconnect_interval = 1000;
        args.keep_alive_interval = 10001;
        assert!(args.validate().is_err());
        args.keep_alive_interval = 1000;
        args.queue_limit = 10001;
        assert!(args.validate().is_err());
        args.queue_dir = "invalid_path".to_string();
        assert!(args.validate().is_err());
        args.queue_dir = "/valid_path".to_string();
        assert!(args.validate().is_ok());
        args.qos = 0;
        assert!(args.validate().is_err());
        args.qos = 1;
        assert!(args.validate().is_ok());
        args.qos = 2;
        assert!(args.validate().is_ok());
    }
}
