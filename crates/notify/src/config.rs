use std::collections::HashMap;

/// Represents a key-value pair in configuration
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KV {
    pub key: String,
    pub value: String,
}

/// Represents a collection of key-value pairs
#[derive(Debug, Clone, Default)]
pub struct KVS {
    kvs: Vec<KV>,
}

impl KVS {
    /// Creates a new empty KVS
    pub fn new() -> Self {
        KVS { kvs: Vec::new() }
    }

    /// Sets a key-value pair
    pub fn set(&mut self, key: impl Into<String>, value: impl Into<String>) {
        let key = key.into();
        let value = value.into();

        // Update existing value or add new
        for kv in &mut self.kvs {
            if kv.key == key {
                kv.value = value;
                return;
            }
        }

        self.kvs.push(KV { key, value });
    }

    /// Looks up a value by key
    pub fn lookup(&self, key: &str) -> Option<&str> {
        self.kvs
            .iter()
            .find(|kv| kv.key == key)
            .map(|kv| kv.value.as_str())
    }

    /// Deletes a key-value pair
    pub fn delete(&mut self, key: &str) {
        self.kvs.retain(|kv| kv.key != key);
    }

    /// Checks if the KVS is empty
    pub fn is_empty(&self) -> bool {
        self.kvs.is_empty()
    }

    /// Returns all keys
    pub fn keys(&self) -> Vec<String> {
        self.kvs.iter().map(|kv| kv.key.clone()).collect()
    }
}

/// Represents the entire configuration
pub type Config = HashMap<String, HashMap<String, KVS>>;

/// Parses configuration from a string
pub fn parse_config(config_str: &str) -> Result<Config, String> {
    let mut config = Config::new();
    let mut current_section = String::new();
    let mut current_subsection = String::new();

    for line in config_str.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        // Parse sections
        if line.starts_with('[') && line.ends_with(']') {
            let section = line[1..line.len() - 1].trim();
            if let Some((section_name, subsection)) = section.split_once(' ') {
                current_section = section_name.to_string();
                current_subsection = subsection.trim_matches('"').to_string();
            } else {
                current_section = section.to_string();
                current_subsection = String::new();
            }
            continue;
        }

        // Parse key-value pairs
        if let Some((key, value)) = line.split_once('=') {
            let key = key.trim();
            let value = value.trim();

            let section = config.entry(current_section.clone()).or_default();

            let kvs = section.entry(current_subsection.clone()).or_default();

            kvs.set(key, value);
        }
    }

    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kvs() {
        let mut kvs = KVS::new();
        assert!(kvs.is_empty());

        kvs.set("key1", "value1");
        kvs.set("key2", "value2");
        assert!(!kvs.is_empty());

        assert_eq!(kvs.lookup("key1"), Some("value1"));
        assert_eq!(kvs.lookup("key2"), Some("value2"));
        assert_eq!(kvs.lookup("key3"), None);

        kvs.set("key1", "new_value");
        assert_eq!(kvs.lookup("key1"), Some("new_value"));

        kvs.delete("key2");
        assert_eq!(kvs.lookup("key2"), None);
    }

    #[test]
    fn test_parse_config() {
        let config_str = r#"
        # Comment line
        [notify_webhook "webhook1"]
        enable = on
        endpoint = http://example.com/webhook
        auth_token = secret
        
        [notify_mqtt "mqtt1"]
        enable = on
        broker = mqtt://localhost:1883
        topic = rustfs/events
        "#;

        let config = parse_config(config_str).unwrap();

        assert!(config.contains_key("notify_webhook"));
        assert!(config.contains_key("notify_mqtt"));

        let webhook = &config["notify_webhook"]["webhook1"];
        assert_eq!(webhook.lookup("enable"), Some("on"));
        assert_eq!(
            webhook.lookup("endpoint"),
            Some("http://example.com/webhook")
        );
        assert_eq!(webhook.lookup("auth_token"), Some("secret"));

        let mqtt = &config["notify_mqtt"]["mqtt1"];
        assert_eq!(mqtt.lookup("enable"), Some("on"));
        assert_eq!(mqtt.lookup("broker"), Some("mqtt://localhost:1883"));
        assert_eq!(mqtt.lookup("topic"), Some("rustfs/events"));
    }
}
