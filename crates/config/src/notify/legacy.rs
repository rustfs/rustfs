use crate::notify::webhook::WebhookArgs;
use crate::notify::mqtt::MQTTArgs;
use std::collections::HashMap;

/// Convert legacy webhook configuration to the new WebhookArgs struct.
pub fn convert_webhook_config(config: &HashMap<String, String>) -> Result<WebhookArgs, String> {
    let mut args = WebhookArgs::new();
    args.enable = config.get("enable").map_or(false, |v| v == "true");
    args.endpoint = config.get("endpoint").unwrap_or(&"".to_string()).clone();
    args.auth_token = config.get("auth_token").unwrap_or(&"".to_string()).clone();
    args.queue_dir = config.get("queue_dir").unwrap_or(&"".to_string()).clone();
    args.queue_limit = config.get("queue_limit").map_or(0, |v| v.parse().unwrap_or(0));
    args.client_cert = config.get("client_cert").unwrap_or(&"".to_string()).clone();
    args.client_key = config.get("client_key").unwrap_or(&"".to_string()).clone();
    Ok(args)
}

/// Convert legacy MQTT configuration to the new MQTTArgs struct.
pub fn convert_mqtt_config(config: &HashMap<String, String>) -> Result<MQTTArgs, String> {
    let mut args = MQTTArgs::new();
    args.enable = config.get("enable").map_or(false, |v| v == "true");
    args.broker = config.get("broker").unwrap_or(&"".to_string()).clone();
    args.topic = config.get("topic").unwrap_or(&"".to_string()).clone();
    args.qos = config.get("qos").map_or(0, |v| v.parse().unwrap_or(0));
    args.username = config.get("username").unwrap_or(&"".to_string()).clone();
    args.password = config.get("password").unwrap_or(&"".to_string()).clone();
    args.reconnect_interval = config.get("reconnect_interval").map_or(0, |v| v.parse().unwrap_or(0));
    args.keep_alive_interval = config.get("keep_alive_interval").map_or(0, |v| v.parse().unwrap_or(0));
    args.queue_dir = config.get("queue_dir").unwrap_or(&"".to_string()).clone();
    args.queue_limit = config.get("queue_limit").map_or(0, |v| v.parse().unwrap_or(0));
    Ok(args)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_webhook_config() {
        let mut old_config = HashMap::new();
        old_config.insert("endpoint".to_string(), "http://example.com".to_string());
        old_config.insert("auth_token".to_string(), "token123".to_string());
        old_config.insert("max_retries".to_string(), "5".to_string());
        old_config.insert("timeout".to_string(), "2000".to_string());

        let result = convert_webhook_config(&old_config);
        assert!(result.is_ok());
        let args = result.unwrap();
        assert_eq!(args.endpoint, "http://example.com");
        assert_eq!(args.auth_token, Some("token123".to_string()));
        assert_eq!(args.max_retries, 5);
        assert_eq!(args.timeout, 2000);
    }

    #[test]
    fn test_convert_mqtt_config() {
        let mut old_config = HashMap::new();
        old_config.insert("broker".to_string(), "mqtt.example.com".to_string());
        old_config.insert("port".to_string(), "1883".to_string());
        old_config.insert("client_id".to_string(), "test_client".to_string());
        old_config.insert("topic".to_string(), "test_topic".to_string());
        old_config.insert("max_retries".to_string(), "4".to_string());

        let result = convert_mqtt_config(&old_config);
        assert!(result.is_ok());
        let args = result.unwrap();
        assert_eq!(args.broker, "mqtt.example.com");
        assert_eq!(args.port, 1883);
        assert_eq!(args.client_id, "test_client");
        assert_eq!(args.topic, "test_topic");
        assert_eq!(args.max_retries, 4);
    }

    #[test]
    fn test_convert_webhook_config_invalid() {
        let mut old_config = HashMap::new();
        old_config.insert("max_retries".to_string(), "invalid".to_string());
        let result = convert_webhook_config(&old_config);
        assert!(result.is_err());
    }

    #[test]
    fn test_convert_mqtt_config_invalid() {
        let mut old_config = HashMap::new();
        old_config.insert("port".to_string(), "invalid".to_string());
        let result = convert_mqtt_config(&old_config);
        assert!(result.is_err());
    }

    #[test]
    fn test_convert_empty_config() {
        let empty_config = HashMap::new();
        let webhook_result = convert_webhook_config(&empty_config);
        assert!(webhook_result.is_ok());
        let mqtt_result = convert_mqtt_config(&empty_config);
        assert!(mqtt_result.is_ok());
    }

    #[test]
    fn test_convert_partial_config() {
        let mut partial_config = HashMap::new();
        partial_config.insert("endpoint".to_string(), "http://example.com".to_string());
        let webhook_result = convert_webhook_config(&partial_config);
        assert!(webhook_result.is_ok());
        let args = webhook_result.unwrap();
        assert_eq!(args.endpoint, "http://example.com");
        assert_eq!(args.max_retries, 3); // default value
        assert_eq!(args.timeout, 1000); // default value

        let mut partial_mqtt_config = HashMap::new();
        partial_mqtt_config.insert("broker".to_string(), "mqtt.example.com".to_string());
        let mqtt_result = convert_mqtt_config(&partial_mqtt_config);
        assert!(mqtt_result.is_ok());
        let args = mqtt_result.unwrap();
        assert_eq!(args.broker, "mqtt.example.com");
        assert_eq!(args.max_retries, 3); // default value
    }

    #[test]
    fn test_convert_config_with_extra_fields() {
        let mut extra_config = HashMap::new();
        extra_config.insert("endpoint".to_string(), "http://example.com".to_string());
        extra_config.insert("extra_field".to_string(), "extra_value".to_string());
        let webhook_result = convert_webhook_config(&extra_config);
        assert!(webhook_result.is_ok());
        let args = webhook_result.unwrap();
        assert_eq!(args.endpoint, "http://example.com");

        let mut extra_mqtt_config = HashMap::new();
        extra_mqtt_config.insert("broker".to_string(), "mqtt.example.com".to_string());
        extra_mqtt_config.insert("extra_field".to_string(), "extra_value".to_string());
        let mqtt_result = convert_mqtt_config(&extra_mqtt_config);
        assert!(mqtt_result.is_ok());
        let args = mqtt_result.unwrap();
        assert_eq!(args.broker, "mqtt.example.com");
    }

    #[test]
    fn test_convert_config_with_empty_values() {
        let mut empty_values_config = HashMap::new();
        empty_values_config.insert("endpoint".to_string(), "".to_string());
        let webhook_result = convert_webhook_config(&empty_values_config);
        assert!(webhook_result.is_ok());
        let args = webhook_result.unwrap();
        assert_eq!(args.endpoint, "");

        let mut empty_mqtt_config = HashMap::new();
        empty_mqtt_config.insert("broker".to_string(), "".to_string());
        let mqtt_result = convert_mqtt_config(&empty_mqtt_config);
        assert!(mqtt_result.is_ok());
        let args = mqtt_result.unwrap();
        assert_eq!(args.broker, "");
    }

    #[test]
    fn test_convert_config_with_whitespace_values() {
        let mut whitespace_config = HashMap::new();
        whitespace_config.insert("endpoint".to_string(), "  http://example.com  ".to_string());
        let webhook_result = convert_webhook_config(&whitespace_config);
        assert!(webhook_result.is_ok());
        let args = webhook_result.unwrap();
        assert_eq!(args.endpoint, "  http://example.com  ");

        let mut whitespace_mqtt_config = HashMap::new();
        whitespace_mqtt_config.insert("broker".to_string(), "  mqtt.example.com  ".to_string());
        let mqtt_result = convert_mqtt_config(&whitespace_mqtt_config);
        assert!(mqtt_result.is_ok());
        let args = mqtt_result.unwrap();
        assert_eq!(args.broker, "  mqtt.example.com  ");
    }

    #[test]
    fn test_convert_config_with_special_characters() {
        let mut special_chars_config = HashMap::new();
        special_chars_config.insert("endpoint".to_string(), "http://example.com/path?param=value&other=123".to_string());
        let webhook_result = convert_webhook_config(&special_chars_config);
        assert!(webhook_result.is_ok());
        let args = webhook_result.unwrap();
        assert_eq!(args.endpoint, "http://example.com/path?param=value&other=123");

        let mut special_chars_mqtt_config = HashMap::new();
        special_chars_mqtt_config.insert("broker".to_string(), "mqtt.example.com:1883".to_string());
        let mqtt_result = convert_mqtt_config(&special_chars_mqtt_config);
        assert!(mqtt_result.is_ok());
        let args = mqtt_result.unwrap();
        assert_eq!(args.broker, "mqtt.example.com:1883");
    }

    #[test]
    fn test_convert_config_with_numeric_values() {
        let mut numeric_config = HashMap::new();
        numeric_config.insert("max_retries".to_string(), "5".to_string());
        numeric_config.insert("timeout".to_string(), "2000".to_string());
        let webhook_result = convert_webhook_config(&numeric_config);
        assert!(webhook_result.is_ok());
        let args = webhook_result.unwrap();
        assert_eq!(args.max_retries, 5);
        assert_eq!(args.timeout, 2000);

        let mut numeric_mqtt_config = HashMap::new();
        numeric_mqtt_config.insert("port".to_string(), "1883".to_string());
        numeric_mqtt_config.insert("max_retries".to_string(), "4".to_string());
        let mqtt_result = convert_mqtt_config(&numeric_mqtt_config);
        assert!(mqtt_result.is_ok());
        let args = mqtt_result.unwrap();
        assert_eq!(args.port, 1883);
        assert_eq!(args.max_retries, 4);
    }

    #[test]
    fn test_convert_config_with_boolean_values() {
        let mut boolean_config = HashMap::new();
        boolean_config.insert("enable".to_string(), "true".to_string());
        let webhook_result = convert_webhook_config(&boolean_config);
        assert!(webhook_result.is_ok());
        let args = webhook_result.unwrap();
        assert_eq!(args.endpoint, ""); // default value

        let mut boolean_mqtt_config = HashMap::new();
        boolean_mqtt_config.insert("enable".to_string(), "false".to_string());
        let mqtt_result = convert_mqtt_config(&boolean_mqtt_config);
        assert!(mqtt_result.is_ok());
        let args = mqtt_result.unwrap();
        assert_eq!(args.broker, "localhost"); // default value
    }

    #[test]
    fn test_convert_config_with_null_values() {
        let mut null_config = HashMap::new();
        null_config.insert("endpoint".to_string(), "null".to_string());
        let webhook_result = convert_webhook_config(&null_config);
        assert!(webhook_result.is_ok());
        let args = webhook_result.unwrap();
        assert_eq!(args.endpoint, "null");

        let mut null_mqtt_config = HashMap::new();
        null_mqtt_config.insert("broker".to_string(), "null".to_string());
        let mqtt_result = convert_mqtt_config(&null_mqtt_config);
        assert!(mqtt_result.is_ok());
        let args = mqtt_result.unwrap();
        assert_eq!(args.broker, "null");
    }

    #[test]
    fn test_convert_config_with_duplicate_keys() {
        let mut duplicate_config = HashMap::new();
        duplicate_config.insert("endpoint".to_string(), "http://example.com".to_string());
        duplicate_config.insert("endpoint".to_string(), "http://example.org".to_string());
        let webhook_result = convert_webhook_config(&duplicate_config);
        assert!(webhook_result.is_ok());
        let args = webhook_result.unwrap();
        assert_eq!(args.endpoint, "http://example.org"); // last value wins

        let mut duplicate_mqtt_config = HashMap::new();
        duplicate_mqtt_config.insert("broker".to_string(), "mqtt.example.com".to_string());
        duplicate_mqtt_config.insert("broker".to_string(), "mqtt.example.org".to_string());
        let mqtt_result = convert_mqtt_config(&duplicate_mqtt_config);
        assert!(mqtt_result.is_ok());
        let args = mqtt_result.unwrap();
        assert_eq!(args.broker, "mqtt.example.org"); // last value wins
    }

    #[test]
    fn test_convert_config_with_case_insensitive_keys() {
        let mut case_insensitive_config = HashMap::new();
        case_insensitive_config.insert("ENDPOINT".to_string(), "http://example.com".to_string());
        let webhook_result = convert_webhook_config(&case_insensitive_config);
        assert!(webhook_result.is_ok());
        let args = webhook_result.unwrap();
        assert_eq!(args.endpoint, "http://example.com");

        let mut case_insensitive_mqtt_config = HashMap::new();
        case_insensitive_mqtt_config.insert("BROKER".to_string(), "mqtt.example.com".to_string());
        let mqtt_result = convert_mqtt_config(&case_insensitive_mqtt_config);
        assert!(mqtt_result.is_ok());
        let args = mqtt_result.unwrap();
        assert_eq!(args.broker, "mqtt.example.com");
    }

    #[test]
    fn test_convert_config_with_mixed_case_keys() {
        let mut mixed_case_config = HashMap::new();
        mixed_case_config.insert("EndPoint".to_string(), "http://example.com".to_string());
        let webhook_result = convert_webhook_config(&mixed_case_config);
        assert!(webhook_result.is_ok());
        let args = webhook_result.unwrap();
        assert_eq!(args.endpoint, "http://example.com");

        let mut mixed_case_mqtt_config = HashMap::new();
        mixed_case_mqtt_config.insert("BroKer".to_string(), "mqtt.example.com".to_string());
        let mqtt_result = convert_mqtt_config(&mixed_case_mqtt_config);
        assert!(mqtt_result.is_ok());
        let args = mqtt_result.unwrap();
        assert_eq!(args.broker, "mqtt.example.com");
    }

    #[test]
    fn test_convert_config_with_snake_case_keys() {
        let mut snake_case_config = HashMap::new();
        snake_case_config.insert("end_point".to_string(), "http://example.com".to_string());
        let webhook_result = convert_webhook_config(&snake_case_config);
        assert!(webhook_result.is_ok());
        let args = webhook_result.unwrap();
        assert_eq!(args.endpoint, "http://example.com");

        let mut snake_case_mqtt_config = HashMap::new();
        snake_case_mqtt_config.insert("bro_ker".to_string(), "mqtt.example.com".to_string());
        let mqtt_result = convert_mqtt_config(&snake_case_mqtt_config);
        assert!(mqtt_result.is_ok());
        let args = mqtt_result.unwrap();
        assert_eq!(args.broker, "mqtt.example.com");
    }

    #[test]
    fn test_convert_config_with_kebab_case_keys() {
        let mut kebab_case_config = HashMap::new();
        kebab_case_config.insert("end-point".to_string(), "http://example.com".to_string());
        let webhook_result = convert_webhook_config(&kebab_case_config);
        assert!(webhook_result.is_ok());
        let args = webhook_result.unwrap();
        assert_eq!(args.endpoint, "http://example.com");

        let mut kebab_case_mqtt_config = HashMap::new();
        kebab_case_mqtt_config.insert("bro-ker".to_string(), "mqtt.example.com".to_string());
        let mqtt_result = convert_mqtt_config(&kebab_case_mqtt_config);
        assert!(mqtt_result.is_ok());
        let args = mqtt_result.unwrap();
        assert_eq!(args.broker, "mqtt.example.com");
    }

    #[test]
    fn test_convert_config_with_camel_case_keys() {
        let mut camel_case_config = HashMap::new();
        camel_case_config.insert("endPoint".to_string(), "http://example.com".to_string());
        let webhook_result = convert_webhook_config(&camel_case_config);
        assert!(webhook_result.is_ok());
        let args = webhook_result.unwrap();
        assert_eq!(args.endpoint, "http://example.com");

        let mut camel_case_mqtt_config = HashMap::new();
        camel_case_mqtt_config.insert("broKer".to_string(), "mqtt.example.com".to_string());
        let mqtt_result = convert_mqtt_config(&camel_case_mqtt_config);
        assert!(mqtt_result.is_ok());
        let args = mqtt_result.unwrap();
        assert_eq!(args.broker, "mqtt.example.com");
    }
}
