use config::ConfigError;
use thiserror::Error;
use tokio::sync::mpsc::error;
use tokio::task::JoinError;

/// The `Error` enum represents all possible errors that can occur in the application.
/// It implements the `std::error::Error` trait and provides a way to convert various error types into a single error type.
#[derive(Error, Debug)]
pub enum Error {
    #[error("Join error: {0}")]
    JoinError(#[from] JoinError),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Serialization error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[cfg(all(feature = "kafka", target_os = "linux"))]
    #[error("Kafka error: {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),
    #[cfg(feature = "mqtt")]
    #[error("MQTT error: {0}")]
    Mqtt(#[from] rumqttc::ClientError),
    #[error("Channel send error: {0}")]
    ChannelSend(#[from] Box<error::SendError<crate::event::Event>>),
    #[error("Feature disabled: {0}")]
    FeatureDisabled(&'static str),
    #[error("Event bus already started")]
    EventBusStarted,
    #[error("necessary fields are missing:{0}")]
    MissingField(&'static str),
    #[error("field verification failed:{0}")]
    ValidationError(&'static str),
    #[error("Custom error: {0}")]
    Custom(String),
    #[error("Configuration error: {0}")]
    ConfigError(String),
    #[error("Configuration loading error: {0}")]
    Config(#[from] ConfigError),
}

impl Error {
    pub fn custom(msg: &str) -> Error {
        Self::Custom(msg.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error as StdError;
    use std::io;
    use tokio::sync::mpsc;

    #[test]
    fn test_error_display() {
        // Test error message display
        let custom_error = Error::custom("test message");
        assert_eq!(custom_error.to_string(), "Custom error: test message");

        let feature_error = Error::FeatureDisabled("test feature");
        assert_eq!(feature_error.to_string(), "Feature disabled: test feature");

        let event_bus_error = Error::EventBusStarted;
        assert_eq!(event_bus_error.to_string(), "Event bus already started");

        let missing_field_error = Error::MissingField("required_field");
        assert_eq!(missing_field_error.to_string(), "necessary fields are missing:required_field");

        let validation_error = Error::ValidationError("invalid format");
        assert_eq!(validation_error.to_string(), "field verification failed:invalid format");

        let config_error = Error::ConfigError("invalid config".to_string());
        assert_eq!(config_error.to_string(), "Configuration error: invalid config");
    }

    #[test]
    fn test_error_debug() {
        // Test Debug trait implementation
        let custom_error = Error::custom("debug test");
        let debug_str = format!("{:?}", custom_error);
        assert!(debug_str.contains("Custom"));
        assert!(debug_str.contains("debug test"));

        let feature_error = Error::FeatureDisabled("debug feature");
        let debug_str = format!("{:?}", feature_error);
        assert!(debug_str.contains("FeatureDisabled"));
        assert!(debug_str.contains("debug feature"));
    }

    #[test]
    fn test_custom_error_creation() {
        // Test custom error creation
        let error = Error::custom("test custom error");
        match error {
            Error::Custom(msg) => assert_eq!(msg, "test custom error"),
            _ => panic!("Expected Custom error variant"),
        }

        // Test empty string
        let empty_error = Error::custom("");
        match empty_error {
            Error::Custom(msg) => assert_eq!(msg, ""),
            _ => panic!("Expected Custom error variant"),
        }

        // Test special characters
        let special_error = Error::custom("Test Chinese 中文 & special chars: !@#$%");
        match special_error {
            Error::Custom(msg) => assert_eq!(msg, "Test Chinese 中文 & special chars: !@#$%"),
            _ => panic!("Expected Custom error variant"),
        }
    }

    #[test]
    fn test_io_error_conversion() {
        // Test IO error conversion
        let io_error = io::Error::new(io::ErrorKind::NotFound, "file not found");
        let converted_error: Error = io_error.into();

        match converted_error {
            Error::Io(err) => {
                assert_eq!(err.kind(), io::ErrorKind::NotFound);
                assert_eq!(err.to_string(), "file not found");
            }
            _ => panic!("Expected Io error variant"),
        }

        // Test different types of IO errors
        let permission_error = io::Error::new(io::ErrorKind::PermissionDenied, "access denied");
        let converted: Error = permission_error.into();
        assert!(matches!(converted, Error::Io(_)));
    }

    #[test]
    fn test_serde_error_conversion() {
        // Test serialization error conversion
        let invalid_json = r#"{"invalid": json}"#;
        let serde_error = serde_json::from_str::<serde_json::Value>(invalid_json).unwrap_err();
        let converted_error: Error = serde_error.into();

        match converted_error {
            Error::Serde(_) => {
                // Verify error type is correct
                assert!(converted_error.to_string().contains("Serialization error"));
            }
            _ => panic!("Expected Serde error variant"),
        }
    }

    #[test]
    fn test_config_error_conversion() {
        // Test configuration error conversion
        let config_error = ConfigError::Message("invalid configuration".to_string());
        let converted_error: Error = config_error.into();

        match converted_error {
            Error::Config(_) => {
                assert!(converted_error.to_string().contains("Configuration loading error"));
            }
            _ => panic!("Expected Config error variant"),
        }
    }

    #[tokio::test]
    async fn test_channel_send_error_conversion() {
        // Test channel send error conversion
        let (tx, rx) = mpsc::channel::<crate::event::Event>(1);
        drop(rx); // Close receiver

        // Create a test event
        use crate::event::{Bucket, Identity, Metadata, Name, Object, Source};
        use std::collections::HashMap;

        let identity = Identity::new("test-user".to_string());
        let bucket = Bucket::new("test-bucket".to_string(), identity.clone(), "arn:aws:s3:::test-bucket".to_string());
        let object = Object::new(
            "test-key".to_string(),
            Some(1024),
            Some("etag123".to_string()),
            Some("text/plain".to_string()),
            Some(HashMap::new()),
            None,
            "sequencer123".to_string(),
        );
        let metadata = Metadata::create("1.0".to_string(), "config1".to_string(), bucket, object);
        let source = Source::new("localhost".to_string(), "8080".to_string(), "test-agent".to_string());

        let test_event = crate::event::Event::builder()
            .event_name(Name::ObjectCreatedPut)
            .s3(metadata)
            .source(source)
            .build()
            .unwrap();

        let send_result = tx.send(test_event).await;
        assert!(send_result.is_err());

        let send_error = send_result.unwrap_err();
        let boxed_error = Box::new(send_error);
        let converted_error: Error = boxed_error.into();

        match converted_error {
            Error::ChannelSend(_) => {
                assert!(converted_error.to_string().contains("Channel send error"));
            }
            _ => panic!("Expected ChannelSend error variant"),
        }
    }

    #[test]
    fn test_error_source_chain() {
        // 测试错误源链
        let io_error = io::Error::new(io::ErrorKind::InvalidData, "invalid data");
        let converted_error: Error = io_error.into();

        // 验证错误源
        assert!(converted_error.source().is_some());
        let source = converted_error.source().unwrap();
        assert_eq!(source.to_string(), "invalid data");
    }

    #[test]
    fn test_error_variants_exhaustive() {
        // 测试所有错误变体的创建
        let errors = vec![
            Error::FeatureDisabled("test"),
            Error::EventBusStarted,
            Error::MissingField("field"),
            Error::ValidationError("validation"),
            Error::Custom("custom".to_string()),
            Error::ConfigError("config".to_string()),
        ];

        for error in errors {
            // 验证每个错误都能正确显示
            let error_str = error.to_string();
            assert!(!error_str.is_empty());

            // 验证每个错误都能正确调试
            let debug_str = format!("{:?}", error);
            assert!(!debug_str.is_empty());
        }
    }

    #[test]
    fn test_error_equality_and_matching() {
        // 测试错误的模式匹配
        let custom_error = Error::custom("test");
        match custom_error {
            Error::Custom(msg) => assert_eq!(msg, "test"),
            _ => panic!("Pattern matching failed"),
        }

        let feature_error = Error::FeatureDisabled("feature");
        match feature_error {
            Error::FeatureDisabled(feature) => assert_eq!(feature, "feature"),
            _ => panic!("Pattern matching failed"),
        }

        let event_bus_error = Error::EventBusStarted;
        match event_bus_error {
            Error::EventBusStarted => {} // 正确匹配
            _ => panic!("Pattern matching failed"),
        }
    }

    #[test]
    fn test_error_message_formatting() {
        // 测试错误消息格式化
        let test_cases = vec![
            (Error::FeatureDisabled("kafka"), "Feature disabled: kafka"),
            (Error::MissingField("bucket_name"), "necessary fields are missing:bucket_name"),
            (Error::ValidationError("invalid email"), "field verification failed:invalid email"),
            (Error::ConfigError("missing file".to_string()), "Configuration error: missing file"),
        ];

        for (error, expected_message) in test_cases {
            assert_eq!(error.to_string(), expected_message);
        }
    }

    #[test]
    fn test_error_memory_efficiency() {
        // 测试错误类型的内存效率
        use std::mem;

        let size = mem::size_of::<Error>();
        // 错误类型应该相对紧凑，考虑到包含多种错误类型，96字节是合理的
        assert!(size <= 128, "Error size should be reasonable, got {} bytes", size);

        // 测试Option<Error>的大小
        let option_size = mem::size_of::<Option<Error>>();
        assert!(option_size <= 136, "Option<Error> should be efficient, got {} bytes", option_size);
    }

    #[test]
    fn test_error_thread_safety() {
        // 测试错误类型的线程安全性
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}

        assert_send::<Error>();
        assert_sync::<Error>();
    }

    #[test]
    fn test_custom_error_edge_cases() {
        // 测试自定义错误的边界情况
        let long_message = "a".repeat(1000);
        let long_error = Error::custom(&long_message);
        match long_error {
            Error::Custom(msg) => assert_eq!(msg.len(), 1000),
            _ => panic!("Expected Custom error variant"),
        }

        // 测试包含换行符的消息
        let multiline_error = Error::custom("line1\nline2\nline3");
        match multiline_error {
            Error::Custom(msg) => assert!(msg.contains('\n')),
            _ => panic!("Expected Custom error variant"),
        }

        // 测试包含Unicode字符的消息
        let unicode_error = Error::custom("🚀 Unicode test 测试 🎉");
        match unicode_error {
            Error::Custom(msg) => assert!(msg.contains('🚀')),
            _ => panic!("Expected Custom error variant"),
        }
    }

    #[test]
    fn test_error_conversion_consistency() {
        // 测试错误转换的一致性
        let original_io_error = io::Error::new(io::ErrorKind::TimedOut, "timeout");
        let error_message = original_io_error.to_string();
        let converted: Error = original_io_error.into();

        // 验证转换后的错误包含原始错误信息
        assert!(converted.to_string().contains(&error_message));
    }

    #[test]
    fn test_error_downcast() {
        // 测试错误的向下转型
        let io_error = io::Error::new(io::ErrorKind::Other, "test error");
        let converted: Error = io_error.into();

        // 验证可以获取源错误
        if let Error::Io(ref inner) = converted {
            assert_eq!(inner.to_string(), "test error");
            assert_eq!(inner.kind(), io::ErrorKind::Other);
        } else {
            panic!("Expected Io error variant");
        }
    }

    #[test]
    fn test_error_chain_depth() {
        // 测试错误链的深度
        let root_cause = io::Error::new(io::ErrorKind::Other, "root cause");
        let converted: Error = root_cause.into();

        let mut depth = 0;
        let mut current_error: &dyn StdError = &converted;

        while let Some(source) = current_error.source() {
            depth += 1;
            current_error = source;
            // 防止无限循环
            if depth > 10 {
                break;
            }
        }

        assert!(depth > 0, "Error should have at least one source");
        assert!(depth <= 3, "Error chain should not be too deep");
    }

    #[test]
    fn test_static_str_lifetime() {
        // 测试静态字符串生命周期
        fn create_feature_error() -> Error {
            Error::FeatureDisabled("static_feature")
        }

        let error = create_feature_error();
        match error {
            Error::FeatureDisabled(feature) => assert_eq!(feature, "static_feature"),
            _ => panic!("Expected FeatureDisabled error variant"),
        }
    }

    #[test]
    fn test_error_formatting_consistency() {
        // 测试错误格式化的一致性
        let errors = vec![
            Error::FeatureDisabled("test"),
            Error::MissingField("field"),
            Error::ValidationError("validation"),
            Error::Custom("custom".to_string()),
        ];

        for error in errors {
            let display_str = error.to_string();
            let debug_str = format!("{:?}", error);

            // Display和Debug都不应该为空
            assert!(!display_str.is_empty());
            assert!(!debug_str.is_empty());

            // Debug输出通常包含更多信息，但不是绝对的
            // 这里我们只验证两者都有内容即可
            assert!(debug_str.len() > 0);
            assert!(display_str.len() > 0);
        }
    }
}
