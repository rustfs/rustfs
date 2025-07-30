use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{event, Level};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptionAuditEvent {
    pub timestamp: u64,
    pub event_type: EncryptionEventType,
    pub bucket: String,
    pub object_key: Option<String>,
    pub encryption_method: Option<String>,
    pub key_id: Option<String>,
    pub operation: String,
    pub success: bool,
    pub error_message: Option<String>,
    pub metadata: HashMap<String, String>,
    pub user_id: Option<String>,
    pub request_id: Option<String>,
    pub bytes_processed: Option<u64>,
    pub duration_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EncryptionEventType {
    ObjectEncrypted,
    ObjectDecrypted,
    BucketEncryptionConfigured,
    BucketEncryptionRemoved,
    KeyRotationInitiated,
    KeyRotationCompleted,
    EncryptionFailed,
    DecryptionFailed,
    ConfigurationError,
    MetadataValidationFailed,
}

impl EncryptionAuditEvent {
    pub fn new(
        event_type: EncryptionEventType,
        bucket: String,
        object_key: Option<String>,
        operation: String,
        success: bool,
    ) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            timestamp,
            event_type,
            bucket,
            object_key,
            encryption_method: None,
            key_id: None,
            operation,
            success,
            error_message: None,
            metadata: HashMap::new(),
            user_id: None,
            request_id: None,
            bytes_processed: None,
            duration_ms: None,
        }
    }

    pub fn with_encryption_method(mut self, method: String) -> Self {
        self.encryption_method = Some(method);
        self
    }

    pub fn with_key_id(mut self, key_id: String) -> Self {
        self.key_id = Some(key_id);
        self
    }

    pub fn with_error(mut self, error: String) -> Self {
        self.error_message = Some(error);
        self
    }

    pub fn with_metadata(mut self, key: String, value: String) -> Self {
        self.metadata.insert(key, value);
        self
    }

    pub fn with_user_id(mut self, user_id: String) -> Self {
        self.user_id = Some(user_id);
        self
    }

    pub fn with_request_id(mut self, request_id: String) -> Self {
        self.request_id = Some(request_id);
        self
    }

    pub fn with_bytes_processed(mut self, bytes: u64) -> Self {
        self.bytes_processed = Some(bytes);
        self
    }

    pub fn with_duration(mut self, duration_ms: u64) -> Self {
        self.duration_ms = Some(duration_ms);
        self
    }

    pub fn log(&self) {
        let level = if self.success { Level::INFO } else { Level::ERROR };
        let json = serde_json::to_string(self).unwrap_or_default();
        event!(level, audit.encryption = %json, "Encryption audit event");
    }
}

pub struct EncryptionAuditLogger;

impl EncryptionAuditLogger {
    pub fn log_object_encrypted(
        bucket: String,
        object_key: String,
        encryption_method: String,
        key_id: String,
        bytes_processed: u64,
        duration_ms: u64,
        user_id: Option<String>,
        request_id: Option<String>,
    ) {
        EncryptionAuditEvent::new(
            EncryptionEventType::ObjectEncrypted,
            bucket,
            Some(object_key),
            "put_object".to_string(),
            true,
        )
        .with_encryption_method(encryption_method)
        .with_key_id(key_id)
        .with_bytes_processed(bytes_processed)
        .with_duration(duration_ms)
        .with_user_id(user_id.unwrap_or_default())
        .with_request_id(request_id.unwrap_or_default())
        .log();
    }

    pub fn log_object_decrypted(
        bucket: String,
        object_key: String,
        encryption_method: String,
        key_id: String,
        bytes_processed: u64,
        duration_ms: u64,
        user_id: Option<String>,
        request_id: Option<String>,
    ) {
        EncryptionAuditEvent::new(
            EncryptionEventType::ObjectDecrypted,
            bucket,
            Some(object_key),
            "get_object".to_string(),
            true,
        )
        .with_encryption_method(encryption_method)
        .with_key_id(key_id)
        .with_bytes_processed(bytes_processed)
        .with_duration(duration_ms)
        .with_user_id(user_id.unwrap_or_default())
        .with_request_id(request_id.unwrap_or_default())
        .log();
    }

    pub fn log_bucket_encryption_configured(
        bucket: String,
        encryption_method: String,
        key_id: String,
        user_id: Option<String>,
        request_id: Option<String>,
    ) {
        EncryptionAuditEvent::new(
            EncryptionEventType::BucketEncryptionConfigured,
            bucket,
            None,
            "configure_bucket_encryption".to_string(),
            true,
        )
        .with_encryption_method(encryption_method)
        .with_key_id(key_id)
        .with_user_id(user_id.unwrap_or_default())
        .with_request_id(request_id.unwrap_or_default())
        .log();
    }

    pub fn log_bucket_encryption_removed(
        bucket: String,
        user_id: Option<String>,
        request_id: Option<String>,
    ) {
        EncryptionAuditEvent::new(
            EncryptionEventType::BucketEncryptionRemoved,
            bucket,
            None,
            "remove_bucket_encryption".to_string(),
            true,
        )
        .with_user_id(user_id.unwrap_or_default())
        .with_request_id(request_id.unwrap_or_default())
        .log();
    }

    pub fn log_encryption_failure(
        bucket: String,
        object_key: Option<String>,
        operation: String,
        error: String,
        user_id: Option<String>,
        request_id: Option<String>,
    ) {
        EncryptionAuditEvent::new(
            EncryptionEventType::EncryptionFailed,
            bucket,
            object_key,
            operation,
            false,
        )
        .with_error(error)
        .with_user_id(user_id.unwrap_or_default())
        .with_request_id(request_id.unwrap_or_default())
        .log();
    }

    pub fn log_decryption_failure(
        bucket: String,
        object_key: String,
        operation: String,
        error: String,
        user_id: Option<String>,
        request_id: Option<String>,
    ) {
        EncryptionAuditEvent::new(
            EncryptionEventType::DecryptionFailed,
            bucket,
            Some(object_key),
            operation,
            false,
        )
        .with_error(error)
        .with_user_id(user_id.unwrap_or_default())
        .with_request_id(request_id.unwrap_or_default())
        .log();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encryption_audit_event_creation() {
        let event = EncryptionAuditEvent::new(
            EncryptionEventType::ObjectEncrypted,
            "test-bucket".to_string(),
            Some("test-object".to_string()),
            "put_object".to_string(),
            true,
        );

        assert_eq!(event.bucket, "test-bucket");
        assert_eq!(event.object_key, Some("test-object".to_string()));
        assert!(event.success);
    }

    #[test]
    fn test_encryption_audit_event_with_details() {
        let event = EncryptionAuditEvent::new(
            EncryptionEventType::ObjectEncrypted,
            "test-bucket".to_string(),
            Some("test-object".to_string()),
            "put_object".to_string(),
            true,
        )
        .with_encryption_method("AES-256-GCM".to_string())
        .with_key_id("test-key-id".to_string())
        .with_error("test error".to_string())
        .with_metadata("version".to_string(), "1.0".to_string())
        .with_user_id("user123".to_string())
        .with_request_id("req123".to_string())
        .with_bytes_processed(1024)
        .with_duration(100);

        assert_eq!(event.encryption_method, Some("AES-256-GCM".to_string()));
        assert_eq!(event.key_id, Some("test-key-id".to_string()));
        assert_eq!(event.error_message, Some("test error".to_string()));
        assert_eq!(event.user_id, Some("user123".to_string()));
        assert_eq!(event.request_id, Some("req123".to_string()));
        assert_eq!(event.bytes_processed, Some(1024));
        assert_eq!(event.duration_ms, Some(100));
    }
}