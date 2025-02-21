//! Logging utilities
///
/// This crate provides utilities for logging.
///
/// # Examples
/// ```
/// use rustfs_logging::{log_info, log_error};
///
/// log_info("This is an informational message");
/// log_error("This is an error message");
/// ```
#[cfg(feature = "audit-kafka")]
pub use audit::KafkaAuditTarget;
#[cfg(feature = "audit-webhook")]
pub use audit::WebhookAuditTarget;
pub use audit::{AuditEntry, AuditLogger, AuditTarget, FileAuditTarget};
pub use logger::{log_debug, log_error, log_info};
pub use telemetry::Telemetry;

mod audit;
mod logger;
mod telemetry;

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::global;
    use opentelemetry::trace::TraceContextExt;
    use std::time::{Duration, SystemTime};
    use tracing::{instrument, Span};
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    #[instrument(fields(bucket, object, user))]
    async fn put_object(audit_logger: &AuditLogger, bucket: String, object: String, user: String) {
        let start_time = SystemTime::now();
        log_info("Starting PUT operation");

        // Simulate the operation
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Record Metrics
        let meter = global::meter("rustfs.rs");
        let request_duration = meter.f64_histogram("s3_request_duration_seconds").build();
        request_duration.record(
            start_time.elapsed().unwrap().as_secs_f64(),
            &[opentelemetry::KeyValue::new("operation", "put_object")],
        );

        // Gets the current span
        let span = Span::current();

        // Use 'OpenTelemetrySpanExt' to get 'SpanContext'
        let span_context = span.context(); // Get context via OpenTelemetrySpanExt
        let span_id = span_context.span().span_context().span_id().to_string(); // Get the SpanId

        // Audit events are logged
        let audit_entry = AuditEntry {
            version: "1.0".to_string(),
            event_type: "s3_put_object".to_string(),
            bucket,
            object,
            user,
            time: chrono::Utc::now().to_rfc3339(),
            user_agent: "rustfs.rs-client".to_string(),
            span_id,
        };
        audit_logger.log(audit_entry).await;

        log_info("PUT operation completed");
    }

    #[tokio::test]
    // #[cfg(feature = "audit-webhook")]
    // #[cfg(feature = "audit-kafka")]
    async fn test_main() {
        let telemetry = Telemetry::init();

        // Initialize multiple audit objectives
        let audit_targets: Vec<Box<dyn AuditTarget>> = vec![
            Box::new(FileAuditTarget),
            // Box::new(KafkaAuditTarget::new("localhost:9092", "rustfs-audit")),
            // Box::new(WebhookAuditTarget::new("http://localhost:8080/audit")),
        ];
        let audit_logger = AuditLogger::new(audit_targets);

        // Test the PUT operation
        put_object(&audit_logger, "my-bucket".to_string(), "my-object.txt".to_string(), "user123".to_string()).await;

        // Wait for the export to complete
        tokio::time::sleep(Duration::from_secs(2)).await;
        drop(telemetry); // Make sure to clean up
    }
}
