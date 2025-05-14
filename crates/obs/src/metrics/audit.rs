use crate::metrics::{new_counter_md, new_gauge_md, subsystems, MetricDescriptor, MetricName};

const TARGET_ID: &str = "target_id";

/// audit related metric descriptors
lazy_static::lazy_static! {
    pub static ref AUDIT_FAILED_MESSAGES_MD: MetricDescriptor =
        new_counter_md(
            MetricName::AuditFailedMessages,
            "Total number of messages that failed to send since start",
            &[TARGET_ID],
            subsystems::AUDIT
        );

    pub static ref AUDIT_TARGET_QUEUE_LENGTH_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::AuditTargetQueueLength,
            "Number of unsent messages in queue for target",
            &[TARGET_ID],
            subsystems::AUDIT
        );

    pub static ref AUDIT_TOTAL_MESSAGES_MD: MetricDescriptor =
        new_counter_md(
            MetricName::AuditTotalMessages,
            "Total number of messages sent since start",
            &[TARGET_ID],
            subsystems::AUDIT
        );
}
