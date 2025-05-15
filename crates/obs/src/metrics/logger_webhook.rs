use crate::metrics::{new_counter_md, new_gauge_md, subsystems, MetricDescriptor, MetricName};

/// 定义标签常量
pub const NAME_LABEL: &str = "name";
pub const ENDPOINT_LABEL: &str = "endpoint";

/// Webhook 日志相关指标描述符
lazy_static::lazy_static! {
    // 所有 Webhook 指标使用的标签
    static ref ALL_WEBHOOK_LABELS: [&'static str; 2] = [NAME_LABEL, ENDPOINT_LABEL];

    pub static ref WEBHOOK_FAILED_MESSAGES_MD: MetricDescriptor =
        new_counter_md(
            MetricName::WebhookFailedMessages,
            "Number of messages that failed to send",
            &ALL_WEBHOOK_LABELS[..],
            subsystems::LOGGER_WEBHOOK
        );

    pub static ref WEBHOOK_QUEUE_LENGTH_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::WebhookQueueLength,
            "Webhook queue length",
            &ALL_WEBHOOK_LABELS[..],
            subsystems::LOGGER_WEBHOOK
        );

    pub static ref WEBHOOK_TOTAL_MESSAGES_MD: MetricDescriptor =
        new_counter_md(
            MetricName::WebhookTotalMessages,
            "Total number of messages sent to this target",
            &ALL_WEBHOOK_LABELS[..],
            subsystems::LOGGER_WEBHOOK
        );
}
