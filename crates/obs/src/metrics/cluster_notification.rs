use crate::metrics::{new_counter_md, subsystems, MetricDescriptor, MetricName};

/// 通知相关指标描述符
lazy_static::lazy_static! {
    pub static ref NOTIFICATION_CURRENT_SEND_IN_PROGRESS_MD: MetricDescriptor =
        new_counter_md(
            MetricName::NotificationCurrentSendInProgress,
            "Number of concurrent async Send calls active to all targets",
            &[],  // 无标签
            subsystems::NOTIFICATION
        );

    pub static ref NOTIFICATION_EVENTS_ERRORS_TOTAL_MD: MetricDescriptor =
        new_counter_md(
            MetricName::NotificationEventsErrorsTotal,
            "Events that were failed to be sent to the targets",
            &[],  // 无标签
            subsystems::NOTIFICATION
        );

    pub static ref NOTIFICATION_EVENTS_SENT_TOTAL_MD: MetricDescriptor =
        new_counter_md(
            MetricName::NotificationEventsSentTotal,
            "Total number of events sent to the targets",
            &[],  // 无标签
            subsystems::NOTIFICATION
        );

    pub static ref NOTIFICATION_EVENTS_SKIPPED_TOTAL_MD: MetricDescriptor =
        new_counter_md(
            MetricName::NotificationEventsSkippedTotal,
            "Events that were skipped to be sent to the targets due to the in-memory queue being full",
            &[],  // 无标签
            subsystems::NOTIFICATION
        );
}
