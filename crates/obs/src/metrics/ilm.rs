/// ILM-related metric descriptors
use crate::metrics::{new_counter_md, new_gauge_md, subsystems, MetricDescriptor, MetricName};

lazy_static::lazy_static! {
    pub static ref ILM_EXPIRY_PENDING_TASKS_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::IlmExpiryPendingTasks,
            "Number of pending ILM expiry tasks in the queue",
            &[],
            subsystems::ILM
        );

    pub static ref ILM_TRANSITION_ACTIVE_TASKS_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::IlmTransitionActiveTasks,
            "Number of active ILM transition tasks",
            &[],
            subsystems::ILM
        );

    pub static ref ILM_TRANSITION_PENDING_TASKS_MD: MetricDescriptor =
        new_gauge_md(
            MetricName::IlmTransitionPendingTasks,
            "Number of pending ILM transition tasks in the queue",
            &[],
            subsystems::ILM
        );

    pub static ref ILM_TRANSITION_MISSED_IMMEDIATE_TASKS_MD: MetricDescriptor =
        new_counter_md(
            MetricName::IlmTransitionMissedImmediateTasks,
            "Number of missed immediate ILM transition tasks",
            &[],
            subsystems::ILM
        );

    pub static ref ILM_VERSIONS_SCANNED_MD: MetricDescriptor =
        new_counter_md(
            MetricName::IlmVersionsScanned,
            "Total number of object versions checked for ILM actions since server start",
            &[],
            subsystems::ILM
        );
}
