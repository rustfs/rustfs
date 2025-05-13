use crate::metrics::{MetricName, MetricNamespace, MetricSubsystem, MetricType};

/// The metric description describes the metric
/// It contains the namespace, subsystem, name, help text, and type of the metric
#[derive(Debug, Clone)]
pub struct MetricDescription {
    pub namespace: MetricNamespace,
    pub subsystem: MetricSubsystem,
    pub name: MetricName,
    pub help: String,
    pub metric_type: MetricType,
}
