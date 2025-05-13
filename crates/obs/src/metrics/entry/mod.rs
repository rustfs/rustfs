use crate::metrics::{MetricDescriptor, MetricName, MetricType};

pub(crate) mod description;
pub(crate) mod descriptor;
pub(crate) mod metric_name;
pub(crate) mod metric_type;
pub(crate) mod namespace;
pub(crate) mod subsystem;

/// Represents a range label constant
pub const RANGE_LABEL: &str = "range";
pub const SERVER_NAME: &str = "server";

/// Create a new counter metric descriptor
pub fn new_counter_md(name: impl Into<MetricName>, help: impl Into<String>, labels: &[&str]) -> MetricDescriptor {
    MetricDescriptor::new(
        name.into(),
        MetricType::Counter,
        help.into(),
        labels.iter().map(|&s| s.to_string()).collect(),
    )
}

/// Create a new gauge metric descriptor
pub fn new_gauge_md(name: impl Into<MetricName>, help: impl Into<String>, labels: &[&str]) -> MetricDescriptor {
    MetricDescriptor::new(
        name.into(),
        MetricType::Gauge,
        help.into(),
        labels.iter().map(|&s| s.to_string()).collect(),
    )
}
