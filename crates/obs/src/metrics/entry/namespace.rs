/// The metric namespace, which represents the top-level grouping of the metric
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetricNamespace {
    RustFS,
}

impl MetricNamespace {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::RustFS => "rustfs",
        }
    }
}
