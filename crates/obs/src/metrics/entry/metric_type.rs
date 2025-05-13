/// MetricType - Indicates the type of indicator
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricType {
    Counter,
    Gauge,
    Histogram,
}

impl MetricType {
    /// convert the metric type to a string representation
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Counter => "counter",
            Self::Gauge => "gauge",
            Self::Histogram => "histogram",
        }
    }

    /// Convert the metric type to the Prometheus value type
    /// In a Rust implementation, this might return the corresponding Prometheus Rust client type
    pub fn to_prom(&self) -> &'static str {
        match self {
            Self::Counter => "counter_value",
            Self::Gauge => "gauge_value",
            Self::Histogram => "counter_value", // 直方图在 Prometheus 中仍使用 counter 值
        }
    }
}
