/// MetricType - Indicates the type of indicator
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricType {
    Counter,
    Gauge,
    Histogram,
}

impl MetricType {
    /// convert the metric type to a string representation
    #[allow(dead_code)]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Counter => "counter",
            Self::Gauge => "gauge",
            Self::Histogram => "histogram",
        }
    }

    /// Convert the metric type to the Prometheus value type
    /// In a Rust implementation, this might return the corresponding Prometheus Rust client type
    #[allow(dead_code)]
    pub fn to_prom(&self) -> &'static str {
        match self {
            Self::Counter => "counter.",
            Self::Gauge => "gauge.",
            Self::Histogram => "histogram.", // Histograms still use the counter value in Prometheus
        }
    }
}
