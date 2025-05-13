/// The metric namespace is the top-level grouping created by the metric name
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetricNamespace {
    Bucket,
    Cluster,
    Heal,
    InterNode,
    Node,
    RustFS,
    S3,
}

impl MetricNamespace {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Bucket => "rustfs_bucket",
            Self::Cluster => "rustfs_cluster",
            Self::Heal => "rustfs_heal",
            Self::InterNode => "rustfs_inter_node",
            Self::Node => "rustfs_node",
            Self::RustFS => "rustfs",
            Self::S3 => "rustfs_s3",
        }
    }
}
