use crate::metrics::entry::path_utils::format_path_to_metric_name;

/// The metrics subsystem is a subgroup of metrics within a namespace
/// 指标子系统，表示命名空间内指标的子分组
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MetricSubsystem {
    // API 相关子系统
    ApiRequests,

    // 桶相关子系统
    BucketApi,
    BucketReplication,

    // 系统相关子系统
    SystemNetworkInternode,
    SystemDrive,
    SystemMemory,
    SystemCpu,
    SystemProcess,

    // 调试相关子系统
    DebugGo,

    // 集群相关子系统
    ClusterHealth,
    ClusterUsageObjects,
    ClusterUsageBuckets,
    ClusterErasureSet,
    ClusterIam,
    ClusterConfig,

    // 其他服务相关子系统
    Ilm,
    Audit,
    LoggerWebhook,
    Replication,
    Notification,
    Scanner,

    // 自定义路径
    Custom(String),
}

impl MetricSubsystem {
    /// 获取原始路径字符串
    pub fn path(&self) -> &str {
        match self {
            // API 相关子系统
            Self::ApiRequests => "/api/requests",

            // 桶相关子系统
            Self::BucketApi => "/bucket/api",
            Self::BucketReplication => "/bucket/replication",

            // 系统相关子系统
            Self::SystemNetworkInternode => "/system/network/internode",
            Self::SystemDrive => "/system/drive",
            Self::SystemMemory => "/system/memory",
            Self::SystemCpu => "/system/cpu",
            Self::SystemProcess => "/system/process",

            // 调试相关子系统
            Self::DebugGo => "/debug/go",

            // 集群相关子系统
            Self::ClusterHealth => "/cluster/health",
            Self::ClusterUsageObjects => "/cluster/usage/objects",
            Self::ClusterUsageBuckets => "/cluster/usage/buckets",
            Self::ClusterErasureSet => "/cluster/erasure-set",
            Self::ClusterIam => "/cluster/iam",
            Self::ClusterConfig => "/cluster/config",

            // 其他服务相关子系统
            Self::Ilm => "/ilm",
            Self::Audit => "/audit",
            Self::LoggerWebhook => "/logger/webhook",
            Self::Replication => "/replication",
            Self::Notification => "/notification",
            Self::Scanner => "/scanner",

            // 自定义路径
            Self::Custom(path) => path,
        }
    }

    /// 获取格式化后的指标名称格式字符串
    pub fn as_str(&self) -> String {
        format_path_to_metric_name(self.path())
    }

    /// 从路径字符串创建子系统枚举
    pub fn from_path(path: &str) -> Self {
        match path {
            // API 相关子系统
            "/api/requests" => Self::ApiRequests,

            // 桶相关子系统
            "/bucket/api" => Self::BucketApi,
            "/bucket/replication" => Self::BucketReplication,

            // 系统相关子系统
            "/system/network/internode" => Self::SystemNetworkInternode,
            "/system/drive" => Self::SystemDrive,
            "/system/memory" => Self::SystemMemory,
            "/system/cpu" => Self::SystemCpu,
            "/system/process" => Self::SystemProcess,

            // 调试相关子系统
            "/debug/go" => Self::DebugGo,

            // 集群相关子系统
            "/cluster/health" => Self::ClusterHealth,
            "/cluster/usage/objects" => Self::ClusterUsageObjects,
            "/cluster/usage/buckets" => Self::ClusterUsageBuckets,
            "/cluster/erasure-set" => Self::ClusterErasureSet,
            "/cluster/iam" => Self::ClusterIam,
            "/cluster/config" => Self::ClusterConfig,

            // 其他服务相关子系统
            "/ilm" => Self::Ilm,
            "/audit" => Self::Audit,
            "/logger/webhook" => Self::LoggerWebhook,
            "/replication" => Self::Replication,
            "/notification" => Self::Notification,
            "/scanner" => Self::Scanner,

            // 其他路径作为自定义处理
            _ => Self::Custom(path.to_string()),
        }
    }

    // 便利方法，直接创建自定义子系统
    pub fn new(path: impl Into<String>) -> Self {
        Self::Custom(path.into())
    }
}

// 便于与字符串相互转换的实现
impl From<&str> for MetricSubsystem {
    fn from(s: &str) -> Self {
        Self::from_path(s)
    }
}

impl From<String> for MetricSubsystem {
    fn from(s: String) -> Self {
        Self::from_path(&s)
    }
}

impl std::fmt::Display for MetricSubsystem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.path())
    }
}

#[allow(dead_code)]
pub mod subsystems {
    use super::MetricSubsystem;

    // 集群基本路径常量
    pub const CLUSTER_BASE_PATH: &str = "/cluster";

    // 快捷访问各子系统的常量
    pub const API_REQUESTS: MetricSubsystem = MetricSubsystem::ApiRequests;
    pub const BUCKET_API: MetricSubsystem = MetricSubsystem::BucketApi;
    pub const BUCKET_REPLICATION: MetricSubsystem = MetricSubsystem::BucketReplication;
    pub const SYSTEM_NETWORK_INTERNODE: MetricSubsystem = MetricSubsystem::SystemNetworkInternode;
    pub const SYSTEM_DRIVE: MetricSubsystem = MetricSubsystem::SystemDrive;
    pub const SYSTEM_MEMORY: MetricSubsystem = MetricSubsystem::SystemMemory;
    pub const SYSTEM_CPU: MetricSubsystem = MetricSubsystem::SystemCpu;
    pub const SYSTEM_PROCESS: MetricSubsystem = MetricSubsystem::SystemProcess;
    pub const DEBUG_GO: MetricSubsystem = MetricSubsystem::DebugGo;
    pub const CLUSTER_HEALTH: MetricSubsystem = MetricSubsystem::ClusterHealth;
    pub const CLUSTER_USAGE_OBJECTS: MetricSubsystem = MetricSubsystem::ClusterUsageObjects;
    pub const CLUSTER_USAGE_BUCKETS: MetricSubsystem = MetricSubsystem::ClusterUsageBuckets;
    pub const CLUSTER_ERASURE_SET: MetricSubsystem = MetricSubsystem::ClusterErasureSet;
    pub const CLUSTER_IAM: MetricSubsystem = MetricSubsystem::ClusterIam;
    pub const CLUSTER_CONFIG: MetricSubsystem = MetricSubsystem::ClusterConfig;
    pub const ILM: MetricSubsystem = MetricSubsystem::Ilm;
    pub const AUDIT: MetricSubsystem = MetricSubsystem::Audit;
    pub const LOGGER_WEBHOOK: MetricSubsystem = MetricSubsystem::LoggerWebhook;
    pub const REPLICATION: MetricSubsystem = MetricSubsystem::Replication;
    pub const NOTIFICATION: MetricSubsystem = MetricSubsystem::Notification;
    pub const SCANNER: MetricSubsystem = MetricSubsystem::Scanner;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::MetricType;
    use crate::metrics::{MetricDescriptor, MetricName, MetricNamespace};

    #[test]
    fn test_metric_subsystem_formatting() {
        assert_eq!(MetricSubsystem::ApiRequests.as_str(), "api_requests");
        assert_eq!(MetricSubsystem::SystemNetworkInternode.as_str(), "system_network_internode");
        assert_eq!(MetricSubsystem::BucketApi.as_str(), "bucket_api");
        assert_eq!(MetricSubsystem::ClusterHealth.as_str(), "cluster_health");

        // 测试自定义路径
        let custom = MetricSubsystem::new("/custom/path-test");
        assert_eq!(custom.as_str(), "custom_path_test");
    }

    #[test]
    fn test_metric_descriptor_name_generation() {
        let md = MetricDescriptor::new(
            MetricName::ApiRequestsTotal,
            MetricType::Counter,
            "Test help".to_string(),
            vec!["label1".to_string(), "label2".to_string()],
            MetricNamespace::RustFS,
            MetricSubsystem::ApiRequests,
        );

        assert_eq!(md.get_full_metric_name(), "counter.rustfs_api_requests_total");

        let custom_md = MetricDescriptor::new(
            MetricName::Custom("test_metric".to_string()),
            MetricType::Gauge,
            "Test help".to_string(),
            vec!["label1".to_string()],
            MetricNamespace::RustFS,
            MetricSubsystem::new("/custom/path-with-dash"),
        );

        assert_eq!(custom_md.get_full_metric_name(), "gauge.rustfs_custom_path_with_dash_test_metric");
    }
}
