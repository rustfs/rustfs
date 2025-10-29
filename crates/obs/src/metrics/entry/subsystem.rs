// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::metrics::entry::path_utils::format_path_to_metric_name;

/// The metrics subsystem is a subgroup of metrics within a namespace
/// The metrics subsystem, which represents a subgroup of metrics within a namespace
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MetricSubsystem {
    // API related subsystems
    ApiRequests,

    // bucket related subsystems
    BucketApi,
    BucketReplication,

    // system related subsystems
    SystemNetworkInternode,
    SystemDrive,
    SystemMemory,
    SystemCpu,
    SystemProcess,

    // debug related subsystems
    DebugGo,

    // cluster related subsystems
    ClusterHealth,
    ClusterUsageObjects,
    ClusterUsageBuckets,
    ClusterErasureSet,
    ClusterIam,
    ClusterConfig,

    // other service related subsystems
    Ilm,
    Audit,
    LoggerWebhook,
    Replication,
    Notification,
    Scanner,

    // Custom paths
    Custom(String),
}

impl MetricSubsystem {
    /// Gets the original path string
    pub fn path(&self) -> &str {
        match self {
            // api related subsystems
            Self::ApiRequests => "/api/requests",

            // bucket related subsystems
            Self::BucketApi => "/bucket/api",
            Self::BucketReplication => "/bucket/replication",

            // system related subsystems
            Self::SystemNetworkInternode => "/system/network/internode",
            Self::SystemDrive => "/system/drive",
            Self::SystemMemory => "/system/memory",
            Self::SystemCpu => "/system/cpu",
            Self::SystemProcess => "/system/process",

            // debug related subsystems
            Self::DebugGo => "/debug/go",

            // cluster related subsystems
            Self::ClusterHealth => "/cluster/health",
            Self::ClusterUsageObjects => "/cluster/usage/objects",
            Self::ClusterUsageBuckets => "/cluster/usage/buckets",
            Self::ClusterErasureSet => "/cluster/erasure-set",
            Self::ClusterIam => "/cluster/iam",
            Self::ClusterConfig => "/cluster/config",

            // other service related subsystems
            Self::Ilm => "/ilm",
            Self::Audit => "/audit",
            Self::LoggerWebhook => "/logger/webhook",
            Self::Replication => "/replication",
            Self::Notification => "/notification",
            Self::Scanner => "/scanner",

            // Custom paths
            Self::Custom(path) => path,
        }
    }

    /// Get the formatted metric name format string
    #[allow(dead_code)]
    pub fn as_str(&self) -> String {
        format_path_to_metric_name(self.path())
    }

    /// Create a subsystem enumeration from a path string
    pub fn from_path(path: &str) -> Self {
        match path {
            // API-related subsystems
            "/api/requests" => Self::ApiRequests,

            // Bucket-related subsystems
            "/bucket/api" => Self::BucketApi,
            "/bucket/replication" => Self::BucketReplication,

            // System-related subsystems
            "/system/network/internode" => Self::SystemNetworkInternode,
            "/system/drive" => Self::SystemDrive,
            "/system/memory" => Self::SystemMemory,
            "/system/cpu" => Self::SystemCpu,
            "/system/process" => Self::SystemProcess,

            // Debug related subsystems
            "/debug/go" => Self::DebugGo,

            // Cluster-related subsystems
            "/cluster/health" => Self::ClusterHealth,
            "/cluster/usage/objects" => Self::ClusterUsageObjects,
            "/cluster/usage/buckets" => Self::ClusterUsageBuckets,
            "/cluster/erasure-set" => Self::ClusterErasureSet,
            "/cluster/iam" => Self::ClusterIam,
            "/cluster/config" => Self::ClusterConfig,

            // Other service-related subsystems
            "/ilm" => Self::Ilm,
            "/audit" => Self::Audit,
            "/logger/webhook" => Self::LoggerWebhook,
            "/replication" => Self::Replication,
            "/notification" => Self::Notification,
            "/scanner" => Self::Scanner,

            // Treat other paths as custom subsystems
            _ => Self::Custom(path.to_string()),
        }
    }

    /// A convenient way to create custom subsystems directly
    #[allow(dead_code)]
    pub fn new(path: impl Into<String>) -> Self {
        Self::Custom(path.into())
    }
}

/// Implementations that facilitate conversion to and from strings
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

    // cluster base path constant
    pub const CLUSTER_BASE_PATH: &str = "/cluster";

    // Quick access to constants for each subsystem
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

        // Test custom paths
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
