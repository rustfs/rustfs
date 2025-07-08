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

//! Policy system for AHM operations
//!
//! Defines configurable policies for:
//! - Scanning behavior and frequency
//! - Healing priorities and strategies
//! - Data retention and lifecycle management

pub mod scan_policy;
pub mod heal_policy;
pub mod retention_policy;

pub use scan_policy::{ScanPolicyConfig, ScanPolicyEngine};
pub use heal_policy::{HealPolicyConfig, HealPolicyEngine};
pub use retention_policy::{RetentionPolicyConfig, RetentionPolicyEngine};

use std::time::{Duration, SystemTime};
use serde::{Deserialize, Serialize};

use crate::scanner::{HealthIssue, Severity};

/// Policy evaluation result
#[derive(Debug, Clone)]
pub struct PolicyResult {
    /// Whether the policy allows the action
    pub allowed: bool,
    /// Reason for the decision
    pub reason: String,
    /// Additional metadata
    pub metadata: Option<serde_json::Value>,
    /// When the policy was evaluated
    pub evaluated_at: SystemTime,
}

/// Policy evaluation context
#[derive(Debug, Clone)]
pub struct PolicyContext {
    /// Current system load
    pub system_load: f64,
    /// Available disk space percentage
    pub disk_space_available: f64,
    /// Number of active operations
    pub active_operations: u64,
    /// Current time
    pub current_time: SystemTime,
    /// Health issues count by severity
    pub health_issues: std::collections::HashMap<Severity, u64>,
    /// Resource usage metrics
    pub resource_usage: ResourceUsage,
}

/// Resource usage information
#[derive(Debug, Clone)]
pub struct ResourceUsage {
    /// CPU usage percentage
    pub cpu_usage: f64,
    /// Memory usage percentage
    pub memory_usage: f64,
    /// Disk I/O usage percentage
    pub disk_io_usage: f64,
    /// Network I/O usage percentage
    pub network_io_usage: f64,
}

impl Default for ResourceUsage {
    fn default() -> Self {
        Self {
            cpu_usage: 0.0,
            memory_usage: 0.0,
            disk_io_usage: 0.0,
            network_io_usage: 0.0,
        }
    }
}

/// Policy manager that coordinates all policies
pub struct PolicyManager {
    scan_policy: ScanPolicyEngine,
    heal_policy: HealPolicyEngine,
    retention_policy: RetentionPolicyEngine,
}

impl PolicyManager {
    /// Create a new policy manager
    pub fn new(
        scan_config: ScanPolicyConfig,
        heal_config: HealPolicyConfig,
        retention_config: RetentionPolicyConfig,
    ) -> Self {
        Self {
            scan_policy: ScanPolicyEngine::new(scan_config),
            heal_policy: HealPolicyEngine::new(heal_config),
            retention_policy: RetentionPolicyEngine::new(retention_config),
        }
    }

    /// Evaluate scan policy
    pub async fn evaluate_scan_policy(&self, context: &PolicyContext) -> PolicyResult {
        self.scan_policy.evaluate(context).await
    }

    /// Evaluate heal policy
    pub async fn evaluate_heal_policy(&self, issue: &HealthIssue, context: &PolicyContext) -> PolicyResult {
        self.heal_policy.evaluate(issue, context).await
    }

    /// Evaluate retention policy
    pub async fn evaluate_retention_policy(&self, object_age: Duration, context: &PolicyContext) -> PolicyResult {
        self.retention_policy.evaluate(object_age, context).await
    }

    /// Get scan policy engine
    pub fn scan_policy(&self) -> &ScanPolicyEngine {
        &self.scan_policy
    }

    /// Get heal policy engine
    pub fn heal_policy(&self) -> &HealPolicyEngine {
        &self.heal_policy
    }

    /// Get retention policy engine
    pub fn retention_policy(&self) -> &RetentionPolicyEngine {
        &self.retention_policy
    }

    /// Update scan policy configuration
    pub async fn update_scan_policy(&mut self, config: ScanPolicyConfig) {
        self.scan_policy = ScanPolicyEngine::new(config);
    }

    /// Update heal policy configuration
    pub async fn update_heal_policy(&mut self, config: HealPolicyConfig) {
        self.heal_policy = HealPolicyEngine::new(config);
    }

    /// Update retention policy configuration
    pub async fn update_retention_policy(&mut self, config: RetentionPolicyConfig) {
        self.retention_policy = RetentionPolicyEngine::new(config);
    }

    /// List all policies
    pub async fn list_policies(&self) -> crate::error::Result<Vec<String>> {
        // In a real implementation, this would return actual policy names
        Ok(vec![
            "scan_policy".to_string(),
            "heal_policy".to_string(),
            "retention_policy".to_string(),
        ])
    }

    /// Get a specific policy
    pub async fn get_policy(&self, name: &str) -> crate::error::Result<String> {
        // In a real implementation, this would return the actual policy
        Ok(format!("Policy configuration for: {}", name))
    }

    /// Get engine configuration
    pub async fn get_config(&self) -> PolicyConfig {
        PolicyConfig::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scanner::{HealthIssue, HealthIssueType};

    #[tokio::test]
    async fn test_policy_manager_creation() {
        let scan_config = ScanPolicyConfig::default();
        let heal_config = HealPolicyConfig::default();
        let retention_config = RetentionPolicyConfig::default();

        let manager = PolicyManager::new(scan_config, heal_config, retention_config);
        
        // Test that all policy engines are available
        assert!(manager.scan_policy().config().max_concurrent_scans > 0);
        assert!(manager.heal_policy().config().max_concurrent_repairs > 0);
        assert!(manager.retention_policy().config().default_retention_days > 0);
    }

    #[tokio::test]
    async fn test_policy_evaluation() {
        let scan_config = ScanPolicyConfig::default();
        let heal_config = HealPolicyConfig::default();
        let retention_config = RetentionPolicyConfig::default();

        let manager = PolicyManager::new(scan_config, heal_config, retention_config);
        
        let context = PolicyContext {
            system_load: 0.5,
            disk_space_available: 80.0,
            active_operations: 10,
            current_time: SystemTime::now(),
            health_issues: std::collections::HashMap::new(),
            resource_usage: ResourceUsage::default(),
        };

        // Test scan policy evaluation
        let scan_result = manager.evaluate_scan_policy(&context).await;
        assert!(scan_result.allowed);

        // Test heal policy evaluation
        let issue = HealthIssue {
            issue_type: HealthIssueType::MissingReplica,
            severity: Severity::Critical,
            bucket: "test-bucket".to_string(),
            object: "test-object".to_string(),
            description: "Test issue".to_string(),
            metadata: None,
        };

        let heal_result = manager.evaluate_heal_policy(&issue, &context).await;
        assert!(heal_result.allowed);

        // Test retention policy evaluation
        let retention_result = manager.evaluate_retention_policy(Duration::from_secs(86400), &context).await;
        assert!(retention_result.allowed);
    }
}

/// Master policy configuration
#[derive(Debug, Clone)]
pub struct PolicyConfig {
    pub scan: ScanPolicyConfig,
    pub heal: HealPolicyConfig,
    pub retention: RetentionPolicyConfig,
}

impl Default for PolicyConfig {
    fn default() -> Self {
        Self {
            scan: ScanPolicyConfig::default(),
            heal: HealPolicyConfig::default(),
            retention: RetentionPolicyConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PolicyManagerConfig {
    #[serde(default)]
    pub default_scan_interval: Duration,
} 