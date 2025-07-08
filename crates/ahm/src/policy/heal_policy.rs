// Copyright 2024 RustFS Team

use std::time::{Duration, SystemTime};

use crate::scanner::{HealthIssue, Severity};

use super::{PolicyContext, PolicyResult, ResourceUsage};

/// Configuration for heal policies
#[derive(Debug, Clone)]
pub struct HealPolicyConfig {
    /// Maximum number of concurrent repairs
    pub max_concurrent_repairs: usize,
    /// Maximum repair duration per operation
    pub max_repair_duration: Duration,
    /// Minimum interval between repairs
    pub min_repair_interval: Duration,
    /// Maximum system load threshold for healing
    pub max_system_load: f64,
    /// Minimum available disk space percentage for healing
    pub min_disk_space: f64,
    /// Maximum number of active operations for healing
    pub max_active_operations: u64,
    /// Whether to enable automatic healing
    pub auto_heal_enabled: bool,
    /// Priority-based healing configuration
    pub priority_config: HealPriorityConfig,
    /// Resource-based healing configuration
    pub resource_config: HealResourceConfig,
    /// Retry configuration
    pub retry_config: HealRetryConfig,
}

/// Priority-based healing configuration
#[derive(Debug, Clone)]
pub struct HealPriorityConfig {
    /// Whether to enable priority-based healing
    pub enabled: bool,
    /// Critical issues heal immediately
    pub critical_immediate: bool,
    /// High priority issues heal within
    pub high_timeout: Duration,
    /// Medium priority issues heal within
    pub medium_timeout: Duration,
    /// Low priority issues heal within
    pub low_timeout: Duration,
}

/// Resource-based healing configuration
#[derive(Debug, Clone)]
pub struct HealResourceConfig {
    /// Maximum CPU usage for healing
    pub max_cpu_usage: f64,
    /// Maximum memory usage for healing
    pub max_memory_usage: f64,
    /// Maximum disk I/O usage for healing
    pub max_disk_io_usage: f64,
    /// Maximum network I/O usage for healing
    pub max_network_io_usage: f64,
    /// Whether to enable resource-based throttling
    pub enable_throttling: bool,
}

/// Retry configuration for healing
#[derive(Debug, Clone)]
pub struct HealRetryConfig {
    /// Maximum number of retry attempts
    pub max_retry_attempts: u32,
    /// Initial backoff delay
    pub initial_backoff: Duration,
    /// Maximum backoff delay
    pub max_backoff: Duration,
    /// Backoff multiplier
    pub backoff_multiplier: f64,
    /// Whether to use exponential backoff
    pub exponential_backoff: bool,
}

impl Default for HealPolicyConfig {
    fn default() -> Self {
        Self {
            max_concurrent_repairs: 4,
            max_repair_duration: Duration::from_secs(1800), // 30 minutes
            min_repair_interval: Duration::from_secs(60),   // 1 minute
            max_system_load: 0.7,
            min_disk_space: 15.0, // 15% minimum disk space
            max_active_operations: 50,
            auto_heal_enabled: true,
            priority_config: HealPriorityConfig::default(),
            resource_config: HealResourceConfig::default(),
            retry_config: HealRetryConfig::default(),
        }
    }
}

impl Default for HealPriorityConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            critical_immediate: true,
            high_timeout: Duration::from_secs(300),   // 5 minutes
            medium_timeout: Duration::from_secs(1800), // 30 minutes
            low_timeout: Duration::from_secs(3600),    // 1 hour
        }
    }
}

impl Default for HealResourceConfig {
    fn default() -> Self {
        Self {
            max_cpu_usage: 80.0,
            max_memory_usage: 80.0,
            max_disk_io_usage: 70.0,
            max_network_io_usage: 70.0,
            enable_throttling: true,
        }
    }
}

impl Default for HealRetryConfig {
    fn default() -> Self {
        Self {
            max_retry_attempts: 3,
            initial_backoff: Duration::from_secs(30),
            max_backoff: Duration::from_secs(300),
            backoff_multiplier: 2.0,
            exponential_backoff: true,
        }
    }
}

/// Heal policy engine
pub struct HealPolicyEngine {
    config: HealPolicyConfig,
    last_repair_time: SystemTime,
    repair_count: u64,
    active_repairs: u64,
}

impl HealPolicyEngine {
    /// Create a new heal policy engine
    pub fn new(config: HealPolicyConfig) -> Self {
        Self {
            config,
            last_repair_time: SystemTime::now(),
            repair_count: 0,
            active_repairs: 0,
        }
    }

    /// Get the configuration
    pub fn config(&self) -> &HealPolicyConfig {
        &self.config
    }

    /// Evaluate heal policy
    pub async fn evaluate(&self, issue: &HealthIssue, context: &PolicyContext) -> PolicyResult {
        let mut reasons = Vec::new();
        let mut allowed = true;

        // Check if auto-heal is enabled
        if !self.config.auto_heal_enabled {
            allowed = false;
            reasons.push("Auto-heal is disabled".to_string());
        }

        // Check system load
        if context.system_load > self.config.max_system_load {
            allowed = false;
            reasons.push(format!(
                "System load too high: {:.2} > {:.2}",
                context.system_load, self.config.max_system_load
            ));
        }

        // Check disk space
        if context.disk_space_available < self.config.min_disk_space {
            allowed = false;
            reasons.push(format!(
                "Disk space too low: {:.1}% < {:.1}%",
                context.disk_space_available, self.config.min_disk_space
            ));
        }

        // Check active operations
        if context.active_operations > self.config.max_active_operations {
            allowed = false;
            reasons.push(format!(
                "Too many active operations: {} > {}",
                context.active_operations, self.config.max_active_operations
            ));
        }

        // Check repair interval
        let time_since_last_repair = context.current_time
            .duration_since(self.last_repair_time)
            .unwrap_or(Duration::ZERO);

        if time_since_last_repair < self.config.min_repair_interval {
            allowed = false;
            reasons.push(format!(
                "Repair interval too short: {:?} < {:?}",
                time_since_last_repair, self.config.min_repair_interval
            ));
        }

        // Check resource usage
        if self.config.resource_config.enable_throttling {
            if context.resource_usage.cpu_usage > self.config.resource_config.max_cpu_usage {
                allowed = false;
                reasons.push(format!(
                    "CPU usage too high: {:.1}% > {:.1}%",
                    context.resource_usage.cpu_usage, self.config.resource_config.max_cpu_usage
                ));
            }

            if context.resource_usage.memory_usage > self.config.resource_config.max_memory_usage {
                allowed = false;
                reasons.push(format!(
                    "Memory usage too high: {:.1}% > {:.1}%",
                    context.resource_usage.memory_usage, self.config.resource_config.max_memory_usage
                ));
            }

            if context.resource_usage.disk_io_usage > self.config.resource_config.max_disk_io_usage {
                allowed = false;
                reasons.push(format!(
                    "Disk I/O usage too high: {:.1}% > {:.1}%",
                    context.resource_usage.disk_io_usage, self.config.resource_config.max_disk_io_usage
                ));
            }

            if context.resource_usage.network_io_usage > self.config.resource_config.max_network_io_usage {
                allowed = false;
                reasons.push(format!(
                    "Network I/O usage too high: {:.1}% > {:.1}%",
                    context.resource_usage.network_io_usage, self.config.resource_config.max_network_io_usage
                ));
            }
        }

        // Check priority-based policies
        if self.config.priority_config.enabled {
            match issue.severity {
                Severity::Critical => {
                    if self.config.priority_config.critical_immediate {
                        // Critical issues should always be allowed unless resource constraints prevent it
                        if allowed {
                            reasons.clear();
                            reasons.push("Critical issue - immediate repair allowed".to_string());
                        }
                    }
                }
                Severity::High => {
                    // Check if we're within the high priority timeout
                    if time_since_last_repair > self.config.priority_config.high_timeout {
                        allowed = false;
                        reasons.push(format!(
                            "High priority issue timeout exceeded: {:?} > {:?}",
                            time_since_last_repair, self.config.priority_config.high_timeout
                        ));
                    }
                }
                Severity::Medium => {
                    // Check if we're within the medium priority timeout
                    if time_since_last_repair > self.config.priority_config.medium_timeout {
                        allowed = false;
                        reasons.push(format!(
                            "Medium priority issue timeout exceeded: {:?} > {:?}",
                            time_since_last_repair, self.config.priority_config.medium_timeout
                        ));
                    }
                }
                Severity::Low => {
                    // Check if we're within the low priority timeout
                    if time_since_last_repair > self.config.priority_config.low_timeout {
                        allowed = false;
                        reasons.push(format!(
                            "Low priority issue timeout exceeded: {:?} > {:?}",
                            time_since_last_repair, self.config.priority_config.low_timeout
                        ));
                    }
                }
            }
        }

        let reason = if reasons.is_empty() {
            "Heal allowed".to_string()
        } else {
            reasons.join("; ")
        };

        PolicyResult {
            allowed,
            reason,
            metadata: Some(serde_json::json!({
                "repair_count": self.repair_count,
                "active_repairs": self.active_repairs,
                "time_since_last_repair": time_since_last_repair.as_secs(),
                "issue_severity": format!("{:?}", issue.severity),
                "issue_type": format!("{:?}", issue.issue_type),
                "system_load": context.system_load,
                "disk_space_available": context.disk_space_available,
                "active_operations": context.active_operations,
            })),
            evaluated_at: context.current_time,
        }
    }

    /// Get repair timeout based on priority
    pub fn get_repair_timeout(&self, severity: Severity) -> Duration {
        if !self.config.priority_config.enabled {
            return self.config.max_repair_duration;
        }

        match severity {
            Severity::Critical => Duration::from_secs(300), // 5 minutes for critical
            Severity::High => self.config.priority_config.high_timeout,
            Severity::Medium => self.config.priority_config.medium_timeout,
            Severity::Low => self.config.priority_config.low_timeout,
        }
    }

    /// Get retry configuration
    pub fn get_retry_config(&self) -> &HealRetryConfig {
        &self.config.retry_config
    }

    /// Update repair statistics
    pub fn record_repair(&mut self) {
        self.last_repair_time = SystemTime::now();
        self.repair_count += 1;
    }

    /// Increment active repairs
    pub fn increment_active_repairs(&mut self) {
        self.active_repairs += 1;
    }

    /// Decrement active repairs
    pub fn decrement_active_repairs(&mut self) {
        if self.active_repairs > 0 {
            self.active_repairs -= 1;
        }
    }

    /// Get heal statistics
    pub fn get_statistics(&self) -> HealPolicyStatistics {
        HealPolicyStatistics {
            total_repairs: self.repair_count,
            active_repairs: self.active_repairs,
            last_repair_time: self.last_repair_time,
            config: self.config.clone(),
        }
    }
}

/// Heal policy statistics
#[derive(Debug, Clone)]
pub struct HealPolicyStatistics {
    pub total_repairs: u64,
    pub active_repairs: u64,
    pub last_repair_time: SystemTime,
    pub config: HealPolicyConfig,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scanner::{HealthIssue, HealthIssueType, Severity};

    #[tokio::test]
    async fn test_heal_policy_creation() {
        let config = HealPolicyConfig::default();
        let engine = HealPolicyEngine::new(config);
        
        assert_eq!(engine.config().max_concurrent_repairs, 4);
        assert_eq!(engine.config().max_system_load, 0.7);
        assert_eq!(engine.config().min_disk_space, 15.0);
    }

    #[tokio::test]
    async fn test_heal_policy_evaluation() {
        let config = HealPolicyConfig::default();
        let engine = HealPolicyEngine::new(config);
        
        let issue = HealthIssue {
            issue_type: HealthIssueType::MissingReplica,
            severity: Severity::Medium,
            bucket: "test-bucket".to_string(),
            object: "test-object".to_string(),
            description: "Test issue".to_string(),
            metadata: None,
        };

        let context = PolicyContext {
            system_load: 0.5,
            disk_space_available: 80.0,
            active_operations: 10,
            current_time: SystemTime::now(),
            health_issues: std::collections::HashMap::new(),
            resource_usage: ResourceUsage::default(),
        };

        let result = engine.evaluate(&issue, &context).await;
        assert!(result.allowed);
        assert!(result.reason.contains("Heal allowed"));
    }

    #[tokio::test]
    async fn test_heal_policy_critical_immediate() {
        let config = HealPolicyConfig::default();
        let engine = HealPolicyEngine::new(config);
        
        let issue = HealthIssue {
            issue_type: HealthIssueType::MissingReplica,
            severity: Severity::Critical,
            bucket: "test-bucket".to_string(),
            object: "test-object".to_string(),
            description: "Test issue".to_string(),
            metadata: None,
        };

        let context = PolicyContext {
            system_load: 0.5,
            disk_space_available: 80.0,
            active_operations: 10,
            current_time: SystemTime::now(),
            health_issues: std::collections::HashMap::new(),
            resource_usage: ResourceUsage::default(),
        };

        let result = engine.evaluate(&issue, &context).await;
        assert!(result.allowed);
        assert!(result.reason.contains("Critical issue - immediate repair allowed"));
    }

    #[tokio::test]
    async fn test_heal_policy_system_load_limit() {
        let config = HealPolicyConfig::default();
        let engine = HealPolicyEngine::new(config);
        
        let issue = HealthIssue {
            issue_type: HealthIssueType::MissingReplica,
            severity: Severity::Medium,
            bucket: "test-bucket".to_string(),
            object: "test-object".to_string(),
            description: "Test issue".to_string(),
            metadata: None,
        };

        let context = PolicyContext {
            system_load: 0.8, // Above threshold
            disk_space_available: 80.0,
            active_operations: 10,
            current_time: SystemTime::now(),
            health_issues: std::collections::HashMap::new(),
            resource_usage: ResourceUsage::default(),
        };

        let result = engine.evaluate(&issue, &context).await;
        assert!(!result.allowed);
        assert!(result.reason.contains("System load too high"));
    }

    #[tokio::test]
    async fn test_repair_timeouts() {
        let config = HealPolicyConfig::default();
        let engine = HealPolicyEngine::new(config);
        
        assert_eq!(
            engine.get_repair_timeout(Severity::Critical),
            Duration::from_secs(300)
        );
        assert_eq!(
            engine.get_repair_timeout(Severity::High),
            Duration::from_secs(300)
        );
        assert_eq!(
            engine.get_repair_timeout(Severity::Medium),
            Duration::from_secs(1800)
        );
        assert_eq!(
            engine.get_repair_timeout(Severity::Low),
            Duration::from_secs(3600)
        );
    }

    #[tokio::test]
    async fn test_heal_statistics() {
        let config = HealPolicyConfig::default();
        let mut engine = HealPolicyEngine::new(config);
        
        assert_eq!(engine.get_statistics().total_repairs, 0);
        assert_eq!(engine.get_statistics().active_repairs, 0);
        
        engine.record_repair();
        engine.increment_active_repairs();
        engine.increment_active_repairs();
        
        let stats = engine.get_statistics();
        assert_eq!(stats.total_repairs, 1);
        assert_eq!(stats.active_repairs, 2);
        
        engine.decrement_active_repairs();
        assert_eq!(engine.get_statistics().active_repairs, 1);
    }
} 