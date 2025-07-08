// Copyright 2024 RustFS Team

use std::time::{Duration, SystemTime};

use super::{PolicyContext, PolicyResult, ResourceUsage};

/// Configuration for retention policies
#[derive(Debug, Clone)]
pub struct RetentionPolicyConfig {
    /// Default retention period in days
    pub default_retention_days: u32,
    /// Whether to enable retention policies
    pub enabled: bool,
    /// Maximum system load threshold for retention operations
    pub max_system_load: f64,
    /// Minimum available disk space percentage for retention operations
    pub min_disk_space: f64,
    /// Maximum number of active operations for retention
    pub max_active_operations: u64,
    /// Retention rules by object type
    pub retention_rules: Vec<RetentionRule>,
    /// Whether to enable automatic cleanup
    pub auto_cleanup_enabled: bool,
    /// Cleanup interval
    pub cleanup_interval: Duration,
    /// Maximum objects to delete per cleanup cycle
    pub max_objects_per_cleanup: u64,
}

/// Retention rule for specific object types
#[derive(Debug, Clone)]
pub struct RetentionRule {
    /// Object type pattern (e.g., "*.log", "temp/*")
    pub pattern: String,
    /// Retention period in days
    pub retention_days: u32,
    /// Whether this rule is enabled
    pub enabled: bool,
    /// Priority of this rule (higher = more important)
    pub priority: u32,
    /// Whether to apply this rule recursively
    pub recursive: bool,
}

impl Default for RetentionPolicyConfig {
    fn default() -> Self {
        Self {
            default_retention_days: 30,
            enabled: true,
            max_system_load: 0.6,
            min_disk_space: 20.0, // 20% minimum disk space
            max_active_operations: 20,
            retention_rules: vec![
                RetentionRule {
                    pattern: "*.log".to_string(),
                    retention_days: 7,
                    enabled: true,
                    priority: 1,
                    recursive: false,
                },
                RetentionRule {
                    pattern: "temp/*".to_string(),
                    retention_days: 1,
                    enabled: true,
                    priority: 2,
                    recursive: true,
                },
                RetentionRule {
                    pattern: "cache/*".to_string(),
                    retention_days: 3,
                    enabled: true,
                    priority: 3,
                    recursive: true,
                },
            ],
            auto_cleanup_enabled: true,
            cleanup_interval: Duration::from_secs(3600), // 1 hour
            max_objects_per_cleanup: 1000,
        }
    }
}

/// Retention policy engine
pub struct RetentionPolicyEngine {
    config: RetentionPolicyConfig,
    last_cleanup_time: SystemTime,
    cleanup_count: u64,
    objects_deleted: u64,
}

impl RetentionPolicyEngine {
    /// Create a new retention policy engine
    pub fn new(config: RetentionPolicyConfig) -> Self {
        Self {
            config,
            last_cleanup_time: SystemTime::now(),
            cleanup_count: 0,
            objects_deleted: 0,
        }
    }

    /// Get the configuration
    pub fn config(&self) -> &RetentionPolicyConfig {
        &self.config
    }

    /// Evaluate retention policy
    pub async fn evaluate(&self, object_age: Duration, context: &PolicyContext) -> PolicyResult {
        let mut reasons = Vec::new();
        let mut allowed = false;

        // Check if retention policies are enabled
        if !self.config.enabled {
            allowed = false;
            reasons.push("Retention policies are disabled".to_string());
        } else {
            // Check if object should be retained based on age
            let retention_days = self.get_retention_days_for_object("default");
            let retention_duration = Duration::from_secs(retention_days as u64 * 24 * 3600);
            
            if object_age > retention_duration {
                allowed = true;
                reasons.push(format!(
                    "Object age exceeds retention period: {:?} > {:?}",
                    object_age, retention_duration
                ));
            } else {
                allowed = false;
                reasons.push(format!(
                    "Object within retention period: {:?} <= {:?}",
                    object_age, retention_duration
                ));
            }
        }

        // Check system constraints
        if context.system_load > self.config.max_system_load {
            allowed = false;
            reasons.push(format!(
                "System load too high: {:.2} > {:.2}",
                context.system_load, self.config.max_system_load
            ));
        }

        if context.disk_space_available < self.config.min_disk_space {
            allowed = false;
            reasons.push(format!(
                "Disk space too low: {:.1}% < {:.1}%",
                context.disk_space_available, self.config.min_disk_space
            ));
        }

        if context.active_operations > self.config.max_active_operations {
            allowed = false;
            reasons.push(format!(
                "Too many active operations: {} > {}",
                context.active_operations, self.config.max_active_operations
            ));
        }

        let reason = if reasons.is_empty() {
            "Retention evaluation completed".to_string()
        } else {
            reasons.join("; ")
        };

        PolicyResult {
            allowed,
            reason,
            metadata: Some(serde_json::json!({
                "object_age_seconds": object_age.as_secs(),
                "cleanup_count": self.cleanup_count,
                "objects_deleted": self.objects_deleted,
                "system_load": context.system_load,
                "disk_space_available": context.disk_space_available,
                "active_operations": context.active_operations,
            })),
            evaluated_at: context.current_time,
        }
    }

    /// Evaluate cleanup policy
    pub async fn evaluate_cleanup(&self, context: &PolicyContext) -> PolicyResult {
        let mut reasons = Vec::new();
        let mut allowed = false;

        // Check if auto-cleanup is enabled
        if !self.config.auto_cleanup_enabled {
            allowed = false;
            reasons.push("Auto-cleanup is disabled".to_string());
        } else {
            // Check cleanup interval
            let time_since_last_cleanup = context.current_time
                .duration_since(self.last_cleanup_time)
                .unwrap_or(Duration::ZERO);

            if time_since_last_cleanup >= self.config.cleanup_interval {
                allowed = true;
                reasons.push("Cleanup interval reached".to_string());
            } else {
                allowed = false;
                reasons.push(format!(
                    "Cleanup interval not reached: {:?} < {:?}",
                    time_since_last_cleanup, self.config.cleanup_interval
                ));
            }
        }

        // Check system constraints
        if context.system_load > self.config.max_system_load {
            allowed = false;
            reasons.push(format!(
                "System load too high: {:.2} > {:.2}",
                context.system_load, self.config.max_system_load
            ));
        }

        if context.disk_space_available < self.config.min_disk_space {
            allowed = false;
            reasons.push(format!(
                "Disk space too low: {:.1}% < {:.1}%",
                context.disk_space_available, self.config.min_disk_space
            ));
        }

        let reason = if reasons.is_empty() {
            "Cleanup evaluation completed".to_string()
        } else {
            reasons.join("; ")
        };

        PolicyResult {
            allowed,
            reason,
            metadata: Some(serde_json::json!({
                "cleanup_count": self.cleanup_count,
                "objects_deleted": self.objects_deleted,
                "max_objects_per_cleanup": self.config.max_objects_per_cleanup,
                "system_load": context.system_load,
                "disk_space_available": context.disk_space_available,
            })),
            evaluated_at: context.current_time,
        }
    }

    /// Get retention days for a specific object
    pub fn get_retention_days_for_object(&self, object_path: &str) -> u32 {
        // Find the highest priority matching rule
        let mut best_rule: Option<&RetentionRule> = None;
        let mut best_priority = 0;

        for rule in &self.config.retention_rules {
            if !rule.enabled {
                continue;
            }

            if self.matches_pattern(object_path, &rule.pattern) {
                if rule.priority > best_priority {
                    best_rule = Some(rule);
                    best_priority = rule.priority;
                }
            }
        }

        best_rule
            .map(|rule| rule.retention_days)
            .unwrap_or(self.config.default_retention_days)
    }

    /// Check if an object path matches a pattern
    fn matches_pattern(&self, object_path: &str, pattern: &str) -> bool {
        // Simple pattern matching - can be enhanced with regex
        if pattern.contains('*') {
            // Wildcard matching
            let pattern_parts: Vec<&str> = pattern.split('*').collect();
            if pattern_parts.len() == 2 {
                let prefix = pattern_parts[0];
                let suffix = pattern_parts[1];
                object_path.starts_with(prefix) && object_path.ends_with(suffix)
            } else {
                false
            }
        } else {
            // Exact match
            object_path == pattern
        }
    }

    /// Get all retention rules
    pub fn get_retention_rules(&self) -> &[RetentionRule] {
        &self.config.retention_rules
    }

    /// Add a new retention rule
    pub fn add_retention_rule(&mut self, rule: RetentionRule) {
        self.config.retention_rules.push(rule);
    }

    /// Remove a retention rule by pattern
    pub fn remove_retention_rule(&mut self, pattern: &str) -> bool {
        let initial_len = self.config.retention_rules.len();
        self.config.retention_rules.retain(|rule| rule.pattern != pattern);
        self.config.retention_rules.len() < initial_len
    }

    /// Update cleanup statistics
    pub fn record_cleanup(&mut self, objects_deleted: u64) {
        self.last_cleanup_time = SystemTime::now();
        self.cleanup_count += 1;
        self.objects_deleted += objects_deleted;
    }

    /// Get retention statistics
    pub fn get_statistics(&self) -> RetentionPolicyStatistics {
        RetentionPolicyStatistics {
            total_cleanups: self.cleanup_count,
            total_objects_deleted: self.objects_deleted,
            last_cleanup_time: self.last_cleanup_time,
            config: self.config.clone(),
        }
    }
}

/// Retention policy statistics
#[derive(Debug, Clone)]
pub struct RetentionPolicyStatistics {
    pub total_cleanups: u64,
    pub total_objects_deleted: u64,
    pub last_cleanup_time: SystemTime,
    pub config: RetentionPolicyConfig,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_retention_policy_creation() {
        let config = RetentionPolicyConfig::default();
        let engine = RetentionPolicyEngine::new(config);
        
        assert_eq!(engine.config().default_retention_days, 30);
        assert_eq!(engine.config().max_system_load, 0.6);
        assert_eq!(engine.config().min_disk_space, 20.0);
    }

    #[tokio::test]
    async fn test_retention_policy_evaluation() {
        let config = RetentionPolicyConfig::default();
        let engine = RetentionPolicyEngine::new(config);
        
        let context = PolicyContext {
            system_load: 0.5,
            disk_space_available: 80.0,
            active_operations: 10,
            current_time: SystemTime::now(),
            health_issues: std::collections::HashMap::new(),
            resource_usage: ResourceUsage::default(),
        };

        // Test object within retention period
        let object_age = Duration::from_secs(7 * 24 * 3600); // 7 days
        let result = engine.evaluate(object_age, &context).await;
        assert!(!result.allowed);
        assert!(result.reason.contains("Object within retention period"));

        // Test object exceeding retention period
        let object_age = Duration::from_secs(40 * 24 * 3600); // 40 days
        let result = engine.evaluate(object_age, &context).await;
        assert!(result.allowed);
        assert!(result.reason.contains("Object age exceeds retention period"));
    }

    #[tokio::test]
    async fn test_retention_policy_system_constraints() {
        let config = RetentionPolicyConfig::default();
        let engine = RetentionPolicyEngine::new(config);
        
        let context = PolicyContext {
            system_load: 0.7, // Above threshold
            disk_space_available: 80.0,
            active_operations: 10,
            current_time: SystemTime::now(),
            health_issues: std::collections::HashMap::new(),
            resource_usage: ResourceUsage::default(),
        };

        let object_age = Duration::from_secs(40 * 24 * 3600); // 40 days
        let result = engine.evaluate(object_age, &context).await;
        assert!(!result.allowed);
        assert!(result.reason.contains("System load too high"));
    }

    #[tokio::test]
    async fn test_retention_rules() {
        let config = RetentionPolicyConfig::default();
        let engine = RetentionPolicyEngine::new(config);
        
        // Test default retention
        assert_eq!(engine.get_retention_days_for_object("unknown.txt"), 30);
        
        // Test log file retention
        assert_eq!(engine.get_retention_days_for_object("app.log"), 7);
        
        // Test temp file retention
        assert_eq!(engine.get_retention_days_for_object("temp/file.txt"), 1);
        
        // Test cache file retention
        assert_eq!(engine.get_retention_days_for_object("cache/data.bin"), 3);
    }

    #[tokio::test]
    async fn test_pattern_matching() {
        let config = RetentionPolicyConfig::default();
        let engine = RetentionPolicyEngine::new(config);
        
        // Test wildcard matching
        assert!(engine.matches_pattern("app.log", "*.log"));
        assert!(engine.matches_pattern("error.log", "*.log"));
        assert!(!engine.matches_pattern("app.txt", "*.log"));
        
        // Test exact matching
        assert!(engine.matches_pattern("temp/file.txt", "temp/file.txt"));
        assert!(!engine.matches_pattern("temp/file.txt", "temp/other.txt"));
    }

    #[tokio::test]
    async fn test_cleanup_evaluation() {
        let config = RetentionPolicyConfig::default();
        let engine = RetentionPolicyEngine::new(config);
        
        let context = PolicyContext {
            system_load: 0.5,
            disk_space_available: 80.0,
            active_operations: 10,
            current_time: SystemTime::now(),
            health_issues: std::collections::HashMap::new(),
            resource_usage: ResourceUsage::default(),
        };

        let result = engine.evaluate_cleanup(&context).await;
        // Should be allowed if enough time has passed since last cleanup
        assert!(result.allowed || result.reason.contains("Cleanup interval not reached"));
    }

    #[tokio::test]
    async fn test_retention_statistics() {
        let config = RetentionPolicyConfig::default();
        let mut engine = RetentionPolicyEngine::new(config);
        
        assert_eq!(engine.get_statistics().total_cleanups, 0);
        assert_eq!(engine.get_statistics().total_objects_deleted, 0);
        
        engine.record_cleanup(50);
        assert_eq!(engine.get_statistics().total_cleanups, 1);
        assert_eq!(engine.get_statistics().total_objects_deleted, 50);
        
        engine.record_cleanup(30);
        assert_eq!(engine.get_statistics().total_cleanups, 2);
        assert_eq!(engine.get_statistics().total_objects_deleted, 80);
    }

    #[tokio::test]
    async fn test_retention_rule_management() {
        let config = RetentionPolicyConfig::default();
        let mut engine = RetentionPolicyEngine::new(config);
        
        let initial_rules = engine.get_retention_rules().len();
        
        // Add a new rule
        let new_rule = RetentionRule {
            pattern: "backup/*".to_string(),
            retention_days: 90,
            enabled: true,
            priority: 4,
            recursive: true,
        };
        engine.add_retention_rule(new_rule);
        
        assert_eq!(engine.get_retention_rules().len(), initial_rules + 1);
        
        // Remove a rule
        let removed = engine.remove_retention_rule("*.log");
        assert!(removed);
        assert_eq!(engine.get_retention_rules().len(), initial_rules);
    }
} 