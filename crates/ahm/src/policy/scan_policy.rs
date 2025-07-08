// Copyright 2024 RustFS Team

use std::time::{Duration, SystemTime};

use crate::scanner::Severity;

use super::{PolicyContext, PolicyResult, ResourceUsage};

/// Configuration for scan policies
#[derive(Debug, Clone)]
pub struct ScanPolicyConfig {
    /// Maximum number of concurrent scans
    pub max_concurrent_scans: usize,
    /// Maximum scan duration per cycle
    pub max_scan_duration: Duration,
    /// Minimum interval between scans
    pub min_scan_interval: Duration,
    /// Maximum system load threshold for scanning
    pub max_system_load: f64,
    /// Minimum available disk space percentage for scanning
    pub min_disk_space: f64,
    /// Maximum number of active operations for scanning
    pub max_active_operations: u64,
    /// Whether to enable deep scanning
    pub enable_deep_scan: bool,
    /// Deep scan interval (how often to perform deep scans)
    pub deep_scan_interval: Duration,
    /// Bandwidth limit for scanning (bytes per second)
    pub bandwidth_limit: Option<u64>,
    /// Priority-based scanning configuration
    pub priority_config: ScanPriorityConfig,
}

/// Priority-based scanning configuration
#[derive(Debug, Clone)]
pub struct ScanPriorityConfig {
    /// Whether to enable priority-based scanning
    pub enabled: bool,
    /// Critical issues scan interval
    pub critical_interval: Duration,
    /// High priority issues scan interval
    pub high_interval: Duration,
    /// Medium priority issues scan interval
    pub medium_interval: Duration,
    /// Low priority issues scan interval
    pub low_interval: Duration,
}

impl Default for ScanPolicyConfig {
    fn default() -> Self {
        Self {
            max_concurrent_scans: 4,
            max_scan_duration: Duration::from_secs(3600), // 1 hour
            min_scan_interval: Duration::from_secs(300),  // 5 minutes
            max_system_load: 0.8,
            min_disk_space: 10.0, // 10% minimum disk space
            max_active_operations: 100,
            enable_deep_scan: true,
            deep_scan_interval: Duration::from_secs(86400), // 24 hours
            bandwidth_limit: Some(100 * 1024 * 1024), // 100 MB/s
            priority_config: ScanPriorityConfig::default(),
        }
    }
}

impl Default for ScanPriorityConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            critical_interval: Duration::from_secs(60),   // 1 minute
            high_interval: Duration::from_secs(300),      // 5 minutes
            medium_interval: Duration::from_secs(1800),   // 30 minutes
            low_interval: Duration::from_secs(3600),      // 1 hour
        }
    }
}

/// Scan policy engine
pub struct ScanPolicyEngine {
    config: ScanPolicyConfig,
    last_scan_time: SystemTime,
    last_deep_scan_time: SystemTime,
    scan_count: u64,
}

impl ScanPolicyEngine {
    /// Create a new scan policy engine
    pub fn new(config: ScanPolicyConfig) -> Self {
        Self {
            config,
            last_scan_time: SystemTime::now(),
            last_deep_scan_time: SystemTime::now(),
            scan_count: 0,
        }
    }

    /// Get the configuration
    pub fn config(&self) -> &ScanPolicyConfig {
        &self.config
    }

    /// Evaluate scan policy
    pub async fn evaluate(&self, context: &PolicyContext) -> PolicyResult {
        let mut reasons = Vec::new();
        let mut allowed = true;

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

        // Check scan interval
        let time_since_last_scan = context.current_time
            .duration_since(self.last_scan_time)
            .unwrap_or(Duration::ZERO);

        if time_since_last_scan < self.config.min_scan_interval {
            allowed = false;
            reasons.push(format!(
                "Scan interval too short: {:?} < {:?}",
                time_since_last_scan, self.config.min_scan_interval
            ));
        }

        // Check resource usage
        if context.resource_usage.cpu_usage > 90.0 {
            allowed = false;
            reasons.push("CPU usage too high".to_string());
        }

        if context.resource_usage.memory_usage > 90.0 {
            allowed = false;
            reasons.push("Memory usage too high".to_string());
        }

        let reason = if reasons.is_empty() {
            "Scan allowed".to_string()
        } else {
            reasons.join("; ")
        };

        PolicyResult {
            allowed,
            reason,
            metadata: Some(serde_json::json!({
                "scan_count": self.scan_count,
                "time_since_last_scan": time_since_last_scan.as_secs(),
                "system_load": context.system_load,
                "disk_space_available": context.disk_space_available,
                "active_operations": context.active_operations,
            })),
            evaluated_at: context.current_time,
        }
    }

    /// Evaluate deep scan policy
    pub async fn evaluate_deep_scan(&self, context: &PolicyContext) -> PolicyResult {
        let mut base_result = self.evaluate(context).await;
        
        if !base_result.allowed {
            return base_result;
        }

        // Check deep scan interval
        let time_since_last_deep_scan = context.current_time
            .duration_since(self.last_deep_scan_time)
            .unwrap_or(Duration::ZERO);

        if time_since_last_deep_scan < self.config.deep_scan_interval {
            base_result.allowed = false;
            base_result.reason = format!(
                "Deep scan interval too short: {:?} < {:?}",
                time_since_last_deep_scan, self.config.deep_scan_interval
            );
        } else {
            base_result.reason = "Deep scan allowed".to_string();
        }

        // Add deep scan metadata
        if let Some(ref mut metadata) = base_result.metadata {
            if let Some(obj) = metadata.as_object_mut() {
                obj.insert(
                    "time_since_last_deep_scan".to_string(),
                    serde_json::Value::Number(serde_json::Number::from(time_since_last_deep_scan.as_secs())),
                );
                obj.insert(
                    "deep_scan_enabled".to_string(),
                    serde_json::Value::Bool(self.config.enable_deep_scan),
                );
            }
        }

        base_result
    }

    /// Get scan interval based on priority
    pub fn get_priority_interval(&self, severity: Severity) -> Duration {
        if !self.config.priority_config.enabled {
            return self.config.min_scan_interval;
        }

        match severity {
            Severity::Critical => self.config.priority_config.critical_interval,
            Severity::High => self.config.priority_config.high_interval,
            Severity::Medium => self.config.priority_config.medium_interval,
            Severity::Low => self.config.priority_config.low_interval,
        }
    }

    /// Update scan statistics
    pub fn record_scan(&mut self) {
        self.last_scan_time = SystemTime::now();
        self.scan_count += 1;
    }

    /// Update deep scan statistics
    pub fn record_deep_scan(&mut self) {
        self.last_deep_scan_time = SystemTime::now();
    }

    /// Get scan statistics
    pub fn get_statistics(&self) -> ScanPolicyStatistics {
        ScanPolicyStatistics {
            total_scans: self.scan_count,
            last_scan_time: self.last_scan_time,
            last_deep_scan_time: self.last_deep_scan_time,
            config: self.config.clone(),
        }
    }
}

/// Scan policy statistics
#[derive(Debug, Clone)]
pub struct ScanPolicyStatistics {
    pub total_scans: u64,
    pub last_scan_time: SystemTime,
    pub last_deep_scan_time: SystemTime,
    pub config: ScanPolicyConfig,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scanner::Severity;

    #[tokio::test]
    async fn test_scan_policy_creation() {
        let config = ScanPolicyConfig::default();
        let engine = ScanPolicyEngine::new(config);
        
        assert_eq!(engine.config().max_concurrent_scans, 4);
        assert_eq!(engine.config().max_system_load, 0.8);
        assert_eq!(engine.config().min_disk_space, 10.0);
    }

    #[tokio::test]
    async fn test_scan_policy_evaluation() {
        let config = ScanPolicyConfig::default();
        let engine = ScanPolicyEngine::new(config);
        
        let context = PolicyContext {
            system_load: 0.5,
            disk_space_available: 80.0,
            active_operations: 10,
            current_time: SystemTime::now(),
            health_issues: std::collections::HashMap::new(),
            resource_usage: ResourceUsage::default(),
        };

        let result = engine.evaluate(&context).await;
        assert!(result.allowed);
        assert!(result.reason.contains("Scan allowed"));
    }

    #[tokio::test]
    async fn test_scan_policy_system_load_limit() {
        let config = ScanPolicyConfig::default();
        let engine = ScanPolicyEngine::new(config);
        
        let context = PolicyContext {
            system_load: 0.9, // Above threshold
            disk_space_available: 80.0,
            active_operations: 10,
            current_time: SystemTime::now(),
            health_issues: std::collections::HashMap::new(),
            resource_usage: ResourceUsage::default(),
        };

        let result = engine.evaluate(&context).await;
        assert!(!result.allowed);
        assert!(result.reason.contains("System load too high"));
    }

    #[tokio::test]
    async fn test_scan_policy_disk_space_limit() {
        let config = ScanPolicyConfig::default();
        let engine = ScanPolicyEngine::new(config);
        
        let context = PolicyContext {
            system_load: 0.5,
            disk_space_available: 5.0, // Below threshold
            active_operations: 10,
            current_time: SystemTime::now(),
            health_issues: std::collections::HashMap::new(),
            resource_usage: ResourceUsage::default(),
        };

        let result = engine.evaluate(&context).await;
        assert!(!result.allowed);
        assert!(result.reason.contains("Disk space too low"));
    }

    #[tokio::test]
    async fn test_priority_intervals() {
        let config = ScanPolicyConfig::default();
        let engine = ScanPolicyEngine::new(config);
        
        assert_eq!(
            engine.get_priority_interval(Severity::Critical),
            Duration::from_secs(60)
        );
        assert_eq!(
            engine.get_priority_interval(Severity::High),
            Duration::from_secs(300)
        );
        assert_eq!(
            engine.get_priority_interval(Severity::Medium),
            Duration::from_secs(1800)
        );
        assert_eq!(
            engine.get_priority_interval(Severity::Low),
            Duration::from_secs(3600)
        );
    }

    #[tokio::test]
    async fn test_scan_statistics() {
        let config = ScanPolicyConfig::default();
        let mut engine = ScanPolicyEngine::new(config);
        
        assert_eq!(engine.get_statistics().total_scans, 0);
        
        engine.record_scan();
        assert_eq!(engine.get_statistics().total_scans, 1);
        
        engine.record_deep_scan();
        let stats = engine.get_statistics();
        assert_eq!(stats.total_scans, 1);
        assert!(stats.last_deep_scan_time > stats.last_scan_time);
    }
} 