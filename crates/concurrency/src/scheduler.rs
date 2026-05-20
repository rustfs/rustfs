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

//! I/O scheduler management

use rustfs_io_core::{
    IoLoadLevel, IoPriority, IoScheduler as CoreIoScheduler, IoSchedulingContext,
    io_profile::{AccessPattern, StorageMedia},
};
use rustfs_io_metrics::io_metrics;
use std::sync::Arc;
use std::time::Duration;

/// Facade policy for the concurrency-layer scheduler manager.
#[derive(Debug, Clone, Copy)]
pub struct SchedulerPolicy {
    /// Base buffer size
    pub base_buffer_size: usize,
    /// Maximum buffer size
    pub max_buffer_size: usize,
    /// High priority threshold
    pub high_priority_threshold: usize,
    /// Low priority threshold
    pub low_priority_threshold: usize,
}

impl Default for SchedulerPolicy {
    fn default() -> Self {
        Self {
            base_buffer_size: 64 * 1024,              // 64KB
            max_buffer_size: 4 * 1024 * 1024,         // 4MB
            high_priority_threshold: 1024 * 1024,     // 1MB
            low_priority_threshold: 10 * 1024 * 1024, // 10MB
        }
    }
}

impl SchedulerPolicy {
    /// Convert facade policy to io-core scheduler config.
    pub fn to_core_config(&self) -> rustfs_io_core::IoSchedulerConfig {
        rustfs_io_core::IoSchedulerConfig {
            base_buffer_size: self.base_buffer_size,
            max_buffer_size: self.max_buffer_size,
            high_priority_size_threshold: self.high_priority_threshold,
            low_priority_size_threshold: self.low_priority_threshold,
            ..rustfs_io_core::IoSchedulerConfig::default()
        }
    }
}

/// Backward-compatible alias for the old scheduler facade name.
pub type SchedulerConfig = SchedulerPolicy;

/// Scheduler manager
pub struct SchedulerManager {
    config: SchedulerPolicy,
    core_config: rustfs_io_core::IoSchedulerConfig,
    scheduler: Arc<CoreIoScheduler>,
}

impl SchedulerManager {
    /// Create a new scheduler manager
    pub fn new(
        base_buffer_size: usize,
        max_buffer_size: usize,
        high_priority_threshold: usize,
        low_priority_threshold: usize,
    ) -> Self {
        Self::from_policy(SchedulerPolicy {
            base_buffer_size,
            max_buffer_size,
            high_priority_threshold,
            low_priority_threshold,
        })
    }

    /// Create a scheduler manager from facade policy.
    pub fn from_policy(config: SchedulerPolicy) -> Self {
        let core_config = config.to_core_config();

        Self {
            config,
            core_config: core_config.clone(),
            scheduler: Arc::new(CoreIoScheduler::new(core_config)),
        }
    }

    /// Get the configuration
    pub fn config(&self) -> &SchedulerPolicy {
        &self.config
    }

    /// Get the derived io-core scheduler config.
    pub fn core_config(&self) -> &rustfs_io_core::IoSchedulerConfig {
        &self.core_config
    }

    /// Get the scheduler
    pub fn scheduler(&self) -> Arc<CoreIoScheduler> {
        self.scheduler.clone()
    }

    /// Create an I/O strategy
    pub fn create_strategy(&self) -> IoStrategy {
        IoStrategy::new(self.config, self.scheduler.clone())
    }

    /// Calculate buffer size
    pub fn calculate_buffer_size(
        &self,
        file_size: i64,
        media: StorageMedia,
        pattern: AccessPattern,
        load: IoLoadLevel,
        concurrent: usize,
    ) -> usize {
        let strategy = self.create_strategy();
        strategy.calculate_buffer_size(file_size, media, pattern, load, concurrent)
    }

    /// Get I/O priority
    pub fn get_priority(&self, size: i64) -> IoPriority {
        IoPriority::from_size(size, self.config.high_priority_threshold, self.config.low_priority_threshold)
    }
}

/// I/O strategy
pub struct IoStrategy {
    config: SchedulerPolicy,
    scheduler: Arc<CoreIoScheduler>,
}

impl IoStrategy {
    fn new(config: SchedulerPolicy, scheduler: Arc<CoreIoScheduler>) -> Self {
        Self { config, scheduler }
    }

    /// Calculate buffer size with multi-factor strategy
    pub fn calculate_buffer_size(
        &self,
        file_size: i64,
        media: StorageMedia,
        pattern: AccessPattern,
        load: IoLoadLevel,
        concurrent: usize,
    ) -> usize {
        // Create scheduling context
        let _ctx = IoSchedulingContext::new(file_size, self.config.base_buffer_size)
            .with_sequential(matches!(pattern, AccessPattern::Sequential))
            .with_media(media);

        // Get base buffer size from core scheduler
        let permit_wait = Duration::from_millis(10); // Default wait time
        let is_sequential = matches!(pattern, AccessPattern::Sequential);
        let core_strategy = self.scheduler.calculate_strategy(file_size, permit_wait, is_sequential);
        let base_size = core_strategy.buffer_size;

        // Apply multi-factor adjustments
        let adjusted_size = self.apply_adjustments(base_size, media, pattern, load, concurrent);

        // Record metrics
        io_metrics::record_io_scheduler_decision(adjusted_size, load.as_str(), pattern.as_str());

        adjusted_size.min(self.config.max_buffer_size)
    }

    fn apply_adjustments(
        &self,
        base_size: usize,
        media: StorageMedia,
        pattern: AccessPattern,
        load: IoLoadLevel,
        concurrent: usize,
    ) -> usize {
        let mut size = base_size;

        // Media adjustment
        size = match media {
            StorageMedia::Nvme => (size as f64 * 1.5) as usize,
            StorageMedia::Ssd => (size as f64 * 1.2) as usize,
            StorageMedia::Hdd => size,
            _ => size,
        };

        // Pattern adjustment
        size = match pattern {
            AccessPattern::Sequential => (size as f64 * 1.5) as usize,
            AccessPattern::Random => (size as f64 * 0.5) as usize,
            _ => size,
        };

        // Load adjustment
        size = match load {
            IoLoadLevel::Low => (size as f64 * 1.2) as usize,
            IoLoadLevel::Medium => size,
            IoLoadLevel::High => (size as f64 * 0.7) as usize,
            IoLoadLevel::Critical => (size as f64 * 0.5) as usize,
        };

        // Concurrency adjustment
        if concurrent > 10 {
            size = (size as f64 * 0.8) as usize;
        }

        size
    }

    /// Get the configuration
    pub fn config(&self) -> &SchedulerPolicy {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scheduler_config() {
        let config = SchedulerPolicy::default();
        assert!(config.base_buffer_size < config.max_buffer_size);
    }

    #[test]
    fn test_scheduler_policy_to_core_config() {
        let policy = SchedulerPolicy::default();
        let core = policy.to_core_config();
        assert_eq!(core.base_buffer_size, policy.base_buffer_size);
        assert_eq!(core.max_buffer_size, policy.max_buffer_size);
        assert_eq!(core.high_priority_size_threshold, policy.high_priority_threshold);
        assert_eq!(core.low_priority_size_threshold, policy.low_priority_threshold);
    }

    #[test]
    fn test_scheduler_manager() {
        let manager = SchedulerManager::new(1024, 4096, 512, 2048);
        let priority = manager.get_priority(100);
        assert!(priority.is_high());
    }

    #[test]
    fn test_io_strategy() {
        let manager = SchedulerManager::new(1024, 4096, 512, 2048);
        let strategy = manager.create_strategy();

        let size = strategy.calculate_buffer_size(1024 * 1024, StorageMedia::Ssd, AccessPattern::Sequential, IoLoadLevel::Low, 1);

        assert!(size > 0);
    }
}
