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

use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime},
};
use tracing::info;

/// Scanner metrics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ScannerMetrics {
    /// Total objects scanned since server start
    pub objects_scanned: u64,
    /// Total object versions scanned since server start
    pub versions_scanned: u64,
    /// Total directories scanned since server start
    pub directories_scanned: u64,
    /// Total bucket scans started since server start
    pub bucket_scans_started: u64,
    /// Total bucket scans finished since server start
    pub bucket_scans_finished: u64,
    /// Total objects with health issues found
    pub objects_with_issues: u64,
    /// Total heal tasks queued
    pub heal_tasks_queued: u64,
    /// Total heal tasks completed
    pub heal_tasks_completed: u64,
    /// Total heal tasks failed
    pub heal_tasks_failed: u64,
    /// Total healthy objects found
    pub healthy_objects: u64,
    /// Total corrupted objects found
    pub corrupted_objects: u64,
    /// Last scan activity time
    pub last_activity: Option<SystemTime>,
    /// Current scan cycle
    pub current_cycle: u64,
    /// Total scan cycles completed
    pub total_cycles: u64,
    /// Current scan duration
    pub current_scan_duration: Option<Duration>,
    /// Average scan duration
    pub avg_scan_duration: Duration,
    /// Objects scanned per second
    pub objects_per_second: f64,
    /// Buckets scanned per second
    pub buckets_per_second: f64,
    /// Storage metrics by bucket
    pub bucket_metrics: HashMap<String, BucketMetrics>,
    /// Disk metrics
    pub disk_metrics: HashMap<String, DiskMetrics>,
}

/// Bucket-specific metrics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BucketMetrics {
    /// Bucket name
    pub bucket: String,
    /// Total objects in bucket
    pub total_objects: u64,
    /// Total size of objects in bucket (bytes)
    pub total_size: u64,
    /// Objects with health issues
    pub objects_with_issues: u64,
    /// Last scan time
    pub last_scan_time: Option<SystemTime>,
    /// Scan duration
    pub scan_duration: Option<Duration>,
    /// Heal tasks queued for this bucket
    pub heal_tasks_queued: u64,
    /// Heal tasks completed for this bucket
    pub heal_tasks_completed: u64,
    /// Heal tasks failed for this bucket
    pub heal_tasks_failed: u64,
}

/// Disk-specific metrics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DiskMetrics {
    /// Disk path
    pub disk_path: String,
    /// Total disk space (bytes)
    pub total_space: u64,
    /// Used disk space (bytes)
    pub used_space: u64,
    /// Free disk space (bytes)
    pub free_space: u64,
    /// Objects scanned on this disk
    pub objects_scanned: u64,
    /// Objects with issues on this disk
    pub objects_with_issues: u64,
    /// Last scan time
    pub last_scan_time: Option<SystemTime>,
    /// Whether disk is online
    pub is_online: bool,
    /// Whether disk is being scanned
    pub is_scanning: bool,
}

/// Thread-safe metrics collector
pub struct MetricsCollector {
    /// Atomic counters for real-time metrics
    objects_scanned: AtomicU64,
    versions_scanned: AtomicU64,
    directories_scanned: AtomicU64,
    bucket_scans_started: AtomicU64,
    bucket_scans_finished: AtomicU64,
    objects_with_issues: AtomicU64,
    heal_tasks_queued: AtomicU64,
    heal_tasks_completed: AtomicU64,
    heal_tasks_failed: AtomicU64,
    current_cycle: AtomicU64,
    total_cycles: AtomicU64,
    healthy_objects: AtomicU64,
    corrupted_objects: AtomicU64,
}

impl MetricsCollector {
    /// Create a new metrics collector
    pub fn new() -> Self {
        Self {
            objects_scanned: AtomicU64::new(0),
            versions_scanned: AtomicU64::new(0),
            directories_scanned: AtomicU64::new(0),
            bucket_scans_started: AtomicU64::new(0),
            bucket_scans_finished: AtomicU64::new(0),
            objects_with_issues: AtomicU64::new(0),
            heal_tasks_queued: AtomicU64::new(0),
            heal_tasks_completed: AtomicU64::new(0),
            heal_tasks_failed: AtomicU64::new(0),
            current_cycle: AtomicU64::new(0),
            total_cycles: AtomicU64::new(0),
            healthy_objects: AtomicU64::new(0),
            corrupted_objects: AtomicU64::new(0),
        }
    }

    /// Increment objects scanned count
    pub fn increment_objects_scanned(&self, count: u64) {
        self.objects_scanned.fetch_add(count, Ordering::Relaxed);
    }

    /// Increment versions scanned count
    pub fn increment_versions_scanned(&self, count: u64) {
        self.versions_scanned.fetch_add(count, Ordering::Relaxed);
    }

    /// Increment directories scanned count
    pub fn increment_directories_scanned(&self, count: u64) {
        self.directories_scanned.fetch_add(count, Ordering::Relaxed);
    }

    /// Increment bucket scans started count
    pub fn increment_bucket_scans_started(&self, count: u64) {
        self.bucket_scans_started.fetch_add(count, Ordering::Relaxed);
    }

    /// Increment bucket scans finished count
    pub fn increment_bucket_scans_finished(&self, count: u64) {
        self.bucket_scans_finished.fetch_add(count, Ordering::Relaxed);
    }

    /// Increment objects with issues count
    pub fn increment_objects_with_issues(&self, count: u64) {
        self.objects_with_issues.fetch_add(count, Ordering::Relaxed);
    }

    /// Increment heal tasks queued count
    pub fn increment_heal_tasks_queued(&self, count: u64) {
        self.heal_tasks_queued.fetch_add(count, Ordering::Relaxed);
    }

    /// Increment heal tasks completed count
    pub fn increment_heal_tasks_completed(&self, count: u64) {
        self.heal_tasks_completed.fetch_add(count, Ordering::Relaxed);
    }

    /// Increment heal tasks failed count
    pub fn increment_heal_tasks_failed(&self, count: u64) {
        self.heal_tasks_failed.fetch_add(count, Ordering::Relaxed);
    }

    /// Set current cycle
    pub fn set_current_cycle(&self, cycle: u64) {
        self.current_cycle.store(cycle, Ordering::Relaxed);
    }

    /// Increment total cycles
    pub fn increment_total_cycles(&self) {
        self.total_cycles.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment healthy objects count
    pub fn increment_healthy_objects(&self) {
        self.healthy_objects.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment corrupted objects count
    pub fn increment_corrupted_objects(&self) {
        self.corrupted_objects.fetch_add(1, Ordering::Relaxed);
    }

    /// Get current metrics snapshot
    pub fn get_metrics(&self) -> ScannerMetrics {
        ScannerMetrics {
            objects_scanned: self.objects_scanned.load(Ordering::Relaxed),
            versions_scanned: self.versions_scanned.load(Ordering::Relaxed),
            directories_scanned: self.directories_scanned.load(Ordering::Relaxed),
            bucket_scans_started: self.bucket_scans_started.load(Ordering::Relaxed),
            bucket_scans_finished: self.bucket_scans_finished.load(Ordering::Relaxed),
            objects_with_issues: self.objects_with_issues.load(Ordering::Relaxed),
            heal_tasks_queued: self.heal_tasks_queued.load(Ordering::Relaxed),
            heal_tasks_completed: self.heal_tasks_completed.load(Ordering::Relaxed),
            heal_tasks_failed: self.heal_tasks_failed.load(Ordering::Relaxed),
            healthy_objects: self.healthy_objects.load(Ordering::Relaxed),
            corrupted_objects: self.corrupted_objects.load(Ordering::Relaxed),
            last_activity: Some(SystemTime::now()),
            current_cycle: self.current_cycle.load(Ordering::Relaxed),
            total_cycles: self.total_cycles.load(Ordering::Relaxed),
            current_scan_duration: None,       // Will be set by scanner
            avg_scan_duration: Duration::ZERO, // Will be calculated
            objects_per_second: 0.0,           // Will be calculated
            buckets_per_second: 0.0,           // Will be calculated
            bucket_metrics: HashMap::new(),    // Will be populated by scanner
            disk_metrics: HashMap::new(),      // Will be populated by scanner
        }
    }

    /// Reset all metrics
    pub fn reset(&self) {
        self.objects_scanned.store(0, Ordering::Relaxed);
        self.versions_scanned.store(0, Ordering::Relaxed);
        self.directories_scanned.store(0, Ordering::Relaxed);
        self.bucket_scans_started.store(0, Ordering::Relaxed);
        self.bucket_scans_finished.store(0, Ordering::Relaxed);
        self.objects_with_issues.store(0, Ordering::Relaxed);
        self.heal_tasks_queued.store(0, Ordering::Relaxed);
        self.heal_tasks_completed.store(0, Ordering::Relaxed);
        self.heal_tasks_failed.store(0, Ordering::Relaxed);
        self.current_cycle.store(0, Ordering::Relaxed);
        self.total_cycles.store(0, Ordering::Relaxed);
        self.healthy_objects.store(0, Ordering::Relaxed);
        self.corrupted_objects.store(0, Ordering::Relaxed);

        info!("Scanner metrics reset");
    }
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_collector_creation() {
        let collector = MetricsCollector::new();
        let metrics = collector.get_metrics();
        assert_eq!(metrics.objects_scanned, 0);
        assert_eq!(metrics.versions_scanned, 0);
    }

    #[test]
    fn test_metrics_increment() {
        let collector = MetricsCollector::new();

        collector.increment_objects_scanned(10);
        collector.increment_versions_scanned(5);
        collector.increment_objects_with_issues(2);

        let metrics = collector.get_metrics();
        assert_eq!(metrics.objects_scanned, 10);
        assert_eq!(metrics.versions_scanned, 5);
        assert_eq!(metrics.objects_with_issues, 2);
    }

    #[test]
    fn test_metrics_reset() {
        let collector = MetricsCollector::new();

        collector.increment_objects_scanned(10);
        collector.reset();

        let metrics = collector.get_metrics();
        assert_eq!(metrics.objects_scanned, 0);
    }
}
