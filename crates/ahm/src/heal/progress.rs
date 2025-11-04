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
use std::time::SystemTime;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct HealProgress {
    /// Objects scanned
    pub objects_scanned: u64,
    /// Objects healed
    pub objects_healed: u64,
    /// Objects failed
    pub objects_failed: u64,
    /// Bytes processed
    pub bytes_processed: u64,
    /// Current object
    pub current_object: Option<String>,
    /// Progress percentage
    pub progress_percentage: f64,
    /// Start time
    pub start_time: Option<SystemTime>,
    /// Last update time
    pub last_update_time: Option<SystemTime>,
    /// Estimated completion time
    pub estimated_completion_time: Option<SystemTime>,
}

impl HealProgress {
    pub fn new() -> Self {
        Self {
            start_time: Some(SystemTime::now()),
            last_update_time: Some(SystemTime::now()),
            ..Default::default()
        }
    }

    pub fn update_progress(&mut self, scanned: u64, healed: u64, failed: u64, bytes: u64) {
        self.objects_scanned = scanned;
        self.objects_healed = healed;
        self.objects_failed = failed;
        self.bytes_processed = bytes;
        self.last_update_time = Some(SystemTime::now());

        // calculate progress percentage
        let total = scanned + healed + failed;
        if total > 0 {
            self.progress_percentage = (healed as f64 / total as f64) * 100.0;
        }
    }

    pub fn set_current_object(&mut self, object: Option<String>) {
        self.current_object = object;
        self.last_update_time = Some(SystemTime::now());
    }

    pub fn is_completed(&self) -> bool {
        self.progress_percentage >= 100.0
            || self.objects_scanned > 0 && self.objects_healed + self.objects_failed >= self.objects_scanned
    }

    pub fn get_success_rate(&self) -> f64 {
        let total = self.objects_healed + self.objects_failed;
        if total > 0 {
            (self.objects_healed as f64 / total as f64) * 100.0
        } else {
            0.0
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealStatistics {
    /// Total heal tasks
    pub total_tasks: u64,
    /// Successful tasks
    pub successful_tasks: u64,
    /// Failed tasks
    pub failed_tasks: u64,
    /// Running tasks
    pub running_tasks: u64,
    /// Total healed objects
    pub total_objects_healed: u64,
    /// Total healed bytes
    pub total_bytes_healed: u64,
    /// Last update time
    pub last_update_time: SystemTime,
}

impl Default for HealStatistics {
    fn default() -> Self {
        Self::new()
    }
}

impl HealStatistics {
    pub fn new() -> Self {
        Self {
            total_tasks: 0,
            successful_tasks: 0,
            failed_tasks: 0,
            running_tasks: 0,
            total_objects_healed: 0,
            total_bytes_healed: 0,
            last_update_time: SystemTime::now(),
        }
    }

    pub fn update_task_completion(&mut self, success: bool) {
        if success {
            self.successful_tasks += 1;
        } else {
            self.failed_tasks += 1;
        }
        self.last_update_time = SystemTime::now();
    }

    pub fn update_running_tasks(&mut self, count: u64) {
        self.running_tasks = count;
        self.last_update_time = SystemTime::now();
    }

    pub fn add_healed_objects(&mut self, count: u64, bytes: u64) {
        self.total_objects_healed += count;
        self.total_bytes_healed += bytes;
        self.last_update_time = SystemTime::now();
    }

    pub fn get_success_rate(&self) -> f64 {
        let total = self.successful_tasks + self.failed_tasks;
        if total > 0 {
            (self.successful_tasks as f64 / total as f64) * 100.0
        } else {
            0.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_heal_progress_new() {
        let progress = HealProgress::new();
        assert_eq!(progress.objects_scanned, 0);
        assert_eq!(progress.objects_healed, 0);
        assert_eq!(progress.objects_failed, 0);
        assert_eq!(progress.bytes_processed, 0);
        assert_eq!(progress.progress_percentage, 0.0);
        assert!(progress.start_time.is_some());
        assert!(progress.last_update_time.is_some());
        assert!(progress.current_object.is_none());
    }

    #[test]
    fn test_heal_progress_update_progress() {
        let mut progress = HealProgress::new();
        progress.update_progress(10, 8, 2, 1024);

        assert_eq!(progress.objects_scanned, 10);
        assert_eq!(progress.objects_healed, 8);
        assert_eq!(progress.objects_failed, 2);
        assert_eq!(progress.bytes_processed, 1024);
        // Progress percentage should be calculated based on healed/total
        // total = scanned + healed + failed = 10 + 8 + 2 = 20
        // healed/total = 8/20 = 0.4 = 40%
        assert!((progress.progress_percentage - 40.0).abs() < 0.001);
        assert!(progress.last_update_time.is_some());
    }

    #[test]
    fn test_heal_progress_update_progress_zero_total() {
        let mut progress = HealProgress::new();
        progress.update_progress(0, 0, 0, 0);

        assert_eq!(progress.progress_percentage, 0.0);
    }

    #[test]
    fn test_heal_progress_update_progress_all_healed() {
        let mut progress = HealProgress::new();
        // When scanned=0, healed=10, failed=0: total=10, progress = 10/10 = 100%
        progress.update_progress(0, 10, 0, 2048);

        // All healed, should be 100%
        assert!((progress.progress_percentage - 100.0).abs() < 0.001);
    }

    #[test]
    fn test_heal_progress_set_current_object() {
        let mut progress = HealProgress::new();
        let initial_time = progress.last_update_time;

        // Small delay to ensure time difference
        std::thread::sleep(std::time::Duration::from_millis(10));

        progress.set_current_object(Some("test-bucket/test-object".to_string()));

        assert_eq!(progress.current_object, Some("test-bucket/test-object".to_string()));
        assert!(progress.last_update_time.is_some());
        // last_update_time should be updated
        assert_ne!(progress.last_update_time, initial_time);
    }

    #[test]
    fn test_heal_progress_set_current_object_none() {
        let mut progress = HealProgress::new();
        progress.set_current_object(Some("test".to_string()));
        progress.set_current_object(None);

        assert!(progress.current_object.is_none());
    }

    #[test]
    fn test_heal_progress_is_completed_by_percentage() {
        let mut progress = HealProgress::new();
        progress.update_progress(10, 10, 0, 1024);

        assert!(progress.is_completed());
    }

    #[test]
    fn test_heal_progress_is_completed_by_processed() {
        let mut progress = HealProgress::new();
        progress.objects_scanned = 10;
        progress.objects_healed = 8;
        progress.objects_failed = 2;
        // healed + failed = 8 + 2 = 10 >= scanned = 10
        assert!(progress.is_completed());
    }

    #[test]
    fn test_heal_progress_is_not_completed() {
        let mut progress = HealProgress::new();
        progress.objects_scanned = 10;
        progress.objects_healed = 5;
        progress.objects_failed = 2;
        // healed + failed = 5 + 2 = 7 < scanned = 10
        assert!(!progress.is_completed());
    }

    #[test]
    fn test_heal_progress_get_success_rate() {
        let mut progress = HealProgress::new();
        progress.objects_healed = 8;
        progress.objects_failed = 2;

        // success_rate = 8 / (8 + 2) * 100 = 80%
        assert!((progress.get_success_rate() - 80.0).abs() < 0.001);
    }

    #[test]
    fn test_heal_progress_get_success_rate_zero_total() {
        let progress = HealProgress::new();
        // No healed or failed objects
        assert_eq!(progress.get_success_rate(), 0.0);
    }

    #[test]
    fn test_heal_progress_get_success_rate_all_success() {
        let mut progress = HealProgress::new();
        progress.objects_healed = 10;
        progress.objects_failed = 0;

        assert!((progress.get_success_rate() - 100.0).abs() < 0.001);
    }

    #[test]
    fn test_heal_statistics_new() {
        let stats = HealStatistics::new();
        assert_eq!(stats.total_tasks, 0);
        assert_eq!(stats.successful_tasks, 0);
        assert_eq!(stats.failed_tasks, 0);
        assert_eq!(stats.running_tasks, 0);
        assert_eq!(stats.total_objects_healed, 0);
        assert_eq!(stats.total_bytes_healed, 0);
    }

    #[test]
    fn test_heal_statistics_default() {
        let stats = HealStatistics::default();
        assert_eq!(stats.total_tasks, 0);
        assert_eq!(stats.successful_tasks, 0);
        assert_eq!(stats.failed_tasks, 0);
    }

    #[test]
    fn test_heal_statistics_update_task_completion_success() {
        let mut stats = HealStatistics::new();
        let initial_time = stats.last_update_time;

        std::thread::sleep(std::time::Duration::from_millis(10));
        stats.update_task_completion(true);

        assert_eq!(stats.successful_tasks, 1);
        assert_eq!(stats.failed_tasks, 0);
        assert!(stats.last_update_time > initial_time);
    }

    #[test]
    fn test_heal_statistics_update_task_completion_failure() {
        let mut stats = HealStatistics::new();
        stats.update_task_completion(false);

        assert_eq!(stats.successful_tasks, 0);
        assert_eq!(stats.failed_tasks, 1);
    }

    #[test]
    fn test_heal_statistics_update_running_tasks() {
        let mut stats = HealStatistics::new();
        let initial_time = stats.last_update_time;

        std::thread::sleep(std::time::Duration::from_millis(10));
        stats.update_running_tasks(5);

        assert_eq!(stats.running_tasks, 5);
        assert!(stats.last_update_time > initial_time);
    }

    #[test]
    fn test_heal_statistics_add_healed_objects() {
        let mut stats = HealStatistics::new();
        let initial_time = stats.last_update_time;

        std::thread::sleep(std::time::Duration::from_millis(10));
        stats.add_healed_objects(10, 10240);

        assert_eq!(stats.total_objects_healed, 10);
        assert_eq!(stats.total_bytes_healed, 10240);
        assert!(stats.last_update_time > initial_time);
    }

    #[test]
    fn test_heal_statistics_add_healed_objects_accumulative() {
        let mut stats = HealStatistics::new();
        stats.add_healed_objects(5, 5120);
        stats.add_healed_objects(3, 3072);

        assert_eq!(stats.total_objects_healed, 8);
        assert_eq!(stats.total_bytes_healed, 8192);
    }

    #[test]
    fn test_heal_statistics_get_success_rate() {
        let mut stats = HealStatistics::new();
        stats.successful_tasks = 8;
        stats.failed_tasks = 2;

        // success_rate = 8 / (8 + 2) * 100 = 80%
        assert!((stats.get_success_rate() - 80.0).abs() < 0.001);
    }

    #[test]
    fn test_heal_statistics_get_success_rate_zero_total() {
        let stats = HealStatistics::new();
        assert_eq!(stats.get_success_rate(), 0.0);
    }

    #[test]
    fn test_heal_statistics_get_success_rate_all_success() {
        let mut stats = HealStatistics::new();
        stats.successful_tasks = 10;
        stats.failed_tasks = 0;

        assert!((stats.get_success_rate() - 100.0).abs() < 0.001);
    }

    #[test]
    fn test_heal_statistics_get_success_rate_all_failure() {
        let mut stats = HealStatistics::new();
        stats.successful_tasks = 0;
        stats.failed_tasks = 5;

        assert_eq!(stats.get_success_rate(), 0.0);
    }
}
