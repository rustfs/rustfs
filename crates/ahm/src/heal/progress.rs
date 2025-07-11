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
