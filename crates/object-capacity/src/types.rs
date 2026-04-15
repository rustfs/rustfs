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

use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CapacityDiskRef {
    pub endpoint: String,
    pub drive_path: String,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct CapacityScanResult {
    pub used_bytes: u64,
    pub file_count: usize,
    pub sampled_count: usize,
    pub is_estimated: bool,
    pub scan_duration: Duration,
    pub had_partial_errors: bool,
}

impl CapacityScanResult {
    pub(crate) fn with_partial_errors(mut self) -> Self {
        self.had_partial_errors = true;
        self
    }
}

/// Public summary type for external tooling such as benches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CapacityScanSummary {
    pub used_bytes: u64,
    pub file_count: usize,
    pub sampled_count: usize,
    pub is_estimated: bool,
    pub had_partial_errors: bool,
    pub scan_duration: Duration,
}

impl From<CapacityScanResult> for CapacityScanSummary {
    fn from(scan: CapacityScanResult) -> Self {
        Self {
            used_bytes: scan.used_bytes,
            file_count: scan.file_count,
            sampled_count: scan.sampled_count,
            is_estimated: scan.is_estimated,
            had_partial_errors: scan.had_partial_errors,
            scan_duration: scan.scan_duration,
        }
    }
}
