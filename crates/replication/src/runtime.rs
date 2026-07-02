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

use crate::ReplicationPriority;

pub const WORKER_MAX_LIMIT: usize = 500;
pub const WORKER_MIN_LIMIT: usize = 50;
pub const WORKER_AUTO_DEFAULT: usize = 100;
pub const MRF_WORKER_MAX_LIMIT: usize = 8;
pub const MRF_WORKER_MIN_LIMIT: usize = 2;
pub const MRF_WORKER_AUTO_DEFAULT: usize = 4;
pub const LARGE_WORKER_COUNT: usize = 10;
pub const MIN_LARGE_OBJ_SIZE: i64 = 128 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct ReplicationPoolOpts {
    pub priority: ReplicationPriority,
    pub max_workers: Option<usize>,
    pub max_l_workers: Option<usize>,
}

impl Default for ReplicationPoolOpts {
    fn default() -> Self {
        Self {
            priority: ReplicationPriority::Auto,
            max_workers: None,
            max_l_workers: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicationWorkerCounts {
    pub workers: usize,
    pub mrf_workers: usize,
}

impl ReplicationWorkerCounts {
    pub const fn new(workers: usize, mrf_workers: usize) -> Self {
        Self { workers, mrf_workers }
    }

    pub fn capped_by_worker_limit(self, max_workers: usize) -> Self {
        Self {
            workers: self.workers.min(max_workers),
            mrf_workers: self.mrf_workers.min(max_workers),
        }
    }

    pub fn mrf_workers_i32(self) -> i32 {
        usize_to_i32_saturating(self.mrf_workers)
    }
}

pub fn initial_worker_counts(opts: &ReplicationPoolOpts) -> ReplicationWorkerCounts {
    worker_counts_for_priority(&opts.priority, WORKER_AUTO_DEFAULT, MRF_WORKER_AUTO_DEFAULT)
        .capped_by_worker_limit(opts.max_workers.unwrap_or(WORKER_MAX_LIMIT))
}

pub fn resized_worker_counts(
    priority: &ReplicationPriority,
    max_workers: Option<usize>,
    current_workers: usize,
    current_mrf_workers: usize,
) -> ReplicationWorkerCounts {
    let counts = worker_counts_for_priority(priority, current_workers, current_mrf_workers);
    if let Some(max_workers) = max_workers {
        counts.capped_by_worker_limit(max_workers)
    } else {
        counts
    }
}

pub fn mrf_worker_size_to_count(size: i32) -> usize {
    let non_negative = size.max(0);
    match usize::try_from(non_negative) {
        Ok(size) => size,
        Err(_) => 0,
    }
}

pub fn worker_counts_for_priority(
    priority: &ReplicationPriority,
    current_workers: usize,
    current_mrf_workers: usize,
) -> ReplicationWorkerCounts {
    match priority {
        ReplicationPriority::Fast => ReplicationWorkerCounts::new(WORKER_MAX_LIMIT, MRF_WORKER_MAX_LIMIT),
        ReplicationPriority::Slow => ReplicationWorkerCounts::new(WORKER_MIN_LIMIT, MRF_WORKER_MIN_LIMIT),
        ReplicationPriority::Auto => ReplicationWorkerCounts::new(
            auto_worker_count(current_workers, WORKER_AUTO_DEFAULT),
            auto_worker_count(current_mrf_workers, MRF_WORKER_AUTO_DEFAULT),
        ),
    }
}

pub fn should_queue_large_object(size: i64) -> bool {
    size >= MIN_LARGE_OBJ_SIZE
}

pub fn should_grow_large_workers(active_lrg_workers: i32, max_l_workers: usize) -> bool {
    active_lrg_workers < usize_to_i32_saturating(max_l_workers.min(LARGE_WORKER_COUNT))
}

pub fn next_large_worker_count(existing_workers: usize, max_l_workers: usize) -> usize {
    existing_workers.saturating_add(1).min(max_l_workers)
}

pub fn next_regular_worker_count(current_workers: usize, active_workers: i32, max_workers: usize) -> Option<usize> {
    let max_workers = max_workers.min(WORKER_MAX_LIMIT);
    if active_workers < usize_to_i32_saturating(max_workers) {
        Some(current_workers.saturating_add(1).min(max_workers))
    } else {
        None
    }
}

pub fn next_mrf_worker_count(current_mrf_workers: i32, active_mrf_workers: i32, max_workers: usize) -> Option<i32> {
    let max_mrf_workers = max_workers.min(MRF_WORKER_MAX_LIMIT);
    let max_mrf_workers = usize_to_i32_saturating(max_mrf_workers);
    if active_mrf_workers < max_mrf_workers {
        Some(current_mrf_workers.saturating_add(1).min(max_mrf_workers))
    } else {
        None
    }
}

fn auto_worker_count(current_workers: usize, auto_default: usize) -> usize {
    if current_workers < auto_default {
        current_workers.saturating_add(1).min(auto_default)
    } else {
        auto_default
    }
}

fn usize_to_i32_saturating(value: usize) -> i32 {
    match i32::try_from(value) {
        Ok(value) => value,
        Err(_) => i32::MAX,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_worker_counts_preserve_priority_defaults() {
        let fast = ReplicationPoolOpts {
            priority: ReplicationPriority::Fast,
            ..Default::default()
        };
        assert_eq!(
            initial_worker_counts(&fast),
            ReplicationWorkerCounts::new(WORKER_MAX_LIMIT, MRF_WORKER_MAX_LIMIT)
        );

        let slow = ReplicationPoolOpts {
            priority: ReplicationPriority::Slow,
            ..Default::default()
        };
        assert_eq!(
            initial_worker_counts(&slow),
            ReplicationWorkerCounts::new(WORKER_MIN_LIMIT, MRF_WORKER_MIN_LIMIT)
        );

        let auto = ReplicationPoolOpts::default();
        assert_eq!(
            initial_worker_counts(&auto),
            ReplicationWorkerCounts::new(WORKER_AUTO_DEFAULT, MRF_WORKER_AUTO_DEFAULT)
        );
    }

    #[test]
    fn worker_counts_respect_configured_max_workers() {
        let opts = ReplicationPoolOpts {
            priority: ReplicationPriority::Fast,
            max_workers: Some(3),
            max_l_workers: None,
        };

        assert_eq!(initial_worker_counts(&opts), ReplicationWorkerCounts::new(3, 3));
        assert_eq!(
            resized_worker_counts(&ReplicationPriority::Fast, Some(5), WORKER_AUTO_DEFAULT, MRF_WORKER_AUTO_DEFAULT),
            ReplicationWorkerCounts::new(5, 5)
        );
    }

    #[test]
    fn mrf_worker_size_to_count_clamps_negative_values() {
        assert_eq!(mrf_worker_size_to_count(-1), 0);
        assert_eq!(mrf_worker_size_to_count(0), 0);
        assert_eq!(mrf_worker_size_to_count(3), 3);
    }

    #[test]
    fn auto_priority_grows_toward_defaults() {
        assert_eq!(
            worker_counts_for_priority(&ReplicationPriority::Auto, 0, 0),
            ReplicationWorkerCounts::new(1, 1)
        );
        assert_eq!(
            worker_counts_for_priority(&ReplicationPriority::Auto, WORKER_AUTO_DEFAULT, MRF_WORKER_AUTO_DEFAULT),
            ReplicationWorkerCounts::new(WORKER_AUTO_DEFAULT, MRF_WORKER_AUTO_DEFAULT)
        );
    }

    #[test]
    fn backpressure_growth_caps_at_limits() {
        assert_eq!(next_regular_worker_count(4, 4, WORKER_MAX_LIMIT), Some(5));
        assert_eq!(
            next_regular_worker_count(WORKER_MAX_LIMIT, usize_to_i32_saturating(WORKER_MAX_LIMIT), WORKER_MAX_LIMIT),
            None
        );

        assert_eq!(next_mrf_worker_count(2, 2, WORKER_MAX_LIMIT), Some(3));
        assert_eq!(
            next_mrf_worker_count(8, usize_to_i32_saturating(MRF_WORKER_MAX_LIMIT), WORKER_MAX_LIMIT),
            None
        );

        assert!(should_queue_large_object(MIN_LARGE_OBJ_SIZE));
        assert!(!should_queue_large_object(MIN_LARGE_OBJ_SIZE - 1));
        assert!(should_grow_large_workers(1, LARGE_WORKER_COUNT));
        assert_eq!(next_large_worker_count(LARGE_WORKER_COUNT, LARGE_WORKER_COUNT), LARGE_WORKER_COUNT);
    }
}
