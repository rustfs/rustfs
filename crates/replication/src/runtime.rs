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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicationWorkerResize {
    pub new_count: usize,
    pub existing_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicationBackpressureState {
    pub current_workers: usize,
    pub active_workers: i32,
    pub current_mrf_workers: i32,
    pub active_mrf_workers: i32,
    pub max_workers: usize,
    pub include_mrf_workers: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicationBackpressureResize {
    pub regular_workers: Option<ReplicationWorkerResize>,
    pub mrf_workers: Option<i32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationBackpressureRecommendation {
    KeepFast,
    SetPriorityAuto,
    Resize(ReplicationBackpressureResize),
    Noop,
}

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
    usize::try_from(non_negative).unwrap_or_default()
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

pub fn large_worker_backpressure_resize(
    existing_workers: usize,
    active_lrg_workers: i32,
    max_l_workers: usize,
) -> Option<ReplicationWorkerResize> {
    should_grow_large_workers(active_lrg_workers, max_l_workers).then_some(ReplicationWorkerResize {
        new_count: next_large_worker_count(existing_workers, max_l_workers),
        existing_count: existing_workers,
    })
}

pub fn replication_backpressure_recommendation(
    priority: &ReplicationPriority,
    state: ReplicationBackpressureState,
) -> ReplicationBackpressureRecommendation {
    match priority {
        ReplicationPriority::Fast => ReplicationBackpressureRecommendation::KeepFast,
        ReplicationPriority::Slow => ReplicationBackpressureRecommendation::SetPriorityAuto,
        ReplicationPriority::Auto => {
            let regular_workers =
                next_regular_worker_count(state.current_workers, state.active_workers, state.max_workers).map(|new_count| {
                    ReplicationWorkerResize {
                        new_count,
                        existing_count: state.current_workers,
                    }
                });
            let mrf_workers = state
                .include_mrf_workers
                .then(|| next_mrf_worker_count(state.current_mrf_workers, state.active_mrf_workers, state.max_workers))
                .flatten();

            if regular_workers.is_none() && mrf_workers.is_none() {
                ReplicationBackpressureRecommendation::Noop
            } else {
                ReplicationBackpressureRecommendation::Resize(ReplicationBackpressureResize {
                    regular_workers,
                    mrf_workers,
                })
            }
        }
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
    i32::try_from(value).unwrap_or(i32::MAX)
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

    #[test]
    fn large_worker_resize_recommendation_respects_growth_limit() {
        assert_eq!(
            large_worker_backpressure_resize(3, 2, LARGE_WORKER_COUNT),
            Some(ReplicationWorkerResize {
                new_count: 4,
                existing_count: 3
            })
        );
        assert_eq!(
            large_worker_backpressure_resize(LARGE_WORKER_COUNT, usize_to_i32_saturating(LARGE_WORKER_COUNT), LARGE_WORKER_COUNT),
            None
        );
    }

    #[test]
    fn queue_backpressure_recommendation_preserves_priority_semantics() {
        let state = ReplicationBackpressureState {
            current_workers: 4,
            active_workers: 4,
            current_mrf_workers: 2,
            active_mrf_workers: 2,
            max_workers: WORKER_MAX_LIMIT,
            include_mrf_workers: true,
        };

        assert_eq!(
            replication_backpressure_recommendation(&ReplicationPriority::Fast, state),
            ReplicationBackpressureRecommendation::KeepFast
        );
        assert_eq!(
            replication_backpressure_recommendation(&ReplicationPriority::Slow, state),
            ReplicationBackpressureRecommendation::SetPriorityAuto
        );

        assert_eq!(
            replication_backpressure_recommendation(&ReplicationPriority::Auto, state),
            ReplicationBackpressureRecommendation::Resize(ReplicationBackpressureResize {
                regular_workers: Some(ReplicationWorkerResize {
                    new_count: 5,
                    existing_count: 4
                }),
                mrf_workers: Some(3),
            })
        );
    }

    #[test]
    fn delete_queue_backpressure_skips_mrf_growth() {
        let state = ReplicationBackpressureState {
            current_workers: 4,
            active_workers: 4,
            current_mrf_workers: 2,
            active_mrf_workers: 2,
            max_workers: WORKER_MAX_LIMIT,
            include_mrf_workers: false,
        };

        assert_eq!(
            replication_backpressure_recommendation(&ReplicationPriority::Auto, state),
            ReplicationBackpressureRecommendation::Resize(ReplicationBackpressureResize {
                regular_workers: Some(ReplicationWorkerResize {
                    new_count: 5,
                    existing_count: 4
                }),
                mrf_workers: None,
            })
        );
    }
}
