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

//! I/O profile helpers for adaptive scheduling.

use std::collections::VecDeque;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StorageMedia {
    Nvme,
    Ssd,
    Hdd,
    Unknown,
}

impl StorageMedia {
    pub fn from_str(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "nvme" => Some(Self::Nvme),
            "ssd" => Some(Self::Ssd),
            "hdd" => Some(Self::Hdd),
            "unknown" => Some(Self::Unknown),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Nvme => "nvme",
            Self::Ssd => "ssd",
            Self::Hdd => "hdd",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AccessPattern {
    Sequential,
    Random,
    Mixed,
    Unknown,
}

impl AccessPattern {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Sequential => "sequential",
            Self::Random => "random",
            Self::Mixed => "mixed",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct StorageProfile {
    pub media: StorageMedia,
    pub buffer_cap: usize,
    pub sequential_boost_multiplier: f64,
    pub random_penalty_multiplier: f64,
    pub prefers_readahead: bool,
}

impl StorageProfile {
    pub fn for_media(media: StorageMedia, nvme_buffer_cap: usize, ssd_buffer_cap: usize, hdd_buffer_cap: usize) -> Self {
        match media {
            StorageMedia::Nvme => Self {
                media,
                buffer_cap: nvme_buffer_cap,
                sequential_boost_multiplier: 1.35,
                random_penalty_multiplier: 0.9,
                prefers_readahead: true,
            },
            StorageMedia::Ssd => Self {
                media,
                buffer_cap: ssd_buffer_cap,
                sequential_boost_multiplier: 1.2,
                random_penalty_multiplier: 0.8,
                prefers_readahead: true,
            },
            StorageMedia::Hdd => Self {
                media,
                buffer_cap: hdd_buffer_cap,
                sequential_boost_multiplier: 1.1,
                random_penalty_multiplier: 0.65,
                prefers_readahead: false,
            },
            StorageMedia::Unknown => Self {
                media,
                buffer_cap: ssd_buffer_cap,
                sequential_boost_multiplier: 1.0,
                random_penalty_multiplier: 0.8,
                prefers_readahead: true,
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct IoPatternDetector {
    history_size: usize,
    sequential_step_tolerance_bytes: u64,
    history: VecDeque<(u64, u64)>,
}

impl IoPatternDetector {
    pub fn new(history_size: usize, sequential_step_tolerance_bytes: u64) -> Self {
        Self {
            history_size: history_size.max(2),
            sequential_step_tolerance_bytes,
            history: VecDeque::with_capacity(history_size.max(2)),
        }
    }

    pub fn record(&mut self, offset: u64, len: u64) {
        if self.history.len() == self.history_size {
            self.history.pop_front();
        }
        self.history.push_back((offset, len));
    }

    pub fn current_pattern(&self) -> AccessPattern {
        if self.history.len() < 2 {
            return AccessPattern::Unknown;
        }

        let history = self.history.iter().copied().collect::<Vec<_>>();
        let mut sequential = 0usize;
        let mut random = 0usize;

        for window in history.windows(2) {
            let (prev_offset, prev_len) = window[0];
            let (curr_offset, _) = window[1];
            let prev_end = prev_offset.saturating_add(prev_len);
            if curr_offset.abs_diff(prev_end) <= self.sequential_step_tolerance_bytes {
                sequential += 1;
            } else {
                random += 1;
            }
        }

        match (sequential, random) {
            (0, 0) => AccessPattern::Unknown,
            (_, 0) => AccessPattern::Sequential,
            (0, _) => AccessPattern::Random,
            _ => AccessPattern::Mixed,
        }
    }
}

pub fn detect_storage_media(storage_detection_enabled: bool, storage_media_override: &str) -> StorageMedia {
    if let Some(media) = StorageMedia::from_str(storage_media_override) {
        return media;
    }

    if !storage_detection_enabled {
        return StorageMedia::Unknown;
    }

    StorageMedia::Unknown
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_media_override() {
        assert_eq!(detect_storage_media(true, "nvme"), StorageMedia::Nvme);
        assert_eq!(detect_storage_media(false, "ssd"), StorageMedia::Ssd);
        assert_eq!(detect_storage_media(false, ""), StorageMedia::Unknown);
    }

    #[test]
    fn test_pattern_detector_sequential() {
        let mut detector = IoPatternDetector::new(4, 1024);
        detector.record(0, 4096);
        detector.record(4096, 4096);
        detector.record(8192, 4096);
        assert_eq!(detector.current_pattern(), AccessPattern::Sequential);
    }

    #[test]
    fn test_pattern_detector_random() {
        let mut detector = IoPatternDetector::new(4, 1024);
        detector.record(0, 4096);
        detector.record(65536, 4096);
        detector.record(4096, 4096);
        assert_eq!(detector.current_pattern(), AccessPattern::Random);
    }
}
