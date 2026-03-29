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
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StorageMedia {
    Nvme,
    Ssd,
    Hdd,
    Unknown,
}

impl StorageMedia {
    #[allow(dead_code)]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Nvme => "nvme",
            Self::Ssd => "ssd",
            Self::Hdd => "hdd",
            Self::Unknown => "unknown",
        }
    }
}

impl FromStr for StorageMedia {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "nvme" => Ok(Self::Nvme),
            "ssd" => Ok(Self::Ssd),
            "hdd" => Ok(Self::Hdd),
            "unknown" => Ok(Self::Unknown),
            _ => Err(()),
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
    #[allow(dead_code)]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Sequential => "sequential",
            Self::Random => "random",
            Self::Mixed => "mixed",
            Self::Unknown => "unknown",
        }
    }

    /// Check if this is a sequential access pattern.
    #[allow(dead_code)]
    pub fn is_sequential(&self) -> bool {
        matches!(self, Self::Sequential)
    }

    /// Check if this is a random access pattern.
    #[allow(dead_code)]
    pub fn is_random(&self) -> bool {
        matches!(self, Self::Random)
    }

    /// Check if this is a mixed access pattern.
    #[allow(dead_code)]
    pub fn is_mixed(&self) -> bool {
        matches!(self, Self::Mixed)
    }

    /// Check if this pattern is unknown.
    #[allow(dead_code)]
    pub fn is_unknown(&self) -> bool {
        matches!(self, Self::Unknown)
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
    if let Ok(media) = StorageMedia::from_str(storage_media_override) {
        return media;
    }

    if !storage_detection_enabled {
        return StorageMedia::Unknown;
    }

    // Try platform-specific detection
    #[cfg(target_os = "linux")]
    {
        if let Ok(media) = detect_linux_storage_media()
            && media != StorageMedia::Unknown
        {
            return media;
        }
    }

    #[cfg(target_os = "macos")]
    {
        if let Ok(media) = detect_macos_storage_media()
            && media != StorageMedia::Unknown
        {
            return media;
        }
    }

    StorageMedia::Unknown
}

#[cfg(target_os = "linux")]
fn detect_linux_storage_media() -> Result<StorageMedia, std::io::Error> {
    use std::path::Path;

    // Try to detect NVMe devices first
    if Path::new("/sys/class/nvme").exists() {
        // Check if there are any NVMe devices
        if let Ok(entries) = std::fs::read_dir("/sys/class/nvme") {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name_str = name.to_string_lossy();
                if name_str.starts_with("nvme") {
                    return Ok(StorageMedia::Nvme);
                }
            }
        }
    }

    // Check rotational flag for common block devices (sda, sdb, etc.)
    for device in &["sda", "sdb", "nvme0n1", "vda"] {
        let rotational_path = format!("/sys/block/{}/queue/rotational", device);
        if let Ok(content) = std::fs::read_to_string(&rotational_path) {
            let rotational = content.trim().parse::<u32>().unwrap_or(1);
            if rotational == 0 {
                // Non-rotating = SSD/NVMe
                // If device name starts with "nvme", it's NVMe
                if device.starts_with("nvme") {
                    return Ok(StorageMedia::Nvme);
                }
                return Ok(StorageMedia::Ssd);
            } else {
                // Rotating = HDD
                return Ok(StorageMedia::Hdd);
            }
        }
    }

    Ok(StorageMedia::Unknown)
}

#[cfg(target_os = "macos")]
fn detect_macos_storage_media() -> Result<StorageMedia, std::io::Error> {
    use std::process::Command;

    // Use diskutil to get disk information
    let output = Command::new("diskutil").args(["info", "/"]).output()?;

    if !output.status.success() {
        return Ok(StorageMedia::Unknown);
    }

    let info = String::from_utf8_lossy(&output.stdout);

    // Check for NVMe
    if info.contains("NVMe") || info.contains("nvme") {
        return Ok(StorageMedia::Nvme);
    }

    // Check for SSD indicators
    if info.contains("Solid State") || info.contains("SSD") || info.contains("Solid-State") {
        return Ok(StorageMedia::Ssd);
    }

    // Check for HDD/rotational indicators
    // Note: macOS typically doesn't explicitly say "HDD", so we assume HDD if not SSD/NVMe
    // when detection is enabled
    if info.contains("Rotational") || info.contains("HDD") {
        return Ok(StorageMedia::Hdd);
    }

    // Default to SSD for modern Macs (most are SSD-based)
    // This is a reasonable default for macOS systems
    Ok(StorageMedia::Ssd)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn detect_platform_storage_media() -> Result<StorageMedia, std::io::Error> {
    Ok(StorageMedia::Unknown)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_media_override() {
        // Override should always take precedence
        assert_eq!(detect_storage_media(true, "nvme"), StorageMedia::Nvme);
        assert_eq!(detect_storage_media(false, "ssd"), StorageMedia::Ssd);
        assert_eq!(detect_storage_media(true, "hdd"), StorageMedia::Hdd);
        assert_eq!(detect_storage_media(false, "unknown"), StorageMedia::Unknown);
    }

    #[test]
    fn test_storage_media_from_str() {
        assert_eq!(StorageMedia::from_str("nvme"), Ok(StorageMedia::Nvme));
        assert_eq!(StorageMedia::from_str("NVMe"), Ok(StorageMedia::Nvme));
        assert_eq!(StorageMedia::from_str("ssd"), Ok(StorageMedia::Ssd));
        assert_eq!(StorageMedia::from_str("SSD"), Ok(StorageMedia::Ssd));
        assert_eq!(StorageMedia::from_str("hdd"), Ok(StorageMedia::Hdd));
        assert_eq!(StorageMedia::from_str("HDD"), Ok(StorageMedia::Hdd));
        assert_eq!(StorageMedia::from_str("unknown"), Ok(StorageMedia::Unknown));
        assert_eq!(StorageMedia::from_str("invalid"), Err(()));
        assert_eq!(StorageMedia::from_str(""), Err(()));
    }

    #[test]
    fn test_storage_media_as_str() {
        assert_eq!(StorageMedia::Nvme.as_str(), "nvme");
        assert_eq!(StorageMedia::Ssd.as_str(), "ssd");
        assert_eq!(StorageMedia::Hdd.as_str(), "hdd");
        assert_eq!(StorageMedia::Unknown.as_str(), "unknown");
    }

    #[test]
    fn test_storage_detection_disabled() {
        // When detection is disabled and no override, should return Unknown
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

    #[test]
    fn test_pattern_detector_mixed() {
        let mut detector = IoPatternDetector::new(10, 1024);
        detector.record(0, 4096); // Sequential to 4096
        detector.record(4096, 4096); // Sequential to 8192
        detector.record(65536, 4096); // Random jump
        detector.record(98304, 4096); // Sequential from random position
        assert_eq!(detector.current_pattern(), AccessPattern::Mixed);
    }

    #[test]
    fn test_pattern_detector_insufficient_history() {
        let detector = IoPatternDetector::new(10, 1024);
        // No records yet
        assert_eq!(detector.current_pattern(), AccessPattern::Unknown);

        // Only one record
        let mut detector = IoPatternDetector::new(10, 1024);
        detector.record(0, 4096);
        assert_eq!(detector.current_pattern(), AccessPattern::Unknown);
    }

    #[test]
    fn test_access_pattern_helpers() {
        assert!(AccessPattern::Sequential.is_sequential());
        assert!(!AccessPattern::Sequential.is_random());
        assert!(!AccessPattern::Sequential.is_mixed());
        assert!(!AccessPattern::Sequential.is_unknown());

        assert!(AccessPattern::Random.is_random());
        assert!(!AccessPattern::Random.is_sequential());

        assert!(AccessPattern::Mixed.is_mixed());
        assert!(!AccessPattern::Mixed.is_sequential());
        assert!(!AccessPattern::Mixed.is_random());

        assert!(AccessPattern::Unknown.is_unknown());
        assert!(!AccessPattern::Unknown.is_sequential());
    }

    #[test]
    fn test_storage_profile_for_media() {
        let nvme_cap = 2 * 1024 * 1024;
        let ssd_cap = 1024 * 1024;
        let hdd_cap = 512 * 1024;

        let nvme_profile = StorageProfile::for_media(StorageMedia::Nvme, nvme_cap, ssd_cap, hdd_cap);
        assert_eq!(nvme_profile.media, StorageMedia::Nvme);
        assert_eq!(nvme_profile.buffer_cap, nvme_cap);
        assert_eq!(nvme_profile.sequential_boost_multiplier, 1.35);
        assert_eq!(nvme_profile.random_penalty_multiplier, 0.9);
        assert!(nvme_profile.prefers_readahead);

        let ssd_profile = StorageProfile::for_media(StorageMedia::Ssd, nvme_cap, ssd_cap, hdd_cap);
        assert_eq!(ssd_profile.media, StorageMedia::Ssd);
        assert_eq!(ssd_profile.buffer_cap, ssd_cap);
        assert_eq!(ssd_profile.sequential_boost_multiplier, 1.2);
        assert_eq!(ssd_profile.random_penalty_multiplier, 0.8);

        let hdd_profile = StorageProfile::for_media(StorageMedia::Hdd, nvme_cap, ssd_cap, hdd_cap);
        assert_eq!(hdd_profile.media, StorageMedia::Hdd);
        assert_eq!(hdd_profile.buffer_cap, hdd_cap);
        assert_eq!(hdd_profile.sequential_boost_multiplier, 1.1);
        assert_eq!(hdd_profile.random_penalty_multiplier, 0.65);
        assert!(!hdd_profile.prefers_readahead);

        let unknown_profile = StorageProfile::for_media(StorageMedia::Unknown, nvme_cap, ssd_cap, hdd_cap);
        assert_eq!(unknown_profile.media, StorageMedia::Unknown);
        // Unknown media uses SSD cap
        assert_eq!(unknown_profile.buffer_cap, ssd_cap);
        assert_eq!(unknown_profile.sequential_boost_multiplier, 1.0);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_linux_storage_detection_exists() {
        // This test just verifies the detection function exists and doesn't panic
        // The actual result depends on the system it's running on
        let result = detect_storage_media(true, "");
        // We should get some result (not panic)
        match result {
            StorageMedia::Nvme | StorageMedia::Ssd | StorageMedia::Hdd | StorageMedia::Unknown => {
                // All valid results
            }
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn test_macos_storage_detection_exists() {
        // This test just verifies the detection function exists and doesn't panic
        let result = detect_storage_media(true, "");
        // We should get some result (not panic)
        match result {
            StorageMedia::Nvme | StorageMedia::Ssd | StorageMedia::Hdd | StorageMedia::Unknown => {
                // All valid results
            }
        }
    }
}
