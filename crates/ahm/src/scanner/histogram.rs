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

use std::collections::HashMap;

/// Size interval for object size histogram
#[derive(Debug, Clone)]
pub struct SizeInterval {
    pub start: u64,
    pub end: u64,
    pub name: &'static str,
}

/// Version interval for object versions histogram
#[derive(Debug, Clone)]
pub struct VersionInterval {
    pub start: u64,
    pub end: u64,
    pub name: &'static str,
}

/// Object size histogram intervals
pub const OBJECTS_HISTOGRAM_INTERVALS: &[SizeInterval] = &[
    SizeInterval {
        start: 0,
        end: 1024 - 1,
        name: "LESS_THAN_1_KiB",
    },
    SizeInterval {
        start: 1024,
        end: 1024 * 1024 - 1,
        name: "1_KiB_TO_1_MiB",
    },
    SizeInterval {
        start: 1024 * 1024,
        end: 10 * 1024 * 1024 - 1,
        name: "1_MiB_TO_10_MiB",
    },
    SizeInterval {
        start: 10 * 1024 * 1024,
        end: 64 * 1024 * 1024 - 1,
        name: "10_MiB_TO_64_MiB",
    },
    SizeInterval {
        start: 64 * 1024 * 1024,
        end: 128 * 1024 * 1024 - 1,
        name: "64_MiB_TO_128_MiB",
    },
    SizeInterval {
        start: 128 * 1024 * 1024,
        end: 512 * 1024 * 1024 - 1,
        name: "128_MiB_TO_512_MiB",
    },
    SizeInterval {
        start: 512 * 1024 * 1024,
        end: u64::MAX,
        name: "MORE_THAN_512_MiB",
    },
];

/// Object version count histogram intervals
pub const OBJECTS_VERSION_COUNT_INTERVALS: &[VersionInterval] = &[
    VersionInterval {
        start: 1,
        end: 1,
        name: "1_VERSION",
    },
    VersionInterval {
        start: 2,
        end: 10,
        name: "2_TO_10_VERSIONS",
    },
    VersionInterval {
        start: 11,
        end: 100,
        name: "11_TO_100_VERSIONS",
    },
    VersionInterval {
        start: 101,
        end: 1000,
        name: "101_TO_1000_VERSIONS",
    },
    VersionInterval {
        start: 1001,
        end: u64::MAX,
        name: "MORE_THAN_1000_VERSIONS",
    },
];

/// Size histogram for object size distribution
#[derive(Debug, Clone, Default)]
pub struct SizeHistogram {
    counts: Vec<u64>,
}

/// Versions histogram for object version count distribution
#[derive(Debug, Clone, Default)]
pub struct VersionsHistogram {
    counts: Vec<u64>,
}

impl SizeHistogram {
    /// Create a new size histogram
    pub fn new() -> Self {
        Self {
            counts: vec![0; OBJECTS_HISTOGRAM_INTERVALS.len()],
        }
    }

    /// Add a size to the histogram
    pub fn add(&mut self, size: u64) {
        for (idx, interval) in OBJECTS_HISTOGRAM_INTERVALS.iter().enumerate() {
            if size >= interval.start && size <= interval.end {
                self.counts[idx] += 1;
                break;
            }
        }
    }

    /// Get the histogram as a map
    pub fn to_map(&self) -> HashMap<String, u64> {
        let mut result = HashMap::new();
        for (idx, count) in self.counts.iter().enumerate() {
            let interval = &OBJECTS_HISTOGRAM_INTERVALS[idx];
            result.insert(interval.name.to_string(), *count);
        }
        result
    }

    /// Merge another histogram into this one
    pub fn merge(&mut self, other: &SizeHistogram) {
        for (idx, count) in other.counts.iter().enumerate() {
            self.counts[idx] += count;
        }
    }

    /// Get total count
    pub fn total_count(&self) -> u64 {
        self.counts.iter().sum()
    }

    /// Reset the histogram
    pub fn reset(&mut self) {
        for count in &mut self.counts {
            *count = 0;
        }
    }
}

impl VersionsHistogram {
    /// Create a new versions histogram
    pub fn new() -> Self {
        Self {
            counts: vec![0; OBJECTS_VERSION_COUNT_INTERVALS.len()],
        }
    }

    /// Add a version count to the histogram
    pub fn add(&mut self, versions: u64) {
        for (idx, interval) in OBJECTS_VERSION_COUNT_INTERVALS.iter().enumerate() {
            if versions >= interval.start && versions <= interval.end {
                self.counts[idx] += 1;
                break;
            }
        }
    }

    /// Get the histogram as a map
    pub fn to_map(&self) -> HashMap<String, u64> {
        let mut result = HashMap::new();
        for (idx, count) in self.counts.iter().enumerate() {
            let interval = &OBJECTS_VERSION_COUNT_INTERVALS[idx];
            result.insert(interval.name.to_string(), *count);
        }
        result
    }

    /// Merge another histogram into this one
    pub fn merge(&mut self, other: &VersionsHistogram) {
        for (idx, count) in other.counts.iter().enumerate() {
            self.counts[idx] += count;
        }
    }

    /// Get total count
    pub fn total_count(&self) -> u64 {
        self.counts.iter().sum()
    }

    /// Reset the histogram
    pub fn reset(&mut self) {
        for count in &mut self.counts {
            *count = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_size_histogram() {
        let mut histogram = SizeHistogram::new();

        // Add some sizes
        histogram.add(512); // LESS_THAN_1_KiB
        histogram.add(1024); // 1_KiB_TO_1_MiB
        histogram.add(1024 * 1024); // 1_MiB_TO_10_MiB
        histogram.add(5 * 1024 * 1024); // 1_MiB_TO_10_MiB

        let map = histogram.to_map();

        assert_eq!(map.get("LESS_THAN_1_KiB"), Some(&1));
        assert_eq!(map.get("1_KiB_TO_1_MiB"), Some(&1));
        assert_eq!(map.get("1_MiB_TO_10_MiB"), Some(&2));
        assert_eq!(map.get("10_MiB_TO_64_MiB"), Some(&0));
    }

    #[test]
    fn test_versions_histogram() {
        let mut histogram = VersionsHistogram::new();

        // Add some version counts
        histogram.add(1); // 1_VERSION
        histogram.add(5); // 2_TO_10_VERSIONS
        histogram.add(50); // 11_TO_100_VERSIONS
        histogram.add(500); // 101_TO_1000_VERSIONS

        let map = histogram.to_map();

        assert_eq!(map.get("1_VERSION"), Some(&1));
        assert_eq!(map.get("2_TO_10_VERSIONS"), Some(&1));
        assert_eq!(map.get("11_TO_100_VERSIONS"), Some(&1));
        assert_eq!(map.get("101_TO_1000_VERSIONS"), Some(&1));
    }

    #[test]
    fn test_histogram_merge() {
        let mut histogram1 = SizeHistogram::new();
        histogram1.add(1024);
        histogram1.add(1024 * 1024);

        let mut histogram2 = SizeHistogram::new();
        histogram2.add(1024);
        histogram2.add(5 * 1024 * 1024);

        histogram1.merge(&histogram2);

        let map = histogram1.to_map();
        assert_eq!(map.get("1_KiB_TO_1_MiB"), Some(&2)); // 1 from histogram1 + 1 from histogram2
        assert_eq!(map.get("1_MiB_TO_10_MiB"), Some(&2)); // 1 from histogram1 + 1 from histogram2
    }

    #[test]
    fn test_histogram_reset() {
        let mut histogram = SizeHistogram::new();
        histogram.add(1024);
        histogram.add(1024 * 1024);

        assert_eq!(histogram.total_count(), 2);

        histogram.reset();
        assert_eq!(histogram.total_count(), 0);
    }
}
