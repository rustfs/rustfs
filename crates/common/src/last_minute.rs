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

use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[allow(dead_code)]
#[derive(Debug, Default)]
struct TimedAction {
    count: u64,
    acc_time: u64,
    min_time: Option<u64>,
    max_time: Option<u64>,
    bytes: u64,
}

#[allow(dead_code)]
impl TimedAction {
    // Avg returns the average time spent on the action.
    pub fn avg(&self) -> Option<Duration> {
        if self.count == 0 {
            return None;
        }
        Some(Duration::from_nanos(self.acc_time / self.count))
    }

    // AvgBytes returns the average bytes processed.
    pub fn avg_bytes(&self) -> u64 {
        if self.count == 0 {
            return 0;
        }
        self.bytes / self.count
    }

    // Merge other into t.
    pub fn merge(&mut self, other: TimedAction) {
        self.count += other.count;
        self.acc_time += other.acc_time;
        self.bytes += other.bytes;

        if self.count == 0 {
            self.min_time = other.min_time;
        }
        if let Some(other_min) = other.min_time {
            self.min_time = self.min_time.map_or(Some(other_min), |min| Some(min.min(other_min)));
        }

        self.max_time = self
            .max_time
            .map_or(other.max_time, |max| Some(max.max(other.max_time.unwrap_or(0))));
    }
}

#[allow(dead_code)]
#[derive(Debug)]
enum SizeCategory {
    SizeLessThan1KiB = 0,
    SizeLessThan1MiB,
    SizeLessThan10MiB,
    SizeLessThan100MiB,
    SizeLessThan1GiB,
    SizeGreaterThan1GiB,
    // Add new entries here
    SizeLastElemMarker,
}

impl std::fmt::Display for SizeCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match *self {
            SizeCategory::SizeLessThan1KiB => "SizeLessThan1KiB",
            SizeCategory::SizeLessThan1MiB => "SizeLessThan1MiB",
            SizeCategory::SizeLessThan10MiB => "SizeLessThan10MiB",
            SizeCategory::SizeLessThan100MiB => "SizeLessThan100MiB",
            SizeCategory::SizeLessThan1GiB => "SizeLessThan1GiB",
            SizeCategory::SizeGreaterThan1GiB => "SizeGreaterThan1GiB",
            SizeCategory::SizeLastElemMarker => "SizeLastElemMarker",
        };
        write!(f, "{s}")
    }
}

#[derive(Clone, Debug, Default, Copy)]
pub struct AccElem {
    pub total: u64,
    pub size: u64,
    pub n: u64,
}

impl AccElem {
    pub fn add(&mut self, dur: &Duration) {
        let dur = dur.as_secs();
        self.total = self.total.wrapping_add(dur);
        self.n = self.n.wrapping_add(1);
    }

    pub fn merge(&mut self, b: &AccElem) {
        self.n = self.n.wrapping_add(b.n);
        self.total = self.total.wrapping_add(b.total);
        self.size = self.size.wrapping_add(b.size);
    }

    pub fn avg(&self) -> Duration {
        if self.n >= 1 && self.total > 0 {
            return Duration::from_secs(self.total / self.n);
        }
        Duration::from_secs(0)
    }
}

#[derive(Clone, Debug)]
pub struct LastMinuteLatency {
    pub totals: Vec<AccElem>,
    pub last_sec: u64,
}

impl Default for LastMinuteLatency {
    fn default() -> Self {
        Self {
            totals: vec![AccElem::default(); 60],
            last_sec: Default::default(),
        }
    }
}

impl LastMinuteLatency {
    pub fn merge(&mut self, o: &LastMinuteLatency) -> LastMinuteLatency {
        let mut merged = LastMinuteLatency::default();
        let mut x = o.clone();
        if self.last_sec > o.last_sec {
            x.forward_to(self.last_sec);
            merged.last_sec = self.last_sec;
        } else {
            self.forward_to(o.last_sec);
            merged.last_sec = o.last_sec;
        }

        for i in 0..merged.totals.len() {
            merged.totals[i] = AccElem {
                total: self.totals[i].total + o.totals[i].total,
                n: self.totals[i].n + o.totals[i].n,
                size: self.totals[i].size + o.totals[i].size,
            }
        }
        merged
    }

    pub fn add(&mut self, t: &Duration) {
        let sec = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();
        self.forward_to(sec);
        let win_idx = sec % 60;
        self.totals[win_idx as usize].add(t);
        self.last_sec = sec;
    }

    pub fn add_all(&mut self, sec: u64, a: &AccElem) {
        self.forward_to(sec);
        let win_idx = sec % 60;
        self.totals[win_idx as usize].merge(a);
        self.last_sec = sec;
    }

    pub fn get_total(&mut self) -> AccElem {
        let mut res = AccElem::default();
        let sec = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();
        self.forward_to(sec);
        for elem in self.totals.iter() {
            res.merge(elem);
        }
        res
    }

    pub fn forward_to(&mut self, t: u64) {
        if self.last_sec >= t {
            return;
        }
        if t - self.last_sec >= 60 {
            self.totals = vec![AccElem::default(); 60];
            self.last_sec = t;
            return;
        }
        while self.last_sec != t {
            let idx = (self.last_sec + 1) % 60;
            self.totals[idx as usize] = AccElem::default();
            self.last_sec += 1;
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_acc_elem_default() {
        let elem = AccElem::default();
        assert_eq!(elem.total, 0);
        assert_eq!(elem.size, 0);
        assert_eq!(elem.n, 0);
    }

    #[test]
    fn test_acc_elem_add_single_duration() {
        let mut elem = AccElem::default();
        let duration = Duration::from_secs(5);

        elem.add(&duration);

        assert_eq!(elem.total, 5);
        assert_eq!(elem.n, 1);
        assert_eq!(elem.size, 0); // size is not modified by add
    }

    #[test]
    fn test_acc_elem_add_multiple_durations() {
        let mut elem = AccElem::default();

        elem.add(&Duration::from_secs(3));
        elem.add(&Duration::from_secs(7));
        elem.add(&Duration::from_secs(2));

        assert_eq!(elem.total, 12);
        assert_eq!(elem.n, 3);
        assert_eq!(elem.size, 0);
    }

    #[test]
    fn test_acc_elem_add_zero_duration() {
        let mut elem = AccElem::default();
        let duration = Duration::from_secs(0);

        elem.add(&duration);

        assert_eq!(elem.total, 0);
        assert_eq!(elem.n, 1);
    }

    #[test]
    fn test_acc_elem_add_subsecond_duration() {
        let mut elem = AccElem::default();
        // Duration less than 1 second should be truncated to 0
        let duration = Duration::from_millis(500);

        elem.add(&duration);

        assert_eq!(elem.total, 0); // as_secs() truncates subsecond values
        assert_eq!(elem.n, 1);
    }

    #[test]
    fn test_acc_elem_merge_empty_elements() {
        let mut elem1 = AccElem::default();
        let elem2 = AccElem::default();

        elem1.merge(&elem2);

        assert_eq!(elem1.total, 0);
        assert_eq!(elem1.size, 0);
        assert_eq!(elem1.n, 0);
    }

    #[test]
    fn test_acc_elem_merge_with_data() {
        let mut elem1 = AccElem {
            total: 10,
            size: 100,
            n: 2,
        };
        let elem2 = AccElem {
            total: 15,
            size: 200,
            n: 3,
        };

        elem1.merge(&elem2);

        assert_eq!(elem1.total, 25);
        assert_eq!(elem1.size, 300);
        assert_eq!(elem1.n, 5);
    }

    #[test]
    fn test_acc_elem_merge_one_empty() {
        let mut elem1 = AccElem {
            total: 10,
            size: 100,
            n: 2,
        };
        let elem2 = AccElem::default();

        elem1.merge(&elem2);

        assert_eq!(elem1.total, 10);
        assert_eq!(elem1.size, 100);
        assert_eq!(elem1.n, 2);
    }

    #[test]
    fn test_acc_elem_avg_with_data() {
        let elem = AccElem {
            total: 15,
            size: 0,
            n: 3,
        };

        let avg = elem.avg();
        assert_eq!(avg, Duration::from_secs(5)); // 15 / 3 = 5
    }

    #[test]
    fn test_acc_elem_avg_zero_count() {
        let elem = AccElem {
            total: 10,
            size: 0,
            n: 0,
        };

        let avg = elem.avg();
        assert_eq!(avg, Duration::from_secs(0));
    }

    #[test]
    fn test_acc_elem_avg_zero_total() {
        let elem = AccElem { total: 0, size: 0, n: 5 };

        let avg = elem.avg();
        assert_eq!(avg, Duration::from_secs(0));
    }

    #[test]
    fn test_acc_elem_avg_rounding() {
        let elem = AccElem {
            total: 10,
            size: 0,
            n: 3,
        };

        let avg = elem.avg();
        assert_eq!(avg, Duration::from_secs(3)); // 10 / 3 = 3 (integer division)
    }

    #[test]
    fn test_last_minute_latency_default() {
        let latency = LastMinuteLatency::default();

        assert_eq!(latency.totals.len(), 60);
        assert_eq!(latency.last_sec, 0);

        // All elements should be default (empty)
        for elem in &latency.totals {
            assert_eq!(elem.total, 0);
            assert_eq!(elem.size, 0);
            assert_eq!(elem.n, 0);
        }
    }

    #[test]
    fn test_last_minute_latency_forward_to_same_time() {
        let mut latency = LastMinuteLatency {
            last_sec: 100,
            ..Default::default()
        };

        // Add some data to verify it's not cleared
        latency.totals[0].total = 10;
        latency.totals[0].n = 1;

        latency.forward_to(100); // Same time

        assert_eq!(latency.last_sec, 100);
        assert_eq!(latency.totals[0].total, 10); // Data should remain
        assert_eq!(latency.totals[0].n, 1);
    }

    #[test]
    fn test_last_minute_latency_forward_to_past_time() {
        let mut latency = LastMinuteLatency {
            last_sec: 100,
            ..Default::default()
        };

        // Add some data to verify it's not cleared
        latency.totals[0].total = 10;
        latency.totals[0].n = 1;

        latency.forward_to(50); // Past time

        assert_eq!(latency.last_sec, 100); // Should not change
        assert_eq!(latency.totals[0].total, 10); // Data should remain
        assert_eq!(latency.totals[0].n, 1);
    }

    #[test]
    fn test_last_minute_latency_forward_to_large_gap() {
        let mut latency = LastMinuteLatency {
            last_sec: 100,
            ..Default::default()
        };

        // Add some data to verify it's cleared
        latency.totals[0].total = 10;
        latency.totals[0].n = 1;

        latency.forward_to(200); // Gap >= 60 seconds

        assert_eq!(latency.last_sec, 200); // last_sec should be updated to target time

        // All data should be cleared
        for elem in &latency.totals {
            assert_eq!(elem.total, 0);
            assert_eq!(elem.size, 0);
            assert_eq!(elem.n, 0);
        }
    }

    #[test]
    fn test_last_minute_latency_forward_to_small_gap() {
        let mut latency = LastMinuteLatency {
            last_sec: 100,
            ..Default::default()
        };

        // Add data at specific indices
        latency.totals[41].total = 10; // (100 + 1) % 60 = 41
        latency.totals[42].total = 20; // (100 + 2) % 60 = 42

        latency.forward_to(102); // Forward by 2 seconds

        assert_eq!(latency.last_sec, 102);

        // The slots that were advanced should be cleared
        assert_eq!(latency.totals[41].total, 0); // Cleared during forward
        assert_eq!(latency.totals[42].total, 0); // Cleared during forward
    }

    #[test]
    fn test_last_minute_latency_add_all() {
        let mut latency = LastMinuteLatency::default();
        let acc_elem = AccElem {
            total: 15,
            size: 100,
            n: 3,
        };

        latency.add_all(1000, &acc_elem);

        assert_eq!(latency.last_sec, 1000);
        let idx = 1000 % 60; // Should be 40
        assert_eq!(latency.totals[idx as usize].total, 15);
        assert_eq!(latency.totals[idx as usize].size, 100);
        assert_eq!(latency.totals[idx as usize].n, 3);
    }

    #[test]
    fn test_last_minute_latency_add_all_multiple() {
        let mut latency = LastMinuteLatency::default();

        let acc_elem1 = AccElem {
            total: 10,
            size: 50,
            n: 2,
        };
        let acc_elem2 = AccElem {
            total: 20,
            size: 100,
            n: 4,
        };

        latency.add_all(1000, &acc_elem1);
        latency.add_all(1000, &acc_elem2); // Same second

        let idx = 1000 % 60;
        assert_eq!(latency.totals[idx as usize].total, 30); // 10 + 20
        assert_eq!(latency.totals[idx as usize].size, 150); // 50 + 100
        assert_eq!(latency.totals[idx as usize].n, 6); // 2 + 4
    }

    #[test]
    fn test_last_minute_latency_merge_same_time() {
        let mut latency1 = LastMinuteLatency::default();
        let mut latency2 = LastMinuteLatency::default();

        latency1.last_sec = 1000;
        latency2.last_sec = 1000;

        // Add data to both
        latency1.totals[0].total = 10;
        latency1.totals[0].n = 2;
        latency2.totals[0].total = 20;
        latency2.totals[0].n = 3;

        let merged = latency1.merge(&latency2);

        assert_eq!(merged.last_sec, 1000);
        assert_eq!(merged.totals[0].total, 30); // 10 + 20
        assert_eq!(merged.totals[0].n, 5); // 2 + 3
    }

    #[test]
    fn test_last_minute_latency_merge_different_times() {
        let mut latency1 = LastMinuteLatency::default();
        let mut latency2 = LastMinuteLatency::default();

        latency1.last_sec = 1000;
        latency2.last_sec = 1010; // 10 seconds later

        // Add data to both
        latency1.totals[0].total = 10;
        latency2.totals[0].total = 20;

        let merged = latency1.merge(&latency2);

        assert_eq!(merged.last_sec, 1010); // Should use the later time
        assert_eq!(merged.totals[0].total, 30);
    }

    #[test]
    fn test_last_minute_latency_merge_empty() {
        let mut latency1 = LastMinuteLatency::default();
        let latency2 = LastMinuteLatency::default();

        let merged = latency1.merge(&latency2);

        assert_eq!(merged.last_sec, 0);
        for elem in &merged.totals {
            assert_eq!(elem.total, 0);
            assert_eq!(elem.size, 0);
            assert_eq!(elem.n, 0);
        }
    }

    #[test]
    fn test_last_minute_latency_window_wraparound() {
        let mut latency = LastMinuteLatency::default();

        // Test that indices wrap around correctly
        for sec in 0..120 {
            // Test for 2 minutes
            let acc_elem = AccElem {
                total: sec,
                size: 0,
                n: 1,
            };
            latency.add_all(sec, &acc_elem);

            let expected_idx = sec % 60;
            assert_eq!(latency.totals[expected_idx as usize].total, sec);
        }
    }

    #[test]
    fn test_last_minute_latency_time_progression() {
        let mut latency = LastMinuteLatency::default();

        // Add data at time 1000
        latency.add_all(
            1000,
            &AccElem {
                total: 10,
                size: 0,
                n: 1,
            },
        );

        // Forward to time 1030 (30 seconds later)
        latency.forward_to(1030);

        // Original data should still be there
        let idx_1000 = 1000 % 60;
        assert_eq!(latency.totals[idx_1000 as usize].total, 10);

        // Forward to time 1070 (70 seconds from original, > 60 seconds)
        latency.forward_to(1070);

        // All data should be cleared due to large gap
        for elem in &latency.totals {
            assert_eq!(elem.total, 0);
            assert_eq!(elem.n, 0);
        }
    }

    #[test]
    fn test_last_minute_latency_realistic_scenario() {
        let mut latency = LastMinuteLatency::default();
        let base_time = 1000u64;

        // Add data for exactly 60 seconds to fill the window
        for i in 0..60 {
            let current_time = base_time + i;
            let duration_secs = i % 10 + 1; // Varying durations 1-10 seconds
            let acc_elem = AccElem {
                total: duration_secs,
                size: 1024 * (i % 5 + 1), // Varying sizes
                n: 1,
            };

            latency.add_all(current_time, &acc_elem);
        }

        // Count non-empty slots after filling the window
        let mut non_empty_count = 0;
        let mut total_n = 0;
        let mut total_sum = 0;

        for elem in &latency.totals {
            if elem.n > 0 {
                non_empty_count += 1;
                total_n += elem.n;
                total_sum += elem.total;
            }
        }

        // We should have exactly 60 non-empty slots (one for each second in the window)
        assert_eq!(non_empty_count, 60);
        assert_eq!(total_n, 60); // 60 data points total
        assert!(total_sum > 0);

        // Test manual total calculation (get_total uses system time which interferes with test)
        let mut manual_total = AccElem::default();
        for elem in &latency.totals {
            manual_total.merge(elem);
        }
        assert_eq!(manual_total.n, 60);
        assert_eq!(manual_total.total, total_sum);
    }

    #[test]
    fn test_acc_elem_clone_and_debug() {
        let elem = AccElem {
            total: 100,
            size: 200,
            n: 5,
        };

        let cloned = elem;
        assert_eq!(elem.total, cloned.total);
        assert_eq!(elem.size, cloned.size);
        assert_eq!(elem.n, cloned.n);

        // Test Debug trait
        let debug_str = format!("{elem:?}");
        assert!(debug_str.contains("100"));
        assert!(debug_str.contains("200"));
        assert!(debug_str.contains("5"));
    }

    #[test]
    fn test_last_minute_latency_clone() {
        let mut latency = LastMinuteLatency {
            last_sec: 1000,
            ..Default::default()
        };
        latency.totals[0].total = 100;
        latency.totals[0].n = 5;

        let cloned = latency.clone();
        assert_eq!(latency.last_sec, cloned.last_sec);
        assert_eq!(latency.totals[0].total, cloned.totals[0].total);
        assert_eq!(latency.totals[0].n, cloned.totals[0].n);
    }

    #[test]
    fn test_edge_case_max_values() {
        let mut elem = AccElem {
            total: u64::MAX - 50,
            size: u64::MAX - 50,
            n: u64::MAX - 50,
        };

        let other = AccElem {
            total: 100,
            size: 100,
            n: 100,
        };

        // This should not panic due to overflow, values will wrap around
        elem.merge(&other);

        // Values should wrap around due to overflow (wrapping_add behavior)
        assert_eq!(elem.total, 49); // (u64::MAX - 50) + 100 wraps to 49
        assert_eq!(elem.size, 49);
        assert_eq!(elem.n, 49);
    }

    #[test]
    fn test_forward_to_boundary_conditions() {
        let mut latency = LastMinuteLatency {
            last_sec: 59,
            ..Default::default()
        };

        // Add data at the last slot
        latency.totals[59].total = 100;
        latency.totals[59].n = 1;

        // Forward exactly 60 seconds (boundary case)
        latency.forward_to(119);

        // All data should be cleared
        for elem in &latency.totals {
            assert_eq!(elem.total, 0);
            assert_eq!(elem.n, 0);
        }
    }

    #[test]
    fn test_get_total_with_data() {
        let mut latency = LastMinuteLatency::default();

        // Set a recent timestamp to avoid forward_to clearing data
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();
        latency.last_sec = current_time;

        // Add data to multiple slots
        latency.totals[0] = AccElem {
            total: 10,
            size: 100,
            n: 1,
        };
        latency.totals[1] = AccElem {
            total: 20,
            size: 200,
            n: 2,
        };
        latency.totals[59] = AccElem {
            total: 30,
            size: 300,
            n: 3,
        };

        let total = latency.get_total();

        assert_eq!(total.total, 60);
        assert_eq!(total.size, 600);
        assert_eq!(total.n, 6);
    }

    #[test]
    fn test_window_index_calculation() {
        // Test that window index calculation works correctly
        let _latency = LastMinuteLatency::default();

        let acc_elem = AccElem { total: 1, size: 1, n: 1 };

        // Test various timestamps
        let test_cases = [(0, 0), (1, 1), (59, 59), (60, 0), (61, 1), (119, 59), (120, 0)];

        for (timestamp, expected_idx) in test_cases {
            let mut test_latency = LastMinuteLatency::default();
            test_latency.add_all(timestamp, &acc_elem);

            assert_eq!(
                test_latency.totals[expected_idx].n, 1,
                "Failed for timestamp {timestamp} (expected index {expected_idx})"
            );
        }
    }

    #[test]
    fn test_concurrent_safety_simulation() {
        // Simulate concurrent access patterns
        let mut latency = LastMinuteLatency::default();

        // Use current time to ensure data doesn't get cleared by get_total
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();

        // Simulate rapid additions within a 60-second window
        for i in 0..1000 {
            let acc_elem = AccElem {
                total: (i % 10) + 1, // Ensure non-zero values
                size: (i % 100) + 1,
                n: 1,
            };
            // Keep all timestamps within the current minute window
            latency.add_all(current_time - (i % 60), &acc_elem);
        }

        let total = latency.get_total();
        assert!(total.n > 0, "Total count should be greater than 0");
        assert!(total.total > 0, "Total time should be greater than 0");
    }

    #[test]
    fn test_acc_elem_debug_format() {
        let elem = AccElem {
            total: 123,
            size: 456,
            n: 789,
        };

        let debug_str = format!("{elem:?}");
        assert!(debug_str.contains("123"));
        assert!(debug_str.contains("456"));
        assert!(debug_str.contains("789"));
    }

    #[test]
    fn test_large_values() {
        let mut elem = AccElem::default();

        // Test with large duration values
        let large_duration = Duration::from_secs(u64::MAX / 2);
        elem.add(&large_duration);

        assert_eq!(elem.total, u64::MAX / 2);
        assert_eq!(elem.n, 1);

        // Test average calculation with large values
        let avg = elem.avg();
        assert_eq!(avg, Duration::from_secs(u64::MAX / 2));
    }

    #[test]
    fn test_zero_duration_handling() {
        let mut elem = AccElem::default();

        let zero_duration = Duration::from_secs(0);
        elem.add(&zero_duration);

        assert_eq!(elem.total, 0);
        assert_eq!(elem.n, 1);
        assert_eq!(elem.avg(), Duration::from_secs(0));
    }
}

const SIZE_LAST_ELEM_MARKER: usize = 10; // Assumed marker size is 10, modify according to actual situation

#[allow(dead_code)]
#[derive(Debug, Default)]
pub struct LastMinuteHistogram {
    histogram: Vec<LastMinuteLatency>,
    size: u32,
}

impl LastMinuteHistogram {
    pub fn merge(&mut self, other: &LastMinuteHistogram) {
        for i in 0..self.histogram.len() {
            self.histogram[i].merge(&other.histogram[i]);
        }
    }

    pub fn add(&mut self, size: i64, t: Duration) {
        let index = size_to_tag(size);
        self.histogram[index].add(&t);
    }

    pub fn get_avg_data(&mut self) -> [AccElem; SIZE_LAST_ELEM_MARKER] {
        let mut res = [AccElem::default(); SIZE_LAST_ELEM_MARKER];
        for (i, elem) in self.histogram.iter_mut().enumerate() {
            res[i] = elem.get_total();
        }
        res
    }
}

fn size_to_tag(size: i64) -> usize {
    match size {
        _ if size < 1024 => 0,               // sizeLessThan1KiB
        _ if size < 1024 * 1024 => 1,        // sizeLessThan1MiB
        _ if size < 10 * 1024 * 1024 => 2,   // sizeLessThan10MiB
        _ if size < 100 * 1024 * 1024 => 3,  // sizeLessThan100MiB
        _ if size < 1024 * 1024 * 1024 => 4, // sizeLessThan1GiB
        _ => 5,                              // sizeGreaterThan1GiB
    }
}
