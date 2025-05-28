use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug, Default)]
pub struct AccElem {
    pub total: u64,
    pub size: u64,
    pub n: u64,
}

impl AccElem {
    pub fn add(&mut self, dur: &Duration) {
        let dur = dur.as_secs();
        self.total += dur;
        self.n += 1;
    }

    pub fn merge(&mut self, b: &AccElem) {
        self.n += b.n;
        self.total += b.total;
        self.size += b.size;
    }

    pub fn avg(&self) -> Duration {
        if self.n >= 1 && self.total > 0 {
            return Duration::from_secs(self.total / self.n);
        }
        Duration::from_secs(0)
    }
}

#[derive(Clone)]
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
    pub fn merge(&mut self, o: &mut LastMinuteLatency) -> LastMinuteLatency {
        let mut merged = LastMinuteLatency::default();
        if self.last_sec > o.last_sec {
            o.forward_to(self.last_sec);
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

    #[test]
    fn test_acc_elem_default() {
        let elem = AccElem::default();
        assert_eq!(elem.total, 0);
        assert_eq!(elem.size, 0);
        assert_eq!(elem.n, 0);
    }

    #[test]
    fn test_acc_elem_add() {
        let mut elem = AccElem::default();

        // Add first duration
        let dur1 = Duration::from_secs(5);
        elem.add(&dur1);
        assert_eq!(elem.total, 5);
        assert_eq!(elem.n, 1);
        assert_eq!(elem.size, 0); // size is not modified by add

        // Add second duration
        let dur2 = Duration::from_secs(10);
        elem.add(&dur2);
        assert_eq!(elem.total, 15);
        assert_eq!(elem.n, 2);
    }

    #[test]
    fn test_acc_elem_add_with_subsecond_duration() {
        let mut elem = AccElem::default();

        // Add duration less than 1 second (should be truncated to 0)
        let dur = Duration::from_millis(500);
        elem.add(&dur);
        assert_eq!(elem.total, 0);
        assert_eq!(elem.n, 1);
    }

    #[test]
    fn test_acc_elem_merge() {
        let mut elem1 = AccElem {
            total: 10,
            size: 100,
            n: 2,
        };

        let elem2 = AccElem {
            total: 20,
            size: 200,
            n: 3,
        };

        elem1.merge(&elem2);
        assert_eq!(elem1.total, 30);
        assert_eq!(elem1.size, 300);
        assert_eq!(elem1.n, 5);
    }

    #[test]
    fn test_acc_elem_merge_with_empty() {
        let mut elem = AccElem {
            total: 10,
            size: 100,
            n: 2,
        };

        let empty_elem = AccElem::default();
        elem.merge(&empty_elem);

        assert_eq!(elem.total, 10);
        assert_eq!(elem.size, 100);
        assert_eq!(elem.n, 2);
    }

    #[test]
    fn test_acc_elem_avg() {
        // Test with valid data
        let elem = AccElem {
            total: 15,
            size: 0,
            n: 3,
        };
        assert_eq!(elem.avg(), Duration::from_secs(5));

        // Test with zero count
        let elem_zero_n = AccElem {
            total: 10,
            size: 0,
            n: 0,
        };
        assert_eq!(elem_zero_n.avg(), Duration::from_secs(0));

        // Test with zero total
        let elem_zero_total = AccElem {
            total: 0,
            size: 0,
            n: 5,
        };
        assert_eq!(elem_zero_total.avg(), Duration::from_secs(0));

        // Test with both zero
        let elem_both_zero = AccElem::default();
        assert_eq!(elem_both_zero.avg(), Duration::from_secs(0));
    }

    #[test]
    fn test_acc_elem_avg_with_single_element() {
        let elem = AccElem {
            total: 7,
            size: 0,
            n: 1,
        };
        assert_eq!(elem.avg(), Duration::from_secs(7));
    }

    #[test]
    fn test_last_minute_latency_default() {
        let latency = LastMinuteLatency::default();
        assert_eq!(latency.totals.len(), 60);
        assert_eq!(latency.last_sec, 0);

        // All elements should be default
        for elem in &latency.totals {
            assert_eq!(elem.total, 0);
            assert_eq!(elem.size, 0);
            assert_eq!(elem.n, 0);
        }
    }

    #[test]
    fn test_last_minute_latency_clone() {
        let mut latency = LastMinuteLatency {
            last_sec: 12345,
            ..Default::default()
        };
        latency.totals[0].total = 100;

        let cloned = latency.clone();
        assert_eq!(cloned.last_sec, 12345);
        assert_eq!(cloned.totals[0].total, 100);
        assert_eq!(cloned.totals.len(), 60);
    }

    #[test]
    fn test_forward_to_same_time() {
        let mut latency = LastMinuteLatency {
            last_sec: 100,
            ..Default::default()
        };

        // Forward to same time should not change anything
        latency.forward_to(100);
        assert_eq!(latency.last_sec, 100);

        // Forward to earlier time should not change anything
        latency.forward_to(99);
        assert_eq!(latency.last_sec, 100);
    }

    #[test]
    fn test_forward_to_large_gap() {
        let mut latency = LastMinuteLatency {
            last_sec: 100,
            ..Default::default()
        };
        latency.totals[0].total = 999; // Set some data

        // Forward by more than 60 seconds should reset all totals
        latency.forward_to(200);
        assert_eq!(latency.last_sec, 100); // last_sec is not updated in this case

        // All totals should be reset
        for elem in &latency.totals {
            assert_eq!(elem.total, 0);
            assert_eq!(elem.size, 0);
            assert_eq!(elem.n, 0);
        }
    }

    #[test]
    fn test_forward_to_small_gap() {
        let mut latency = LastMinuteLatency {
            last_sec: 100,
            ..Default::default()
        };
        latency.totals[1].total = 999; // Set some data at index 1

        // Forward by 2 seconds
        latency.forward_to(102);
        assert_eq!(latency.last_sec, 102);

        // Index 1 should still have data
        assert_eq!(latency.totals[1].total, 999);

        // Indices that were cleared should be zero
        assert_eq!(latency.totals[(101 % 60) as usize].total, 0);
        assert_eq!(latency.totals[(102 % 60) as usize].total, 0);
    }

    #[test]
    fn test_add_all() {
        let mut latency = LastMinuteLatency::default();
        let acc_elem = AccElem {
            total: 50,
            size: 1000,
            n: 5,
        };

        let test_sec = 12345;
        latency.add_all(test_sec, &acc_elem);

        assert_eq!(latency.last_sec, test_sec);
        let win_idx = (test_sec % 60) as usize;
        assert_eq!(latency.totals[win_idx].total, 50);
        assert_eq!(latency.totals[win_idx].size, 1000);
        assert_eq!(latency.totals[win_idx].n, 5);
    }

    #[test]
    fn test_add_all_multiple_times() {
        let mut latency = LastMinuteLatency::default();

        let acc_elem1 = AccElem {
            total: 10,
            size: 100,
            n: 1,
        };

        let acc_elem2 = AccElem {
            total: 20,
            size: 200,
            n: 2,
        };

        let test_sec = 12345;
        latency.add_all(test_sec, &acc_elem1);
        latency.add_all(test_sec, &acc_elem2);

        let win_idx = (test_sec % 60) as usize;
        assert_eq!(latency.totals[win_idx].total, 30);
        assert_eq!(latency.totals[win_idx].size, 300);
        assert_eq!(latency.totals[win_idx].n, 3);
    }

    #[test]
    fn test_merge_with_same_last_sec() {
        let mut latency1 = LastMinuteLatency::default();
        let mut latency2 = LastMinuteLatency::default();

        latency1.last_sec = 100;
        latency2.last_sec = 100;

        latency1.totals[0].total = 10;
        latency1.totals[0].n = 1;

        latency2.totals[0].total = 20;
        latency2.totals[0].n = 2;

        let merged = latency1.merge(&mut latency2);

        assert_eq!(merged.last_sec, 100);
        assert_eq!(merged.totals[0].total, 30);
        assert_eq!(merged.totals[0].n, 3);
    }

    #[test]
    fn test_merge_with_different_last_sec() {
        let mut latency1 = LastMinuteLatency::default();
        let mut latency2 = LastMinuteLatency::default();

        latency1.last_sec = 100;
        latency2.last_sec = 105;

        latency1.totals[0].total = 10;
        latency2.totals[5].total = 20;

        let merged = latency1.merge(&mut latency2);

        // Should use the later timestamp
        assert_eq!(merged.last_sec, 105);
    }

    #[test]
    fn test_merge_all_slots() {
        let mut latency1 = LastMinuteLatency::default();
        let mut latency2 = LastMinuteLatency::default();

        // Fill all slots with different values
        for i in 0..60 {
            latency1.totals[i].total = i as u64;
            latency1.totals[i].n = 1;

            latency2.totals[i].total = (i * 2) as u64;
            latency2.totals[i].n = 2;
        }

        let merged = latency1.merge(&mut latency2);

        for i in 0..60 {
            assert_eq!(merged.totals[i].total, (i + i * 2) as u64);
            assert_eq!(merged.totals[i].n, 3);
        }
    }

    #[test]
    fn test_get_total_empty() {
        let mut latency = LastMinuteLatency::default();
        let total = latency.get_total();

        assert_eq!(total.total, 0);
        assert_eq!(total.size, 0);
        assert_eq!(total.n, 0);
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
        latency.totals[0] = AccElem { total: 10, size: 100, n: 1 };
        latency.totals[1] = AccElem { total: 20, size: 200, n: 2 };
        latency.totals[59] = AccElem { total: 30, size: 300, n: 3 };

        let total = latency.get_total();

        assert_eq!(total.total, 60);
        assert_eq!(total.size, 600);
        assert_eq!(total.n, 6);
    }

    #[test]
    fn test_window_index_calculation() {
        // Test that window index calculation works correctly
        let _latency = LastMinuteLatency::default();

        let acc_elem = AccElem {
            total: 1,
            size: 1,
            n: 1,
        };

        // Test various timestamps
        let test_cases = [
            (0, 0),
            (1, 1),
            (59, 59),
            (60, 0),
            (61, 1),
            (119, 59),
            (120, 0),
        ];

        for (timestamp, expected_idx) in test_cases {
            let mut test_latency = LastMinuteLatency::default();
            test_latency.add_all(timestamp, &acc_elem);

            assert_eq!(test_latency.totals[expected_idx].n, 1,
                "Failed for timestamp {} (expected index {})", timestamp, expected_idx);
        }
    }

    #[test]
    fn test_edge_case_boundary_conditions() {
        let mut latency = LastMinuteLatency {
            last_sec: 59,
            ..Default::default()
        };

        // Test boundary at 60 seconds
        latency.forward_to(119); // Exactly 60 seconds later

        // Should reset all totals
        for elem in &latency.totals {
            assert_eq!(elem.total, 0);
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

        let debug_str = format!("{:?}", elem);
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
