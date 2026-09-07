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

use std::fmt;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

/// Bounds a repetitive diagnostic without changing its underlying counters.
/// Each emitted event includes the number suppressed since the previous one.
pub struct LogThrottle {
    interval_ms: u64,
    last_ms: AtomicU64,
    suppressed: AtomicU64,
}

impl LogThrottle {
    pub const fn new(interval_ms: u64) -> Self {
        Self {
            interval_ms,
            last_ms: AtomicU64::new(u64::MAX),
            suppressed: AtomicU64::new(0),
        }
    }

    pub fn claim(&self) -> Option<u64> {
        static ANCHOR: OnceLock<std::time::Instant> = OnceLock::new();
        let now = ANCHOR.get_or_init(std::time::Instant::now).elapsed().as_millis();
        self.claim_at(u64::try_from(now).unwrap_or(u64::MAX - 1))
    }

    fn claim_at(&self, now: u64) -> Option<u64> {
        let last = self.last_ms.load(Ordering::Relaxed);
        if (last == u64::MAX || now.saturating_sub(last) >= self.interval_ms)
            && self
                .last_ms
                .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            Some(self.suppressed.swap(0, Ordering::Relaxed))
        } else {
            self.suppressed.fetch_add(1, Ordering::Relaxed);
            None
        }
    }
}

#[derive(Clone, Copy)]
pub struct MaskedAccessKey<'a>(pub &'a str);

impl fmt::Display for MaskedAccessKey<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = self.0;
        if value.is_empty() {
            return Ok(());
        }

        let chars: Vec<char> = value.chars().collect();
        match chars.len() {
            0 => Ok(()),
            1..=4 => f.write_str("***"),
            5..=8 => write!(f, "{}***{}", chars[0], chars[chars.len() - 1]),
            len => {
                for ch in &chars[..4] {
                    write!(f, "{ch}")?;
                }
                f.write_str("***")?;
                for ch in &chars[len - 4..] {
                    write!(f, "{ch}")?;
                }
                Ok(())
            }
        }
    }
}

impl fmt::Debug for MaskedAccessKey<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

#[cfg(test)]
mod tests {
    use super::{LogThrottle, MaskedAccessKey};

    #[test]
    fn log_throttle_emits_once_per_interval_and_reports_suppression() {
        let throttle = LogThrottle::new(5_000);
        assert_eq!(throttle.claim_at(0), Some(0));
        assert_eq!(throttle.claim_at(1), None);
        assert_eq!(throttle.claim_at(4_999), None);
        assert_eq!(throttle.claim_at(5_000), Some(2));
        assert_eq!(throttle.claim_at(5_001), None);
    }

    #[test]
    fn log_throttle_allows_only_one_concurrent_claim() {
        let throttle = LogThrottle::new(5_000);
        let reported = std::thread::scope(|scope| {
            let threads: Vec<_> = (0..16).map(|_| scope.spawn(|| throttle.claim_at(0))).collect();
            let emitted: Vec<_> = threads
                .into_iter()
                .filter_map(|thread| thread.join().expect("claim worker"))
                .collect();
            assert_eq!(emitted.len(), 1);
            emitted[0]
        });
        assert_eq!(reported + throttle.claim_at(5_000).expect("next window"), 15);
    }

    #[test]
    fn masks_short_values() {
        assert_eq!(MaskedAccessKey("").to_string(), "");
        assert_eq!(MaskedAccessKey("a").to_string(), "***");
        assert_eq!(MaskedAccessKey("abcd").to_string(), "***");
        assert_eq!(MaskedAccessKey("abcde").to_string(), "a***e");
        assert_eq!(MaskedAccessKey("abcdefgh").to_string(), "a***h");
    }

    #[test]
    fn masks_long_values() {
        assert_eq!(MaskedAccessKey("AKIAIOSFODNN7EXAMPLE").to_string(), "AKIA***MPLE");
        assert_eq!(format!("{:?}", MaskedAccessKey("keystone:user-1234")), "keys***1234");
    }
}
