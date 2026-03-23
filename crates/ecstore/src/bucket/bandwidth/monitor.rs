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

use crate::bucket::bandwidth::reader::BucketOptions;
use ratelimit::{Error as RatelimitError, Ratelimiter};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use tracing::warn;

/// BETA_BUCKET is the weight used to calculate exponential moving average
const BETA_BUCKET: f64 = 0.1;

#[derive(Clone)]
pub struct BucketThrottle {
    limiter: Arc<Mutex<Ratelimiter>>,
    pub node_bandwidth_per_sec: i64,
}

impl BucketThrottle {
    fn new(node_bandwidth_per_sec: i64) -> Result<Self, RatelimitError> {
        let node_bandwidth_per_sec = node_bandwidth_per_sec.max(1);
        let amount = node_bandwidth_per_sec as u64;
        let limiter_inner = Ratelimiter::builder(amount).max_tokens(amount).build()?;
        Ok(Self {
            limiter: Arc::new(Mutex::new(limiter_inner)),
            node_bandwidth_per_sec,
        })
    }

    pub fn burst(&self) -> u64 {
        self.limiter.lock().unwrap_or_else(|e| e.into_inner()).max_tokens()
    }

    /// The ratelimit crate (0.10.0) does not provide a bulk token consumption API.
    /// try_wait() first to consume 1 token AND trigger the internal refill
    /// mechanism (tokens are only refilled during try_wait/wait calls).
    /// directly adjust available tokens via set_available() to consume the remaining amount.
    pub(crate) fn consume(&self, n: u64) -> (u64, f64, u64) {
        let guard = self.limiter.lock().unwrap_or_else(|e| {
            warn!("bucket throttle mutex poisoned, recovering");
            e.into_inner()
        });
        if n == 0 {
            return (0, guard.rate() as f64, 0);
        }
        let mut consumed = 0u64;
        if guard.try_wait().is_ok() {
            consumed = 1;
        }
        let available = guard.available();
        let to_consume = n - consumed;
        let batch = to_consume.min(available);
        if batch > 0 {
            let _ = guard.set_rate(available - batch);
            consumed += batch;
        }
        let deficit = n.saturating_sub(consumed);
        let rate = guard.rate();
        (deficit, rate as f64, consumed)
    }
}

#[derive(Debug)]
pub struct BucketMeasurement {
    bytes_since_last_window: AtomicU64,
    start_time: Mutex<Option<Instant>>,
    exp_moving_avg: Mutex<f64>,
}

impl BucketMeasurement {
    pub fn new(init_time: Instant) -> Self {
        Self {
            bytes_since_last_window: AtomicU64::new(0),
            start_time: Mutex::new(Some(init_time)),
            exp_moving_avg: Mutex::new(0.0),
        }
    }

    pub fn increment_bytes(&self, bytes: u64) {
        self.bytes_since_last_window.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn update_exponential_moving_average(&self, end_time: Instant) {
        let mut start_time = self.start_time.lock().unwrap_or_else(|e| {
            warn!("bucket measurement start_time mutex poisoned, recovering");
            e.into_inner()
        });
        let previous_start = *start_time;
        *start_time = Some(end_time);

        let Some(prev_start) = previous_start else {
            return;
        };

        if prev_start > end_time {
            return;
        }

        let duration = end_time.duration_since(prev_start);
        if duration.is_zero() {
            return;
        }

        let bytes_since_last_window = self.bytes_since_last_window.swap(0, Ordering::Relaxed);
        let increment = bytes_since_last_window as f64 / duration.as_secs_f64();

        let mut exp_moving_avg = self.exp_moving_avg.lock().unwrap_or_else(|e| {
            warn!("bucket measurement exp_moving_avg mutex poisoned, recovering");
            e.into_inner()
        });

        if *exp_moving_avg == 0.0 {
            *exp_moving_avg = increment;
            return;
        }

        *exp_moving_avg = exponential_moving_average(BETA_BUCKET, *exp_moving_avg, increment);
    }

    pub fn get_exp_moving_avg_bytes_per_second(&self) -> f64 {
        *self.exp_moving_avg.lock().unwrap_or_else(|e| {
            warn!("bucket measurement exp_moving_avg mutex poisoned, recovering");
            e.into_inner()
        })
    }
}

fn exponential_moving_average(beta: f64, previous_avg: f64, increment_avg: f64) -> f64 {
    (1f64 - beta) * increment_avg + beta * previous_avg
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BandwidthDetails {
    pub limit_bytes_per_sec: i64,
    pub current_bandwidth_bytes_per_sec: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketBandwidthReport {
    pub bucket_stats: HashMap<BucketOptions, BandwidthDetails>,
}

pub struct Monitor {
    t_lock: RwLock<HashMap<BucketOptions, BucketThrottle>>,
    m_lock: RwLock<HashMap<BucketOptions, BucketMeasurement>>,
    pub node_count: u64,
}

impl Monitor {
    pub fn new(num_nodes: u64) -> Arc<Self> {
        let node_cnt = num_nodes.max(1);
        let m = Arc::new(Monitor {
            t_lock: RwLock::new(HashMap::new()),
            m_lock: RwLock::new(HashMap::new()),
            node_count: node_cnt,
        });
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let weak = Arc::downgrade(&m);
            handle.spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(2));
                loop {
                    interval.tick().await;
                    let Some(monitor) = weak.upgrade() else { break };
                    monitor.update_moving_avg();
                }
            });
        }
        m
    }

    pub fn init_measurement(&self, opts: &BucketOptions) {
        let mut guard = self.m_lock.write().unwrap_or_else(|e| {
            warn!("bucket monitor measurement rwlock write poisoned, recovering");
            e.into_inner()
        });
        guard
            .entry(opts.clone())
            .or_insert_with(|| BucketMeasurement::new(Instant::now()));
    }

    pub fn update_measurement(&self, opts: &BucketOptions, bytes: u64) {
        {
            let guard = self.m_lock.read().unwrap_or_else(|e| {
                warn!("bucket monitor measurement rwlock read poisoned, recovering");
                e.into_inner()
            });
            if let Some(measurement) = guard.get(opts) {
                measurement.increment_bytes(bytes);
                return;
            }
        }

        // Miss path: write lock + insert once, then increment.
        let mut guard = self.m_lock.write().unwrap_or_else(|e| {
            warn!("bucket monitor measurement rwlock write poisoned, recovering");
            e.into_inner()
        });

        // Double-check after lock upgrade in case another thread inserted it.
        let measurement = guard
            .entry(opts.clone())
            .or_insert_with(|| BucketMeasurement::new(Instant::now()));

        measurement.increment_bytes(bytes);
    }

    pub fn update_moving_avg(&self) {
        let now = Instant::now();
        let guard = self.m_lock.read().unwrap_or_else(|e| {
            warn!("bucket monitor measurement rwlock read poisoned, recovering");
            e.into_inner()
        });
        for measurement in guard.values() {
            measurement.update_exponential_moving_average(now);
        }
    }

    pub fn get_report(&self, select_bucket: impl Fn(&str) -> bool) -> BucketBandwidthReport {
        let t_guard = self.t_lock.read().unwrap_or_else(|e| {
            warn!("bucket monitor throttle rwlock read poisoned, recovering");
            e.into_inner()
        });
        let m_guard = self.m_lock.read().unwrap_or_else(|e| {
            warn!("bucket monitor measurement rwlock read poisoned, recovering");
            e.into_inner()
        });
        let mut bucket_stats = HashMap::new();
        for (opts, throttle) in t_guard.iter() {
            if !select_bucket(&opts.name) {
                continue;
            }
            let mut current_bandwidth_bytes_per_sec = 0.0;
            if let Some(measurement) = m_guard.get(opts) {
                current_bandwidth_bytes_per_sec = measurement.get_exp_moving_avg_bytes_per_second();
            }
            bucket_stats.insert(
                opts.clone(),
                BandwidthDetails {
                    limit_bytes_per_sec: throttle.node_bandwidth_per_sec * self.node_count as i64,
                    current_bandwidth_bytes_per_sec,
                },
            );
        }
        BucketBandwidthReport { bucket_stats }
    }

    pub fn delete_bucket(&self, bucket: &str) {
        self.t_lock
            .write()
            .unwrap_or_else(|e| {
                warn!("bucket monitor throttle rwlock write poisoned, recovering");
                e.into_inner()
            })
            .retain(|opts, _| opts.name != bucket);
        self.m_lock
            .write()
            .unwrap_or_else(|e| {
                warn!("bucket monitor measurement rwlock write poisoned, recovering");
                e.into_inner()
            })
            .retain(|opts, _| opts.name != bucket);
    }

    pub fn delete_bucket_throttle(&self, bucket: &str, arn: &str) {
        let opts = BucketOptions {
            name: bucket.to_string(),
            replication_arn: arn.to_string(),
        };
        self.t_lock
            .write()
            .unwrap_or_else(|e| {
                warn!("bucket monitor throttle rwlock write poisoned, recovering");
                e.into_inner()
            })
            .remove(&opts);
        self.m_lock
            .write()
            .unwrap_or_else(|e| {
                warn!("bucket monitor measurement rwlock write poisoned, recovering");
                e.into_inner()
            })
            .remove(&opts);
    }

    pub fn throttle(&self, opts: &BucketOptions) -> Option<BucketThrottle> {
        self.t_lock
            .read()
            .unwrap_or_else(|e| {
                warn!("bucket monitor throttle rwlock read poisoned, recovering");
                e.into_inner()
            })
            .get(opts)
            .cloned()
    }

    pub fn set_bandwidth_limit(&self, bucket: &str, arn: &str, limit: i64) {
        if limit <= 0 {
            warn!(
                bucket = bucket,
                arn = arn,
                limit = limit,
                "invalid bandwidth limit, must be positive; ignoring"
            );
            return;
        }
        let limit_bytes = limit / self.node_count as i64;
        if limit_bytes == 0 && limit > 0 {
            warn!(
                bucket = bucket,
                arn = arn,
                limit = limit,
                node_count = self.node_count,
                "bandwidth limit too small for cluster size, per-node limit will clamp to 1 byte/s"
            );
        }
        let opts = BucketOptions {
            name: bucket.to_string(),
            replication_arn: arn.to_string(),
        };
        let throttle = match BucketThrottle::new(limit_bytes) {
            Ok(t) => t,
            Err(e) => {
                warn!(
                    bucket = bucket,
                    arn = arn,
                    limit_bytes = limit_bytes,
                    err = %e,
                    "failed to build bandwidth throttle, throttling disabled for this target"
                );
                return;
            }
        };
        self.t_lock
            .write()
            .unwrap_or_else(|e| {
                warn!("bucket monitor throttle rwlock write poisoned, recovering");
                e.into_inner()
            })
            .insert(opts, throttle);
    }

    pub fn is_throttled(&self, bucket: &str, arn: &str) -> bool {
        let opt = BucketOptions {
            name: bucket.to_string(),
            replication_arn: arn.to_string(),
        };
        self.t_lock
            .read()
            .unwrap_or_else(|e| {
                warn!("bucket monitor throttle rwlock read poisoned, recovering");
                e.into_inner()
            })
            .contains_key(&opt)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::panic::{AssertUnwindSafe, catch_unwind};

    #[test]
    fn test_set_and_get_throttle_with_node_split() {
        let monitor = Monitor::new(4);
        monitor.set_bandwidth_limit("b1", "arn1", 400);

        let throttle = monitor
            .throttle(&BucketOptions {
                name: "b1".to_string(),
                replication_arn: "arn1".to_string(),
            })
            .expect("throttle should exist");

        assert_eq!(throttle.node_bandwidth_per_sec, 100);
        assert!(monitor.is_throttled("b1", "arn1"));
    }

    #[test]
    fn test_delete_bucket_throttle() {
        let monitor = Monitor::new(2);
        monitor.set_bandwidth_limit("b1", "arn1", 200);
        assert!(monitor.is_throttled("b1", "arn1"));

        monitor.delete_bucket_throttle("b1", "arn1");
        assert!(!monitor.is_throttled("b1", "arn1"));
    }

    #[test]
    fn test_delete_bucket_removes_all_arns() {
        let monitor = Monitor::new(1);
        monitor.set_bandwidth_limit("b1", "arn1", 100);
        monitor.set_bandwidth_limit("b1", "arn2", 100);
        monitor.set_bandwidth_limit("b2", "arn3", 100);

        monitor.delete_bucket("b1");

        assert!(!monitor.is_throttled("b1", "arn1"));
        assert!(!monitor.is_throttled("b1", "arn2"));
        assert!(monitor.is_throttled("b2", "arn3"));
    }

    #[test]
    fn test_set_bandwidth_limit_ignores_non_positive() {
        let monitor = Monitor::new(2);

        monitor.set_bandwidth_limit("b1", "arn1", 0);
        assert!(!monitor.is_throttled("b1", "arn1"));

        monitor.set_bandwidth_limit("b1", "arn1", -10);
        assert!(!monitor.is_throttled("b1", "arn1"));
    }

    #[test]
    fn test_consume_returns_deficit_when_tokens_exhausted() {
        let throttle = BucketThrottle::new(100).expect("test");

        let (deficit, rate, _consumed) = throttle.consume(200);

        assert!(deficit > 0);
        assert!(rate > 0.0);
    }

    #[test]
    fn test_consume_no_deficit_when_tokens_sufficient() {
        let throttle = BucketThrottle::new(10000).expect("test");

        std::thread::sleep(std::time::Duration::from_millis(1100));

        let (deficit, _rate, _consumed) = throttle.consume(5000);
        assert_eq!(deficit, 0);
    }

    #[test]
    fn test_burst_equals_bandwidth() {
        let throttle = BucketThrottle::new(500).expect("test");
        assert_eq!(throttle.burst(), 500);
    }

    #[test]
    fn test_concurrent_consume() {
        let throttle = BucketThrottle::new(10000).expect("test");
        std::thread::sleep(std::time::Duration::from_millis(1100));

        let mut handles = vec![];
        for _ in 0..10 {
            let t = throttle.clone();
            handles.push(std::thread::spawn(move || t.consume(100)));
        }

        let mut total_deficit = 0u64;
        let mut total_consumed = 0u64;
        for h in handles {
            let (deficit, _rate, consumed) = h.join().unwrap();
            total_consumed += consumed;
            total_deficit += deficit;
        }

        assert_eq!(total_consumed + total_deficit, 1000);
        assert!(total_consumed <= 10000);
    }

    #[test]
    fn test_zero_bandwidth_clamped_to_one() {
        let throttle = BucketThrottle::new(0).expect("test");
        assert_eq!(throttle.burst(), 1);
        assert_eq!(throttle.node_bandwidth_per_sec, 1);
    }

    #[test]
    fn test_negative_bandwidth_clamped_to_one() {
        let throttle = BucketThrottle::new(-100).expect("test");
        assert_eq!(throttle.burst(), 1);
        assert_eq!(throttle.node_bandwidth_per_sec, 1);
    }

    #[test]
    fn test_update_bandwidth_limit_overrides_previous() {
        let monitor = Monitor::new(1);
        monitor.set_bandwidth_limit("b1", "arn1", 1000);

        let opts = BucketOptions {
            name: "b1".to_string(),
            replication_arn: "arn1".to_string(),
        };
        let t1 = monitor.throttle(&opts).expect("throttle should exist");
        assert_eq!(t1.burst(), 1000);
        assert_eq!(t1.node_bandwidth_per_sec, 1000);

        monitor.set_bandwidth_limit("b1", "arn1", 500);

        let t2 = monitor.throttle(&opts).expect("throttle should exist after update");
        assert_eq!(t2.burst(), 500);
        assert_eq!(t2.node_bandwidth_per_sec, 500);
    }

    #[test]
    fn test_bucket_measurement_recovers_from_poisoned_mutexes() {
        let measurement = BucketMeasurement::new(Instant::now());

        let _ = catch_unwind(AssertUnwindSafe(|| {
            let _guard = measurement.start_time.lock().unwrap();
            panic!("poison start_time mutex");
        }));
        measurement.increment_bytes(64);
        measurement.update_exponential_moving_average(Instant::now() + Duration::from_secs(1));

        let _ = catch_unwind(AssertUnwindSafe(|| {
            let _guard = measurement.exp_moving_avg.lock().unwrap();
            panic!("poison exp_moving_avg mutex");
        }));
        measurement.increment_bytes(32);
        measurement.update_exponential_moving_average(Instant::now() + Duration::from_secs(2));

        let value = measurement.get_exp_moving_avg_bytes_per_second();
        assert!(value.is_finite());
        assert!(value >= 0.0);
    }

    #[test]
    fn test_get_report_limit_and_current_bandwidth_after_measurement() {
        let monitor = Monitor::new(4);
        monitor.set_bandwidth_limit("b1", "arn1", 400);
        let opts = BucketOptions {
            name: "b1".to_string(),
            replication_arn: "arn1".to_string(),
        };
        monitor.init_measurement(&opts);
        monitor.update_measurement(&opts, 500);
        monitor.update_measurement(&opts, 500);
        std::thread::sleep(Duration::from_millis(110));
        monitor.update_moving_avg();

        let report = monitor.get_report(|name| name == "b1");
        let details = report.bucket_stats.get(&opts).expect("report should contain b1/arn1");
        assert_eq!(details.limit_bytes_per_sec, 400);
        assert!(
            details.current_bandwidth_bytes_per_sec > 0.0,
            "current_bandwidth should be positive after update_measurement and update_moving_avg"
        );
        assert!(
            details.current_bandwidth_bytes_per_sec < 20000.0,
            "current_bandwidth should be in reasonable range"
        );
    }

    #[test]
    fn test_get_report_select_bucket_filters() {
        let monitor = Monitor::new(2);
        monitor.set_bandwidth_limit("b1", "arn1", 100);
        monitor.set_bandwidth_limit("b2", "arn2", 200);
        let opts_b1 = BucketOptions {
            name: "b1".to_string(),
            replication_arn: "arn1".to_string(),
        };
        let opts_b2 = BucketOptions {
            name: "b2".to_string(),
            replication_arn: "arn2".to_string(),
        };
        monitor.init_measurement(&opts_b1);
        monitor.init_measurement(&opts_b2);

        let report_all = monitor.get_report(|_| true);
        assert_eq!(report_all.bucket_stats.len(), 2);

        let report_b1 = monitor.get_report(|name| name == "b1");
        assert_eq!(report_b1.bucket_stats.len(), 1);
        assert_eq!(report_b1.bucket_stats.get(&opts_b1).unwrap().limit_bytes_per_sec, 100);

        let report_b2 = monitor.get_report(|name| name == "b2");
        assert_eq!(report_b2.bucket_stats.len(), 1);
        assert_eq!(report_b2.bucket_stats.get(&opts_b2).unwrap().limit_bytes_per_sec, 200);
    }
}
