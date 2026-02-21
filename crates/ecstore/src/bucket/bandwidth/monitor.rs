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
use ratelimit::Ratelimiter;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use tokio::time::sleep;
use tracing::warn;

#[derive(Clone)]
pub struct BucketThrottle {
    limiter: Arc<Mutex<Ratelimiter>>,
    pub node_bandwidth_per_sec: i64,
}

impl BucketThrottle {
    fn new(node_bandwidth_per_sec: i64) -> Self {
        let node_bandwidth_per_sec = node_bandwidth_per_sec.max(1);
        let amount = node_bandwidth_per_sec as u64;
        let limiter = Arc::new(Mutex::new(
            Ratelimiter::builder(amount, Duration::from_secs(1))
                .max_tokens(amount)
                .build()
                .unwrap(),
        ));
        Self {
            limiter,
            node_bandwidth_per_sec,
        }
    }

    pub fn burst(&self) -> u64 {
        self.limiter.lock().unwrap().max_tokens()
    }

    pub fn tokens(&self) -> u64 {
        let guard = self.limiter.lock().unwrap();
        let _ = guard.try_wait();
        guard.available()
    }

    pub fn wait_n(&self, n: u64) -> std::io::Result<()> {
        if n == 0 {
            return Ok(());
        }
        let (deficit, rate) = self.consume(n);
        if deficit > 0 && rate > 0.0 {
            std::thread::sleep(Duration::from_secs_f64(deficit as f64 / rate));
        }
        Ok(())
    }

    pub async fn wait_n_async(&self, n: u64) {
        if n == 0 {
            return;
        }
        let (deficit, rate) = self.consume(n);
        if deficit > 0 && rate > 0.0 {
            sleep(Duration::from_secs_f64(deficit as f64 / rate)).await;
        }
    }

    /// The ratelimit crate (0.10.0) does not provide a bulk token consumption API.
    /// try_wait() first to consume 1 token AND trigger the internal refill
    /// mechanism (tokens are only refilled during try_wait/wait calls).
    /// directly adjust available tokens via set_available() to consume the remaining amount.
    pub(crate) fn consume(&self, n: u64) -> (u64, f64) {
        let guard = self.limiter.lock().unwrap();
        let mut consumed = 0u64;
        if guard.try_wait().is_ok() {
            consumed = 1;
        }
        if consumed < n {
            let available = guard.available();
            let batch = (n - consumed).min(available);
            if batch > 0 {
                let _ = guard.set_available(available - batch);
                consumed += batch;
            }
        }
        let deficit = n.saturating_sub(consumed);
        let rate = guard.rate();
        (deficit, rate)
    }
}

pub struct Monitor {
    t_lock: RwLock<HashMap<BucketOptions, BucketThrottle>>,
    pub node_count: u64,
}

impl Monitor {
    pub fn new(num_nodes: u64) -> Arc<Self> {
        let node_cnt = num_nodes.max(1);
        Arc::new(Monitor {
            t_lock: RwLock::new(HashMap::new()),
            node_count: node_cnt,
        })
    }

    pub fn delete_bucket(&self, bucket: &str) {
        self.t_lock
            .write()
            .expect("bandwidth monitor lock poisoned")
            .retain(|opts, _| opts.name != bucket);
    }

    pub fn delete_bucket_throttle(&self, bucket: &str, arn: &str) {
        let opts = BucketOptions {
            name: bucket.to_string(),
            replication_arn: arn.to_string(),
        };
        self.t_lock.write().expect("bandwidth monitor lock poisoned").remove(&opts);
    }

    pub fn throttle(&self, opts: &BucketOptions) -> Option<BucketThrottle> {
        self.t_lock
            .read()
            .expect("bandwidth monitor lock poisoned")
            .get(opts)
            .cloned()
    }

    pub fn set_bandwidth_limit(&self, bucket: &str, arn: &str, limit: i64) {
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
        let throttle = BucketThrottle::new(limit_bytes);
        self.t_lock
            .write()
            .expect("bandwidth monitor lock poisoned")
            .insert(opts, throttle);
    }

    pub fn is_throttled(&self, bucket: &str, arn: &str) -> bool {
        let opt = BucketOptions {
            name: bucket.to_string(),
            replication_arn: arn.to_string(),
        };
        self.t_lock
            .read()
            .expect("bandwidth monitor lock poisoned")
            .contains_key(&opt)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_consume_returns_deficit_when_tokens_exhausted() {
        let throttle = BucketThrottle::new(100);

        let (deficit, rate) = throttle.consume(200);

        assert!(deficit > 0);
        assert!(rate > 0.0);
    }

    #[test]
    fn test_consume_no_deficit_when_tokens_sufficient() {
        let throttle = BucketThrottle::new(10000);

        std::thread::sleep(std::time::Duration::from_millis(1100));

        let (deficit, _rate) = throttle.consume(5000);
        assert_eq!(deficit, 0);
    }

    #[test]
    fn test_burst_equals_bandwidth() {
        let throttle = BucketThrottle::new(500);
        assert_eq!(throttle.burst(), 500);
    }

    #[test]
    fn test_wait_n_zero_returns_immediately() {
        let throttle = BucketThrottle::new(1);
        let start = std::time::Instant::now();
        throttle.wait_n(0).unwrap();
        assert!(start.elapsed() < std::time::Duration::from_millis(10));
    }

    #[test]
    fn test_wait_n_blocks_on_deficit() {
        let throttle = BucketThrottle::new(100);

        let _ = throttle.consume(200);

        let start = std::time::Instant::now();
        throttle.wait_n(100).unwrap();
        let elapsed = start.elapsed();

        assert!(elapsed >= std::time::Duration::from_millis(500));
    }

    #[test]
    fn test_concurrent_consume() {
        let throttle = BucketThrottle::new(10000);
        std::thread::sleep(std::time::Duration::from_millis(1100));

        let mut handles = vec![];
        for _ in 0..10 {
            let t = throttle.clone();
            handles.push(std::thread::spawn(move || t.consume(100)));
        }

        let mut total_deficit = 0u64;
        let mut total_consumed = 0u64;
        for h in handles {
            let (deficit, _) = h.join().unwrap();
            total_consumed += 100 - deficit;
            total_deficit += deficit;
        }

        assert_eq!(total_consumed + total_deficit, 1000);
        assert!(total_consumed <= 10000);
    }

    #[test]
    fn test_zero_bandwidth_clamped_to_one() {
        let throttle = BucketThrottle::new(0);
        assert_eq!(throttle.burst(), 1);
        assert_eq!(throttle.node_bandwidth_per_sec, 1);
    }

    #[test]
    fn test_negative_bandwidth_clamped_to_one() {
        let throttle = BucketThrottle::new(-100);
        assert_eq!(throttle.burst(), 1);
        assert_eq!(throttle.node_bandwidth_per_sec, 1);
    }
}
