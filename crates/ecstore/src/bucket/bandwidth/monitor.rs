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
        self.t_lock.write().unwrap().retain(|opts, _| opts.name != bucket);
    }

    pub fn delete_bucket_throttle(&self, bucket: &str, arn: &str) {
        let opts = BucketOptions {
            name: bucket.to_string(),
            replication_arn: arn.to_string(),
        };
        self.t_lock.write().unwrap().remove(&opts);
    }

    pub fn throttle(&self, opts: &BucketOptions) -> Option<BucketThrottle> {
        self.t_lock.read().unwrap().get(opts).cloned()
    }

    pub fn set_bandwidth_limit(&self, bucket: &str, arn: &str, limit: i64) {
        let limit_bytes = limit / self.node_count as i64;
        let opts = BucketOptions {
            name: bucket.to_string(),
            replication_arn: arn.to_string(),
        };
        let throttle = BucketThrottle::new(limit_bytes);
        self.t_lock.write().unwrap().insert(opts, throttle);
    }

    pub fn is_throttled(&self, bucket: &str, arn: &str) -> bool {
        let opt = BucketOptions {
            name: bucket.to_string(),
            replication_arn: arn.to_string(),
        };
        self.t_lock.read().unwrap().contains_key(&opt)
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
}
