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

//! Example demonstrating metrics and configuration usage.

use rustfs_io_metrics::{
    AccessTracker, AdaptiveTTL, CacheConfig, CacheSettings, IoConfig, IoSchedulerSettings, record_cache_size,
};
use std::time::Duration;

fn main() {
    println!("=== rustfs-io-metrics Example ===\n");

    // 1. Cache configuration example
    cache_config_example();

    // 2. Adaptive TTL example
    adaptive_ttl_example();

    // 3. Access tracking example
    access_tracker_example();

    // 4. Unified configuration example
    unified_config_example();

    // 5. Metrics recording example
    metrics_recording_example();
}

fn cache_config_example() {
    println!("--- Cache Configuration ---");

    // Create default configuration
    let config = CacheConfig::new();
    println!("  Max capacity: {}", config.max_capacity);
    println!("  Default TTL: {} seconds", config.default_ttl().as_secs());
    println!("  Max memory: {} bytes", config.max_memory_bytes);

    // Validate configuration
    match config.validate() {
        Ok(()) => println!("  Validation: passed"),
        Err(e) => println!("  Validation: failed - {}", e),
    }

    // Custom configuration
    let custom_config = CacheConfig::new().with_max_capacity(5000).with_ttl_range(60, 600, 3600);
    println!("  Custom capacity: {}", custom_config.max_capacity);

    println!();
}

fn adaptive_ttl_example() {
    println!("--- Adaptive TTL ---");

    let config = CacheConfig::new().with_ttl_range(60, 300, 3600);
    let ttl = AdaptiveTTL::new(config);

    // Calculate TTL for different access frequencies
    let access_counts = [0u64, 1, 3, 5, 10, 20];
    for count in access_counts {
        let calculated = ttl.calculate_ttl(Duration::from_secs(60), count, 0.8);
        println!("  Access {} times: TTL = {} seconds", count, calculated.as_secs());
    }

    // Check if should evict early
    let should_evict = ttl.should_evict_early(1, Duration::from_secs(30), Duration::from_secs(300));
    println!("  Should evict low-frequency item early: {}", should_evict);

    println!();
}

fn access_tracker_example() {
    println!("--- Access Tracking ---");

    let mut tracker = AccessTracker::new(100, Duration::from_secs(300));

    // Simulate accesses
    let objects = [("hot-object", 10), ("warm-object", 5), ("cold-object", 1)];

    for (key, count) in objects {
        for _ in 0..count {
            tracker.record_access(key, 1024);
        }
    }

    // Query access information
    for (key, _) in objects {
        let count = tracker.get_access_count(key);
        let is_hot = tracker.is_hot(key, 5);
        let is_cold = tracker.is_cold(key, 5);
        println!("  {}: count={}, hot={}, cold={}", key, count, is_hot, is_cold);
    }

    // Get top keys
    let top_keys = tracker.top_keys(3);
    println!("  Top keys: {:?}", top_keys);

    println!();
}

fn unified_config_example() {
    println!("--- Unified Configuration ---");

    let config = IoConfig::new()
        .with_cache(
            CacheSettings::new()
                .with_max_capacity(5000)
                .with_ttl(Duration::from_secs(600)),
        )
        .with_scheduler(IoSchedulerSettings::new().with_max_concurrent_reads(64));

    println!("  Cache capacity: {}", config.cache.max_capacity);
    println!("  Cache TTL: {:?}", config.cache.default_ttl);
    println!("  Max concurrent reads: {}", config.scheduler.max_concurrent_reads);
    println!("  Backpressure high watermark: {}", config.backpressure.high_watermark);
    println!("  Default timeout: {:?}", config.timeout.default_timeout);

    println!();
}

fn metrics_recording_example() {
    println!("--- Metrics Recording ---");

    // Record cache operations
    for i in 0..10 {
        if i % 3 == 0 {
            record_cache_size("L1", 0, 0); // miss
        } else {
            record_cache_size("L1", 1024, 1); // hit
        }
    }

    println!("  Recorded 10 cache operations (hits: 7, misses: 3)");
    println!("  Metrics reported via metrics crate");
    println!("  Export via rustfs-obs OTEL pipeline");

    println!();
}
