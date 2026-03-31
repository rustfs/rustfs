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

//! Example demonstrating I/O scheduler usage.

use rustfs_io_core::io_profile::StorageMedia;
use rustfs_io_core::{
    BackpressureMonitor, BackpressureState, DeadlockDetector, IoLoadLevel, IoScheduler, IoSchedulerConfig, KI_B, LockOptimizer,
    LockType, MI_B, calculate_optimal_buffer_size, get_buffer_size_for_media,
};
use std::time::Duration;

fn main() {
    println!("=== rustfs-io-core Example ===\n");

    // 1. I/O scheduler example
    io_scheduler_example();

    // 2. Buffer size calculation example
    buffer_size_example();

    // 3. Backpressure control example
    backpressure_example();

    // 4. Deadlock detection example
    deadlock_detection_example();

    // 5. Lock optimizer example
    lock_optimizer_example();
}

fn io_scheduler_example() {
    println!("--- I/O Scheduler ---");

    // Create scheduler with configuration
    let config = IoSchedulerConfig {
        max_concurrent_reads: 64,
        base_buffer_size: 64 * KI_B,
        max_buffer_size: MI_B,
        ..Default::default()
    };
    let scheduler = IoScheduler::new(config);

    println!("  Max concurrent reads: {}", scheduler.config().max_concurrent_reads);
    println!("  Base buffer size: {} KB", scheduler.config().base_buffer_size / KI_B);
    println!("  Max buffer size: {} KB", scheduler.config().max_buffer_size / KI_B);

    // Calculate buffer sizes for different scenarios
    let scenarios = [
        ("Small file", 10 * KI_B as i64, true, StorageMedia::Ssd),
        ("Medium file", MI_B as i64, true, StorageMedia::Ssd),
        ("Large sequential", 100 * MI_B as i64, true, StorageMedia::Ssd),
        ("Large random", 100 * MI_B as i64, false, StorageMedia::Ssd),
        ("NVMe large", 100 * MI_B as i64, true, StorageMedia::Nvme),
        ("HDD large", 100 * MI_B as i64, true, StorageMedia::Hdd),
    ];

    for (name, size, sequential, media) in scenarios {
        let buffer = calculate_optimal_buffer_size(size, 64 * KI_B, sequential, 4, media, IoLoadLevel::Low);
        println!("  {}: {} bytes ({} KB)", name, buffer, buffer / KI_B);
    }

    println!();
}

fn buffer_size_example() {
    println!("--- Buffer Size Calculation ---");

    // Comprehensive calculation
    let size1 = calculate_optimal_buffer_size(10 * MI_B as i64, 64 * KI_B, true, 4, StorageMedia::Ssd, IoLoadLevel::Low);
    println!("  Comprehensive (10MB, sequential, SSD): {} KB", size1 / KI_B);

    // Media type optimization
    let media_types = [
        StorageMedia::Nvme,
        StorageMedia::Ssd,
        StorageMedia::Hdd,
        StorageMedia::Unknown,
    ];
    for media in media_types {
        let size = get_buffer_size_for_media(64 * KI_B, media);
        println!("  {} optimized: {} KB", media.as_str(), size / KI_B);
    }

    println!();
}

fn backpressure_example() {
    println!("--- Backpressure Control ---");

    let monitor = BackpressureMonitor::with_defaults();

    // Check initial state
    let state = monitor.state();
    let state_str = match state {
        BackpressureState::Normal => "Normal",
        BackpressureState::Warning => "Warning",
        BackpressureState::Critical => "Critical",
    };
    println!("  Initial state: {}", state_str);

    // Check if active
    let is_active = monitor.is_active();
    println!("  Backpressure active: {}", is_active);

    // Try to acquire permit
    if monitor.try_acquire() {
        println!("  Successfully acquired permit");
        monitor.release();
        println!("  Released permit");
    }

    // View statistics
    println!("  Total processed: {}", monitor.total_processed());
    println!("  Total rejected: {}", monitor.total_rejected());

    println!();
}

fn deadlock_detection_example() {
    println!("--- Deadlock Detection ---");

    let detector = DeadlockDetector::with_defaults();

    // Register locks
    let mutex1 = detector.register_lock(LockType::Mutex);
    let mutex2 = detector.register_lock(LockType::Mutex);
    println!("  Registered locks: mutex1={}, mutex2={}", mutex1, mutex2);

    // Simulate normal operation
    detector.record_acquire(mutex1, 1); // Thread 1 acquires mutex1
    detector.record_acquire(mutex2, 2); // Thread 2 acquires mutex2
    println!("  Normal operation: no deadlock");

    // Detect deadlock
    if detector.detect_deadlock().is_none() {
        println!("  Detection result: no deadlock");
    }

    // Simulate deadlock scenario
    detector.record_wait(mutex2, 1); // Thread 1 waits for mutex2
    detector.record_wait(mutex1, 2); // Thread 2 waits for mutex1

    // Detect deadlock
    if let Some(deadlock) = detector.detect_deadlock() {
        println!("  Detection result: deadlock found {:?}", deadlock);
    }

    // Cleanup
    detector.unregister_lock(mutex1);
    detector.unregister_lock(mutex2);
    println!();
}

fn lock_optimizer_example() {
    println!("--- Lock Optimizer ---");

    let optimizer = LockOptimizer::with_defaults();

    // Simulate lock operations
    for _i in 0..5 {
        optimizer.on_acquire();
        // Simulate work
        std::thread::sleep(Duration::from_millis(10));
        optimizer.on_release(Duration::from_millis(10));
    }

    // View statistics
    let stats = optimizer.stats();
    let acquired = stats.total_acquired();
    let avg_hold = stats.avg_hold_time();
    let contention = stats.contention_rate();

    println!("  Locks acquired: {}", acquired);
    println!("  Average hold time: {:?}", avg_hold);
    println!("  Contention rate: {:.2}%", contention * 100.0);

    println!();
}
