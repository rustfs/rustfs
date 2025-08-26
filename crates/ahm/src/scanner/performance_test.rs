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

#[cfg(test)]
mod performance_tests {
    use super::super::data_scanner::{Scanner, ScannerConfig, ScanMode};
    use crate::heal::{HealConfig, HealManager, storage::ECStoreHealStorage};
    use rustfs_ecstore::{
        disk::local::LocalDiskStore,
        set_disk::SetDisks,
        store::ECStore,
        store_api::{MakeBucketOptions, ObjectOptions, PutObjReader, StorageAPI, ObjectIO},
    };
    use std::{
        sync::Arc,
        time::{Duration, Instant, SystemTime},
    };
    use tokio::sync::RwLock;

    /// Helper to create test environment with configurable scanner settings
    async fn create_test_env(
        scan_interval_secs: u64,
        max_concurrent_scans: usize,
        io_rate_limit: u64,
    ) -> (Arc<ECStore>, Arc<Scanner>) {
        let test_dir = format!("/tmp/rustfs_scanner_perf_test_{}", uuid::Uuid::new_v4());
        std::fs::create_dir_all(&test_dir).unwrap();

        // Create ECStore with test configuration
        let disk = LocalDiskStore::new(&test_dir);
        let set_disks = SetDisks::new(vec![Arc::new(disk)], 0, 0);
        let ecstore = Arc::new(ECStore::new(vec![set_disks]));

        // Create heal manager
        let heal_config = HealConfig::default();
        let heal_storage = Arc::new(ECStoreHealStorage::new(ecstore.clone()));
        let heal_manager = Arc::new(HealManager::new(heal_storage, Some(heal_config)));

        // Create scanner with custom configuration
        let scanner_config = ScannerConfig {
            scan_interval: Duration::from_secs(scan_interval_secs),
            deep_scan_interval: Duration::from_secs(scan_interval_secs * 10),
            max_concurrent_scans,
            enable_healing: false, // Disable healing for performance tests
            enable_metrics: true,
            scan_mode: ScanMode::Normal,
            enable_data_usage_stats: false,
            io_rate_limit_mb_per_sec: io_rate_limit,
            scan_delay_ms: if io_rate_limit > 0 { 10 } else { 0 },
        };

        let scanner = Arc::new(Scanner::new(Some(scanner_config), Some(heal_manager)));

        (ecstore, scanner)
    }

    /// Test: Measure scan performance with default (old) settings vs optimized settings
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_scan_performance_comparison() {
        println!("\n=== Scanner Performance Comparison Test ===\n");

        // Create test data
        const NUM_BUCKETS: usize = 3;
        const OBJECTS_PER_BUCKET: usize = 1000;
        const OBJECT_SIZE: usize = 1024; // 1KB

        // Test with old settings (aggressive scanning)
        println!("Testing with OLD settings (aggressive scanning)...");
        let (ecstore_old, scanner_old) = create_test_env(
            60,  // 60 second scan interval (old default)
            20,  // 20 concurrent scans (old default)
            0,   // No I/O rate limiting
        ).await;

        // Create test buckets and objects
        for i in 0..NUM_BUCKETS {
            let bucket_name = format!("test-bucket-{}", i);
            ecstore_old.make_bucket(&bucket_name, &MakeBucketOptions::default()).await.unwrap();

            for j in 0..OBJECTS_PER_BUCKET {
                let object_name = format!("object-{}", j);
                let data = vec![42u8; OBJECT_SIZE];
                let mut reader = PutObjReader::from_vec(data);
                ecstore_old.put_object(&bucket_name, &object_name, &mut reader, &ObjectOptions::default()).await.unwrap();
            }
        }

        // Measure scan performance with old settings
        let start_old = Instant::now();
        scanner_old.scan_cycle().await.unwrap();
        let duration_old = start_old.elapsed();
        let metrics_old = scanner_old.get_metrics().await;

        println!("Old settings results:");
        println!("  - Scan duration: {:?}", duration_old);
        println!("  - Objects scanned: {}", metrics_old.objects_scanned);
        println!("  - Objects/sec: {:.2}", metrics_old.objects_scanned as f64 / duration_old.as_secs_f64());

        // Test with new optimized settings
        println!("\nTesting with NEW settings (optimized scanning)...");
        let (ecstore_new, scanner_new) = create_test_env(
            300, // 5 minute scan interval (new default)
            4,   // 4 concurrent scans (new default)
            50,  // 50 MB/s I/O rate limit
        ).await;

        // Create same test data
        for i in 0..NUM_BUCKETS {
            let bucket_name = format!("test-bucket-{}", i);
            ecstore_new.make_bucket(&bucket_name, &MakeBucketOptions::default()).await.unwrap();

            for j in 0..OBJECTS_PER_BUCKET {
                let object_name = format!("object-{}", j);
                let data = vec![42u8; OBJECT_SIZE];
                let mut reader = PutObjReader::from_vec(data);
                ecstore_new.put_object(&bucket_name, &object_name, &mut reader, &ObjectOptions::default()).await.unwrap();
            }
        }

        // Measure scan performance with new settings
        let start_new = Instant::now();
        scanner_new.scan_cycle().await.unwrap();
        let duration_new = start_new.elapsed();
        let metrics_new = scanner_new.get_metrics().await;

        println!("New settings results:");
        println!("  - Scan duration: {:?}", duration_new);
        println!("  - Objects scanned: {}", metrics_new.objects_scanned);
        println!("  - Objects/sec: {:.2}", metrics_new.objects_scanned as f64 / duration_new.as_secs_f64());

        // Calculate improvements
        let scan_time_improvement = (duration_old.as_secs_f64() - duration_new.as_secs_f64()) / duration_old.as_secs_f64() * 100.0;
        println!("\n=== Performance Improvements ===");
        println!("Scan time improvement: {:.1}%", scan_time_improvement);

        // Assert that new settings don't significantly degrade scan performance
        // We expect similar or slightly slower scan times due to throttling, but with much less I/O impact
        assert!(duration_new.as_secs() < duration_old.as_secs() * 2, "New settings should not double scan time");
    }

    /// Test: Measure write performance impact during scanning
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_write_performance_during_scan() {
        println!("\n=== Write Performance During Scan Test ===\n");

        // Create environment with optimized settings
        let (ecstore, scanner) = create_test_env(300, 4, 50).await;

        // Create test bucket
        let bucket_name = "write-test-bucket";
        ecstore.make_bucket(bucket_name, &MakeBucketOptions::default()).await.unwrap();

        // Pre-populate with some objects
        for i in 0..100 {
            let object_name = format!("initial-object-{}", i);
            let data = vec![42u8; 1024];
            let mut reader = PutObjReader::from_vec(data);
            ecstore.put_object(bucket_name, &object_name, &mut reader, &ObjectOptions::default()).await.unwrap();
        }

        // Start scanner
        scanner.start().await.unwrap();

        // Measure write performance during scanning
        let write_count = Arc::new(RwLock::new(0u64));
        let write_latencies = Arc::new(RwLock::new(Vec::new()));

        // Spawn write workload
        let ecstore_clone = ecstore.clone();
        let write_count_clone = write_count.clone();
        let write_latencies_clone = write_latencies.clone();
        let write_task = tokio::spawn(async move {
            for i in 0..50 {
                let object_name = format!("concurrent-object-{}", i);
                let data = vec![42u8; 10240]; // 10KB objects
                let mut reader = PutObjReader::from_vec(data);
                
                let start = Instant::now();
                ecstore_clone.put_object(bucket_name, &object_name, &mut reader, &ObjectOptions::default()).await.unwrap();
                let latency = start.elapsed();
                
                let mut count = write_count_clone.write().await;
                *count += 1;
                let mut latencies = write_latencies_clone.write().await;
                latencies.push(latency);
                
                // Small delay between writes
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        });

        // Let scanner run for a bit
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Trigger a scan cycle while writes are happening
        scanner.scan_cycle().await.unwrap();

        // Wait for writes to complete
        write_task.await.unwrap();

        // Stop scanner
        scanner.stop().await.unwrap();

        // Analyze results
        let total_writes = *write_count.read().await;
        let latencies = write_latencies.read().await;
        let avg_latency = latencies.iter().sum::<Duration>() / latencies.len() as u32;
        let max_latency = latencies.iter().max().unwrap();
        let min_latency = latencies.iter().min().unwrap();

        println!("Write performance during scanning:");
        println!("  - Total writes: {}", total_writes);
        println!("  - Average latency: {:?}", avg_latency);
        println!("  - Min latency: {:?}", min_latency);
        println!("  - Max latency: {:?}", max_latency);

        // Assert reasonable write latencies
        assert!(avg_latency < Duration::from_millis(500), "Average write latency should be under 500ms");
        assert!(max_latency < Duration::from_secs(2), "Max write latency should be under 2 seconds");
    }

    /// Test: Verify incremental scanning reduces I/O
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_incremental_scanning_efficiency() {
        println!("\n=== Incremental Scanning Efficiency Test ===\n");

        let (ecstore, scanner) = create_test_env(300, 4, 50).await;

        // Create test bucket with objects
        let bucket_name = "incremental-test-bucket";
        ecstore.make_bucket(bucket_name, &MakeBucketOptions::default()).await.unwrap();

        // Create initial objects
        for i in 0..500 {
            let object_name = format!("object-{}", i);
            let data = vec![42u8; 1024];
            let mut reader = PutObjReader::from_vec(data);
            ecstore.put_object(bucket_name, &object_name, &mut reader, &ObjectOptions::default()).await.unwrap();
        }

        // First scan (full scan)
        println!("Performing first scan (full scan)...");
        let start_first = Instant::now();
        scanner.scan_cycle().await.unwrap();
        let duration_first = start_first.elapsed();
        let metrics_first = scanner.get_metrics().await;

        println!("First scan results:");
        println!("  - Duration: {:?}", duration_first);
        println!("  - Objects scanned: {}", metrics_first.objects_scanned);

        // Second scan (should be incremental)
        println!("\nPerforming second scan (incremental)...");
        let start_second = Instant::now();
        scanner.scan_cycle().await.unwrap();
        let duration_second = start_second.elapsed();
        let metrics_second = scanner.get_metrics().await;

        let objects_in_second_scan = metrics_second.objects_scanned - metrics_first.objects_scanned;
        println!("Second scan results:");
        println!("  - Duration: {:?}", duration_second);
        println!("  - Objects scanned: {}", objects_in_second_scan);

        // Add a few new objects
        println!("\nAdding 10 new objects...");
        for i in 500..510 {
            let object_name = format!("object-{}", i);
            let data = vec![42u8; 1024];
            let mut reader = PutObjReader::from_vec(data);
            ecstore.put_object(bucket_name, &object_name, &mut reader, &ObjectOptions::default()).await.unwrap();
        }

        // Third scan (should scan new objects and some old ones)
        println!("\nPerforming third scan (with new objects)...");
        let start_third = Instant::now();
        scanner.scan_cycle().await.unwrap();
        let duration_third = start_third.elapsed();
        let metrics_third = scanner.get_metrics().await;

        let objects_in_third_scan = metrics_third.objects_scanned - metrics_second.objects_scanned;
        println!("Third scan results:");
        println!("  - Duration: {:?}", duration_third);
        println!("  - Objects scanned: {}", objects_in_third_scan);

        // Verify incremental scanning efficiency
        println!("\n=== Efficiency Analysis ===");
        let efficiency_gain = (duration_first.as_secs_f64() - duration_second.as_secs_f64()) / duration_first.as_secs_f64() * 100.0;
        println!("Incremental scan efficiency gain: {:.1}%", efficiency_gain);

        // Assert incremental scanning is more efficient
        assert!(duration_second < duration_first, "Incremental scan should be faster than full scan");
        assert!(objects_in_second_scan < 500, "Second scan should skip most unchanged objects");
    }

    /// Test: Verify write load detection backs off scanning
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_write_load_detection() {
        println!("\n=== Write Load Detection Test ===\n");

        let (ecstore, scanner) = create_test_env(1, 4, 50).await; // 1 second scan interval for testing

        // Create test bucket
        let bucket_name = "load-test-bucket";
        ecstore.make_bucket(bucket_name, &MakeBucketOptions::default()).await.unwrap();

        // Simulate write load by updating scanner state
        scanner.set_test_write_load(20).await; // Simulate 20 recent writes

        // Try to scan - should be skipped due to write load
        println!("Attempting scan with simulated write load...");
        let start = Instant::now();
        let result = scanner.scan_cycle().await;
        let duration = start.elapsed();

        match result {
            Ok(_) => {
                println!("Scan completed (or skipped) in {:?}", duration);
                // If scan completes, it should be very fast (skipped)
                assert!(duration < Duration::from_millis(100), "Scan should be skipped quickly under write load");
            }
            Err(e) => {
                println!("Scan failed: {}", e);
                panic!("Scan should not fail, just skip");
            }
        }

        // Clear write load
        scanner.clear_test_write_load().await;

        // Now scan should proceed normally
        println!("\nAttempting scan without write load...");
        let start = Instant::now();
        scanner.scan_cycle().await.unwrap();
        let duration = start.elapsed();
        println!("Scan completed normally in {:?}", duration);

        // Normal scan should take longer than skipped scan
        assert!(duration > Duration::from_millis(100), "Normal scan should take some time");
    }

    /// Test: Benchmark I/O throttling effectiveness
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_io_throttling_effectiveness() {
        println!("\n=== I/O Throttling Effectiveness Test ===\n");

        // Test without throttling
        println!("Testing without I/O throttling...");
        let (ecstore_no_throttle, scanner_no_throttle) = create_test_env(300, 4, 0).await; // 0 = no throttling

        let bucket_name = "throttle-test-bucket";
        ecstore_no_throttle.make_bucket(bucket_name, &MakeBucketOptions::default()).await.unwrap();

        // Create many small objects
        for i in 0..200 {
            let object_name = format!("object-{}", i);
            let data = vec![42u8; 512]; // Small objects
            let mut reader = PutObjReader::from_vec(data);
            ecstore_no_throttle.put_object(bucket_name, &object_name, &mut reader, &ObjectOptions::default()).await.unwrap();
        }

        let start_no_throttle = Instant::now();
        scanner_no_throttle.scan_cycle().await.unwrap();
        let duration_no_throttle = start_no_throttle.elapsed();

        println!("Without throttling:");
        println!("  - Scan duration: {:?}", duration_no_throttle);

        // Test with throttling
        println!("\nTesting with I/O throttling (50 MB/s)...");
        let (ecstore_throttled, scanner_throttled) = create_test_env(300, 4, 50).await;

        ecstore_throttled.make_bucket(bucket_name, &MakeBucketOptions::default()).await.unwrap();

        for i in 0..200 {
            let object_name = format!("object-{}", i);
            let data = vec![42u8; 512];
            let mut reader = PutObjReader::from_vec(data);
            ecstore_throttled.put_object(bucket_name, &object_name, &mut reader, &ObjectOptions::default()).await.unwrap();
        }

        let start_throttled = Instant::now();
        scanner_throttled.scan_cycle().await.unwrap();
        let duration_throttled = start_throttled.elapsed();

        println!("With throttling:");
        println!("  - Scan duration: {:?}", duration_throttled);

        // Calculate throttling impact
        let throttling_overhead = (duration_throttled.as_secs_f64() - duration_no_throttle.as_secs_f64()) / duration_no_throttle.as_secs_f64() * 100.0;
        println!("\n=== Throttling Analysis ===");
        println!("Throttling overhead: {:.1}%", throttling_overhead.abs());
        println!("This overhead reduces I/O pressure on the system during writes");

        // Throttled scan should take slightly longer but not excessively
        assert!(duration_throttled.as_secs() < duration_no_throttle.as_secs() * 3, "Throttling should not triple scan time");
    }
}