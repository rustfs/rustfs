// Copyright 2024 RustFS Team
//
// Benchmarks comparing fast lock vs old lock performance

#[cfg(test)]
#[allow(dead_code)] // Temporarily disable benchmark tests
mod benchmarks {
    use super::super::*;
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use tokio::task;

    /// Benchmark single-threaded lock operations
    #[tokio::test]
    async fn bench_single_threaded_fast_locks() {
        let manager = Arc::new(FastObjectLockManager::new());
        let iterations = 10000;
        
        // Warm up
        for i in 0..100 {
            let _guard = manager
                .acquire_write_lock("bucket", &format!("warm_{}", i), "owner")
                .await
                .unwrap();
        }

        // Benchmark write locks
        let start = Instant::now();
        for i in 0..iterations {
            let _guard = manager
                .acquire_write_lock("bucket", &format!("object_{}", i), "owner")
                .await
                .unwrap();
        }
        let duration = start.elapsed();
        
        println!("Fast locks: {} write locks in {:?}", iterations, duration);
        println!("Average: {:?} per lock", duration / iterations);
        
        let metrics = manager.get_metrics();
        println!("Fast path rate: {:.2}%", metrics.shard_metrics.fast_path_rate() * 100.0);
        
        // Should be much faster than old implementation
        assert!(duration.as_millis() < 1000, "Should complete 10k locks in <1s");
        assert!(metrics.shard_metrics.fast_path_rate() > 0.95, "Should have >95% fast path rate");
    }

    /// Benchmark concurrent lock operations
    #[tokio::test]
    async fn bench_concurrent_fast_locks() {
        let manager = Arc::new(FastObjectLockManager::new());
        let concurrent_tasks = 100;
        let iterations_per_task = 100;
        
        let start = Instant::now();
        
        let mut handles = Vec::new();
        for task_id in 0..concurrent_tasks {
            let manager_clone = manager.clone();
            let handle = task::spawn(async move {
                for i in 0..iterations_per_task {
                    let object_name = format!("obj_{}_{}", task_id, i);
                    let _guard = manager_clone
                        .acquire_write_lock("bucket", &object_name, &format!("owner_{}", task_id))
                        .await
                        .unwrap();
                    
                    // Simulate some work
                    tokio::task::yield_now().await;
                }
            });
            handles.push(handle);
        }
        
        // Wait for all tasks
        for handle in handles {
            handle.await.unwrap();
        }
        
        let duration = start.elapsed();
        let total_ops = concurrent_tasks * iterations_per_task;
        
        println!("Concurrent fast locks: {} operations across {} tasks in {:?}", 
                 total_ops, concurrent_tasks, duration);
        println!("Throughput: {:.2} ops/sec", total_ops as f64 / duration.as_secs_f64());
        
        let metrics = manager.get_metrics();
        println!("Fast path rate: {:.2}%", metrics.shard_metrics.fast_path_rate() * 100.0);
        println!("Contention events: {}", metrics.shard_metrics.contention_events);
        
        // Should maintain high throughput even with concurrency
        assert!(duration.as_millis() < 5000, "Should complete concurrent ops in <5s");
    }

    /// Benchmark contended lock operations
    #[tokio::test]
    async fn bench_contended_locks() {
        let manager = Arc::new(FastObjectLockManager::new());
        let concurrent_tasks = 50;
        let shared_objects = 10; // High contention on few objects
        let iterations_per_task = 50;
        
        let start = Instant::now();
        
        let mut handles = Vec::new();
        for task_id in 0..concurrent_tasks {
            let manager_clone = manager.clone();
            let handle = task::spawn(async move {
                for i in 0..iterations_per_task {
                    let object_name = format!("shared_{}", i % shared_objects);
                    
                    // Mix of read and write operations
                    if i % 3 == 0 {
                        // Write operation
                        if let Ok(_guard) = manager_clone
                            .acquire_write_lock("bucket", &object_name, &format!("owner_{}", task_id))
                            .await
                        {
                            tokio::task::yield_now().await;
                        }
                    } else {
                        // Read operation
                        if let Ok(_guard) = manager_clone
                            .acquire_read_lock("bucket", &object_name, &format!("owner_{}", task_id))
                            .await
                        {
                            tokio::task::yield_now().await;
                        }
                    }
                }
            });
            handles.push(handle);
        }
        
        // Wait for all tasks
        for handle in handles {
            handle.await.unwrap();
        }
        
        let duration = start.elapsed();
        
        println!("Contended locks: {} tasks on {} objects in {:?}", 
                 concurrent_tasks, shared_objects, duration);
        
        let metrics = manager.get_metrics();
        println!("Total acquisitions: {}", metrics.shard_metrics.total_acquisitions());
        println!("Fast path rate: {:.2}%", metrics.shard_metrics.fast_path_rate() * 100.0);
        println!("Average wait time: {:?}", metrics.shard_metrics.avg_wait_time());
        println!("Timeout rate: {:.2}%", metrics.shard_metrics.timeout_rate() * 100.0);
        
        // Even with contention, should maintain reasonable performance
        assert!(metrics.shard_metrics.timeout_rate() < 0.1, "Should have <10% timeout rate");
        assert!(metrics.shard_metrics.avg_wait_time() < Duration::from_millis(100), "Avg wait should be <100ms");
    }

    /// Benchmark batch operations
    #[tokio::test]
    async fn bench_batch_operations() {
        let manager = FastObjectLockManager::new();
        let batch_sizes = vec![10, 50, 100, 500];
        
        for batch_size in batch_sizes {
            // Create batch request
            let mut batch = BatchLockRequest::new("batch_owner");
            for i in 0..batch_size {
                batch = batch.add_write_lock("bucket", &format!("batch_obj_{}", i));
            }
            
            let start = Instant::now();
            let result = manager.acquire_locks_batch(batch).await;
            let duration = start.elapsed();
            
            assert!(result.all_acquired, "Batch should succeed");
            println!("Batch size {}: {:?} ({:.2} μs per lock)", 
                     batch_size, 
                     duration,
                     duration.as_micros() as f64 / batch_size as f64);
            
            // Batch should be much faster than individual acquisitions
            assert!(duration.as_millis() < batch_size as u128 / 10, 
                    "Batch should be 10x+ faster than individual locks");
        }
    }

    /// Benchmark version-specific locks
    #[tokio::test]
    async fn bench_versioned_locks() {
        let manager = Arc::new(FastObjectLockManager::new());
        let objects = 100;
        let versions_per_object = 10;
        
        let start = Instant::now();
        
        let mut handles = Vec::new();
        for obj_id in 0..objects {
            let manager_clone = manager.clone();
            let handle = task::spawn(async move {
                for version in 0..versions_per_object {
                    let _guard = manager_clone
                        .acquire_write_lock_versioned(
                            "bucket",
                            &format!("obj_{}", obj_id),
                            &format!("v{}", version),
                            "version_owner"
                        )
                        .await
                        .unwrap();
                }
            });
            handles.push(handle);
        }
        
        for handle in handles {
            handle.await.unwrap();
        }
        
        let duration = start.elapsed();
        let total_ops = objects * versions_per_object;
        
        println!("Versioned locks: {} version locks in {:?}", total_ops, duration);
        println!("Throughput: {:.2} locks/sec", total_ops as f64 / duration.as_secs_f64());
        
        let metrics = manager.get_metrics();
        println!("Fast path rate: {:.2}%", metrics.shard_metrics.fast_path_rate() * 100.0);
        
        // Versioned locks should not interfere with each other
        assert!(metrics.shard_metrics.fast_path_rate() > 0.9, "Should maintain high fast path rate");
    }

    /// Compare with theoretical maximum performance
    #[tokio::test]
    async fn bench_theoretical_maximum() {
        let manager = Arc::new(FastObjectLockManager::new());
        let iterations = 100000;
        
        // Measure pure fast path performance (no contention)
        let start = Instant::now();
        for i in 0..iterations {
            let _guard = manager
                .acquire_write_lock("bucket", &format!("unique_{}", i), "owner")
                .await
                .unwrap();
        }
        let duration = start.elapsed();
        
        println!("Theoretical maximum: {} unique locks in {:?}", iterations, duration);
        println!("Rate: {:.2} locks/sec", iterations as f64 / duration.as_secs_f64());
        println!("Latency: {:?} per lock", duration / iterations);
        
        let metrics = manager.get_metrics();
        println!("Fast path rate: {:.2}%", metrics.shard_metrics.fast_path_rate() * 100.0);
        
        // Should achieve very high performance with no contention
        assert!(metrics.shard_metrics.fast_path_rate() > 0.99, "Should be nearly 100% fast path");
        assert!(duration.as_secs_f64() / (iterations as f64) < 0.0001, "Should be <100μs per lock");
    }

    /// Performance regression test
    #[tokio::test]
    async fn performance_regression_test() {
        let manager = Arc::new(FastObjectLockManager::new());
        
        // This test ensures we maintain performance targets
        let test_cases = vec![
            ("single_thread", 1, 10000),
            ("low_contention", 10, 1000),
            ("high_contention", 100, 100),
        ];
        
        for (test_name, threads, ops_per_thread) in test_cases {
            let start = Instant::now();
            
            let mut handles = Vec::new();
            for thread_id in 0..threads {
                let manager_clone = manager.clone();
                let handle = task::spawn(async move {
                    for op_id in 0..ops_per_thread {
                        let object = if threads == 1 {
                            format!("obj_{}_{}", thread_id, op_id)
                        } else {
                            format!("obj_{}", op_id % 100) // Create contention
                        };
                        
                        let owner = format!("owner_{}", thread_id);
                        let _guard = manager_clone
                            .acquire_write_lock("bucket", object, owner)
                            .await
                            .unwrap();
                    }
                });
                handles.push(handle);
            }
            
            for handle in handles {
                handle.await.unwrap();
            }
            
            let duration = start.elapsed();
            let total_ops = threads * ops_per_thread;
            let ops_per_sec = total_ops as f64 / duration.as_secs_f64();
            
            println!("{}: {:.2} ops/sec", test_name, ops_per_sec);
            
            // Performance targets (adjust based on requirements)
            match test_name {
                "single_thread" => assert!(ops_per_sec > 50000.0, "Single thread should exceed 50k ops/sec"),
                "low_contention" => assert!(ops_per_sec > 20000.0, "Low contention should exceed 20k ops/sec"),
                "high_contention" => assert!(ops_per_sec > 5000.0, "High contention should exceed 5k ops/sec"),
                _ => {}
            }
        }
    }
}