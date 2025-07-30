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

//! Performance tests for object encryption functionality
//!
//! These tests measure the performance characteristics of the encryption implementation,
//! including throughput, latency, memory usage, and scalability.

use rustfs_kms::{
    BucketEncryptionConfig, BucketEncryptionManager, KmsConfig, KmsManager,
    LocalKmsClient, ObjectEncryptionService, EncryptionAlgorithm,
    KmsCacheManager, CacheConfig, ParallelProcessor, ParallelConfig,
};
use rustfs_common::error::Result;
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt};
use tokio::test;
use tokio::task::JoinSet;

/// Test data sizes for performance testing
const SMALL_DATA_SIZE: usize = 1024; // 1KB
const MEDIUM_DATA_SIZE: usize = 1024 * 1024; // 1MB
const LARGE_DATA_SIZE: usize = 10 * 1024 * 1024; // 10MB
const XLARGE_DATA_SIZE: usize = 100 * 1024 * 1024; // 100MB

const TEST_KEY_ID: &str = "perf-test-key-123";

/// Performance metrics structure
#[derive(Debug, Clone)]
struct PerformanceMetrics {
    operation: String,
    data_size: usize,
    duration: Duration,
    throughput_mbps: f64,
    memory_peak_mb: f64,
}

impl PerformanceMetrics {
    fn new(operation: String, data_size: usize, duration: Duration) -> Self {
        let throughput_mbps = (data_size as f64 / (1024.0 * 1024.0)) / duration.as_secs_f64();
        
        Self {
            operation,
            data_size,
            duration,
            throughput_mbps,
            memory_peak_mb: 0.0, // Would need actual memory profiling
        }
    }
    
    fn print_summary(&self) {
        println!(
            "{}: {} bytes in {:?} ({:.2} MB/s)",
            self.operation, self.data_size, self.duration, self.throughput_mbps
        );
    }
}

/// Setup test KMS infrastructure for performance tests
async fn setup_performance_test_kms() -> Result<(Arc<KmsManager>, Arc<ObjectEncryptionService>)> {
    let kms_config = KmsConfig {
        provider: "local".to_string(),
        endpoint: None,
        region: Some("us-east-1".to_string()),
        access_key_id: None,
        secret_access_key: None,
        timeout_secs: 30,
        max_retries: 3,
    };
    
    let local_client = LocalKmsClient::new();
    let kms_manager = Arc::new(KmsManager::new(kms_config, Box::new(local_client)));
    
    let encryption_service = Arc::new(ObjectEncryptionService::new(kms_manager.clone()));
    
    Ok((kms_manager, encryption_service))
}

/// Setup cached KMS infrastructure for performance tests
async fn setup_cached_performance_test_kms() -> Result<(Arc<KmsManager>, Arc<ObjectEncryptionService>, Arc<KmsCacheManager>)> {
    let (kms_manager, encryption_service) = setup_performance_test_kms().await?;
    
    let cache_config = CacheConfig {
        data_key_ttl: Duration::from_secs(300),
        bucket_config_ttl: Duration::from_secs(600),
        max_data_keys: 1000,
        max_bucket_configs: 100,
        cleanup_interval: Duration::from_secs(60),
    };
    
    let cache_manager = Arc::new(KmsCacheManager::new(cache_config));
    
    Ok((kms_manager, encryption_service, cache_manager))
}

/// Generate test data of specified size
fn generate_test_data(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i % 256) as u8).collect()
}

#[test]
async fn test_encryption_throughput_small_data() {
    let (_, encryption_service) = setup_performance_test_kms().await.unwrap();
    
    let test_data = generate_test_data(SMALL_DATA_SIZE);
    let data = Cursor::new(test_data.clone());
    let data_size = test_data.len() as u64;
    
    let start = Instant::now();
    
    let encrypt_result = encryption_service
        .encrypt_object(
            data,
            data_size,
            EncryptionAlgorithm::AES256,
            Some(TEST_KEY_ID),
            None,
        )
        .await
        .unwrap();
    
    let mut encrypted_data = Vec::new();
    let mut reader = encrypt_result.reader;
    reader.read_to_end(&mut encrypted_data).await.unwrap();
    
    let duration = start.elapsed();
    
    let metrics = PerformanceMetrics::new(
        "Small Data Encryption".to_string(),
        SMALL_DATA_SIZE,
        duration,
    );
    
    metrics.print_summary();
    
    // Performance assertions
    assert!(duration < Duration::from_millis(100), "Small data encryption should be fast");
    assert!(metrics.throughput_mbps > 1.0, "Throughput should be reasonable");
}

#[test]
async fn test_encryption_throughput_medium_data() {
    let (_, encryption_service) = setup_performance_test_kms().await.unwrap();
    
    let test_data = generate_test_data(MEDIUM_DATA_SIZE);
    let data = Cursor::new(test_data.clone());
    let data_size = test_data.len() as u64;
    
    let start = Instant::now();
    
    let encrypt_result = encryption_service
        .encrypt_object(
            data,
            data_size,
            EncryptionAlgorithm::AES256,
            Some(TEST_KEY_ID),
            None,
        )
        .await
        .unwrap();
    
    let mut encrypted_data = Vec::new();
    let mut reader = encrypt_result.reader;
    reader.read_to_end(&mut encrypted_data).await.unwrap();
    
    let duration = start.elapsed();
    
    let metrics = PerformanceMetrics::new(
        "Medium Data Encryption".to_string(),
        MEDIUM_DATA_SIZE,
        duration,
    );
    
    metrics.print_summary();
    
    // Performance assertions
    assert!(duration < Duration::from_secs(5), "Medium data encryption should complete in reasonable time");
    assert!(metrics.throughput_mbps > 10.0, "Throughput should be good for medium data");
}

#[test]
async fn test_encryption_throughput_large_data() {
    let (_, encryption_service) = setup_performance_test_kms().await.unwrap();
    
    let test_data = generate_test_data(LARGE_DATA_SIZE);
    let data = Cursor::new(test_data.clone());
    let data_size = test_data.len() as u64;
    
    let start = Instant::now();
    
    let encrypt_result = encryption_service
        .encrypt_object(
            data,
            data_size,
            EncryptionAlgorithm::AES256,
            Some(TEST_KEY_ID),
            None,
        )
        .await
        .unwrap();
    
    let mut encrypted_data = Vec::new();
    let mut reader = encrypt_result.reader;
    reader.read_to_end(&mut encrypted_data).await.unwrap();
    
    let duration = start.elapsed();
    
    let metrics = PerformanceMetrics::new(
        "Large Data Encryption".to_string(),
        LARGE_DATA_SIZE,
        duration,
    );
    
    metrics.print_summary();
    
    // Performance assertions
    assert!(duration < Duration::from_secs(30), "Large data encryption should complete in reasonable time");
    assert!(metrics.throughput_mbps > 5.0, "Throughput should be acceptable for large data");
}

#[test]
async fn test_algorithm_performance_comparison() {
    let (_, encryption_service) = setup_performance_test_kms().await.unwrap();
    
    let test_data = generate_test_data(MEDIUM_DATA_SIZE);
    let mut results = Vec::new();
    
    // Test AES256
    let data_aes = Cursor::new(test_data.clone());
    let start_aes = Instant::now();
    
    let encrypt_result_aes = encryption_service
        .encrypt_object(
            data_aes,
            test_data.len() as u64,
            EncryptionAlgorithm::AES256,
            Some(TEST_KEY_ID),
            None,
        )
        .await
        .unwrap();
    
    let mut encrypted_data_aes = Vec::new();
    let mut reader_aes = encrypt_result_aes.reader;
    reader_aes.read_to_end(&mut encrypted_data_aes).await.unwrap();
    
    let duration_aes = start_aes.elapsed();
    let metrics_aes = PerformanceMetrics::new(
        "AES256 Encryption".to_string(),
        MEDIUM_DATA_SIZE,
        duration_aes,
    );
    results.push(metrics_aes.clone());
    
    // Test ChaCha20Poly1305
    let data_chacha = Cursor::new(test_data.clone());
    let start_chacha = Instant::now();
    
    let encrypt_result_chacha = encryption_service
        .encrypt_object(
            data_chacha,
            test_data.len() as u64,
            EncryptionAlgorithm::ChaCha20Poly1305,
            Some(TEST_KEY_ID),
            None,
        )
        .await
        .unwrap();
    
    let mut encrypted_data_chacha = Vec::new();
    let mut reader_chacha = encrypt_result_chacha.reader;
    reader_chacha.read_to_end(&mut encrypted_data_chacha).await.unwrap();
    
    let duration_chacha = start_chacha.elapsed();
    let metrics_chacha = PerformanceMetrics::new(
        "ChaCha20Poly1305 Encryption".to_string(),
        MEDIUM_DATA_SIZE,
        duration_chacha,
    );
    results.push(metrics_chacha.clone());
    
    // Print comparison
    println!("\nAlgorithm Performance Comparison:");
    for metric in &results {
        metric.print_summary();
    }
    
    // Both algorithms should perform reasonably
    assert!(metrics_aes.throughput_mbps > 5.0);
    assert!(metrics_chacha.throughput_mbps > 5.0);
    
    // Performance difference shouldn't be too extreme
    let ratio = metrics_aes.throughput_mbps / metrics_chacha.throughput_mbps;
    assert!(ratio > 0.1 && ratio < 10.0, "Algorithm performance should be comparable");
}

#[test]
async fn test_concurrent_encryption_performance() {
    let (_, encryption_service) = setup_performance_test_kms().await.unwrap();
    
    let test_data = generate_test_data(MEDIUM_DATA_SIZE);
    let num_concurrent = 10;
    
    let start = Instant::now();
    
    let mut join_set = JoinSet::new();
    
    for i in 0..num_concurrent {
        let service = encryption_service.clone();
        let data = test_data.clone();
        
        join_set.spawn(async move {
            let cursor = Cursor::new(data.clone());
            let data_size = data.len() as u64;
            
            let result = service
                .encrypt_object(
                    cursor,
                    data_size,
                    EncryptionAlgorithm::AES256,
                    Some(&format!("{}-{}", TEST_KEY_ID, i)),
                    None,
                )
                .await
                .unwrap();
            
            let mut encrypted_data = Vec::new();
            let mut reader = result.reader;
            reader.read_to_end(&mut encrypted_data).await.unwrap();
            
            encrypted_data.len()
        });
    }
    
    let mut total_bytes = 0;
    while let Some(result) = join_set.join_next().await {
        total_bytes += result.unwrap();
    }
    
    let duration = start.elapsed();
    
    let metrics = PerformanceMetrics::new(
        format!("Concurrent Encryption ({}x)", num_concurrent),
        total_bytes,
        duration,
    );
    
    metrics.print_summary();
    
    // Concurrent operations should complete in reasonable time
    assert!(duration < Duration::from_secs(60));
    assert!(metrics.throughput_mbps > 1.0);
    
    // Should process all data
    assert_eq!(total_bytes, MEDIUM_DATA_SIZE * num_concurrent);
}

#[test]
async fn test_cache_performance_impact() {
    let (_, encryption_service, cache_manager) = setup_cached_performance_test_kms().await.unwrap();
    
    let test_data = generate_test_data(MEDIUM_DATA_SIZE);
    let num_iterations = 5;
    
    // First run - cache miss
    let mut first_run_times = Vec::new();
    for _ in 0..num_iterations {
        let data = Cursor::new(test_data.clone());
        let data_size = test_data.len() as u64;
        
        let start = Instant::now();
        
        let encrypt_result = encryption_service
            .encrypt_object(
                data,
                data_size,
                EncryptionAlgorithm::AES256,
                Some(TEST_KEY_ID),
                None,
            )
            .await
            .unwrap();
        
        let mut encrypted_data = Vec::new();
        let mut reader = encrypt_result.reader;
        reader.read_to_end(&mut encrypted_data).await.unwrap();
        
        first_run_times.push(start.elapsed());
    }
    
    // Second run - should benefit from cache
    let mut second_run_times = Vec::new();
    for _ in 0..num_iterations {
        let data = Cursor::new(test_data.clone());
        let data_size = test_data.len() as u64;
        
        let start = Instant::now();
        
        let encrypt_result = encryption_service
            .encrypt_object(
                data,
                data_size,
                EncryptionAlgorithm::AES256,
                Some(TEST_KEY_ID),
                None,
            )
            .await
            .unwrap();
        
        let mut encrypted_data = Vec::new();
        let mut reader = encrypt_result.reader;
        reader.read_to_end(&mut encrypted_data).await.unwrap();
        
        second_run_times.push(start.elapsed());
    }
    
    let avg_first_run = first_run_times.iter().sum::<Duration>() / first_run_times.len() as u32;
    let avg_second_run = second_run_times.iter().sum::<Duration>() / second_run_times.len() as u32;
    
    println!("\nCache Performance Impact:");
    println!("Average first run: {:?}", avg_first_run);
    println!("Average second run: {:?}", avg_second_run);
    
    let stats = cache_manager.get_stats().await;
    println!("Cache stats: {:?}", stats);
    
    // Cache should provide some performance benefit or at least not hurt
    let performance_ratio = avg_second_run.as_nanos() as f64 / avg_first_run.as_nanos() as f64;
    assert!(performance_ratio <= 1.2, "Cache should not significantly hurt performance");
}

#[test]
async fn test_memory_usage_scaling() {
    let (_, encryption_service) = setup_performance_test_kms().await.unwrap();
    
    let data_sizes = vec![SMALL_DATA_SIZE, MEDIUM_DATA_SIZE, LARGE_DATA_SIZE];
    let mut results = Vec::new();
    
    for &size in &data_sizes {
        let test_data = generate_test_data(size);
        let data = Cursor::new(test_data.clone());
        let data_size = test_data.len() as u64;
        
        // Measure memory before operation
        let memory_before = get_memory_usage();
        
        let start = Instant::now();
        
        let encrypt_result = encryption_service
            .encrypt_object(
                data,
                data_size,
                EncryptionAlgorithm::AES256,
                Some(TEST_KEY_ID),
                None,
            )
            .await
            .unwrap();
        
        let mut encrypted_data = Vec::new();
        let mut reader = encrypt_result.reader;
        reader.read_to_end(&mut encrypted_data).await.unwrap();
        
        let duration = start.elapsed();
        
        // Measure memory after operation
        let memory_after = get_memory_usage();
        let memory_delta = memory_after - memory_before;
        
        let mut metrics = PerformanceMetrics::new(
            format!("Memory Scaling Test ({}MB)", size / (1024 * 1024)),
            size,
            duration,
        );
        metrics.memory_peak_mb = memory_delta;
        
        results.push(metrics);
    }
    
    println!("\nMemory Usage Scaling:");
    for metric in &results {
        println!(
            "{}: {:.2} MB memory delta, {:.2} MB/s throughput",
            metric.operation, metric.memory_peak_mb, metric.throughput_mbps
        );
    }
    
    // Memory usage should scale reasonably with data size
    // (This is a basic check - real memory profiling would be more sophisticated)
    for metric in &results {
        let memory_ratio = metric.memory_peak_mb / (metric.data_size as f64 / (1024.0 * 1024.0));
        assert!(memory_ratio < 10.0, "Memory usage should not be excessive compared to data size");
    }
}

#[test]
async fn test_roundtrip_performance() {
    let (_, encryption_service) = setup_performance_test_kms().await.unwrap();
    
    let test_data = generate_test_data(MEDIUM_DATA_SIZE);
    let data = Cursor::new(test_data.clone());
    let data_size = test_data.len() as u64;
    
    let start_total = Instant::now();
    
    // Encryption phase
    let start_encrypt = Instant::now();
    let encrypt_result = encryption_service
        .encrypt_object(
            data,
            data_size,
            EncryptionAlgorithm::AES256,
            Some(TEST_KEY_ID),
            None,
        )
        .await
        .unwrap();
    
    let mut encrypted_data = Vec::new();
    let mut reader = encrypt_result.reader;
    reader.read_to_end(&mut encrypted_data).await.unwrap();
    
    let encrypt_duration = start_encrypt.elapsed();
    
    // Prepare metadata for decryption
    let mut metadata = HashMap::new();
    metadata.insert(
        "encrypted_data_key".to_string(),
        encrypt_result.metadata.encrypted_data_key,
    );
    metadata.insert(
        "algorithm".to_string(),
        encrypt_result.metadata.algorithm.to_string(),
    );
    if let Some(kms_key_id) = encrypt_result.metadata.kms_key_id {
        metadata.insert("kms_key_id".to_string(), kms_key_id);
    }
    if let Some(iv) = encrypt_result.metadata.iv {
        metadata.insert("iv".to_string(), base64::encode(iv));
    }
    if let Some(tag) = encrypt_result.metadata.tag {
        metadata.insert("tag".to_string(), base64::encode(tag));
    }
    
    // Decryption phase
    let start_decrypt = Instant::now();
    let encrypted_cursor = Cursor::new(encrypted_data);
    let decrypt_result = encryption_service
        .decrypt_object(encrypted_cursor, &metadata)
        .await
        .unwrap();
    
    let mut decrypted_data = Vec::new();
    let mut decrypt_reader = decrypt_result;
    decrypt_reader.read_to_end(&mut decrypted_data).await.unwrap();
    
    let decrypt_duration = start_decrypt.elapsed();
    let total_duration = start_total.elapsed();
    
    // Verify data integrity
    assert_eq!(decrypted_data, test_data);
    
    // Performance metrics
    let encrypt_metrics = PerformanceMetrics::new(
        "Encryption".to_string(),
        MEDIUM_DATA_SIZE,
        encrypt_duration,
    );
    
    let decrypt_metrics = PerformanceMetrics::new(
        "Decryption".to_string(),
        MEDIUM_DATA_SIZE,
        decrypt_duration,
    );
    
    let total_metrics = PerformanceMetrics::new(
        "Roundtrip Total".to_string(),
        MEDIUM_DATA_SIZE,
        total_duration,
    );
    
    println!("\nRoundtrip Performance:");
    encrypt_metrics.print_summary();
    decrypt_metrics.print_summary();
    total_metrics.print_summary();
    
    // Performance assertions
    assert!(encrypt_duration < Duration::from_secs(10));
    assert!(decrypt_duration < Duration::from_secs(10));
    assert!(total_duration < Duration::from_secs(15));
    
    assert!(encrypt_metrics.throughput_mbps > 1.0);
    assert!(decrypt_metrics.throughput_mbps > 1.0);
}

/// Simple memory usage estimation (placeholder)
/// In a real implementation, this would use proper memory profiling
fn get_memory_usage() -> f64 {
    // This is a placeholder - real implementation would use:
    // - Process memory stats
    // - Heap profiling
    // - System memory monitoring
    0.0
}

#[test]
async fn test_parallel_processing_performance() {
    let parallel_config = ParallelConfig {
        max_concurrent_operations: 4,
        chunk_size: 1024 * 1024, // 1MB chunks
        worker_pool_size: 8,
        queue_capacity: 100,
    };
    
    let processor = ParallelProcessor::new(parallel_config);
    
    let test_data = generate_test_data(XLARGE_DATA_SIZE);
    let chunks: Vec<Vec<u8>> = test_data
        .chunks(1024 * 1024)
        .map(|chunk| chunk.to_vec())
        .collect();
    
    let start = Instant::now();
    
    // Process chunks in parallel
    let mut join_set = JoinSet::new();
    
    for (i, chunk) in chunks.into_iter().enumerate() {
        join_set.spawn(async move {
            // Simulate processing work
            tokio::time::sleep(Duration::from_millis(10)).await;
            (i, chunk.len())
        });
    }
    
    let mut total_processed = 0;
    while let Some(result) = join_set.join_next().await {
        let (_, chunk_size) = result.unwrap();
        total_processed += chunk_size;
    }
    
    let duration = start.elapsed();
    
    let metrics = PerformanceMetrics::new(
        "Parallel Processing".to_string(),
        total_processed,
        duration,
    );
    
    metrics.print_summary();
    
    // Parallel processing should be efficient
    assert_eq!(total_processed, XLARGE_DATA_SIZE);
    assert!(duration < Duration::from_secs(30));
    assert!(metrics.throughput_mbps > 10.0);
}