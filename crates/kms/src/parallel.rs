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

//! Parallel processing utilities for KMS operations
//!
//! This module provides parallel encryption/decryption capabilities
//! to improve performance for large data operations.

use crate::{KmsError, Result};
use bytes::Bytes;
use rayon::prelude::*;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

/// Configuration for parallel processing
#[derive(Debug, Clone)]
pub struct ParallelConfig {
    /// Maximum number of concurrent operations
    pub max_concurrency: usize,
    /// Chunk size for parallel processing (in bytes)
    pub chunk_size: usize,
    /// Buffer size for async I/O operations
    pub buffer_size: usize,
}

impl Default for ParallelConfig {
    fn default() -> Self {
        Self {
            max_concurrency: num_cpus::get(),
            chunk_size: 64 * 1024, // 64KB chunks
            buffer_size: 8 * 1024, // 8KB buffer
        }
    }
}

/// Parallel processor for encryption/decryption operations
pub struct ParallelProcessor {
    config: ParallelConfig,
    semaphore: Arc<Semaphore>,
}

impl ParallelProcessor {
    /// Create a new parallel processor with the given configuration
    pub fn new(config: ParallelConfig) -> Self {
        let semaphore = Arc::new(Semaphore::new(config.max_concurrency));
        Self { config, semaphore }
    }

    /// Process data chunks in parallel using the provided operation
    pub async fn process_chunks<F, Fut>(&self, data: Bytes, operation: F) -> Result<Vec<Bytes>>
    where
        F: Fn(Bytes, usize) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<Bytes>> + Send,
    {
        let chunks = self.split_into_chunks(data);
        let operation = Arc::new(operation);
        let mut join_set = JoinSet::new();
        let mut results = Vec::with_capacity(chunks.len());

        // Process chunks in parallel
        for (index, chunk) in chunks.into_iter().enumerate() {
            let permit = self
                .semaphore
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| KmsError::InternalError {
                    message: "Failed to acquire semaphore permit".to_string(),
                })?;

            let operation = operation.clone();
            join_set.spawn(async move {
                let _permit = permit; // Keep permit alive
                let result = operation(chunk, index).await;
                (index, result)
            });
        }

        // Collect results in order
        let mut indexed_results = Vec::new();
        while let Some(result) = join_set.join_next().await {
            let task_result = result.map_err(|e| KmsError::InternalError {
                message: format!("Task join error: {e}"),
            })?;
            let (index, chunk_result) = task_result;
            indexed_results.push((index, chunk_result?));
        }

        // Sort by index to maintain order
        indexed_results.sort_by_key(|(index, _)| *index);
        for (_, chunk) in indexed_results {
            results.push(chunk);
        }

        Ok(results)
    }

    /// Split data into chunks for parallel processing
    fn split_into_chunks(&self, data: Bytes) -> Vec<Bytes> {
        let mut chunks = Vec::new();
        let chunk_size = self.config.chunk_size;

        for chunk_start in (0..data.len()).step_by(chunk_size) {
            let chunk_end = std::cmp::min(chunk_start + chunk_size, data.len());
            chunks.push(data.slice(chunk_start..chunk_end));
        }

        chunks
    }

    /// Get the current configuration
    pub fn config(&self) -> &ParallelConfig {
        &self.config
    }

    /// Process data chunks in parallel using CPU threads (rayon)
    /// This is suitable for CPU-intensive operations like encryption/decryption
    pub fn process_chunks_cpu<F, R>(&self, data: Bytes, operation: F) -> Result<Vec<R>>
    where
        F: Fn(Bytes, usize) -> Result<R> + Send + Sync,
        R: Send,
    {
        let chunks = self.split_into_chunks(data);

        chunks
            .into_par_iter()
            .enumerate()
            .map(|(index, chunk)| operation(chunk, index))
            .collect::<Result<Vec<_>>>()
    }

    /// Process data in parallel using both CPU threads and async tasks
    /// This combines rayon for CPU work with tokio for I/O operations
    pub async fn process_hybrid<F, Fut, R>(&self, data: Bytes, cpu_operation: F) -> Result<Vec<R>>
    where
        F: Fn(Bytes, usize) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<R>> + Send,
        R: Send + 'static,
    {
        let chunks = self.split_into_chunks(data);
        let operation = Arc::new(cpu_operation);
        let mut join_set = JoinSet::new();

        // Process chunks using async tasks
        for (index, chunk) in chunks.into_iter().enumerate() {
            let permit = self
                .semaphore
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| KmsError::InternalError {
                    message: "Failed to acquire semaphore permit".to_string(),
                })?;

            let operation = operation.clone();
            join_set.spawn(async move {
                let _permit = permit;
                let result = operation(chunk, index).await?;
                Ok::<(usize, R), KmsError>((index, result))
            });
        }

        // Collect results in order
        let mut indexed_results = Vec::new();
        while let Some(result) = join_set.join_next().await {
            let task_result = result.map_err(|e| KmsError::InternalError {
                message: format!("Task join error: {e}"),
            })?;
            let (index, chunk_result) = task_result?;
            indexed_results.push((index, chunk_result));
        }

        // Sort by index to maintain order
        indexed_results.sort_by_key(|(index, _)| *index);
        Ok(indexed_results.into_iter().map(|(_, result)| result).collect())
    }
}

/// Connection pool for managing KMS client connections
pub struct ConnectionPool<T> {
    connections: Arc<tokio::sync::Mutex<VecDeque<T>>>,
    max_size: usize,
    current_size: Arc<tokio::sync::Mutex<usize>>,
    factory: Arc<dyn Fn() -> Result<T> + Send + Sync>,
}

impl<T> ConnectionPool<T>
where
    T: Send + 'static,
{
    /// Create a new connection pool
    pub fn new<F>(max_size: usize, factory: F) -> Self
    where
        F: Fn() -> Result<T> + Send + Sync + 'static,
    {
        Self {
            connections: Arc::new(tokio::sync::Mutex::new(VecDeque::new())),
            max_size,
            current_size: Arc::new(tokio::sync::Mutex::new(0)),
            factory: Arc::new(factory),
        }
    }

    /// Get a connection from the pool
    pub async fn get(&self) -> Result<PooledConnection<T>> {
        let mut connections = self.connections.lock().await;

        if let Some(connection) = connections.pop_front() {
            return Ok(PooledConnection {
                connection: Some(connection),
                pool: self.connections.clone(),
            });
        }

        drop(connections);

        // Create new connection if pool is not at max capacity
        let mut current_size = self.current_size.lock().await;
        if *current_size < self.max_size {
            let connection = (self.factory)()?;
            *current_size += 1;
            Ok(PooledConnection {
                connection: Some(connection),
                pool: self.connections.clone(),
            })
        } else {
            // Wait for a connection to be returned
            drop(current_size);
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            Box::pin(self.get()).await
        }
    }

    /// Get the current pool size
    pub async fn size(&self) -> usize {
        *self.current_size.lock().await
    }
}

/// A connection wrapper that returns the connection to the pool when dropped
pub struct PooledConnection<T: Send + 'static> {
    connection: Option<T>,
    pool: Arc<tokio::sync::Mutex<VecDeque<T>>>,
}

impl<T: Send> PooledConnection<T> {
    /// Get a reference to the underlying connection
    pub fn get_ref(&self) -> &T {
        self.connection.as_ref().expect("Connection should be available")
    }

    /// Get a mutable reference to the underlying connection
    pub fn get_mut(&mut self) -> &mut T {
        self.connection.as_mut().expect("Connection should be available")
    }
}

impl<T: Send + 'static> Drop for PooledConnection<T> {
    fn drop(&mut self) {
        if let Some(connection) = self.connection.take() {
            let pool = self.pool.clone();
            tokio::spawn(async move {
                let mut connections = pool.lock().await;
                connections.push_back(connection);
            });
        }
    }
}

/// Async I/O optimization utilities
pub struct AsyncIoOptimizer {
    read_buffer_size: usize,
    write_buffer_size: usize,
}

impl AsyncIoOptimizer {
    /// Create a new async I/O optimizer
    pub fn new(read_buffer_size: usize, write_buffer_size: usize) -> Self {
        Self {
            read_buffer_size,
            write_buffer_size,
        }
    }

    /// Get the read buffer size
    pub fn read_buffer_size(&self) -> usize {
        self.read_buffer_size
    }

    /// Get the write buffer size
    pub fn write_buffer_size(&self) -> usize {
        self.write_buffer_size
    }

    /// Optimize read operations with buffering
    pub async fn optimized_read<R>(&self, mut reader: R) -> Result<Vec<u8>>
    where
        R: tokio::io::AsyncRead + Unpin,
    {
        use tokio::io::AsyncReadExt;

        let mut buffer = Vec::new();
        let mut chunk = vec![0u8; self.read_buffer_size];

        loop {
            let bytes_read = reader.read(&mut chunk).await.map_err(|e| KmsError::InternalError {
                message: format!("Read error: {e}"),
            })?;

            if bytes_read == 0 {
                break;
            }

            buffer.extend_from_slice(&chunk[..bytes_read]);
        }

        Ok(buffer)
    }

    /// Optimize write operations with buffering
    pub async fn optimized_write<W>(&self, mut writer: W, data: &[u8]) -> Result<()>
    where
        W: tokio::io::AsyncWrite + Unpin,
    {
        use tokio::io::AsyncWriteExt;

        for chunk in data.chunks(self.write_buffer_size) {
            writer.write_all(chunk).await.map_err(|e| KmsError::InternalError {
                message: format!("Write error: {e}"),
            })?;
        }

        writer.flush().await.map_err(|e| KmsError::InternalError {
            message: format!("Flush error: {e}"),
        })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::test;

    #[test]
    async fn test_parallel_processor() {
        let config = ParallelConfig::default();
        let processor = ParallelProcessor::new(config);

        let data = Bytes::from(vec![1u8; 1000]);

        let results = processor
            .process_chunks(data, |chunk, _index| async move {
                // Simple identity operation
                Ok(chunk)
            })
            .await
            .expect("Process chunks should succeed");

        assert!(!results.is_empty());

        // Verify total length is preserved
        let total_len: usize = results.iter().map(|chunk| chunk.len()).sum();
        assert_eq!(total_len, 1000);
    }

    #[tokio::test]
    async fn test_connection_pool() {
        let pool = ConnectionPool::new(2, || Ok("connection".to_string()));

        let conn1 = pool.get().await.expect("Should get connection from pool");
        assert_eq!(conn1.get_ref(), "connection");

        let conn2 = pool.get().await.expect("Should get connection from pool");
        assert_eq!(conn2.get_ref(), "connection");

        assert_eq!(pool.size().await, 2);
    }

    #[tokio::test]
    async fn test_async_io_optimizer() {
        let optimizer = AsyncIoOptimizer::new(1024, 2048);

        assert_eq!(optimizer.read_buffer_size(), 1024);
        assert_eq!(optimizer.write_buffer_size(), 2048);

        // Test optimized read
        let data = b"test data for reading";
        let reader = tokio::io::BufReader::new(std::io::Cursor::new(data));
        let result = optimizer.optimized_read(reader).await.expect("Optimized read should succeed");
        assert_eq!(result, data.to_vec());

        // Test optimized write
        let mut writer = Vec::new();
        let write_data = b"test data for writing";
        optimizer
            .optimized_write(&mut writer, write_data)
            .await
            .expect("Optimized write should succeed");
        assert_eq!(writer, write_data);
    }

    #[tokio::test]
    async fn test_cpu_parallel_processing() {
        let config = ParallelConfig {
            max_concurrency: 4,
            chunk_size: 10,
            buffer_size: 1024,
        };
        let processor = ParallelProcessor::new(config);

        // Test data
        let data = Bytes::from("Hello, World! This is a test for CPU parallel processing.");

        // Simple operation: count bytes in each chunk
        let operation = |chunk: Bytes, _index: usize| -> Result<usize> { Ok(chunk.len()) };

        let results = processor
            .process_chunks_cpu(data, operation)
            .expect("CPU parallel processing should succeed");

        // Verify results
        assert!(!results.is_empty());
        let total_length: usize = results.iter().sum();
        assert_eq!(total_length, "Hello, World! This is a test for CPU parallel processing.".len());
    }

    #[tokio::test]
    async fn test_hybrid_parallel_processing() {
        let config = ParallelConfig {
            max_concurrency: 2,
            chunk_size: 5,
            buffer_size: 1024,
        };
        let processor = ParallelProcessor::new(config);

        // Test data
        let data = Bytes::from("Hello, World!");

        // Async operation: simulate some async work
        let operation = |chunk: Bytes, _index: usize| async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            Ok::<usize, KmsError>(chunk.len())
        };

        let results = processor
            .process_hybrid(data, operation)
            .await
            .expect("Hybrid processing should succeed");

        // Verify results
        assert!(!results.is_empty());
        let total_length: usize = results.iter().sum();
        assert_eq!(total_length, "Hello, World!".len());
    }
}
