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

//! High-performance batch processor using JoinSet
//!
//! This module provides optimized batching utilities to reduce async runtime overhead
//! and improve concurrent operation performance.

use crate::disk::error::{Error, Result};
use std::future::Future;
use std::sync::Arc;
use tokio::task::JoinSet;

/// Batch processor that executes tasks concurrently with a semaphore
pub struct AsyncBatchProcessor {
    max_concurrent: usize,
}

impl AsyncBatchProcessor {
    pub fn new(max_concurrent: usize) -> Self {
        Self { max_concurrent }
    }

    /// Execute a batch of tasks concurrently with concurrency control
    pub async fn execute_batch<T, F>(&self, tasks: Vec<F>) -> Vec<Result<T>>
    where
        T: Send + 'static,
        F: Future<Output = Result<T>> + Send + 'static,
    {
        if tasks.is_empty() {
            return Vec::new();
        }

        let semaphore = Arc::new(tokio::sync::Semaphore::new(self.max_concurrent));
        let mut join_set = JoinSet::new();
        let mut results = Vec::with_capacity(tasks.len());
        for _ in 0..tasks.len() {
            results.push(Err(Error::other("Not completed")));
        }

        // Spawn all tasks with semaphore control
        for (i, task) in tasks.into_iter().enumerate() {
            let sem = semaphore.clone();
            join_set.spawn(async move {
                let _permit = sem.acquire().await.map_err(|_| Error::other("Semaphore error"))?;
                let result = task.await;
                Ok::<(usize, Result<T>), Error>((i, result))
            });
        }

        // Collect results
        while let Some(join_result) = join_set.join_next().await {
            match join_result {
                Ok(Ok((index, task_result))) => {
                    if index < results.len() {
                        results[index] = task_result;
                    }
                }
                Ok(Err(e)) => {
                    // Semaphore or other system error - this is rare
                    tracing::warn!("Batch processor system error: {:?}", e);
                }
                Err(join_error) => {
                    // Task panicked - log but continue
                    tracing::warn!("Task panicked in batch processor: {:?}", join_error);
                }
            }
        }

        results
    }

    /// Execute batch with early termination when sufficient successful results are obtained
    pub async fn execute_batch_with_quorum<T, F>(&self, tasks: Vec<F>, required_successes: usize) -> Result<Vec<T>>
    where
        T: Send + 'static,
        F: Future<Output = Result<T>> + Send + 'static,
    {
        let results = self.execute_batch(tasks).await;
        let mut successes = Vec::new();

        for value in results.into_iter().flatten() {
            successes.push(value);
            if successes.len() >= required_successes {
                return Ok(successes);
            }
        }

        if successes.len() >= required_successes {
            Ok(successes)
        } else {
            Err(Error::other(format!(
                "Insufficient successful results: got {}, needed {}",
                successes.len(),
                required_successes
            )))
        }
    }
}

/// Global batch processor instances
pub struct GlobalBatchProcessors {
    read_processor: AsyncBatchProcessor,
    write_processor: AsyncBatchProcessor,
    metadata_processor: AsyncBatchProcessor,
}

impl GlobalBatchProcessors {
    pub fn new() -> Self {
        Self {
            read_processor: AsyncBatchProcessor::new(16),     // Higher concurrency for reads
            write_processor: AsyncBatchProcessor::new(8),     // Lower concurrency for writes
            metadata_processor: AsyncBatchProcessor::new(12), // Medium concurrency for metadata
        }
    }

    pub fn read_processor(&self) -> &AsyncBatchProcessor {
        &self.read_processor
    }

    pub fn write_processor(&self) -> &AsyncBatchProcessor {
        &self.write_processor
    }

    pub fn metadata_processor(&self) -> &AsyncBatchProcessor {
        &self.metadata_processor
    }
}

impl Default for GlobalBatchProcessors {
    fn default() -> Self {
        Self::new()
    }
}

// Global instance
use std::sync::OnceLock;

static GLOBAL_PROCESSORS: OnceLock<GlobalBatchProcessors> = OnceLock::new();

pub fn get_global_processors() -> &'static GlobalBatchProcessors {
    GLOBAL_PROCESSORS.get_or_init(GlobalBatchProcessors::new)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_batch_processor_basic() {
        let processor = AsyncBatchProcessor::new(4);

        let tasks: Vec<_> = (0..10)
            .map(|i| async move {
                tokio::time::sleep(Duration::from_millis(10)).await;
                Ok::<i32, Error>(i)
            })
            .collect();

        let results = processor.execute_batch(tasks).await;
        assert_eq!(results.len(), 10);

        // All tasks should succeed
        for (i, result) in results.iter().enumerate() {
            assert!(result.is_ok());
            assert_eq!(result.as_ref().unwrap(), &(i as i32));
        }
    }

    #[tokio::test]
    async fn test_batch_processor_with_errors() {
        let processor = AsyncBatchProcessor::new(2);

        let tasks: Vec<_> = (0..5)
            .map(|i| async move {
                tokio::time::sleep(Duration::from_millis(10)).await;
                if i % 2 == 0 {
                    Ok::<i32, Error>(i)
                } else {
                    Err(Error::other("Test error"))
                }
            })
            .collect();

        let results = processor.execute_batch(tasks).await;
        assert_eq!(results.len(), 5);

        // Check results pattern
        for (i, result) in results.iter().enumerate() {
            if i % 2 == 0 {
                assert!(result.is_ok());
                assert_eq!(result.as_ref().unwrap(), &(i as i32));
            } else {
                assert!(result.is_err());
            }
        }
    }

    #[tokio::test]
    async fn test_batch_processor_quorum() {
        let processor = AsyncBatchProcessor::new(4);

        let tasks: Vec<_> = (0..10)
            .map(|i| async move {
                tokio::time::sleep(Duration::from_millis(10)).await;
                if i < 3 {
                    Ok::<i32, Error>(i)
                } else {
                    Err(Error::other("Test error"))
                }
            })
            .collect();

        let results = processor.execute_batch_with_quorum(tasks, 2).await;
        assert!(results.is_ok());
        let successes = results.unwrap();
        assert!(successes.len() >= 2);
    }
}
