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
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::task::JoinSet;

const BATCH_PROCESSOR_OPERATION_CUSTOM: &str = "custom";
const BATCH_PROCESSOR_OPERATION_READ: &str = "read";
const BATCH_PROCESSOR_OPERATION_WRITE: &str = "write";
const BATCH_PROCESSOR_OPERATION_METADATA: &str = "metadata";
const ENV_RUSTFS_BATCH_PROCESSOR_ADAPTIVE: &str = "RUSTFS_BATCH_PROCESSOR_ADAPTIVE";
const BATCH_PROCESSOR_ADAPTIVE_OFF: &str = "off";
const BATCH_PROCESSOR_ADAPTIVE_OBSERVE: &str = "observe";
const BATCH_PROCESSOR_ADAPTIVE_ON: &str = "on";
const BATCH_SUGGESTION_REASON_IMPROVING: &str = "improving";
const BATCH_SUGGESTION_REASON_DEGRADING: &str = "degrading";
const BATCH_SUGGESTION_REASON_STABLE: &str = "stable";
const BATCH_SUGGESTION_REASON_COOLDOWN: &str = "cooldown";
const BATCH_OBSERVATION_IMPROVING_LATENCY: Duration = Duration::from_millis(50);
const BATCH_OBSERVATION_DEGRADING_LATENCY: Duration = Duration::from_millis(250);
const BATCH_OBSERVATION_COOLDOWN: Duration = Duration::from_secs(30);
const BATCH_ADAPTIVE_MAX_CONCURRENCY_FACTOR: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BatchProcessorAdaptiveMode {
    Off,
    Observe,
    On,
}

impl BatchProcessorAdaptiveMode {
    fn should_observe(self) -> bool {
        matches!(self, Self::Observe | Self::On)
    }

    fn should_apply(self) -> bool {
        matches!(self, Self::On)
    }
}

fn parse_batch_processor_adaptive_mode(raw: &str) -> BatchProcessorAdaptiveMode {
    match raw.trim() {
        value if value.eq_ignore_ascii_case(BATCH_PROCESSOR_ADAPTIVE_OBSERVE) => BatchProcessorAdaptiveMode::Observe,
        value if value.eq_ignore_ascii_case(BATCH_PROCESSOR_ADAPTIVE_ON) => BatchProcessorAdaptiveMode::On,
        value if value.eq_ignore_ascii_case(BATCH_PROCESSOR_ADAPTIVE_OFF) => BatchProcessorAdaptiveMode::Off,
        _ => BatchProcessorAdaptiveMode::Off,
    }
}

fn batch_processor_adaptive_mode_from_env() -> BatchProcessorAdaptiveMode {
    rustfs_utils::get_env_opt_str(ENV_RUSTFS_BATCH_PROCESSOR_ADAPTIVE)
        .as_deref()
        .map(parse_batch_processor_adaptive_mode)
        .unwrap_or(BatchProcessorAdaptiveMode::Off)
}

fn batch_processor_adaptive_mode() -> BatchProcessorAdaptiveMode {
    // The gate cannot change at runtime; parse it once instead of re-reading
    // the environment on every batch.
    static MODE: std::sync::LazyLock<BatchProcessorAdaptiveMode> =
        std::sync::LazyLock::new(batch_processor_adaptive_mode_from_env);
    *MODE
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BatchConcurrencySuggestion {
    concurrency: usize,
    reason: &'static str,
}

#[derive(Debug, Clone, Copy)]
struct BatchObservation {
    batch_size: usize,
    success_count: usize,
    error_count: usize,
    timeout_count: usize,
    max_queue_wait: Duration,
    execution_latency: Duration,
}

#[derive(Debug)]
struct BatchObservationState {
    last_suggestion_at: Option<Instant>,
    last_suggested_concurrency: usize,
}

impl BatchObservationState {
    fn new(configured_concurrency: usize) -> Self {
        Self {
            last_suggestion_at: None,
            last_suggested_concurrency: configured_concurrency,
        }
    }

    fn suggest(
        &mut self,
        current_concurrency: usize,
        configured_concurrency: usize,
        observation: BatchObservation,
        now: Instant,
    ) -> BatchConcurrencySuggestion {
        let raw = calculate_batch_concurrency_suggestion(current_concurrency, configured_concurrency, observation);
        if raw.concurrency == current_concurrency {
            self.last_suggested_concurrency = current_concurrency;
            return raw;
        }

        if self
            .last_suggestion_at
            .is_some_and(|last_suggestion_at| now.duration_since(last_suggestion_at) < BATCH_OBSERVATION_COOLDOWN)
        {
            return BatchConcurrencySuggestion {
                concurrency: self.last_suggested_concurrency,
                reason: BATCH_SUGGESTION_REASON_COOLDOWN,
            };
        }

        self.last_suggestion_at = Some(now);
        self.last_suggested_concurrency = raw.concurrency;
        raw
    }
}

fn calculate_batch_concurrency_suggestion(
    current_concurrency: usize,
    configured_concurrency: usize,
    observation: BatchObservation,
) -> BatchConcurrencySuggestion {
    let current_concurrency = current_concurrency.max(1);
    let configured_concurrency = configured_concurrency.max(1);

    if observation.batch_size == 0 {
        return BatchConcurrencySuggestion {
            concurrency: current_concurrency,
            reason: BATCH_SUGGESTION_REASON_STABLE,
        };
    }

    if observation.timeout_count > 0
        || observation.error_count > 0
        || (observation.batch_size >= current_concurrency && observation.execution_latency >= BATCH_OBSERVATION_DEGRADING_LATENCY)
    {
        return BatchConcurrencySuggestion {
            concurrency: decrease_batch_concurrency(current_concurrency),
            reason: BATCH_SUGGESTION_REASON_DEGRADING,
        };
    }

    if observation.success_count == observation.batch_size
        && observation.batch_size >= current_concurrency
        && observation.execution_latency <= BATCH_OBSERVATION_IMPROVING_LATENCY
    {
        return BatchConcurrencySuggestion {
            concurrency: increase_batch_concurrency(current_concurrency, configured_concurrency, observation.batch_size),
            reason: BATCH_SUGGESTION_REASON_IMPROVING,
        };
    }

    BatchConcurrencySuggestion {
        concurrency: current_concurrency,
        reason: BATCH_SUGGESTION_REASON_STABLE,
    }
}

fn decrease_batch_concurrency(current_concurrency: usize) -> usize {
    (current_concurrency.saturating_mul(3) / 4).max(1)
}

fn increase_batch_concurrency(current_concurrency: usize, configured_concurrency: usize, batch_size: usize) -> usize {
    let step = (current_concurrency / 4).max(1);
    // Suggestions build on the previous suggestion, so bound the ratchet at a
    // hard multiple of the configured baseline instead of the current value.
    let upper_bound = configured_concurrency
        .saturating_mul(BATCH_ADAPTIVE_MAX_CONCURRENCY_FACTOR)
        .max(current_concurrency);
    current_concurrency
        .saturating_add(step)
        .min(upper_bound)
        .min(batch_size.max(current_concurrency))
}

fn is_timeout_error(err: &Error) -> bool {
    matches!(err, Error::Timeout | Error::SourceStalled)
        || matches!(err, Error::Io(io_err) if io_err.kind() == std::io::ErrorKind::TimedOut)
}

/// Batch processor that executes tasks concurrently with a semaphore
pub struct AsyncBatchProcessor {
    max_concurrent: usize,
    operation: &'static str,
    observation_state: Mutex<BatchObservationState>,
}

impl AsyncBatchProcessor {
    pub fn new(max_concurrent: usize) -> Self {
        Self::new_with_operation(max_concurrent, BATCH_PROCESSOR_OPERATION_CUSTOM)
    }

    fn new_with_operation(max_concurrent: usize, operation: &'static str) -> Self {
        let max_concurrent = max_concurrent.max(1);
        Self {
            max_concurrent,
            operation,
            observation_state: Mutex::new(BatchObservationState::new(max_concurrent)),
        }
    }

    fn execution_concurrency(&self, mode: BatchProcessorAdaptiveMode) -> usize {
        if !mode.should_apply() {
            return self.max_concurrent;
        }

        self.observation_state
            .lock()
            .map(|state| state.last_suggested_concurrency.max(1))
            .unwrap_or(self.max_concurrent)
    }

    fn observe_batch_with_mode(
        &self,
        mode: BatchProcessorAdaptiveMode,
        execution_concurrency: usize,
        observation: BatchObservation,
    ) -> BatchConcurrencySuggestion {
        let execution_concurrency = execution_concurrency.max(1);
        if !mode.should_observe() {
            return BatchConcurrencySuggestion {
                concurrency: execution_concurrency,
                reason: BATCH_SUGGESTION_REASON_STABLE,
            };
        }

        let suggestion = match self.observation_state.lock() {
            Ok(mut state) => state.suggest(execution_concurrency, self.max_concurrent, observation, Instant::now()),
            Err(_) => calculate_batch_concurrency_suggestion(execution_concurrency, self.max_concurrent, observation),
        };

        rustfs_io_metrics::record_batch_processor_observation(rustfs_io_metrics::BatchProcessorObservation {
            operation: self.operation,
            batch_size: observation.batch_size,
            configured_concurrency: execution_concurrency,
            max_queue_wait_secs: observation.max_queue_wait.as_secs_f64(),
            execution_latency_secs: observation.execution_latency.as_secs_f64(),
            successes: observation.success_count,
            errors: observation.error_count,
            timeouts: observation.timeout_count,
            suggested_concurrency: suggestion.concurrency,
            suggestion_reason: suggestion.reason,
        });

        suggestion
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

        let batch_size = tasks.len();
        let batch_started_at = Instant::now();
        let adaptive_mode = batch_processor_adaptive_mode();
        let execution_concurrency = self.execution_concurrency(adaptive_mode);
        let semaphore = Arc::new(tokio::sync::Semaphore::new(execution_concurrency));
        let mut join_set = JoinSet::new();
        let mut results = Vec::with_capacity(batch_size);
        for _ in 0..batch_size {
            results.push(Err(Error::other("Not completed")));
        }

        // Spawn all tasks with semaphore control
        for (i, task) in tasks.into_iter().enumerate() {
            let sem = semaphore.clone();
            let queued_at = Instant::now();
            join_set.spawn(async move {
                let _permit = sem.acquire().await.map_err(|_| Error::other("Semaphore error"))?;
                let queue_wait = queued_at.elapsed();
                let result = task.await;
                Ok::<(usize, Result<T>, Duration), Error>((i, result, queue_wait))
            });
        }

        // Collect results
        let mut max_queue_wait = Duration::ZERO;
        while let Some(join_result) = join_set.join_next().await {
            match join_result {
                Ok(Ok((index, task_result, queue_wait))) => {
                    max_queue_wait = max_queue_wait.max(queue_wait);
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

        let mut success_count = 0;
        let mut error_count = 0;
        let mut timeout_count = 0;
        for result in &results {
            match result {
                Ok(_) => success_count += 1,
                Err(err) if is_timeout_error(err) => timeout_count += 1,
                Err(_) => error_count += 1,
            }
        }
        self.observe_batch_with_mode(
            adaptive_mode,
            execution_concurrency,
            BatchObservation {
                batch_size,
                success_count,
                error_count,
                timeout_count,
                max_queue_wait,
                execution_latency: batch_started_at.elapsed(),
            },
        );

        results
    }

    /// Execute batch with early termination when sufficient successful results are obtained
    pub async fn execute_batch_with_quorum<T, F>(&self, tasks: Vec<F>, required_successes: usize) -> Result<Vec<T>>
    where
        T: Send + 'static,
        F: Future<Output = Result<T>> + Send + 'static,
    {
        if required_successes == 0 {
            return Ok(Vec::new());
        }

        if tasks.is_empty() {
            return Err(Error::other(format!(
                "Insufficient successful results: got 0, needed {required_successes}"
            )));
        }

        let batch_size = tasks.len();
        let batch_started_at = Instant::now();
        let adaptive_mode = batch_processor_adaptive_mode();
        let execution_concurrency = self.execution_concurrency(adaptive_mode);
        let semaphore = Arc::new(tokio::sync::Semaphore::new(execution_concurrency));
        let mut join_set = JoinSet::new();
        let mut successes = Vec::new();
        let mut pending_tasks = batch_size;
        let mut first_error = None;
        let mut error_count = 0;
        let mut timeout_count = 0;
        let mut max_queue_wait = Duration::ZERO;

        for task in tasks {
            let sem = semaphore.clone();
            let queued_at = Instant::now();
            join_set.spawn(async move {
                let _permit = sem
                    .acquire()
                    .await
                    .map_err(|_| (Error::other("Semaphore error"), queued_at.elapsed()))?;
                let queue_wait = queued_at.elapsed();
                task.await.map(|value| (value, queue_wait)).map_err(|err| (err, queue_wait))
            });
        }

        while let Some(join_result) = join_set.join_next().await {
            pending_tasks = pending_tasks.saturating_sub(1);

            match join_result {
                Ok(Ok((value, queue_wait))) => {
                    max_queue_wait = max_queue_wait.max(queue_wait);
                    successes.push(value);
                    if successes.len() >= required_successes {
                        self.observe_batch_with_mode(
                            adaptive_mode,
                            execution_concurrency,
                            BatchObservation {
                                batch_size,
                                success_count: successes.len(),
                                error_count,
                                timeout_count,
                                max_queue_wait,
                                execution_latency: batch_started_at.elapsed(),
                            },
                        );
                        return Ok(successes);
                    }
                }
                Ok(Err((err, queue_wait))) => {
                    max_queue_wait = max_queue_wait.max(queue_wait);
                    if is_timeout_error(&err) {
                        timeout_count += 1;
                    } else {
                        error_count += 1;
                    }
                    if first_error.is_none() {
                        first_error = Some(err);
                    }
                }
                Err(join_error) => {
                    error_count += 1;
                    if first_error.is_none() {
                        first_error = Some(Error::other(format!("Task panicked in quorum batch processor: {join_error}")));
                    }
                }
            }

            if successes.len() + pending_tasks < required_successes {
                self.observe_batch_with_mode(
                    adaptive_mode,
                    execution_concurrency,
                    BatchObservation {
                        batch_size,
                        success_count: successes.len(),
                        error_count,
                        timeout_count,
                        max_queue_wait,
                        execution_latency: batch_started_at.elapsed(),
                    },
                );
                return Err(first_error.unwrap_or_else(|| {
                    Error::other(format!(
                        "Insufficient successful results: got {}, needed {}",
                        successes.len(),
                        required_successes
                    ))
                }));
            }
        }

        self.observe_batch_with_mode(
            adaptive_mode,
            execution_concurrency,
            BatchObservation {
                batch_size,
                success_count: successes.len(),
                error_count,
                timeout_count,
                max_queue_wait,
                execution_latency: batch_started_at.elapsed(),
            },
        );
        Err(first_error.unwrap_or_else(|| {
            Error::other(format!(
                "Insufficient successful results: got {}, needed {}",
                successes.len(),
                required_successes
            ))
        }))
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
            read_processor: AsyncBatchProcessor::new_with_operation(16, BATCH_PROCESSOR_OPERATION_READ),
            write_processor: AsyncBatchProcessor::new_with_operation(8, BATCH_PROCESSOR_OPERATION_WRITE),
            metadata_processor: AsyncBatchProcessor::new_with_operation(12, BATCH_PROCESSOR_OPERATION_METADATA),
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

    #[tokio::test(start_paused = true)]
    async fn test_batch_processor_quorum_returns_before_slow_tail() {
        let processor = AsyncBatchProcessor::new(4);
        let started = tokio::time::Instant::now();

        let tasks: Vec<_> = [(10_u64, Ok(1_i32)), (15, Ok(2)), (250, Ok(3))]
            .into_iter()
            .map(|(delay_ms, outcome)| async move {
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                outcome
            })
            .collect();

        let results = processor
            .execute_batch_with_quorum(tasks, 2)
            .await
            .expect("quorum should succeed");
        assert_eq!(results.len(), 2);
        assert!(started.elapsed() < Duration::from_millis(100));
    }

    #[tokio::test(start_paused = true)]
    async fn test_batch_processor_quorum_fails_once_quorum_becomes_impossible() {
        let processor = AsyncBatchProcessor::new(4);
        let started = tokio::time::Instant::now();

        let tasks: Vec<_> = vec![
            (10_u64, Ok(1_i32)),
            (15, Err(Error::other("first failure"))),
            (20, Err(Error::other("second failure"))),
            (250, Ok(4)),
        ]
        .into_iter()
        .map(|(delay_ms, outcome)| async move {
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            outcome
        })
        .collect();

        let err = processor
            .execute_batch_with_quorum(tasks, 3)
            .await
            .expect_err("quorum should fail once it becomes impossible");

        assert!(err.to_string().contains("first failure"));
        assert!(started.elapsed() < Duration::from_millis(120));
    }

    #[tokio::test]
    async fn test_batch_processor_observe_only_keeps_configured_concurrency() {
        let processor = AsyncBatchProcessor::new(2);

        let tasks: Vec<_> = (0..4)
            .map(|i| async move {
                tokio::time::sleep(Duration::from_millis(5)).await;
                Ok::<i32, Error>(i)
            })
            .collect();

        let results = processor.execute_batch(tasks).await;
        assert_eq!(results.len(), 4);
        assert_eq!(processor.max_concurrent, 2);
    }

    #[test]
    fn batch_processor_adaptive_mode_defaults_to_off_and_parses_supported_values() {
        assert_eq!(parse_batch_processor_adaptive_mode(""), BatchProcessorAdaptiveMode::Off);
        assert_eq!(parse_batch_processor_adaptive_mode("off"), BatchProcessorAdaptiveMode::Off);
        assert_eq!(parse_batch_processor_adaptive_mode("observe"), BatchProcessorAdaptiveMode::Observe);
        assert_eq!(parse_batch_processor_adaptive_mode("on"), BatchProcessorAdaptiveMode::On);
        assert_eq!(parse_batch_processor_adaptive_mode("unknown"), BatchProcessorAdaptiveMode::Off);
        assert!(!BatchProcessorAdaptiveMode::Off.should_observe());
        assert!(BatchProcessorAdaptiveMode::Observe.should_observe());
        assert!(BatchProcessorAdaptiveMode::On.should_observe());
        assert!(!BatchProcessorAdaptiveMode::Observe.should_apply());
        assert!(BatchProcessorAdaptiveMode::On.should_apply());
    }

    #[test]
    fn batch_processor_adaptive_mode_env_gate_is_parsed_from_env() {
        temp_env::with_var(ENV_RUSTFS_BATCH_PROCESSOR_ADAPTIVE, None::<&str>, || {
            assert_eq!(batch_processor_adaptive_mode_from_env(), BatchProcessorAdaptiveMode::Off);
        });
        temp_env::with_var(ENV_RUSTFS_BATCH_PROCESSOR_ADAPTIVE, Some("observe"), || {
            assert_eq!(batch_processor_adaptive_mode_from_env(), BatchProcessorAdaptiveMode::Observe);
        });
    }

    #[test]
    fn batch_processor_observation_gate_keeps_default_off_stable() {
        let processor = AsyncBatchProcessor::new(8);
        let suggestion = processor.observe_batch_with_mode(
            BatchProcessorAdaptiveMode::Off,
            processor.execution_concurrency(BatchProcessorAdaptiveMode::Off),
            BatchObservation {
                batch_size: 16,
                success_count: 16,
                error_count: 0,
                timeout_count: 0,
                max_queue_wait: Duration::ZERO,
                execution_latency: Duration::from_millis(20),
            },
        );

        assert_eq!(suggestion.reason, BATCH_SUGGESTION_REASON_STABLE);
        assert_eq!(suggestion.concurrency, 8);
    }

    #[test]
    fn batch_processor_observation_gate_observe_records_suggestion() {
        let processor = AsyncBatchProcessor::new(8);
        let suggestion = processor.observe_batch_with_mode(
            BatchProcessorAdaptiveMode::Observe,
            processor.execution_concurrency(BatchProcessorAdaptiveMode::Observe),
            BatchObservation {
                batch_size: 16,
                success_count: 16,
                error_count: 0,
                timeout_count: 0,
                max_queue_wait: Duration::ZERO,
                execution_latency: Duration::from_millis(20),
            },
        );

        assert_eq!(suggestion.reason, BATCH_SUGGESTION_REASON_IMPROVING);
        assert!(suggestion.concurrency > 8);
        assert_eq!(processor.execution_concurrency(BatchProcessorAdaptiveMode::Observe), 8);
    }

    #[test]
    fn batch_processor_adaptive_on_applies_last_suggestion_to_next_batch() {
        let processor = AsyncBatchProcessor::new(8);
        let suggestion = processor.observe_batch_with_mode(
            BatchProcessorAdaptiveMode::On,
            processor.execution_concurrency(BatchProcessorAdaptiveMode::On),
            BatchObservation {
                batch_size: 16,
                success_count: 16,
                error_count: 0,
                timeout_count: 0,
                max_queue_wait: Duration::ZERO,
                execution_latency: Duration::from_millis(20),
            },
        );

        assert_eq!(suggestion.reason, BATCH_SUGGESTION_REASON_IMPROVING);
        assert!(suggestion.concurrency > 8);
        assert_eq!(processor.execution_concurrency(BatchProcessorAdaptiveMode::On), suggestion.concurrency);
    }

    #[test]
    fn batch_processor_suggestion_increases_for_fast_successful_batches() {
        let suggestion = calculate_batch_concurrency_suggestion(
            8,
            8,
            BatchObservation {
                batch_size: 16,
                success_count: 16,
                error_count: 0,
                timeout_count: 0,
                max_queue_wait: Duration::ZERO,
                execution_latency: Duration::from_millis(20),
            },
        );

        assert_eq!(suggestion.reason, BATCH_SUGGESTION_REASON_IMPROVING);
        assert!(suggestion.concurrency > 8);
    }

    #[test]
    fn batch_processor_suggestion_decreases_for_timeout_batches() {
        let suggestion = calculate_batch_concurrency_suggestion(
            8,
            8,
            BatchObservation {
                batch_size: 16,
                success_count: 14,
                error_count: 0,
                timeout_count: 2,
                max_queue_wait: Duration::ZERO,
                execution_latency: Duration::from_millis(20),
            },
        );

        assert_eq!(suggestion.reason, BATCH_SUGGESTION_REASON_DEGRADING);
        assert!(suggestion.concurrency < 8);
        assert!(suggestion.concurrency >= 1);
    }

    #[test]
    fn batch_processor_suggestion_clamps_to_batch_size() {
        let suggestion = calculate_batch_concurrency_suggestion(
            64,
            64,
            BatchObservation {
                batch_size: 65,
                success_count: 65,
                error_count: 0,
                timeout_count: 0,
                max_queue_wait: Duration::ZERO,
                execution_latency: Duration::from_millis(20),
            },
        );

        assert_eq!(suggestion.reason, BATCH_SUGGESTION_REASON_IMPROVING);
        assert_eq!(suggestion.concurrency, 65);
    }

    #[test]
    fn batch_processor_suggestion_cooldown_prevents_rapid_oscillation() {
        let mut state = BatchObservationState::new(8);
        let now = Instant::now();
        let observation = BatchObservation {
            batch_size: 16,
            success_count: 16,
            error_count: 0,
            timeout_count: 0,
            max_queue_wait: Duration::ZERO,
            execution_latency: Duration::from_millis(20),
        };

        let first = state.suggest(8, 8, observation, now);
        let second = state.suggest(8, 8, observation, now + Duration::from_secs(1));

        assert_eq!(first.reason, BATCH_SUGGESTION_REASON_IMPROVING);
        assert_eq!(second.reason, BATCH_SUGGESTION_REASON_COOLDOWN);
        assert_eq!(second.concurrency, first.concurrency);
    }

    #[test]
    fn batch_processor_suggestion_growth_is_capped_relative_to_configured_concurrency() {
        const CONFIGURED: usize = 8;
        let mut state = BatchObservationState::new(CONFIGURED);
        let mut current = CONFIGURED;
        let mut now = Instant::now();
        let observation = BatchObservation {
            batch_size: 1024,
            success_count: 1024,
            error_count: 0,
            timeout_count: 0,
            max_queue_wait: Duration::ZERO,
            execution_latency: Duration::from_millis(20),
        };

        for _ in 0..64 {
            current = state.suggest(current, CONFIGURED, observation, now).concurrency;
            now += BATCH_OBSERVATION_COOLDOWN;
        }

        assert_eq!(current, CONFIGURED * BATCH_ADAPTIVE_MAX_CONCURRENCY_FACTOR);
    }
}
