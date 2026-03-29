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

//! Auto-tuner for performance optimization.
//!
//! Analyzes performance metrics and applies tuning adjustments at regular intervals.
//!
//! # Example
//!
//! ```rust,no_run
//! use rustfs_io_metrics::AutoTuner;
//!
//! # #[tokio::main]
//! # async fn main() {
//! let mut tuner = AutoTuner::new();
//!
//! // Run a single tuning iteration
//! if let Err(e) = tuner.tune().await {
//!     tracing::warn!("Auto-tuner failed: {}", e);
//! }
//! # }
//! ```

use super::performance::PerformanceMetrics;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Auto-tuner for automatic performance optimization.
///
/// Analyzes performance metrics and applies tuning adjustments at regular intervals.
pub struct AutoTuner {
    /// Current configuration
    config: Arc<RwLock<TunerConfig>>,
    /// Metrics history for trend analysis
    metrics_history: MetricsHistory,
    /// Tuner state
    state: Arc<RwLock<TunerState>>,
    /// Performance metrics reference
    performance_metrics: Option<Arc<PerformanceMetrics>>,
}

/// Tuner configuration parameters.
#[derive(Debug, Clone, Default)]
pub struct TunerConfig {
    /// Cache tuning parameters
    pub cache: CacheTunerConfig,
    /// I/O tuning parameters
    pub io: IoTunerConfig,
}

/// Cache tuner configuration.
#[derive(Debug, Clone)]
pub struct CacheTunerConfig {
    /// Enable automatic cache tuning
    pub enabled: bool,
    /// Minimum cache size (MB)
    #[allow(dead_code)] // Reserved for future cache size tuning
    pub min_size_mb: usize,
    /// Maximum cache size (MB)
    #[allow(dead_code)] // Reserved for future cache size tuning
    pub max_size_mb: usize,
    /// Target cache hit rate (0.0 - 1.0)
    pub target_hit_rate: f64,
    /// Hit rate threshold for tuning (0.0 - 1.0)
    pub hit_rate_threshold: f64,
}

impl Default for CacheTunerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            min_size_mb: 50,
            max_size_mb: 1000,
            target_hit_rate: 0.8,
            hit_rate_threshold: 0.05,
        }
    }
}

/// I/O tuner configuration.
#[derive(Debug, Clone)]
pub struct IoTunerConfig {
    /// Enable automatic I/O tuning
    pub enabled: bool,
    /// Minimum buffer size (bytes)
    #[allow(dead_code)] // Reserved for future buffer size tuning
    pub min_buffer_size: usize,
    /// Maximum buffer size (bytes)
    #[allow(dead_code)] // Reserved for future buffer size tuning
    pub max_buffer_size: usize,
    /// Target I/O latency threshold (ms)
    pub target_latency_ms: f64,
    /// Latency threshold for tuning (ms)
    pub latency_threshold_ms: f64,
}

impl Default for IoTunerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            min_buffer_size: 32 * 1024,
            max_buffer_size: 4 * 1024 * 1024,
            target_latency_ms: 50.0,
            latency_threshold_ms: 10.0,
        }
    }
}

/// Metrics history for trend analysis.
struct MetricsHistory {
    /// Cache hit rate history
    cache_hit_rates: Vec<f64>,
    /// I/O latency history
    io_latencies: Vec<Duration>,
    /// Maximum history length
    max_length: usize,
}

/// Tuner state.
#[derive(Debug, Default)]
struct TunerState {
    /// Last tuning time
    last_tuned: Option<Instant>,
    /// Number of tunings performed
    tuning_count: u64,
    /// Last tuning results
    last_results: Vec<TuningResult>,
}

/// Result of a tuning operation.
#[derive(Debug, Clone)]
pub struct TuningResult {
    /// Tuner name
    #[allow(dead_code)] // Reserved for future logging
    pub tuner: String,
    /// Action taken
    #[allow(dead_code)] // Reserved for future logging
    pub action: String,
    /// Previous value
    #[allow(dead_code)] // Reserved for future logging
    pub previous_value: String,
    /// New value
    #[allow(dead_code)] // Reserved for future logging
    pub new_value: String,
    /// Reason for tuning
    #[allow(dead_code)] // Reserved for future logging
    pub reason: String,
}

impl Default for AutoTuner {
    fn default() -> Self {
        Self::new()
    }
}

impl AutoTuner {
    /// Create a new auto-tuner with default configuration.
    pub fn new() -> Self {
        Self::with_config(TunerConfig::default())
    }

    /// Create a new auto-tuner with custom configuration.
    pub fn with_config(config: TunerConfig) -> Self {
        Self {
            config: Arc::new(RwLock::new(config)),
            metrics_history: MetricsHistory::new(100),
            state: Arc::new(RwLock::new(TunerState::default())),
            performance_metrics: None,
        }
    }

    /// Set the performance metrics reference.
    pub fn with_metrics(mut self, metrics: Arc<PerformanceMetrics>) -> Self {
        self.performance_metrics = Some(metrics);
        self
    }

    /// Perform a single tuning iteration.
    ///
    /// Analyzes current metrics and applies necessary tuning adjustments.
    pub async fn tune(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // Update metrics history first
        self.update_metrics_history().await;

        let config = self.config.read().await;
        let mut results = Vec::new();

        // Tune cache
        if config.cache.enabled {
            match self.tune_cache(&config.cache).await {
                Ok(result) => {
                    if let Some(r) = result {
                        info!("Cache tuning: {}", r.action);
                        results.push(r);
                    }
                }
                Err(e) => warn!("Cache tuning failed: {}", e),
            }
        }

        // Tune I/O
        if config.io.enabled {
            match self.tune_io(&config.io).await {
                Ok(result) => {
                    if let Some(r) = result {
                        info!("I/O tuning: {}", r.action);
                        results.push(r);
                    }
                }
                Err(e) => warn!("I/O tuning failed: {}", e),
            }
        }

        // Update state
        let mut state = self.state.write().await;
        state.last_tuned = Some(Instant::now());
        state.tuning_count += 1;
        state.last_results = results;

        debug!("Auto-tuning completed (iteration #{})", state.tuning_count);

        Ok(())
    }

    /// Update metrics history with current values.
    async fn update_metrics_history(&mut self) {
        // Get cache hit rate
        let hit_rate = self.get_cache_hit_rate().await;
        self.metrics_history.push_cache_hit_rate(hit_rate);

        // Get I/O latency
        let avg_latency = self.get_avg_io_latency().await;
        self.metrics_history.push_io_latency(avg_latency);
    }

    /// Tune cache parameters based on hit rate.
    async fn tune_cache(&self, config: &CacheTunerConfig) -> Result<Option<TuningResult>, Box<dyn std::error::Error>> {
        let hit_rate = self.get_cache_hit_rate().await;

        // Check if hit rate is below target
        if hit_rate < config.target_hit_rate {
            let threshold_met = (config.target_hit_rate - hit_rate).abs() < config.hit_rate_threshold;

            if !threshold_met {
                return Ok(Some(TuningResult {
                    tuner: "cache".to_string(),
                    action: format!(
                        "Increase cache size (hit rate: {:.1}%, target: {:.1}%)",
                        hit_rate * 100.0,
                        config.target_hit_rate * 100.0
                    ),
                    previous_value: format!("{:.1}%", hit_rate * 100.0),
                    new_value: format!("Increase to {}MB", config.max_size_mb),
                    reason: "Cache hit rate below target".to_string(),
                }));
            }
        }

        Ok(None)
    }

    /// Tune I/O parameters based on latency.
    async fn tune_io(&self, config: &IoTunerConfig) -> Result<Option<TuningResult>, Box<dyn std::error::Error>> {
        let avg_latency_ms = self.get_avg_io_latency().await.as_millis() as f64;

        // Check if latency is above target
        if avg_latency_ms > config.target_latency_ms {
            let threshold_met = (avg_latency_ms - config.target_latency_ms).abs() < config.latency_threshold_ms;

            if !threshold_met {
                return Ok(Some(TuningResult {
                    tuner: "io".to_string(),
                    action: format!(
                        "Reduce buffer size (latency: {:.1}ms, target: {:.1}ms)",
                        avg_latency_ms, config.target_latency_ms
                    ),
                    previous_value: format!("{:.1}ms", avg_latency_ms),
                    new_value: format!("Reduce to {} bytes", config.min_buffer_size),
                    reason: "I/O latency above target".to_string(),
                }));
            }
        }

        Ok(None)
    }

    /// Get current cache hit rate.
    async fn get_cache_hit_rate(&self) -> f64 {
        if let Some(metrics) = &self.performance_metrics {
            metrics.cache_hit_rate()
        } else {
            0.0
        }
    }

    /// Get average I/O latency.
    async fn get_avg_io_latency(&self) -> Duration {
        if let Some(metrics) = &self.performance_metrics {
            let avg_us = metrics.avg_io_latency_us.load(Ordering::Relaxed);
            Duration::from_micros(avg_us)
        } else {
            Duration::from_millis(10) // Default fallback
        }
    }
}

impl MetricsHistory {
    fn new(max_length: usize) -> Self {
        Self {
            cache_hit_rates: Vec::new(),
            io_latencies: Vec::new(),
            max_length,
        }
    }

    fn push_cache_hit_rate(&mut self, rate: f64) {
        self.cache_hit_rates.push(rate);
        if self.cache_hit_rates.len() > self.max_length {
            self.cache_hit_rates.remove(0);
        }
    }

    fn push_io_latency(&mut self, latency: Duration) {
        self.io_latencies.push(latency);
        if self.io_latencies.len() > self.max_length {
            self.io_latencies.remove(0);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_autotuner_creation() {
        let mut tuner = AutoTuner::new();
        assert!(tuner.tune().await.is_ok());
    }

    #[tokio::test]
    async fn test_autotuner_with_config() {
        let config = TunerConfig {
            cache: CacheTunerConfig {
                enabled: true,
                ..Default::default()
            },
            ..Default::default()
        };

        let mut tuner = AutoTuner::with_config(config);
        assert!(tuner.tune().await.is_ok());
    }

    #[tokio::test]
    async fn test_metrics_history() {
        let mut history = MetricsHistory::new(3);

        history.push_cache_hit_rate(0.7);
        history.push_cache_hit_rate(0.75);
        history.push_cache_hit_rate(0.8);

        assert_eq!(history.cache_hit_rates.len(), 3);
        assert_eq!(history.cache_hit_rates[2], 0.8);

        // Should remove oldest when exceeding max_length
        history.push_cache_hit_rate(0.85);
        assert_eq!(history.cache_hit_rates.len(), 3);
        assert_eq!(history.cache_hit_rates[0], 0.75);
    }
}
