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

use crate::{MultiProxyProcessor, ProxyChainAnalysis, ProxyError};
use metrics::{Counter, Gauge, Histogram, counter, gauge, histogram};
use std::net::SocketAddr;
use std::time::Duration;

/// Add cloud metadata monitoring
pub struct CloudMetadataMetrics {
    fetch_success: Counter,
    fetch_failure: Counter,
    fetch_duration: Histogram,
    ip_ranges_count: Gauge,
}

impl CloudMetadataMetrics {
    pub fn record_fetch(&self, success: bool, duration: Duration, count: usize) {
        if success {
            self.fetch_success.increment(1);
            self.ip_ranges_count.set(count as f64);
        } else {
            self.fetch_failure.increment(1);
        }
        self.fetch_duration.record(duration.as_secs_f64());
    }
}

/// Agents process monitoring metrics
pub struct ProxyMetrics {
    /// The total number of requests processed
    pub total_requests: Counter,
    /// Number of requests from trusted agents
    pub trusted_proxy_requests: Counter,
    /// Number of requests from untrusted sources
    pub untrusted_requests: Counter,
    /// The number of requests that failed to validate
    pub validation_failed: Counter,
    /// Proxy chain length distribution
    pub chain_length: Histogram,
    /// Validation is time-consuming
    pub validation_duration: Histogram,
    /// Cache hit rate
    pub cache_hit_ratio: Gauge,
}

impl ProxyMetrics {
    pub fn new() -> Self {
        Self {
            total_requests: counter!("proxy.total_requests", "The total number of requests processed"),
            trusted_proxy_requests: counter!("proxy.trusted_requests", "Requests from trusted agents"),
            untrusted_requests: counter!("proxy.untrusted_requests", "Requests from untrusted sources"),
            validation_failed: counter!("proxy.validation_failed", "Validate failed requests"),
            chain_length: histogram!("proxy.chain_length", "Proxy chain length distribution"),
            validation_duration: histogram!("proxy.validation_duration_ms", "Validation Time (ms)"),
            cache_hit_ratio: gauge!("proxy.cache_hit_ratio", "Cache hit rate"),
        }
    }

    pub fn record_request(&self, analysis: &ProxyChainAnalysis, duration_ms: f64) {
        self.total_requests.increment(1);
        self.chain_length.record(analysis.full_proxy_chain.len() as f64);
        self.validation_duration.record(duration_ms);

        if analysis.validated_hops > 0 {
            self.trusted_proxy_requests.increment(1);
        } else {
            self.untrusted_requests.increment(1);
        }

        if !analysis.security_warnings.is_empty() {
            self.validation_failed.increment(1);
        }
    }
}

/// Add monitoring in the MultiProxyProcessor
pub struct MonitoredMultiProxyProcessor {
    processor: MultiProxyProcessor,
    metrics: ProxyMetrics,
}

impl MonitoredMultiProxyProcessor {
    pub fn process_request_with_metrics(
        &self,
        peer_addr: &SocketAddr,
        headers: &http::HeaderMap,
    ) -> Result<ProxyChainAnalysis, ProxyError> {
        let start = std::time::Instant::now();

        let result = self.processor.process_request(peer_addr, headers);

        let duration_ms = start.elapsed().as_secs_f64() * 1000.0;

        match &result {
            Ok(analysis) => {
                self.metrics.record_request(analysis, duration_ms);
            }
            Err(_) => {
                self.metrics.validation_failed.increment(1);
                self.metrics.total_requests.increment(1);
            }
        }

        result
    }
}
