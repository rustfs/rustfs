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

/// MetricType - Indicates the type of indicator
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricType {
    Counter,
    Gauge,
    Histogram,
}

impl MetricType {
    /// convert the metric type to a string representation
    #[allow(dead_code)]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Counter => "counter",
            Self::Gauge => "gauge",
            Self::Histogram => "histogram",
        }
    }

    /// Convert the metric type to the Prometheus value type
    /// In a Rust implementation, this might return the corresponding Prometheus Rust client type
    #[allow(dead_code)]
    pub fn as_prom(&self) -> &'static str {
        match self {
            Self::Counter => "counter.",
            Self::Gauge => "gauge.",
            Self::Histogram => "histogram.", // Histograms still use the counter value in Prometheus
        }
    }
}
