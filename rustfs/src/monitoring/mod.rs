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

//! Performance monitoring and auto-tuning module.
//!
//! This module provides real-time performance metrics collection, analysis,
//! and automatic parameter tuning for the RustFS object storage system.

pub mod metrics;
pub mod collector;

// Re-exports
pub use metrics::PerformanceMetrics;
