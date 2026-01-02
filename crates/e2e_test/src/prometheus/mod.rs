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

//! Prometheus Metrics End-to-End Tests
//!
//! This module contains comprehensive end-to-end tests for RustFS Prometheus
//! metrics endpoints, including:
//! - Configuration endpoint with JWT token generation
//! - Cluster, bucket, node, and resource metrics endpoints
//! - Bearer token authentication validation
//! - Invalid token and missing auth rejection

#[cfg(test)]
mod prometheus_test;
