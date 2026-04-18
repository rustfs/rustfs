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

//! Compatibility facade for RustFS metrics migration.
//!
//! Preferred new APIs:
//! - Runtime entry: `rustfs_obs::init_metrics_runtime(token)`
//! - Metrics schema/report/collectors: `rustfs_obs::metrics::*`
//! - Sampler and low-level process/system snapshots: `rustfs_io_metrics::*`
//!
//! Old to new import mapping examples:
//! - `rustfs_metrics::init_metrics_system` -> `rustfs_obs::init_metrics_runtime`
//! - `rustfs_metrics::collectors::*` -> `rustfs_obs::metrics::collectors::*`
//! - `rustfs_metrics::MetricDescriptor` -> `rustfs_obs::metrics::schema::MetricDescriptor`

pub mod collectors;
pub mod constants;
pub mod format;
mod global;
mod metrics_type;

pub use format::report_metrics;
pub use global::init_metrics_system;
pub use metrics_type::*;
pub use rustfs_io_metrics as io_metrics;
pub use rustfs_obs::init_metrics_runtime;
pub use rustfs_obs::metrics as obs_metrics;
