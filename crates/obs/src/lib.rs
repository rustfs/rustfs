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

//! # RustFS Observability
//!
//! provides tools for system and service monitoring
//!
//! ## feature mark
//!
//! - `gpu`: gpu monitoring function
//! - `full`: includes all functions
//!
//! to enable gpu monitoring add in cargo toml
//!
//! ```toml
//! # using gpu monitoring
//! rustfs-obs = { version = "0.1.0", features = ["gpu"] }
//!
//! # use all functions
//! rustfs-obs = { version = "0.1.0", features = ["full"] }
//! ```
///
/// ## Usage
///
/// ```no_run
/// use rustfs_obs::init_obs;
///
/// # #[tokio::main]
/// # async fn main() {
/// #   let guard = init_obs(None).await;
/// # }
/// ```
mod config;
mod global;
mod metrics;
mod system;
mod telemetry;

pub use config::AppConfig;
pub use global::*;
pub use system::SystemObserver;
