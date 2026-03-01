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
//! - `default`: default monitoring function
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
//!
//! ## Usage
//!
//! ```no_run
//! use rustfs_obs::init_obs;
//!
//! # #[tokio::main]
//! # async fn main() {
//! #   let _guard = match init_obs(None).await {
//! #         Ok(g) => g,
//! #         Err(e) => {
//! #             panic!("Failed to initialize observability: {:?}", e);
//! #         }
//! #     };
//! #   // Application logic here
//! #   {
//! #       // Simulate some work
//! #       tokio::time::sleep(std::time::Duration::from_secs(2)).await;
//! #       println!("Application is running...");
//! #   }
//! #   // Guard will be dropped here, flushing telemetry data
//! # }
//! ```
mod cleaner;
mod config;
mod error;
mod global;
mod system;
mod telemetry;

pub use cleaner::*;
pub use config::*;
pub use error::*;
pub use global::*;
pub use system::SystemObserver;
pub use telemetry::{OtelGuard, Recorder};
