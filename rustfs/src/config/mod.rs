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

//! Configuration module for RustFS.
//!
//! This module is organized into the following submodules:
//!
//! - [`cli`]: Command-line interface definitions (Cli, Commands, ServerOpts, InfoOpts)
//! - [`opt`]: Parsed server options (Opt) and parsing methods
//! - [`config_struct`]: Server configuration (Config)
//! - [`snapshot`]: Configuration snapshot for info command
//! - [`info`]: Info command execution
//!
//! # Usage
//!
//! ```ignore
//! use rustfs::config::{Config, Opt, CommandResult};
//!
//! // Parse command line arguments
//! let result = Opt::parse_command(std::env::args())?;
//!
//! match result {
//!     CommandResult::Server(config) => {
//!         // Start server with config
//!     }
//!     CommandResult::Info(opts) => {
//!         // Display info
//!     }
//! }
//! ```

mod cli;
mod config_struct;
mod info;
mod opt;
mod snapshot;

#[cfg(test)]
mod config_test;

// Re-export public types
pub use cli::{CommandResult, InfoOpts, InfoType};
pub use config_struct::Config;
pub use info::execute_info;
pub use opt::Opt;
pub use snapshot::{get_config_snapshot_for_display, init_config_snapshot};

// Re-export workload profiles
pub mod workload_profiles;
