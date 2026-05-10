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

//! RustFS — high-performance S3-compatible object storage.
//!
//! This library exposes the [`embedded`] module which lets you start a
//! fully-functional RustFS server **in-process** — ideal for integration
//! tests that need a local S3 endpoint without Docker or child processes.
//!
//! # Quick start
//!
//! ```rust,no_run
//! use rustfs::embedded::{find_available_port, RustFSServerBuilder};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//!     let port = find_available_port()?;
//!     let server = RustFSServerBuilder::new()
//!         .address(format!("127.0.0.1:{port}"))
//!         .access_key("minioadmin")
//!         .secret_key("minioadmin")
//!         .build()
//!         .await?;
//!
//!     println!("S3 endpoint: {}", server.endpoint());
//!
//!     // ... use any S3 client against server.endpoint() ...
//!
//!     server.shutdown().await;
//!     Ok(())
//! }
//! ```
//!
//! # Limitations
//!
//! Because the underlying storage engine uses process-global singletons,
//! **only one `RustFSServer` may exist per process**. Attempting to start
//! a second server will return an error. This is fine for integration
//! tests where you start one server in a background task, run all your
//! tests, and then shut it down.

pub mod admin;
pub mod allocator_reclaim;
pub mod app;
pub mod auth;
pub mod auth_keystone;
pub mod capacity;
pub mod config;
pub mod delete_tail_activity;
pub mod embedded;
pub mod error;
pub mod init;
pub mod license;
pub mod memory_observability;
pub mod profiling;
#[cfg(any(feature = "ftps", feature = "webdav", feature = "sftp"))]
pub mod protocols;
pub mod server;
pub mod storage;
pub mod update;
pub mod version;

// Re-export from rustfs_utils so that config sub-modules can use
// `crate::apply_external_env_compat` without breaking.
pub use rustfs_utils::{ExternalEnvCompatReport, apply_external_env_compat};
