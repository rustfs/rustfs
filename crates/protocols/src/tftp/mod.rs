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

//! TFTP protocol support for RustFS.
//!
//! Provides a Trivial File Transfer Protocol (TFTP) server as defined
//! in RFC 1350. Each RRQ (read) and WRQ (write) is translated into an
//! S3 GetObject or PutObject call against the supplied StorageBackend.
//!
//! The module is feature-gated behind the `tftp` feature.
//!
//! Architecture. Three submodules:
//!
//! - config: TftpConfig, TftpAccessMode, and TftpInitError types.
//! - handler: TftpdHandler (async-tftp Handler impl), path resolution
//!   helpers, and the VecWriter adapter for S3 uploads.
//! - server: TftpServer entry point that binds the UDP socket and
//!   dispatches RRQ/WRQ requests.
//!
//! Authentication. TFTP has no built-in authentication. To compensate,
//! the server supports authenticating as a specific access_key, three
//! access modes (read-only, write-only, read-write), and restricting
//! access to a single bucket. Operators must still secure the TFTP
//! port at the network layer.
//!
//! Configuration contract. Environment variables that drive the server:
//! RUSTFS_TFTP_ENABLE, RUSTFS_TFTP_ADDRESS,
//! RUSTFS_TFTP_DEFAULT_BUCKET, RUSTFS_TFTP_ACCESS_MODE,
//! RUSTFS_TFTP_ACCESS_KEY. Defaults and validation live in config.rs.
//!
//! Public types: TftpServer is the entry point. TftpConfig and
//! TftpInitError are configuration and error types.

pub mod config;
pub mod handler;
pub mod server;

pub use config::{TftpAccessMode, TftpConfig, TftpInitError};
pub use server::TftpServer;
