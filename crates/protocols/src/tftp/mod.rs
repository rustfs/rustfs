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

//! TFTP protocol support for RustFS (RFC 1350 read/write via `async-tftp`).
//!
//! RRQ and WRQ map to ranged S3 GETs and multipart/put uploads with
//! commit-on-close semantics. Failed WRQ transfers abort in-progress
//! multipart uploads from `Drop` and never commit partial objects.

mod config;
mod constants;
mod errors;
mod handler;
mod paths;
mod reader;
mod server;
mod writer;

pub use config::{TftpAccessMode, TftpConfig, TftpInitError};
pub use handler::TftpStorageHandler;
pub use server::TftpServer;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::session::Protocol;

    #[test]
    fn protocol_variant_is_named() {
        let _ = Protocol::Tftp;
    }
}
