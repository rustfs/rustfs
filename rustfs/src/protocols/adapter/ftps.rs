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

//! FTPS protocol adapter for RustFS
//!
//! This module provides the FTP over TLS protocol adapter implementation.

use std::io::Result;
use libunftp::{Server, ServerBuilder};
use crate::auth::IamAdapter;
use crate::storage::ecfs::EcStoreAdapter;

/// Start the FTPS server
pub async fn start_ftps_server(
    address: &str,
    certs_file: Option<&str>,
    key_file: Option<&str>,
    iam_adapter: IamAdapter,
    ecstore_adapter: EcStoreAdapter,
) -> Result<()> {
    // Create FTPS storage backend using the existing EcStoreAdapter
    // This is a placeholder - in a real implementation, you would create
    // a storage backend that wraps the EcStoreAdapter

    // Create FTPS authenticator using the existing IamAdapter
    // This is a placeholder - in a real implementation, you would create
    // an authenticator that wraps the IamAdapter

    println!("FTPS server would start on {} with certs: {:?} and keys: {:?}", address, certs_file, key_file);
    println!("Using existing auth and storage modules through session context");

    Ok(())
}