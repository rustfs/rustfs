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

//! SFTP protocol adapter for RustFS
//!
//! This module provides the SFTP (SSH File Transfer Protocol) adapter implementation.

use std::io::Result;
use crate::auth::IamAdapter;
use crate::storage::ecfs::EcStoreAdapter;

/// Start the SFTP server
pub async fn start_sftp_server(
    address: &str,
    host_key: Option<&str>,
    iam_adapter: IamAdapter,
    ecstore_adapter: EcStoreAdapter,
) -> Result<()> {
    // Create SFTP filesystem using the existing EcStoreAdapter
    // This is a placeholder - in a real implementation, you would create
    // a filesystem that wraps the EcStoreAdapter

    // Create SFTP authenticator using the existing IamAdapter
    // This is a placeholder - in a real implementation, you would create
    // an authenticator that wraps the IamAdapter

    println!("SFTP server would start on {} with host key: {:?}", address, host_key);
    println!("Using existing auth and storage modules through session context");

    Ok(())
}