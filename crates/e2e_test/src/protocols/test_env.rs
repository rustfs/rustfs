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

//! Protocol test environment for FTPS and SFTP

use std::net::TcpStream;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{info, warn};

/// Default credentials
pub const DEFAULT_ACCESS_KEY: &str = "rustfsadmin";
pub const DEFAULT_SECRET_KEY: &str = "rustfsadmin";

/// Custom test environment that doesn't automatically stop servers
pub struct ProtocolTestEnvironment {
    pub temp_dir: String,
}

impl ProtocolTestEnvironment {
    /// Create a new test environment
    /// This environment won't stop any server when dropped
    pub fn new() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let temp_dir = format!("/tmp/rustfs_protocol_test_{}", uuid::Uuid::new_v4());
        std::fs::create_dir_all(&temp_dir)?;

        Ok(Self { temp_dir })
    }

    /// Wait for server to be ready
    pub async fn wait_for_port_ready(port: u16, max_attempts: u32) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let address = format!("127.0.0.1:{}", port);

        info!("Waiting for server to be ready on {}", address);

        for i in 0..max_attempts {
            if TcpStream::connect(&address).is_ok() {
                info!("Server is ready after {} s", i + 1);
                return Ok(());
            }

            if i == max_attempts - 1 {
                return Err(format!("Server did not become ready within {} s", max_attempts).into());
            }

            sleep(Duration::from_secs(1)).await;
        }

        Ok(())
    }
}

// Implement Drop trait that doesn't stop servers
impl Drop for ProtocolTestEnvironment {
    fn drop(&mut self) {
        // Clean up temp directory only, don't stop any server
        if let Err(e) = std::fs::remove_dir_all(&self.temp_dir) {
            warn!("Failed to clean up temp directory {}: {}", self.temp_dir, e);
        }
    }
}
