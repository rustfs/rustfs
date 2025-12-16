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

//! Custom test environment for policy variables tests
//!
//! This module provides a custom test environment that doesn't automatically
//! stop servers when destroyed, addressing the server stopping issue.

use aws_sdk_s3::Client;
use aws_sdk_s3::config::{Config, Credentials, Region};
use std::net::TcpStream;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{info, warn};

// Default credentials
const DEFAULT_ACCESS_KEY: &str = "rustfsadmin";
const DEFAULT_SECRET_KEY: &str = "rustfsadmin";

/// Custom test environment that doesn't automatically stop servers
pub struct PolicyTestEnvironment {
    pub temp_dir: String,
    pub address: String,
    pub url: String,
    pub access_key: String,
    pub secret_key: String,
}

impl PolicyTestEnvironment {
    /// Create a new test environment with specific address
    /// This environment won't stop any server when dropped
    pub async fn with_address(address: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let temp_dir = format!("/tmp/rustfs_policy_test_{}", uuid::Uuid::new_v4());
        tokio::fs::create_dir_all(&temp_dir).await?;

        let url = format!("http://{address}");

        Ok(Self {
            temp_dir,
            address: address.to_string(),
            url,
            access_key: DEFAULT_ACCESS_KEY.to_string(),
            secret_key: DEFAULT_SECRET_KEY.to_string(),
        })
    }

    /// Create an AWS S3 client configured for this RustFS instance
    pub fn create_s3_client(&self, access_key: &str, secret_key: &str) -> Client {
        let credentials = Credentials::new(access_key, secret_key, None, None, "policy-test");
        let config = Config::builder()
            .credentials_provider(credentials)
            .region(Region::new("us-east-1"))
            .endpoint_url(&self.url)
            .force_path_style(true)
            .behavior_version_latest()
            .build();
        Client::from_conf(config)
    }

    /// Wait for RustFS server to be ready by checking TCP connectivity
    pub async fn wait_for_server_ready(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Waiting for RustFS server to be ready on {}", self.address);

        for i in 0..30 {
            if TcpStream::connect(&self.address).is_ok() {
                info!("✅ RustFS server is ready after {} attempts", i + 1);
                return Ok(());
            }

            if i == 29 {
                return Err("RustFS server failed to become ready within 30 seconds".into());
            }

            sleep(Duration::from_secs(1)).await;
        }

        Ok(())
    }
}

// Implement Drop trait that doesn't stop servers
impl Drop for PolicyTestEnvironment {
    fn drop(&mut self) {
        // Clean up temp directory only, don't stop any server
        if let Err(e) = std::fs::remove_dir_all(&self.temp_dir) {
            warn!("Failed to clean up temp directory {}: {}", self.temp_dir, e);
        }
    }
}
