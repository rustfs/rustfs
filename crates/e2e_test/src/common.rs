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

//! Common utilities for all E2E tests
//!
//! This module provides general-purpose functionality needed across
//! different test modules, including:
//! - RustFS server process management
//! - AWS S3 client creation and configuration
//! - Basic health checks and server readiness detection
//! - Common test constants and utilities

use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::{Client, Config};
use std::path::PathBuf;
use std::process::{Child, Command};
use std::sync::Once;
use std::time::Duration;
use tokio::fs;
use tokio::net::TcpStream;
use tokio::time::sleep;
use tracing::{error, info, warn};
use uuid::Uuid;

// Common constants for all E2E tests
pub const DEFAULT_ACCESS_KEY: &str = "rustfsadmin";
pub const DEFAULT_SECRET_KEY: &str = "rustfsadmin";
pub const TEST_BUCKET: &str = "e2e-test-bucket";
pub fn workspace_root() -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.pop(); // e2e_test
    path.pop(); // crates
    path
}

/// Resolve the RustFS binary relative to the workspace.
/// Always builds the binary to ensure it's up to date.
pub fn rustfs_binary_path() -> PathBuf {
    if let Some(path) = std::env::var_os("CARGO_BIN_EXE_rustfs") {
        return PathBuf::from(path);
    }

    // Always build the binary to ensure it's up to date
    info!("Building RustFS binary to ensure it's up to date...");
    build_rustfs_binary();

    let mut binary_path = workspace_root();
    binary_path.push("target");
    let profile_dir = if cfg!(debug_assertions) { "debug" } else { "release" };
    binary_path.push(profile_dir);
    binary_path.push(format!("rustfs{}", std::env::consts::EXE_SUFFIX));

    info!("Using RustFS binary at {:?}", binary_path);
    binary_path
}

/// Build the RustFS binary using cargo
fn build_rustfs_binary() {
    let workspace = workspace_root();
    info!("Building RustFS binary from workspace: {:?}", workspace);

    let _profile = if cfg!(debug_assertions) {
        info!("Building in debug mode");
        "dev"
    } else {
        info!("Building in release mode");
        "release"
    };

    let mut cmd = Command::new("cargo");
    cmd.current_dir(&workspace).args(["build", "--bin", "rustfs"]);

    if !cfg!(debug_assertions) {
        cmd.arg("--release");
    }

    info!(
        "Executing: cargo build --bin rustfs {}",
        if cfg!(debug_assertions) { "" } else { "--release" }
    );

    let output = cmd.output().expect("Failed to execute cargo build command");

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!("Failed to build RustFS binary. Error: {stderr}");
    }

    info!("✅ RustFS binary built successfully");
}

fn awscurl_binary_path() -> PathBuf {
    std::env::var_os("AWSCURL_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("awscurl"))
}

// Global initialization
static INIT: Once = Once::new();

/// Initialize tracing for all E2E tests
pub fn init_logging() {
    INIT.call_once(|| {
        tracing_subscriber::fmt().with_env_filter("rustfs=info,e2e_test=debug").init();
    });
}

/// RustFS server environment for E2E testing
pub struct RustFSTestEnvironment {
    pub temp_dir: String,
    pub address: String,
    pub url: String,
    pub access_key: String,
    pub secret_key: String,
    pub process: Option<Child>,
}

impl RustFSTestEnvironment {
    /// Create a new test environment with unique temporary directory and port
    pub async fn new() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let temp_dir = format!("/tmp/rustfs_e2e_test_{}", Uuid::new_v4());
        fs::create_dir_all(&temp_dir).await?;

        // Use a unique port for each test environment
        let port = Self::find_available_port().await?;
        let address = format!("127.0.0.1:{port}");
        let url = format!("http://{address}");

        Ok(Self {
            temp_dir,
            address,
            url,
            access_key: DEFAULT_ACCESS_KEY.to_string(),
            secret_key: DEFAULT_SECRET_KEY.to_string(),
            process: None,
        })
    }

    /// Create a new test environment with specific address
    pub async fn with_address(address: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let temp_dir = format!("/tmp/rustfs_e2e_test_{}", Uuid::new_v4());
        fs::create_dir_all(&temp_dir).await?;

        let url = format!("http://{address}");

        Ok(Self {
            temp_dir,
            address: address.to_string(),
            url,
            access_key: DEFAULT_ACCESS_KEY.to_string(),
            secret_key: DEFAULT_SECRET_KEY.to_string(),
            process: None,
        })
    }

    /// Find an available port for the test
    pub async fn find_available_port() -> Result<u16, Box<dyn std::error::Error + Send + Sync>> {
        use std::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let port = listener.local_addr()?.port();
        drop(listener);
        Ok(port)
    }

    /// Kill any existing RustFS processes
    pub async fn cleanup_existing_processes(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Cleaning up any existing RustFS processes");
        let binary_path = rustfs_binary_path();
        let binary_name = binary_path.to_string_lossy();
        let output = Command::new("pkill").args(["-f", &binary_name]).output();

        if let Ok(output) = output
            && output.status.success()
        {
            info!("Killed existing RustFS processes: {}", binary_name);
            sleep(Duration::from_millis(1000)).await;
        }
        Ok(())
    }

    /// Start RustFS server with basic configuration
    pub async fn start_rustfs_server(&mut self, extra_args: Vec<&str>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.cleanup_existing_processes().await?;

        let mut args = vec![
            "--address",
            &self.address,
            "--access-key",
            &self.access_key,
            "--secret-key",
            &self.secret_key,
        ];

        // Add extra arguments
        args.extend(extra_args);

        // Add temp directory as the last argument
        args.push(&self.temp_dir);

        info!("Starting RustFS server with args: {:?}", args);

        let binary_path = rustfs_binary_path();
        let process = Command::new(&binary_path).args(&args).spawn()?;

        self.process = Some(process);

        // Wait for server to be ready
        self.wait_for_server_ready().await?;

        Ok(())
    }

    /// Wait for RustFS server to be ready by checking TCP connectivity
    pub async fn wait_for_server_ready(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Waiting for RustFS server to be ready on {}", self.address);

        for i in 0..30 {
            if TcpStream::connect(&self.address).await.is_ok() {
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

    /// Create an AWS S3 client configured for this RustFS instance
    pub fn create_s3_client(&self) -> Client {
        let credentials = Credentials::new(&self.access_key, &self.secret_key, None, None, "e2e-test");
        let config = Config::builder()
            .credentials_provider(credentials)
            .region(Region::new("us-east-1"))
            .endpoint_url(&self.url)
            .force_path_style(true)
            .behavior_version_latest()
            .build();

        Client::from_conf(config)
    }

    /// Create test bucket
    pub async fn create_test_bucket(&self, bucket_name: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let s3_client = self.create_s3_client();
        s3_client.create_bucket().bucket(bucket_name).send().await?;
        info!("Created test bucket: {}", bucket_name);
        Ok(())
    }

    /// Delete test bucket
    pub async fn delete_test_bucket(&self, bucket_name: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let s3_client = self.create_s3_client();
        let _ = s3_client.delete_bucket().bucket(bucket_name).send().await;
        info!("Deleted test bucket: {}", bucket_name);
        Ok(())
    }

    /// Stop the RustFS server
    pub fn stop_server(&mut self) {
        if let Some(mut process) = self.process.take() {
            info!("Stopping RustFS server");
            if let Err(e) = process.kill() {
                error!("Failed to kill RustFS process: {}", e);
            } else {
                let _ = process.wait();
                info!("RustFS server stopped");
            }
        }
    }
}

impl Drop for RustFSTestEnvironment {
    fn drop(&mut self) {
        self.stop_server();

        // Clean up temp directory
        if let Err(e) = std::fs::remove_dir_all(&self.temp_dir) {
            warn!("Failed to clean up temp directory {}: {}", self.temp_dir, e);
        }
    }
}

/// Utility function to execute awscurl commands
pub async fn execute_awscurl(
    url: &str,
    method: &str,
    body: Option<&str>,
    access_key: &str,
    secret_key: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let mut args = vec![
        "--fail-with-body",
        "--service",
        "s3",
        "--region",
        "us-east-1",
        "--access_key",
        access_key,
        "--secret_key",
        secret_key,
        "-X",
        method,
        url,
    ];

    if let Some(body_content) = body {
        args.extend(&["-d", body_content]);
    }

    info!("Executing awscurl: {} {}", method, url);
    let awscurl_path = awscurl_binary_path();
    let output = Command::new(&awscurl_path).args(&args).output()?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        return Err(format!("awscurl failed: stderr='{stderr}', stdout='{stdout}'").into());
    }

    let response = String::from_utf8_lossy(&output.stdout).to_string();
    Ok(response)
}

/// Helper function for POST requests
pub async fn awscurl_post(
    url: &str,
    body: &str,
    access_key: &str,
    secret_key: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    execute_awscurl(url, "POST", Some(body), access_key, secret_key).await
}

/// Helper function for GET requests
pub async fn awscurl_get(
    url: &str,
    access_key: &str,
    secret_key: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    execute_awscurl(url, "GET", None, access_key, secret_key).await
}

/// Helper function for PUT requests
pub async fn awscurl_put(
    url: &str,
    body: &str,
    access_key: &str,
    secret_key: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    execute_awscurl(url, "PUT", Some(body), access_key, secret_key).await
}

/// Helper function for DELETE requests
pub async fn awscurl_delete(
    url: &str,
    access_key: &str,
    secret_key: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    execute_awscurl(url, "DELETE", None, access_key, secret_key).await
}

pub struct ClusterNode {
    pub address: String,
    pub url: String,
    pub data_dir: String,
    pub process: Option<Child>,
}

pub struct RustFSTestClusterEnvironment {
    pub nodes: Vec<ClusterNode>,
    pub temp_dir: String,
    pub access_key: String,
    pub secret_key: String,
}

impl RustFSTestClusterEnvironment {
    pub async fn new(node_count: usize) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let temp_dir = format!("/tmp/rustfs_cluster_test_{}", Uuid::new_v4());
        fs::create_dir_all(&temp_dir).await?;

        let mut nodes = Vec::with_capacity(node_count);
        for i in 0..node_count {
            let port = RustFSTestEnvironment::find_available_port().await?;
            let address = format!("127.0.0.1:{}", port);
            let url = format!("http://{}", address);
            let data_dir = format!("{}/node{}", temp_dir, i);
            fs::create_dir_all(&data_dir).await?;

            nodes.push(ClusterNode {
                address,
                url,
                data_dir,
                process: None,
            });
        }

        Ok(Self {
            nodes,
            temp_dir,
            access_key: DEFAULT_ACCESS_KEY.to_string(),
            secret_key: DEFAULT_SECRET_KEY.to_string(),
        })
    }

    fn build_volumes_arg(&self) -> String {
        self.nodes
            .iter()
            .map(|n| format!("http://{}{}", n.address, n.data_dir))
            .collect::<Vec<_>>()
            .join(" ")
    }

    pub async fn start(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let binary_path = rustfs_binary_path();
        let volumes_arg = self.build_volumes_arg();

        for (i, node) in self.nodes.iter_mut().enumerate() {
            info!("Starting cluster node {} on {}", i, node.address);

            let process = Command::new(&binary_path)
                .env("RUSTFS_VOLUMES", &volumes_arg)
                .env("RUSTFS_ADDRESS", &node.address)
                .env("RUSTFS_ACCESS_KEY", &self.access_key)
                .env("RUSTFS_SECRET_KEY", &self.secret_key)
                .env("RUSTFS_CONSOLE_ENABLE", "false")
                .current_dir(&node.data_dir)
                .spawn()?;

            node.process = Some(process);
        }

        for (i, node) in self.nodes.iter().enumerate() {
            self.wait_for_node_ready(&node.address, i).await?;
        }

        self.wait_for_service_ready().await?;

        Ok(())
    }

    async fn wait_for_node_ready(&self, address: &str, idx: usize) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for attempt in 0..60 {
            if TcpStream::connect(address).await.is_ok() {
                info!("Node {} ({}) TCP ready after {} attempts", idx, address, attempt + 1);
                return Ok(());
            }
            sleep(Duration::from_secs(1)).await;
        }
        Err(format!("Node {} failed to become ready", idx).into())
    }

    async fn wait_for_service_ready(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let client = self.create_s3_client(0);

        for attempt in 0..120 {
            match client.list_buckets().send().await {
                Ok(_) => {
                    info!("Cluster service ready after {} attempts", attempt + 1);
                    return Ok(());
                }
                Err(_) => {
                    sleep(Duration::from_secs(1)).await;
                }
            }
        }
        Err("Cluster service failed to become ready".into())
    }

    pub fn create_s3_client(&self, node_idx: usize) -> Client {
        let credentials = Credentials::new(&self.access_key, &self.secret_key, None, None, "cluster-test");
        let config = Config::builder()
            .credentials_provider(credentials)
            .region(Region::new("us-east-1"))
            .endpoint_url(&self.nodes[node_idx].url)
            .force_path_style(true)
            .behavior_version_latest()
            .build();
        Client::from_conf(config)
    }

    pub fn create_all_clients(&self) -> Vec<Client> {
        (0..self.nodes.len()).map(|i| self.create_s3_client(i)).collect()
    }

    pub async fn create_test_bucket(&self, bucket_name: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let client = self.create_s3_client(0);
        client.create_bucket().bucket(bucket_name).send().await?;
        info!("Created test bucket: {}", bucket_name);
        Ok(())
    }

    pub fn stop(&mut self) {
        for (i, node) in self.nodes.iter_mut().enumerate() {
            if let Some(mut process) = node.process.take() {
                info!("Stopping cluster node {}", i);
                let _ = process.kill();
                let _ = process.wait();
            }
        }
    }
}

impl Drop for RustFSTestClusterEnvironment {
    fn drop(&mut self) {
        self.stop();
        if let Err(e) = std::fs::remove_dir_all(&self.temp_dir) {
            warn!("Failed to clean up cluster temp directory {}: {}", self.temp_dir, e);
        }
    }
}
