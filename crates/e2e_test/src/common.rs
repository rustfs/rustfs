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

    // Read features from environment variable for e2e tests
    if let Ok(features) = std::env::var("RUSTFS_BUILD_FEATURES")
        && !features.is_empty()
    {
        cmd.arg("--features").arg(&features);
        info!("Building with features: {}", features);
    }

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

/// Represents a single RustFS server instance in a test cluster.
///
/// Each `ClusterNode` tracks the node's network address, base URL for
/// S3-compatible requests, on-disk data directory, and the underlying
/// child process handle when the node is running.
pub struct ClusterNode {
    pub address: String,
    pub url: String,
    pub data_dir: String,
    pub process: Option<Child>,
}

/// Test environment for managing a multi-node RustFS cluster.
///
/// `RustFSTestClusterEnvironment` is responsible for starting and stopping
/// a group of `ClusterNode`s, managing their temporary storage directory,
/// and providing the shared access and secret keys used by tests to
/// interact with the cluster.
pub struct RustFSTestClusterEnvironment {
    pub nodes: Vec<ClusterNode>,
    pub temp_dir: String,
    pub access_key: String,
    pub secret_key: String,
}

impl RustFSTestClusterEnvironment {
    /// Create a new RustFS test cluster environment with the specified number of nodes.
    ///
    /// Generates a unique temporary root directory for the cluster, allocates an available TCP port
    /// for each node, creates an independent data directory for every node, and initializes basic
    /// cluster node configurations (node processes are not started at this stage).
    ///
    /// # Arguments
    ///
    /// * `node_count` - The number of nodes to create in the cluster, must be a positive integer
    ///   (an empty cluster will cause errors in subsequent startup operations).
    ///
    /// # Returns
    ///
    /// * `Ok(Self)` - A new instance of `RustFSTestClusterEnvironment` with initialized node
    ///   configurations and temporary directory info on success.
    /// * `Err(Box<dyn Error + Send + Sync>)` - An error if any step fails, such as temporary
    ///   directory creation failure or available port lookup failure.
    pub async fn new(node_count: usize) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        if node_count == 0 {
            return Err("Node count must be greater than zero".into());
        }
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

    /// Build the volumes argument string for RustFS binary (internal helper method).
    ///
    /// Concatenates the address and data directory of all cluster nodes into a single string
    /// used as the `RUSTFS_VOLUMES` environment variable for RustFS node processes.
    fn build_volumes_arg(&self) -> String {
        self.nodes
            .iter()
            .map(|n| format!("http://{}{}", n.address, n.data_dir))
            .collect::<Vec<_>>()
            .join(" ")
    }

    /// Start all node processes in the RustFS cluster and wait for the cluster service to be ready.
    ///
    /// Spawns a RustFS binary process for each node with necessary environment variable configurations,
    /// first waits for each node's TCP port to be reachable, then verifies the cluster's S3-compatible
    /// service availability via the S3 API.
    ///
    /// # Returns
    ///
    /// * `Ok(())` - All nodes start successfully and the cluster S3 service is ready for requests.
    /// * `Err(Box<dyn Error + Send + Sync>)` - An error if process spawning fails, TCP port readiness
    ///   times out, or cluster service readiness times out.
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

    /// Wait for a single cluster node's TCP port to become reachable (internal helper method).
    ///
    /// Attempts to establish a TCP connection to the node's address, retries up to 60 times
    /// with a 1-second interval between attempts. Fails if the port is unreachable after all retries.
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

    /// Wait for the entire cluster's S3-compatible service to be ready (internal helper method).
    ///
    /// Verifies service availability by calling the S3 `list_buckets` API, retries up to 120 times
    /// with a 1-second interval between attempts. Fails if the API call remains unsuccessful after all retries.
    async fn wait_for_service_ready(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let client = self.create_s3_client(0)?;

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

    /// Create an S3 client configured to communicate with a specific cluster node.
    ///
    /// Configures the S3 client with the cluster's authentication credentials, a fixed `us-east-1` region,
    /// the target node's endpoint URL, and enforces path-style access (required for RustFS S3 compatibility).
    /// Performs a validity check on the node index before creating the client to avoid out-of-bounds errors.
    ///
    /// # Arguments
    ///
    /// * `node_idx` - The zero-based index of the target cluster node. Must be in the range `[0, total_nodes - 1]`.
    ///
    /// # Returns
    ///
    /// * `Ok(Client)` - A fully configured AWS S3 `Client` instance for the specified node on success.
    /// * `Err(Box<dyn Error + Send + Sync>)` - An error if the node index is invalid, or if the S3 client configuration fails.
    pub fn create_s3_client(&self, node_idx: usize) -> Result<Client, Box<dyn std::error::Error + Send + Sync>> {
        if node_idx >= self.nodes.len() {
            return Err("node_idx is invalid".into());
        }
        let credentials = Credentials::new(&self.access_key, &self.secret_key, None, None, "cluster-test");
        let config = Config::builder()
            .credentials_provider(credentials)
            .region(Region::new("us-east-1"))
            .endpoint_url(&self.nodes[node_idx].url)
            .force_path_style(true)
            .behavior_version_latest()
            .build();
        Ok(Client::from_conf(config))
    }

    /// Create S3 clients for all nodes in the RustFS cluster and collect them into a vector.
    ///
    /// Iterates over all cluster node indices, calls `create_s3_client` for each index, and aggregates
    /// the resulting clients into a pre-allocated vector. Terminates immediately and returns an error
    /// if any single node's S3 client creation fails (fails fast behavior).
    ///
    /// # Returns
    ///
    /// * `Ok(Vec<Client>)` - A vector of configured S3 `Client` instances (one per cluster node) on full success.
    /// * `Err(Box<dyn Error + Send + Sync>)` - An error with a descriptive message if any client creation fails,
    ///   including the underlying error from `create_s3_client`.
    pub fn create_all_clients(&self) -> Result<Vec<Client>, Box<dyn std::error::Error + Send + Sync>> {
        (0..self.nodes.len()).map(|i| self.create_s3_client(i)).try_fold(
            Vec::with_capacity(self.nodes.len()),
            |mut clients, result| match result {
                Ok(client) => {
                    clients.push(client);
                    Ok(clients)
                }
                Err(e) => Err(format!("Failed to create S3 client for node: {}", e).into()),
            },
        )
    }

    /// Create a test S3 bucket in the RustFS cluster.
    ///
    /// Uses the S3 client of the first cluster node to call the S3 `create_bucket` API and
    /// create a bucket with the specified name (follows S3 bucket naming conventions).
    ///
    /// # Arguments
    ///
    /// * `bucket_name` - The name of the bucket to create, must comply with S3 bucket naming
    ///   rules (lowercase, no spaces, valid characters only).
    ///
    /// # Returns
    ///
    /// * `Ok(())` - The test bucket is created successfully via the S3 API.
    /// * `Err(Box<dyn Error + Send + Sync>)` - An error if the S3 `create_bucket` API call fails,
    ///   such as invalid bucket name, insufficient permissions, or an unready cluster.
    pub async fn create_test_bucket(&self, bucket_name: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let client = self.create_s3_client(0)?;
        client.create_bucket().bucket(bucket_name).send().await?;
        info!("Created test bucket: {}", bucket_name);
        Ok(())
    }

    /// Stop all running node processes in the RustFS cluster.
    ///
    /// Iterates over all cluster nodes, attempts to kill the spawned RustFS process (if running),
    /// and waits for the process to exit. Logs an error if process termination or waiting fails,
    /// but does not panic (fails gracefully).
    ///
    /// This method is automatically called by the `Drop` trait when the cluster environment
    /// is destroyed, and can also be called manually to stop the cluster early.
    pub fn stop(&mut self) {
        for (i, node) in self.nodes.iter_mut().enumerate() {
            if let Some(mut process) = node.process.take() {
                info!("Stopping cluster node {}", i);
                if let Err(e) = process.kill() {
                    error!("Failed to kill cluster node {}: {}", i, e);
                }
                if let Err(e) = process.wait() {
                    error!("Failed to wait for cluster node {} to exit: {}", i, e);
                }
            }
        }
    }
}

impl Drop for RustFSTestClusterEnvironment {
    /// Clean up the RustFS test cluster environment when the instance is dropped.
    ///
    /// Automatically calls the `stop` method to terminate all running node processes, then
    /// attempts to delete the cluster's temporary root directory and all its contents.
    /// Logs a warning if directory deletion fails (does not affect program exit).
    fn drop(&mut self) {
        self.stop();
        if let Err(e) = std::fs::remove_dir_all(&self.temp_dir) {
            warn!("Failed to clean up cluster temp directory {}: {}", self.temp_dir, e);
        }
    }
}
