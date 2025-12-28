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

//! Common test framework for protocol e2e tests

use crate::common::RustFSTestEnvironment;
use std::time::Duration;
use tokio::time::timeout;

pub struct ProtocolTestConfig {
    pub test_timeout: Duration,
    pub large_file_size: usize,
    pub max_retries: u32,
}

impl Default for ProtocolTestConfig {
    fn default() -> Self {
        Self {
            test_timeout: Duration::from_secs(30),
            large_file_size: 10 * 1024 * 1024, // 10MB
            max_retries: 3,
        }
    }
}

pub struct ProtocolTestSuite {
    env: RustFSTestEnvironment,
    config: ProtocolTestConfig,
}

impl ProtocolTestSuite {
    pub fn new(env: RustFSTestEnvironment) -> Self {
        Self {
            env,
            config: ProtocolTestConfig::default(),
        }
    }

    pub fn with_config(mut self, config: ProtocolTestConfig) -> Self {
        self.config = config;
        self
    }

    pub fn env(&self) -> &RustFSTestEnvironment {
        &self.env
    }

    pub fn env_mut(&mut self) -> &mut RustFSTestEnvironment {
        &mut self.env
    }

    /// Test authentication with invalid credentials
    pub async fn test_authentication_errors<T, F, Fut>(&self, test_fn: F) -> anyhow::Result<()>
    where
        F: Fn(&RustFSTestEnvironment) -> Fut,
        Fut: std::future::Future<Output = anyhow::Result<T>>,
    {
        let result = timeout(self.config.test_timeout, test_fn(&self.env)).await;
        match result {
            Ok(Ok(_)) => anyhow::bail!("Expected authentication error but operation succeeded"),
            Ok(Err(e)) => {
                if e.to_string().contains("authentication")
                    || e.to_string().contains("login")
                    || e.to_string().contains("credentials")
                    || e.to_string().contains("permission")
                {
                    Ok(())
                } else {
                    anyhow::bail!("Expected authentication error but got: {}", e);
                }
            }
            Err(_) => anyhow::bail!("Test timed out waiting for authentication error"),
        }
    }

    /// Test permission denied scenarios
    pub async fn test_permission_errors<T, F, Fut>(&self, test_fn: F) -> anyhow::Result<()>
    where
        F: Fn(&RustFSTestEnvironment) -> Fut,
        Fut: std::future::Future<Output = anyhow::Result<T>>,
    {
        let result = timeout(self.config.test_timeout, test_fn(&self.env)).await;
        match result {
            Ok(Ok(_)) => anyhow::bail!("Expected permission error but operation succeeded"),
            Ok(Err(e)) => {
                if e.to_string().contains("permission")
                    || e.to_string().contains("denied")
                    || e.to_string().contains("access")
                    || e.to_string().contains("unauthorized")
                {
                    Ok(())
                } else {
                    anyhow::bail!("Expected permission error but got: {}", e);
                }
            }
            Err(_) => anyhow::bail!("Test timed out waiting for permission error"),
        }
    }

    /// Test file not found scenarios
    pub async fn test_not_found_errors<T, F, Fut>(&self, test_fn: F) -> anyhow::Result<()>
    where
        F: Fn(&RustFSTestEnvironment) -> Fut,
        Fut: std::future::Future<Output = anyhow::Result<T>>,
    {
        let result = timeout(self.config.test_timeout, test_fn(&self.env)).await;
        match result {
            Ok(Ok(_)) => anyhow::bail!("Expected not found error but operation succeeded"),
            Ok(Err(e)) => {
                if e.to_string().contains("not found") ||
                   e.to_string().contains("no such") ||
                   e.to_string().contains("doesn't exist") ||
                   e.to_string().contains("550") || // FTP error code
                   e.to_string().contains("NoSuchFile")
                {
                    // SFTP error
                    Ok(())
                } else {
                    anyhow::bail!("Expected not found error but got: {}", e);
                }
            }
            Err(_) => anyhow::bail!("Test timed out waiting for not found error"),
        }
    }

    /// Test operation with retry logic
    pub async fn test_with_retry<T, F, Fut>(&self, test_fn: F) -> anyhow::Result<T>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = anyhow::Result<T>>,
    {
        let mut last_error = None;

        for attempt in 1..=self.config.max_retries {
            match timeout(self.config.test_timeout, test_fn()).await {
                Ok(Ok(result)) => return Ok(result),
                Ok(Err(e)) => {
                    last_error = Some(e);
                    if attempt < self.config.max_retries {
                        tokio::time::sleep(Duration::from_millis(1000 * attempt as u64)).await;
                    }
                }
                Err(_) => {
                    last_error = Some(anyhow::anyhow!("Test timed out on attempt {}", attempt));
                    if attempt < self.config.max_retries {
                        tokio::time::sleep(Duration::from_millis(1000 * attempt as u64)).await;
                    }
                }
            }
        }

        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("All retry attempts failed")))
    }

    /// Test network failure scenarios
    pub async fn test_network_resilience<T, F, Fut>(&self, test_fn: F) -> anyhow::Result<T>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = anyhow::Result<T>>,
    {
        // For now, this is a simple wrapper around retry logic
        // In the future, we could simulate network failures here
        self.test_with_retry(test_fn).await
    }
}

/// Test data factory for creating various test scenarios
pub struct TestDataFactory;

impl TestDataFactory {
    /// Create test data of specified size
    pub fn create_test_data(size: usize) -> Vec<u8> {
        let mut data = Vec::with_capacity(size);
        for i in 0..size {
            data.push((i % 256) as u8);
        }
        data
    }

    /// Create large test file data
    pub fn create_large_file(size_mb: usize) -> Vec<u8> {
        Self::create_test_data(size_mb * 1024 * 1024)
    }

    /// Create test data with special characters in filename
    pub fn create_special_char_names() -> Vec<String> {
        vec![
            "test file.txt".to_string(),
            "测试文件.txt".to_string(),
            "file with spaces.txt".to_string(),
            "file-with-dashes.txt".to_string(),
            "file_with_underscores.txt".to_string(),
            "file.with.dots.txt".to_string(),
            "file(with)parentheses.txt".to_string(),
            "file[with]brackets.txt".to_string(),
            "file{with}braces.txt".to_string(),
            "file'with'apostrophes.txt".to_string(),
            "file\"with\"quotes.txt".to_string(),
            "file@with@symbols.txt".to_string(),
            "file#with#hash.txt".to_string(),
            "file$with$dollar.txt".to_string(),
            "file%with%percent.txt".to_string(),
            "file^with^caret.txt".to_string(),
            "file&with&ampersand.txt".to_string(),
            "file*with*asterisk.txt".to_string(),
            "file+with+plus.txt".to_string(),
            "file=with=equals.txt".to_string(),
            "file;with;semicolon.txt".to_string(),
            "file:with:colon.txt".to_string(),
            "file<with>angles.txt".to_string(),
            "file?with?question.txt".to_string(),
            "file/with/slash.txt".to_string(),
            "file\\with\\backslash.txt".to_string(),
            "file|with|pipe.txt".to_string(),
        ]
    }

    /// Create deeply nested directory structure
    pub fn create_deep_structure(base: &str, depth: usize) -> Vec<String> {
        let mut paths = Vec::new();
        for i in 1..=depth {
            let mut path = base.to_string();
            for j in 1..=i {
                path.push('/');
                path.push_str(&format!("level{}", j));
            }
            paths.push(format!("{}/file{}.txt", path, i));
        }
        paths
    }

    /// Create test data for concurrent access testing
    pub fn create_concurrent_test_files(count: usize, size: usize) -> Vec<(String, Vec<u8>)> {
        let mut files = Vec::new();
        for i in 0..count {
            let filename = format!("concurrent_file_{:04}.txt", i);
            let data = Self::create_test_data(size);
            files.push((filename, data));
        }
        files
    }
}

/// Utility functions for testing
pub async fn wait_for_port_ready(port: u16, timeout_secs: u64) -> anyhow::Result<()> {
    let timeout_duration = Duration::from_secs(timeout_secs);
    let start = std::time::Instant::now();

    while start.elapsed() < timeout_duration {
        if let Ok(_) = tokio::net::TcpStream::connect(("127.0.0.1", port)).await {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    anyhow::bail!("Port {} not ready after {} seconds", port, timeout_secs)
}

/// Verify file content matches expected
pub fn verify_file_content(actual: &[u8], expected: &[u8]) -> bool {
    actual == expected
}

/// Calculate file checksum for integrity verification
pub fn calculate_checksum(data: &[u8]) -> String {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    data.hash(&mut hasher);
    format!("{:x}", hasher.finish())
}
