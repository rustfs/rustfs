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

//! Protocol test runner

use crate::common::init_logging;
use crate::protocols::ftps_core::test_ftps_core_operations;
use std::time::Instant;
use tokio::time::{Duration, sleep};
use tracing::{error, info};

/// Test result
#[derive(Debug, Clone)]
pub struct TestResult {
    pub test_name: String,
    pub success: bool,
    pub error_message: Option<String>,
}

impl TestResult {
    pub fn success(test_name: String) -> Self {
        Self {
            test_name,
            success: true,
            error_message: None,
        }
    }

    pub fn failure(test_name: String, error: String) -> Self {
        Self {
            test_name,
            success: false,
            error_message: Some(error),
        }
    }
}

/// Protocol test suite
pub struct ProtocolTestSuite {
    tests: Vec<TestDefinition>,
}

#[derive(Debug, Clone)]
struct TestDefinition {
    name: String,
}

impl ProtocolTestSuite {
    /// Create default test suite
    pub fn new() -> Self {
        let tests = vec![
            TestDefinition {
                name: "test_ftps_core_operations".to_string(),
            },
            // TestDefinition { name: "test_sftp_core_operations".to_string() },
        ];

        Self { tests }
    }

    /// Run test suite
    pub async fn run_test_suite(&self) -> Vec<TestResult> {
        init_logging();
        info!("Starting Protocol test suite");

        let start_time = Instant::now();
        let mut results = Vec::new();

        info!("Scheduled {} tests", self.tests.len());

        // Run tests
        for (i, test_def) in self.tests.iter().enumerate() {
            let test_description = match test_def.name.as_str() {
                "test_ftps_core_operations" => {
                    info!("=== Starting FTPS Module Test ===");
                    "FTPS core operations (put, ls, mkdir, rmdir, delete)"
                }
                "test_sftp_core_operations" => {
                    info!("=== Starting SFTP Module Test ===");
                    "SFTP core operations (put, ls, mkdir, rmdir, delete)"
                }
                _ => "",
            };

            info!("Test {}/{} - {}", i + 1, self.tests.len(), test_description);
            info!("Running: {}", test_def.name);

            let test_start = Instant::now();

            let result = self.run_single_test(test_def).await;
            let test_duration = test_start.elapsed();

            match result {
                Ok(_) => {
                    info!("Test passed: {} ({:.2}s)", test_def.name, test_duration.as_secs_f64());
                    results.push(TestResult::success(test_def.name.clone()));
                }
                Err(e) => {
                    error!("Test failed: {} ({:.2}s): {}", test_def.name, test_duration.as_secs_f64(), e);
                    results.push(TestResult::failure(test_def.name.clone(), e.to_string()));
                }
            }

            // Delay between tests to avoid resource conflicts
            if i < self.tests.len() - 1 {
                sleep(Duration::from_secs(2)).await;
            }
        }

        // Print summary
        self.print_summary(&results, start_time.elapsed());

        results
    }

    /// Run a single test
    async fn run_single_test(&self, test_def: &TestDefinition) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match test_def.name.as_str() {
            "test_ftps_core_operations" => test_ftps_core_operations().await.map_err(|e| e.into()),
            // "test_sftp_core_operations" => test_sftp_core_operations().await.map_err(|e| e.into()),
            _ => Err(format!("Test {} not implemented", test_def.name).into()),
        }
    }

    /// Print test summary
    fn print_summary(&self, results: &[TestResult], total_duration: Duration) {
        info!("=== Test Suite Summary ===");
        info!("Total duration: {:.2}s", total_duration.as_secs_f64());
        info!("Total tests: {}", results.len());

        let passed = results.iter().filter(|r| r.success).count();
        let failed = results.len() - passed;
        let success_rate = (passed as f64 / results.len() as f64) * 100.0;

        info!("Passed: {} | Failed: {}", passed, failed);
        info!("Success rate: {:.1}%", success_rate);

        if failed > 0 {
            error!("Failed tests:");
            for result in results.iter().filter(|r| !r.success) {
                error!("  - {}: {}", result.test_name, result.error_message.as_ref().unwrap());
            }
        }
    }
}

/// Test suite
#[tokio::test]
async fn test_protocol_core_suite() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let suite = ProtocolTestSuite::new();
    let results = suite.run_test_suite().await;

    let failed = results.iter().filter(|r| !r.success).count();
    if failed > 0 {
        return Err(format!("Protocol tests failed: {failed} failures").into());
    }

    info!("All protocol tests passed");
    Ok(())
}
