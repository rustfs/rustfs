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
use crate::common::{requested_rustfs_build_features, rustfs_build_feature_enabled};
use crate::protocols::ftps_core::test_ftps_core_operations;
use crate::protocols::sftp_compliance::{
    test_sftp_compliance_readonly, test_sftp_compliance_standalone, test_sftp_compliance_suite,
};
use crate::protocols::sftp_core::{test_sftp_core_operations, test_sftp_idle_timeout_disconnects};
use crate::protocols::webdav_core::test_webdav_core_operations;
use serial_test::serial;
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
    name: &'static str,
    required_feature: &'static str,
}

impl ProtocolTestSuite {
    /// Create default test suite
    pub fn new() -> Self {
        let requested_features = requested_rustfs_build_features();
        Self::with_requested_features(requested_features.as_deref())
    }

    fn with_requested_features(requested_features: Option<&str>) -> Self {
        let tests = all_protocol_tests()
            .into_iter()
            .filter(|test| rustfs_build_feature_enabled(requested_features, test.required_feature))
            .collect();
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
            let test_description = match test_def.name {
                "test_ftps_core_operations" => {
                    info!("=== Starting FTPS Module Test ===");
                    "FTPS core operations (put, ls, mkdir, rmdir, delete)"
                }
                "test_webdav_core_operations" => {
                    info!("=== Starting WebDAV Core Test ===");
                    "WebDAV core operations (MKCOL, PUT, GET, DELETE, PROPFIND)"
                }
                "test_sftp_core_operations" => {
                    info!("=== Starting SFTP Core Test ===");
                    "SFTP core operations (banner, mkdir, put, get with SHA compare, rename, delete, rmdir)"
                }
                "test_sftp_compliance_suite" => {
                    info!("=== Starting SFTP Compliance Suite ===");
                    "SFTP compliance regression suite (zero-byte, mutation rejection, traversal, rename, implicit dirs, FSETSTAT)"
                }
                "test_sftp_compliance_readonly" => {
                    info!("=== Starting SFTP Read-Only Compliance Suite ===");
                    "SFTP read-only mode (RUSTFS_SFTP_READ_ONLY=true rejects mutations and allows reads)"
                }
                "test_sftp_idle_timeout_disconnects" => {
                    info!("=== Starting SFTP Idle-Timeout Test ===");
                    "SFTP idle-timeout disconnects (server closes the session past RUSTFS_SFTP_IDLE_TIMEOUT)"
                }
                "test_sftp_compliance_standalone" => {
                    info!("=== Starting SFTP Standalone-Server Compliance Suite ===");
                    "SFTP standalone-server compliance suite"
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
                    results.push(TestResult::success(test_def.name.to_string()));
                }
                Err(e) => {
                    error!("Test failed: {} ({:.2}s): {}", test_def.name, test_duration.as_secs_f64(), e);
                    results.push(TestResult::failure(test_def.name.to_string(), e.to_string()));
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
        match test_def.name {
            "test_ftps_core_operations" => test_ftps_core_operations().await.map_err(|e| e.into()),
            "test_webdav_core_operations" => test_webdav_core_operations().await.map_err(|e| e.into()),
            "test_sftp_core_operations" => test_sftp_core_operations().await.map_err(|e| e.into()),
            "test_sftp_compliance_suite" => test_sftp_compliance_suite().await.map_err(|e| e.into()),
            "test_sftp_compliance_readonly" => test_sftp_compliance_readonly().await.map_err(|e| e.into()),
            "test_sftp_idle_timeout_disconnects" => test_sftp_idle_timeout_disconnects().await.map_err(|e| e.into()),
            "test_sftp_compliance_standalone" => test_sftp_compliance_standalone().await.map_err(|e| e.into()),
            _ => Err(format!("Test {} not implemented", test_def.name).into()),
        }
    }

    /// Print test summary
    fn print_summary(&self, results: &[TestResult], total_duration: Duration) {
        info!("=== Test Suite Summary ===");
        info!("Total duration: {:.2}s", total_duration.as_secs_f64());
        info!("Total tests: {}", results.len());
        if results.is_empty() {
            info!("No protocol tests scheduled for the requested feature set");
            return;
        }

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

fn all_protocol_tests() -> Vec<TestDefinition> {
    vec![
        TestDefinition {
            name: "test_ftps_core_operations",
            required_feature: "ftps",
        },
        TestDefinition {
            name: "test_webdav_core_operations",
            required_feature: "webdav",
        },
        TestDefinition {
            name: "test_sftp_core_operations",
            required_feature: "sftp",
        },
        TestDefinition {
            name: "test_sftp_compliance_suite",
            required_feature: "sftp",
        },
        TestDefinition {
            name: "test_sftp_compliance_readonly",
            required_feature: "sftp",
        },
        TestDefinition {
            name: "test_sftp_idle_timeout_disconnects",
            required_feature: "sftp",
        },
        TestDefinition {
            name: "test_sftp_compliance_standalone",
            required_feature: "sftp",
        },
    ]
}

/// Test suite
#[tokio::test]
#[serial]
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

#[cfg(test)]
mod tests {
    use super::*;

    fn scheduled_names(suite: ProtocolTestSuite) -> Vec<&'static str> {
        suite.tests.into_iter().map(|test| test.name).collect()
    }

    #[test]
    fn schedules_all_protocol_tests_without_feature_filter() {
        let names = scheduled_names(ProtocolTestSuite::with_requested_features(None));

        assert_eq!(names.len(), 7);
        assert!(names.contains(&"test_ftps_core_operations"));
        assert!(names.contains(&"test_webdav_core_operations"));
        assert!(names.contains(&"test_sftp_core_operations"));
        assert!(names.contains(&"test_sftp_compliance_standalone"));
    }

    #[test]
    fn schedules_only_requested_non_sftp_protocols() {
        let names = scheduled_names(ProtocolTestSuite::with_requested_features(Some("ftps, webdav")));

        assert_eq!(names, vec!["test_ftps_core_operations", "test_webdav_core_operations"]);
    }

    #[test]
    fn schedules_all_sftp_entries_for_sftp_feature() {
        let names = scheduled_names(ProtocolTestSuite::with_requested_features(Some("sftp")));

        assert_eq!(
            names,
            vec![
                "test_sftp_core_operations",
                "test_sftp_compliance_suite",
                "test_sftp_compliance_readonly",
                "test_sftp_idle_timeout_disconnects",
                "test_sftp_compliance_standalone",
            ]
        );
    }

    #[test]
    fn feature_filter_is_case_insensitive() {
        let names = scheduled_names(ProtocolTestSuite::with_requested_features(Some("SFTP")));

        assert_eq!(names.len(), 5);
        assert!(names.iter().all(|name| name.contains("sftp")));
    }

    #[test]
    fn full_feature_schedules_all_protocol_tests() {
        let names = scheduled_names(ProtocolTestSuite::with_requested_features(Some("full")));

        assert_eq!(names.len(), 7);
        assert!(names.contains(&"test_ftps_core_operations"));
        assert!(names.contains(&"test_webdav_core_operations"));
        assert!(names.contains(&"test_sftp_core_operations"));
        assert!(names.contains(&"test_sftp_compliance_standalone"));
    }

    #[test]
    fn schedules_no_tests_when_requested_features_have_no_protocols() {
        let names = scheduled_names(ProtocolTestSuite::with_requested_features(Some("diagnostics")));

        assert!(names.is_empty());
    }
}
