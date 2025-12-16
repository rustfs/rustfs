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

use crate::common::init_logging;
use crate::policy::test_env::PolicyTestEnvironment;
use serial_test::serial;
use std::time::Instant;
use tokio::time::{Duration, sleep};
use tracing::{error, info};

/// Core test categories
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TestCategory {
    SingleValue,
    MultiValue,
    Concatenation,
    Nested,
    DenyScenarios,
}

impl TestCategory {}

/// Test case definition
#[derive(Debug, Clone)]
pub struct TestDefinition {
    pub name: String,
    #[allow(dead_code)]
    pub category: TestCategory,
    pub is_critical: bool,
}

impl TestDefinition {
    pub fn new(name: impl Into<String>, category: TestCategory, is_critical: bool) -> Self {
        Self {
            name: name.into(),
            category,
            is_critical,
        }
    }
}

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

/// Test suite configuration
#[derive(Debug, Clone, Default)]
pub struct TestSuiteConfig {
    pub include_critical_only: bool,
}

/// Policy test suite
pub struct PolicyTestSuite {
    tests: Vec<TestDefinition>,
    config: TestSuiteConfig,
}

impl PolicyTestSuite {
    /// Create default test suite
    pub fn new() -> Self {
        let tests = vec![
            TestDefinition::new("test_aws_policy_variables_single_value", TestCategory::SingleValue, true),
            TestDefinition::new("test_aws_policy_variables_multi_value", TestCategory::MultiValue, true),
            TestDefinition::new("test_aws_policy_variables_concatenation", TestCategory::Concatenation, true),
            TestDefinition::new("test_aws_policy_variables_nested", TestCategory::Nested, true),
            TestDefinition::new("test_aws_policy_variables_deny", TestCategory::DenyScenarios, true),
            TestDefinition::new("test_aws_policy_variables_sts", TestCategory::SingleValue, true),
        ];

        Self {
            tests,
            config: TestSuiteConfig::default(),
        }
    }

    /// Configure test suite
    pub fn with_config(mut self, config: TestSuiteConfig) -> Self {
        self.config = config;
        self
    }

    /// Run test suite
    pub async fn run_test_suite(&self) -> Vec<TestResult> {
        init_logging();
        info!("Starting Policy Variables test suite");

        let start_time = Instant::now();
        let mut results = Vec::new();

        // Create test environment
        let env = match PolicyTestEnvironment::with_address("127.0.0.1:9000").await {
            Ok(env) => env,
            Err(e) => {
                error!("Failed to create test environment: {}", e);
                return vec![TestResult::failure("env_creation".into(), e.to_string())];
            }
        };

        // Wait for server to be ready
        if env.wait_for_server_ready().await.is_err() {
            error!("Server is not ready");
            return vec![TestResult::failure("server_check".into(), "Server not ready".into())];
        }

        // Filter tests
        let tests_to_run: Vec<&TestDefinition> = self
            .tests
            .iter()
            .filter(|test| !self.config.include_critical_only || test.is_critical)
            .collect();

        info!("Scheduled {} tests", tests_to_run.len());

        // Run tests
        for (i, test_def) in tests_to_run.iter().enumerate() {
            info!("Running test {}/{}: {}", i + 1, tests_to_run.len(), test_def.name);
            let test_start = Instant::now();

            let result = self.run_single_test(test_def, &env).await;
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
            if i < tests_to_run.len() - 1 {
                sleep(Duration::from_secs(2)).await;
            }
        }

        // Print summary
        self.print_summary(&results, start_time.elapsed());

        results
    }

    /// Run a single test
    async fn run_single_test(
        &self,
        test_def: &TestDefinition,
        env: &PolicyTestEnvironment,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match test_def.name.as_str() {
            "test_aws_policy_variables_single_value" => {
                super::policy_variables_test::test_aws_policy_variables_single_value_impl_with_env(env).await
            }
            "test_aws_policy_variables_multi_value" => {
                super::policy_variables_test::test_aws_policy_variables_multi_value_impl_with_env(env).await
            }
            "test_aws_policy_variables_concatenation" => {
                super::policy_variables_test::test_aws_policy_variables_concatenation_impl_with_env(env).await
            }
            "test_aws_policy_variables_nested" => {
                super::policy_variables_test::test_aws_policy_variables_nested_impl_with_env(env).await
            }
            "test_aws_policy_variables_deny" => {
                super::policy_variables_test::test_aws_policy_variables_deny_impl_with_env(env).await
            }
            "test_aws_policy_variables_sts" => {
                super::policy_variables_test::test_aws_policy_variables_sts_impl_with_env(env).await
            }
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
#[serial]
#[ignore = "Connects to existing rustfs server"]
async fn test_policy_critical_suite() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config = TestSuiteConfig {
        include_critical_only: true,
    };
    let suite = PolicyTestSuite::new().with_config(config);
    let results = suite.run_test_suite().await;

    let failed = results.iter().filter(|r| !r.success).count();
    if failed > 0 {
        return Err(format!("Critical tests failed: {failed} failures").into());
    }

    info!("All critical tests passed");
    Ok(())
}
