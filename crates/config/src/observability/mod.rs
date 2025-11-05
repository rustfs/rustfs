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

// Observability Keys

pub const ENV_OBS_ENDPOINT: &str = "RUSTFS_OBS_ENDPOINT";
pub const ENV_OBS_USE_STDOUT: &str = "RUSTFS_OBS_USE_STDOUT";
pub const ENV_OBS_SAMPLE_RATIO: &str = "RUSTFS_OBS_SAMPLE_RATIO";
pub const ENV_OBS_METER_INTERVAL: &str = "RUSTFS_OBS_METER_INTERVAL";
pub const ENV_OBS_SERVICE_NAME: &str = "RUSTFS_OBS_SERVICE_NAME";
pub const ENV_OBS_SERVICE_VERSION: &str = "RUSTFS_OBS_SERVICE_VERSION";
pub const ENV_OBS_ENVIRONMENT: &str = "RUSTFS_OBS_ENVIRONMENT";
pub const ENV_OBS_LOGGER_LEVEL: &str = "RUSTFS_OBS_LOGGER_LEVEL";
pub const ENV_OBS_LOG_STDOUT_ENABLED: &str = "RUSTFS_OBS_LOG_STDOUT_ENABLED";
pub const ENV_OBS_LOG_DIRECTORY: &str = "RUSTFS_OBS_LOG_DIRECTORY";
pub const ENV_OBS_LOG_FILENAME: &str = "RUSTFS_OBS_LOG_FILENAME";
pub const ENV_OBS_LOG_ROTATION_SIZE_MB: &str = "RUSTFS_OBS_LOG_ROTATION_SIZE_MB";
pub const ENV_OBS_LOG_ROTATION_TIME: &str = "RUSTFS_OBS_LOG_ROTATION_TIME";
pub const ENV_OBS_LOG_KEEP_FILES: &str = "RUSTFS_OBS_LOG_KEEP_FILES";

/// Log pool capacity for async logging
pub const ENV_OBS_LOG_POOL_CAPA: &str = "RUSTFS_OBS_LOG_POOL_CAPA";

/// Log message capacity for async logging
pub const ENV_OBS_LOG_MESSAGE_CAPA: &str = "RUSTFS_OBS_LOG_MESSAGE_CAPA";

/// Log flush interval in milliseconds for async logging
pub const ENV_OBS_LOG_FLUSH_MS: &str = "RUSTFS_OBS_LOG_FLUSH_MS";

/// Default values for log pool
pub const DEFAULT_OBS_LOG_POOL_CAPA: usize = 10240;

/// Default values for message capacity
pub const DEFAULT_OBS_LOG_MESSAGE_CAPA: usize = 32768;

/// Default values for flush interval in milliseconds
pub const DEFAULT_OBS_LOG_FLUSH_MS: u64 = 200;

/// Default values for observability configuration
// ### Supported Environment Values
// - `production` - Secure file-only logging
// - `development` - Full debugging with stdout
// - `test` - Test environment with stdout support
// - `staging` - Staging environment with stdout support
pub const DEFAULT_OBS_ENVIRONMENT_PRODUCTION: &str = "production";
pub const DEFAULT_OBS_ENVIRONMENT_DEVELOPMENT: &str = "development";
pub const DEFAULT_OBS_ENVIRONMENT_TEST: &str = "test";
pub const DEFAULT_OBS_ENVIRONMENT_STAGING: &str = "staging";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_env_keys() {
        assert_eq!(ENV_OBS_ENDPOINT, "RUSTFS_OBS_ENDPOINT");
        assert_eq!(ENV_OBS_USE_STDOUT, "RUSTFS_OBS_USE_STDOUT");
        assert_eq!(ENV_OBS_SAMPLE_RATIO, "RUSTFS_OBS_SAMPLE_RATIO");
        assert_eq!(ENV_OBS_METER_INTERVAL, "RUSTFS_OBS_METER_INTERVAL");
        assert_eq!(ENV_OBS_SERVICE_NAME, "RUSTFS_OBS_SERVICE_NAME");
        assert_eq!(ENV_OBS_SERVICE_VERSION, "RUSTFS_OBS_SERVICE_VERSION");
        assert_eq!(ENV_OBS_ENVIRONMENT, "RUSTFS_OBS_ENVIRONMENT");
        assert_eq!(ENV_OBS_LOGGER_LEVEL, "RUSTFS_OBS_LOGGER_LEVEL");
        assert_eq!(ENV_OBS_LOG_STDOUT_ENABLED, "RUSTFS_OBS_LOG_STDOUT_ENABLED");
        assert_eq!(ENV_OBS_LOG_DIRECTORY, "RUSTFS_OBS_LOG_DIRECTORY");
        assert_eq!(ENV_OBS_LOG_FILENAME, "RUSTFS_OBS_LOG_FILENAME");
        assert_eq!(ENV_OBS_LOG_ROTATION_SIZE_MB, "RUSTFS_OBS_LOG_ROTATION_SIZE_MB");
        assert_eq!(ENV_OBS_LOG_ROTATION_TIME, "RUSTFS_OBS_LOG_ROTATION_TIME");
        assert_eq!(ENV_OBS_LOG_KEEP_FILES, "RUSTFS_OBS_LOG_KEEP_FILES");
    }

    #[test]
    fn test_default_values() {
        assert_eq!(DEFAULT_OBS_ENVIRONMENT_PRODUCTION, "production");
        assert_eq!(DEFAULT_OBS_ENVIRONMENT_DEVELOPMENT, "development");
        assert_eq!(DEFAULT_OBS_ENVIRONMENT_TEST, "test");
        assert_eq!(DEFAULT_OBS_ENVIRONMENT_STAGING, "staging");
    }
}
