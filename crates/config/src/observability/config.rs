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
pub const ENV_OBS_LOCAL_LOGGING_ENABLED: &str = "RUSTFS_OBS_LOCAL_LOGGING_ENABLED";
pub const ENV_OBS_LOG_DIRECTORY: &str = "RUSTFS_OBS_LOG_DIRECTORY";
pub const ENV_OBS_LOG_FILENAME: &str = "RUSTFS_OBS_LOG_FILENAME";
pub const ENV_OBS_LOG_ROTATION_SIZE_MB: &str = "RUSTFS_OBS_LOG_ROTATION_SIZE_MB";
pub const ENV_OBS_LOG_ROTATION_TIME: &str = "RUSTFS_OBS_LOG_ROTATION_TIME";
pub const ENV_OBS_LOG_KEEP_FILES: &str = "RUSTFS_OBS_LOG_KEEP_FILES";

pub const ENV_AUDIT_LOGGER_QUEUE_CAPACITY: &str = "RUSTFS_AUDIT_LOGGER_QUEUE_CAPACITY";

// Default values for observability configuration
pub const DEFAULT_AUDIT_LOGGER_QUEUE_CAPACITY: usize = 10000;
