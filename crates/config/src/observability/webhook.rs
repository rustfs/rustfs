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

// RUSTFS_SINKS_WEBHOOK_ENDPOINT
pub const ENV_SINKS_WEBHOOK_ENDPOINT: &str = "RUSTFS_SINKS_WEBHOOK_ENDPOINT";
// RUSTFS_SINKS_WEBHOOK_AUTH_TOKEN
pub const ENV_SINKS_WEBHOOK_AUTH_TOKEN: &str = "RUSTFS_SINKS_WEBHOOK_AUTH_TOKEN";
// max_retries
pub const ENV_SINKS_WEBHOOK_MAX_RETRIES: &str = "RUSTFS_SINKS_WEBHOOK_MAX_RETRIES";
// retry_delay_ms
pub const ENV_SINKS_WEBHOOK_RETRY_DELAY_MS: &str = "RUSTFS_SINKS_WEBHOOK_RETRY_DELAY_MS";

// Default values for webhook sink configuration
pub const DEFAULT_SINKS_WEBHOOK_ENDPOINT: &str = "http://localhost:8080";
pub const DEFAULT_SINKS_WEBHOOK_AUTH_TOKEN: &str = "";
pub const DEFAULT_SINKS_WEBHOOK_MAX_RETRIES: usize = 3;
pub const DEFAULT_SINKS_WEBHOOK_RETRY_DELAY_MS: u64 = 100;
