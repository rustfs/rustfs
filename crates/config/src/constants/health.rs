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

/// Enable or disable public `/health` and `/health/ready` endpoints.
/// When disabled, the routes are not registered and return 404.
pub const ENV_HEALTH_ENDPOINT_ENABLE: &str = "RUSTFS_HEALTH_ENDPOINT_ENABLE";
pub const DEFAULT_HEALTH_ENDPOINT_ENABLE: bool = true;

/// Cache TTL for storage readiness runtime-state evaluation (milliseconds).
/// This reduces storage-layer pressure when probes are called at high frequency.
pub const ENV_HEALTH_READINESS_CACHE_TTL_MS: &str = "RUSTFS_HEALTH_READINESS_CACHE_TTL_MS";
pub const DEFAULT_HEALTH_READINESS_CACHE_TTL_MS: u64 = 1000;

/// Enable minimal health payload mode for GET `/health*` responses.
/// When enabled, only `status` and `ready` fields are returned.
pub const ENV_HEALTH_MINIMAL_RESPONSE_ENABLE: &str = "RUSTFS_HEALTH_MINIMAL_RESPONSE_ENABLE";
pub const DEFAULT_HEALTH_MINIMAL_RESPONSE_ENABLE: bool = false;
