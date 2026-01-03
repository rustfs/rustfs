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

use moka::future::Cache;
use std::net::IpAddr;
use std::time::Duration;

/// High-performance IP verification cache
pub struct IpValidationCache {
    /// Moka cache
    cache: Cache<IpAddr, bool>,
    /// Default cache expiration time
    default_ttl: Duration,
}

impl IpValidationCache {
    pub fn new(capacity: usize, default_ttl: Duration) -> Self {
        Self {
            cache: Cache::builder()
                .max_capacity(capacity as u64)
                .time_to_live(default_ttl)
                .build(),
            default_ttl,
        }
    }

    /// Check if the IP is trusted (with cache)
    pub async fn is_trusted(&self, ip: &IpAddr, validator: impl FnOnce(&IpAddr) -> bool) -> bool {
        if let Some(is_trusted) = self.cache.get(ip).await {
            return is_trusted;
        }

        // Call the validation function
        let is_trusted = validator(ip);

        // Update the cache
        self.cache.insert(*ip, is_trusted).await;
        is_trusted
    }
}
