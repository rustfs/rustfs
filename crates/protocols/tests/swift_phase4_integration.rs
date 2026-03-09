// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Integration tests for Swift API Phase 4 features
//!
//! These tests validate the integration between different Swift API modules,
//! ensuring they work together correctly.

#[cfg(feature = "swift")]
mod swift_integration {
    use rustfs_protocols::swift::*;
    use std::collections::HashMap;

    #[test]
    fn test_phase4_modules_compile() {
        // This test ensures all Phase 4 modules are properly integrated
        // Actual integration test would require full runtime with storage
    }

    #[test]
    fn test_symlink_with_expiration_metadata() {
        let mut metadata = HashMap::new();
        metadata.insert("x-object-symlink-target".to_string(), "original.txt".to_string());
        metadata.insert("x-delete-at".to_string(), "1740000000".to_string());

        // Both features should coexist in metadata
        assert!(symlink::is_symlink(&metadata));
        let target = symlink::get_symlink_target(&metadata).unwrap();
        assert!(target.is_some());

        let delete_at = metadata.get("x-delete-at").unwrap();
        let parsed = expiration::parse_delete_at(delete_at).unwrap();
        assert_eq!(parsed, 1740000000);
    }

    #[test]
    fn test_multiple_rate_limit_keys() {
        let limiter = ratelimit::RateLimiter::new();
        let rate = ratelimit::RateLimit {
            limit: 3,
            window_seconds: 60,
        };

        // Different keys should have separate limits
        for _ in 0..3 {
            assert!(limiter.check_rate_limit("key1", &rate).is_ok());
            assert!(limiter.check_rate_limit("key2", &rate).is_ok());
        }

        // Both keys should now be exhausted
        assert!(limiter.check_rate_limit("key1", &rate).is_err());
        assert!(limiter.check_rate_limit("key2", &rate).is_err());
    }

    #[test]
    fn test_rate_limit_metadata_extraction() {
        let mut metadata = HashMap::new();
        metadata.insert("x-account-meta-rate-limit".to_string(), "1000/60".to_string());

        let rate_limit = ratelimit::extract_rate_limit(&metadata);
        assert!(rate_limit.is_some());

        let rate_limit = rate_limit.unwrap();
        assert_eq!(rate_limit.limit, 1000);
        assert_eq!(rate_limit.window_seconds, 60);
    }
}
