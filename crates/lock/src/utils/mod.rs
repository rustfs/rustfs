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

pub mod path;
pub mod uuid;

use std::time::{Duration, SystemTime};

/// Retry strategy
pub struct RetryStrategy {
    max_attempts: usize,
    base_delay: Duration,
    max_delay: Duration,
    backoff_multiplier: f64,
}

impl Default for RetryStrategy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            backoff_multiplier: 2.0,
        }
    }
}

impl RetryStrategy {
    /// Create new retry strategy
    pub fn new(max_attempts: usize, base_delay: Duration) -> Self {
        Self {
            max_attempts,
            base_delay,
            max_delay: Duration::from_secs(30),
            backoff_multiplier: 2.0,
        }
    }

    /// Set maximum delay
    pub fn with_max_delay(mut self, max_delay: Duration) -> Self {
        self.max_delay = max_delay;
        self
    }

    /// Set backoff multiplier
    pub fn with_backoff_multiplier(mut self, multiplier: f64) -> Self {
        self.backoff_multiplier = multiplier;
        self
    }

    /// Calculate delay time for nth retry
    pub fn delay_for_attempt(&self, attempt: usize) -> Duration {
        if attempt == 0 {
            return Duration::ZERO;
        }

        let delay = self.base_delay.mul_f64(self.backoff_multiplier.powi(attempt as i32 - 1));
        delay.min(self.max_delay)
    }

    /// Get maximum retry attempts
    pub fn max_attempts(&self) -> usize {
        self.max_attempts
    }
}

/// Operation executor with retry
pub async fn with_retry<F, Fut, T, E>(strategy: &RetryStrategy, mut operation: F) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: std::fmt::Debug,
{
    let mut last_error = None;

    for attempt in 0..strategy.max_attempts() {
        match operation().await {
            Ok(result) => return Ok(result),
            Err(e) => {
                last_error = Some(e);

                if attempt < strategy.max_attempts() - 1 {
                    let delay = strategy.delay_for_attempt(attempt + 1);
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    Err(last_error.unwrap())
}

/// Timeout wrapper
pub async fn with_timeout<Fut, T>(timeout: Duration, future: Fut) -> Result<T, crate::error::LockError>
where
    Fut: std::future::Future<Output = Result<T, crate::error::LockError>>,
{
    tokio::time::timeout(timeout, future)
        .await
        .map_err(|_| crate::error::LockError::timeout("operation", timeout))?
}

/// Calculate duration between two time points
pub fn duration_between(start: SystemTime, end: SystemTime) -> Duration {
    end.duration_since(start).unwrap_or_default()
}

/// Check if time is expired
pub fn is_expired(expiry_time: SystemTime) -> bool {
    SystemTime::now() >= expiry_time
}

/// Calculate remaining time
pub fn remaining_time(expiry_time: SystemTime) -> Duration {
    expiry_time.duration_since(SystemTime::now()).unwrap_or_default()
}

/// Generate random delay time
pub fn random_delay(base_delay: Duration, jitter_factor: f64) -> Duration {
    use rand::Rng;
    let mut rng = rand::rng();
    let jitter = rng.random_range(-jitter_factor..jitter_factor);
    let multiplier = 1.0 + jitter;
    base_delay.mul_f64(multiplier)
}

/// Calculate hash value
pub fn calculate_hash(data: &[u8]) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    data.hash(&mut hasher);
    hasher.finish()
}

/// Generate resource identifier
pub fn generate_resource_id(prefix: &str, components: &[&str]) -> String {
    let mut id = prefix.to_string();
    for component in components {
        id.push('/');
        id.push_str(component);
    }
    id
}

/// Validate resource path
pub fn validate_resource_path(path: &str) -> bool {
    !path.is_empty() && !path.contains('\0') && path.len() <= 1024
}

/// Normalize resource path
pub fn normalize_resource_path(path: &str) -> String {
    let mut normalized = path.to_string();

    // Remove leading and trailing slashes
    normalized = normalized.trim_matches('/').to_string();

    // Replace multiple consecutive slashes with single slash
    while normalized.contains("//") {
        normalized = normalized.replace("//", "/");
    }

    // If path is empty, return root path
    if normalized.is_empty() {
        normalized = "/".to_string();
    }

    normalized
}

/// Parse resource path components
pub fn parse_resource_components(path: &str) -> Vec<String> {
    let normalized = normalize_resource_path(path);
    if normalized == "/" {
        return vec![];
    }

    normalized
        .split('/')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect()
}

/// Check if path matches pattern
pub fn path_matches_pattern(path: &str, pattern: &str) -> bool {
    let path_components = parse_resource_components(path);
    let pattern_components = parse_resource_components(pattern);

    if pattern_components.is_empty() {
        return path_components.is_empty();
    }

    if path_components.len() != pattern_components.len() {
        return false;
    }

    for (path_comp, pattern_comp) in path_components.iter().zip(pattern_components.iter()) {
        if pattern_comp == "*" {
            continue;
        }
        if path_comp != pattern_comp {
            return false;
        }
    }

    true
}

/// Generate lock key
pub fn generate_lock_key(resource: &str, lock_type: crate::types::LockType) -> String {
    let type_str = match lock_type {
        crate::types::LockType::Exclusive => "exclusive",
        crate::types::LockType::Shared => "shared",
    };

    format!("lock:{type_str}:{resource}")
}

/// Parse lock key
pub fn parse_lock_key(lock_key: &str) -> Option<(crate::types::LockType, String)> {
    let parts: Vec<&str> = lock_key.splitn(3, ':').collect();
    if parts.len() != 3 || parts[0] != "lock" {
        return None;
    }

    let lock_type = match parts[1] {
        "exclusive" => crate::types::LockType::Exclusive,
        "shared" => crate::types::LockType::Shared,
        _ => return None,
    };

    Some((lock_type, parts[2].to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::LockType;

    #[test]
    fn test_retry_strategy() {
        let strategy = RetryStrategy::new(3, Duration::from_millis(100));

        assert_eq!(strategy.max_attempts(), 3);
        assert_eq!(strategy.delay_for_attempt(0), Duration::ZERO);
        assert_eq!(strategy.delay_for_attempt(1), Duration::from_millis(100));
        assert_eq!(strategy.delay_for_attempt(2), Duration::from_millis(200));
    }

    #[test]
    fn test_time_utilities() {
        let now = SystemTime::now();
        let future = now + Duration::from_secs(10);

        assert!(!is_expired(future));
        assert!(remaining_time(future) > Duration::ZERO);

        let past = now - Duration::from_secs(10);
        assert!(is_expired(past));
        assert_eq!(remaining_time(past), Duration::ZERO);
    }

    #[test]
    fn test_resource_path_validation() {
        assert!(validate_resource_path("/valid/path"));
        assert!(validate_resource_path("valid/path"));
        assert!(!validate_resource_path(""));
        assert!(!validate_resource_path("path\0with\0null"));

        let long_path = "a".repeat(1025);
        assert!(!validate_resource_path(&long_path));
    }

    #[test]
    fn test_resource_path_normalization() {
        assert_eq!(normalize_resource_path("/path/to/resource"), "path/to/resource");
        assert_eq!(normalize_resource_path("path//to///resource"), "path/to/resource");
        assert_eq!(normalize_resource_path(""), "/");
        assert_eq!(normalize_resource_path("/"), "/");
    }

    #[test]
    fn test_resource_path_components() {
        assert_eq!(parse_resource_components("/"), vec![] as Vec<String>);
        assert_eq!(parse_resource_components("/path/to/resource"), vec!["path", "to", "resource"]);
        assert_eq!(parse_resource_components("path/to/resource"), vec!["path", "to", "resource"]);
    }

    #[test]
    fn test_path_pattern_matching() {
        assert!(path_matches_pattern("/path/to/resource", "/path/to/resource"));
        assert!(path_matches_pattern("/path/to/resource", "/path/*/resource"));
        assert!(path_matches_pattern("/path/to/resource", "/*/*/*"));
        assert!(!path_matches_pattern("/path/to/resource", "/path/to/other"));
        assert!(!path_matches_pattern("/path/to/resource", "/path/to/resource/extra"));
    }

    #[test]
    fn test_lock_key_generation() {
        let key1 = generate_lock_key("/path/to/resource", LockType::Exclusive);
        assert_eq!(key1, "lock:exclusive:/path/to/resource");

        let key2 = generate_lock_key("/path/to/resource", LockType::Shared);
        assert_eq!(key2, "lock:shared:/path/to/resource");
    }

    #[test]
    fn test_lock_key_parsing() {
        let (lock_type, resource) = parse_lock_key("lock:exclusive:/path/to/resource").unwrap();
        assert_eq!(lock_type, LockType::Exclusive);
        assert_eq!(resource, "/path/to/resource");

        let (lock_type, resource) = parse_lock_key("lock:shared:/path/to/resource").unwrap();
        assert_eq!(lock_type, LockType::Shared);
        assert_eq!(resource, "/path/to/resource");

        assert!(parse_lock_key("invalid:key").is_none());
        assert!(parse_lock_key("lock:invalid:/path").is_none());
    }

    #[tokio::test]
    async fn test_with_retry() {
        let strategy = RetryStrategy::new(3, Duration::from_millis(10));
        let attempts = std::sync::Arc::new(std::sync::Mutex::new(0));

        let result = with_retry(&strategy, {
            let attempts = attempts.clone();
            move || {
                let attempts = attempts.clone();
                async move {
                    let mut count = attempts.lock().unwrap();
                    *count += 1;
                    if *count < 3 { Err("temporary error") } else { Ok("success") }
                }
            }
        })
        .await;

        assert_eq!(result, Ok("success"));
        assert_eq!(*attempts.lock().unwrap(), 3);
    }

    #[tokio::test]
    async fn test_with_timeout() {
        let result = with_timeout(Duration::from_millis(100), async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok::<&str, crate::error::LockError>("success")
        })
        .await;

        assert!(result.is_ok());

        let result = with_timeout(Duration::from_millis(50), async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok::<&str, crate::error::LockError>("success")
        })
        .await;

        assert!(result.is_err());
    }
}
