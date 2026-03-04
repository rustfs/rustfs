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

//! Swift URL routing and parsing

use axum::http::{Method, Uri};
use regex::Regex;
use std::sync::LazyLock;

/// Decode percent-encoded URL segment
fn decode_url_segment(segment: &str) -> String {
    percent_encoding::percent_decode_str(segment).decode_utf8_lossy().into_owned()
}

/// Regex pattern for Swift account URLs: /v1/AUTH_{project_id}
/// Accepts any non-empty alphanumeric string with hyphens and underscores
static ACCOUNT_PATTERN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^AUTH_([a-zA-Z0-9_-]+)$").expect("ACCOUNT_PATTERN regex is hardcoded and must be valid"));

/// Represents a parsed Swift route
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SwiftRoute {
    /// Account operation: /v1/{account}
    Account { account: String, method: Method },
    /// Container operation: /v1/{account}/{container}
    Container {
        account: String,
        container: String,
        method: Method,
    },
    /// Object operation: /v1/{account}/{container}/{object}
    Object {
        account: String,
        container: String,
        object: String,
        method: Method,
    },
}

impl SwiftRoute {
    /// Get the account identifier from the route
    pub fn account(&self) -> &str {
        match self {
            SwiftRoute::Account { account, .. } => account,
            SwiftRoute::Container { account, .. } => account,
            SwiftRoute::Object { account, .. } => account,
        }
    }

    /// Extract project_id from account string (removes AUTH_ prefix)
    pub fn project_id(&self) -> Option<&str> {
        let account = self.account();
        ACCOUNT_PATTERN
            .captures(account)
            .and_then(|caps| caps.get(1).map(|m| m.as_str()))
    }
}

/// Swift URL router
#[derive(Debug, Clone)]
pub struct SwiftRouter {
    /// Enable Swift API
    enabled: bool,
    /// Optional URL prefix (e.g., "swift" for /swift/v1/... URLs)
    url_prefix: Option<String>,
}

impl SwiftRouter {
    /// Create a new Swift router
    pub fn new(enabled: bool, url_prefix: Option<String>) -> Self {
        Self { enabled, url_prefix }
    }

    /// Parse a URI and return a SwiftRoute if it matches Swift URL pattern
    pub fn route(&self, uri: &Uri, method: Method) -> Option<SwiftRoute> {
        if !self.enabled {
            return None;
        }

        let path = uri.path();

        // Strip optional prefix
        let path = if let Some(prefix) = &self.url_prefix {
            path.strip_prefix(&format!("/{}/", prefix))?
        } else {
            path
        };

        // Split path into segments
        let segments: Vec<&str> = path.trim_start_matches('/').split('/').filter(|s| !s.is_empty()).collect();

        // Swift URLs must start with "v1"
        if segments.first() != Some(&"v1") {
            return None;
        }

        // Match path segments
        match segments.as_slice() {
            // /v1/{account}
            ["v1", account] => {
                if !Self::is_valid_account(account) {
                    return None;
                }
                Some(SwiftRoute::Account {
                    account: decode_url_segment(account),
                    method,
                })
            }
            // /v1/{account}/{container}
            ["v1", account, container] => {
                if !Self::is_valid_account(account) {
                    return None;
                }
                Some(SwiftRoute::Container {
                    account: decode_url_segment(account),
                    container: decode_url_segment(container),
                    method,
                })
            }
            // /v1/{account}/{container}/{object...}
            ["v1", account, container, object @ ..] if !object.is_empty() => {
                if !Self::is_valid_account(account) {
                    return None;
                }
                // Join remaining segments as object key, decoding each segment
                let object_key = object.iter().map(|s| decode_url_segment(s)).collect::<Vec<_>>().join("/");
                Some(SwiftRoute::Object {
                    account: decode_url_segment(account),
                    container: decode_url_segment(container),
                    object: object_key,
                    method,
                })
            }
            _ => None,
        }
    }

    /// Validate account format (must be AUTH_{uuid})
    fn is_valid_account(account: &str) -> bool {
        ACCOUNT_PATTERN.is_match(account)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_account_pattern() {
        // Valid UUID-style project IDs
        assert!(ACCOUNT_PATTERN.is_match("AUTH_7188e165c0ae4424ac68ae2e89a05c50"));
        assert!(ACCOUNT_PATTERN.is_match("AUTH_550e8400-e29b-41d4-a716-446655440000"));

        // Valid alphanumeric project IDs (non-UUID)
        assert!(ACCOUNT_PATTERN.is_match("AUTH_project123"));
        assert!(ACCOUNT_PATTERN.is_match("AUTH_my-project_01"));

        // Invalid patterns
        assert!(!ACCOUNT_PATTERN.is_match("AUTH_")); // Empty project ID
        assert!(!ACCOUNT_PATTERN.is_match("7188e165c0ae4424ac68ae2e89a05c50")); // Missing AUTH_ prefix
        assert!(!ACCOUNT_PATTERN.is_match("AUTH_project with spaces")); // Spaces not allowed
    }

    #[test]
    fn test_route_account() {
        let router = SwiftRouter::new(true, None);
        let uri = "/v1/AUTH_7188e165c0ae4424ac68ae2e89a05c50".parse().unwrap();
        let route = router.route(&uri, Method::GET);

        assert_eq!(
            route,
            Some(SwiftRoute::Account {
                account: "AUTH_7188e165c0ae4424ac68ae2e89a05c50".to_string(),
                method: Method::GET
            })
        );
    }

    #[test]
    fn test_route_container() {
        let router = SwiftRouter::new(true, None);
        let uri = "/v1/AUTH_7188e165c0ae4424ac68ae2e89a05c50/photos".parse().unwrap();
        let route = router.route(&uri, Method::PUT);

        assert_eq!(
            route,
            Some(SwiftRoute::Container {
                account: "AUTH_7188e165c0ae4424ac68ae2e89a05c50".to_string(),
                container: "photos".to_string(),
                method: Method::PUT
            })
        );
    }

    #[test]
    fn test_route_object() {
        let router = SwiftRouter::new(true, None);
        let uri = "/v1/AUTH_7188e165c0ae4424ac68ae2e89a05c50/photos/vacation/beach.jpg"
            .parse()
            .unwrap();
        let route = router.route(&uri, Method::GET);

        assert_eq!(
            route,
            Some(SwiftRoute::Object {
                account: "AUTH_7188e165c0ae4424ac68ae2e89a05c50".to_string(),
                container: "photos".to_string(),
                object: "vacation/beach.jpg".to_string(),
                method: Method::GET
            })
        );
    }

    #[test]
    fn test_route_with_prefix() {
        let router = SwiftRouter::new(true, Some("swift".to_string()));
        let uri = "/swift/v1/AUTH_7188e165c0ae4424ac68ae2e89a05c50/photos".parse().unwrap();
        let route = router.route(&uri, Method::GET);

        assert_eq!(
            route,
            Some(SwiftRoute::Container {
                account: "AUTH_7188e165c0ae4424ac68ae2e89a05c50".to_string(),
                container: "photos".to_string(),
                method: Method::GET
            })
        );
    }

    #[test]
    fn test_route_invalid_account() {
        let router = SwiftRouter::new(true, None);
        let uri = "/v1/invalid_account/photos".parse().unwrap();
        let route = router.route(&uri, Method::GET);

        assert_eq!(route, None);
    }

    #[test]
    fn test_route_disabled() {
        let router = SwiftRouter::new(false, None);
        let uri = "/v1/AUTH_7188e165c0ae4424ac68ae2e89a05c50".parse().unwrap();
        let route = router.route(&uri, Method::GET);

        assert_eq!(route, None);
    }

    #[test]
    fn test_project_id_extraction() {
        let route = SwiftRoute::Account {
            account: "AUTH_7188e165c0ae4424ac68ae2e89a05c50".to_string(),
            method: Method::GET,
        };

        assert_eq!(route.project_id(), Some("7188e165c0ae4424ac68ae2e89a05c50"));
    }
}
