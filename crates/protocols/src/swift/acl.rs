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

//! Access Control List (ACL) Support for Swift API
//!
//! Implements Swift container ACLs for fine-grained access control.
//!
//! # ACL Types
//!
//! ## Read ACLs (X-Container-Read)
//!
//! - **Public read**: `.r:*` - Anyone can read
//! - **Referrer restriction**: `.r:*.example.com` - Only specific referrers
//! - **Account access**: `AUTH_project123` - Specific account
//! - **User access**: `AUTH_project123:user1` - Specific user in account
//!
//! ## Write ACLs (X-Container-Write)
//!
//! - **Account access**: `AUTH_project123` - Specific account can write
//! - **User access**: `AUTH_project123:user1` - Specific user can write
//! - **No public write** - Public write is not supported for security
//!
//! # Examples
//!
//! ```text
//! # Public read container
//! X-Container-Read: .r:*
//!
//! # Referrer-restricted public read
//! X-Container-Read: .r:*.example.com,.r:*.cdn.com
//!
//! # Specific accounts can read
//! X-Container-Read: AUTH_abc123,AUTH_def456
//!
//! # Mixed ACL
//! X-Container-Read: .r:*.example.com,AUTH_abc123,AUTH_def456:user1
//!
//! # Write access
//! X-Container-Write: AUTH_abc123,AUTH_def456:user1
//! ```
//!
//! # Storage
//!
//! ACLs are stored in S3 bucket tags:
//! - Tag key: `swift-acl-read` with comma-separated grants
//! - Tag key: `swift-acl-write` with comma-separated grants

use super::{SwiftError, SwiftResult};
use std::fmt;
use tracing::debug;

/// Container ACL configuration
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ContainerAcl {
    /// Grants allowing read access (GET, HEAD)
    pub read: Vec<AclGrant>,

    /// Grants allowing write access (PUT, POST, DELETE)
    pub write: Vec<AclGrant>,
}

/// ACL grant entry
#[derive(Debug, Clone, PartialEq)]
pub enum AclGrant {
    /// Public read access (.r:*)
    /// Allows anyone to read without authentication
    PublicRead,

    /// Public read with referrer restriction (.r:*.example.com)
    /// Allows read only if HTTP Referer header matches pattern
    PublicReadReferrer(String),

    /// Specific account access (AUTH_project_id)
    /// Allows all users in the account
    Account(String),

    /// Specific user access (AUTH_project_id:user_id)
    /// Allows only the specific user in the account
    User { account: String, user: String },
}

impl fmt::Display for AclGrant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AclGrant::PublicRead => write!(f, ".r:*"),
            AclGrant::PublicReadReferrer(pattern) => write!(f, ".r:{}", pattern),
            AclGrant::Account(account) => write!(f, "{}", account),
            AclGrant::User { account, user } => write!(f, "{}:{}", account, user),
        }
    }
}

impl ContainerAcl {
    /// Create a new empty ACL
    pub fn new() -> Self {
        Self::default()
    }

    /// Parse read ACL from header value
    ///
    /// Format: comma-separated list of grants
    /// - `.r:*` = public read
    /// - `.r:*.example.com` = referrer restriction
    /// - `AUTH_abc123` = account access
    /// - `AUTH_abc123:user1` = user access
    ///
    /// # Example
    /// ```ignore
    /// let acl = ContainerAcl::parse_read(".r:*,AUTH_abc123")?;
    /// ```
    pub fn parse_read(header: &str) -> SwiftResult<Vec<AclGrant>> {
        let header = header.trim();
        if header.is_empty() {
            return Ok(Vec::new());
        }

        let mut grants = Vec::new();

        for grant_str in header.split(',') {
            let grant_str = grant_str.trim();
            if grant_str.is_empty() {
                continue;
            }

            let grant = Self::parse_grant(grant_str, true)?;
            grants.push(grant);
        }

        Ok(grants)
    }

    /// Parse write ACL from header value
    ///
    /// Format: comma-separated list of grants
    /// - `AUTH_abc123` = account access
    /// - `AUTH_abc123:user1` = user access
    /// - Public write is NOT allowed
    ///
    /// # Example
    /// ```ignore
    /// let acl = ContainerAcl::parse_write("AUTH_abc123,AUTH_def456:user1")?;
    /// ```
    pub fn parse_write(header: &str) -> SwiftResult<Vec<AclGrant>> {
        let header = header.trim();
        if header.is_empty() {
            return Ok(Vec::new());
        }

        let mut grants = Vec::new();

        for grant_str in header.split(',') {
            let grant_str = grant_str.trim();
            if grant_str.is_empty() {
                continue;
            }

            let grant = Self::parse_grant(grant_str, false)?;
            grants.push(grant);
        }

        Ok(grants)
    }

    /// Parse a single ACL grant string
    fn parse_grant(grant_str: &str, allow_public: bool) -> SwiftResult<AclGrant> {
        // Check for public read patterns (.r:*)
        if let Some(pattern) = grant_str.strip_prefix(".r:") {
            if !allow_public {
                return Err(SwiftError::BadRequest("Public access not allowed in write ACL".to_string()));
            }

            // Skip ".r:"
            if pattern == "*" {
                Ok(AclGrant::PublicRead)
            } else if !pattern.is_empty() {
                Ok(AclGrant::PublicReadReferrer(pattern.to_string()))
            } else {
                Err(SwiftError::BadRequest("Invalid referrer pattern".to_string()))
            }
        }
        // Check for account or user pattern (AUTH_*)
        else if grant_str.starts_with("AUTH_") {
            if let Some(colon_pos) = grant_str.find(':') {
                // User-specific: AUTH_project:user
                let account = grant_str[..colon_pos].to_string();
                let user = grant_str[colon_pos + 1..].to_string();

                if user.is_empty() {
                    return Err(SwiftError::BadRequest("Empty user ID in ACL".to_string()));
                }

                Ok(AclGrant::User { account, user })
            } else {
                // Account-level: AUTH_project
                Ok(AclGrant::Account(grant_str.to_string()))
            }
        } else {
            Err(SwiftError::BadRequest(format!("Invalid ACL grant format: {}", grant_str)))
        }
    }

    /// Check if a request has read access based on this ACL
    ///
    /// # Arguments
    /// * `request_account` - The account making the request (if authenticated)
    /// * `request_user` - The user making the request (if known)
    /// * `referrer` - The HTTP Referer header value (if present)
    ///
    /// # Returns
    /// `true` if access is allowed, `false` otherwise
    pub fn check_read_access(&self, request_account: Option<&str>, request_user: Option<&str>, referrer: Option<&str>) -> bool {
        if self.read.is_empty() {
            // No read ACL means default behavior: owner can read
            return request_account.is_some();
        }

        for grant in &self.read {
            match grant {
                AclGrant::PublicRead => {
                    debug!("Read access granted: public read enabled");
                    return true;
                }
                AclGrant::PublicReadReferrer(pattern) => {
                    if let Some(ref_header) = referrer
                        && Self::matches_referrer_pattern(ref_header, pattern)
                    {
                        debug!("Read access granted: referrer matches pattern {}", pattern);
                        return true;
                    }
                }
                AclGrant::Account(account) => {
                    if let Some(req_account) = request_account
                        && req_account == account
                    {
                        debug!("Read access granted: account {} matches", account);
                        return true;
                    }
                }
                AclGrant::User {
                    account,
                    user: grant_user,
                } => {
                    if let (Some(req_account), Some(req_user)) = (request_account, request_user)
                        && req_account == account
                        && req_user == grant_user
                    {
                        debug!("Read access granted: user {}:{} matches", account, grant_user);
                        return true;
                    }
                }
            }
        }

        debug!("Read access denied: no matching ACL grant");
        false
    }

    /// Check if a request has write access based on this ACL
    ///
    /// # Arguments
    /// * `request_account` - The account making the request
    /// * `request_user` - The user making the request (if known)
    ///
    /// # Returns
    /// `true` if access is allowed, `false` otherwise
    pub fn check_write_access(&self, request_account: &str, request_user: Option<&str>) -> bool {
        if self.write.is_empty() {
            // No write ACL means default behavior: owner can write
            return true;
        }

        for grant in &self.write {
            match grant {
                AclGrant::PublicRead | AclGrant::PublicReadReferrer(_) => {
                    // These should never appear in write ACL (validated during parse)
                    continue;
                }
                AclGrant::Account(account) => {
                    if request_account == account {
                        debug!("Write access granted: account {} matches", account);
                        return true;
                    }
                }
                AclGrant::User {
                    account,
                    user: grant_user,
                } => {
                    if let Some(req_user) = request_user
                        && request_account == account
                        && req_user == grant_user
                    {
                        debug!("Write access granted: user {}:{} matches", account, grant_user);
                        return true;
                    }
                }
            }
        }

        debug!("Write access denied: no matching ACL grant");
        false
    }

    /// Check if a referrer header matches a pattern
    ///
    /// Pattern matching rules:
    /// - `*` at start matches any subdomain: `*.example.com` matches `www.example.com`
    /// - Exact match otherwise
    fn matches_referrer_pattern(referrer: &str, pattern: &str) -> bool {
        if let Some(suffix) = pattern.strip_prefix('*') {
            // Wildcard match: *.example.com matches www.example.com, api.example.com, etc.
            // Remove leading *
            referrer.ends_with(suffix)
        } else {
            // Exact match
            referrer == pattern
        }
    }

    /// Convert read grants to header value
    pub fn read_to_header(&self) -> Option<String> {
        if self.read.is_empty() {
            None
        } else {
            Some(self.read.iter().map(|g| g.to_string()).collect::<Vec<_>>().join(","))
        }
    }

    /// Convert write grants to header value
    pub fn write_to_header(&self) -> Option<String> {
        if self.write.is_empty() {
            None
        } else {
            Some(self.write.iter().map(|g| g.to_string()).collect::<Vec<_>>().join(","))
        }
    }

    /// Check if this ACL allows public read access
    pub fn is_public_read(&self) -> bool {
        self.read
            .iter()
            .any(|g| matches!(g, AclGrant::PublicRead | AclGrant::PublicReadReferrer(_)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_public_read() {
        let grants = ContainerAcl::parse_read(".r:*").unwrap();
        assert_eq!(grants.len(), 1);
        assert_eq!(grants[0], AclGrant::PublicRead);
    }

    #[test]
    fn test_parse_referrer_restriction() {
        let grants = ContainerAcl::parse_read(".r:*.example.com").unwrap();
        assert_eq!(grants.len(), 1);
        assert_eq!(grants[0], AclGrant::PublicReadReferrer("*.example.com".to_string()));
    }

    #[test]
    fn test_parse_account() {
        let grants = ContainerAcl::parse_read("AUTH_abc123").unwrap();
        assert_eq!(grants.len(), 1);
        assert_eq!(grants[0], AclGrant::Account("AUTH_abc123".to_string()));
    }

    #[test]
    fn test_parse_user() {
        let grants = ContainerAcl::parse_read("AUTH_abc123:user1").unwrap();
        assert_eq!(grants.len(), 1);
        assert_eq!(
            grants[0],
            AclGrant::User {
                account: "AUTH_abc123".to_string(),
                user: "user1".to_string()
            }
        );
    }

    #[test]
    fn test_parse_multiple_grants() {
        let grants = ContainerAcl::parse_read(".r:*,AUTH_abc123,AUTH_def456:user1").unwrap();
        assert_eq!(grants.len(), 3);
        assert_eq!(grants[0], AclGrant::PublicRead);
        assert_eq!(grants[1], AclGrant::Account("AUTH_abc123".to_string()));
        assert_eq!(
            grants[2],
            AclGrant::User {
                account: "AUTH_def456".to_string(),
                user: "user1".to_string()
            }
        );
    }

    #[test]
    fn test_parse_write_no_public() {
        // Public read in write ACL should fail
        let result = ContainerAcl::parse_write(".r:*");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Public access not allowed"));
    }

    #[test]
    fn test_parse_write_accounts() {
        let grants = ContainerAcl::parse_write("AUTH_abc123,AUTH_def456:user1").unwrap();
        assert_eq!(grants.len(), 2);
        assert_eq!(grants[0], AclGrant::Account("AUTH_abc123".to_string()));
        assert_eq!(
            grants[1],
            AclGrant::User {
                account: "AUTH_def456".to_string(),
                user: "user1".to_string()
            }
        );
    }

    #[test]
    fn test_parse_empty() {
        let grants = ContainerAcl::parse_read("").unwrap();
        assert_eq!(grants.len(), 0);

        let grants = ContainerAcl::parse_write("   ").unwrap();
        assert_eq!(grants.len(), 0);
    }

    #[test]
    fn test_parse_invalid_format() {
        // Invalid patterns
        assert!(ContainerAcl::parse_read("invalid_format").is_err());
        assert!(ContainerAcl::parse_read(".r:").is_err());
        assert!(ContainerAcl::parse_read("AUTH_abc:").is_err());
    }

    #[test]
    fn test_check_public_read_access() {
        let mut acl = ContainerAcl::new();
        acl.read.push(AclGrant::PublicRead);

        // Anyone can read
        assert!(acl.check_read_access(None, None, None));
        assert!(acl.check_read_access(Some("AUTH_other"), None, None));
    }

    #[test]
    fn test_check_referrer_access() {
        let mut acl = ContainerAcl::new();
        acl.read.push(AclGrant::PublicReadReferrer("*.example.com".to_string()));

        // Matches wildcard
        assert!(acl.check_read_access(None, None, Some("www.example.com")));
        assert!(acl.check_read_access(None, None, Some("api.example.com")));

        // Doesn't match
        assert!(!acl.check_read_access(None, None, Some("www.other.com")));
        assert!(!acl.check_read_access(None, None, None));
    }

    #[test]
    fn test_check_account_access() {
        let mut acl = ContainerAcl::new();
        acl.read.push(AclGrant::Account("AUTH_abc123".to_string()));

        // Matches account
        assert!(acl.check_read_access(Some("AUTH_abc123"), None, None));

        // Doesn't match
        assert!(!acl.check_read_access(Some("AUTH_other"), None, None));
        assert!(!acl.check_read_access(None, None, None));
    }

    #[test]
    fn test_check_user_access() {
        let mut acl = ContainerAcl::new();
        acl.read.push(AclGrant::User {
            account: "AUTH_abc123".to_string(),
            user: "user1".to_string(),
        });

        // Matches user
        assert!(acl.check_read_access(Some("AUTH_abc123"), Some("user1"), None));

        // Doesn't match (wrong user)
        assert!(!acl.check_read_access(Some("AUTH_abc123"), Some("user2"), None));

        // Doesn't match (no user)
        assert!(!acl.check_read_access(Some("AUTH_abc123"), None, None));
    }

    #[test]
    fn test_check_write_access() {
        let mut acl = ContainerAcl::new();
        acl.write.push(AclGrant::Account("AUTH_abc123".to_string()));

        // Matches account
        assert!(acl.check_write_access("AUTH_abc123", None));

        // Doesn't match
        assert!(!acl.check_write_access("AUTH_other", None));
    }

    #[test]
    fn test_default_access_no_acl() {
        let acl = ContainerAcl::new();

        // No ACL means default: authenticated users can read
        assert!(acl.check_read_access(Some("AUTH_owner"), None, None));
        assert!(!acl.check_read_access(None, None, None));

        // No write ACL means owner can write (default)
        assert!(acl.check_write_access("AUTH_owner", None));
    }

    #[test]
    fn test_to_header() {
        let mut acl = ContainerAcl::new();
        acl.read.push(AclGrant::PublicRead);
        acl.read.push(AclGrant::Account("AUTH_abc123".to_string()));

        let header = acl.read_to_header().unwrap();
        assert_eq!(header, ".r:*,AUTH_abc123");
    }

    #[test]
    fn test_is_public_read() {
        let mut acl = ContainerAcl::new();
        assert!(!acl.is_public_read());

        acl.read.push(AclGrant::PublicRead);
        assert!(acl.is_public_read());

        let mut acl2 = ContainerAcl::new();
        acl2.read.push(AclGrant::PublicReadReferrer("*.example.com".to_string()));
        assert!(acl2.is_public_read());
    }

    // Integration-style tests for ACL workflows

    #[test]
    fn test_acl_roundtrip_public_read() {
        // Simulate: swift post container -r ".r:*"
        let header = ".r:*";
        let grants = ContainerAcl::parse_read(header).unwrap();

        let mut acl = ContainerAcl::new();
        acl.read = grants;

        // Verify anyone can read
        assert!(acl.check_read_access(None, None, None));
        assert!(acl.check_read_access(Some("AUTH_other"), None, None));
    }

    #[test]
    fn test_acl_roundtrip_referrer_restriction() {
        // Simulate: swift post container -r ".r:*.example.com"
        let header = ".r:*.example.com";
        let grants = ContainerAcl::parse_read(header).unwrap();

        let mut acl = ContainerAcl::new();
        acl.read = grants;

        // Verify referrer matching
        assert!(acl.check_read_access(None, None, Some("www.example.com")));
        assert!(acl.check_read_access(None, None, Some("api.example.com")));
        assert!(!acl.check_read_access(None, None, Some("www.other.com")));
        assert!(!acl.check_read_access(None, None, None));
    }

    #[test]
    fn test_acl_roundtrip_account_access() {
        // Simulate: swift post container -r "AUTH_abc123,AUTH_def456"
        let header = "AUTH_abc123,AUTH_def456";
        let grants = ContainerAcl::parse_read(header).unwrap();

        let mut acl = ContainerAcl::new();
        acl.read = grants;

        // Verify account matching
        assert!(acl.check_read_access(Some("AUTH_abc123"), None, None));
        assert!(acl.check_read_access(Some("AUTH_def456"), None, None));
        assert!(!acl.check_read_access(Some("AUTH_other"), None, None));
        assert!(!acl.check_read_access(None, None, None));
    }

    #[test]
    fn test_acl_roundtrip_mixed_grants() {
        // Simulate: swift post container -r ".r:*.cdn.com,AUTH_abc123,AUTH_def456:user1"
        let header = ".r:*.cdn.com,AUTH_abc123,AUTH_def456:user1";
        let grants = ContainerAcl::parse_read(header).unwrap();

        let mut acl = ContainerAcl::new();
        acl.read = grants;

        // Verify various access patterns
        assert!(acl.check_read_access(None, None, Some("api.cdn.com"))); // Referrer
        assert!(acl.check_read_access(Some("AUTH_abc123"), None, None)); // Account
        assert!(acl.check_read_access(Some("AUTH_def456"), Some("user1"), None)); // User
        assert!(!acl.check_read_access(Some("AUTH_def456"), Some("user2"), None)); // Wrong user
        assert!(!acl.check_read_access(Some("AUTH_other"), None, None)); // Wrong account
    }

    #[test]
    fn test_acl_write_account_only() {
        // Simulate: swift post container -w "AUTH_abc123"
        let header = "AUTH_abc123";
        let grants = ContainerAcl::parse_write(header).unwrap();

        let mut acl = ContainerAcl::new();
        acl.write = grants;

        // Verify write access
        assert!(acl.check_write_access("AUTH_abc123", None));
        assert!(!acl.check_write_access("AUTH_other", None));
    }

    #[test]
    fn test_acl_write_user_specific() {
        // Simulate: swift post container -w "AUTH_abc123:user1,AUTH_def456:user2"
        let header = "AUTH_abc123:user1,AUTH_def456:user2";
        let grants = ContainerAcl::parse_write(header).unwrap();

        let mut acl = ContainerAcl::new();
        acl.write = grants;

        // Verify user-specific write access
        assert!(acl.check_write_access("AUTH_abc123", Some("user1")));
        assert!(acl.check_write_access("AUTH_def456", Some("user2")));
        assert!(!acl.check_write_access("AUTH_abc123", Some("user2"))); // Wrong user
        assert!(!acl.check_write_access("AUTH_abc123", None)); // No user specified
        assert!(!acl.check_write_access("AUTH_other", Some("user1"))); // Wrong account
    }

    #[test]
    fn test_acl_permission_denied_scenarios() {
        // Test various permission denied scenarios
        let mut acl = ContainerAcl::new();
        acl.read.push(AclGrant::Account("AUTH_abc123".to_string()));
        acl.write.push(AclGrant::Account("AUTH_abc123".to_string()));

        // Read denied
        assert!(!acl.check_read_access(Some("AUTH_other"), None, None));
        assert!(!acl.check_read_access(None, None, None));

        // Write denied
        assert!(!acl.check_write_access("AUTH_other", None));
    }

    #[test]
    fn test_acl_empty_means_owner_only() {
        // When no ACL is set, only authenticated owner should have access
        let acl = ContainerAcl::new();

        // Empty read ACL: authenticated users can read
        assert!(acl.check_read_access(Some("AUTH_owner"), None, None));
        assert!(!acl.check_read_access(None, None, None)); // Unauthenticated denied

        // Empty write ACL: owner can write (default behavior)
        assert!(acl.check_write_access("AUTH_owner", None));
    }

    #[test]
    fn test_acl_remove_scenario() {
        // Simulate removing ACLs (setting to empty)
        let mut acl = ContainerAcl::new();
        acl.read.push(AclGrant::PublicRead);
        acl.write.push(AclGrant::Account("AUTH_abc123".to_string()));

        // Initially has ACLs
        assert!(acl.is_public_read());
        assert!(!acl.write.is_empty());

        // Remove ACLs
        acl.read.clear();
        acl.write.clear();

        // Now reverts to default behavior
        assert!(!acl.is_public_read());
        assert!(acl.read.is_empty());
        assert!(acl.write.is_empty());
    }

    #[test]
    fn test_acl_wildcard_referrer_patterns() {
        let mut acl = ContainerAcl::new();
        acl.read.push(AclGrant::PublicReadReferrer("*.example.com".to_string()));

        // Test various subdomain patterns
        assert!(acl.check_read_access(None, None, Some("www.example.com")));
        assert!(acl.check_read_access(None, None, Some("api.example.com")));
        assert!(acl.check_read_access(None, None, Some("cdn.example.com")));
        assert!(acl.check_read_access(None, None, Some("a.b.c.example.com")));

        // Should not match
        assert!(!acl.check_read_access(None, None, Some("example.com"))); // No subdomain
        assert!(!acl.check_read_access(None, None, Some("example.org")));
        assert!(!acl.check_read_access(None, None, Some("notexample.com")));
        assert!(!acl.check_read_access(None, None, None)); // No referrer
    }

    #[test]
    fn test_acl_exact_referrer_match() {
        let mut acl = ContainerAcl::new();
        acl.read.push(AclGrant::PublicReadReferrer("cdn.example.com".to_string()));

        // Exact match only
        assert!(acl.check_read_access(None, None, Some("cdn.example.com")));

        // Should not match
        assert!(!acl.check_read_access(None, None, Some("api.cdn.example.com")));
        assert!(!acl.check_read_access(None, None, Some("www.example.com")));
        assert!(!acl.check_read_access(None, None, None));
    }

    #[test]
    fn test_acl_header_serialization() {
        // Test round-trip: parse → serialize → parse
        let original = ".r:*,AUTH_abc123,AUTH_def456:user1";
        let grants = ContainerAcl::parse_read(original).unwrap();

        let mut acl = ContainerAcl::new();
        acl.read = grants;

        let serialized = acl.read_to_header().unwrap();
        let reparsed = ContainerAcl::parse_read(&serialized).unwrap();

        // Should match original parsing
        assert_eq!(reparsed.len(), 3);
        assert!(matches!(reparsed[0], AclGrant::PublicRead));
        assert!(matches!(reparsed[1], AclGrant::Account(_)));
        assert!(matches!(reparsed[2], AclGrant::User { .. }));
    }

    #[test]
    fn test_acl_whitespace_handling() {
        // Test that whitespace is properly trimmed
        let header = "  .r:* , AUTH_abc123 , AUTH_def456:user1  ";
        let grants = ContainerAcl::parse_read(header).unwrap();

        assert_eq!(grants.len(), 3);
        assert_eq!(grants[0], AclGrant::PublicRead);
        assert_eq!(grants[1], AclGrant::Account("AUTH_abc123".to_string()));
    }

    #[test]
    fn test_acl_multiple_referrer_patterns() {
        // Multiple referrer restrictions
        let header = ".r:*.example.com,.r:*.cdn.com";
        let grants = ContainerAcl::parse_read(header).unwrap();

        let mut acl = ContainerAcl::new();
        acl.read = grants;

        // Should match either pattern
        assert!(acl.check_read_access(None, None, Some("www.example.com")));
        assert!(acl.check_read_access(None, None, Some("api.cdn.com")));
        assert!(!acl.check_read_access(None, None, Some("www.other.com")));
    }

    #[test]
    fn test_acl_user_requires_both_account_and_user() {
        let mut acl = ContainerAcl::new();
        acl.read.push(AclGrant::User {
            account: "AUTH_abc123".to_string(),
            user: "user1".to_string(),
        });

        // Need both account and user to match
        assert!(acl.check_read_access(Some("AUTH_abc123"), Some("user1"), None));

        // Missing user
        assert!(!acl.check_read_access(Some("AUTH_abc123"), None, None));

        // Wrong user
        assert!(!acl.check_read_access(Some("AUTH_abc123"), Some("user2"), None));

        // Wrong account
        assert!(!acl.check_read_access(Some("AUTH_other"), Some("user1"), None));
    }

    #[test]
    fn test_acl_complex_scenario() {
        // Complex real-world scenario: public CDN access + specific accounts
        let read_header = ".r:*.cloudfront.net,AUTH_admin,AUTH_support:viewer";
        let write_header = "AUTH_admin,AUTH_publisher:editor";

        let read_grants = ContainerAcl::parse_read(read_header).unwrap();
        let write_grants = ContainerAcl::parse_write(write_header).unwrap();

        let mut acl = ContainerAcl::new();
        acl.read = read_grants;
        acl.write = write_grants;

        // Read access scenarios
        assert!(acl.check_read_access(None, None, Some("d111.cloudfront.net"))); // CDN
        assert!(acl.check_read_access(Some("AUTH_admin"), None, None)); // Admin account
        assert!(acl.check_read_access(Some("AUTH_support"), Some("viewer"), None)); // Support viewer
        assert!(!acl.check_read_access(Some("AUTH_support"), Some("other"), None)); // Wrong user
        assert!(!acl.check_read_access(Some("AUTH_other"), None, None)); // Unauthorized

        // Write access scenarios
        assert!(acl.check_write_access("AUTH_admin", None)); // Admin account
        assert!(acl.check_write_access("AUTH_publisher", Some("editor"))); // Publisher editor
        assert!(!acl.check_write_access("AUTH_publisher", Some("viewer"))); // Wrong role
        assert!(!acl.check_write_access("AUTH_support", Some("viewer"))); // Read-only
        assert!(!acl.check_write_access("AUTH_other", None)); // Unauthorized
    }
}
