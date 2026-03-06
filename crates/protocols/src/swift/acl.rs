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
        if grant_str.starts_with(".r:") {
            if !allow_public {
                return Err(SwiftError::BadRequest(
                    "Public access not allowed in write ACL".to_string(),
                ));
            }

            let pattern = &grant_str[3..]; // Skip ".r:"
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
            Err(SwiftError::BadRequest(format!(
                "Invalid ACL grant format: {}",
                grant_str
            )))
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
    pub fn check_read_access(
        &self,
        request_account: Option<&str>,
        request_user: Option<&str>,
        referrer: Option<&str>,
    ) -> bool {
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
                    if let Some(ref_header) = referrer {
                        if Self::matches_referrer_pattern(ref_header, pattern) {
                            debug!("Read access granted: referrer matches pattern {}", pattern);
                            return true;
                        }
                    }
                }
                AclGrant::Account(account) => {
                    if let Some(req_account) = request_account {
                        if req_account == account {
                            debug!("Read access granted: account {} matches", account);
                            return true;
                        }
                    }
                }
                AclGrant::User {
                    account,
                    user: grant_user,
                } => {
                    if let (Some(req_account), Some(req_user)) = (request_account, request_user) {
                        if req_account == account && req_user == grant_user {
                            debug!("Read access granted: user {}:{} matches", account, grant_user);
                            return true;
                        }
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
    pub fn check_write_access(
        &self,
        request_account: &str,
        request_user: Option<&str>,
    ) -> bool {
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
                    if let Some(req_user) = request_user {
                        if request_account == account && req_user == grant_user {
                            debug!("Write access granted: user {}:{} matches", account, grant_user);
                            return true;
                        }
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
        if pattern.starts_with('*') {
            // Wildcard match: *.example.com matches www.example.com, api.example.com, etc.
            let suffix = &pattern[1..]; // Remove leading *
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
            Some(
                self.read
                    .iter()
                    .map(|g| g.to_string())
                    .collect::<Vec<_>>()
                    .join(","),
            )
        }
    }

    /// Convert write grants to header value
    pub fn write_to_header(&self) -> Option<String> {
        if self.write.is_empty() {
            None
        } else {
            Some(
                self.write
                    .iter()
                    .map(|g| g.to_string())
                    .collect::<Vec<_>>()
                    .join(","),
            )
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
        assert_eq!(
            grants[0],
            AclGrant::PublicReadReferrer("*.example.com".to_string())
        );
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
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Public access not allowed"));
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
        acl.read
            .push(AclGrant::PublicReadReferrer("*.example.com".to_string()));

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
        acl.read
            .push(AclGrant::Account("AUTH_abc123".to_string()));

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
        acl.write
            .push(AclGrant::Account("AUTH_abc123".to_string()));

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
        acl.read
            .push(AclGrant::Account("AUTH_abc123".to_string()));

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
        acl2.read
            .push(AclGrant::PublicReadReferrer("*.example.com".to_string()));
        assert!(acl2.is_public_read());
    }
}
