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

use crate::KeystoneClient;
use rustfs_policy::policy::Policy;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

/// Maps Keystone identities to RustFS concepts
pub struct KeystoneIdentityMapper {
    #[allow(dead_code)]
    client: Arc<KeystoneClient>,
    role_policy_map: HashMap<String, String>,
    enable_tenant_prefix: bool,
}

impl KeystoneIdentityMapper {
    /// Create new identity mapper
    pub fn new(client: Arc<KeystoneClient>, enable_tenant_prefix: bool) -> Self {
        let mut role_policy_map = HashMap::new();

        // Default Keystone role mappings
        role_policy_map.insert("admin".to_string(), "AdminPolicy".to_string());
        role_policy_map.insert("Admin".to_string(), "AdminPolicy".to_string());
        role_policy_map.insert("Member".to_string(), "ReadWritePolicy".to_string());
        role_policy_map.insert("_member_".to_string(), "ReadOnlyPolicy".to_string());
        role_policy_map.insert("ResellerAdmin".to_string(), "AdminPolicy".to_string());
        role_policy_map.insert("SwiftOperator".to_string(), "ReadWritePolicy".to_string());
        role_policy_map.insert("objectstore:admin".to_string(), "AdminPolicy".to_string());
        role_policy_map.insert("objectstore:creator".to_string(), "ReadWritePolicy".to_string());

        Self {
            client,
            role_policy_map,
            enable_tenant_prefix,
        }
    }

    /// Add custom role-to-policy mapping
    pub fn add_role_mapping(&mut self, keystone_role: String, rustfs_policy: String) {
        info!("Adding role mapping: {} -> {}", keystone_role, rustfs_policy);
        self.role_policy_map.insert(keystone_role, rustfs_policy);
    }

    /// Add multiple role mappings
    pub fn add_role_mappings(&mut self, mappings: Vec<(String, String)>) {
        for (role, policy) in mappings {
            self.add_role_mapping(role, policy);
        }
    }

    /// Map Keystone roles to RustFS policy names
    pub fn map_roles_to_policies(&self, roles: &[String]) -> Vec<String> {
        let policies: Vec<String> = roles
            .iter()
            .filter_map(|role| self.role_policy_map.get(role).cloned())
            .collect();

        debug!("Mapped roles {:?} to policies {:?}", roles, policies);
        policies
    }

    /// Generate tenant-prefixed bucket name
    /// Format: <project_id>:<bucket_name>
    pub fn apply_tenant_prefix(&self, bucket: &str, project_id: Option<&str>) -> String {
        if !self.enable_tenant_prefix {
            return bucket.to_string();
        }

        if let Some(proj_id) = project_id {
            let prefixed = format!("{}:{}", proj_id, bucket);
            debug!("Applied tenant prefix: {} -> {}", bucket, prefixed);
            prefixed
        } else {
            bucket.to_string()
        }
    }

    /// Remove tenant prefix from bucket name
    pub fn remove_tenant_prefix(&self, prefixed_bucket: &str, project_id: Option<&str>) -> String {
        if !self.enable_tenant_prefix {
            return prefixed_bucket.to_string();
        }

        if let Some(proj_id) = project_id {
            let prefix = format!("{}:", proj_id);
            if prefixed_bucket.starts_with(&prefix) {
                let unprefixed = prefixed_bucket[prefix.len()..].to_string();
                debug!("Removed tenant prefix: {} -> {}", prefixed_bucket, unprefixed);
                return unprefixed;
            }
        }

        prefixed_bucket.to_string()
    }

    /// Check if bucket belongs to project
    pub fn is_project_bucket(&self, bucket: &str, project_id: Option<&str>) -> bool {
        if !self.enable_tenant_prefix {
            return true; // No multi-tenancy, all buckets accessible
        }

        if let Some(proj_id) = project_id {
            let prefix = format!("{}:", proj_id);
            bucket.starts_with(&prefix)
        } else {
            !bucket.contains(':') // No project ID, only unprefixed buckets
        }
    }

    /// Extract project ID from prefixed bucket name
    pub fn extract_project_id(&self, bucket: &str) -> Option<String> {
        if !self.enable_tenant_prefix {
            return None;
        }

        bucket.find(':').map(|pos| bucket[..pos].to_string())
    }

    /// Create default policies for Keystone roles
    pub fn create_default_policies(&self) -> HashMap<String, Policy> {
        let mut policies = HashMap::new();

        // Admin policy - full access
        let admin_json = r#"{
            "Version": "2012-10-17",
            "ID": "AdminPolicy",
            "Statement": [{
                "Sid": "AdminFullAccess",
                "Effect": "Allow",
                "Action": ["s3:*"],
                "Resource": ["arn:aws:s3:::*"]
            }]
        }"#;
        if let Ok(policy) = serde_json::from_str::<Policy>(admin_json) {
            policies.insert("AdminPolicy".to_string(), policy);
        }

        // ReadWrite policy - read/write access
        let readwrite_json = r#"{
            "Version": "2012-10-17",
            "ID": "ReadWritePolicy",
            "Statement": [{
                "Sid": "ReadWriteAccess",
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:DeleteObject",
                    "s3:ListBucket",
                    "s3:GetBucketLocation",
                    "s3:ListBucketMultipartUploads",
                    "s3:ListMultipartUploadParts",
                    "s3:AbortMultipartUpload"
                ],
                "Resource": ["arn:aws:s3:::*"]
            }]
        }"#;
        if let Ok(policy) = serde_json::from_str::<Policy>(readwrite_json) {
            policies.insert("ReadWritePolicy".to_string(), policy);
        }

        // ReadOnly policy - read-only access
        let readonly_json = r#"{
            "Version": "2012-10-17",
            "ID": "ReadOnlyPolicy",
            "Statement": [{
                "Sid": "ReadOnlyAccess",
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:ListBucket",
                    "s3:GetBucketLocation"
                ],
                "Resource": ["arn:aws:s3:::*"]
            }]
        }"#;
        if let Ok(policy) = serde_json::from_str::<Policy>(readonly_json) {
            policies.insert("ReadOnlyPolicy".to_string(), policy);
        }

        policies
    }

    /// Check if user has permission based on Keystone roles
    pub fn has_permission(&self, roles: &[String], action: &str, _resource: &str) -> bool {
        // Admin always has access
        if roles.iter().any(|r| r.eq_ignore_ascii_case("admin") || r == "ResellerAdmin") {
            return true;
        }

        // Check role-based permissions
        for role in roles {
            if let Some(policy_name) = self.role_policy_map.get(role) {
                match policy_name.as_str() {
                    "AdminPolicy" => return true,
                    "ReadWritePolicy" => {
                        if action.starts_with("s3:Get")
                            || action.starts_with("s3:Put")
                            || action.starts_with("s3:Delete")
                            || action.starts_with("s3:List")
                        {
                            return true;
                        }
                    }
                    "ReadOnlyPolicy" => {
                        if action.starts_with("s3:Get") || action.starts_with("s3:List") {
                            return true;
                        }
                    }
                    _ => continue,
                }
            }
        }

        false
    }

    /// Check if tenant prefixing is enabled
    pub fn is_tenant_prefix_enabled(&self) -> bool {
        self.enable_tenant_prefix
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::KeystoneVersion;

    fn create_mapper() -> KeystoneIdentityMapper {
        let client = KeystoneClient::new("http://localhost:5000".to_string(), KeystoneVersion::V3, None, None, None, true);
        KeystoneIdentityMapper::new(Arc::new(client), true)
    }

    #[test]
    fn test_tenant_prefix() {
        let mapper = create_mapper();

        let prefixed = mapper.apply_tenant_prefix("mybucket", Some("proj123"));
        assert_eq!(prefixed, "proj123:mybucket");

        let unprefixed = mapper.remove_tenant_prefix("proj123:mybucket", Some("proj123"));
        assert_eq!(unprefixed, "mybucket");

        // No project ID
        let no_prefix = mapper.apply_tenant_prefix("mybucket", None);
        assert_eq!(no_prefix, "mybucket");
    }

    #[test]
    fn test_is_project_bucket() {
        let mapper = create_mapper();

        assert!(mapper.is_project_bucket("proj123:mybucket", Some("proj123")));
        assert!(!mapper.is_project_bucket("proj456:mybucket", Some("proj123")));
        assert!(!mapper.is_project_bucket("mybucket", Some("proj123")));
    }

    #[test]
    fn test_extract_project_id() {
        let mapper = create_mapper();

        assert_eq!(mapper.extract_project_id("proj123:mybucket"), Some("proj123".to_string()));
        assert_eq!(mapper.extract_project_id("mybucket"), None);
    }

    #[test]
    fn test_role_mapping() {
        let mapper = create_mapper();

        let roles = vec!["Member".to_string(), "admin".to_string()];
        let policies = mapper.map_roles_to_policies(&roles);

        assert!(policies.contains(&"ReadWritePolicy".to_string()));
        assert!(policies.contains(&"AdminPolicy".to_string()));
    }

    #[test]
    fn test_has_permission() {
        let mapper = create_mapper();

        // Admin has all permissions
        assert!(mapper.has_permission(&["admin".to_string()], "s3:DeleteBucket", ""));

        // Member has read/write permissions
        assert!(mapper.has_permission(&["Member".to_string()], "s3:PutObject", ""));
        assert!(mapper.has_permission(&["Member".to_string()], "s3:GetObject", ""));

        // _member_ has read-only permissions
        assert!(mapper.has_permission(&["_member_".to_string()], "s3:GetObject", ""));
        assert!(!mapper.has_permission(&["_member_".to_string()], "s3:PutObject", ""));
    }

    #[test]
    fn test_add_role_mapping() {
        let mut mapper = create_mapper();

        mapper.add_role_mapping("CustomRole".to_string(), "CustomPolicy".to_string());

        let policies = mapper.map_roles_to_policies(&["CustomRole".to_string()]);
        assert_eq!(policies, vec!["CustomPolicy".to_string()]);
    }
}
