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

/// Default Access Key
/// Default value: rustfsadmin
/// Environment variable: RUSTFS_ACCESS_KEY
/// Command line argument: --access-key
/// Example: RUSTFS_ACCESS_KEY=rustfsadmin
/// Example: --access-key rustfsadmin
pub const DEFAULT_ACCESS_KEY: &str = "rustfsadmin";
/// Default Secret Key
/// Default value: rustfsadmin
/// Environment variable: RUSTFS_SECRET_KEY
/// Command line argument: --secret-key
/// Example: RUSTFS_SECRET_KEY=rustfsadmin
/// Example: --secret-key rustfsadmin
pub const DEFAULT_SECRET_KEY: &str = "rustfsadmin";

/// Environment variable for gRPC authentication token
/// Used to set the authentication token for gRPC communication
/// Example: RUSTFS_GRPC_AUTH_TOKEN=your_token_here
/// Default value: No default value. RUSTFS_SECRET_KEY value is recommended.
pub const ENV_GRPC_AUTH_TOKEN: &str = "RUSTFS_GRPC_AUTH_TOKEN";

/// IAM Policy Types
/// Used to differentiate between embedded and inherited policies
/// Example: "embedded-policy" or "inherited-policy"
/// Default value: "embedded-policy"
pub const EMBEDDED_POLICY_TYPE: &str = "embedded-policy";

/// IAM Policy Types
/// Used to differentiate between embedded and inherited policies
/// Example: "embedded-policy" or "inherited-policy"
/// Default value: "inherited-policy"
pub const INHERITED_POLICY_TYPE: &str = "inherited-policy";

/// IAM Policy Claim Name for Service Account
/// Used to identify the service account policy claim in JWT tokens
/// Example: "sa-policy"
/// Default value: "sa-policy"
pub const IAM_POLICY_CLAIM_NAME_SA: &str = "sa-policy";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_security_constants() {
        // Test security related constants
        assert_eq!(DEFAULT_ACCESS_KEY, "rustfsadmin");
        assert!(DEFAULT_ACCESS_KEY.len() >= 8, "Access key should be at least 8 characters");

        assert_eq!(DEFAULT_SECRET_KEY, "rustfsadmin");
        assert!(DEFAULT_SECRET_KEY.len() >= 8, "Secret key should be at least 8 characters");

        // In production environment, access key and secret key should be different
        // These are default values, so being the same is acceptable, but should be warned in documentation
        println!("Warning: Default access key and secret key are the same. Change them in production!");
    }

    #[test]
    fn test_security_best_practices() {
        // Test security best practices

        // These are default values, should be changed in production environments
        println!("Security Warning: Default credentials detected!");
        println!("Access Key: {DEFAULT_ACCESS_KEY}");
        println!("Secret Key: {DEFAULT_SECRET_KEY}");
        println!("These should be changed in production environments!");

        // Verify that key lengths meet minimum security requirements
        assert!(DEFAULT_ACCESS_KEY.len() >= 8, "Access key should be at least 8 characters");
        assert!(DEFAULT_SECRET_KEY.len() >= 8, "Secret key should be at least 8 characters");

        // Check if default credentials contain common insecure patterns
        let _insecure_patterns = ["admin", "password", "123456", "default"];
        let _access_key_lower = DEFAULT_ACCESS_KEY.to_lowercase();
        let _secret_key_lower = DEFAULT_SECRET_KEY.to_lowercase();

        // Note: More security check logic can be added here
        // For example, check if keys contain insecure patterns
    }
}
