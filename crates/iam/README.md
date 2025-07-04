[![RustFS](https://rustfs.com/images/rustfs-github.png)](https://rustfs.com)

# RustFS IAM - Identity and Access Management

<p align="center">
  <strong>Enterprise-grade identity and access management for RustFS distributed object storage</strong>
</p>

<p align="center">
  <a href="https://github.com/rustfs/rustfs/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/rustfs/rustfs/actions/workflows/ci.yml/badge.svg" /></a>
  <a href="https://docs.rustfs.com/en/">📖 Documentation</a>
  · <a href="https://github.com/rustfs/rustfs/issues">🐛 Bug Reports</a>
  · <a href="https://github.com/rustfs/rustfs/discussions">💬 Discussions</a>
</p>

---

## 📖 Overview

**RustFS IAM** is the identity and access management module for the [RustFS](https://rustfs.com) distributed object storage system. It provides comprehensive authentication, authorization, and access control capabilities, ensuring secure and compliant access to storage resources.

> **Note:** This is a core submodule of RustFS and provides essential security and access control features for the distributed object storage system. For the complete RustFS experience, please visit the [main RustFS repository](https://github.com/rustfs/rustfs).

## ✨ Features

### 🔐 Authentication & Authorization

- **Multi-Factor Authentication**: Support for various authentication methods
- **Access Key Management**: Secure generation and management of access keys
- **JWT Token Support**: Stateless authentication with JWT tokens
- **Session Management**: Secure session handling and token refresh

### 👥 User Management

- **User Accounts**: Complete user lifecycle management
- **Service Accounts**: Automated service authentication
- **Temporary Accounts**: Time-limited access credentials
- **Group Management**: Organize users into groups for easier management

### 🛡️ Access Control

- **Role-Based Access Control (RBAC)**: Flexible role and permission system
- **Policy-Based Access Control**: Fine-grained access policies
- **Resource-Level Permissions**: Granular control over storage resources
- **API-Level Authorization**: Secure API access control

### 🔑 Credential Management

- **Secure Key Generation**: Cryptographically secure key generation
- **Key Rotation**: Automatic and manual key rotation capabilities
- **Credential Validation**: Real-time credential verification
- **Secret Management**: Secure storage and retrieval of secrets

### 🏢 Enterprise Features

- **LDAP Integration**: Enterprise directory service integration
- **SSO Support**: Single Sign-On capabilities
- **Audit Logging**: Comprehensive access audit trails
- **Compliance Features**: Meet regulatory compliance requirements

## 🏗️ Architecture

### IAM System Architecture

```
IAM Architecture:
┌─────────────────────────────────────────────────────────────┐
│                    IAM API Layer                            │
├─────────────────────────────────────────────────────────────┤
│  Authentication  │  Authorization  │  User Management       │
├─────────────────────────────────────────────────────────────┤
│              Policy Engine Integration                       │
├─────────────────────────────────────────────────────────────┤
│    Credential Store   │    Cache Layer    │   Token Manager │
├─────────────────────────────────────────────────────────────┤
│              Storage Backend Integration                     │
└─────────────────────────────────────────────────────────────┘
```

### Security Model

| Component | Description | Security Level |
|-----------|-------------|----------------|
| Access Keys | API authentication credentials | High |
| JWT Tokens | Stateless authentication tokens | High |
| Session Management | User session handling | Medium |
| Policy Enforcement | Access control policies | Critical |
| Audit Logging | Security event tracking | High |

## 📦 Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
rustfs-iam = "0.1.0"
```

## 🔧 Usage

### Basic IAM Setup

```rust
use rustfs_iam::{init_iam_sys, get};
use rustfs_ecstore::ECStore;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize with ECStore backend
    let ecstore = Arc::new(ECStore::new("/path/to/storage").await?);

    // Initialize IAM system
    init_iam_sys(ecstore).await?;

    // Get IAM system instance
    let iam = get()?;

    println!("IAM system initialized successfully");
    Ok(())
}
```

### User Management

```rust
use rustfs_iam::{get, manager::UserInfo};

async fn user_management_example() -> Result<(), Box<dyn std::error::Error>> {
    let iam = get()?;

    // Create a new user
    let user_info = UserInfo {
        access_key: "AKIAIOSFODNN7EXAMPLE".to_string(),
        secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
        status: "enabled".to_string(),
        ..Default::default()
    };

    iam.create_user("john-doe", user_info).await?;

    // List users
    let users = iam.list_users().await?;
    for user in users {
        println!("User: {}, Status: {}", user.name, user.status);
    }

    // Update user status
    iam.set_user_status("john-doe", "disabled").await?;

    // Delete user
    iam.delete_user("john-doe").await?;

    Ok(())
}
```

### Group Management

```rust
use rustfs_iam::{get, manager::GroupInfo};

async fn group_management_example() -> Result<(), Box<dyn std::error::Error>> {
    let iam = get()?;

    // Create a group
    let group_info = GroupInfo {
        name: "developers".to_string(),
        members: vec!["john-doe".to_string(), "jane-smith".to_string()],
        policies: vec!["read-only-policy".to_string()],
        ..Default::default()
    };

    iam.create_group(group_info).await?;

    // Add user to group
    iam.add_user_to_group("alice", "developers").await?;

    // Remove user from group
    iam.remove_user_from_group("alice", "developers").await?;

    // List groups
    let groups = iam.list_groups().await?;
    for group in groups {
        println!("Group: {}, Members: {}", group.name, group.members.len());
    }

    Ok(())
}
```

### Policy Management

```rust
use rustfs_iam::{get, manager::PolicyDocument};

async fn policy_management_example() -> Result<(), Box<dyn std::error::Error>> {
    let iam = get()?;

    // Create a policy
    let policy_doc = PolicyDocument {
        version: "2012-10-17".to_string(),
        statement: vec![
            Statement {
                effect: "Allow".to_string(),
                action: vec!["s3:GetObject".to_string()],
                resource: vec!["arn:aws:s3:::my-bucket/*".to_string()],
                ..Default::default()
            }
        ],
        ..Default::default()
    };

    iam.create_policy("read-only-policy", policy_doc).await?;

    // Attach policy to user
    iam.attach_user_policy("john-doe", "read-only-policy").await?;

    // Detach policy from user
    iam.detach_user_policy("john-doe", "read-only-policy").await?;

    // List policies
    let policies = iam.list_policies().await?;
    for policy in policies {
        println!("Policy: {}", policy.name);
    }

    Ok(())
}
```

### Service Account Management

```rust
use rustfs_iam::{get, manager::ServiceAccountInfo};

async fn service_account_example() -> Result<(), Box<dyn std::error::Error>> {
    let iam = get()?;

    // Create service account
    let service_account = ServiceAccountInfo {
        name: "backup-service".to_string(),
        description: "Automated backup service".to_string(),
        policies: vec!["backup-policy".to_string()],
        ..Default::default()
    };

    iam.create_service_account(service_account).await?;

    // Generate credentials for service account
    let credentials = iam.generate_service_account_credentials("backup-service").await?;
    println!("Service Account Credentials: {:?}", credentials);

    // Rotate service account credentials
    iam.rotate_service_account_credentials("backup-service").await?;

    Ok(())
}
```

### Authentication and Authorization

```rust
use rustfs_iam::{get, auth::Credentials};

async fn auth_example() -> Result<(), Box<dyn std::error::Error>> {
    let iam = get()?;

    // Authenticate user
    let credentials = Credentials {
        access_key: "AKIAIOSFODNN7EXAMPLE".to_string(),
        secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
        session_token: None,
    };

    let auth_result = iam.authenticate(&credentials).await?;
    println!("Authentication successful: {}", auth_result.user_name);

    // Check authorization
    let authorized = iam.is_authorized(
        &auth_result.user_name,
        "s3:GetObject",
        "arn:aws:s3:::my-bucket/file.txt"
    ).await?;

    if authorized {
        println!("User is authorized to access the resource");
    } else {
        println!("User is not authorized to access the resource");
    }

    Ok(())
}
```

### Temporary Credentials

```rust
use rustfs_iam::{get, manager::TemporaryCredentials};
use std::time::Duration;

async fn temp_credentials_example() -> Result<(), Box<dyn std::error::Error>> {
    let iam = get()?;

    // Create temporary credentials
    let temp_creds = iam.create_temporary_credentials(
        "john-doe",
        Duration::from_secs(3600), // 1 hour
        Some("read-only-policy".to_string())
    ).await?;

    println!("Temporary Access Key: {}", temp_creds.access_key);
    println!("Expires at: {}", temp_creds.expiration);

    // Validate temporary credentials
    let is_valid = iam.validate_temporary_credentials(&temp_creds.access_key).await?;
    println!("Temporary credentials valid: {}", is_valid);

    Ok(())
}
```

## 🧪 Testing

Run the test suite:

```bash
# Run all tests
cargo test

# Run tests with specific features
cargo test --features "ldap,sso"

# Run integration tests
cargo test --test integration

# Run authentication tests
cargo test auth

# Run authorization tests
cargo test authz
```

## 🔒 Security Best Practices

### Key Management

- Rotate access keys regularly
- Use strong, randomly generated keys
- Store keys securely using environment variables or secret management systems
- Implement key rotation policies

### Access Control

- Follow the principle of least privilege
- Use groups for easier permission management
- Regularly audit user permissions
- Implement resource-based policies

### Monitoring and Auditing

- Enable comprehensive audit logging
- Monitor failed authentication attempts
- Set up alerts for suspicious activities
- Regular security reviews

## 📊 Performance Considerations

### Caching Strategy

- **User Cache**: Cache user information for faster lookups
- **Policy Cache**: Cache policy documents to reduce latency
- **Token Cache**: Cache JWT tokens for stateless authentication
- **Permission Cache**: Cache authorization decisions

### Scalability

- **Distributed Cache**: Use distributed caching for multi-node deployments
- **Database Optimization**: Optimize database queries for user/group lookups
- **Connection Pooling**: Use connection pooling for database connections
- **Async Operations**: Leverage async programming for better throughput

## 🔧 Configuration

### Basic Configuration

```toml
[iam]
# Authentication settings
jwt_secret = "your-jwt-secret-key"
jwt_expiration = "24h"
session_timeout = "30m"

# Password policy
min_password_length = 8
require_special_chars = true
require_numbers = true
require_uppercase = true

# Account lockout
max_login_attempts = 5
lockout_duration = "15m"

# Audit settings
audit_enabled = true
audit_log_path = "/var/log/rustfs/iam-audit.log"
```

### Advanced Configuration

```rust
use rustfs_iam::config::IamConfig;

let config = IamConfig {
    // Authentication settings
    jwt_secret: "your-secure-jwt-secret".to_string(),
    jwt_expiration_hours: 24,
    session_timeout_minutes: 30,

    // Security settings
    password_policy: PasswordPolicy {
        min_length: 8,
        require_special_chars: true,
        require_numbers: true,
        require_uppercase: true,
        max_age_days: 90,
    },

    // Rate limiting
    rate_limit: RateLimit {
        max_requests_per_minute: 100,
        burst_size: 10,
    },

    // Audit settings
    audit_enabled: true,
    audit_log_level: "info".to_string(),

    ..Default::default()
};
```

## 🤝 Integration with RustFS

IAM integrates seamlessly with other RustFS components:

- **ECStore**: Provides user and policy storage backend
- **Policy Engine**: Implements fine-grained access control
- **Crypto Module**: Handles secure key generation and JWT operations
- **API Server**: Provides authentication and authorization for S3 API
- **Admin Interface**: Manages users, groups, and policies

## 📋 Requirements

- **Rust**: 1.70.0 or later
- **Platforms**: Linux, macOS, Windows
- **Database**: Compatible with RustFS storage backend
- **Memory**: Minimum 2GB RAM for caching
- **Network**: Secure connections for authentication

## 🐛 Troubleshooting

### Common Issues

1. **Authentication Failures**:
   - Check access key and secret key validity
   - Verify user account status (enabled/disabled)
   - Check for account lockout due to failed attempts

2. **Authorization Errors**:
   - Verify user has required permissions
   - Check policy attachments (user/group policies)
   - Validate resource ARN format

3. **Performance Issues**:
   - Monitor cache hit rates
   - Check database connection pool utilization
   - Verify JWT token size and complexity

### Debug Commands

```bash
# Check IAM system status
rustfs-cli iam status

# List all users
rustfs-cli iam list-users

# Validate user credentials
rustfs-cli iam validate-credentials --access-key <key>

# Test policy evaluation
rustfs-cli iam test-policy --user <user> --action <action> --resource <resource>
```

## 🌍 Related Projects

This module is part of the RustFS ecosystem:

- [RustFS Main](https://github.com/rustfs/rustfs) - Core distributed storage system
- [RustFS ECStore](../ecstore) - Erasure coding storage engine
- [RustFS Policy](../policy) - Policy engine for access control
- [RustFS Crypto](../crypto) - Cryptographic operations
- [RustFS MadAdmin](../madmin) - Administrative interface

## 📚 Documentation

For comprehensive documentation, visit:

- [RustFS Documentation](https://docs.rustfs.com)
- [IAM API Reference](https://docs.rustfs.com/iam/)
- [Security Guide](https://docs.rustfs.com/security/)
- [Authentication Guide](https://docs.rustfs.com/auth/)

## 🔗 Links

- [Documentation](https://docs.rustfs.com) - Complete RustFS manual
- [Changelog](https://github.com/rustfs/rustfs/releases) - Release notes and updates
- [GitHub Discussions](https://github.com/rustfs/rustfs/discussions) - Community support

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](https://github.com/rustfs/rustfs/blob/main/CONTRIBUTING.md) for details on:

- Security-first development practices
- IAM system architecture guidelines
- Authentication and authorization patterns
- Testing procedures for security features
- Documentation standards for security APIs

### Development Setup

```bash
# Clone the repository
git clone https://github.com/rustfs/rustfs.git
cd rustfs

# Navigate to IAM module
cd crates/iam

# Install dependencies
cargo build

# Run tests
cargo test

# Run security tests
cargo test security

# Format code
cargo fmt

# Run linter
cargo clippy
```

## 💬 Getting Help

- **Documentation**: [docs.rustfs.com](https://docs.rustfs.com)
- **Issues**: [GitHub Issues](https://github.com/rustfs/rustfs/issues)
- **Discussions**: [GitHub Discussions](https://github.com/rustfs/rustfs/discussions)
- **Security**: Report security issues to <security@rustfs.com>

## 📞 Contact

- **Bugs**: [GitHub Issues](https://github.com/rustfs/rustfs/issues)
- **Business**: <hello@rustfs.com>
- **Jobs**: <jobs@rustfs.com>
- **General Discussion**: [GitHub Discussions](https://github.com/rustfs/rustfs/discussions)

## 👥 Contributors

This module is maintained by the RustFS security team and community contributors. Special thanks to all who have contributed to making RustFS secure and compliant.

<a href="https://github.com/rustfs/rustfs/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=rustfs/rustfs" />
</a>

## 📄 License

Licensed under the Apache License, Version 2.0. See [LICENSE](https://github.com/rustfs/rustfs/blob/main/LICENSE) for details.

```
Copyright 2024 RustFS Team

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```

---

<p align="center">
  <strong>RustFS</strong> is a trademark of RustFS, Inc.<br>
  All other trademarks are the property of their respective owners.
</p>

<p align="center">
  Made with 🔐 by the RustFS Security Team
</p>
