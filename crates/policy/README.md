[![RustFS](https://rustfs.com/images/rustfs-github.png)](https://rustfs.com)

# RustFS Policy Engine

<p align="center">
  <strong>Advanced policy-based access control engine for RustFS distributed object storage</strong>
</p>

<p align="center">
  <a href="https://github.com/rustfs/rustfs/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/rustfs/rustfs/actions/workflows/ci.yml/badge.svg" /></a>
  <a href="https://docs.rustfs.com/en/">📖 Documentation</a>
  · <a href="https://github.com/rustfs/rustfs/issues">🐛 Bug Reports</a>
  · <a href="https://github.com/rustfs/rustfs/discussions">💬 Discussions</a>
</p>

---

## 📖 Overview

**RustFS Policy Engine** is a sophisticated access control system for the [RustFS](https://rustfs.com) distributed object storage platform. It provides fine-grained, attribute-based access control (ABAC) with support for complex policy expressions, dynamic evaluation, and AWS IAM-compatible policy syntax.

> **Note:** This is a core submodule of RustFS that provides essential access control and authorization capabilities for the distributed object storage system. For the complete RustFS experience, please visit the [main RustFS repository](https://github.com/rustfs/rustfs).

## ✨ Features

### 🔐 Access Control

- **AWS IAM Compatible**: Full support for AWS IAM policy syntax
- **Fine-Grained Permissions**: Resource-level and action-level access control
- **Dynamic Policy Evaluation**: Real-time policy evaluation with context
- **Conditional Access**: Support for complex conditional expressions

### 📜 Policy Management

- **Policy Documents**: Structured policy definition and management
- **Policy Versioning**: Version control for policy documents
- **Policy Validation**: Syntax and semantic validation
- **Policy Templates**: Pre-built policy templates for common use cases

### 🎯 Advanced Features

- **Attribute-Based Access Control (ABAC)**: Context-aware access decisions
- **Function-Based Conditions**: Rich set of condition functions
- **Principal-Based Policies**: User, group, and service account policies
- **Resource-Based Policies**: Bucket and object-level policies

### 🛠️ Integration Features

- **ARN Support**: AWS-style Amazon Resource Names
- **Multi-Tenant Support**: Isolated policy evaluation per tenant
- **Real-Time Evaluation**: High-performance policy evaluation engine
- **Audit Trail**: Comprehensive policy evaluation logging

## 🏗️ Architecture

### Policy Engine Architecture

```
Policy Engine Architecture:
┌─────────────────────────────────────────────────────────────┐
│                    Policy API Layer                         │
├─────────────────────────────────────────────────────────────┤
│   Policy Parser   │   Policy Validator   │   Policy Store   │
├─────────────────────────────────────────────────────────────┤
│              Policy Evaluation Engine                        │
├─────────────────────────────────────────────────────────────┤
│  Condition Functions  │  Principal Resolver  │  Resource Mgr │
├─────────────────────────────────────────────────────────────┤
│              Authentication Integration                      │
└─────────────────────────────────────────────────────────────┘
```

### Policy Decision Flow

```
Policy Decision Flow:
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Request   │───▶│   Policy    │───▶│   Decision  │
│  (Subject,  │    │  Evaluation │    │  (Allow/    │
│   Action,   │    │   Engine    │    │   Deny/     │
│  Resource)  │    │             │    │  Not Found) │
└─────────────┘    └─────────────┘    └─────────────┘
       │                   │                   │
       ▼                   ▼                   ▼
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  Context    │    │  Condition  │    │   Audit     │
│ Information │    │ Functions   │    │    Log      │
└─────────────┘    └─────────────┘    └─────────────┘
```

## 📦 Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
rustfs-policy = "0.1.0"
```

## 🔧 Usage

### Basic Policy Creation

```rust
use rustfs_policy::policy::{Policy, Statement, Effect, Action, Resource};
use rustfs_policy::arn::ARN;
use serde_json::json;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a simple policy
    let policy = Policy::new(
        "2012-10-17".to_string(),
        vec![
            Statement::new(
                Effect::Allow,
                vec![Action::from_str("s3:GetObject")?],
                vec![Resource::from_str("arn:aws:s3:::my-bucket/*")?],
                None, // No conditions
            ),
        ],
    );

    // Serialize to JSON
    let policy_json = serde_json::to_string_pretty(&policy)?;
    println!("Policy JSON:\n{}", policy_json);

    Ok(())
}
```

### Advanced Policy with Conditions

```rust
use rustfs_policy::policy::{Policy, Statement, Effect, Action, Resource};
use rustfs_policy::policy::function::{Function, FunctionName};
use serde_json::json;

fn create_conditional_policy() -> Result<Policy, Box<dyn std::error::Error>> {
    // Create a policy with IP address restrictions
    let policy = Policy::new(
        "2012-10-17".to_string(),
        vec![
            Statement::builder()
                .effect(Effect::Allow)
                .action(Action::from_str("s3:GetObject")?)
                .resource(Resource::from_str("arn:aws:s3:::secure-bucket/*")?)
                .condition(
                    "IpAddress",
                    "aws:SourceIp",
                    json!(["192.168.1.0/24", "10.0.0.0/8"])
                )
                .build(),
            Statement::builder()
                .effect(Effect::Deny)
                .action(Action::from_str("s3:*")?)
                .resource(Resource::from_str("arn:aws:s3:::secure-bucket/*")?)
                .condition(
                    "DateGreaterThan",
                    "aws:CurrentTime",
                    json!("2024-12-31T23:59:59Z")
                )
                .build(),
        ],
    );

    Ok(policy)
}
```

### Policy Evaluation

```rust
use rustfs_policy::policy::{Policy, PolicyDoc};
use rustfs_policy::auth::{Identity, Request};
use rustfs_policy::arn::ARN;

async fn evaluate_policy_example() -> Result<(), Box<dyn std::error::Error>> {
    // Load policy from storage
    let policy_doc = PolicyDoc::try_from(policy_bytes)?;

    // Create evaluation context
    let identity = Identity::new(
        "user123".to_string(),
        vec!["group1".to_string(), "group2".to_string()],
    );

    let request = Request::new(
        "s3:GetObject".to_string(),
        ARN::from_str("arn:aws:s3:::my-bucket/file.txt")?,
        Some(json!({
            "aws:SourceIp": "192.168.1.100",
            "aws:CurrentTime": "2024-01-15T10:30:00Z"
        })),
    );

    // Evaluate policy
    let result = policy_doc.policy.evaluate(&identity, &request).await?;

    match result {
        Effect::Allow => println!("Access allowed"),
        Effect::Deny => println!("Access denied"),
        Effect::NotEvaluated => println!("No applicable policy found"),
    }

    Ok(())
}
```

### Policy Templates

```rust
use rustfs_policy::policy::{Policy, Statement, Effect};
use rustfs_policy::templates::PolicyTemplate;

fn create_common_policies() -> Result<(), Box<dyn std::error::Error>> {
    // Read-only policy template
    let read_only_policy = PolicyTemplate::read_only_bucket("my-bucket")?;

    // Full access policy template
    let full_access_policy = PolicyTemplate::full_access_bucket("my-bucket")?;

    // Admin policy template
    let admin_policy = PolicyTemplate::admin_all_resources()?;

    // Custom policy with multiple permissions
    let custom_policy = Policy::builder()
        .version("2012-10-17")
        .statement(
            Statement::builder()
                .effect(Effect::Allow)
                .action("s3:GetObject")
                .action("s3:PutObject")
                .resource("arn:aws:s3:::uploads/*")
                .condition("StringEquals", "s3:x-amz-acl", "bucket-owner-full-control")
                .build()
        )
        .statement(
            Statement::builder()
                .effect(Effect::Allow)
                .action("s3:ListBucket")
                .resource("arn:aws:s3:::uploads")
                .condition("StringLike", "s3:prefix", "user/${aws:username}/*")
                .build()
        )
        .build();

    Ok(())
}
```

### Resource-Based Policies

```rust
use rustfs_policy::policy::{Policy, Statement, Effect, Principal};
use rustfs_policy::arn::ARN;

fn create_resource_policy() -> Result<Policy, Box<dyn std::error::Error>> {
    // Create a bucket policy allowing cross-account access
    let bucket_policy = Policy::builder()
        .version("2012-10-17")
        .statement(
            Statement::builder()
                .effect(Effect::Allow)
                .principal(Principal::AWS("arn:aws:iam::123456789012:root".to_string()))
                .action("s3:GetObject")
                .resource("arn:aws:s3:::shared-bucket/*")
                .condition("StringEquals", "s3:ExistingObjectTag/Department", "Finance")
                .build()
        )
        .statement(
            Statement::builder()
                .effect(Effect::Allow)
                .principal(Principal::AWS("arn:aws:iam::123456789012:user/john".to_string()))
                .action("s3:PutObject")
                .resource("arn:aws:s3:::shared-bucket/uploads/*")
                .condition("StringEquals", "s3:x-amz-acl", "bucket-owner-full-control")
                .build()
        )
        .build();

    Ok(bucket_policy)
}
```

## 🧪 Testing

Run the test suite:

```bash
# Run all tests
cargo test

# Run policy evaluation tests
cargo test policy_evaluation

# Run condition function tests
cargo test condition_functions

# Run ARN parsing tests
cargo test arn_parsing

# Run policy validation tests
cargo test policy_validation

# Run with test coverage
cargo test --features test-coverage
```

## 📋 Policy Syntax

### Basic Policy Structure

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject"
      ],
      "Resource": [
        "arn:aws:s3:::my-bucket/*"
      ],
      "Condition": {
        "StringEquals": {
          "s3:x-amz-acl": "bucket-owner-full-control"
        }
      }
    }
  ]
}
```

### Supported Actions

| Action Category | Actions | Description |
|----------------|---------|-------------|
| Object Operations | `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject` | Object-level operations |
| Bucket Operations | `s3:ListBucket`, `s3:CreateBucket`, `s3:DeleteBucket` | Bucket-level operations |
| Access Control | `s3:GetBucketAcl`, `s3:PutBucketAcl` | Access control operations |
| Versioning | `s3:GetObjectVersion`, `s3:DeleteObjectVersion` | Object versioning operations |
| Multipart Upload | `s3:ListMultipartUploads`, `s3:AbortMultipartUpload` | Multipart upload operations |

### Condition Functions

| Function | Description | Example |
|----------|-------------|---------|
| `StringEquals` | Exact string matching | `"StringEquals": {"s3:x-amz-acl": "private"}` |
| `StringLike` | Wildcard string matching | `"StringLike": {"s3:prefix": "photos/*"}` |
| `IpAddress` | IP address/CIDR matching | `"IpAddress": {"aws:SourceIp": "192.168.1.0/24"}` |
| `DateGreaterThan` | Date comparison | `"DateGreaterThan": {"aws:CurrentTime": "2024-01-01T00:00:00Z"}` |
| `NumericEquals` | Numeric comparison | `"NumericEquals": {"s3:max-keys": "100"}` |
| `Bool` | Boolean comparison | `"Bool": {"aws:SecureTransport": "true"}` |

## 🔧 Configuration

### Policy Engine Configuration

```toml
[policy]
# Policy evaluation settings
max_policy_size = 2048  # Maximum policy size in KB
max_conditions_per_statement = 10
max_statements_per_policy = 100

# Performance settings
cache_policy_documents = true
cache_ttl_seconds = 300
max_cached_policies = 1000

# Security settings
require_secure_transport = true
allow_anonymous_access = false
default_effect = "deny"

# Audit settings
audit_policy_evaluation = true
audit_log_path = "/var/log/rustfs/policy-audit.log"
```

### Advanced Configuration

```rust
use rustfs_policy::config::PolicyConfig;

let config = PolicyConfig {
    // Policy parsing settings
    max_policy_size_kb: 2048,
    max_conditions_per_statement: 10,
    max_statements_per_policy: 100,

    // Evaluation settings
    default_effect: Effect::Deny,
    require_explicit_deny: false,
    cache_policy_documents: true,
    cache_ttl_seconds: 300,

    // Security settings
    require_secure_transport: true,
    allow_anonymous_access: false,
    validate_resource_arns: true,

    // Performance settings
    max_cached_policies: 1000,
    evaluation_timeout_ms: 100,

    ..Default::default()
};
```

## 🚀 Performance Optimization

### Caching Strategy

- **Policy Document Cache**: Cache parsed policy documents
- **Evaluation Result Cache**: Cache evaluation results for identical requests
- **Condition Cache**: Cache condition function results
- **Principal Cache**: Cache principal resolution results

### Best Practices

1. **Minimize Policy Size**: Keep policies as small as possible
2. **Use Specific Actions**: Avoid overly broad action patterns
3. **Optimize Conditions**: Use efficient condition functions
4. **Cache Frequently Used Policies**: Enable policy caching for better performance

## 🤝 Integration with RustFS

The Policy Engine integrates seamlessly with other RustFS components:

- **IAM Module**: Provides policy storage and user/group management
- **ECStore**: Implements resource-based access control
- **API Server**: Enforces policies on S3 API operations
- **Audit System**: Logs policy evaluation decisions
- **Admin Interface**: Manages policy documents and templates

## 📋 Requirements

- **Rust**: 1.70.0 or later
- **Platforms**: Linux, macOS, Windows
- **Memory**: Minimum 1GB RAM for policy caching
- **Storage**: Compatible with RustFS storage backend

## 🐛 Troubleshooting

### Common Issues

1. **Policy Parse Errors**:
   - Check JSON syntax validity
   - Verify action and resource ARN formats
   - Validate condition function syntax

2. **Policy Evaluation Failures**:
   - Check principal resolution
   - Verify resource ARN matching
   - Debug condition function evaluation

3. **Performance Issues**:
   - Monitor policy cache hit rates
   - Check policy document sizes
   - Optimize condition functions

### Debug Commands

```bash
# Validate policy syntax
rustfs-cli policy validate --file policy.json

# Test policy evaluation
rustfs-cli policy test --policy policy.json --user john --action s3:GetObject --resource arn:aws:s3:::bucket/key

# Show policy evaluation trace
rustfs-cli policy trace --policy policy.json --user john --action s3:GetObject --resource arn:aws:s3:::bucket/key
```

## 🌍 Related Projects

This module is part of the RustFS ecosystem:

- [RustFS Main](https://github.com/rustfs/rustfs) - Core distributed storage system
- [RustFS IAM](../iam) - Identity and access management
- [RustFS ECStore](../ecstore) - Erasure coding storage engine
- [RustFS Crypto](../crypto) - Cryptographic operations
- [RustFS Utils](../utils) - Utility functions

## 📚 Documentation

For comprehensive documentation, visit:

- [RustFS Documentation](https://docs.rustfs.com)
- [Policy Engine API Reference](https://docs.rustfs.com/policy/)
- [Policy Language Guide](https://docs.rustfs.com/policy-language/)
- [Access Control Guide](https://docs.rustfs.com/access-control/)

## 🔗 Links

- [Documentation](https://docs.rustfs.com) - Complete RustFS manual
- [Changelog](https://github.com/rustfs/rustfs/releases) - Release notes and updates
- [GitHub Discussions](https://github.com/rustfs/rustfs/discussions) - Community support

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](https://github.com/rustfs/rustfs/blob/main/CONTRIBUTING.md) for details on:

- Policy engine architecture and design patterns
- Policy language syntax and semantics
- Condition function implementation
- Performance optimization techniques
- Security considerations for access control

### Development Setup

```bash
# Clone the repository
git clone https://github.com/rustfs/rustfs.git
cd rustfs

# Navigate to Policy module
cd crates/policy

# Install dependencies
cargo build

# Run tests
cargo test

# Run policy validation tests
cargo test policy_validation

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

This module is maintained by the RustFS security team and community contributors. Special thanks to all who have contributed to making RustFS access control robust and flexible.

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
  Made with 🛡️ by the RustFS Security Team
</p>
