# RustFS Documentation Center

Welcome to the RustFS distributed file system documentation center!

## 📚 Documentation Navigation

### ⚡ Performance Optimization

RustFS provides intelligent performance optimization features for different workloads.

| Document | Description | Audience |
|------|------|----------|
| [Adaptive Buffer Sizing](./adaptive-buffer-sizing.md) | Intelligent buffer sizing optimization for optimal performance across workload types | Developers and system administrators |
| [Phase 3 Migration Guide](./MIGRATION_PHASE3.md) | Migration guide from Phase 2 to Phase 3 (Default Enablement) | Operations and DevOps teams |
| [Phase 4 Full Integration Guide](./PHASE4_GUIDE.md) | Complete guide to Phase 4 features: deprecated legacy functions, performance metrics | Advanced users and performance engineers |
| [Performance Testing Guide](./PERFORMANCE_TESTING.md) | Performance benchmarking and optimization guide | Performance engineers |

### 🔐 KMS (Key Management Service)

RustFS KMS delivers enterprise-grade key management and data encryption.

| Document | Description | Audience |
|------|------|----------|
| [KMS User Guide](./kms/README.md) | Comprehensive KMS guide with quick start, configuration, and deployment steps | Required reading for all users |
| [HTTP API Reference](./kms/http-api.md) | HTTP REST API reference with usage examples | Administrators and operators |
| [Programming API Reference](./kms/api.md) | Rust library APIs and code samples | Developers |
| [Configuration Reference](./kms/configuration.md) | Complete configuration options and environment variables | System administrators |
| [Troubleshooting](./kms/troubleshooting.md) | Diagnosis tips and solutions for common issues | Operations engineers |
| [Security Guide](./kms/security.md) | Security best practices and compliance guidance | Security architects |

## 🚀 Quick Start

### 1. Deploy KMS in 5 Minutes

**Production (Vault backend)**

```bash
# 1. Enable the Vault feature flag
cargo build --features vault --release

# 2. Configure environment variables
export RUSTFS_VAULT_ADDRESS=https://vault.company.com:8200
export RUSTFS_VAULT_TOKEN=hvs.CAESIJ...

# 3. Launch the service
./target/release/rustfs server
```

**Development & Testing (Local backend)**

```bash
# 1. Build a release binary
cargo build --release

# 2. Configure local storage
export RUSTFS_KMS_BACKEND=Local
export RUSTFS_KMS_LOCAL_KEY_DIR=/tmp/rustfs-keys

# 3. Launch the service
./target/release/rustfs server
```

### 2. S3-Compatible Encryption

```bash
# Upload an encrypted object
curl -X PUT https://rustfs.company.com/bucket/sensitive.txt \
  -H "x-amz-server-side-encryption: AES256" \
  --data-binary @sensitive.txt

# Download with automatic decryption
curl https://rustfs.company.com/bucket/sensitive.txt
```

## 🏗️ Architecture Overview

### Three-Layer KMS Security Architecture

```
┌─────────────────────────────────────────────────┐
│                  Application Layer              │
│  ┌─────────────┐    ┌─────────────┐             │
│  │    S3 API    │    │   REST API  │             │
│  └─────────────┘    └─────────────┘             │
├─────────────────────────────────────────────────┤
│                  Encryption Layer               │
│  ┌─────────────┐ Encrypt ┌─────────────────┐    │
│  │ Object Data │ ◄──────► │ Data Key (DEK) │    │
│  └─────────────┘          └─────────────────┘   │
├─────────────────────────────────────────────────┤
│                 Key Management Layer            │
│  ┌─────────────────┐ Encrypt ┌──────────────┐   │
│  │ Data Key (DEK) │ ◄───────│  Master Key   │   │
│  └─────────────────┘        │ (Vault/HSM)  │   │
│                             └──────────────┘   │
└─────────────────────────────────────────────────┘
```

### Key Features

- ✅ **Multi-layer encryption**: Master Key → DEK → Object Data
- ✅ **High performance**: 1 MB streaming encryption with large file support
- ✅ **Multiple backends**: Vault (production) + Local (testing)
- ✅ **S3 compatibility**: Supports standard SSE-S3/SSE-KMS headers
- ✅ **Enterprise-ready**: Auditing, monitoring, and compliance features

## 📖 Learning Paths

### 👨‍💻 Developers

1. Read the [Programming API Reference](./kms/api.md) to learn the Rust library
2. Review the sample code to understand integration patterns
3. Consult [Troubleshooting](./kms/troubleshooting.md) when issues occur

### 👨‍💼 System Administrators

1. Start with the [KMS User Guide](./kms/README.md)
2. Learn the [HTTP API Reference](./kms/http-api.md) for management tasks
3. Study the [Configuration Reference](./kms/configuration.md) in depth
4. Configure monitoring and logging

### 👨‍🔧 Operations Engineers

1. Become familiar with the [HTTP API Reference](./kms/http-api.md) for day-to-day work
2. Master the [Troubleshooting](./kms/troubleshooting.md) procedures
3. Understand the requirements in the [Security Guide](./kms/security.md)
4. Establish operational runbooks

### 🔒 Security Architects

1. Dive into the [Security Guide](./kms/security.md)
2. Evaluate threat models and risk posture
3. Define security policies

## 🤝 Contribution Guide

We welcome community contributions!

### Documentation Contributions

```bash
# 1. Fork the repository
git clone https://github.com/your-username/rustfs.git

# 2. Create a documentation branch
git checkout -b docs/improve-kms-guide

# 3. Edit the documentation
# Update Markdown files under docs/kms/

# 4. Commit the changes
git add docs/
git commit -m "docs: improve KMS configuration examples"

# 5. Open a Pull Request
gh pr create --title "Improve KMS documentation"
```

### Documentation Guidelines

- Use clear headings and structure
- Provide runnable code examples
- Include warnings and tips where appropriate
- Support multiple usage scenarios
- Keep the content up to date

## 📞 Support & Feedback

### Getting Help

- **GitHub Issues**: https://github.com/rustfs/rustfs/issues
- **Discussion Forum**: https://github.com/rustfs/rustfs/discussions
- **Documentation Questions**: Open an issue on the relevant document
- **Security Concerns**: security@rustfs.com

### Issue Reporting Template

When reporting a problem, please provide:

```markdown
**Environment**
- RustFS version: v1.0.0
- Operating system: Ubuntu 20.04
- Rust version: 1.75.0

**Issue Description**
Summarize the problem you encountered...

**Reproduction Steps**
1. Step one
2. Step two
3. Step three

**Expected Behavior**
Describe what you expected to happen...

**Actual Behavior**
Describe what actually happened...

**Relevant Logs**
```bash
# Paste relevant log excerpts
```

**Additional Information**
Any other details that may help...
```

## 📈 Release History

| Version | Release Date | Highlights |
|------|----------|----------|
| v1.0.0 | 2024-01-15 | 🎉 First official release with full KMS functionality |
| v0.9.0 | 2024-01-01 | 🔐 KMS system refactor with performance optimizations |
| v0.8.0 | 2023-12-15 | ⚡ Streaming encryption with 1 MB block size tuning |

## 🗺️ Roadmap

### Coming Soon (v1.1.0)

- [ ] Automatic key rotation
- [ ] HSM integration support
- [ ] Web UI management console
- [ ] Additional compliance support (SOC2, HIPAA)

### Long-Term Plans

- [ ] Multi-tenant key isolation
- [ ] Key import/export tooling
- [ ] Performance benchmarking suite
- [ ] Kubernetes Operator

## 📋 Documentation Feedback

Help us improve the documentation!

**Was this documentation helpful?**
- 👍 Very helpful
- 👌 Mostly satisfied
- 👎 Needs improvement

**Suggestions for improvement:**
Share specific ideas via GitHub Issues.

---

**Last Updated**: 2024-01-15
**Documentation Version**: v1.0.0

*Thank you for using RustFS! We are committed to delivering the best distributed file system solution.*
