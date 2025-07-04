[![RustFS](https://rustfs.com/images/rustfs-github.png)](https://rustfs.com)

# RustFS Crypto Module

<p align="center">
  <strong>High-performance cryptographic module for RustFS distributed object storage</strong>
</p>

<p align="center">
  <a href="https://github.com/rustfs/rustfs/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/rustfs/rustfs/actions/workflows/ci.yml/badge.svg" /></a>
  <a href="https://docs.rustfs.com/en/">📖 Documentation</a>
  · <a href="https://github.com/rustfs/rustfs/issues">🐛 Bug Reports</a>
  · <a href="https://github.com/rustfs/rustfs/discussions">💬 Discussions</a>
</p>

---

## 📖 Overview

The **RustFS Crypto Module** is a core cryptographic component of the [RustFS](https://rustfs.com) distributed object storage system. This module provides secure, high-performance encryption and decryption capabilities, JWT token management, and cross-platform cryptographic operations designed specifically for enterprise-grade storage systems.

> **Note:** This is a submodule of RustFS and is designed to work seamlessly within the RustFS ecosystem. For the complete RustFS experience, please visit the [main RustFS repository](https://github.com/rustfs/rustfs).

## ✨ Features

### 🔐 Encryption & Decryption

- **Multiple Algorithms**: Support for AES-GCM, ChaCha20Poly1305, and PBKDF2
- **Key Derivation**: Argon2id and PBKDF2 for secure key generation
- **Memory Safety**: Built with Rust's memory safety guarantees
- **Cross-Platform**: Optimized for x86_64, aarch64, s390x, and other architectures

### 🎫 JWT Management

- **Token Generation**: Secure JWT token creation with HS512 algorithm
- **Token Validation**: Robust JWT token verification and decoding
- **Claims Management**: Flexible claims handling with JSON support

### 🛡️ Security Features

- **FIPS Compliance**: Optional FIPS 140-2 compatible mode
- **Hardware Acceleration**: Automatic detection and utilization of CPU crypto extensions
- **Secure Random**: Cryptographically secure random number generation
- **Side-Channel Protection**: Resistant to timing attacks

### 🚀 Performance

- **Zero-Copy Operations**: Efficient memory usage with `Bytes` support
- **Async/Await**: Full async support for non-blocking operations
- **Hardware Optimization**: CPU-specific optimizations for better performance

## 📦 Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
rustfs-crypto = "0.1.0"
```

### Feature Flags

```toml
[dependencies]
rustfs-crypto = { version = "0.1.0", features = ["crypto", "fips"] }
```

Available features:

- `crypto` (default): Enable all cryptographic functions
- `fips`: Enable FIPS 140-2 compliance mode
- `default`: Includes both `crypto` and `fips`

## 🔧 Usage

### Basic Encryption/Decryption

```rust
use rustfs_crypto::{encrypt_data, decrypt_data};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let password = b"my_secure_password";
    let data = b"sensitive information";

    // Encrypt data
    let encrypted = encrypt_data(password, data)?;
    println!("Encrypted {} bytes", encrypted.len());

    // Decrypt data
    let decrypted = decrypt_data(password, &encrypted)?;
    assert_eq!(data, decrypted.as_slice());
    println!("Successfully decrypted data");

    Ok(())
}
```

### JWT Token Management

```rust
use rustfs_crypto::{jwt_encode, jwt_decode};
use serde_json::json;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let secret = b"jwt_secret_key";
    let claims = json!({
        "sub": "user123",
        "exp": 1234567890,
        "iat": 1234567890
    });

    // Create JWT token
    let token = jwt_encode(secret, &claims)?;
    println!("Generated token: {}", token);

    // Verify and decode token
    let decoded = jwt_decode(&token, secret)?;
    println!("Decoded claims: {:?}", decoded.claims);

    Ok(())
}
```

### Advanced Usage with Custom Configuration

```rust
use rustfs_crypto::{encrypt_data, decrypt_data, Error};

#[cfg(feature = "crypto")]
fn secure_storage_example() -> Result<(), Error> {
    // Large data encryption
    let large_data = vec![0u8; 1024 * 1024]; // 1MB
    let password = b"complex_password_123!@#";

    // Encrypt with automatic algorithm selection
    let encrypted = encrypt_data(password, &large_data)?;

    // Decrypt and verify
    let decrypted = decrypt_data(password, &encrypted)?;
    assert_eq!(large_data.len(), decrypted.len());

    println!("Successfully processed {} bytes", large_data.len());
    Ok(())
}
```

## 🏗️ Architecture

### Supported Encryption Algorithms

| Algorithm | Key Derivation | Use Case | FIPS Compliant |
|-----------|---------------|----------|----------------|
| AES-GCM | Argon2id | General purpose, hardware accelerated | ✅ |
| ChaCha20Poly1305 | Argon2id | Software-only environments | ❌ |
| AES-GCM | PBKDF2 | FIPS compliance required | ✅ |

### Cross-Platform Support

The module automatically detects and optimizes for:

- **x86/x86_64**: AES-NI and PCLMULQDQ instructions
- **aarch64**: ARM Crypto Extensions
- **s390x**: IBM Z Crypto Extensions
- **Other architectures**: Fallback to software implementations

## 🧪 Testing

Run the test suite:

```bash
# Run all tests
cargo test

# Run tests with all features
cargo test --all-features

# Run benchmarks
cargo bench

# Test cross-platform compatibility
cargo test --target x86_64-unknown-linux-gnu
cargo test --target aarch64-unknown-linux-gnu
```

## 📊 Performance

The crypto module is designed for high-performance scenarios:

- **Encryption Speed**: Up to 2GB/s on modern hardware
- **Memory Usage**: Minimal heap allocation with zero-copy operations
- **CPU Utilization**: Automatic hardware acceleration detection
- **Scalability**: Thread-safe operations for concurrent access

## 🤝 Integration with RustFS

This module is specifically designed to integrate with other RustFS components:

- **Storage Layer**: Provides encryption for object storage
- **Authentication**: JWT tokens for API authentication
- **Configuration**: Secure configuration data encryption
- **Metadata**: Encrypted metadata storage

## 📋 Requirements

- **Rust**: 1.70.0 or later
- **Platforms**: Linux, macOS, Windows
- **Architectures**: x86_64, aarch64, s390x, and more

## 🔒 Security Considerations

- All cryptographic operations use industry-standard algorithms
- Key derivation follows best practices (Argon2id, PBKDF2)
- Memory is securely cleared after use
- Timing attack resistance is built-in
- Hardware security modules (HSM) support planned

## 🐛 Known Issues

- Hardware acceleration detection may not work on all virtualized environments
- FIPS mode requires additional system-level configuration
- Some older CPU architectures may have reduced performance

## 🌍 Related Projects

This module is part of the RustFS ecosystem:

- [RustFS Main](https://github.com/rustfs/rustfs) - Core distributed storage system
- [RustFS ECStore](../ecstore) - Erasure coding storage engine
- [RustFS IAM](../iam) - Identity and access management
- [RustFS Policy](../policy) - Policy engine

## 📚 Documentation

For comprehensive documentation, visit:

- [RustFS Documentation](https://docs.rustfs.com)
- [API Reference](https://docs.rustfs.com/crypto/)
- [Security Guide](https://docs.rustfs.com/security/)

## 🔗 Links

- [Documentation](https://docs.rustfs.com) - Complete RustFS manual
- [Changelog](https://github.com/rustfs/rustfs/releases) - Release notes and updates
- [GitHub Discussions](https://github.com/rustfs/rustfs/discussions) - Community support

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](https://github.com/rustfs/rustfs/blob/main/CONTRIBUTING.md) for details on:

- Code style and formatting requirements
- Testing procedures and coverage
- Security considerations for cryptographic code
- Pull request process and review guidelines

### Development Setup

```bash
# Clone the repository
git clone https://github.com/rustfs/rustfs.git
cd rustfs

# Navigate to crypto module
cd crates/crypto

# Install dependencies
cargo build

# Run tests
cargo test

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

This module is maintained by the RustFS team and community contributors. Special thanks to all who have contributed to making RustFS cryptography secure and efficient.

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
  Made with ❤️ by the RustFS Team
</p>
