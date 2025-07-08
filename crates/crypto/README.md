# RustFS Crypto Module

**RustFS Crypto Module** is a core cryptographic component of the [RustFS](https://rustfs.com) distributed object storage system. This module provides secure, high-performance encryption and decryption capabilities, JWT token management, and cross-platform cryptographic operations designed specifically for enterprise-grade storage systems.

## Features

- **Multiple Algorithms**: Support for AES-GCM, ChaCha20Poly1305, and PBKDF2
- **JWT Management**: Secure JWT token creation and validation
- **Memory Safety**: Built with Rust's memory safety guarantees
- **Cross-Platform**: Optimized for x86_64, aarch64, s390x, and other architectures
- **Hardware Acceleration**: Automatic detection and utilization of CPU crypto extensions
- **Zero-Copy Operations**: Efficient memory usage with `Bytes` support

## Documentation

For complete documentation, examples, and usage information, please visit the main [RustFS repository](https://github.com/rustfs/rustfs).

## License

This project is licensed under the Apache License, Version 2.0.
