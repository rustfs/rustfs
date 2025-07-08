[![RustFS](https://rustfs.com/images/rustfs-github.png)](https://rustfs.com)

# RustFS Signer - Request Signing & Authentication

<p align="center">
  <strong>AWS-compatible request signing and authentication for RustFS object storage</strong>
</p>

<p align="center">
  <a href="https://github.com/rustfs/rustfs/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/rustfs/rustfs/actions/workflows/ci.yml/badge.svg" /></a>
  <a href="https://docs.rustfs.com/en/">📖 Documentation</a>
  · <a href="https://github.com/rustfs/rustfs/issues">🐛 Bug Reports</a>
  · <a href="https://github.com/rustfs/rustfs/discussions">💬 Discussions</a>
</p>

---

## 📖 Overview

**RustFS Signer** provides AWS-compatible request signing and authentication for the [RustFS](https://rustfs.com) distributed object storage system. It implements AWS Signature Version 4 (SigV4) signing algorithm, pre-signed URLs, and various authentication methods to ensure secure API access.

> **Note:** This is a security-critical submodule of RustFS that provides essential authentication capabilities for the distributed object storage system. For the complete RustFS experience, please visit the [main RustFS repository](https://github.com/rustfs/rustfs).

## ✨ Features

### 🔐 AWS-Compatible Signing

- **SigV4 Implementation**: Full AWS Signature Version 4 support
- **Pre-signed URLs**: Temporary access URLs with expiration
- **Chunked Upload**: Streaming upload with signature validation
- **Multi-part Upload**: Signature validation for large files

### 🛡️ Authentication Methods

- **Access Key/Secret**: Traditional AWS-style authentication
- **STS Token**: Temporary security token support
- **IAM Role**: Role-based authentication
- **Anonymous Access**: Public read access support

### 🚀 Performance Features

- **Signature Caching**: Avoid repeated signature calculations
- **Batch Signing**: Sign multiple requests efficiently
- **Streaming Support**: Sign data streams without buffering
- **Hardware Acceleration**: Use hardware crypto when available

### 🔧 Advanced Features

- **Custom Headers**: Support for custom and vendor headers
- **Regional Signing**: Multi-region signature support
- **Clock Skew Handling**: Automatic time synchronization
- **Signature Validation**: Server-side signature verification

## 📦 Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
rustfs-signer = "0.1.0"
```

## 🔧 Usage

### Basic Request Signing

```rust
use rustfs_signer::{Signer, SigningConfig, Credentials};
use http::{Request, Method};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create credentials
    let credentials = Credentials::new(
        "AKIAIOSFODNN7EXAMPLE".to_string(),
        "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
        None, // No session token
    );

    // Create signing configuration
    let config = SigningConfig {
        region: "us-east-1".to_string(),
        service: "s3".to_string(),
        credentials,
        ..Default::default()
    };

    // Create signer
    let signer = Signer::new(config);

    // Create request
    let request = Request::builder()
        .method(Method::GET)
        .uri("https://example-bucket.s3.amazonaws.com/example-object")
        .body(Vec::new())?;

    // Sign request
    let signed_request = signer.sign_request(request).await?;

    println!("Authorization header: {:?}", signed_request.headers().get("authorization"));

    Ok(())
}
```

### Pre-signed URLs

```rust
use rustfs_signer::{Signer, PresignedUrlRequest};
use std::time::Duration;

async fn presigned_url_example() -> Result<(), Box<dyn std::error::Error>> {
    let signer = Signer::new(signing_config);

    // Create pre-signed URL for GET request
    let presigned_request = PresignedUrlRequest {
        method: Method::GET,
        uri: "https://example-bucket.s3.amazonaws.com/example-object".parse()?,
        headers: Default::default(),
        expires_in: Duration::from_secs(3600), // 1 hour
    };

    let presigned_url = signer.presign_url(presigned_request).await?;
    println!("Pre-signed URL: {}", presigned_url);

    // Create pre-signed URL for PUT request
    let put_request = PresignedUrlRequest {
        method: Method::PUT,
        uri: "https://example-bucket.s3.amazonaws.com/upload-object".parse()?,
        headers: {
            let mut headers = HeaderMap::new();
            headers.insert("content-type", "text/plain".parse()?);
            headers
        },
        expires_in: Duration::from_secs(1800), // 30 minutes
    };

    let upload_url = signer.presign_url(put_request).await?;
    println!("Pre-signed upload URL: {}", upload_url);

    Ok(())
}
```

### Streaming Upload Signing

```rust
use rustfs_signer::{StreamingSigner, ChunkedUploadSigner};
use tokio::io::AsyncReadExt;

async fn streaming_upload_example() -> Result<(), Box<dyn std::error::Error>> {
    let signer = Signer::new(signing_config);

    // Create streaming signer
    let streaming_signer = StreamingSigner::new(signer, "s3".to_string());

    // Create chunked upload signer
    let mut chunked_signer = ChunkedUploadSigner::new(
        streaming_signer,
        "example-bucket".to_string(),
        "large-file.dat".to_string(),
    );

    // Initialize multipart upload
    let upload_id = chunked_signer.initiate_multipart_upload().await?;
    println!("Upload ID: {}", upload_id);

    // Upload chunks
    let mut file = tokio::fs::File::open("large-file.dat").await?;
    let mut chunk_buffer = vec![0u8; 5 * 1024 * 1024]; // 5MB chunks
    let mut part_number = 1;
    let mut etags = Vec::new();

    loop {
        let bytes_read = file.read(&mut chunk_buffer).await?;
        if bytes_read == 0 {
            break;
        }

        let chunk = &chunk_buffer[..bytes_read];
        let etag = chunked_signer.upload_part(part_number, chunk).await?;
        etags.push((part_number, etag));

        part_number += 1;
    }

    // Complete multipart upload
    chunked_signer.complete_multipart_upload(upload_id, etags).await?;
    println!("Upload completed successfully");

    Ok(())
}
```

### Signature Validation

```rust
use rustfs_signer::{SignatureValidator, ValidationResult};
use http::HeaderMap;

async fn signature_validation_example() -> Result<(), Box<dyn std::error::Error>> {
    let validator = SignatureValidator::new(signing_config);

    // Extract signature from request headers
    let headers = HeaderMap::new(); // Headers from incoming request
    let method = "GET";
    let uri = "/example-bucket/example-object";
    let body = b""; // Request body

    // Validate signature
    let validation_result = validator.validate_signature(
        method,
        uri,
        &headers,
        body,
    ).await?;

    match validation_result {
        ValidationResult::Valid { credentials, .. } => {
            println!("Signature valid for user: {}", credentials.access_key);
        }
        ValidationResult::Invalid { reason } => {
            println!("Signature invalid: {}", reason);
        }
        ValidationResult::Expired { expired_at } => {
            println!("Signature expired at: {}", expired_at);
        }
    }

    Ok(())
}
```

### Batch Signing

```rust
use rustfs_signer::{BatchSigner, BatchSigningRequest};

async fn batch_signing_example() -> Result<(), Box<dyn std::error::Error>> {
    let signer = Signer::new(signing_config);
    let batch_signer = BatchSigner::new(signer);

    // Create multiple requests
    let requests = vec![
        BatchSigningRequest {
            method: Method::GET,
            uri: "https://bucket1.s3.amazonaws.com/object1".parse()?,
            headers: HeaderMap::new(),
            body: Vec::new(),
        },
        BatchSigningRequest {
            method: Method::PUT,
            uri: "https://bucket2.s3.amazonaws.com/object2".parse()?,
            headers: HeaderMap::new(),
            body: b"Hello, World!".to_vec(),
        },
    ];

    // Sign all requests in batch
    let signed_requests = batch_signer.sign_batch(requests).await?;

    for (i, signed_request) in signed_requests.iter().enumerate() {
        println!("Request {}: {:?}", i + 1, signed_request.headers().get("authorization"));
    }

    Ok(())
}
```

### Custom Authentication

```rust
use rustfs_signer::{CustomAuthenticator, AuthenticationResult};
use async_trait::async_trait;

struct CustomAuth {
    // Custom authentication logic
}

#[async_trait]
impl CustomAuthenticator for CustomAuth {
    async fn authenticate(&self, request: &Request<Vec<u8>>) -> Result<AuthenticationResult, Box<dyn std::error::Error>> {
        // Custom authentication logic
        let auth_header = request.headers().get("authorization");

        if let Some(auth) = auth_header {
            // Parse and validate custom authentication
            let auth_str = auth.to_str()?;

            if auth_str.starts_with("Custom ") {
                // Validate custom token
                let token = &auth_str[7..];
                if self.validate_token(token).await? {
                    return Ok(AuthenticationResult::Authenticated {
                        user_id: "custom-user".to_string(),
                        permissions: vec!["read".to_string(), "write".to_string()],
                    });
                }
            }
        }

        Ok(AuthenticationResult::Unauthenticated)
    }
}

impl CustomAuth {
    async fn validate_token(&self, token: &str) -> Result<bool, Box<dyn std::error::Error>> {
        // Implement token validation logic
        Ok(token.len() > 10) // Simple example
    }
}
```

## 🏗️ Architecture

### Signer Architecture

```
Signer Architecture:
┌─────────────────────────────────────────────────────────────┐
│                    Signing API Layer                        │
├─────────────────────────────────────────────────────────────┤
│   SigV4      │   Pre-signed  │   Streaming  │   Batch      │
├─────────────────────────────────────────────────────────────┤
│              Signature Calculation                          │
├─────────────────────────────────────────────────────────────┤
│   HMAC-SHA256  │   Canonicalization │   String to Sign     │
├─────────────────────────────────────────────────────────────┤
│              Cryptographic Primitives                       │
└─────────────────────────────────────────────────────────────┘
```

### Signing Process

| Step | Description | Purpose |
|------|-------------|---------|
| 1. Canonicalize | Format request components | Consistent representation |
| 2. Create String to Sign | Combine canonicalized data | Prepare for signing |
| 3. Calculate Signature | HMAC-SHA256 computation | Generate signature |
| 4. Add Headers | Add signature to request | Complete authentication |

## 🧪 Testing

Run the test suite:

```bash
# Run all tests
cargo test

# Test signature generation
cargo test signature

# Test pre-signed URLs
cargo test presigned

# Test validation
cargo test validation
```

## 📋 Requirements

- **Rust**: 1.70.0 or later
- **Platforms**: Linux, macOS, Windows
- **Dependencies**: Cryptographic libraries (ring, rustls)
- **Compatibility**: AWS S3 API compatible

## 🌍 Related Projects

This module is part of the RustFS ecosystem:

- [RustFS Main](https://github.com/rustfs/rustfs) - Core distributed storage system
- [RustFS IAM](../iam) - Identity and access management
- [RustFS Crypto](../crypto) - Cryptographic operations

## 📚 Documentation

For comprehensive documentation, visit:

- [RustFS Documentation](https://docs.rustfs.com)
- [Signer API Reference](https://docs.rustfs.com/signer/)
- [AWS S3 Compatibility](https://docs.rustfs.com/s3-compatibility/)

## 🔗 Links

- [Documentation](https://docs.rustfs.com) - Complete RustFS manual
- [Changelog](https://github.com/rustfs/rustfs/releases) - Release notes and updates
- [GitHub Discussions](https://github.com/rustfs/rustfs/discussions) - Community support

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](https://github.com/rustfs/rustfs/blob/main/CONTRIBUTING.md) for details.

## 📄 License

Licensed under the Apache License, Version 2.0. See [LICENSE](https://github.com/rustfs/rustfs/blob/main/LICENSE) for details.

---

<p align="center">
  <strong>RustFS</strong> is a trademark of RustFS, Inc.<br>
  All other trademarks are the property of their respective owners.
</p>

<p align="center">
  Made with 🔐 by the RustFS Team
</p>
