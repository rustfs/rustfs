[![RustFS](https://rustfs.com/images/rustfs-github.png)](https://rustfs.com)

# RustFS AppAuth - Application Authentication

<p align="center">
  <strong>Secure application authentication and authorization for RustFS object storage</strong>
</p>

<p align="center">
  <a href="https://github.com/rustfs/rustfs/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/rustfs/rustfs/actions/workflows/ci.yml/badge.svg" /></a>
  <a href="https://docs.rustfs.com/en/">📖 Documentation</a>
  · <a href="https://github.com/rustfs/rustfs/issues">🐛 Bug Reports</a>
  · <a href="https://github.com/rustfs/rustfs/discussions">💬 Discussions</a>
</p>

---

## 📖 Overview

**RustFS AppAuth** provides secure application authentication and authorization mechanisms for the [RustFS](https://rustfs.com) distributed object storage system. It implements modern cryptographic standards including RSA-based authentication, JWT tokens, and secure session management for application-level access control.

> **Note:** This is a security-critical submodule of RustFS that provides essential application authentication capabilities for the distributed object storage system. For the complete RustFS experience, please visit the [main RustFS repository](https://github.com/rustfs/rustfs).

## ✨ Features

### 🔐 Authentication Methods

- **RSA Authentication**: Public-key cryptography for secure authentication
- **JWT Tokens**: JSON Web Token support for stateless authentication
- **API Keys**: Simple API key-based authentication
- **Session Management**: Secure session handling and lifecycle management

### 🛡️ Security Features

- **Cryptographic Signing**: RSA digital signatures for request validation
- **Token Encryption**: Encrypted token storage and transmission
- **Key Rotation**: Automatic key rotation and management
- **Audit Logging**: Comprehensive authentication event logging

### 🚀 Performance Features

- **Base64 Optimization**: High-performance base64 encoding/decoding
- **Token Caching**: Efficient token validation caching
- **Parallel Verification**: Concurrent authentication processing
- **Hardware Acceleration**: Leverage CPU crypto extensions

### 🔧 Integration Features

- **S3 Compatibility**: AWS S3-compatible authentication
- **Multi-Tenant**: Support for multiple application tenants
- **Permission Mapping**: Fine-grained permission assignment
- **External Integration**: LDAP, OAuth, and custom authentication providers

## 📦 Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
rustfs-appauth = "0.1.0"
```

## 🔧 Usage

### Basic Authentication Setup

```rust
use rustfs_appauth::{AppAuthenticator, AuthConfig, AuthMethod};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure authentication
    let config = AuthConfig {
        auth_method: AuthMethod::RSA,
        key_size: 2048,
        token_expiry: Duration::from_hours(24),
        enable_caching: true,
        audit_logging: true,
    };

    // Initialize authenticator
    let authenticator = AppAuthenticator::new(config).await?;

    // Generate application credentials
    let app_credentials = authenticator.generate_app_credentials("my-app").await?;

    println!("App ID: {}", app_credentials.app_id);
    println!("Public Key: {}", app_credentials.public_key);

    Ok(())
}
```

### RSA-Based Authentication

```rust
use rustfs_appauth::{RSAAuthenticator, AuthRequest, AuthResponse};

async fn rsa_authentication_example() -> Result<(), Box<dyn std::error::Error>> {
    // Create RSA authenticator
    let rsa_auth = RSAAuthenticator::new(2048).await?;

    // Generate key pair for application
    let (private_key, public_key) = rsa_auth.generate_keypair().await?;

    // Register application
    let app_id = rsa_auth.register_application("my-storage-app", &public_key).await?;
    println!("Application registered with ID: {}", app_id);

    // Create authentication request
    let auth_request = AuthRequest {
        app_id: app_id.clone(),
        timestamp: chrono::Utc::now(),
        request_data: b"GET /bucket/object".to_vec(),
    };

    // Sign request with private key
    let signed_request = rsa_auth.sign_request(&auth_request, &private_key).await?;

    // Verify authentication
    let auth_response = rsa_auth.authenticate(&signed_request).await?;

    match auth_response {
        AuthResponse::Success { session_token, permissions } => {
            println!("Authentication successful!");
            println!("Session token: {}", session_token);
            println!("Permissions: {:?}", permissions);
        }
        AuthResponse::Failed { reason } => {
            println!("Authentication failed: {}", reason);
        }
    }

    Ok(())
}
```

### JWT Token Management

```rust
use rustfs_appauth::{JWTManager, TokenClaims, TokenRequest};

async fn jwt_management_example() -> Result<(), Box<dyn std::error::Error>> {
    // Create JWT manager
    let jwt_manager = JWTManager::new("your-secret-key").await?;

    // Create token claims
    let claims = TokenClaims {
        app_id: "my-app".to_string(),
        user_id: Some("user123".to_string()),
        permissions: vec![
            "read:bucket".to_string(),
            "write:bucket".to_string(),
        ],
        expires_at: chrono::Utc::now() + chrono::Duration::hours(24),
        issued_at: chrono::Utc::now(),
    };

    // Generate JWT token
    let token = jwt_manager.generate_token(&claims).await?;
    println!("Generated token: {}", token);

    // Validate token
    let validation_result = jwt_manager.validate_token(&token).await?;

    match validation_result {
        Ok(validated_claims) => {
            println!("Token valid for app: {}", validated_claims.app_id);
            println!("Permissions: {:?}", validated_claims.permissions);
        }
        Err(e) => {
            println!("Token validation failed: {}", e);
        }
    }

    // Refresh token
    let refreshed_token = jwt_manager.refresh_token(&token).await?;
    println!("Refreshed token: {}", refreshed_token);

    Ok(())
}
```

### API Key Authentication

```rust
use rustfs_appauth::{APIKeyManager, APIKeyConfig, KeyPermissions};

async fn api_key_authentication() -> Result<(), Box<dyn std::error::Error>> {
    let api_key_manager = APIKeyManager::new().await?;

    // Create API key configuration
    let key_config = APIKeyConfig {
        app_name: "storage-client".to_string(),
        permissions: KeyPermissions {
            read_buckets: vec!["public-*".to_string()],
            write_buckets: vec!["uploads".to_string()],
            admin_access: false,
        },
        expires_at: Some(chrono::Utc::now() + chrono::Duration::days(90)),
        rate_limit: Some(1000), // requests per hour
    };

    // Generate API key
    let api_key = api_key_manager.generate_key(&key_config).await?;
    println!("Generated API key: {}", api_key.key);
    println!("Key ID: {}", api_key.key_id);

    // Authenticate with API key
    let auth_result = api_key_manager.authenticate(&api_key.key).await?;

    if auth_result.is_valid {
        println!("API key authentication successful");
        println!("Rate limit remaining: {}", auth_result.rate_limit_remaining);
    }

    // List API keys for application
    let keys = api_key_manager.list_keys("storage-client").await?;
    for key in keys {
        println!("Key: {} - Status: {} - Expires: {:?}",
            key.key_id, key.status, key.expires_at);
    }

    // Revoke API key
    api_key_manager.revoke_key(&api_key.key_id).await?;
    println!("API key revoked successfully");

    Ok(())
}
```

### Session Management

```rust
use rustfs_appauth::{SessionManager, SessionConfig, SessionInfo};

async fn session_management_example() -> Result<(), Box<dyn std::error::Error>> {
    // Configure session management
    let session_config = SessionConfig {
        session_timeout: Duration::from_hours(8),
        max_sessions_per_app: 10,
        require_refresh: true,
        secure_cookies: true,
    };

    let session_manager = SessionManager::new(session_config).await?;

    // Create new session
    let session_info = SessionInfo {
        app_id: "web-app".to_string(),
        user_id: Some("user456".to_string()),
        ip_address: "192.168.1.100".to_string(),
        user_agent: "RustFS-Client/1.0".to_string(),
    };

    let session = session_manager.create_session(&session_info).await?;
    println!("Session created: {}", session.session_id);

    // Validate session
    let validation = session_manager.validate_session(&session.session_id).await?;

    if validation.is_valid {
        println!("Session is valid, expires at: {}", validation.expires_at);
    }

    // Refresh session
    session_manager.refresh_session(&session.session_id).await?;
    println!("Session refreshed");

    // Get active sessions
    let active_sessions = session_manager.get_active_sessions("web-app").await?;
    println!("Active sessions: {}", active_sessions.len());

    // Terminate session
    session_manager.terminate_session(&session.session_id).await?;
    println!("Session terminated");

    Ok(())
}
```

### Multi-Tenant Authentication

```rust
use rustfs_appauth::{MultiTenantAuth, TenantConfig, TenantPermissions};

async fn multi_tenant_auth_example() -> Result<(), Box<dyn std::error::Error>> {
    let multi_tenant_auth = MultiTenantAuth::new().await?;

    // Create tenant configurations
    let tenant1_config = TenantConfig {
        tenant_id: "company-a".to_string(),
        name: "Company A".to_string(),
        permissions: TenantPermissions {
            max_buckets: 100,
            max_storage_gb: 1000,
            allowed_regions: vec!["us-east-1".to_string(), "us-west-2".to_string()],
        },
        auth_methods: vec![AuthMethod::RSA, AuthMethod::JWT],
    };

    let tenant2_config = TenantConfig {
        tenant_id: "company-b".to_string(),
        name: "Company B".to_string(),
        permissions: TenantPermissions {
            max_buckets: 50,
            max_storage_gb: 500,
            allowed_regions: vec!["eu-west-1".to_string()],
        },
        auth_methods: vec![AuthMethod::APIKey],
    };

    // Register tenants
    multi_tenant_auth.register_tenant(&tenant1_config).await?;
    multi_tenant_auth.register_tenant(&tenant2_config).await?;

    // Authenticate application for specific tenant
    let auth_request = TenantAuthRequest {
        tenant_id: "company-a".to_string(),
        app_id: "app-1".to_string(),
        credentials: AuthCredentials::RSA {
            signature: "signed-data".to_string(),
            public_key: "public-key-data".to_string(),
        },
    };

    let auth_result = multi_tenant_auth.authenticate(&auth_request).await?;

    if auth_result.is_authenticated {
        println!("Multi-tenant authentication successful");
        println!("Tenant: {}", auth_result.tenant_id);
        println!("Permissions: {:?}", auth_result.permissions);
    }

    Ok(())
}
```

### Authentication Middleware

```rust
use rustfs_appauth::{AuthMiddleware, AuthContext, MiddlewareConfig};
use axum::{Router, middleware, Extension};

async fn setup_auth_middleware() -> Result<Router, Box<dyn std::error::Error>> {
    // Configure authentication middleware
    let middleware_config = MiddlewareConfig {
        skip_paths: vec!["/health".to_string(), "/metrics".to_string()],
        require_auth: true,
        audit_requests: true,
    };

    let auth_middleware = AuthMiddleware::new(middleware_config).await?;

    // Create router with authentication middleware
    let app = Router::new()
        .route("/api/buckets", axum::routing::get(list_buckets))
        .route("/api/objects", axum::routing::post(upload_object))
        .layer(middleware::from_fn(auth_middleware.authenticate))
        .layer(Extension(auth_middleware));

    Ok(app)
}

async fn list_buckets(
    Extension(auth_context): Extension<AuthContext>,
) -> Result<String, Box<dyn std::error::Error>> {
    // Use authentication context
    println!("Authenticated app: {}", auth_context.app_id);
    println!("Permissions: {:?}", auth_context.permissions);

    // Your bucket listing logic here
    Ok("Bucket list".to_string())
}
```

## 🏗️ Architecture

### AppAuth Architecture

```
AppAuth Architecture:
┌─────────────────────────────────────────────────────────────┐
│                    Authentication API                       │
├─────────────────────────────────────────────────────────────┤
│   RSA Auth    │   JWT Tokens  │   API Keys   │   Sessions   │
├─────────────────────────────────────────────────────────────┤
│              Cryptographic Operations                       │
├─────────────────────────────────────────────────────────────┤
│  Signing/     │   Token       │   Key        │   Session    │
│  Verification │   Management  │   Management │   Storage    │
├─────────────────────────────────────────────────────────────┤
│              Security Infrastructure                        │
└─────────────────────────────────────────────────────────────┘
```

### Authentication Methods

| Method | Security Level | Use Case | Performance |
|--------|----------------|----------|-------------|
| RSA | High | Enterprise applications | Medium |
| JWT | Medium-High | Web applications | High |
| API Key | Medium | Service-to-service | Very High |
| Session | Medium | Interactive applications | High |

## 🧪 Testing

Run the test suite:

```bash
# Run all tests
cargo test

# Test RSA authentication
cargo test rsa_auth

# Test JWT tokens
cargo test jwt_tokens

# Test API key management
cargo test api_keys

# Test session management
cargo test sessions

# Integration tests
cargo test --test integration
```

## 📋 Requirements

- **Rust**: 1.70.0 or later
- **Platforms**: Linux, macOS, Windows
- **Dependencies**: RSA cryptographic libraries
- **Security**: Secure key storage recommended

## 🌍 Related Projects

This module is part of the RustFS ecosystem:

- [RustFS Main](https://github.com/rustfs/rustfs) - Core distributed storage system
- [RustFS IAM](../iam) - Identity and access management
- [RustFS Signer](../signer) - Request signing
- [RustFS Crypto](../crypto) - Cryptographic operations

## 📚 Documentation

For comprehensive documentation, visit:

- [RustFS Documentation](https://docs.rustfs.com)
- [AppAuth API Reference](https://docs.rustfs.com/appauth/)
- [Security Guide](https://docs.rustfs.com/security/)

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
