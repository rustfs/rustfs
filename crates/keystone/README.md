# RustFS Keystone Integration

OpenStack Keystone authentication integration for RustFS S3-compatible object storage.

## Features

- **Keystone v3 API support** - Modern Keystone authentication
- **Token-based authentication** - Support for X-Auth-Token header
- **EC2 credentials** - S3 API compatibility with Keystone EC2 credentials
- **Multi-tenancy** - Project-based bucket isolation
- **Role mapping** - Map Keystone roles to RustFS IAM policies
- **Token caching** - High-performance token validation with caching
- **Swift compatibility** - Support for X-Storage-Token header

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
rustfs-keystone = "0.0.5"
```

## Usage

```rust
use rustfs_keystone::{KeystoneConfig, KeystoneClient, KeystoneAuthProvider};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load configuration from environment
    let config = KeystoneConfig::from_env()?;

    // Create Keystone client
    let client = KeystoneClient::new(
        config.auth_url.clone(),
        config.get_version()?,
        config.admin_user.clone(),
        config.admin_password.clone(),
        config.admin_project.clone(),
        config.verify_ssl,
    );

    // Create authentication provider
    let auth_provider = KeystoneAuthProvider::new(
        client,
        config.cache_size,
        config.get_cache_ttl(),
    );

    // Authenticate with Keystone token
    let token = "your-keystone-token";
    let credentials = auth_provider.authenticate_with_token(token).await?;

    println!("Authenticated user: {}", credentials.parent_user);
    println!("Project: {:?}", credentials.claims);

    Ok(())
}
```

## Configuration

Configure via environment variables:

```bash
# Enable Keystone
export RUSTFS_KEYSTONE_ENABLE=true
export RUSTFS_KEYSTONE_AUTH_URL=http://keystone:5000
export RUSTFS_KEYSTONE_VERSION=v3

# Admin credentials (optional, for privileged operations)
export RUSTFS_KEYSTONE_ADMIN_USER=admin
export RUSTFS_KEYSTONE_ADMIN_PASSWORD=secret
export RUSTFS_KEYSTONE_ADMIN_PROJECT=admin

# Multi-tenancy
export RUSTFS_KEYSTONE_TENANT_PREFIX=true

# Performance tuning
export RUSTFS_KEYSTONE_CACHE_SIZE=10000
export RUSTFS_KEYSTONE_CACHE_TTL=300
```

## API Documentation

### KeystoneClient

The `KeystoneClient` provides low-level API access to Keystone services:

```rust
let client = KeystoneClient::new(
    "http://keystone:5000".to_string(),
    KeystoneVersion::V3,
    Some("admin".to_string()),
    Some("secret".to_string()),
    Some("admin".to_string()),
    true, // verify SSL
);

// Validate a token
let token_info = client.validate_token("token123").await?;
println!("User: {}, Project: {:?}", token_info.username, token_info.project_name);

// Get EC2 credentials
let ec2_creds = client.get_ec2_credentials("user_id", Some("project_id")).await?;
```

### KeystoneAuthProvider

The `KeystoneAuthProvider` provides high-level authentication with caching:

```rust
let provider = KeystoneAuthProvider::new(client, 10000, Duration::from_secs(300));

// Authenticate with token
let cred = provider.authenticate_with_token("token123").await?;

// Check if user is admin
if provider.is_admin(&cred) {
    println!("User has admin privileges");
}

// Get project ID
if let Some(project_id) = provider.get_project_id(&cred) {
    println!("User's project: {}", project_id);
}
```

### KeystoneIdentityMapper

The `KeystoneIdentityMapper` handles multi-tenancy and role mapping:

```rust
let mapper = KeystoneIdentityMapper::new(Arc::new(client), true);

// Apply tenant prefix to bucket name
let prefixed = mapper.apply_tenant_prefix("mybucket", Some("proj123"));
// Returns: "proj123:mybucket"

// Remove tenant prefix
let unprefixed = mapper.remove_tenant_prefix("proj123:mybucket", Some("proj123"));
// Returns: "mybucket"

// Map Keystone roles to RustFS policies
let roles = vec!["Member".to_string(), "admin".to_string()];
let policies = mapper.map_roles_to_policies(&roles);
// Returns: ["ReadWritePolicy", "AdminPolicy"]

// Check permissions
if mapper.has_permission(&roles, "s3:PutObject", "bucket/key") {
    println!("User can write objects");
}
```

## Architecture

```
KeystoneClient (API calls)
    ↓
KeystoneAuthProvider (Authentication + Caching)
    ↓
KeystoneIdentityMapper (Multi-tenancy + Role Mapping)
    ↓
RustFS Credentials
```

## Token Cache

The token cache improves performance by caching validated tokens:

- **Cache Size**: Number of tokens to cache (default: 10,000)
- **Cache TTL**: Time-to-live for cached tokens (default: 300 seconds)
- **Thread-Safe**: Uses `moka::future::Cache` for concurrent access

## Multi-Tenancy

When tenant prefixing is enabled:

1. **Bucket Creation**: `mybucket` → stored as `project_id:mybucket`
2. **Bucket Listing**: Only shows buckets belonging to user's project
3. **Access Control**: Users can only access their project's buckets

## Role Mapping

Default role mappings:

| Keystone Role | RustFS Policy | Permissions |
|---------------|---------------|-------------|
| admin | AdminPolicy | Full access (s3:*) |
| Member | ReadWritePolicy | Read/write operations |
| _member_ | ReadOnlyPolicy | Read-only access |
| ResellerAdmin | AdminPolicy | Full access (s3:*) |

Add custom mappings:

```rust
let mut mapper = KeystoneIdentityMapper::new(client, true);
mapper.add_role_mapping("CustomRole".to_string(), "CustomPolicy".to_string());
```

## Error Handling

All operations return `Result<T, KeystoneError>`:

```rust
use rustfs_keystone::{KeystoneError, Result};

match auth_provider.authenticate_with_token(token).await {
    Ok(cred) => println!("Success: {}", cred.parent_user),
    Err(KeystoneError::InvalidToken) => eprintln!("Token is invalid"),
    Err(KeystoneError::TokenExpired) => eprintln!("Token has expired"),
    Err(e) => eprintln!("Error: {}", e),
}
```

## Testing

Run tests with:

```bash
cargo test -p rustfs-keystone
```

Integration tests require a running Keystone instance.

## License

Licensed under the Apache License, Version 2.0. See LICENSE file for details.
