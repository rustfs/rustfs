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

### Component Architecture

```
KeystoneClient (API calls)
    ↓
KeystoneAuthProvider (Authentication + Caching)
    ↓
KeystoneIdentityMapper (Multi-tenancy + Role Mapping)
    ↓
RustFS Credentials
```

### Middleware Architecture

The keystone crate includes a Tower middleware (`KeystoneAuthMiddleware`) that integrates directly into RustFS's HTTP service stack. The middleware is self-contained within this crate and exported via the `middleware` module:

```rust
use rustfs_keystone::{KeystoneAuthLayer, KEYSTONE_CREDENTIALS};

// In RustFS HTTP service setup
let layer = KeystoneAuthLayer::new(keystone_auth_provider);
```

The middleware uses Tokio task-local storage (`KEYSTONE_CREDENTIALS`) to pass authenticated credentials between the middleware layer and authentication handlers without modifying the HTTP request.

### RustFS Integration Architecture

The Keystone integration uses a middleware-based approach that intercepts HTTP requests before they reach the S3 service layer:

```
HTTP Request
    ↓
RemoteAddr/TrustedProxy Layers (Extract client IP)
    ↓
SetRequestId/CatchPanic Layers (Request metadata)
    ↓
ReadinessGate Layer (System health check)
    ↓
KeystoneAuthMiddleware ⭐ (Token validation)
    ├─ No X-Auth-Token? → Pass through to S3 auth
    ├─ Has X-Auth-Token? → Validate with Keystone
    │   ├─ Valid? → Store credentials in task-local storage → Continue
    │   └─ Invalid? → Return 401 Unauthorized immediately
    ↓
TraceLayer (Logging/observability)
    ↓
S3 Service Layer
    ↓
IAMAuth (Authentication)
    ├─ Keystone credential? (access_key starts with "keystone:")
    │   ├─ Return empty secret_key (bypass signature validation)
    │   └─ Retrieve credentials from task-local storage
    └─ Standard credential? → Normal AWS Signature v4 validation
    ↓
check_key_valid (Authorization)
    ├─ Keystone credential?
    │   ├─ Get credentials from task-local storage
    │   ├─ Check user roles (admin/reseller_admin = owner)
    │   └─ Return (Credentials, is_owner)
    └─ Standard credential? → Normal IAM validation
    ↓
S3 Operation (PutObject, GetObject, etc.)
```

## Integration with RustFS

### How It Works

The Keystone integration provides seamless OpenStack authentication for RustFS S3 API. Here's how the complete request flow works:

#### 1. Request with Keystone Token

When a client makes an S3 API request with a Keystone token:

```bash
curl -X GET http://rustfs:9000/mybucket/myobject \
  -H "X-Auth-Token: gAAAAABk..."
```

**Flow:**
1. **Middleware Intercepts**: The `KeystoneAuthMiddleware` extracts the `X-Auth-Token` header
2. **Token Validation**: Calls Keystone API to validate the token and retrieve user information
3. **Credential Mapping**: Creates RustFS credentials with:
   - `access_key`: `keystone:<user_id>` (special prefix to identify Keystone users)
   - `parent_user`: Keystone username
   - `claims`: Project ID, roles, and other Keystone attributes in JSON format
4. **Task-Local Storage**: Stores credentials in async task-local storage (request-scoped)
5. **Pass Through**: Request continues to S3 service layer
6. **Authentication**: IAMAuth detects `keystone:` prefix, returns empty secret (bypasses AWS signature check)
7. **Authorization**: `check_key_valid()` retrieves credentials from task-local storage
8. **Role Check**: Determines if user is admin based on roles:
   - `admin` role → owner permissions (full access)
   - `reseller_admin` role → owner permissions (full access)
   - Other roles → non-owner permissions (restricted access)
9. **S3 Operation**: Proceeds with appropriate permissions

#### 2. Request without Keystone Token

When a client makes a standard S3 request:

```bash
aws s3 cp file.txt s3://mybucket/file.txt \
  --endpoint-url http://rustfs:9000
```

**Flow:**
1. **Middleware Pass-Through**: No `X-Auth-Token` header found, request passes through unchanged
2. **Standard S3 Auth**: AWS Signature v4 validation
3. **IAM Validation**: Normal RustFS IAM authentication
4. **S3 Operation**: Proceeds with IAM-based permissions

#### 3. Invalid Token Handling

When a token is invalid or expired:

**Flow:**
1. **Token Validation Fails**: Keystone returns error (invalid/expired token)
2. **Immediate 401**: Middleware returns `401 Unauthorized` immediately
3. **No Fallback**: Does NOT fall back to standard S3 authentication
4. **XML Error Response**: Returns S3-compatible error XML:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<Error>
    <Code>InvalidToken</Code>
    <Message>Invalid Keystone token</Message>
    <Details>Token validation failed: token expired</Details>
</Error>
```

### Permission Model

The integration uses Keystone roles to determine RustFS permissions:

**Owner Permissions (is_owner=true):**
- Granted to users with `admin` or `reseller_admin` roles
- Full access to all operations (equivalent to root/admin access)
- Can create/delete buckets, manage policies, access all objects

**Non-Owner Permissions (is_owner=false):**
- Granted to users with other roles (member, reader, etc.)
- Restricted access based on bucket policies and IAM policies
- Cannot perform administrative operations

**Example:**
```json
{
  "roles": ["admin", "member"]
}
```
→ `is_owner=true` (has admin role)

```json
{
  "roles": ["member", "reader"]
}
```
→ `is_owner=false` (no admin role)

### Task-Local Storage

The integration uses Tokio task-local storage to pass credentials between middleware and authentication handlers:

**Why Task-Local Storage?**
- **Async-Safe**: Works correctly with async/await and Tokio runtime
- **Request-Scoped**: Automatically cleaned up when request completes
- **No Request Modification**: Credentials don't need to be added to HTTP headers/extensions
- **Thread-Safe**: Each async task has its own isolated storage

**How It Works:**
1. Middleware validates token and stores credentials using `KEYSTONE_CREDENTIALS.scope()`
2. Auth handlers retrieve credentials using `KEYSTONE_CREDENTIALS.try_with()`
3. Storage is automatically scoped to the current async task (request)
4. Storage is empty/inaccessible outside the scope

### Token Caching

To minimize Keystone API calls, the integration includes a high-performance token cache:

**Cache Behavior:**
- **Cache Hit**: Token found in cache → Returns cached credentials (no Keystone API call)
- **Cache Miss**: Token not in cache → Validates with Keystone → Caches result
- **Cache TTL**: Tokens are cached for configured duration (default: 300 seconds)
- **Cache Invalidation**: Expired entries are automatically removed
- **Thread-Safe**: Uses `moka::future::Cache` for concurrent access

**Performance Impact:**
- First request with token: ~50-100ms (network call to Keystone)
- Subsequent requests: ~1-2ms (cache lookup)
- Recommended cache size: 10,000 tokens (configurable)

### Configuration in RustFS

To enable Keystone authentication in RustFS:

1. **Set Environment Variables:**
```bash
export RUSTFS_KEYSTONE_ENABLE=true
export RUSTFS_KEYSTONE_AUTH_URL=http://keystone:5000
export RUSTFS_KEYSTONE_VERSION=v3
export RUSTFS_KEYSTONE_ADMIN_USER=admin
export RUSTFS_KEYSTONE_ADMIN_PASSWORD=secret
export RUSTFS_KEYSTONE_ADMIN_PROJECT=admin
export RUSTFS_KEYSTONE_ADMIN_DOMAIN=Default
export RUSTFS_KEYSTONE_CACHE_SIZE=10000
export RUSTFS_KEYSTONE_CACHE_TTL=300
export RUSTFS_KEYSTONE_VERIFY_SSL=true
```

2. **Start RustFS:**
```bash
rustfs --address 127.0.0.1:9000 \
  --access-key minioadmin \
  --secret-key minioadmin \
  volumes /data
```

3. **RustFS will automatically:**
   - Initialize Keystone client on startup (in `rustfs/src/main.rs`)
   - Register `KeystoneAuthLayer` middleware from this crate in HTTP service stack (in `rustfs/src/server/http.rs`)
   - Start accepting both Keystone and standard S3 authentication

The middleware is entirely self-contained in the `rustfs-keystone` crate and integrated into RustFS via the exported `KeystoneAuthLayer`. No separate middleware directory is required in the main RustFS binary.

### Dual Authentication Support

RustFS supports **both** Keystone and standard S3 authentication simultaneously:

- **Keystone Users**: Use `X-Auth-Token` header with Keystone token
- **IAM Users**: Use standard AWS Signature v4 authentication
- **No Conflict**: Requests are routed based on presence of `X-Auth-Token` header
- **Automatic Detection**: Middleware automatically detects authentication method

This allows gradual migration from standard S3 auth to Keystone auth, or mixed environments where some users authenticate via Keystone and others via IAM.

## Manual Testing

### Prerequisites

1. **Running Keystone Instance**

Using Docker:
```bash
docker run -d --name keystone \
  -p 5000:5000 \
  -e KEYSTONE_ADMIN_PASSWORD=secret \
  ghcr.io/openstack/keystone:latest
```

Or using DevStack:
```bash
# Follow DevStack installation guide
git clone https://opendev.org/openstack/devstack
cd devstack
./stack.sh
```

2. **Running RustFS with Keystone Enabled**

```bash
# Configure Keystone
export RUSTFS_KEYSTONE_ENABLE=true
export RUSTFS_KEYSTONE_AUTH_URL=http://localhost:5000
export RUSTFS_KEYSTONE_VERSION=v3
export RUSTFS_KEYSTONE_ADMIN_USER=admin
export RUSTFS_KEYSTONE_ADMIN_PASSWORD=secret
export RUSTFS_KEYSTONE_ADMIN_PROJECT=admin
export RUSTFS_KEYSTONE_ADMIN_DOMAIN=Default

# Start RustFS
cargo run --bin rustfs -- \
  --address 127.0.0.1:9000 \
  --access-key minioadmin \
  --secret-key minioadmin \
  volumes /data
```

### Test Scenarios

#### Test 1: Get Keystone Token

```bash
# Request scoped token from Keystone
curl -X POST http://localhost:5000/v3/auth/tokens \
  -H "Content-Type: application/json" \
  -d '{
    "auth": {
      "identity": {
        "methods": ["password"],
        "password": {
          "user": {
            "name": "admin",
            "domain": {"name": "Default"},
            "password": "secret"
          }
        }
      },
      "scope": {
        "project": {
          "name": "admin",
          "domain": {"name": "Default"}
        }
      }
    }
  }' -i

# Look for X-Subject-Token in response headers
# Example: X-Subject-Token: gAAAAABk1a2b3c...
```

Save the token from the `X-Subject-Token` header.

#### Test 2: List Buckets with Keystone Token

```bash
# Replace TOKEN with your actual token
export KEYSTONE_TOKEN="gAAAAABk1a2b3c..."

curl -X GET http://localhost:9000/ \
  -H "X-Auth-Token: $KEYSTONE_TOKEN" \
  -v
```

**Expected Result:**
- Status: `200 OK`
- Response: XML list of buckets
- Logs should show: `Keystone middleware: Authentication successful for user: admin`

#### Test 3: Create Bucket

```bash
curl -X PUT http://localhost:9000/test-keystone-bucket \
  -H "X-Auth-Token: $KEYSTONE_TOKEN" \
  -v
```

**Expected Result:**
- Status: `200 OK`
- Bucket created successfully
- Logs show Keystone credentials being used

#### Test 4: Upload Object

```bash
echo "Hello from Keystone!" > test.txt

curl -X PUT http://localhost:9000/test-keystone-bucket/test.txt \
  -H "X-Auth-Token: $KEYSTONE_TOKEN" \
  -T test.txt \
  -v
```

**Expected Result:**
- Status: `200 OK`
- Object uploaded successfully

#### Test 5: Download Object

```bash
curl -X GET http://localhost:9000/test-keystone-bucket/test.txt \
  -H "X-Auth-Token: $KEYSTONE_TOKEN" \
  -o downloaded.txt \
  -v

cat downloaded.txt
```

**Expected Result:**
- Status: `200 OK`
- File content: `Hello from Keystone!`

#### Test 6: Invalid Token (Negative Test)

```bash
curl -X GET http://localhost:9000/ \
  -H "X-Auth-Token: invalid-token-12345" \
  -v
```

**Expected Result:**
- Status: `401 Unauthorized`
- Response:
```xml
<?xml version="1.0" encoding="UTF-8"?>
<Error>
    <Code>InvalidToken</Code>
    <Message>Invalid Keystone token</Message>
    <Details>...</Details>
</Error>
```
- Logs show: `Keystone middleware: Authentication failed`

#### Test 7: No Token (Standard S3 Auth)

```bash
# Using AWS CLI with standard credentials
aws s3 ls s3:// \
  --endpoint-url http://localhost:9000 \
  --no-sign-request
```

**Expected Result:**
- Falls back to standard S3 authentication
- Works as normal (if anonymous access allowed)
- Logs show: `Keystone middleware: No X-Auth-Token header, passing through to S3 auth`

#### Test 8: Admin Role Permissions

```bash
# Create a user with admin role in Keystone
# Get token for admin user

curl -X DELETE http://localhost:9000/test-keystone-bucket \
  -H "X-Auth-Token: $ADMIN_TOKEN" \
  -v
```

**Expected Result:**
- Status: `204 No Content` (bucket deleted)
- Admin has owner permissions (`is_owner=true`)

#### Test 9: Non-Admin Role Permissions

```bash
# Create a user with only "member" role in Keystone
# Get token for member user

curl -X DELETE http://localhost:9000/test-keystone-bucket \
  -H "X-Auth-Token: $MEMBER_TOKEN" \
  -v
```

**Expected Result:**
- Status: `403 Forbidden` (depending on bucket policy)
- Member does not have owner permissions (`is_owner=false`)

#### Test 10: Token Caching Performance

```bash
# First request (cache miss)
time curl -X GET http://localhost:9000/ \
  -H "X-Auth-Token: $KEYSTONE_TOKEN" \
  -o /dev/null -s

# Second request (cache hit)
time curl -X GET http://localhost:9000/ \
  -H "X-Auth-Token: $KEYSTONE_TOKEN" \
  -o /dev/null -s
```

**Expected Result:**
- First request: ~50-100ms (includes Keystone API call)
- Second request: ~1-5ms (cache hit, no Keystone call)
- Logs show: `Cache hit` for second request

### Troubleshooting

**Issue: "Keystone authentication is not enabled"**
- Check `RUSTFS_KEYSTONE_ENABLE=true` is set
- Verify environment variables are exported before starting RustFS
- Check RustFS startup logs for "Keystone authentication initialized successfully"

**Issue: "Connection refused" to Keystone**
- Verify Keystone is running: `curl http://localhost:5000/v3`
- Check `RUSTFS_KEYSTONE_AUTH_URL` points to correct Keystone endpoint
- Verify network connectivity between RustFS and Keystone

**Issue: "Invalid token" errors**
- Check token hasn't expired (Keystone tokens typically expire after 1 hour)
- Request a fresh token
- Verify token format is correct (no newlines, extra spaces)

**Issue: "SSL verification failed"**
- If using self-signed certificates, set `RUSTFS_KEYSTONE_VERIFY_SSL=false`
- Or install Keystone's CA certificate in system trust store

**Issue: Slow performance**
- Increase cache size: `RUSTFS_KEYSTONE_CACHE_SIZE=50000`
- Increase cache TTL: `RUSTFS_KEYSTONE_CACHE_TTL=600`
- Check network latency to Keystone

**Issue: Permissions denied**
- Verify user's Keystone roles
- Check if user needs `admin` or `reseller_admin` role
- Review RustFS logs for `is_owner` value

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

### Test Structure

The crate includes comprehensive test coverage:

**Unit Tests** (16 tests in `src/` modules):
- Config parsing and validation
- Client creation
- Auth provider functionality
- Identity mapping and role permissions
- Middleware token extraction and validation

**Integration Tests** (10 tests in `tests/integration/`):
- Middleware layer creation and configuration
- Task-local storage isolation and scope management
- Credential passing between middleware and auth handlers
- Nested and sequential scope behavior
- Multi-task concurrency safety

**Total: 27 tests** covering all public APIs and integration scenarios.

Integration tests require a running Keystone instance.

## License

Licensed under the Apache License, Version 2.0. See LICENSE file for details.
