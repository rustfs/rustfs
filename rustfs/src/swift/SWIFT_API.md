# OpenStack Swift API Support

RustFS provides native support for the OpenStack Swift API, enabling seamless integration with OpenStack environments and Swift-compatible applications.

## Overview

The Swift API implementation in RustFS provides S3-compatible object storage through the OpenStack Swift protocol. This allows applications designed for OpenStack Swift to work with RustFS without modification.

### Key Features

- **Keystone Authentication**: Native support for OpenStack Keystone v3 authentication with X-Auth-Token headers
- **Tenant Isolation**: Multi-tenant support with project-scoped access using AUTH_{project_id} account URLs
- **Container Management**: Create, list, update, and delete containers with metadata support
- **Object Operations**: Upload, download, and manage object metadata
- **Server-Side Copy**: Efficient object copying within and across containers
- **HTTP Range Requests**: Support for partial object downloads
- **Security Hardening**: Input validation, metadata size limits, and error sanitization

## Configuration

### Enable Swift API

Swift API support is disabled by default. Enable it with:

```bash
export RUSTFS_SWIFT_ENABLE=true
```

### Optional URL Prefix

Configure a URL prefix for Swift endpoints:

```bash
export RUSTFS_SWIFT_URL_PREFIX=swift
```

With this configuration, Swift endpoints will be accessible at `/swift/v1/...` instead of `/v1/...`

### Keystone Configuration

Swift API requires Keystone authentication. Configure Keystone with:

```bash
export RUSTFS_KEYSTONE_URL=http://keystone:5000
export RUSTFS_KEYSTONE_ADMIN_TENANT=admin
export RUSTFS_KEYSTONE_ADMIN_USER=admin
export RUSTFS_KEYSTONE_ADMIN_PASSWORD=secret
```

See [Keystone Integration](KEYSTONE_SWIFT_REFERENCE.md) for detailed configuration.

## Supported Operations

### Account Operations

| Operation | Method | Endpoint | Status |
|-----------|--------|----------|--------|
| List Containers | GET | `/v1/AUTH_{project_id}` | ✅ Supported |

### Container Operations

| Operation | Method | Endpoint | Status |
|-----------|--------|----------|--------|
| Create Container | PUT | `/v1/AUTH_{project_id}/{container}` | ✅ Supported |
| List Objects | GET | `/v1/AUTH_{project_id}/{container}` | ✅ Supported |
| Get Metadata | HEAD | `/v1/AUTH_{project_id}/{container}` | ✅ Supported |
| Delete Container | DELETE | `/v1/AUTH_{project_id}/{container}` | ✅ Supported |
| Update Metadata | POST | `/v1/AUTH_{project_id}/{container}` | ⚠️ Limited* |

### Object Operations

| Operation | Method | Endpoint | Status |
|-----------|--------|----------|--------|
| Get Metadata | HEAD | `/v1/AUTH_{project_id}/{container}/{object}` | ✅ Supported |
| Delete Object | DELETE | `/v1/AUTH_{project_id}/{container}/{object}` | ✅ Supported |
| Upload Object | PUT | `/v1/AUTH_{project_id}/{container}/{object}` | ⚠️ Limited* |
| Download Object | GET | `/v1/AUTH_{project_id}/{container}/{object}` | ⚠️ Limited* |
| Update Metadata | POST | `/v1/AUTH_{project_id}/{container}/{object}` | ⚠️ Limited* |
| Copy Object | COPY | `/v1/AUTH_{project_id}/{container}/{object}` | ⚠️ Limited* |

\* _Operations marked as "Limited" have core logic implemented but require handler architecture improvements for full functionality. See [Known Limitations](#known-limitations)._

## Usage Examples

### Authentication

First, obtain a Keystone token:

```bash
curl -X POST http://keystone:5000/v3/auth/tokens \
  -H "Content-Type: application/json" \
  -d '{
    "auth": {
      "identity": {
        "methods": ["password"],
        "password": {
          "user": {
            "domain": {"name": "Default"},
            "name": "demo",
            "password": "secret"
          }
        }
      },
      "scope": {
        "project": {
          "domain": {"name": "Default"},
          "name": "demo"
        }
      }
    }
  }'
```

Extract the `X-Subject-Token` from response headers and project ID from response body.

### List Containers

```bash
curl -X GET http://rustfs:9000/v1/AUTH_abc123def456 \
  -H "X-Auth-Token: $TOKEN"
```

### Create Container

```bash
curl -X PUT http://rustfs:9000/v1/AUTH_abc123def456/photos \
  -H "X-Auth-Token: $TOKEN"
```

### List Objects in Container

```bash
curl -X GET http://rustfs:9000/v1/AUTH_abc123def456/photos \
  -H "X-Auth-Token: $TOKEN"
```

### Get Container Metadata

```bash
curl -I http://rustfs:9000/v1/AUTH_abc123def456/photos \
  -H "X-Auth-Token: $TOKEN"
```

### Get Object Metadata

```bash
curl -I http://rustfs:9000/v1/AUTH_abc123def456/photos/vacation.jpg \
  -H "X-Auth-Token: $TOKEN"
```

### Delete Object

```bash
curl -X DELETE http://rustfs:9000/v1/AUTH_abc123def456/photos/vacation.jpg \
  -H "X-Auth-Token: $TOKEN"
```

### Delete Container

```bash
curl -X DELETE http://rustfs:9000/v1/AUTH_abc123def456/photos \
  -H "X-Auth-Token: $TOKEN"
```

## Security Features

### Tenant Isolation

RustFS enforces strict tenant isolation:
- All operations are scoped to the authenticated project
- Account URLs must match the project ID from the Keystone token
- S3 buckets are prefixed with project ID (e.g., `abc123-photos`)
- Cross-tenant access is prevented at the handler level

### Input Validation

- Account names must follow `AUTH_{project_id}` format
- Object names validated against directory traversal attacks
- Path normalization prevents security bypasses
- Null byte injection protection

### Metadata Limits

To prevent DoS attacks:
- Maximum 90 metadata headers per object/container
- Maximum 256 bytes per metadata value
- Maximum 5GB object size

### Error Sanitization

All storage layer errors are sanitized before returning to clients to prevent information disclosure.

## Architecture

### Handler Flow

```
HTTP Request
    ↓
[Keystone Auth Middleware]
    ↓ (sets KEYSTONE_CREDENTIALS in task-local storage)
[Swift Service Layer]
    ↓ (routes Swift URLs)
[Swift Handler]
    ↓ (validates tenant access)
[Container/Object Operations]
    ↓ (maps to S3 backend)
[S3 Storage Layer]
```

### Container-to-Bucket Mapping

Swift containers are mapped to S3 buckets with tenant prefixing:

```
Swift: /v1/AUTH_abc123/photos
  ↓
S3: abc123-photos
```

This ensures tenant isolation at the storage layer.

### Object Key Mapping

Swift object paths are URL-decoded and normalized:

```
Swift: /v1/AUTH_abc123/photos/vacation/beach.jpg
  ↓
S3: vacation/beach.jpg (in bucket abc123-photos)
```

## Known Limitations

The current implementation has architectural limitations in the handler layer:

### Handler Signature
```rust
async fn handle_swift_request(route: SwiftRoute, credentials: Option<Credentials>)
```

This signature doesn't provide access to:
- Request body (needed for object uploads)
- Request headers (needed for metadata operations, Range requests)
- Streaming response bodies (needed for object downloads)

### Impact

Operations requiring these features have limited functionality:
- **PUT /object**: Upload logic exists but cannot access request body
- **GET /object**: Download logic exists but cannot stream response body
- **POST /object**: Metadata update logic exists but cannot access headers
- **POST /container**: Metadata update logic exists but cannot access headers
- **COPY /object**: Copy logic exists but cannot access Destination header

### Required Refactoring

Full support requires changing the handler to:
```rust
async fn handle_swift_request(
    req: Request<B>,
    route: SwiftRoute,
    credentials: Option<Credentials>
)
```

This would provide access to request body, headers, and enable streaming responses.

## Performance

Swift operations use the same high-performance S3 backend as native S3 operations:
- Zero-copy streaming (where supported)
- Efficient erasure coding
- Distributed data placement
- In-memory metadata caching

## Compatibility

### OpenStack Compatibility

RustFS Swift API is designed to be compatible with:
- OpenStack Swift clients (python-swiftclient)
- Keystone v3 authentication
- Standard Swift URL patterns

### Ceph RGW Patterns

The implementation follows patterns from Ceph RGW for:
- Account URL format (`AUTH_{project_id}`)
- Tenant isolation mechanisms
- Keystone token validation

## Testing

### Unit Tests

Run Swift module unit tests:
```bash
cargo test --bin rustfs swift
```

### Integration Tests

Integration tests require a running RustFS server with Swift enabled:
```bash
export RUSTFS_SWIFT_ENABLE=true
export RUSTFS_KEYSTONE_URL=http://keystone:5000
# ... other Keystone config ...

cargo test swift_container_integration_test
cargo test swift_object_integration_test
```

## Troubleshooting

### 401 Unauthorized

Ensure:
- Keystone is accessible at configured URL
- Token is valid and not expired
- User has appropriate roles (Member or admin)

### 403 Forbidden

Check:
- Project ID in URL matches token's project
- User has access to the specified project

### 404 Not Found (Container)

Verify:
- Container name is correct
- Container was created for this project
- Tenant isolation is properly configured

## Future Enhancements

Planned improvements:
1. Handler refactoring to support full operation set
2. Streaming uploads and downloads
3. Account-level operations (HEAD, POST)
4. Bulk operations
5. Static website hosting
6. Temporary URL support
7. Object versioning through Swift API

## Contributing

Contributions to Swift API support are welcome! See [CONTRIBUTING.md](../CONTRIBUTING.md) for guidelines.

## References

- [OpenStack Swift API Documentation](https://docs.openstack.org/swift/latest/api/object_api_v1.html)
- [Keystone Integration Reference](KEYSTONE_SWIFT_REFERENCE.md)
- [Swift Implementation Plan](SWIFT_IMPLEMENTATION_PLAN.md)

## License

Swift API implementation is licensed under Apache 2.0, same as RustFS core.
