# OpenStack Swift API for RustFS

Swift-compatible object storage API implementation for RustFS.

## Features

- Full Swift Object Storage API v1 compatibility
- Container CRUD operations
- Object CRUD with streaming support
- Keystone token authentication
- Multi-tenant isolation with secure bucket prefixing
- Server-side object copy (COPY method)
- HTTP Range requests for partial downloads
- Custom metadata support (X-Object-Meta-*, X-Container-Meta-*)

## Enable Feature

```bash
cargo build --features swift
```

Or for all features:

```bash
cargo build --features full
```

## Configuration

Swift API uses Keystone for authentication. Configure the following environment variables:

| Variable | Description |
|----------|-------------|
| `RUSTFS_KEYSTONE_URL` | Keystone authentication endpoint URL |
| `RUSTFS_KEYSTONE_ADMIN_TENANT` | Admin tenant/project name |
| `RUSTFS_KEYSTONE_ADMIN_USER` | Admin username |
| `RUSTFS_KEYSTONE_ADMIN_PASSWORD` | Admin password |

## API Endpoints

Swift API endpoints follow the pattern: `/v1/AUTH_{project_id}/...`

### Account Operations
- `GET /v1/AUTH_{project}` - List containers
- `HEAD /v1/AUTH_{project}` - Get account metadata (not yet implemented)
- `POST /v1/AUTH_{project}` - Update account metadata (not yet implemented)

### Container Operations
- `PUT /v1/AUTH_{project}/{container}` - Create container
- `GET /v1/AUTH_{project}/{container}` - List objects
- `HEAD /v1/AUTH_{project}/{container}` - Get container metadata
- `POST /v1/AUTH_{project}/{container}` - Update container metadata
- `DELETE /v1/AUTH_{project}/{container}` - Delete container

### Object Operations
- `PUT /v1/AUTH_{project}/{container}/{object}` - Upload object
- `GET /v1/AUTH_{project}/{container}/{object}` - Download object
- `HEAD /v1/AUTH_{project}/{container}/{object}` - Get object metadata
- `POST /v1/AUTH_{project}/{container}/{object}` - Update object metadata
- `DELETE /v1/AUTH_{project}/{container}/{object}` - Delete object
- `COPY /v1/AUTH_{project}/{container}/{object}` - Server-side copy

## Architecture

The Swift API is implemented as a Tower service layer (`SwiftService`) that wraps the S3 service:

```
HTTP Request
    │
    ▼
┌───────────────┐
│ SwiftService  │ ← Routes /v1/AUTH_* requests
└───────┬───────┘
        │
   ┌────┴────┐
   │         │
   ▼         ▼
Swift     S3 Service
Handler   (fallback)
```

### Key Components

- **handler.rs** - Main service implementing Tower's Service trait
- **router.rs** - URL routing and parsing for Swift paths
- **container.rs** - Container operations with tenant isolation
- **object.rs** - Object operations including copy and range requests
- **account.rs** - Account validation and tenant access control
- **errors.rs** - Swift-specific error types
- **types.rs** - Data structures for Swift API responses

### Tenant Isolation

Swift containers are mapped to S3 buckets with a secure hash prefix:

```
Swift: /v1/AUTH_abc123/mycontainer
  ↓
S3 Bucket: {sha256(abc123)[0:16]}-mycontainer
```

This ensures:
- Complete tenant isolation at the storage layer
- No collision between tenants with similar container names
- S3-compatible bucket naming (lowercase alphanumeric + hyphen)

## Documentation

See the `docs/` directory for detailed documentation:

- `SWIFT_API.md` - Complete API reference
- `TESTING_GUIDE.md` - Manual testing procedures
- `COMPLETION_ANALYSIS.md` - Protocol coverage tracking
- `COPY_IMPLEMENTATION.md` - Server-side copy documentation
- `RANGE_REQUESTS.md` - Range request implementation details

## License

Apache License 2.0
