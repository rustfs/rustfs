# OpenStack Swift API for RustFS

Swift-compatible object storage API implementation for RustFS.

## Features

The lists below are bounded to what `router.rs` / `handler.rs` dispatch and
what the test suite exercises. A module existing under `src/swift/` does not
by itself mean the feature is reachable over HTTP.

### Wired through the router and handler

- ✅ Account listing (`GET /v1/AUTH_{project}`, JSON) and additive account
  metadata updates (`POST`, `X-Account-Meta-*` / `X-Remove-Account-Meta-*`)
- ✅ Container CRUD (create, list, head, update metadata, delete)
- ✅ Object CRUD with streaming downloads, HTTP Range requests (206 / 416),
  and server-side copy via the `COPY` method
- ✅ Keystone token authentication and multi-tenant isolation with
  SHA256-based bucket prefixing
- ✅ Custom metadata (`X-Object-Meta-*`, `X-Container-Meta-*`); container and
  account POSTs are additive, object POSTs replace the set
- ✅ Container ACLs (`X-Container-Read` / `X-Container-Write`, set and remove on
  container POST, reported on HEAD). Enforcement is account-level plus
  referrer checks; per-user grants are not evaluated because credentials
  carry no user id
- ✅ CORS: `OPTIONS` preflight on container and object routes, and response
  header injection driven by `X-Container-Meta-Access-Control-*`
- ✅ TempURL (`temp_url_sig` / `temp_url_expires` on object GET, HEAD, PUT;
  key stored as account metadata; optional client-IP restriction)
- ✅ FormPost (container POST with `multipart/form-data`, signed with the
  account TempURL key)
- ✅ Large objects: Static Large Objects (`?multipart-manifest=put|get|delete`)
  and Dynamic Large Objects (`X-Object-Manifest`)
- ✅ Bulk operations: `DELETE /v1/AUTH_{project}?bulk-delete` and
  `PUT /v1/AUTH_{project}/{container}?extract-archive=tar|tar.gz|tar.bz2`
- ✅ Object versioning in the Swift `X-Versions-Location` style: the previous
  copy is archived on PUT / DELETE and restored on DELETE
- ✅ Symlinks (`X-Symlink-Target` on PUT, resolved on GET / HEAD with loop and
  depth checks)
- ✅ Container quotas (`X-Container-Meta-Quota-Bytes` / `-Quota-Count`),
  enforced on object PUT
- ✅ Static website serving on object GET when `web-index` / `web-listings`
  container metadata is set
- ✅ Object expiration headers: `X-Delete-At` / `X-Delete-After` are validated,
  stored, and returned on GET / HEAD

### Not yet wired, or partially wired

- ⏳ Account `HEAD` returns `501 Not Implemented`; no account-level usage
  statistics are exposed
- ⏳ Automatic deletion of expired objects: `expiration_worker.rs` exists but
  the server never starts it, so objects past `X-Delete-At` are not removed
- ⏳ Container sync (`sync.rs`): no `X-Container-Sync-*` header handling and no
  background worker; the module is unit-tested only
- ⏳ `X-Copy-From` on object PUT (only the `COPY` method is supported)
- ⏳ `X-History-Location` versioning mode
- ⏳ Static website index / listing pages at the container root (only the
  object GET route consults static-web settings)
- ⏳ XML / plain-text listing formats; the `format=` query parameter is
  ignored and listings are always JSON

### Test coverage

- Unit tests live next to each module (`acl.rs`, `bulk.rs`, `cors.rs`,
  `dlo.rs`, `slo.rs`, `tempurl.rs`, `formpost.rs`, `staticweb.rs`,
  `symlink.rs`, `quota.rs`, `expiration.rs`, `versioning.rs`, `router.rs`,
  `handler.rs`, and others) and run in the CI `swift` feature lane
- `crates/protocols/tests/swift_metadata_persistence.rs` runs account,
  container, ACL, TempURL-key, and versioning metadata writes against a real
  ECStore and reloads them from disk
- `crates/protocols/tests/swift_versioning_integration.rs`,
  `swift_listing_symlink_tests.rs`, `swift_simple_integration.rs`, and
  `swift_phase4_integration.rs` cover version naming, listing parameters,
  symlink parsing, and module-level helpers without a server
- `rustfs/tests/swift_container_integration_test.rs` and
  `swift_object_integration_test.rs` exercise the HTTP surface end to end but
  are `#[ignore]` and need a running server (`TEST_RUSTFS_SERVER`); they are
  not part of CI

## Enable Feature

**Swift API is opt-in and must be explicitly enabled.**

Build with Swift support:

```bash
cargo build --features swift
```

Or enable all protocol features:

```bash
cargo build --features full
```

**Note:** Swift is NOT enabled by default to avoid unexpected API surface changes in existing deployments.

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
- `GET /v1/AUTH_{project}` - List containers (JSON)
- `HEAD /v1/AUTH_{project}` - Get account metadata (returns 501, not yet implemented)
- `POST /v1/AUTH_{project}` - Update account metadata and TempURL key
- `DELETE /v1/AUTH_{project}?bulk-delete` - Bulk delete

### Container Operations
- `PUT /v1/AUTH_{project}/{container}` - Create container (`?extract-archive=` for bulk upload)
- `GET /v1/AUTH_{project}/{container}` - List objects (JSON; `limit`, `marker`, `end_marker`, `prefix`, `delimiter`)
- `HEAD /v1/AUTH_{project}/{container}` - Get container metadata
- `POST /v1/AUTH_{project}/{container}` - Update container metadata, ACLs, versioning location; FormPost when `multipart/form-data`
- `DELETE /v1/AUTH_{project}/{container}` - Delete container
- `OPTIONS /v1/AUTH_{project}/{container}` - CORS preflight

### Object Operations
- `PUT /v1/AUTH_{project}/{container}/{object}` - Upload object (SLO manifest with `?multipart-manifest=put`, DLO with `X-Object-Manifest`, symlink with `X-Symlink-Target`)
- `GET /v1/AUTH_{project}/{container}/{object}` - Download object (Range, SLO/DLO assembly, symlink resolution, `?multipart-manifest=get`)
- `HEAD /v1/AUTH_{project}/{container}/{object}` - Get object metadata
- `POST /v1/AUTH_{project}/{container}/{object}` - Update object metadata
- `DELETE /v1/AUTH_{project}/{container}/{object}` - Delete object (`?multipart-manifest=delete` removes SLO segments)
- `COPY /v1/AUTH_{project}/{container}/{object}` - Server-side copy
- `OPTIONS /v1/AUTH_{project}/{container}/{object}` - CORS preflight

Object GET, HEAD, and PUT also accept TempURL query parameters without an auth token.

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

- **handler.rs** - Main service implementing Tower's Service trait and method dispatch
- **router.rs** - URL routing and parsing for Swift paths
- **container.rs** - Container operations with tenant isolation
- **object.rs** - Object operations including copy and range requests
- **account.rs** - Account validation, tenant access control, account metadata and TempURL key
- **acl.rs**, **cors.rs** - Container ACL evaluation and CORS config
- **slo.rs**, **dlo.rs** - Static and dynamic large objects
- **tempurl.rs**, **formpost.rs** - Signed URL and form upload validation
- **bulk.rs** - Bulk delete and archive extraction
- **versioning.rs**, **symlink.rs**, **quota.rs**, **staticweb.rs**, **expiration.rs** - Per-feature helpers called from the handler
- **expiration_worker.rs**, **sync.rs** - Background workers that are not started by the server (see above)
- **metadata_update.rs** - Additive account/container metadata merge
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
