# Swift Object Operations Integration Tests

This directory contains integration tests for the Swift API object operations.

## Test Coverage

The integration test suite covers all Phase 3 object operations:

1. **test_upload_object** - Uploads an object (PUT)
   - Verifies 201 Created status
   - Checks ETag header
   - Checks Swift transaction headers (X-Trans-Id, X-OpenStack-Request-Id)

2. **test_upload_object_with_metadata** - Uploads object with custom metadata
   - Tests X-Object-Meta-* header handling
   - Verifies metadata is preserved and retrievable via HEAD

3. **test_download_object** - Downloads an object (GET)
   - Verifies 200 OK status
   - Confirms content matches uploaded data
   - Checks Content-Length header

4. **test_head_object** - Gets object metadata (HEAD)
   - Verifies 200 OK status
   - Checks Content-Length header
   - Checks ETag header
   - Checks Last-Modified header

5. **test_update_object_metadata** - Updates metadata (POST)
   - Verifies 204 No Content status
   - Tests X-Object-Meta-* header updates
   - Confirms content is not modified

6. **test_delete_object** - Deletes an object (DELETE)
   - Verifies 204 No Content status
   - Confirms object is removed (404 on subsequent GET)

7. **test_delete_nonexistent_object** - Tests idempotency
   - Verifies 204 No Content for non-existent object (Swift semantics)

8. **test_list_objects** - Lists objects in container (GET container)
   - Verifies 200 OK status
   - Parses JSON response
   - Confirms uploaded objects appear in list

9. **test_object_lifecycle** - Complete workflow
   - Full lifecycle: upload → download → HEAD → POST → list → delete → verify

## Prerequisites

1. **Running RustFS Server**
   ```bash
   # Start RustFS with Swift support enabled
   export RUSTFS_SWIFT_ENABLE=true
   cargo run --bin rustfs
   ```

2. **Environment Variables**
   ```bash
   # Optional: Override default settings
   export TEST_RUSTFS_SERVER="http://localhost:9000"
   export TEST_SWIFT_TOKEN="your-keystone-token"
   export TEST_SWIFT_ACCOUNT="AUTH_test-project-123"
   ```

3. **Keystone Authentication** (for real testing)
   - Configure Keystone middleware in RustFS
   - Obtain a valid Keystone token
   - Set TEST_SWIFT_TOKEN to the token value

## Running Tests

### Run All Object Integration Tests
```bash
cargo test --test swift_object_integration_test -- --ignored --nocapture
```

### Run Specific Test
```bash
cargo test --test swift_object_integration_test test_upload_object -- --ignored --nocapture
```

### Run with Serial Execution (recommended)
```bash
# Tests are marked with #[serial] to avoid conflicts
cargo test --test swift_object_integration_test -- --ignored --test-threads=1
```

## Test Configuration

Tests use the following defaults (configurable via environment variables):

- **Endpoint**: `http://localhost:9000` (TEST_RUSTFS_SERVER)
- **Auth Token**: `test-token` (TEST_SWIFT_TOKEN)
- **Account**: `AUTH_test-project-123` (TEST_SWIFT_ACCOUNT)

## Important Notes

1. **Tests are Ignored by Default**
   - All tests are marked with `#[ignore]`
   - They require a running RustFS server
   - Use `--ignored` flag to run them

2. **Serial Execution**
   - Tests are marked with `#[serial]` to prevent race conditions
   - Container names use UUIDs to avoid conflicts
   - Tests clean up after themselves (delete created objects and containers)

3. **Mock vs Real Testing**
   - For unit tests without a server, use the existing tests in `src/swift/`
   - For real Keystone integration, configure actual Keystone auth
   - Mock tokens work for basic testing without Keystone

## Object Operations Tested

### Upload Object (PUT)
- URL: `PUT /v1/{account}/{container}/{object}`
- Status: 201 Created
- Headers: ETag, X-Trans-Id
- Body: Object content (binary)

### Download Object (GET)
- URL: `GET /v1/{account}/{container}/{object}`
- Status: 200 OK
- Headers: Content-Length, Content-Type, ETag, Last-Modified
- Body: Object content (binary)

### Get Object Metadata (HEAD)
- URL: `HEAD /v1/{account}/{container}/{object}`
- Status: 200 OK
- Headers: Content-Length, Content-Type, ETag, Last-Modified, X-Object-Meta-*
- Body: Empty

### Update Object Metadata (POST)
- URL: `POST /v1/{account}/{container}/{object}`
- Status: 204 No Content
- Headers: X-Object-Meta-* (in request)
- Body: Empty

### Delete Object (DELETE)
- URL: `DELETE /v1/{account}/{container}/{object}`
- Status: 204 No Content
- Headers: X-Trans-Id
- Body: Empty

### List Objects (GET Container)
- URL: `GET /v1/{account}/{container}`
- Status: 200 OK
- Headers: Content-Type: application/json
- Body: JSON array of objects

## Cleanup

Tests automatically clean up objects and containers they create using:
```rust
let _ = client.delete_object(&container_name, &object_name).await;
let _ = client.delete_container(&container_name).await;
```

If tests fail mid-execution, you may need to manually clean up:
```bash
# List objects in container
curl -X GET http://localhost:9000/v1/AUTH_test-project-123/test-container-{uuid} \
  -H "X-Auth-Token: test-token"

# Delete test objects
curl -X DELETE http://localhost:9000/v1/AUTH_test-project-123/test-container-{uuid}/test-object.txt \
  -H "X-Auth-Token: test-token"

# Delete test containers
curl -X DELETE http://localhost:9000/v1/AUTH_test-project-123/test-container-{uuid} \
  -H "X-Auth-Token: test-token"
```

## Troubleshooting

### Test Failures

1. **Connection Refused**
   - Ensure RustFS server is running
   - Check TEST_RUSTFS_SERVER is correct

2. **401 Unauthorized**
   - Verify TEST_SWIFT_TOKEN is valid
   - Check Keystone authentication is configured

3. **404 Not Found**
   - Verify Swift support is enabled (RUSTFS_SWIFT_ENABLE=true)
   - Check account name matches Keystone project_id

4. **500 Internal Server Error**
   - Check RustFS server logs for errors
   - Verify storage backend is accessible

### Debug Mode

Run tests with verbose output:
```bash
RUST_LOG=debug cargo test --test swift_object_integration_test -- --ignored --nocapture
```

## Test Statistics

- **Total Tests**: 9
- **Operations Covered**: 6 (PUT, GET, HEAD, POST, DELETE, LIST)
- **Lifecycle Test**: 1 comprehensive end-to-end test
- **Code Coverage**: All Phase 3 object operations

## Related Documentation

- Container Integration Tests: `SWIFT_INTEGRATION_TESTS.md`
- Phase 3 Implementation Plan: `../SWIFT_IMPLEMENTATION_PLAN.md`
- Swift API Reference: [OpenStack Swift API](https://docs.openstack.org/api-ref/object-store/)
