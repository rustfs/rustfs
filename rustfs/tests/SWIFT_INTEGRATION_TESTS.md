# Swift Container Operations Integration Tests

This directory contains integration tests for the Swift API container operations.

## Test Coverage

The integration test suite covers all Phase 2 container operations:

1. **test_create_container** - Creates a new container (PUT)
   - Verifies 201 Created status
   - Checks Swift transaction headers (X-Trans-Id, X-OpenStack-Request-Id)

2. **test_create_container_idempotent** - Tests idempotency
   - First PUT returns 201 Created
   - Second PUT returns 202 Accepted

3. **test_list_containers** - Lists containers (GET)
   - Verifies 200 OK status
   - Parses JSON response
   - Confirms created container appears in list

4. **test_container_metadata** - Gets metadata (HEAD)
   - Verifies 204 No Content status
   - Checks X-Container-Object-Count header
   - Checks X-Container-Bytes-Used header
   - Checks X-Timestamp header

5. **test_update_container_metadata** - Updates metadata (POST)
   - Verifies 204 No Content status
   - Tests X-Container-Meta-* header handling

6. **test_delete_container** - Deletes container (DELETE)
   - Verifies 204 No Content status
   - Confirms container is removed (404 on subsequent HEAD)

7. **test_delete_nonexistent_container** - Error handling
   - Verifies 404 Not Found for non-existent container

8. **test_container_name_validation** - Name validation
   - Empty names rejected
   - Names with '/' rejected
   - Names > 256 chars rejected

9. **test_container_lifecycle** - Complete workflow
   - Full lifecycle: create → list → metadata → update → delete → verify

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

### Run All Integration Tests
```bash
cargo test --test swift_container_integration_test -- --ignored --nocapture
```

### Run Specific Test
```bash
cargo test --test swift_container_integration_test test_create_container -- --ignored --nocapture
```

### Run with Serial Execution (recommended)
```bash
# Tests are marked with #[serial] to avoid conflicts
cargo test --test swift_container_integration_test -- --ignored --test-threads=1
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
   - Tests clean up after themselves (delete created containers)

3. **Mock vs Real Testing**
   - For unit tests without a server, use the existing tests in `src/swift/`
   - For real Keystone integration, configure actual Keystone auth
   - Mock tokens work for basic testing without Keystone

## Cleanup

Tests automatically clean up containers they create using:
```rust
let _ = client.delete_container(&container_name).await;
```

If tests fail mid-execution, you may need to manually clean up test containers:
```bash
# List containers
curl -X GET http://localhost:9000/v1/AUTH_test-project-123 \
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

### Debug Mode

Run tests with verbose output:
```bash
RUST_LOG=debug cargo test --test swift_container_integration_test -- --ignored --nocapture
```

## Future Enhancements

Phase 3 will add:
- Object upload/download tests
- Large object (SLO/DLO) tests
- ACL and permission tests
- Concurrent operation tests
- Performance benchmarks
