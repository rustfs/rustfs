# KMS Encryption Tests

This directory contains comprehensive end-to-end tests for KMS (Key Management Service) encryption functionality in RustFS.

## Test Modules

### bucket_encryption_config.rs
Tests for bucket-level encryption configuration:
- `test_put_bucket_encryption_sse_s3` - Set bucket encryption with SSE-S3
- `test_put_bucket_encryption_sse_kms` - Set bucket encryption with SSE-KMS
- `test_bucket_default_encryption_inheritance` - Verify objects inherit bucket encryption
- `test_bucket_encryption_override_with_request_headers` - Request headers override bucket defaults
- `test_delete_bucket_encryption` - Remove bucket encryption configuration
- `test_multipart_upload_with_bucket_encryption` - Multipart uploads with bucket encryption
- `test_copy_object_with_bucket_encryption` - Copy objects with bucket encryption

### s3_encryption.rs
Tests for S3 server-side encryption:
- SSE-S3 encryption tests
- SSE-KMS encryption tests
- SSE-C encryption tests
- Multipart upload encryption tests

### encryption_key_management.rs
Tests for encryption key management operations.

### encryption_security.rs
Tests for encryption security and validation.

## Running Tests

### Prerequisites
1. Start RustFS server at `localhost:9000`
2. Ensure the server is configured with KMS support

### Run All KMS Tests
```bash
cargo test --package e2e_test kms -- --nocapture
```

### Run Specific Test Module
```bash
# Bucket encryption configuration tests
cargo test --package e2e_test bucket_encryption_config -- --nocapture

# S3 encryption tests
cargo test --package e2e_test s3_encryption -- --nocapture
```

### Run Individual Tests
```bash
# Test bucket encryption with SSE-S3
cargo test --package e2e_test test_put_bucket_encryption_sse_s3 -- --nocapture

# Test bucket encryption inheritance
cargo test --package e2e_test test_bucket_default_encryption_inheritance -- --nocapture
```

## Test Configuration

Tests use the following default configuration:
- RustFS server: `http://localhost:9000`
- Credentials: `rustfsadmin` / `rustfsadmin`
- Region: `us-east-1`

## Test Features

### Bucket Encryption Configuration
- **PUT/GET/DELETE bucket encryption**: Complete CRUD operations for bucket encryption settings
- **Algorithm support**: SSE-S3 and SSE-KMS encryption algorithms
- **KMS key management**: Custom KMS key ID specification
- **Inheritance testing**: Verify objects inherit bucket-level encryption settings
- **Override behavior**: Request headers take precedence over bucket defaults

### Object Operations with Encryption
- **PUT object**: Objects inherit bucket encryption when no explicit headers provided
- **Copy object**: Destination bucket encryption applies to copied objects
- **Multipart upload**: Large file uploads respect bucket encryption settings
- **Data integrity**: All tests verify encrypted data can be correctly decrypted

### Error Handling
- **Invalid configurations**: Tests handle malformed encryption configurations
- **Missing encryption**: Tests verify behavior when encryption is not configured
- **Permission errors**: Tests handle KMS permission issues

## Notes

- All tests are marked with `#[ignore]` and require a running RustFS server
- Tests use unique bucket names with timestamps to avoid conflicts
- Cleanup is performed after each test to maintain test isolation
- Tests verify both encryption metadata and data integrity