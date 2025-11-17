# KMS End-to-End Tests

This directory contains the integration suites used to validate the full RustFS KMS (Key Management Service) workflow.

## 📁 Test Overview

### `kms_local_test.rs`
End-to-end coverage for the local KMS backend:
- Auto-start and configure the local backend
- Configure KMS through the dynamic configuration API
- Verify SSE-C (client-provided keys)
- Exercise S3-compatible encryption/decryption
- Validate key lifecycle management

### `kms_vault_test.rs`
End-to-end coverage for the Vault backend:
- Launch a Vault dev server automatically
- Configure the transit engine and encryption keys
- Configure KMS via the dynamic configuration API
- Run the full Vault integration flow
- Validate token authentication and encryption operations

### `kms_comprehensive_test.rs`
**Full KMS capability suite** (currently disabled because of AWS SDK compatibility issues):
- **Bucket encryption configuration**: SSE-S3 and SSE-KMS defaults
- **All SSE encryption modes**:
  - SSE-S3 (S3-managed server-side encryption)
  - SSE-KMS (KMS-managed server-side encryption)
  - SSE-C (client-provided keys)
- **Object operations**: upload, download, and validation for every SSE mode
- **Multipart uploads**: cover each SSE mode
- **Object replication**: cross-mode replication scenarios
- **Complete KMS API management**:
  - Key lifecycle (create, list, describe, delete, cancel delete)
  - Direct encrypt/decrypt operations
  - Data key generation and handling
  - KMS service lifecycle (start, stop, status)

### `kms_integration_test.rs`
Broad integration tests that exercise:
- Multiple backends
- KMS lifecycle management
- Error handling and recovery
- **Note**: currently disabled because of AWS SDK compatibility gaps

## 🚀 Running Tests

### Prerequisites

1. **System dependencies**
   ```bash
   # macOS
   brew install vault awscurl

   # Ubuntu/Debian
   apt-get install vault
   pip install awscurl
   ```

2. **Build RustFS**
   ```bash
   cargo build
   ```

### Run individual suites

#### Local backend
```bash
cd crates/e2e_test
cargo test test_local_kms_end_to_end -- --nocapture
```

#### Vault backend
```bash
cd crates/e2e_test
cargo test test_vault_kms_end_to_end -- --nocapture
```

#### High availability
```bash
cd crates/e2e_test
cargo test test_vault_kms_high_availability -- --nocapture
```

#### Comprehensive features (disabled)
```bash
cd crates/e2e_test
# Disabled due to AWS SDK compatibility gaps
# cargo test test_comprehensive_kms_functionality -- --nocapture
# cargo test test_sse_modes_compatibility -- --nocapture
# cargo test test_kms_api_comprehensive -- --nocapture
```

### Run all KMS suites
```bash
cd crates/e2e_test
cargo test kms -- --nocapture
```

### Run serially (avoid port conflicts)
```bash
cd crates/e2e_test
cargo test kms -- --nocapture --test-threads=1
```

## 🔧 Configuration

### Environment variables
```bash
# Optional: custom RustFS port (default 9050)
export RUSTFS_TEST_PORT=9050

# Optional: custom Vault port (default 8200)
export VAULT_TEST_PORT=8200

# Optional: enable verbose logging
export RUST_LOG=debug
```

### Required binaries

Tests look for:
- `../../target/debug/rustfs` – RustFS server
- `vault` – Vault CLI (must be on PATH)
- `/Users/dandan/Library/Python/3.9/bin/awscurl` – AWS SigV4 helper

## 📋 Test Flow

### Local backend
1. **Prepare environment** – create temporary directories and key storage paths
2. **Start RustFS** – launch the server with KMS enabled
3. **Wait for readiness** – confirm the port listener and S3 API
4. **Configure KMS** – send configuration via awscurl to the admin API
5. **Start KMS** – activate the KMS service
6. **Exercise functionality**
   - Create a test bucket
   - Run SSE-C encryption with client-provided keys
   - Validate encryption/decryption behavior
7. **Cleanup** – stop processes and remove temporary files

### Vault backend
1. **Launch Vault** – start the dev-mode server
2. **Configure Vault**
   - Enable the transit secrets engine
   - Create the `rustfs-master-key`
3. **Start RustFS** – run the server with KMS enabled
4. **Configure KMS** – point RustFS at Vault (address, token, transit config, key path)
5. **Exercise functionality** – complete the encryption/decryption workflow
6. **Cleanup** – stop all services

## 🛠️ Troubleshooting

### Common issues

**Q: `RustFS server failed to become ready`**
```bash
lsof -i :9050
kill -9 <PID>  # Free the port if necessary
```

**Q: Vault fails to start**
```bash
which vault
vault version
```

**Q: awscurl authentication fails**
```bash
ls /Users/dandan/Library/Python/3.9/bin/awscurl
# Or install elsewhere
pip install awscurl
which awscurl  # Update the path in tests accordingly
```

**Q: Tests time out**
```bash
RUST_LOG=debug cargo test test_local_kms_end_to_end -- --nocapture
```

### Debug tips

1. **Enable verbose logs**
   ```bash
   RUST_LOG=rustfs_kms=debug,rustfs=info cargo test -- --nocapture
   ```

2. **Keep temporary files** – comment out cleanup logic to inspect generated configs

3. **Pause execution** – add `std::thread::sleep` for manual inspection during tests

4. **Monitor ports**
   ```bash
   netstat -an | grep 9050
   curl http://127.0.0.1:9050/minio/health/ready
   ```

## 📊 Coverage

### Functional
- ✅ Dynamic KMS configuration
- ✅ Local and Vault backends
- ✅ AWS S3-compatible encryption APIs
- ✅ Key lifecycle management
- ✅ Error handling and recovery paths
- ✅ High-availability behavior

### Encryption modes
- ✅ SSE-C (customer-provided)
- ✅ SSE-S3 (S3-managed)
- ✅ SSE-KMS (KMS-managed)

### S3 operations
- ✅ Object upload/download (SSE-C)
- 🚧 Multipart uploads (pending AWS SDK fixes)
- 🚧 Object replication (pending AWS SDK fixes)
- 🚧 Bucket encryption defaults (pending AWS SDK fixes)

### KMS API
- ✅ Basic key management (create/list)
- 🚧 Full key lifecycle (pending AWS SDK fixes)
- 🚧 Direct encrypt/decrypt (pending AWS SDK fixes)
- 🚧 Data key operations (pending AWS SDK fixes)
- ✅ Service lifecycle (configure/start/stop/status)

### Authentication
- ✅ Vault token auth
- 🚧 Vault AppRole auth

## 🔄 CI Integration

Designed to run inside CI/CD pipelines:

```yaml
- name: Run KMS E2E Tests
  run: |
    sudo apt-get update
    sudo apt-get install -y vault
    pip install awscurl

    cargo build
    cd crates/e2e_test
    cargo test kms -- --nocapture --test-threads=1
```

## 📚 References

- [KMS configuration guide](../../../../docs/kms/README.md)
- [Dynamic configuration API](../../../../docs/kms/http-api.md)
- [Troubleshooting](../../../../docs/kms/troubleshooting.md)

---

*These suites ensure KMS stability and reliability, building confidence for production deployments.*
