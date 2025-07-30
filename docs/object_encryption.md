# Object Encryption Documentation

## Overview

RustFS provides comprehensive object encryption capabilities that ensure data confidentiality and integrity for stored objects. The encryption system supports multiple algorithms, integrates with Key Management Service (KMS), and provides transparent encryption/decryption operations.

## Architecture

### Core Components

1. **KMS Integration**
   - Local KMS client for development
   - HashiCorp Vault integration for production
   - Extensible interface for other KMS providers

2. **Encryption Service**
   - Object-level encryption/decryption
   - Streaming encryption for large objects
   - Multiple algorithm support (AES-256, ChaCha20-Poly1305)

3. **Bucket Configuration**
   - Per-bucket encryption settings
   - Default encryption policies
   - Algorithm and key management configuration

4. **Performance Optimizations**
   - Caching for frequently used keys
   - Parallel processing for large objects
   - Connection pooling for KMS operations

### Encryption Flow

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Client    │───▶│   RustFS    │───▶│     KMS     │
│             │    │   Server    │    │             │
└─────────────┘    └─────────────┘    └─────────────┘
                           │
                           ▼
                   ┌─────────────┐
                   │  Encrypted  │
                   │   Storage   │
                   └─────────────┘
```

1. Client uploads object to RustFS
2. RustFS requests data encryption key from KMS
3. Object is encrypted using the data key
4. Encrypted object and metadata stored
5. Data key is encrypted and stored with metadata

## Configuration

### Bucket Encryption Configuration

```rust
use rustfs_kms::{BucketEncryptionConfig, EncryptionAlgorithm};

let config = BucketEncryptionConfig {
    enabled: true,
    algorithm: EncryptionAlgorithm::AES256,
    kms_key_id: Some("my-kms-key-id".to_string()),
    encryption_context: Some({
        let mut context = std::collections::HashMap::new();
        context.insert("bucket".to_string(), "my-bucket".to_string());
        context.insert("purpose".to_string(), "data-protection".to_string());
        context
    }),
};
```

### KMS Configuration

```rust
use rustfs_kms::KmsConfig;

let kms_config = KmsConfig {
    provider: "vault".to_string(),
    endpoint: Some("https://vault.example.com".to_string()),
    region: Some("us-east-1".to_string()),
    access_key_id: Some("your-access-key".to_string()),
    secret_access_key: Some("your-secret-key".to_string()),
    timeout_secs: 30,
    max_retries: 3,
};
```

## Usage Examples

### Basic Object Encryption

```rust
use rustfs_kms::{ObjectEncryptionService, EncryptionAlgorithm};
use std::io::Cursor;

// Setup encryption service
let encryption_service = ObjectEncryptionService::new(kms_manager);

// Encrypt object
let data = Cursor::new(b"Hello, encrypted world!".to_vec());
let result = encryption_service
    .encrypt_object(
        data,
        23, // data length
        EncryptionAlgorithm::AES256,
        Some("my-kms-key-id"),
        None, // encryption context
    )
    .await?;

// Access encrypted data and metadata
let encrypted_reader = result.reader;
let metadata = result.metadata;
```

### Object Decryption

```rust
use std::collections::HashMap;

// Prepare metadata for decryption
let mut metadata = HashMap::new();
metadata.insert("encrypted_data_key".to_string(), encrypted_data_key);
metadata.insert("algorithm".to_string(), "AES256".to_string());
metadata.insert("iv".to_string(), base64::encode(iv));
metadata.insert("tag".to_string(), base64::encode(tag));

// Decrypt object
let decrypted_reader = encryption_service
    .decrypt_object(encrypted_data_reader, &metadata)
    .await?;
```

### Bucket Encryption Management

```rust
use rustfs_kms::BucketEncryptionManager;

// Setup bucket encryption manager
let bucket_manager = BucketEncryptionManager::new(kms_manager);

// Configure bucket encryption
bucket_manager
    .set_bucket_encryption("my-bucket", config)
    .await?;

// Get bucket encryption configuration
let bucket_config = bucket_manager
    .get_bucket_encryption("my-bucket")
    .await?;

// Disable bucket encryption
bucket_manager
    .delete_bucket_encryption("my-bucket")
    .await?;
```

## Supported Algorithms

### AES-256-GCM
- **Key Size**: 256 bits
- **IV Size**: 96 bits (12 bytes)
- **Tag Size**: 128 bits (16 bytes)
- **Performance**: Excellent on modern CPUs with AES-NI
- **Security**: NIST approved, widely adopted

### ChaCha20-Poly1305
- **Key Size**: 256 bits
- **Nonce Size**: 96 bits (12 bytes)
- **Tag Size**: 128 bits (16 bytes)
- **Performance**: Excellent on CPUs without AES-NI
- **Security**: Modern, secure alternative to AES

## Security Considerations

### Key Management
- Data encryption keys are generated per object
- Keys are encrypted using KMS master keys
- Keys are never stored in plaintext
- Automatic key rotation support

### Encryption Context
- Additional authenticated data (AAD)
- Prevents ciphertext substitution attacks
- Includes bucket and object metadata
- Customizable per use case

### Memory Security
- Sensitive data cleared from memory
- Secure random number generation
- Protection against timing attacks
- Constant-time operations where possible

## Performance Optimization

### Caching

```rust
use rustfs_kms::{KmsCacheManager, CacheConfig};
use std::time::Duration;

let cache_config = CacheConfig {
    data_key_ttl: Duration::from_secs(300),
    bucket_config_ttl: Duration::from_secs(600),
    max_data_keys: 1000,
    max_bucket_configs: 100,
    cleanup_interval: Duration::from_secs(60),
};

let cache_manager = KmsCacheManager::new(cache_config);
```

### Parallel Processing

```rust
use rustfs_kms::{ParallelProcessor, ParallelConfig};

let parallel_config = ParallelConfig {
    max_concurrent_operations: 4,
    chunk_size: 1024 * 1024, // 1MB chunks
    worker_pool_size: 8,
    queue_capacity: 100,
};

let processor = ParallelProcessor::new(parallel_config);
```

### Streaming Encryption

```rust
use rustfs::server::handlers::{StreamingUploadHandler, StreamingConfig};

let streaming_config = StreamingConfig {
    chunk_size: 64 * 1024, // 64KB chunks
    max_concurrent_chunks: 4,
    buffer_size: 256 * 1024, // 256KB buffer
    enable_compression: true,
    bandwidth_limit_mbps: None,
};

let upload_handler = StreamingUploadHandler::new(
    encryption_service,
    streaming_config,
);
```

## Monitoring and Auditing

### Metrics Collection

```rust
use rustfs_kms::{KmsMonitor, MonitoringConfig};

let monitoring_config = MonitoringConfig {
    enable_metrics: true,
    enable_audit_log: true,
    metrics_interval: Duration::from_secs(60),
    audit_log_level: "INFO".to_string(),
    max_audit_entries: 10000,
};

let monitor = KmsMonitor::new(monitoring_config);
```

### Audit Logging

The system automatically logs:
- Encryption/decryption operations
- Key generation and usage
- Configuration changes
- Error conditions
- Performance metrics

## Error Handling

### Common Error Types

```rust
use rustfs_kms::KmsError;

match result {
    Ok(data) => {
        // Handle success
    }
    Err(KmsError::KeyNotFound(key_id)) => {
        // Handle missing key
    }
    Err(KmsError::DecryptionFailed(reason)) => {
        // Handle decryption failure
    }
    Err(KmsError::InvalidConfiguration(msg)) => {
        // Handle configuration error
    }
    Err(KmsError::NetworkError(err)) => {
        // Handle network issues
    }
    Err(err) => {
        // Handle other errors
    }
}
```

### Retry Logic

The system includes automatic retry logic for:
- Transient network errors
- KMS service unavailability
- Rate limiting responses
- Temporary authentication failures

## Testing

### Unit Tests

Run encryption-specific unit tests:

```bash
cargo test --package rustfs-kms
```

### Integration Tests

Run full integration tests:

```bash
cargo test --test integration_encryption_test
```

### Security Tests

Run security-focused tests:

```bash
cargo test --test security_encryption_test
```

### Performance Tests

Run performance benchmarks:

```bash
cargo test --test performance_encryption_test --release
```

## Deployment Considerations

### Production Setup

1. **KMS Configuration**
   - Use production KMS service (HashiCorp Vault, AWS KMS, etc.)
   - Configure proper authentication and authorization
   - Set up key rotation policies
   - Monitor KMS service health

2. **Performance Tuning**
   - Adjust cache sizes based on workload
   - Configure parallel processing limits
   - Monitor memory usage and adjust accordingly
   - Set appropriate timeout values

3. **Security Hardening**
   - Enable audit logging
   - Configure encryption context validation
   - Set up monitoring and alerting
   - Regular security assessments

### Backup and Recovery

- Encrypted objects require access to KMS for decryption
- Ensure KMS keys are properly backed up
- Test recovery procedures regularly
- Document key recovery processes

## Troubleshooting

### Common Issues

1. **Decryption Failures**
   - Verify KMS connectivity
   - Check key permissions
   - Validate metadata integrity
   - Review encryption context

2. **Performance Issues**
   - Monitor KMS response times
   - Check cache hit rates
   - Review parallel processing configuration
   - Analyze memory usage patterns

3. **Configuration Problems**
   - Validate KMS configuration
   - Check bucket encryption settings
   - Verify algorithm support
   - Review timeout settings

### Debug Logging

Enable debug logging for troubleshooting:

```bash
RUST_LOG=rustfs_kms=debug cargo run
```

## API Reference

For detailed API documentation, run:

```bash
cargo doc --package rustfs-kms --open
```

## Contributing

When contributing to the encryption functionality:

1. Follow security best practices
2. Add comprehensive tests
3. Update documentation
4. Consider performance implications
5. Review security implications

See [CONTRIBUTING.md](../CONTRIBUTING.md) for detailed guidelines.