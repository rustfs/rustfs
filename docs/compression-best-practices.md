# HTTP Response Compression Best Practices in RustFS

## Overview

This document outlines best practices for HTTP response compression in RustFS, based on lessons learned from fixing the
NoSuchKey error response regression (Issue #901) and the whitelist-based compression redesign (Issue #902).

## Whitelist-Based Compression (Issue #902)

### Design Philosophy

After Issue #901, we identified that the blacklist approach (compress everything except known problematic types) was
still causing issues with browser downloads showing "unknown file size". In Issue #902, we redesigned the compression
system using a **whitelist approach** aligned with MinIO's behavior:

1. **Compression is disabled by default** - Opt-in rather than opt-out
2. **Only explicitly configured content types are compressed** - Preserves Content-Length for all other responses
3. **Fine-grained configuration** - Control via file extensions, MIME types, and size thresholds
4. **Skip already-encoded content** - Avoid double compression

### Configuration Options

RustFS provides flexible compression configuration via environment variables and command-line arguments:

| Environment Variable | CLI Argument | Default | Description |
|---------------------|--------------|---------|-------------|
| `RUSTFS_COMPRESS_ENABLE` |  | `false` | Enable/disable compression |
| `RUSTFS_COMPRESS_EXTENSIONS` |  | `""` | File extensions to compress (e.g., `.txt,.log,.csv`) |
| `RUSTFS_COMPRESS_MIME_TYPES` |  | `text/*,application/json,...` | MIME types to compress (supports wildcards) |
| `RUSTFS_COMPRESS_MIN_SIZE` |  | `1000` | Minimum file size (bytes) for compression |

### Usage Examples

```bash
# Enable compression for text files and JSON
RUSTFS_COMPRESS_ENABLE=on \
RUSTFS_COMPRESS_EXTENSIONS=.txt,.log,.csv,.json,.xml \
RUSTFS_COMPRESS_MIME_TYPES=text/*,application/json,application/xml \
RUSTFS_COMPRESS_MIN_SIZE=1000 \
rustfs /data

# Or using command-line arguments
rustfs /data \
  --compress-enable \
  --compress-extensions ".txt,.log,.csv" \
  --compress-mime-types "text/*,application/json" \
  --compress-min-size 1000
```

### Implementation Details

The `CompressionPredicate` implements intelligent compression decisions:

```rust
impl Predicate for CompressionPredicate {
    fn should_compress<B>(&self, response: &Response<B>) -> bool {
        // 1. Check if compression is enabled
        if !self.config.enabled { return false; }

        // 2. Never compress error responses
        if status.is_client_error() || status.is_server_error() { return false; }

        // 3. Skip already-encoded content (gzip, br, deflate, etc.)
        if has_content_encoding(response) { return false; }

        // 4. Check minimum size threshold
        if content_length < self.config.min_size { return false; }

        // 5. Check whitelist: extension OR MIME type must match
        if matches_extension(response) || matches_mime_type(response) {
            return true;
        }

        // 6. Default: don't compress (whitelist approach)
        false
    }
}
```

### Benefits of Whitelist Approach

| Aspect | Blacklist (Old) | Whitelist (New) |
|--------|-----------------|-----------------|
| Default behavior | Compress most content | No compression |
| Content-Length | Often removed | Preserved for unmatched types |
| Browser downloads | "Unknown file size" | Accurate file size shown |
| Configuration | Complex exclusion rules | Simple inclusion rules |
| MinIO compatibility | Different behavior | Aligned behavior |

## Key Principles

### 1. Never Compress Error Responses

**Rationale**: Error responses are typically small (100-500 bytes) and need to be transmitted accurately. Compression
can:

- Introduce Content-Length header mismatches
- Add unnecessary overhead for small payloads
- Potentially corrupt error details during buffering

**Implementation**:

```rust
// Always check status code first
if status.is_client_error() || status.is_server_error() {
    return false; // Don't compress
}
```

**Affected Status Codes**:

- 4xx Client Errors (400, 403, 404, etc.)
- 5xx Server Errors (500, 502, 503, etc.)

### 2. Size-Based Compression Threshold

**Rationale**: Compression has overhead in terms of CPU and potentially network roundtrips. For very small responses:

- Compression overhead > space savings
- May actually increase payload size
- Adds latency without benefit

**Recommended Threshold**: 1000 bytes minimum (configurable via `RUSTFS_COMPRESS_MIN_SIZE`)

**Implementation**:

```rust
if let Some(content_length) = response.headers().get(CONTENT_LENGTH) {
    if let Ok(length) = content_length.to_str()?.parse::<u64>()? {
        if length < self.config.min_size {
            return false; // Don't compress small responses
        }
    }
}
```

### 3. Skip Already-Encoded Content

**Rationale**: If the response already has a `Content-Encoding` header (e.g., gzip, br, deflate, zstd), the content
is already compressed. Re-compressing provides no benefit and may cause issues:

- Double compression wastes CPU cycles
- May corrupt data or increase size
- Breaks decompression on client side

**Implementation**:

```rust
// Skip if content is already encoded (e.g., gzip, br, deflate, zstd)
if let Some(content_encoding) = response.headers().get(CONTENT_ENCODING) {
    if let Ok(encoding) = content_encoding.to_str() {
        let encoding_lower = encoding.to_lowercase();
        // "identity" means no encoding, so we can still compress
        if encoding_lower != "identity" && !encoding_lower.is_empty() {
            debug!("Skipping compression for already encoded response: {}", encoding);
            return false;
        }
    }
}
```

**Common Content-Encoding Values**:

- `gzip` - GNU zip compression
- `br` - Brotli compression
- `deflate` - Deflate compression
- `zstd` - Zstandard compression
- `identity` - No encoding (compression allowed)

### 4. Maintain Observability

**Rationale**: Compression decisions can affect debugging and troubleshooting. Always log when compression is skipped.

**Implementation**:

```rust
debug!(
    "Skipping compression for error response: status={}",
    status.as_u16()
);
```

**Log Analysis**:

```bash
# Monitor compression decisions
RUST_LOG=rustfs::server::http=debug ./target/release/rustfs

# Look for patterns
grep "Skipping compression" logs/rustfs.log | wc -l
```

## Common Pitfalls

### ❌ Compressing All Responses Blindly

```rust
// BAD - No filtering
.layer(CompressionLayer::new())
```

**Problem**: Can cause Content-Length mismatches with error responses and browser download issues

### ❌ Using Blacklist Approach

```rust
// BAD - Blacklist approach (compress everything except...)
fn should_compress(&self, response: &Response<B>) -> bool {
    // Skip images, videos, archives...
    if is_already_compressed_type(content_type) { return false; }
    true  // Compress everything else
}
```

**Problem**: Removes Content-Length for many file types, causing "unknown file size" in browsers

### ✅ Using Whitelist-Based Predicate

```rust
// GOOD - Whitelist approach with configurable predicate
.layer(CompressionLayer::new().compress_when(CompressionPredicate::new(config)))
```

### ❌ Ignoring Content-Encoding Header

```rust
// BAD - May double-compress already compressed content
fn should_compress(&self, response: &Response<B>) -> bool {
    matches_mime_type(response)  // Missing Content-Encoding check
}
```

**Problem**: Double compression wastes CPU and may corrupt data

### ✅ Comprehensive Checks

```rust
// GOOD - Multi-criteria whitelist decision
fn should_compress(&self, response: &Response<B>) -> bool {
    // 1. Must be enabled
    if !self.config.enabled { return false; }

    // 2. Skip error responses
    if response.status().is_error() { return false; }

    // 3. Skip already-encoded content
    if has_content_encoding(response) { return false; }

    // 4. Check minimum size
    if get_content_length(response) < self.config.min_size { return false; }

    // 5. Must match whitelist (extension OR MIME type)
    matches_extension(response) || matches_mime_type(response)
}
```

## Performance Considerations

### CPU Usage

- **Compression CPU Cost**: ~1-5ms for typical responses
- **Benefit**: 70-90% size reduction for text/json
- **Break-even**: Responses > 512 bytes on fast networks

### Network Latency

- **Savings**: Proportional to size reduction
- **Break-even**: ~256 bytes on typical connections
- **Diminishing Returns**: Below 128 bytes

### Memory Usage

- **Buffer Size**: Usually 4-16KB per connection
- **Trade-off**: Memory vs. bandwidth
- **Recommendation**: Profile in production

## Testing Guidelines

### Unit Tests

Test compression predicate logic:

```rust
#[test]
fn test_should_not_compress_errors() {
    let predicate = ShouldCompress;
    let response = Response::builder()
        .status(404)
        .body(())
        .unwrap();

    assert!(!predicate.should_compress(&response));
}

#[test]
fn test_should_not_compress_small_responses() {
    let predicate = ShouldCompress;
    let response = Response::builder()
        .status(200)
        .header(CONTENT_LENGTH, "100")
        .body(())
        .unwrap();

    assert!(!predicate.should_compress(&response));
}
```

### Integration Tests

Test actual S3 API responses:

```rust
#[tokio::test]
async fn test_error_response_not_truncated() {
    let response = client
        .get_object()
        .bucket("test")
        .key("nonexistent")
        .send()
        .await;

    // Should get proper error, not truncation error
    match response.unwrap_err() {
        SdkError::ServiceError(err) => {
            assert!(err.is_no_such_key());
        }
        other => panic!("Expected ServiceError, got {:?}", other),
    }
}
```

## Monitoring and Alerts

### Metrics to Track

1. **Compression Ratio**: `compressed_size / original_size`
2. **Compression Skip Rate**: `skipped_count / total_count`
3. **Error Response Size Distribution**
4. **CPU Usage During Compression**

### Alert Conditions

```yaml
# Prometheus alert rules
- alert: HighCompressionSkipRate
  expr: |
    rate(http_compression_skipped_total[5m]) 
    / rate(http_responses_total[5m]) > 0.5
  annotations:
    summary: "More than 50% of responses skipping compression"

- alert: LargeErrorResponses
  expr: |
    histogram_quantile(0.95, 
      rate(http_error_response_size_bytes_bucket[5m])) > 1024
  annotations:
    summary: "Error responses larger than 1KB"
```

## Migration Guide

### Migrating from Blacklist to Whitelist Approach

If you're upgrading from an older RustFS version with blacklist-based compression:

1. **Compression is now disabled by default**
   - Set `RUSTFS_COMPRESS_ENABLE=on` to enable
   - This ensures backward compatibility for existing deployments

2. **Configure your whitelist**
   ```bash
   # Example: Enable compression for common text formats
   RUSTFS_COMPRESS_ENABLE=on
   RUSTFS_COMPRESS_EXTENSIONS=.txt,.log,.csv,.json,.xml,.html,.css,.js
   RUSTFS_COMPRESS_MIME_TYPES=text/*,application/json,application/xml,application/javascript
   RUSTFS_COMPRESS_MIN_SIZE=1000
   ```

3. **Verify browser downloads**
   - Check that file downloads show accurate file sizes
   - Verify Content-Length headers are preserved for non-compressed content

### Updating Existing Code

If you're adding compression to an existing service:

1. **Start with compression disabled** (default)
2. **Define your whitelist**: Identify content types that benefit from compression
3. **Set appropriate thresholds**: Start with 1KB minimum size
4. **Enable and monitor**: Watch CPU, latency, and download behavior

### Rollout Strategy

1. **Stage 1**: Deploy to canary (5% traffic)
    - Monitor for 24 hours
    - Check error rates and latency
    - Verify browser download behavior

2. **Stage 2**: Expand to 25% traffic
    - Monitor for 48 hours
    - Validate compression ratios
    - Check Content-Length preservation

3. **Stage 3**: Full rollout (100% traffic)
    - Continue monitoring for 1 week
    - Document any issues
    - Fine-tune whitelist based on actual usage

## Related Documentation

- [Fix NoSuchKey Regression](./fix-nosuchkey-regression.md)
- [tower-http Compression](https://docs.rs/tower-http/latest/tower_http/compression/)
- [HTTP Content-Encoding](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Encoding)

## Architecture

### Module Structure

The compression functionality is organized in a dedicated module for maintainability:

```
rustfs/src/server/
├── compress.rs        # Compression configuration and predicate
├── http.rs            # HTTP server (uses compress module)
└── mod.rs             # Module declarations
```

### Key Components

1. **`CompressionConfig`** - Stores compression settings parsed from environment/CLI
2. **`CompressionPredicate`** - Implements `tower_http::compression::predicate::Predicate`
3. **Configuration Constants** - Defined in `crates/config/src/constants/compress.rs`

## References

1. Issue #901: NoSuchKey error response regression
2. Issue #902: Whitelist-based compression redesign
3. [Google Web Fundamentals - Text Compression](https://web.dev/reduce-network-payloads-using-text-compression/)
4. [AWS Best Practices - Response Compression](https://docs.aws.amazon.com/whitepapers/latest/s3-optimizing-performance-best-practices/)

---

**Last Updated**: 2025-12-13  
**Maintainer**: RustFS Team
