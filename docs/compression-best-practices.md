# HTTP Response Compression Best Practices in RustFS

## Overview

This document outlines best practices for HTTP response compression in RustFS, based on lessons learned from fixing the
NoSuchKey error response regression (Issue #901).

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

**Recommended Threshold**: 256 bytes minimum

**Implementation**:

```rust
if let Some(content_length) = response.headers().get(CONTENT_LENGTH) {
    if let Ok(length) = content_length.to_str()?.parse::<u64>()? {
        if length < 256 {
            return false; // Don't compress small responses
        }
    }
}
```

### 3. Maintain Observability

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

**Problem**: Can cause Content-Length mismatches with error responses

### ✅ Using Intelligent Predicates

```rust
// GOOD - Filter based on status and size
.layer(CompressionLayer::new().compress_when(ShouldCompress))
```

### ❌ Ignoring Content-Length Header

```rust
// BAD - Only checking status
fn should_compress(&self, response: &Response<B>) -> bool {
    !response.status().is_client_error()
}
```

**Problem**: May compress tiny responses unnecessarily

### ✅ Checking Both Status and Size

```rust
// GOOD - Multi-criteria decision
fn should_compress(&self, response: &Response<B>) -> bool {
    // Check status
    if response.status().is_error() { return false; }

    // Check size
    if get_content_length(response) < 256 { return false; }

    true
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

### Updating Existing Code

If you're adding compression to an existing service:

1. **Start Conservative**: Only compress responses > 1KB
2. **Monitor Impact**: Watch CPU and latency metrics
3. **Lower Threshold Gradually**: Test with smaller thresholds
4. **Always Exclude Errors**: Never compress 4xx/5xx

### Rollout Strategy

1. **Stage 1**: Deploy to canary (5% traffic)
    - Monitor for 24 hours
    - Check error rates and latency

2. **Stage 2**: Expand to 25% traffic
    - Monitor for 48 hours
    - Validate compression ratios

3. **Stage 3**: Full rollout (100% traffic)
    - Continue monitoring for 1 week
    - Document any issues

## Related Documentation

- [Fix NoSuchKey Regression](./fix-nosuchkey-regression.md)
- [tower-http Compression](https://docs.rs/tower-http/latest/tower_http/compression/)
- [HTTP Content-Encoding](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Encoding)

## References

1. Issue #901: NoSuchKey error response regression
2. [Google Web Fundamentals - Text Compression](https://web.dev/reduce-network-payloads-using-text-compression/)
3. [AWS Best Practices - Response Compression](https://docs.aws.amazon.com/whitepapers/latest/s3-optimizing-performance-best-practices/)

---

**Last Updated**: 2025-11-24  
**Maintainer**: RustFS Team
