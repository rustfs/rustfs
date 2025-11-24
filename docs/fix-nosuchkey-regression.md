# Fix for NoSuchKey Error Response Regression (Issue #901)

## Problem Statement

In RustFS version 1.0.69, a regression was introduced where attempting to download a non-existent or deleted object would return a networking error instead of the expected `NoSuchKey` S3 error:

```
Expected: Aws::S3::Errors::NoSuchKey
Actual: Seahorse::Client::NetworkingError: "http response body truncated, expected 119 bytes, received 0 bytes"
```

## Root Cause Analysis

The issue was caused by the `CompressionLayer` middleware being applied to **all** HTTP responses, including S3 error responses. The sequence of events that led to the bug:

1. Client requests a non-existent object via `GetObject`
2. RustFS determines the object doesn't exist
3. The s3s library generates a `NoSuchKey` error response (XML format, ~119 bytes)
4. HTTP headers are written, including `Content-Length: 119`
5. The `CompressionLayer` attempts to compress the error response body
6. Due to compression buffering or encoding issues with small payloads, the body becomes empty (0 bytes)
7. The client receives `Content-Length: 119` but the actual body is 0 bytes
8. AWS SDK throws a "truncated body" networking error instead of parsing the S3 error

## Solution

The fix implements an intelligent compression predicate (`ShouldCompress`) that excludes certain responses from compression:

### Exclusion Criteria

1. **Error Responses (4xx and 5xx)**: Never compress error responses to ensure error details are preserved and transmitted accurately
2. **Small Responses (< 256 bytes)**: Skip compression for very small responses where compression overhead outweighs benefits

### Implementation Details

```rust
impl Predicate for ShouldCompress {
    fn should_compress<B>(&self, response: &Response<B>) -> bool
    where
        B: http_body::Body,
    {
        let status = response.status();
        
        // Never compress error responses (4xx and 5xx status codes)
        if status.is_client_error() || status.is_server_error() {
            debug!("Skipping compression for error response: status={}", status.as_u16());
            return false;
        }
        
        // Check Content-Length header to avoid compressing very small responses
        if let Some(content_length) = response.headers().get(http::header::CONTENT_LENGTH) {
            if let Ok(length_str) = content_length.to_str() {
                if let Ok(length) = length_str.parse::<u64>() {
                    if length < 256 {
                        debug!("Skipping compression for small response: size={} bytes", length);
                        return false;
                    }
                }
            }
        }
        
        // Compress successful responses with sufficient size
        true
    }
}
```

## Benefits

1. **Correctness**: Error responses are now transmitted with accurate Content-Length headers
2. **Compatibility**: AWS SDKs and other S3 clients correctly receive and parse error responses
3. **Performance**: Small responses avoid unnecessary compression overhead
4. **Observability**: Debug logging provides visibility into compression decisions

## Testing

Comprehensive test coverage was added to prevent future regressions:

### Test Cases

1. **`test_get_deleted_object_returns_nosuchkey`**: Verifies that getting a deleted object returns NoSuchKey
2. **`test_head_deleted_object_returns_nosuchkey`**: Verifies HeadObject also returns NoSuchKey for deleted objects
3. **`test_get_nonexistent_object_returns_nosuchkey`**: Tests objects that never existed
4. **`test_multiple_gets_deleted_object`**: Ensures stability across multiple consecutive requests

### Running Tests

```bash
# Run the specific test
cargo test --test get_deleted_object_test -- --ignored

# Or start RustFS server and run tests
./scripts/dev_rustfs.sh
cargo test --test get_deleted_object_test
```

## Impact Assessment

### Affected APIs

- `GetObject`
- `HeadObject`
- Any S3 API that returns 4xx/5xx error responses

### Backward Compatibility

- **No breaking changes**: The fix only affects error response handling
- **Improved compatibility**: Better alignment with S3 specification and AWS SDK expectations
- **No performance degradation**: Small responses were already not compressed by default in most cases

## Deployment Considerations

### Verification Steps

1. Deploy the fix to a staging environment
2. Run the provided Ruby reproduction script to verify the fix
3. Monitor error logs for any compression-related warnings
4. Verify that large successful responses are still being compressed

### Monitoring

Enable debug logging to observe compression decisions:

```bash
RUST_LOG=rustfs::server::http=debug
```

Look for log messages like:
- `Skipping compression for error response: status=404`
- `Skipping compression for small response: size=119 bytes`

## Related Issues

- Issue #901: Regression in exception when downloading non-existent key in alpha 69
- Commit: 86185703836c9584ba14b1b869e1e2c4598126e0 (getobjectlength fix)

## References

- [AWS S3 Error Responses](https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html)
- [tower-http CompressionLayer](https://docs.rs/tower-http/latest/tower_http/compression/index.html)
- [s3s Library](https://github.com/Nugine/s3s)
