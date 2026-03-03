# Swift Range Requests Implementation

## Overview

HTTP Range requests allow clients to request specific byte ranges of an object, which is essential for:
- Resumable downloads
- Streaming media (seeking to specific positions)
- Downloading large files in chunks
- Bandwidth optimization

## Implementation Status

### ✅ Completed

1. **Range Header Parsing** (`parse_range_header()`)
   - Parses standard HTTP Range header format
   - Supports all range types: regular, open-ended, and suffix
   - Comprehensive validation and error handling

2. **Content-Range Formatting** (`format_content_range()`)
   - Formats Content-Range response headers
   - Standard HTTP format: "bytes start-end/total"

3. **Get Object with Range** (`get_object()`)
   - Updated to accept optional HTTPRangeSpec parameter
   - Passes range to storage layer for efficient partial reads
   - Storage layer handles actual byte range extraction

4. **Unit Tests** (19 tests passing)
   - Regular range parsing
   - Open-ended range parsing
   - Suffix range parsing
   - Invalid format detection
   - Content-Range formatting

### 🚧 Pending Handler Integration

Handler needs to be updated to:
1. Extract Range header from HTTP request
2. Parse it using `parse_range_header()`
3. Pass parsed range to `get_object()`
4. Add Content-Range and Content-Length headers to response
5. Return 206 Partial Content status instead of 200 OK

## HTTP Range Specification

### Range Header Format

The Range header uses the format: `Range: bytes=<start>-<end>`

### Supported Range Types

#### 1. Regular Range (Specific Bytes)
```http
Range: bytes=0-1023
```
- Returns bytes 0 through 1023 (inclusive)
- Both start and end are specified
- End must be >= start

**Example:**
```rust
let range = parse_range_header("bytes=0-1023")?;
// range.start = 0, range.end = 1023, range.is_suffix_length = false
```

#### 2. Open-Ended Range (From Position to End)
```http
Range: bytes=1000-
```
- Returns from byte 1000 to end of file
- Only start is specified, end is omitted
- Useful for resuming downloads

**Example:**
```rust
let range = parse_range_header("bytes=1000-")?;
// range.start = 1000, range.end = -1, range.is_suffix_length = false
```

#### 3. Suffix Range (Last N Bytes)
```http
Range: bytes=-500
```
- Returns the last 500 bytes
- Only length is specified (negative)
- Useful for tailing log files

**Example:**
```rust
let range = parse_range_header("bytes=-500")?;
// range.start = -500, range.end = -1, range.is_suffix_length = true
```

## Response Headers

### Successful Range Request (206 Partial Content)

```http
HTTP/1.1 206 Partial Content
Content-Type: application/octet-stream
Content-Length: 1024
Content-Range: bytes 0-1023/5000
ETag: "abc123def456"
Last-Modified: Wed, 27 Feb 2026 12:00:00 GMT
X-Trans-Id: tx000000000000000001
X-OpenStack-Request-Id: tx000000000000000001
```

**Key Headers:**
- `206 Partial Content` - Status code for range responses
- `Content-Length` - Size of the partial content (not total size)
- `Content-Range` - Describes which bytes are being returned

### Full Object Request (200 OK)

```http
HTTP/1.1 200 OK
Content-Type: application/octet-stream
Content-Length: 5000
ETag: "abc123def456"
Last-Modified: Wed, 27 Feb 2026 12:00:00 GMT
X-Trans-Id: tx000000000000000001
X-OpenStack-Request-Id: tx000000000000000001
```

### Invalid Range (416 Range Not Satisfiable)

```http
HTTP/1.1 416 Range Not Satisfiable
Content-Range: bytes */5000
Content-Length: 0
```

Returned when:
- Range start is beyond file size
- Range format is invalid
- Requested range is not satisfiable

## Implementation Details

### Function Signatures

```rust
/// Parse HTTP Range header
pub fn parse_range_header(range_str: &str) -> SwiftResult<HTTPRangeSpec>

/// Format Content-Range header
pub fn format_content_range(start: i64, end: i64, total: i64) -> String

/// Get object with optional range
pub async fn get_object(
    account: &str,
    container: &str,
    object: &str,
    credentials: &Credentials,
    range: Option<HTTPRangeSpec>,
) -> SwiftResult<GetObjectReader>
```

### HTTPRangeSpec Structure

```rust
pub struct HTTPRangeSpec {
    pub is_suffix_length: bool,  // true for suffix ranges (bytes=-500)
    pub start: i64,               // Start position or negative length
    pub end: i64,                 // End position or -1 for open-ended
}
```

### Storage Layer Integration

The ECStore storage layer handles actual byte range extraction:
- Efficient partial reads without loading entire object
- Supports compressed objects with range requests
- Handles multi-part objects correctly
- Returns actual bytes read

## Usage Examples

### Download First 1MB

```http
GET /v1/AUTH_project-123/my-container/large-file.bin HTTP/1.1
Host: swift.example.com
X-Auth-Token: token123
Range: bytes=0-1048575
```

**Response:**
```http
HTTP/1.1 206 Partial Content
Content-Length: 1048576
Content-Range: bytes 0-1048575/10485760
Content-Type: application/octet-stream
ETag: "abc123def456"
```

### Resume Download from 5MB

```http
GET /v1/AUTH_project-123/my-container/large-file.bin HTTP/1.1
Host: swift.example.com
X-Auth-Token: token123
Range: bytes=5242880-
```

**Response:**
```http
HTTP/1.1 206 Partial Content
Content-Length: 5242880
Content-Range: bytes 5242880-10485759/10485760
Content-Type: application/octet-stream
ETag: "abc123def456"
```

### Download Last 1KB (Log Tailing)

```http
GET /v1/AUTH_project-123/my-container/application.log HTTP/1.1
Host: swift.example.com
X-Auth-Token: token123
Range: bytes=-1024
```

**Response:**
```http
HTTP/1.1 206 Partial Content
Content-Length: 1024
Content-Range: bytes 999976-1000999/1001000
Content-Type: text/plain
ETag: "abc123def456"
```

## Error Handling

### Invalid Range Format

```rust
parse_range_header("bytes=abc-def")
// Error: SwiftError::BadRequest("Invalid range format: start not a number")

parse_range_header("range=0-100")
// Error: SwiftError::BadRequest("Range header must start with 'bytes='")

parse_range_header("bytes=100-50")
// Error: SwiftError::BadRequest("Invalid range format: end must be >= start")
```

### Range Beyond File Size

The storage layer handles this:
- If start >= file size → 416 Range Not Satisfiable
- If end >= file size → Adjust end to file size - 1
- Content-Range header shows actual bytes returned

## Performance Considerations

### Efficient Partial Reads

1. **No Full Object Load**: Storage layer only reads requested bytes
2. **Streaming**: Data is streamed directly to client, no buffering
3. **Compressed Objects**: Range requests work on decompressed data
4. **Multi-Part Objects**: Ranges correctly span part boundaries

### Use Cases

**Good Use Cases:**
- Resumable downloads (save bandwidth on connection loss)
- Media streaming (seek to specific timestamp)
- Large file downloads in chunks (parallel downloads)
- Log tailing (get last N bytes)
- Preview generation (get first N bytes for file type detection)

**Not Recommended:**
- Many small range requests to same object (use full download instead)
- Random access patterns (cache full object if possible)

## Handler Integration Guide

### Required Changes

**1. Extract Range Header in Handler**

```rust
async fn handle_swift_request(
    route: SwiftRoute,
    credentials: Option<Credentials>,
    headers: HeaderMap,  // ADD THIS
) -> Result<Response<Body>, SwiftError> {
    // ...
    match route {
        SwiftRoute::Object { account, container, object, method } => {
            match method {
                Method::GET => {
                    // Parse Range header if present
                    let range = headers.get("range")
                        .and_then(|v| v.to_str().ok())
                        .map(|s| parse_range_header(s))
                        .transpose()?;

                    // Get object with range
                    let reader = get_object(&account, &container, &object, &credentials, range).await?;

                    // Build response...
                }
            }
        }
    }
}
```

**2. Build Response with Appropriate Headers**

```rust
let status = if range.is_some() {
    StatusCode::PARTIAL_CONTENT  // 206
} else {
    StatusCode::OK  // 200
};

let mut response = Response::builder()
    .status(status)
    .header("content-type", reader.object_info.content_type.unwrap_or_default())
    .header("content-length", actual_content_length.to_string())
    .header("etag", reader.object_info.etag.unwrap_or_default());

// Add Content-Range header for partial content
if let Some(range_spec) = range {
    let (start, length) = range_spec.get_offset_length(reader.object_info.size)?;
    let end = start as i64 + length - 1;
    response = response.header(
        "content-range",
        format_content_range(start as i64, end, reader.object_info.size)
    );
}

response.header("x-trans-id", trans_id)
    .body(Body::from_reader(reader.stream))
    .unwrap()
```

## Testing

### Unit Tests

All unit tests pass (19 tests):
```bash
cargo test --bin rustfs swift::object::tests::test_parse_range
cargo test --bin rustfs swift::object::tests::test_format_content_range
```

### Integration Tests

Example integration test structure (to be implemented once handler is updated):

```rust
#[tokio::test]
#[serial]
#[ignore]
async fn test_range_request_regular() -> Result<()> {
    let client = SwiftClient::new(/* ... */);

    // Upload 5KB object
    let content = vec![b'A'; 5000];
    client.put_object(container, "test.bin", &content, None).await?;

    // Request first 1KB
    let response = client.get_object_range(
        container,
        "test.bin",
        Some("bytes=0-1023")
    ).await?;

    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        response.headers().get("content-range").unwrap(),
        "bytes 0-1023/5000"
    );
    assert_eq!(
        response.headers().get("content-length").unwrap(),
        "1024"
    );

    let body = response.bytes().await?;
    assert_eq!(body.len(), 1024);
    assert!(body.iter().all(|&b| b == b'A'));

    Ok(())
}
```

## References

- [RFC 7233 - HTTP Range Requests](https://tools.ietf.org/html/rfc7233)
- [OpenStack Swift API - GET Object](https://docs.openstack.org/api-ref/object-store/#get-object-content-and-metadata)
- Implementation: `rustfs/src/swift/object.rs::parse_range_header()`
- Implementation: `rustfs/src/swift/object.rs::get_object()`
