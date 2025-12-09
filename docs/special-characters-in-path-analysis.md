# Special Characters in Object Path - Comprehensive Analysis and Solution

## Executive Summary

This document provides an in-depth analysis of the issues with special characters (spaces, plus signs, etc.) in object paths within RustFS, along with a comprehensive solution strategy.

## Problem Statement

### Issue Description

Users encounter problems when working with object paths containing special characters:

**Part A: Spaces in Paths**
```bash
mc cp README.md "local/dummy/a%20f+/b/c/3/README.md"
```
- The UI allows navigation to the folder `%20f+/`
- However, it cannot display the contents within that folder
- CLI tools like `mc ls` correctly show the file exists

**Part B: Plus Signs in Paths**
```
Error: blob (key "/test/data/org_main-org/dashboards/ES+net/LHC+Data+Challenge/firefly-details.json")
api error InvalidArgument: Invalid argument
```
- Files with `+` signs in paths cause 400 (Bad Request) errors
- This affects clients using the Go Cloud Development Kit or similar libraries

## Root Cause Analysis

### URL Encoding in S3 API

According to the AWS S3 API specification:

1. **Object keys in HTTP URLs MUST be URL-encoded**
   - Space character → `%20`
   - Plus sign → `%2B`
   - Literal `+` in URL path → stays as `+` (represents itself, not space)
   
2. **URL encoding rules for S3 paths:**
   - In HTTP URLs: `/bucket/path%20with%20spaces/file%2Bname.txt`
   - Decoded key: `path with spaces/file+name.txt`
   - Note: `+` in URL path represents a literal `+`, NOT a space

3. **Important distinction:**
   - In **query parameters**, `+` represents space (form URL encoding)
   - In **URL paths**, `+` represents a literal plus sign
   - Space in paths must be encoded as `%20`

### The s3s Library Behavior

The s3s library (version 0.12.0-rc.4) handles HTTP request parsing and URL decoding:

1. **Expected behavior**: s3s should URL-decode the path from HTTP requests before passing keys to our handlers
2. **Current observation**: There appears to be inconsistency or a bug in how keys are decoded
3. **Hypothesis**: The library may not be properly handling certain special characters or edge cases

### Where the Problem Manifests

The issue affects multiple operations:

1. **PUT Object**: Uploading files with special characters in path
2. **GET Object**: Retrieving files with special characters
3. **LIST Objects**: Listing directory contents with special characters in path
4. **DELETE Object**: Deleting files with special characters

### Consistency Issues

The core problem is **inconsistency** in how paths are handled:

- **Storage layer**: May store objects with URL-encoded names
- **Retrieval layer**: May expect decoded names
- **Comparison layer**: Path matching fails when encoding differs
- **List operation**: Returns encoded or decoded names inconsistently

## Technical Analysis

### Current Implementation

#### 1. Storage Layer (ecfs.rs)

```rust
// In put_object
let PutObjectInput {
    bucket,
    key,  // ← This comes from s3s, should be URL-decoded
    ...
} = input;

store.put_object(&bucket, &key, &mut reader, &opts).await
```

#### 2. List Objects Implementation

```rust
// In list_objects_v2
let object_infos = store
    .list_objects_v2(
        &bucket,
        &prefix,  // ← Should this be decoded?
        continuation_token,
        delimiter.clone(),
        max_keys,
        fetch_owner.unwrap_or_default(),
        start_after,
        incl_deleted,
    )
    .await
```

#### 3. Object Retrieval

The key (object name) needs to match exactly between:
- How it's stored (during PUT)
- How it's queried (during GET/LIST)
- How it's compared (path matching)

### The URL Encoding Problem

Consider this scenario:

1. Client uploads: `PUT /bucket/a%20f+/file.txt`
2. s3s decodes to: `a f+/file.txt` (correct: %20→space, +→plus)
3. We store as: `a f+/file.txt`
4. Client lists: `GET /bucket?prefix=a%20f+/`
5. s3s decodes to: `a f+/` 
6. We search for: `a f+/`
7. Should work! ✓

But what if s3s is NOT decoding properly? Or decoding inconsistently?

1. Client uploads: `PUT /bucket/a%20f+/file.txt`
2. s3s passes: `a%20f+/file.txt` (BUG: not decoded!)
3. We store as: `a%20f+/file.txt`
4. Client lists: `GET /bucket?prefix=a%20f+/`
5. s3s passes: `a%20f+/`
6. We search for: `a%20f+/`
7. Works by accident! ✓

But then:
8. Client lists: `GET /bucket?prefix=a+f%2B/` (encoding + as %2B)
9. s3s passes: `a+f%2B/` or `a+f+/` ??
10. We search for that, but stored name was `a%20f+/`
11. Mismatch! ✗

## Solution Strategy

### Approach 1: Trust s3s Library (Recommended)

**Assumption**: s3s library correctly URL-decodes all keys from HTTP requests

**Strategy**:
1. Assume all keys received from s3s are already decoded
2. Store objects with decoded names (UTF-8 strings with literal special chars)
3. Use decoded names for all operations (GET, LIST, DELETE)
4. Never manually URL-encode/decode keys in our handlers
5. Trust s3s to handle HTTP-level encoding/decoding

**Advantages**:
- Follows separation of concerns
- Simpler code
- Relies on well-tested library behavior

**Risks**:
- If s3s has a bug, we're affected
- Need to verify s3s actually does this correctly

### Approach 2: Explicit URL Decoding (Defensive)

**Assumption**: s3s may not decode keys properly, or there are edge cases

**Strategy**:
1. Explicitly URL-decode all keys when received from s3s
2. Use `urlencoding::decode()` on all keys in handlers
3. Store and operate on decoded names
4. Add safety checks and error handling

**Implementation**:
```rust
use urlencoding::decode;

// In put_object
let key = decode(&input.key)
    .map_err(|e| s3_error!(InvalidArgument, format!("Invalid URL encoding in key: {}", e)))?
    .into_owned();
```

**Advantages**:
- More defensive
- Explicit control
- Handles s3s bugs or limitations

**Risks**:
- Double-decoding if s3s already decodes
- May introduce new bugs
- More complex code

### Approach 3: Hybrid Strategy (Most Robust)

**Strategy**:
1. Add logging to understand what s3s actually passes us
2. Create tests with various special characters
3. Determine if s3s decodes correctly
4. If yes → use Approach 1
5. If no → use Approach 2 with explicit decoding

## Recommended Implementation Plan

### Phase 1: Investigation & Testing

1. **Create comprehensive tests** for special characters:
   - Spaces (` ` / `%20`)
   - Plus signs (`+` / `%2B`)
   - Percent signs (`%` / `%25`)
   - Slashes in names (usually not allowed, but test edge cases)
   - Unicode characters
   - Mixed special characters

2. **Add detailed logging**:
   ```rust
   debug!("Received key from s3s: {:?}", key);
   debug!("Key bytes: {:?}", key.as_bytes());
   ```

3. **Test with real S3 clients**:
   - AWS SDK
   - MinIO client (mc)
   - Go Cloud Development Kit
   - boto3 (Python)

### Phase 2: Fix Implementation

Based on Phase 1 findings, implement one of:

#### Option A: s3s handles decoding correctly
- Add tests to verify behavior
- Document the assumption
- Add assertions or validation

#### Option B: s3s has bugs or doesn't decode
- Add explicit URL decoding to all handlers
- Use `urlencoding::decode()` consistently
- Add error handling for invalid encoding
- Document the workaround

### Phase 3: Ensure Consistency

1. **Audit all key usage**:
   - PutObject
   - GetObject
   - DeleteObject
   - ListObjects/ListObjectsV2
   - CopyObject (source and destination)
   - HeadObject
   - Multi-part upload operations

2. **Standardize key handling**:
   - Create a helper function `normalize_object_key()`
   - Use it consistently everywhere
   - Add validation

3. **Update path utilities** (`crates/utils/src/path.rs`):
   - Ensure path manipulation functions handle special chars
   - Add tests for path operations with special characters

### Phase 4: Testing & Validation

1. **Unit tests**:
   ```rust
   #[test]
   fn test_object_key_with_space() {
       let key = "path with spaces/file.txt";
       // test PUT, GET, LIST operations
   }

   #[test]
   fn test_object_key_with_plus() {
       let key = "path+with+plus/file+name.txt";
       // test all operations
   }

   #[test]
   fn test_object_key_with_mixed_special_chars() {
       let key = "complex/path with spaces+plus%percent.txt";
       // test all operations
   }
   ```

2. **Integration tests**:
   - Test with real S3 clients
   - Test mc (MinIO client) scenarios from the issue
   - Test Go Cloud Development Kit scenario
   - Test AWS SDK compatibility

3. **Regression testing**:
   - Ensure existing tests still pass
   - Test with normal filenames (no special chars)
   - Test with existing data

## Implementation Details

### Key Functions to Modify

1. **rustfs/src/storage/ecfs.rs**:
   - `put_object()` - line ~2763
   - `get_object()` - find implementation
   - `list_objects_v2()` - line ~2564
   - `delete_object()` - find implementation
   - `copy_object()` - handle source and dest keys
   - `head_object()` - find implementation

2. **Helper function to add**:
```rust
/// Normalizes an object key by ensuring it's properly URL-decoded
/// and contains only valid UTF-8 characters.
///
/// This function should be called on all object keys received from
/// the S3 API to ensure consistent handling of special characters.
fn normalize_object_key(key: &str) -> S3Result<String> {
    // If s3s already decodes, this is a no-op validation
    // If not, this explicitly decodes
    match urlencoding::decode(key) {
        Ok(decoded) => Ok(decoded.into_owned()),
        Err(e) => Err(s3_error!(
            InvalidArgument,
            format!("Invalid URL encoding in object key: {}", e)
        )),
    }
}
```

### Testing Strategy

Create a new test module:

```rust
// crates/e2e_test/src/special_chars_test.rs

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_put_get_object_with_space() {
        // Upload file with space in path
        let bucket = "test-bucket";
        let key = "folder/file with spaces.txt";
        let content = b"test content";
        
        // PUT
        put_object(bucket, key, content).await.unwrap();
        
        // GET
        let retrieved = get_object(bucket, key).await.unwrap();
        assert_eq!(retrieved, content);
        
        // LIST
        let objects = list_objects(bucket, "folder/").await.unwrap();
        assert!(objects.iter().any(|obj| obj.key == key));
    }

    #[tokio::test]
    async fn test_put_get_object_with_plus() {
        let bucket = "test-bucket";
        let key = "folder/ES+net/file+name.txt";
        // ... similar test
    }

    #[tokio::test]
    async fn test_mc_client_scenario() {
        // Reproduce the exact scenario from the issue
        let bucket = "dummy";
        let key = "a f+/b/c/3/README.md"; // Decoded form
        // ... test with mc client or simulate its behavior
    }
}
```

## Edge Cases and Considerations

### 1. Directory Markers

RustFS uses `__XLDIR__` suffix for directories:
- Ensure special characters in directory names are handled
- Test: `"folder with spaces/__XLDIR__"`

### 2. Multipart Upload

- Upload ID and part operations must handle special chars
- Test: Multipart upload of object with special char path

### 3. Copy Operations

CopyObject has both source and destination keys:
```rust
// Both need consistent handling
let src_key = input.copy_source.key();
let dest_key = input.key;
```

### 4. Presigned URLs

If RustFS supports presigned URLs, they need special attention:
- URL encoding in presigned URLs
- Signature calculation with encoded vs decoded keys

### 5. Event Notifications

Events include object keys:
- Ensure event payloads have properly encoded/decoded keys
- Test: Webhook target receives correct key format

### 6. Versioning

Version IDs with special character keys:
- Test: List object versions with special char keys

## Security Considerations

### Path Traversal

Ensure URL decoding doesn't enable path traversal:
```rust
// BAD: Don't allow
key = "../../../etc/passwd"

// After decoding:
key = "..%2F..%2F..%2Fetc%2Fpasswd" → "../../../etc/passwd"

// Solution: Validate decoded keys
fn validate_object_key(key: &str) -> S3Result<()> {
    if key.contains("..") {
        return Err(s3_error!(InvalidArgument, "Invalid object key"));
    }
    if key.starts_with('/') {
        return Err(s3_error!(InvalidArgument, "Object key cannot start with /"));
    }
    Ok(())
}
```

### Null Bytes

Ensure no null bytes in decoded keys:
```rust
if key.contains('\0') {
    return Err(s3_error!(InvalidArgument, "Object key contains null byte"));
}
```

## Testing with Real Clients

### MinIO Client (mc)

```bash
# Test space in path (from issue)
mc cp README.md "local/dummy/a%20f+/b/c/3/README.md"
mc ls "local/dummy/a%20f+/"
mc ls "local/dummy/a%20f+/b/c/3/"

# Test plus in path
mc cp test.txt "local/bucket/ES+net/file+name.txt"
mc ls "local/bucket/ES+net/"

# Test mixed
mc cp data.json "local/bucket/folder%20with%20spaces+plus/file.json"
```

### AWS CLI

```bash
# Upload with space
aws --endpoint-url=http://localhost:9000 s3 cp test.txt "s3://bucket/path with spaces/file.txt"

# List
aws --endpoint-url=http://localhost:9000 s3 ls "s3://bucket/path with spaces/"
```

### Go Cloud Development Kit

```go
import "gocloud.dev/blob"

// Test the exact scenario from the issue
key := "/test/data/org_main-org/dashboards/ES+net/LHC+Data+Challenge/firefly-details.json"
err := bucket.WriteAll(ctx, key, data, nil)
```

## Success Criteria

The fix is successful when:

1. ✅ mc client can upload files with spaces in path
2. ✅ UI correctly displays folders with special characters
3. ✅ UI can list contents of folders with special characters
4. ✅ Files with `+` in path can be uploaded without errors
5. ✅ All S3 operations (PUT, GET, LIST, DELETE) work with special chars
6. ✅ Go Cloud Development Kit can upload files with `+` in path
7. ✅ All existing tests still pass (no regressions)
8. ✅ New tests cover various special character scenarios

## Documentation Updates

After implementation, update:

1. **API Documentation**: Document how special characters are handled
2. **Developer Guide**: Best practices for object naming
3. **Migration Guide**: If storage format changes
4. **FAQ**: Common issues with special characters
5. **This Document**: Final solution and lessons learned

## References

- AWS S3 API Specification: https://docs.aws.amazon.com/AmazonS3/latest/API/
- URL Encoding RFC 3986: https://tools.ietf.org/html/rfc3986
- s3s Library: https://docs.rs/s3s/0.12.0-rc.4/
- urlencoding crate: https://docs.rs/urlencoding/
- Issue #1072 (referenced in comments)

## Conclusion

The issue with special characters in object paths is a critical correctness bug that affects S3 API compatibility. The solution requires:

1. **Understanding** how s3s library handles URL encoding
2. **Implementing** consistent key handling across all operations
3. **Testing** thoroughly with real S3 clients
4. **Validating** that all edge cases are covered

The recommended approach is to start with investigation and testing (Phase 1) to understand the current behavior, then implement the appropriate fix with comprehensive test coverage.

---

**Document Version**: 1.0  
**Date**: 2025-12-09  
**Author**: RustFS Team  
**Status**: Draft - Awaiting Investigation Results
