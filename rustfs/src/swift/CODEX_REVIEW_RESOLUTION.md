# Codex Review Resolution - PR #2066

**Review**: https://github.com/rustfs/rustfs/pull/2066#pullrequestreview-3890571917
**Date**: 2026-03-04
**Commit**: 5b653bad

---

## Summary

Addressed 4 of 5 priority items from Codex code review. One item (P1 upload streaming) deferred to Phase 3 as it requires architectural refactoring beyond Phase 2 scope.

---

## Issues Addressed

### ✅ P2: Account HEAD returns wrong error code
**Issue**: Account HEAD returned 500 Internal Server Error instead of 501 Not Implemented

**Impact**: Low - Minor HTTP semantics issue

**Fix**: Changed error type from `InternalServerError` to `NotImplemented`

**Location**: `rustfs/src/swift/handler.rs:250`

```rust
// Before:
Method::HEAD => {
    Err(SwiftError::InternalServerError("Swift Account HEAD...".to_string()))
}

// After:
Method::HEAD => {
    Err(SwiftError::NotImplemented("Swift Account HEAD operation not yet implemented".to_string()))
}
```

---

### ✅ P2: Range header not parsed in GET object
**Issue**: GET object ignored Range header, always returned full object with 200 OK

**Impact**: Low - Missing optimization feature, but not breaking

**Fix**: Parse Range header, pass to backend, return 206 Partial Content

**Location**: `rustfs/src/swift/handler.rs:334-353`

```rust
// Parse Range header if present
let range = headers
    .get("range")
    .and_then(|v| v.to_str().ok())
    .and_then(|r| object::parse_range_header(r).ok());

// Determine status code before moving range value
let status = if range.is_some() {
    StatusCode::PARTIAL_CONTENT
} else {
    StatusCode::OK
};

// Pass range to backend
let reader = object::get_object(&account, &container, &object, &credentials, range).await?;
```

**Testing**: Verified with existing range request tests in object.rs

---

### ✅ P1: Router drops empty path segments
**Issue**: Router filtered out empty segments, breaking valid Swift object names like "dir/" and "a//b"

**Impact**: High - Data loss risk, breaks legitimate use cases

**Root Cause**:
```rust
// Before:
let segments: Vec<&str> = path
    .trim_start_matches('/')
    .split('/')
    .filter(|s| !s.is_empty())  // ← This broke empty segments
    .collect();
```

**Fix**: Remove `.filter(|s| !s.is_empty())` to preserve all segments

**Location**: `rustfs/src/swift/router.rs:102`

```rust
// After:
let segments: Vec<&str> = path.trim_start_matches('/').split('/').collect();
```

**Rationale**:
- Swift explicitly allows trailing slashes and consecutive slashes in object names
- These are semantically meaningful (e.g., "dir/" indicates a directory placeholder)
- The split naturally preserves segment boundaries

**Testing**: Existing router tests verify correct behavior

---

### ✅ P1: COPY destination validation too strict
**Issue**: COPY validation rejected valid nested keys like "/container/path/to/deep/object.txt"

**Root Cause**:
```rust
// Before:
if dest_parts.len() > 3 {
    return Err(SwiftError::BadRequest("Invalid destination format".to_string()));
}
```

This incorrectly assumed destination paths could only have 3 segments (account/container/object), but object keys can contain multiple slashes.

**Impact**: High - Blocks legitimate COPY operations for nested objects

**Fix**: Remove slash count restriction, only validate for path traversal

**Location**: `rustfs/src/swift/handler.rs:473-476`

```rust
// After:
// Only check for path traversal attempts
if destination.contains("..") {
    return Err(SwiftError::BadRequest("Path traversal not allowed".to_string()));
}
```

**Rationale**:
- Swift object keys are opaque strings that can contain slashes
- Path validation should focus on security (no "../"), not structure
- The backend already handles nested paths correctly

**Testing**: Verified with COPY tests in handler.rs

---

## Issue Deferred

### ⏸️ P1: Upload buffering causes OOM risk
**Issue**: PUT object buffers entire body in memory before uploading to backend

**Location**: `rustfs/src/swift/handler.rs:282-312`

```rust
// Current implementation:
let collected = body.collect().await
    .map_err(|e| SwiftError::BadRequest(format!("Failed to read body: {}", e)))?;
let data = collected.to_bytes();

// Custom BytesReader to adapt Bytes to AsyncRead
struct BytesReader {
    bytes: bytes::Bytes,
    pos: usize,
}
```

**Impact**: High - Can OOM on large uploads (>1GB), blocking production use

**Why Deferred**:
1. **Architectural constraint**: `axum::body::Body` is `HttpBody`, but `put_object` expects `AsyncRead`
2. **No direct adapter**: No standard library provides `HttpBody → AsyncRead` streaming
3. **Phase 2 scope**: Proper fix requires refactoring object storage layer
4. **Phase 3 alignment**: Large Object Support (SLO/DLO) will address this properly

**Mitigation**:
- Added comprehensive TODO comment documenting the issue
- Linked to Codex review for tracking
- Set upload limit to 100MB to prevent worst-case OOM

**TODO Comment**:
```rust
// TODO(P1): Stream uploads directly instead of buffering entire body in memory.
// Current implementation buffers the entire request body before uploading,
// which can cause OOM on large uploads. This should be refactored to stream
// the body directly to put_object using a proper AsyncRead adapter.
// See: https://github.com/rustfs/rustfs/pull/2066#pullrequestreview-3890571917
```

**Phase 3 Solution**:
- Implement streaming adapter: `HttpBody → AsyncRead`
- Or: Modify backend to accept `HttpBody` directly
- Or: Use `tokio_util::codec` for proper stream transformation
- Integrate with Large Object Support (multipart uploads)

---

## Testing Results

### Build Status
```
✅ cargo build --package rustfs
   Compiling rustfs v0.0.5
   Finished `dev` profile [unoptimized + debuginfo] target(s) in 13.63s
   1 warning (benign dead_code)
```

### Test Status
```
✅ cargo test --package rustfs --bin rustfs swift::
   test result: ok. 49 passed; 0 failed; 0 ignored; 0 measured
```

### Code Quality
```
✅ cargo clippy --package rustfs --bin rustfs
   1 warning: dead_code (benign - public API methods for future use)
```

---

## Verification Checklist

- [x] P2: Account HEAD returns 501 Not Implemented
- [x] P2: GET parses Range header and returns 206 Partial Content
- [x] P1: Router preserves empty path segments ("dir/", "a//b")
- [x] P1: COPY allows nested destination keys (any depth)
- [x] P1: Upload streaming documented with TODO (deferred to Phase 3)
- [x] All 49 tests passing
- [x] Clippy clean (no blocking warnings)
- [x] Code formatted with rustfmt
- [x] Committed and pushed to PR #2066

---

## Remaining Work for Phase 3

1. **Upload Streaming (P1)**:
   - Implement proper HttpBody → AsyncRead adapter
   - Remove memory buffering
   - Support large files (>1GB)

2. **Large Object Support** (complements streaming):
   - Static Large Objects (SLO)
   - Dynamic Large Objects (DLO)
   - Multipart upload integration

3. **Performance Testing**:
   - Benchmark range request performance
   - Load test with nested object paths
   - Memory profiling under concurrent uploads

---

## References

- **Codex Review**: https://github.com/rustfs/rustfs/pull/2066#pullrequestreview-3890571917
- **Fix Commit**: 5b653bad
- **Phase 2 Implementation**: b226b2ca
- **Security Fixes**: 932a1a68
