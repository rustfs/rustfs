# DoS Prevention: Request/Response Body Size Limits

## Executive Summary

This document describes the implementation of request and response body size limits in RustFS to prevent Denial of Service (DoS) attacks through unbounded memory allocation. The previous use of `usize::MAX` with `store_all_limited()` posed a critical security risk allowing attackers to exhaust server memory.

## Security Risk Assessment

### Vulnerability: Unbounded Memory Allocation

**Severity**: High  
**Impact**: Server memory exhaustion, service unavailability  
**Likelihood**: High (easily exploitable)

**Previous Code** (vulnerable):
```rust
let body = input.store_all_limited(usize::MAX).await?;
```

On a 64-bit system, `usize::MAX` is approximately 18 exabytes, effectively unlimited.

## Implemented Limits

| Limit | Size | Use Cases |
|-------|------|-----------|
| `MAX_ADMIN_REQUEST_BODY_SIZE` | 1 MB | User management, policies, tier/KMS/event configs |
| `MAX_IAM_IMPORT_SIZE` | 10 MB | IAM import/export (ZIP archives) |
| `MAX_BUCKET_METADATA_IMPORT_SIZE` | 100 MB | Bucket metadata import |
| `MAX_HEAL_REQUEST_SIZE` | 1 MB | Healing operations |
| `MAX_S3_RESPONSE_SIZE` | 10 MB | S3 client responses from remote services |

## Rationale

- AWS IAM policy limit: 6KB-10KB
- Typical payloads: < 100KB
- 1MB-100MB limits provide generous headroom while preventing DoS
- Based on real-world usage analysis and industry standards

## Files Modified

- 22 files updated across admin handlers and S3 client modules
- 2 new files: `rustfs/src/admin/constants.rs`, `crates/ecstore/src/client/body_limits.rs`
