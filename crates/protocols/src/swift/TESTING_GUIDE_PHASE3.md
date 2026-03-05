# Phase 3 Testing Guide: Streaming Uploads, SLO, and DLO

This guide provides manual testing procedures for Phase 3 features using `python-swiftclient` and `curl`.

## Prerequisites

```bash
# Install python-swiftclient
pip install python-swiftclient

# Set environment variables
export ST_AUTH=http://localhost:8014/v3
export ST_USER=admin
export ST_KEY=secret
export OS_AUTH_URL=http://localhost:8014/v3
export OS_USERNAME=admin
export OS_PASSWORD=secret
export OS_PROJECT_NAME=admin
export OS_PROJECT_DOMAIN_NAME=default
export OS_USER_DOMAIN_NAME=default
```

## Test 1: Streaming Upload (Large Files)

### Objective
Verify that large file uploads (>100MB) work without memory exhaustion.

### Procedure

```bash
# Create a 500MB test file
dd if=/dev/zero of=large-500mb.bin bs=1M count=500

# Monitor RustFS memory before upload
ps aux | grep rustfs | grep -v grep

# Upload the file
swift upload test-container large-500mb.bin

# Monitor RustFS memory during and after upload (should stay flat)
ps aux | grep rustfs | grep -v grep

# Verify upload
swift list test-container
swift stat test-container large-500mb.bin

# Download and verify integrity
swift download test-container large-500mb.bin -o downloaded-500mb.bin
md5sum large-500mb.bin downloaded-500mb.bin
```

### Expected Results
- Upload completes successfully
- RustFS memory usage remains constant (< 500MB)
- MD5 checksums match
- No OOM errors in RustFS logs

---

## Test 2: Static Large Objects (SLO)

### Objective
Verify SLO manifest creation, validation, and streaming assembly.

### Test 2.1: Create SLO from Segments

```bash
# Create test segments (5 segments of 100MB each)
for i in {1..5}; do
  dd if=/dev/urandom of=segment_$i.bin bs=1M count=100
  swift upload test-container segment_$i.bin
done

# Get segment ETags and sizes
for i in {1..5}; do
  swift stat test-container segment_$i.bin | grep -E "(ETag|Content Length)"
done

# Create SLO manifest JSON
cat > slo-manifest.json <<EOF
[
  {
    "path": "/test-container/segment_1.bin",
    "etag": "ETAG_FROM_ABOVE",
    "size_bytes": 104857600
  },
  {
    "path": "/test-container/segment_2.bin",
    "etag": "ETAG_FROM_ABOVE",
    "size_bytes": 104857600
  },
  {
    "path": "/test-container/segment_3.bin",
    "etag": "ETAG_FROM_ABOVE",
    "size_bytes": 104857600
  },
  {
    "path": "/test-container/segment_4.bin",
    "etag": "ETAG_FROM_ABOVE",
    "size_bytes": 104857600
  },
  {
    "path": "/test-container/segment_5.bin",
    "etag": "ETAG_FROM_ABOVE",
    "size_bytes": 104857600
  }
]
EOF

# Upload SLO manifest
curl -X PUT \
  -H "X-Auth-Token: $TOKEN" \
  -H "Content-Type: application/json" \
  --data-binary @slo-manifest.json \
  "http://localhost:8080/v1/AUTH_admin/test-container/large-slo.bin?multipart-manifest=put"

# Verify SLO was created
swift stat test-container large-slo.bin
```

### Test 2.2: Download SLO

```bash
# Download assembled SLO
swift download test-container large-slo.bin

# Verify size (should be 500MB)
ls -lh large-slo.bin

# Compare with concatenated segments
cat segment_*.bin > expected-slo.bin
md5sum large-slo.bin expected-slo.bin
```

### Test 2.3: SLO Range Requests

```bash
# Request specific byte range (cross-segment)
curl -H "X-Auth-Token: $TOKEN" \
  -H "Range: bytes=50000000-150000000" \
  "http://localhost:8080/v1/AUTH_admin/test-container/large-slo.bin" \
  -o slo-range.bin

# Verify range size (should be 100000001 bytes)
ls -l slo-range.bin
```

### Test 2.4: SLO Metadata

```bash
# Get SLO manifest JSON
curl -H "X-Auth-Token: $TOKEN" \
  "http://localhost:8080/v1/AUTH_admin/test-container/large-slo.bin?multipart-manifest=get"

# Verify headers
swift stat test-container large-slo.bin | grep -i "x-static-large-object"
```

### Test 2.5: Delete SLO with Segments

```bash
# Delete SLO and all segments
curl -X DELETE \
  -H "X-Auth-Token: $TOKEN" \
  "http://localhost:8080/v1/AUTH_admin/test-container/large-slo.bin?multipart-manifest=delete"

# Verify segments are deleted
swift list test-container | grep segment
```

### Expected Results
- SLO creation validates segment ETags and sizes
- Download assembles segments in correct order
- Range requests span multiple segments correctly
- ETag follows format: `"{MD5}-{count}"`
- Delete removes manifest and all segments

---

## Test 3: Dynamic Large Objects (DLO)

### Objective
Verify DLO segment discovery, lexicographic ordering, and streaming.

### Test 3.1: Create DLO Segments

```bash
# Create segments with zero-padded names (important for ordering)
for i in $(seq -f "%03g" 1 10); do
  dd if=/dev/urandom of=video_${i}.bin bs=1M count=50
  swift upload test-container video_${i}.bin
done

# Verify segment ordering
swift list test-container --prefix video_
```

### Test 3.2: Register DLO

```bash
# Create DLO marker object with manifest header
curl -X PUT \
  -H "X-Auth-Token: $TOKEN" \
  -H "X-Object-Manifest: test-container/video_" \
  -H "Content-Length: 0" \
  "http://localhost:8080/v1/AUTH_admin/test-container/video.mp4"

# Verify DLO registration
swift stat test-container video.mp4 | grep -i "x-object-manifest"
```

### Test 3.3: Download DLO

```bash
# Download assembled DLO
swift download test-container video.mp4

# Verify size (should be 500MB = 10 segments * 50MB)
ls -lh video.mp4

# Compare with concatenated segments
cat video_*.bin | sort | md5sum
md5sum video.mp4
```

### Test 3.4: DLO Auto-Discovery

```bash
# Add a new segment
dd if=/dev/urandom of=video_011.bin bs=1M count=50
swift upload test-container video_011.bin

# Download again (should include new segment)
swift download test-container video.mp4 -o video-updated.mp4

# Verify size increased (should be 550MB)
ls -lh video-updated.mp4
```

### Test 3.5: DLO Range Requests

```bash
# Request byte range
curl -H "X-Auth-Token: $TOKEN" \
  -H "Range: bytes=100000000-200000000" \
  "http://localhost:8080/v1/AUTH_admin/test-container/video.mp4" \
  -o dlo-range.bin

# Verify range size
ls -l dlo-range.bin
```

### Expected Results
- Segments discovered in lexicographic order
- New segments automatically included in assembly
- Range requests work across segments
- X-Object-Manifest header preserved

---

## Test 4: Error Handling

### Test 4.1: SLO Validation Errors

```bash
# Invalid ETag
cat > invalid-manifest.json <<EOF
[
  {
    "path": "/test-container/segment_1.bin",
    "etag": "wrong-etag",
    "size_bytes": 104857600
  }
]
EOF

curl -X PUT \
  -H "X-Auth-Token: $TOKEN" \
  -H "Content-Type: application/json" \
  --data-binary @invalid-manifest.json \
  "http://localhost:8080/v1/AUTH_admin/test-container/bad-slo.bin?multipart-manifest=put"

# Expected: 409 Conflict (ETag mismatch)
```

### Test 4.2: DLO Missing Segments

```bash
# Register DLO with non-existent prefix
curl -X PUT \
  -H "X-Auth-Token: $TOKEN" \
  -H "X-Object-Manifest: test-container/nonexistent_" \
  -H "Content-Length: 0" \
  "http://localhost:8080/v1/AUTH_admin/test-container/empty-dlo.bin"

# Try to download
curl -H "X-Auth-Token: $TOKEN" \
  "http://localhost:8080/v1/AUTH_admin/test-container/empty-dlo.bin"

# Expected: 404 Not Found (no segments)
```

---

## Performance Benchmarks

### Streaming Upload Performance

```bash
# Create 1GB file
dd if=/dev/zero of=1gb-test.bin bs=1M count=1024

# Time upload
time swift upload test-container 1gb-test.bin

# Expected: Memory usage < 200MB, Time < 60s
```

### SLO Assembly Performance

```bash
# Create 100-segment SLO (5GB total)
for i in $(seq -f "%03g" 1 100); do
  dd if=/dev/urandom of=bigseg_${i}.bin bs=1M count=50
  swift upload test-container bigseg_${i}.bin
done

# Create manifest (omitted for brevity)

# Time download
time swift download test-container large-100-segment-slo.bin

# Expected: Assembly overhead < 500ms
```

---

## Cleanup

```bash
# Delete test container and all objects
swift delete test-container --all

# Verify cleanup
swift list test-container
```

---

## Troubleshooting

### Issue: OOM during large uploads
- **Check**: RustFS memory usage with `ps aux`
- **Expected**: Memory should stay flat (< 500MB)
- **If failing**: Verify StreamReader is used in handler.rs

### Issue: SLO validation fails
- **Check**: Segment ETags match manifest
- **Tool**: `swift stat test-container segment.bin | grep ETag`
- **Expected**: ETags in manifest must exactly match segment ETags

### Issue: DLO segments out of order
- **Check**: Segment names are lexicographically ordered
- **Tool**: `swift list test-container --prefix video_ | sort`
- **Expected**: Names should be zero-padded (e.g., `video_001`, `video_002`)

### Issue: Range requests incorrect
- **Check**: Content-Range header in response
- **Tool**: `curl -I -H "Range: bytes=0-999" ...`
- **Expected**: `Content-Range: bytes 0-999/total_size`

---

## Summary Checklist

- [ ] Streaming upload: 500MB file uploads without OOM
- [ ] SLO creation: Manifest validates ETags and sizes
- [ ] SLO download: Segments assemble correctly
- [ ] SLO ranges: Cross-segment ranges work
- [ ] SLO delete: Removes manifest and segments
- [ ] DLO registration: X-Object-Manifest header set
- [ ] DLO download: Segments discovered in order
- [ ] DLO auto-discovery: New segments included
- [ ] DLO ranges: Cross-segment ranges work
- [ ] Error handling: Invalid manifests rejected
- [ ] Performance: Memory usage stays flat
- [ ] python-swiftclient: All operations compatible

---

## Next Steps

After Phase 3 validation:
1. Run full test suite: `cargo test -p rustfs-protocols`
2. Verify Phase 2 tests still pass (49/49)
3. Document any edge cases discovered
4. Proceed to Phase 4 planning (versioning, sync, etc.)
