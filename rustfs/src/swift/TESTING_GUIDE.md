# Swift API Manual Testing Guide

This guide provides instructions for manually testing the Swift API implementation using python-swiftclient and curl.

## Prerequisites

### 1. Install python-swiftclient
```bash
pip install python-swiftclient python-keystoneclient
```

### 2. Setup RustFS with Keystone
Ensure RustFS is running and configured to work with Keystone authentication.

### 3. Configure Environment Variables
```bash
# Keystone credentials
export OS_AUTH_URL=http://localhost:5000/v3
export OS_USERNAME=demo
export OS_PASSWORD=secret
export OS_PROJECT_NAME=demo
export OS_PROJECT_DOMAIN_NAME=Default
export OS_USER_DOMAIN_NAME=Default

# Swift endpoint
export OS_STORAGE_URL=http://localhost:8000/v1/AUTH_<project_id>
# Or let swift client discover it from Keystone service catalog
```

## Test Scenarios

### Scenario 1: Container Operations

#### 1.1 Create Container
```bash
# Using swift client
swift post test-container

# Using curl with Keystone token
TOKEN=$(openstack token issue -f value -c id)
curl -i -X PUT http://localhost:8000/v1/AUTH_demo/test-container \
  -H "X-Auth-Token: $TOKEN"

# Expected: 201 Created (new) or 202 Accepted (exists)
```

#### 1.2 List Containers
```bash
# List all containers
swift list

# Using curl
curl -i http://localhost:8000/v1/AUTH_demo \
  -H "X-Auth-Token: $TOKEN"

# Expected: JSON array of container names
```

#### 1.3 Get Container Metadata
```bash
# Using swift client
swift stat test-container

# Using curl
curl -I http://localhost:8000/v1/AUTH_demo/test-container \
  -H "X-Auth-Token: $TOKEN"

# Expected: Headers with X-Container-Object-Count, X-Container-Bytes-Used
```

#### 1.4 Update Container Metadata
```bash
# Using swift client
swift post test-container -m "Color:Blue" -m "Purpose:Testing"

# Using curl
curl -i -X POST http://localhost:8000/v1/AUTH_demo/test-container \
  -H "X-Auth-Token: $TOKEN" \
  -H "X-Container-Meta-Color: Blue" \
  -H "X-Container-Meta-Purpose: Testing"

# Expected: 204 No Content
```

#### 1.5 Delete Container (empty only)
```bash
# Using swift client
swift delete test-container

# Using curl
curl -i -X DELETE http://localhost:8000/v1/AUTH_demo/test-container \
  -H "X-Auth-Token: $TOKEN"

# Expected: 204 No Content (if empty) or 409 Conflict (if not empty)
```

### Scenario 2: Object Operations

#### 2.1 Upload Object
```bash
# Create a test file
echo "Hello, Swift!" > test.txt

# Using swift client
swift upload test-container test.txt

# Using curl
curl -i -X PUT http://localhost:8000/v1/AUTH_demo/test-container/test.txt \
  -H "X-Auth-Token: $TOKEN" \
  -H "Content-Type: text/plain" \
  -H "X-Object-Meta-Author: TestUser" \
  --data-binary @test.txt

# Expected: 201 Created with ETag header
```

#### 2.2 Download Object
```bash
# Using swift client
swift download test-container test.txt -o downloaded.txt

# Using curl
curl -o downloaded.txt http://localhost:8000/v1/AUTH_demo/test-container/test.txt \
  -H "X-Auth-Token: $TOKEN"

# Verify content
cat downloaded.txt
# Expected: "Hello, Swift!"
```

#### 2.3 List Objects in Container
```bash
# Using swift client
swift list test-container

# With details (JSON format)
swift list test-container --json

# Using curl
curl http://localhost:8000/v1/AUTH_demo/test-container \
  -H "X-Auth-Token: $TOKEN" \
  -H "Accept: application/json"

# Expected: JSON array with object details (name, bytes, hash, last_modified)
```

#### 2.4 Get Object Metadata
```bash
# Using swift client
swift stat test-container test.txt

# Using curl
curl -I http://localhost:8000/v1/AUTH_demo/test-container/test.txt \
  -H "X-Auth-Token: $TOKEN"

# Expected: Headers with Content-Length, Content-Type, ETag, X-Object-Meta-*
```

#### 2.5 Update Object Metadata
```bash
# Using swift client
swift post test-container test.txt -m "Version:1.0" -m "Status:Final"

# Using curl
curl -i -X POST http://localhost:8000/v1/AUTH_demo/test-container/test.txt \
  -H "X-Auth-Token: $TOKEN" \
  -H "X-Object-Meta-Version: 1.0" \
  -H "X-Object-Meta-Status: Final"

# Expected: 202 Accepted
```

#### 2.6 Copy Object (Server-side)
```bash
# Using swift client
swift copy test-container test.txt --destination /test-container/test-copy.txt

# Using curl with COPY method
curl -i -X COPY http://localhost:8000/v1/AUTH_demo/test-container/test.txt \
  -H "X-Auth-Token: $TOKEN" \
  -H "Destination: /test-container/test-copy.txt"

# Expected: 201 Created
```

#### 2.7 Delete Object
```bash
# Using swift client
swift delete test-container test.txt

# Using curl
curl -i -X DELETE http://localhost:8000/v1/AUTH_demo/test-container/test.txt \
  -H "X-Auth-Token: $TOKEN"

# Expected: 204 No Content
```

### Scenario 3: Range Requests

#### 3.1 Download Partial Object
```bash
# Create a larger file
echo "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ" > large.txt
swift upload test-container large.txt

# Download first 10 bytes
curl -H "X-Auth-Token: $TOKEN" \
  -H "Range: bytes=0-9" \
  http://localhost:8000/v1/AUTH_demo/test-container/large.txt

# Expected: "0123456789" with 206 Partial Content
```

### Scenario 4: Account Operations

#### 4.1 Get Account Metadata
```bash
# Using swift client
swift stat

# Using curl
curl -I http://localhost:8000/v1/AUTH_demo \
  -H "X-Auth-Token: $TOKEN"

# Expected: Headers with X-Account-Container-Count, X-Account-Bytes-Used
```

### Scenario 5: Multi-Tenancy Testing

#### 5.1 Verify Tenant Isolation
```bash
# Login as user1 in project1
export OS_PROJECT_NAME=project1
TOKEN1=$(openstack token issue -f value -c id)

# Create container as user1
curl -i -X PUT http://localhost:8000/v1/AUTH_project1/container1 \
  -H "X-Auth-Token: $TOKEN1"

# Upload object as user1
echo "User1 data" > user1.txt
curl -i -X PUT http://localhost:8000/v1/AUTH_project1/container1/user1.txt \
  -H "X-Auth-Token: $TOKEN1" \
  --data-binary @user1.txt

# Switch to user2 in project2
export OS_PROJECT_NAME=project2
TOKEN2=$(openstack token issue -f value -c id)

# Try to access user1's container (should fail)
curl -i http://localhost:8000/v1/AUTH_project1/container1 \
  -H "X-Auth-Token: $TOKEN2"

# Expected: 403 Forbidden

# List containers as user2 (should only see project2 containers)
curl http://localhost:8000/v1/AUTH_project2 \
  -H "X-Auth-Token: $TOKEN2"

# Expected: Empty list or only project2 containers
```

### Scenario 6: Error Cases

#### 6.1 Test Authentication Errors
```bash
# No token
curl -i http://localhost:8000/v1/AUTH_demo

# Expected: 401 Unauthorized

# Invalid token
curl -i http://localhost:8000/v1/AUTH_demo \
  -H "X-Auth-Token: invalid-token"

# Expected: 401 Unauthorized
```

#### 6.2 Test Invalid Container Names
```bash
# Container name with slash
curl -i -X PUT http://localhost:8000/v1/AUTH_demo/invalid/name \
  -H "X-Auth-Token: $TOKEN"

# Expected: 400 Bad Request

# Container name too long (>256 chars)
LONG_NAME=$(python3 -c "print('a' * 257)")
curl -i -X PUT "http://localhost:8000/v1/AUTH_demo/$LONG_NAME" \
  -H "X-Auth-Token: $TOKEN"

# Expected: 400 Bad Request
```

#### 6.3 Test Delete Non-Empty Container
```bash
# Create container and add object
curl -X PUT http://localhost:8000/v1/AUTH_demo/test-container -H "X-Auth-Token: $TOKEN"
echo "test" | curl -X PUT http://localhost:8000/v1/AUTH_demo/test-container/test.txt \
  -H "X-Auth-Token: $TOKEN" --data-binary @-

# Try to delete non-empty container
curl -i -X DELETE http://localhost:8000/v1/AUTH_demo/test-container \
  -H "X-Auth-Token: $TOKEN"

# Expected: 409 Conflict
```

#### 6.4 Test Object Not Found
```bash
curl -I http://localhost:8000/v1/AUTH_demo/test-container/nonexistent.txt \
  -H "X-Auth-Token: $TOKEN"

# Expected: 404 Not Found
```

## Verification Checklist

After running the test scenarios, verify:

### Functional Requirements
- [ ] Can create containers
- [ ] Can list containers with tenant filtering
- [ ] Can delete empty containers
- [ ] Can upload objects with metadata
- [ ] Can download objects
- [ ] Can list objects in container
- [ ] Can update object/container metadata
- [ ] Can copy objects server-side
- [ ] Can delete objects
- [ ] Range requests work for partial downloads
- [ ] Account statistics are accurate

### Security Requirements
- [ ] Authentication required for all operations
- [ ] Tenants cannot access other tenants' data
- [ ] Invalid tokens are rejected
- [ ] Project ID from token matches account in URL

### Error Handling
- [ ] 401 for missing/invalid authentication
- [ ] 403 for unauthorized access to other tenants
- [ ] 404 for non-existent containers/objects
- [ ] 409 for deleting non-empty containers
- [ ] 400 for invalid names/parameters
- [ ] All errors include X-Trans-Id header

### Response Format
- [ ] Transaction IDs are unique and properly formatted (tx<hex>)
- [ ] Content-Type negotiation works (JSON/XML/text)
- [ ] Swift headers are present (X-Trans-Id, X-Timestamp, etc.)
- [ ] ETag headers match object hash
- [ ] Metadata headers use X-*-Meta-* format

## Performance Testing

### Large Object Upload/Download
```bash
# Create a 100MB file
dd if=/dev/zero of=large.bin bs=1M count=100

# Upload
time swift upload test-container large.bin

# Download
time swift download test-container large.bin -o /tmp/large.bin

# Verify integrity
md5sum large.bin /tmp/large.bin
```

### Concurrent Operations
```bash
# Upload multiple files in parallel
for i in {1..10}; do
  echo "File $i" > file$i.txt
  swift upload test-container file$i.txt &
done
wait

# List and verify all uploaded
swift list test-container
```

## Troubleshooting

### Common Issues

1. **"Connection refused"**
   - Verify RustFS is running on the correct port
   - Check firewall settings

2. **"401 Unauthorized"**
   - Verify Keystone is running and accessible
   - Check token validity: `openstack token issue`
   - Verify OS_AUTH_URL and credentials

3. **"403 Forbidden"**
   - Check project ID in URL matches token project
   - Verify user has appropriate role in project

4. **"404 Not Found" for valid containers**
   - Verify container name mapping is working
   - Check S3 backend bucket names (should have hash prefix)
   - Review RustFS logs for errors

5. **"409 Conflict" on delete**
   - Container must be empty
   - List objects: `swift list container-name`
   - Delete all objects before deleting container

### Debug Logging

Enable debug logging in RustFS to see detailed request/response info:
```bash
export RUST_LOG=rustfs::swift=debug
```

Check RustFS logs for:
- Swift route matching
- Tenant ID extraction
- S3 bucket name mapping
- Error details

## Next Steps

After successful manual testing:
1. Run performance benchmarks
2. Test with production-like workloads
3. Monitor resource usage (memory, CPU)
4. Review error logs for any issues
5. Consider integration testing with real applications

## Known Limitations (Phase 2)

- Large Object support (SLO/DLO) not yet implemented
- TempURL not yet implemented
- Object versioning not yet implemented
- Account POST (metadata update) deferred to Phase 3
- Content-Type negotiation limited (JSON primary format)
