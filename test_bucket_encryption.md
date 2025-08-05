# S3 Bucket Encryption API Test Guide

This document provides test commands for the S3 bucket encryption APIs that are now implemented using standard S3 protocol interfaces.

## Prerequisites

1. Start RustFS server
2. Create a test bucket
3. Have AWS CLI or curl available

## Test Commands

### 1. Set Bucket Encryption (PUT)

Using AWS CLI:
```bash
aws s3api put-bucket-encryption \
  --bucket test-bucket \
  --server-side-encryption-configuration '{
    "Rules": [
      {
        "ApplyServerSideEncryptionByDefault": {
          "SSEAlgorithm": "AES256"
        }
      }
    ]
  }' \
  --endpoint-url http://localhost:9000
```

Using curl:
```bash
curl -X PUT "http://localhost:9000/test-bucket?encryption" \
  -H "Content-Type: application/xml" \
  -d '<?xml version="1.0" encoding="UTF-8"?>
<ServerSideEncryptionConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Rule>
    <ApplyServerSideEncryptionByDefault>
      <SSEAlgorithm>AES256</SSEAlgorithm>
    </ApplyServerSideEncryptionByDefault>
  </Rule>
</ServerSideEncryptionConfiguration>'
```

### 2. Get Bucket Encryption (GET)

Using AWS CLI:
```bash
aws s3api get-bucket-encryption \
  --bucket test-bucket \
  --endpoint-url http://localhost:9000
```

Using curl:
```bash
curl -X GET "http://localhost:9000/test-bucket?encryption"
```

### 3. Delete Bucket Encryption (DELETE)

Using AWS CLI:
```bash
aws s3api delete-bucket-encryption \
  --bucket test-bucket \
  --endpoint-url http://localhost:9000
```

Using curl:
```bash
curl -X DELETE "http://localhost:9000/test-bucket?encryption"
```

## Expected Responses

### PUT Response
- Status: 200 OK
- Empty body

### GET Response (when encryption is configured)
- Status: 200 OK
- XML body with encryption configuration

### GET Response (when no encryption configured)
- Status: 404 Not Found
- Error message about missing encryption configuration

### DELETE Response
- Status: 204 No Content
- Empty body

## Migration Notes

The bucket encryption functionality has been migrated from custom admin endpoints to standard S3 protocol interfaces:

**Old endpoints (removed):**
- `PUT /rustfs/admin/v3/bucket-encryption/{bucket}`
- `GET /rustfs/admin/v3/bucket-encryption/{bucket}`
- `DELETE /rustfs/admin/v3/bucket-encryption/{bucket}`
- `GET /rustfs/admin/v3/bucket-encryptions`

**New S3 standard endpoints:**
- `PUT /{bucket}?encryption`
- `GET /{bucket}?encryption`
- `DELETE /{bucket}?encryption`

This change provides better S3 compatibility and follows AWS S3 API standards.