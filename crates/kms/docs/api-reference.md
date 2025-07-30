# RustFS Object Encryption API Reference

## Overview

RustFS provides comprehensive object encryption capabilities compatible with AWS S3 encryption standards. This document describes the encryption-related APIs and their usage.

## Server-Side Encryption (SSE) APIs

### SSE-S3 (AES256)

SSE-S3 uses AES256 encryption with keys managed by RustFS.

#### Usage

```bash
# Upload with SSE-S3
curl -X PUT \
  http://localhost:9000/mybucket/myobject \
  -H "x-amz-server-side-encryption: AES256" \
  -d "Hello World"

# Download (automatically decrypted)
curl http://localhost:9000/mybucket/myobject
```

#### SDK Examples

**Python (boto3)**
```python
import boto3

s3 = boto3.client('s3', endpoint_url='http://localhost:9000')

# Upload with SSE-S3
s3.put_object(
    Bucket='mybucket',
    Key='myobject',
    Body=b'Hello World',
    ServerSideEncryption='AES256'
)

# Download
response = s3.get_object(Bucket='mybucket', Key='myobject')
data = response['Body'].read()
```

**JavaScript (AWS SDK)**
```javascript
const AWS = require('aws-sdk');
const s3 = new AWS.S3({ endpoint: 'http://localhost:9000' });

// Upload with SSE-S3
await s3.putObject({
  Bucket: 'mybucket',
  Key: 'myobject',
  Body: 'Hello World',
  ServerSideEncryption: 'AES256'
}).promise();

// Download
const response = await s3.getObject({
  Bucket: 'mybucket',
  Key: 'myobject'
}).promise();
```

### SSE-KMS (Key Management Service)

SSE-KMS uses customer-managed keys via RustFS KMS service.

#### Usage

```bash
# Upload with SSE-KMS
curl -X PUT \
  http://localhost:9000/mybucket/myobject \
  -H "x-amz-server-side-encryption: aws:kms" \
  -H "x-amz-server-side-encryption-aws-kms-key-id: my-key-id" \
  -d "Hello World"

# Upload with KMS key auto-generation
curl -X PUT \
  http://localhost:9000/mybucket/myobject \
  -H "x-amz-server-side-encryption: aws:kms" \
  -d "Hello World"
```

#### SDK Examples

**Python (boto3)**
```python
# Upload with specific KMS key
s3.put_object(
    Bucket='mybucket',
    Key='myobject',
    Body=b'Hello World',
    ServerSideEncryption='aws:kms',
    SSEKMSKeyId='my-key-id'
)

# Upload with default KMS key
s3.put_object(
    Bucket='mybucket',
    Key='myobject',
    Body=b'Hello World',
    ServerSideEncryption='aws:kms'
)
```

### SSE-C (Customer-Provided Keys)

SSE-C uses encryption keys provided by the client.

#### Usage

```bash
# Generate a 256-bit key
KEY=$(openssl rand -base64 32)

# Upload with SSE-C
curl -X PUT \
  http://localhost:9000/mybucket/myobject \
  -H "x-amz-server-side-encryption-customer-algorithm: AES256" \
  -H "x-amz-server-side-encryption-customer-key: $KEY" \
  -H "x-amz-server-side-encryption-customer-key-md5: $(echo $KEY | base64 -d | md5sum | cut -d' ' -f1 | xxd -r -p | base64)" \
  -d "Hello World"

# Download with same key
curl -X GET \
  http://localhost:9000/mybucket/myobject \
  -H "x-amz-server-side-encryption-customer-algorithm: AES256" \
  -H "x-amz-server-side-encryption-customer-key: $KEY" \
  -H "x-amz-server-side-encryption-customer-key-md5: $(echo $KEY | base64 -d | md5sum | cut -d' ' -f1 | xxd -r -p | base64)"
```

#### SDK Examples

**Python (boto3)**
```python
import base64
import hashlib

# Generate key
key = b'0123456789abcdef0123456789abcdef'  # 32 bytes
key_b64 = base64.b64encode(key).decode()
key_md5 = base64.b64encode(hashlib.md5(key).digest()).decode()

# Upload with SSE-C
s3.put_object(
    Bucket='mybucket',
    Key='myobject',
    Body=b'Hello World',
    SSECustomerAlgorithm='AES256',
    SSECustomerKey=key_b64,
    SSECustomerKeyMD5=key_md5
)

# Download with same key
response = s3.get_object(
    Bucket='mybucket',
    Key='myobject',
    SSECustomerAlgorithm='AES256',
    SSECustomerKey=key_b64,
    SSECustomerKeyMD5=key_md5
)
```

## Bucket Encryption Configuration

### Get Bucket Encryption

```bash
curl -X GET http://localhost:9000/mybucket?encryption
```

**Response**
```json
{
  "ServerSideEncryptionConfiguration": {
    "Rules": [
      {
        "ApplyServerSideEncryptionByDefault": {
          "SSEAlgorithm": "AES256"
        }
      }
    ]
  }
}
```

### Put Bucket Encryption

```bash
curl -X PUT http://localhost:9000/mybucket?encryption \
  -H "Content-Type: application/json" \
  -d '{
    "ServerSideEncryptionConfiguration": {
      "Rules": [
        {
          "ApplyServerSideEncryptionByDefault": {
            "SSEAlgorithm": "aws:kms",
            "KMSMasterKeyID": "my-key-id"
          }
        }
      ]
    }
  }'
```

### Delete Bucket Encryption

```bash
curl -X DELETE http://localhost:9000/mybucket?encryption
```

## Multipart Upload Encryption

### Initiate Encrypted Multipart Upload

```bash
# Initiate with SSE-S3
curl -X POST \
  "http://localhost:9000/mybucket/large-object?uploads" \
  -H "x-amz-server-side-encryption: AES256"

# Initiate with SSE-KMS
curl -X POST \
  "http://localhost:9000/mybucket/large-object?uploads" \
  -H "x-amz-server-side-encryption: aws:kms" \
  -H "x-amz-server-side-encryption-aws-kms-key-id: my-key-id"

# Initiate with SSE-C
KEY=$(openssl rand -base64 32)
curl -X POST \
  "http://localhost:9000/mybucket/large-object?uploads" \
  -H "x-amz-server-side-encryption-customer-algorithm: AES256" \
  -H "x-amz-server-side-encryption-customer-key: $KEY" \
  -H "x-amz-server-side-encryption-customer-key-md5: $(echo $KEY | base64 -d | md5sum | cut -d' ' -f1 | xxd -r -p | base64)"
```

### Upload Parts with Encryption

```bash
# Upload part 1
curl -X PUT \
  "http://localhost:9000/mybucket/large-object?partNumber=1&uploadId=UPLOAD_ID" \
  -H "x-amz-server-side-encryption: AES256" \
  --data-binary @part1.bin

# Upload part 2
curl -X PUT \
  "http://localhost:9000/mybucket/large-object?partNumber=2&uploadId=UPLOAD_ID" \
  -H "x-amz-server-side-encryption: AES256" \
  --data-binary @part2.bin
```

### Complete Multipart Upload

```bash
curl -X POST \
  "http://localhost:9000/mybucket/large-object?uploadId=UPLOAD_ID" \
  -H "Content-Type: application/xml" \
  -d '{
    "CompleteMultipartUpload": {
      "Part": [
        {
          "PartNumber": 1,
          "ETag": "etag1"
        },
        {
          "PartNumber": 2,
          "ETag": "etag2"
        }
      ]
    }
  }'
```

## Copy Object with Encryption

### Copy with SSE-S3

```bash
curl -X PUT \
  http://localhost:9000/destbucket/destobject \
  -H "x-amz-copy-source: sourcebucket/sourceobject" \
  -H "x-amz-server-side-encryption: AES256"
```

### Copy with SSE-KMS

```bash
curl -X PUT \
  http://localhost:9000/destbucket/destobject \
  -H "x-amz-copy-source: sourcebucket/sourceobject" \
  -H "x-amz-server-side-encryption: aws:kms" \
  -H "x-amz-server-side-encryption-aws-kms-key-id: dest-key-id"
```

### Copy with SSE-C

```bash
# Source object with SSE-C
SOURCE_KEY=$(openssl rand -base64 32)

# Destination object with SSE-C
DEST_KEY=$(openssl rand -base64 32)

curl -X PUT \
  http://localhost:9000/destbucket/destobject \
  -H "x-amz-copy-source: sourcebucket/sourceobject" \
  -H "x-amz-server-side-encryption-customer-algorithm: AES256" \
  -H "x-amz-server-side-encryption-customer-key: $DEST_KEY" \
  -H "x-amz-server-side-encryption-customer-key-md5: $(echo $DEST_KEY | base64 -d | md5sum | cut -d' ' -f1 | xxd -r -p | base64)" \
  -H "x-amz-copy-source-server-side-encryption-customer-algorithm: AES256" \
  -H "x-amz-copy-source-server-side-encryption-customer-key: $SOURCE_KEY" \
  -H "x-amz-copy-source-server-side-encryption-customer-key-md5: $(echo $SOURCE_KEY | base64 -d | md5sum | cut -d' ' -f1 | xxd -r -p | base64)"
```

## Error Handling

### Common Error Responses

#### Invalid Encryption Algorithm

```json
{
  "Error": {
    "Code": "InvalidArgument",
    "Message": "Invalid server-side encryption algorithm specified",
    "ArgumentName": "x-amz-server-side-encryption",
    "ArgumentValue": "INVALID"
  }
}
```

#### Invalid KMS Key

```json
{
  "Error": {
    "Code": "InvalidArgument",
    "Message": "Invalid KMS key ID",
    "ArgumentName": "x-amz-server-side-encryption-aws-kms-key-id",
    "ArgumentValue": "invalid-key"
  }
}
```

#### Missing Customer Key

```json
{
  "Error": {
    "Code": "InvalidArgument",
    "Message": "Server-side encryption customer key is missing",
    "ArgumentName": "x-amz-server-side-encryption-customer-key"
  }
}
```

#### Incorrect Customer Key

```json
{
  "Error": {
    "Code": "InvalidArgument",
    "Message": "The calculated MD5 hash of the key did not match the hash that was provided."
  }
}
```

## Best Practices

### Key Management

1. **Use SSE-S3 for general-purpose encryption**
2. **Use SSE-KMS for customer-managed keys**
3. **Use SSE-C only when you must manage your own keys**
4. **Rotate KMS keys regularly**
5. **Monitor key usage with audit logs**

### Performance Optimization

1. **Use bucket default encryption to avoid per-object overhead**
2. **Consider multipart uploads for large objects**
3. **Use SSE-S3 for better performance than SSE-KMS**
4. **Cache KMS responses appropriately**

### Security Considerations

1. **Never log encryption keys in plaintext**
2. **Use HTTPS for all API calls**
3. **Implement proper access controls**
4. **Monitor encryption operations for anomalies"
5. **Test key rotation procedures regularly"

## Monitoring and Logging

### Encryption Metrics

The following metrics are exposed via Prometheus:

- `rustfs_encryption_operations_total`: Total encryption operations
- `rustfs_encryption_failures_total`: Failed encryption operations
- `rustfs_kms_operations_total`: KMS operations
- `rustfs_encryption_duration_seconds`: Encryption operation duration
- `rustfs_encrypted_bytes_total`: Total bytes encrypted/decrypted

### Audit Logging

All encryption operations are logged with:
- Operation type (encrypt/decrypt)
- Bucket and object names
- Encryption algorithm used
- Key ID (if applicable)
- User identity
- Timestamps
- Success/failure status