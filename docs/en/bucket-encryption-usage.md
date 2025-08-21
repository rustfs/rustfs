# Bucket Encryption Usage Guide

This document describes how to use S3-compatible bucket encryption features in RustFS. RustFS supports multiple server-side encryption methods including SSE-S3, SSE-KMS, and SSE-C.

## Encryption Types Overview

### 1. SSE-S3 (Server-Side Encryption with S3-Managed Keys)
- Uses S3 service-managed keys for encryption
- Simplest encryption method with no additional configuration required
- Algorithm: AES-256

### 2. SSE-KMS (Server-Side Encryption with KMS-Managed Keys)
- Uses KMS-managed keys for encryption
- Provides fine-grained key control and auditing
- Supports custom KMS keys
 - DSSE compatibility: accepts aws:kms:dsse and normalizes to aws:kms in responses/HEAD

### 3. SSE-C (Server-Side Encryption with Customer-Provided Keys)
- Uses customer-provided keys for encryption
- Customer has complete control over encryption keys
- Requires providing the key with each request
 - Not supported for multipart uploads (use single PUT or COPY)

## Usage Methods

### SSE-S3 Encryption

#### Specify Encryption When Uploading Objects

```rust
use aws_sdk_s3::{primitives::ByteStream, types::ServerSideEncryption};

// Create S3 client
let client = aws_sdk_s3::Client::new(&config);

// Upload object with SSE-S3 encryption enabled
let test_data = b"Hello, SSE-S3 encryption!";
client
    .put_object()
    .bucket("my-bucket")
    .key("my-object")
    .body(ByteStream::from(test_data.to_vec()))
    .server_side_encryption(ServerSideEncryption::Aes256)  // Enable SSE-S3
    .send()
    .await?;
```

#### Using curl Command

```bash
curl -X PUT "http://localhost:9000/my-bucket/my-object" \
  -H "x-amz-server-side-encryption: AES256" \
  -d "Hello, SSE-S3 encryption!"
```

### SSE-KMS Encryption

#### Using Default KMS Key

```rust
use aws_sdk_s3::{primitives::ByteStream, types::ServerSideEncryption};

// Upload object with SSE-KMS encryption (using default key)
let test_data = b"Hello, SSE-KMS encryption!";
client
    .put_object()
    .bucket("my-bucket")
    .key("my-object")
    .body(ByteStream::from(test_data.to_vec()))
    .server_side_encryption(ServerSideEncryption::AwsKms)  // Enable SSE-KMS
    .send()
    .await?;
```

#### Using Specified KMS Key

```rust
// Upload object using specified KMS key
let test_data = b"Hello, SSE-KMS encryption!";
let kms_key_id = "my-custom-kms-key";

client
    .put_object()
    .bucket("my-bucket")
    .key("my-object")
    .body(ByteStream::from(test_data.to_vec()))
    .server_side_encryption(ServerSideEncryption::AwsKms)
    .ssekms_key_id(kms_key_id)  // Specify KMS key ID
    .send()
    .await?;
```

#### Using curl Command

```bash
# Using default KMS key
curl -X PUT "http://localhost:9000/my-bucket/my-object" \
  -H "x-amz-server-side-encryption: aws:kms" \
  -d "Hello, SSE-KMS encryption!"

# Using specified KMS key
curl -X PUT "http://localhost:9000/my-bucket/my-object" \
  -H "x-amz-server-side-encryption: aws:kms" \
  -H "x-amz-server-side-encryption-aws-kms-key-id: my-custom-kms-key" \
  -d "Hello, SSE-KMS encryption!"
```

### SSE-C Encryption

#### Using Customer-Provided Keys

```rust
use base64::{Engine, engine::general_purpose::STANDARD};
use md5::{Digest, Md5};

// Prepare customer key
let customer_key = b"1234567890abcdef1234567890abcdef"; // 32-byte key
let mut hasher = Md5::new();
hasher.update(customer_key);
let customer_key_md5 = STANDARD.encode(hasher.finalize().as_slice());

// Upload object
let test_data = b"Hello, SSE-C encryption!";
client
    .put_object()
    .bucket("my-bucket")
    .key("my-object")
    .body(ByteStream::from(test_data.to_vec()))
    .sse_customer_algorithm("AES256")  // Specify encryption algorithm
    .sse_customer_key(STANDARD.encode(customer_key))  // Base64-encoded key
    .sse_customer_key_md5(customer_key_md5)  // MD5 hash of the key
    .send()
    .await?;

// When downloading, the same key must be provided
let response = client
    .get_object()
    .bucket("my-bucket")
    .key("my-object")
    .sse_customer_algorithm("AES256")
    .sse_customer_key(STANDARD.encode(customer_key))
    .sse_customer_key_md5(customer_key_md5)
    .send()
    .await?;
```

#### Using curl Command

```bash
# Prepare key and MD5 hash
KEY="MTIzNDU2Nzg5MGFiY2RlZjEyMzQ1Njc4OTBhYmNkZWY="  # Base64-encoded 32-byte key
KEY_MD5="$(echo -n "1234567890abcdef1234567890abcdef" | md5sum | cut -d' ' -f1 | xxd -r -p | base64)"

# Upload object
curl -X PUT "http://localhost:9000/my-bucket/my-object" \
  -H "x-amz-server-side-encryption-customer-algorithm: AES256" \
  -H "x-amz-server-side-encryption-customer-key: $KEY" \
  -H "x-amz-server-side-encryption-customer-key-MD5: $KEY_MD5" \
  -d "Hello, SSE-C encryption!"

# Download object
curl "http://localhost:9000/my-bucket/my-object" \
  -H "x-amz-server-side-encryption-customer-algorithm: AES256" \
  -H "x-amz-server-side-encryption-customer-key: $KEY" \
  -H "x-amz-server-side-encryption-customer-key-MD5: $KEY_MD5"

# COPY with SSE-C source and SSE-KMS destination
curl -X PUT "http://localhost:9000/my-bucket/copied" \
    -H "x-amz-copy-source: /my-bucket/my-object" \
    -H "x-amz-copy-source-server-side-encryption-customer-algorithm: AES256" \
    -H "x-amz-copy-source-server-side-encryption-customer-key: $KEY" \
    -H "x-amz-copy-source-server-side-encryption-customer-key-MD5: $KEY_MD5" \
    -H "x-amz-server-side-encryption: aws:kms" \
    -H "x-amz-server-side-encryption-aws-kms-key-id: my-default-kms-key"
```

## Bucket Default Encryption Configuration

### Setting Bucket Default Encryption

You can set default encryption configuration for a bucket, so objects uploaded to that bucket will automatically apply encryption settings.

```rust
use aws_sdk_s3::types::{
    ServerSideEncryption, ServerSideEncryptionByDefault, 
    ServerSideEncryptionConfiguration, ServerSideEncryptionRule
};

// Configure default encryption as SSE-S3
let by_default = ServerSideEncryptionByDefault::builder()
    .sse_algorithm(ServerSideEncryption::Aes256)
    .build()
    .unwrap();

let rule = ServerSideEncryptionRule::builder()
    .apply_server_side_encryption_by_default(by_default)
    .build();

let encryption_config = ServerSideEncryptionConfiguration::builder()
    .rules(rule)
    .build()
    .unwrap();

// Apply encryption configuration to bucket
client
    .put_bucket_encryption()
    .bucket("my-bucket")
    .server_side_encryption_configuration(encryption_config)
    .send()
    .await?;
```

### Configure SSE-KMS Default Encryption

```rust
// Configure default encryption as SSE-KMS
let by_default = ServerSideEncryptionByDefault::builder()
    .sse_algorithm(ServerSideEncryption::AwsKms)
    .kms_master_key_id("my-default-kms-key")  // Optional: specify default KMS key
    .build()
    .unwrap();

let rule = ServerSideEncryptionRule::builder()
    .apply_server_side_encryption_by_default(by_default)
    .build();

let encryption_config = ServerSideEncryptionConfiguration::builder()
    .rules(rule)
    .build()
    .unwrap();

client
    .put_bucket_encryption()
    .bucket("my-bucket")
    .server_side_encryption_configuration(encryption_config)
    .send()
    .await?;
```

### Setting Default Encryption with curl

```bash
# Set SSE-S3 default encryption
curl -X PUT "http://localhost:9000/my-bucket?encryption" \
  -H "Content-Type: application/xml" \
  -d '<?xml version="1.0" encoding="UTF-8"?>
<ServerSideEncryptionConfiguration>
  <Rule>
    <ApplyServerSideEncryptionByDefault>
      <SSEAlgorithm>AES256</SSEAlgorithm>
    </ApplyServerSideEncryptionByDefault>
  </Rule>
</ServerSideEncryptionConfiguration>'

# Set SSE-KMS default encryption
curl -X PUT "http://localhost:9000/my-bucket?encryption" \
  -H "Content-Type: application/xml" \
  -d '<?xml version="1.0" encoding="UTF-8"?>
<ServerSideEncryptionConfiguration>
  <Rule>
    <ApplyServerSideEncryptionByDefault>
      <SSEAlgorithm>aws:kms</SSEAlgorithm>
      <KMSMasterKeyID>my-default-kms-key</KMSMasterKeyID>
    </ApplyServerSideEncryptionByDefault>
  </Rule>
</ServerSideEncryptionConfiguration>'
```

## Multipart Upload Encryption

For large file multipart uploads, encryption is also supported:

```rust
// Create multipart upload with encryption enabled
let multipart_upload = client
    .create_multipart_upload()
    .bucket("my-bucket")
    .key("large-object")
    .server_side_encryption(ServerSideEncryption::Aes256)  // Enable encryption
    .send()
    .await?;

let upload_id = multipart_upload.upload_id().unwrap();

// Upload part
let part_data = vec![b'A'; 5 * 1024 * 1024]; // 5MB part
let upload_part = client
    .upload_part()
    .bucket("my-bucket")
    .key("large-object")
    .upload_id(upload_id)
    .part_number(1)
    .body(ByteStream::from(part_data))
    .send()
    .await?;

// Complete multipart upload
let completed_part = aws_sdk_s3::types::CompletedPart::builder()
    .part_number(1)
    .e_tag(upload_part.e_tag().unwrap())
    .build();

let completed_upload = aws_sdk_s3::types::CompletedMultipartUpload::builder()
    .parts(completed_part)
    .build();

client
    .complete_multipart_upload()
    .bucket("my-bucket")
    .key("large-object")
    .upload_id(upload_id)
    .multipart_upload(completed_upload)
    .send()
    .await?;

Notes
- When a bucket has a default SSE (SSE-S3 or SSE-KMS), CreateMultipartUpload records the intent when the request omits SSE headers.
- CompleteMultipartUpload returns the appropriate SSE headers (and KMS KeyId if applicable) in the response, aligned with S3/MinIO behavior.
- Multipart + SSE-C is not supported.
```

## Viewing Object Encryption Information

```rust
// Get object metadata, including encryption information
let response = client
    .head_object()
    .bucket("my-bucket")
    .key("my-object")
    .send()
    .await?;

// Check encryption type
if let Some(encryption) = response.server_side_encryption() {
    println!("Encryption type: {:?}", encryption);
}

// Check KMS key ID (if using SSE-KMS)
if let Some(key_id) = response.ssekms_key_id() {
    println!("KMS Key ID: {}", key_id);
}
```

## Programming Language Examples

### Python Example

```python
import boto3
import base64
import hashlib

# Create S3 client
s3_client = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='your-access-key',
    aws_secret_access_key='your-secret-key'
)

# SSE-S3 encryption
s3_client.put_object(
    Bucket='my-bucket',
    Key='sse-s3-object',
    Body=b'Hello, SSE-S3!',
    ServerSideEncryption='AES256'
)

# SSE-KMS encryption
s3_client.put_object(
    Bucket='my-bucket',
    Key='sse-kms-object',
    Body=b'Hello, SSE-KMS!',
    ServerSideEncryption='aws:kms',
    SSEKMSKeyId='my-kms-key'
)

# SSE-C encryption
customer_key = b'1234567890abcdef1234567890abcdef'
key_md5 = base64.b64encode(hashlib.md5(customer_key).digest()).decode()

s3_client.put_object(
    Bucket='my-bucket',
    Key='sse-c-object',
    Body=b'Hello, SSE-C!',
    SSECustomerAlgorithm='AES256',
    SSECustomerKey=base64.b64encode(customer_key).decode(),
    SSECustomerKeyMD5=key_md5
)

# Set bucket default encryption
s3_client.put_bucket_encryption(
    Bucket='my-bucket',
    ServerSideEncryptionConfiguration={
        'Rules': [
            {
                'ApplyServerSideEncryptionByDefault': {
                    'SSEAlgorithm': 'AES256'
                }
            }
        ]
    }
)
```

### JavaScript/Node.js Example

```javascript
const AWS = require('aws-sdk');
const crypto = require('crypto');

// Configure S3 client
const s3 = new AWS.S3({
    endpoint: 'http://localhost:9000',
    accessKeyId: 'your-access-key',
    secretAccessKey: 'your-secret-key',
    s3ForcePathStyle: true
});

// SSE-S3 encryption
await s3.putObject({
    Bucket: 'my-bucket',
    Key: 'sse-s3-object',
    Body: 'Hello, SSE-S3!',
    ServerSideEncryption: 'AES256'
}).promise();

// SSE-KMS encryption
await s3.putObject({
    Bucket: 'my-bucket',
    Key: 'sse-kms-object',
    Body: 'Hello, SSE-KMS!',
    ServerSideEncryption: 'aws:kms',
    SSEKMSKeyId: 'my-kms-key'
}).promise();

// SSE-C encryption
const customerKey = Buffer.from('1234567890abcdef1234567890abcdef');
const keyMd5 = crypto.createHash('md5').update(customerKey).digest('base64');

await s3.putObject({
    Bucket: 'my-bucket',
    Key: 'sse-c-object',
    Body: 'Hello, SSE-C!',
    SSECustomerAlgorithm: 'AES256',
    SSECustomerKey: customerKey.toString('base64'),
    SSECustomerKeyMD5: keyMd5
}).promise();

// Set bucket default encryption
await s3.putBucketEncryption({
    Bucket: 'my-bucket',
    ServerSideEncryptionConfiguration: {
        Rules: [
            {
                ApplyServerSideEncryptionByDefault: {
                    SSEAlgorithm: 'AES256'
                }
            }
        ]
    }
}).promise();
```

### Java Example

```java
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.core.sync.RequestBody;
import java.security.MessageDigest;
import java.util.Base64;

// Create S3 client
S3Client s3Client = S3Client.builder()
    .endpointOverride(URI.create("http://localhost:9000"))
    .credentialsProvider(StaticCredentialsProvider.create(
        AwsBasicCredentials.create("your-access-key", "your-secret-key")))
    .build();

// SSE-S3 encryption
s3Client.putObject(PutObjectRequest.builder()
    .bucket("my-bucket")
    .key("sse-s3-object")
    .serverSideEncryption(ServerSideEncryption.AES256)
    .build(),
    RequestBody.fromString("Hello, SSE-S3!"));

// SSE-KMS encryption
s3Client.putObject(PutObjectRequest.builder()
    .bucket("my-bucket")
    .key("sse-kms-object")
    .serverSideEncryption(ServerSideEncryption.AWS_KMS)
    .ssekmsKeyId("my-kms-key")
    .build(),
    RequestBody.fromString("Hello, SSE-KMS!"));

// SSE-C encryption
byte[] customerKey = "1234567890abcdef1234567890abcdef".getBytes();
String keyMd5 = Base64.getEncoder().encodeToString(
    MessageDigest.getInstance("MD5").digest(customerKey));

s3Client.putObject(PutObjectRequest.builder()
    .bucket("my-bucket")
    .key("sse-c-object")
    .sseCustomerAlgorithm("AES256")
    .sseCustomerKey(Base64.getEncoder().encodeToString(customerKey))
    .sseCustomerKeyMD5(keyMd5)
    .build(),
    RequestBody.fromString("Hello, SSE-C!"));
```

## Best Practices

### 1. Choosing the Right Encryption Method
- **SSE-S3**: Suitable for most scenarios, simple and easy to use
- **SSE-KMS**: Use when you need key auditing and fine-grained control
- **SSE-C**: Use when you need complete control over keys

### 2. Key Management
- Regularly rotate KMS keys
- Use different keys for different applications or environments
- Backup important customer-provided keys
- Implement proper key lifecycle management

### 3. Performance Considerations
- Encryption adds minimal CPU overhead
- Performance difference between SSE-S3 and SSE-KMS is negligible
- SSE-C requires transmitting keys with each request
- Consider caching strategies for frequently accessed encrypted objects

### 4. Security Recommendations
- Always use HTTPS in production environments
- Regularly audit encryption configurations
- Monitor KMS key usage
- Use SSE-KMS or SSE-C for sensitive data
- Implement proper access controls and IAM policies

### 5. Compatibility
- RustFS is fully compatible with AWS S3 encryption APIs
- Can use any S3-compatible client library
- Supports all standard S3 encryption headers
- Seamless migration from AWS S3

### 6. Monitoring and Compliance
- Set up monitoring for encryption status
- Implement compliance checks for encryption requirements
- Log encryption-related events for audit purposes
- Regular security assessments and penetration testing

## Troubleshooting

### Common Errors

1. **KMS Key Does Not Exist**
   ```
   Error: The specified KMS key does not exist
   ```
   Solution: Ensure the KMS key is created and accessible

2. **Invalid SSE-C Key Format**
   ```
   Error: The encryption key provided is not valid
   ```
   Solution: Ensure the key is 32 bytes and properly Base64 encoded

3. **Insufficient Permissions**
   ```
   Error: Access denied to KMS key
   ```
   Solution: Check IAM permissions and KMS key policies

4. **Missing Encryption Headers**
   ```
   Error: SSE-C key required for encrypted object
   ```
   Solution: Provide the same SSE-C key used for encryption when accessing the object

### Debugging Tips

1. Use `head_object` to check object encryption status
2. Check server logs for encryption-related errors
3. Verify KMS service health status
4. Ensure client and server time synchronization
5. Test with simple curl commands before integrating into applications

### Performance Optimization

1. **Connection Pooling**: Use HTTP connection pooling for better performance
2. **Batch Operations**: Group multiple operations when possible
3. **Async Processing**: Use asynchronous patterns for non-blocking operations
4. **Caching**: Cache encryption metadata to reduce overhead

## Advanced Features

### Encryption Context (SSE-KMS)

```rust
// Using encryption context with SSE-KMS
let encryption_context = HashMap::from([
    ("department".to_string(), "finance".to_string()),
    ("project".to_string(), "audit-2024".to_string()),
]);

// Note: Encryption context is handled internally by RustFS
// when using SSE-KMS encryption
```

### Cross-Region Replication with Encryption

```rust
// Configure replication with encryption
let replication_config = ReplicationConfiguration::builder()
    .role("arn:aws:iam::account:role/replication-role")
    .rules(
        ReplicationRule::builder()
            .id("replicate-encrypted")
            .status(ReplicationRuleStatus::Enabled)
            .destination(
                Destination::builder()
                    .bucket("arn:aws:s3:::destination-bucket")
                    .encryption_configuration(
                        EncryptionConfiguration::builder()
                            .replica_kms_key_id("destination-kms-key")
                            .build()
                    )
                    .build()
            )
            .build()
    )
    .build();
```

### Bucket Notifications for Encrypted Objects

```rust
// Set up notifications for encrypted object events
let notification_config = NotificationConfiguration::builder()
    .lambda_configurations(
        LambdaConfiguration::builder()
            .id("encrypted-object-processor")
            .lambda_function_arn("arn:aws:lambda:region:account:function:process-encrypted")
            .events(Event::S3ObjectCreatedPut)
            .filter(
                NotificationConfigurationFilter::builder()
                    .key(
                        S3KeyFilter::builder()
                            .filter_rules(
                                FilterRule::builder()
                                    .name(FilterRuleName::Prefix)
                                    .value("encrypted/")
                                    .build()
                            )
                            .build()
                    )
                    .build()
            )
            .build()
    )
    .build();
```

## Reference

For complete test examples and implementation details, see:
- `crates/e2e_test/src/kms/s3_encryption.rs` - Complete encryption test cases
- `rustfs/src/storage/ecfs.rs` - Encryption implementation details
- `crates/kms/src/object_encryption_service.rs` - Object encryption service

These examples demonstrate how to use all encryption features in real-world scenarios and provide comprehensive test coverage for all encryption methods.