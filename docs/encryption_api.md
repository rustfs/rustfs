# Encryption API Reference

This document describes the REST API endpoints for managing object encryption in RustFS.

## Base URL

```
http://localhost:8080/api/v1
```

## Authentication

All API endpoints require authentication. Include the authorization header:

```
Authorization: Bearer <your-token>
```

## Bucket Encryption Management

### Get Bucket Encryption Configuration

Retrieve the encryption configuration for a specific bucket.

**Endpoint:** `GET /buckets/{bucket_name}/encryption`

**Parameters:**
- `bucket_name` (path): Name of the bucket

**Response:**

```json
{
  "enabled": true,
  "algorithm": "AES256",
  "kms_key_id": "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012",
  "encryption_context": {
    "bucket": "my-bucket",
    "purpose": "data-protection"
  }
}
```

**Status Codes:**
- `200 OK`: Configuration retrieved successfully
- `404 Not Found`: Bucket not found or encryption not configured
- `403 Forbidden`: Insufficient permissions

**Example:**

```bash
curl -X GET \
  http://localhost:8080/api/v1/buckets/my-bucket/encryption \
  -H "Authorization: Bearer <token>"
```

### Set Bucket Encryption Configuration

Configure encryption settings for a bucket.

**Endpoint:** `PUT /buckets/{bucket_name}/encryption`

**Parameters:**
- `bucket_name` (path): Name of the bucket

**Request Body:**

```json
{
  "enabled": true,
  "algorithm": "AES256",
  "kms_key_id": "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012",
  "encryption_context": {
    "bucket": "my-bucket",
    "purpose": "data-protection"
  }
}
```

**Fields:**
- `enabled` (boolean): Whether encryption is enabled
- `algorithm` (string): Encryption algorithm (`AES256` or `ChaCha20Poly1305`)
- `kms_key_id` (string, optional): KMS key identifier
- `encryption_context` (object, optional): Additional encryption context

**Response:**

```json
{
  "message": "Bucket encryption configuration updated successfully",
  "configuration": {
    "enabled": true,
    "algorithm": "AES256",
    "kms_key_id": "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012",
    "encryption_context": {
      "bucket": "my-bucket",
      "purpose": "data-protection"
    }
  }
}
```

**Status Codes:**
- `200 OK`: Configuration updated successfully
- `400 Bad Request`: Invalid configuration
- `404 Not Found`: Bucket not found
- `403 Forbidden`: Insufficient permissions

**Example:**

```bash
curl -X PUT \
  http://localhost:8080/api/v1/buckets/my-bucket/encryption \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{
    "enabled": true,
    "algorithm": "AES256",
    "kms_key_id": "my-kms-key-id"
  }'
```

### Delete Bucket Encryption Configuration

Disable encryption for a bucket.

**Endpoint:** `DELETE /buckets/{bucket_name}/encryption`

**Parameters:**
- `bucket_name` (path): Name of the bucket

**Response:**

```json
{
  "message": "Bucket encryption configuration deleted successfully"
}
```

**Status Codes:**
- `200 OK`: Configuration deleted successfully
- `404 Not Found`: Bucket not found or encryption not configured
- `403 Forbidden`: Insufficient permissions

**Example:**

```bash
curl -X DELETE \
  http://localhost:8080/api/v1/buckets/my-bucket/encryption \
  -H "Authorization: Bearer <token>"
```

## Object Operations with Encryption

### Upload Encrypted Object

Upload an object with encryption.

**Endpoint:** `PUT /buckets/{bucket_name}/objects/{object_key}`

**Parameters:**
- `bucket_name` (path): Name of the bucket
- `object_key` (path): Key/name of the object

**Headers:**
- `Content-Type`: MIME type of the object
- `Content-Length`: Size of the object in bytes
- `x-amz-server-side-encryption` (optional): Encryption algorithm (`AES256` or `ChaCha20Poly1305`)
- `x-amz-server-side-encryption-aws-kms-key-id` (optional): KMS key ID
- `x-amz-server-side-encryption-context` (optional): Base64-encoded encryption context JSON

**Request Body:** Binary object data

**Response:**

```json
{
  "etag": "\"d41d8cd98f00b204e9800998ecf8427e\"",
  "version_id": "null",
  "encryption": {
    "algorithm": "AES256",
    "kms_key_id": "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012"
  }
}
```

**Status Codes:**
- `200 OK`: Object uploaded successfully
- `400 Bad Request`: Invalid request
- `404 Not Found`: Bucket not found
- `403 Forbidden`: Insufficient permissions
- `507 Insufficient Storage`: Not enough storage space

**Example:**

```bash
curl -X PUT \
  http://localhost:8080/api/v1/buckets/my-bucket/objects/my-file.txt \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: text/plain" \
  -H "x-amz-server-side-encryption: AES256" \
  -H "x-amz-server-side-encryption-aws-kms-key-id: my-kms-key-id" \
  --data-binary @my-file.txt
```

### Download Encrypted Object

Download and decrypt an object.

**Endpoint:** `GET /buckets/{bucket_name}/objects/{object_key}`

**Parameters:**
- `bucket_name` (path): Name of the bucket
- `object_key` (path): Key/name of the object

**Query Parameters:**
- `version_id` (optional): Specific version of the object
- `range` (optional): Byte range to download (format: `bytes=start-end`)

**Response Headers:**
- `Content-Type`: MIME type of the object
- `Content-Length`: Size of the decrypted object
- `ETag`: Entity tag of the object
- `Last-Modified`: Last modification date
- `x-amz-server-side-encryption`: Encryption algorithm used
- `x-amz-server-side-encryption-aws-kms-key-id`: KMS key ID used

**Response Body:** Decrypted object data

**Status Codes:**
- `200 OK`: Object downloaded successfully
- `206 Partial Content`: Partial object downloaded (when using range)
- `404 Not Found`: Object not found
- `403 Forbidden`: Insufficient permissions
- `416 Range Not Satisfiable`: Invalid range request

**Example:**

```bash
curl -X GET \
  http://localhost:8080/api/v1/buckets/my-bucket/objects/my-file.txt \
  -H "Authorization: Bearer <token>" \
  -o downloaded-file.txt
```

### Get Object Metadata

Retrieve metadata for an encrypted object without downloading the content.

**Endpoint:** `HEAD /buckets/{bucket_name}/objects/{object_key}`

**Parameters:**
- `bucket_name` (path): Name of the bucket
- `object_key` (path): Key/name of the object

**Response Headers:**
- `Content-Type`: MIME type of the object
- `Content-Length`: Size of the decrypted object
- `ETag`: Entity tag of the object
- `Last-Modified`: Last modification date
- `x-amz-server-side-encryption`: Encryption algorithm used
- `x-amz-server-side-encryption-aws-kms-key-id`: KMS key ID used

**Status Codes:**
- `200 OK`: Metadata retrieved successfully
- `404 Not Found`: Object not found
- `403 Forbidden`: Insufficient permissions

**Example:**

```bash
curl -I \
  http://localhost:8080/api/v1/buckets/my-bucket/objects/my-file.txt \
  -H "Authorization: Bearer <token>"
```

## KMS Management

### List KMS Keys

List available KMS keys.

**Endpoint:** `GET /kms/keys`

**Query Parameters:**
- `limit` (optional): Maximum number of keys to return (default: 100)
- `marker` (optional): Pagination marker

**Response:**

```json
{
  "keys": [
    {
      "key_id": "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012",
      "alias": "alias/my-encryption-key",
      "description": "Key for bucket encryption",
      "enabled": true,
      "created_date": "2024-01-01T00:00:00Z"
    }
  ],
  "truncated": false,
  "next_marker": null
}
```

**Status Codes:**
- `200 OK`: Keys listed successfully
- `403 Forbidden`: Insufficient permissions
- `500 Internal Server Error`: KMS service error

**Example:**

```bash
curl -X GET \
  http://localhost:8080/api/v1/kms/keys \
  -H "Authorization: Bearer <token>"
```

### Create KMS Key

Create a new KMS key.

**Endpoint:** `POST /kms/keys`

**Request Body:**

```json
{
  "description": "Key for bucket encryption",
  "usage": "ENCRYPT_DECRYPT",
  "key_spec": "SYMMETRIC_DEFAULT"
}
```

**Response:**

```json
{
  "key_id": "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012",
  "description": "Key for bucket encryption",
  "enabled": true,
  "created_date": "2024-01-01T00:00:00Z"
}
```

**Status Codes:**
- `201 Created`: Key created successfully
- `400 Bad Request`: Invalid request
- `403 Forbidden`: Insufficient permissions
- `500 Internal Server Error`: KMS service error

**Example:**

```bash
curl -X POST \
  http://localhost:8080/api/v1/kms/keys \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{
    "description": "Key for bucket encryption",
    "usage": "ENCRYPT_DECRYPT"
  }'
```

## Monitoring and Statistics

### Get Encryption Statistics

Retrieve encryption-related statistics.

**Endpoint:** `GET /encryption/stats`

**Query Parameters:**
- `bucket` (optional): Filter by bucket name
- `start_date` (optional): Start date for statistics (ISO 8601 format)
- `end_date` (optional): End date for statistics (ISO 8601 format)

**Response:**

```json
{
  "total_encrypted_objects": 1500,
  "total_encrypted_bytes": 1073741824,
  "encryption_operations": {
    "encrypt_count": 1500,
    "decrypt_count": 3000,
    "avg_encrypt_time_ms": 25.5,
    "avg_decrypt_time_ms": 15.2
  },
  "algorithm_usage": {
    "AES256": 1200,
    "ChaCha20Poly1305": 300
  },
  "cache_stats": {
    "hit_rate": 0.85,
    "total_requests": 5000,
    "cache_hits": 4250,
    "cache_misses": 750
  }
}
```

**Status Codes:**
- `200 OK`: Statistics retrieved successfully
- `403 Forbidden`: Insufficient permissions

**Example:**

```bash
curl -X GET \
  http://localhost:8080/api/v1/encryption/stats \
  -H "Authorization: Bearer <token>"
```

### Get Audit Logs

Retrieve encryption audit logs.

**Endpoint:** `GET /encryption/audit`

**Query Parameters:**
- `bucket` (optional): Filter by bucket name
- `operation` (optional): Filter by operation type (`encrypt`, `decrypt`, `key_generation`)
- `start_date` (optional): Start date for logs (ISO 8601 format)
- `end_date` (optional): End date for logs (ISO 8601 format)
- `limit` (optional): Maximum number of entries to return (default: 100)
- `offset` (optional): Number of entries to skip (default: 0)

**Response:**

```json
{
  "entries": [
    {
      "timestamp": "2024-01-01T12:00:00Z",
      "operation": "encrypt",
      "bucket": "my-bucket",
      "object_key": "my-file.txt",
      "algorithm": "AES256",
      "kms_key_id": "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012",
      "user_id": "user123",
      "status": "success",
      "duration_ms": 25
    }
  ],
  "total_count": 1500,
  "has_more": true
}
```

**Status Codes:**
- `200 OK`: Audit logs retrieved successfully
- `403 Forbidden`: Insufficient permissions

**Example:**

```bash
curl -X GET \
  "http://localhost:8080/api/v1/encryption/audit?operation=encrypt&limit=50" \
  -H "Authorization: Bearer <token>"
```

## Error Responses

All endpoints may return the following error format:

```json
{
  "error": {
    "code": "ENCRYPTION_ERROR",
    "message": "Failed to decrypt object: invalid key",
    "details": {
      "bucket": "my-bucket",
      "object_key": "my-file.txt",
      "kms_key_id": "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012"
    }
  }
}
```

### Common Error Codes

- `BUCKET_NOT_FOUND`: Specified bucket does not exist
- `OBJECT_NOT_FOUND`: Specified object does not exist
- `ENCRYPTION_ERROR`: General encryption/decryption error
- `KMS_ERROR`: KMS service error
- `INVALID_ALGORITHM`: Unsupported encryption algorithm
- `KEY_NOT_FOUND`: Specified KMS key not found
- `INSUFFICIENT_PERMISSIONS`: User lacks required permissions
- `INVALID_CONFIGURATION`: Invalid encryption configuration
- `NETWORK_ERROR`: Network connectivity issue
- `TIMEOUT_ERROR`: Operation timed out

## Rate Limiting

API endpoints are subject to rate limiting:

- **Bucket operations**: 100 requests per minute
- **Object operations**: 1000 requests per minute
- **KMS operations**: 50 requests per minute
- **Monitoring operations**: 20 requests per minute

Rate limit headers are included in responses:

```
X-RateLimit-Limit: 1000
X-RateLimit-Remaining: 999
X-RateLimit-Reset: 1640995200
```

## SDK Examples

### Python

```python
import requests
import json

base_url = "http://localhost:8080/api/v1"
headers = {
    "Authorization": "Bearer <your-token>",
    "Content-Type": "application/json"
}

# Configure bucket encryption
config = {
    "enabled": True,
    "algorithm": "AES256",
    "kms_key_id": "my-kms-key-id"
}

response = requests.put(
    f"{base_url}/buckets/my-bucket/encryption",
    headers=headers,
    json=config
)

print(response.json())
```

### JavaScript

```javascript
const baseUrl = 'http://localhost:8080/api/v1';
const headers = {
    'Authorization': 'Bearer <your-token>',
    'Content-Type': 'application/json'
};

// Upload encrypted object
const uploadObject = async (bucketName, objectKey, data) => {
    const response = await fetch(
        `${baseUrl}/buckets/${bucketName}/objects/${objectKey}`,
        {
            method: 'PUT',
            headers: {
                ...headers,
                'x-amz-server-side-encryption': 'AES256',
                'x-amz-server-side-encryption-aws-kms-key-id': 'my-kms-key-id'
            },
            body: data
        }
    );
    
    return response.json();
};
```

### Rust

```rust
use reqwest::Client;
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new();
    let base_url = "http://localhost:8080/api/v1";
    
    // Get bucket encryption configuration
    let response = client
        .get(&format!("{}/buckets/my-bucket/encryption", base_url))
        .header("Authorization", "Bearer <your-token>")
        .send()
        .await?;
    
    let config: serde_json::Value = response.json().await?;
    println!("Encryption config: {}", config);
    
    Ok(())
}
```

## Testing

Test the encryption API endpoints:

```bash
# Run API tests
cargo test --test api_encryption_test

# Test with curl
./scripts/test_encryption_api.sh
```

For more examples and detailed testing procedures, see the [Testing Guide](testing.md).