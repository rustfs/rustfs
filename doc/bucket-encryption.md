# Bucket Encryption API Documentation

This document describes the bucket encryption management APIs available in RustFS.

## Overview

The bucket encryption APIs allow you to manage encryption configurations for S3 buckets. These APIs support setting, retrieving, deleting, and listing bucket encryption configurations.

## Base URL

All bucket encryption APIs are available under the admin prefix:
```
{ADMIN_PREFIX}/v3/bucket-encryption
```

## Authentication

All APIs require valid credentials passed as query parameters:
- `access_key`: Your access key
- `secret_key`: Your secret key
- `session_token`: Optional session token

## API Endpoints

### 1. Set Bucket Encryption Configuration

**Endpoint:** `PUT /v3/bucket-encryption/{bucket}`

**Description:** Sets or updates the encryption configuration for a specific bucket.

**Parameters:**
- `bucket` (path): The name of the bucket

**Request Body:**
```json
{
  "Rules": [
    {
      "ApplyServerSideEncryptionByDefault": {
        "SSEAlgorithm": "AES256",
        "KMSMasterKeyID": "optional-kms-key-id"
      },
      "BucketKeyEnabled": true
    }
  ]
}
```

**Response:**
- `200 OK`: Configuration set successfully
- `400 Bad Request`: Invalid request body or parameters
- `403 Forbidden`: Authentication failed
- `404 Not Found`: Bucket not found

**Example:**
```bash
curl -X PUT "http://localhost:9000/minio/admin/v3/bucket-encryption/my-bucket?access_key=minioadmin&secret_key=minioadmin" \
  -H "Content-Type: application/json" \
  -d '{
    "Rules": [
      {
        "ApplyServerSideEncryptionByDefault": {
          "SSEAlgorithm": "AES256"
        },
        "BucketKeyEnabled": true
      }
    ]
  }'
```

### 2. Get Bucket Encryption Configuration

**Endpoint:** `GET /v3/bucket-encryption/{bucket}`

**Description:** Retrieves the encryption configuration for a specific bucket.

**Parameters:**
- `bucket` (path): The name of the bucket

**Response:**
```json
{
  "enabled": true,
  "algorithm": "AES256",
  "kms_key_id": "optional-kms-key-id",
  "encrypt_metadata": true,
  "encryption_context": {},
  "created_at": "2024-01-01T00:00:00Z",
  "updated_at": "2024-01-01T00:00:00Z"
}
```

**Status Codes:**
- `200 OK`: Configuration retrieved successfully
- `403 Forbidden`: Authentication failed
- `404 Not Found`: Bucket not found or no encryption configuration

**Example:**
```bash
curl "http://localhost:9000/minio/admin/v3/bucket-encryption/my-bucket?access_key=minioadmin&secret_key=minioadmin"
```

### 3. Delete Bucket Encryption Configuration

**Endpoint:** `DELETE /v3/bucket-encryption/{bucket}`

**Description:** Removes the encryption configuration for a specific bucket.

**Parameters:**
- `bucket` (path): The name of the bucket

**Response:**
- `204 No Content`: Configuration deleted successfully
- `403 Forbidden`: Authentication failed
- `404 Not Found`: Bucket not found or no encryption configuration

**Example:**
```bash
curl -X DELETE "http://localhost:9000/minio/admin/v3/bucket-encryption/my-bucket?access_key=minioadmin&secret_key=minioadmin"
```

### 4. List All Bucket Encryption Configurations

**Endpoint:** `GET /v3/bucket-encryptions`

**Description:** Lists encryption configurations for all buckets.

**Response:**
```json
{
  "configurations": [
    {
      "bucket": "bucket1",
      "enabled": true,
      "algorithm": "AES256",
      "kms_key_id": null,
      "created_at": "2024-01-01T00:00:00Z",
      "updated_at": "2024-01-01T00:00:00Z"
    },
    {
      "bucket": "bucket2",
      "enabled": true,
      "algorithm": "aws:kms",
      "kms_key_id": "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012",
      "created_at": "2024-01-01T00:00:00Z",
      "updated_at": "2024-01-01T00:00:00Z"
    }
  ]
}
```

**Status Codes:**
- `200 OK`: Configurations retrieved successfully
- `403 Forbidden`: Authentication failed

**Example:**
```bash
curl "http://localhost:9000/minio/admin/v3/bucket-encryptions?access_key=minioadmin&secret_key=minioadmin"
```

## Supported Encryption Algorithms

- `AES256`: Server-side encryption with Amazon S3-managed keys
- `aws:kms`: Server-side encryption with AWS KMS-managed keys

## Error Responses

All APIs return standard HTTP status codes. Error responses include a JSON body with error details:

```json
{
  "code": "InvalidRequest",
  "message": "Detailed error message",
  "resource": "/v3/bucket-encryption/my-bucket",
  "request_id": "unique-request-id"
}
```

## Notes

- Bucket names must be valid S3 bucket names
- KMS key IDs are optional and only used with `aws:kms` algorithm
- Encryption configurations are persistent and survive server restarts
- Changes to encryption configuration only affect new objects uploaded to the bucket
- Existing objects retain their original encryption settings

## Security Considerations

- Always use HTTPS in production environments
- Store access keys and secret keys securely
- Regularly rotate access credentials
- Use IAM policies to restrict access to encryption management APIs
- Monitor encryption configuration changes through audit logs