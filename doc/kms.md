# KMS (Key Management Service) API Documentation

## Overview

The KMS module provides key management functionality for RustFS, allowing you to create, manage, and use encryption keys for data protection. This document describes the available KMS APIs and their usage.

## Configuration

### Configure KMS

Before using KMS functionality, you need to configure the KMS service.

**Endpoint:** `POST /rustfs/admin/v3/kms/configure`

**Request Body:**
```json
{
  "kms_type": "vault",
  "vault_address": "https://vault.example.com:8200",
  "vault_token": "your-vault-token",
  "vault_namespace": "admin",
  "vault_mount_path": "transit"
}
```

**Response:**
```json
{
  "status": "success",
  "message": "KMS configured successfully"
}
```

## Key Management

### Create KMS Key

Create a new encryption key in the KMS.

**Endpoint:** `POST /rustfs/admin/v3/kms/key/create`

**Query Parameters:**
- `keyName` (optional): Name for the key (if not provided, a UUID will be generated)

**Request Body:** None (empty body)

**Response:**
```json
{
  "keyId": "my-encryption-key",
  "status": "created",
  "createdAt": "2024-01-15T10:30:00Z"
}
```

### List KMS Keys

Retrieve a list of all available KMS keys.

**Endpoint:** `GET /rustfs/admin/v3/kms/key/list`

**Query Parameters:**
- `pattern` (optional): Filter keys by pattern

**Response:**
```json
{
  "keys": [
    {
      "keyId": "my-encryption-key",
      "status": "enabled",
      "createdAt": "2024-01-15T10:30:00Z"
    }
  ]
}
```

### Get KMS Key Status

Get the status of a specific KMS key.

**Endpoint:** `GET /rustfs/admin/v3/kms/key/status`

**Query Parameters:**
- `keyName` (required): The key identifier

**Response:**
```json
{
  "keyId": "my-encryption-key",
  "status": "enabled",
  "createdAt": "2024-01-15T10:30:00Z",
  "algorithm": "AES256"
}
```

### Enable KMS Key

Enable a disabled KMS key.

**Endpoint:** `POST /rustfs/admin/v3/kms/key/enable`

**Query Parameters:**
- `keyName` (required): The key identifier

**Response:**
```json
{
  "keyId": "my-encryption-key",
  "status": "enabled",
  "message": "Key enabled successfully"
}
```

### Disable KMS Key

Disable an active KMS key.

**Endpoint:** `POST /rustfs/admin/v3/kms/key/disable`

**Query Parameters:**
- `keyName` (required): The key identifier

**Response:**
```json
{
  "keyId": "my-encryption-key",
  "status": "disabled",
  "message": "Key disabled successfully"
}
```

## KMS Status

### Get KMS Status

Get the overall status of the KMS service.

**Endpoint:** `GET /rustfs/admin/v3/kms/status`

**Response:**
```json
{
  "status": "connected",
  "kmsType": "vault",
  "endpoint": "https://vault.example.com:8200",
  "keyCount": 5,
  "defaultKeyId": "my-encryption-key"
}
```

## Authentication

All KMS API endpoints require proper authentication. Include the following headers in your requests:

```
Authorization: AWS4-HMAC-SHA256 Credential=...
X-Amz-Date: 20240115T103000Z
X-Amz-Content-Sha256: ...
```

## Error Handling

The KMS API returns standard HTTP status codes and error responses:

**Common Error Response:**
```json
{
  "error": {
    "code": "KMSKeyNotFound",
    "message": "The specified KMS key does not exist",
    "resource": "/minio/admin/v3/kms/key/status"
  }
}
```

**Common Error Codes:**
- `KMSNotConfigured`: KMS service is not configured
- `KMSKeyNotFound`: Specified key does not exist
- `KMSKeyAlreadyExists`: Key with the same ID already exists
- `KMSConnectionError`: Cannot connect to KMS backend
- `KMSInvalidRequest`: Invalid request parameters

## Examples

### Using curl

```bash
# Configure KMS
curl -X POST http://localhost:9000/minio/admin/v3/kms/configure \
  -H "Content-Type: application/json" \
  -H "Authorization: AWS4-HMAC-SHA256 ..." \
  -d '{
    "kms_type": "vault",
    "vault_address": "https://vault.example.com:8200",
    "vault_token": "your-vault-token"
  }'

# Create a new key
curl -X POST "http://localhost:9000/minio/admin/v3/kms/key/create?keyName=my-key" \
  -H "Authorization: AWS4-HMAC-SHA256 ..."

# List all keys
curl -X GET http://localhost:9000/minio/admin/v3/kms/key/list \
  -H "Authorization: AWS4-HMAC-SHA256 ..."

# Get key status
curl -X GET "http://localhost:9000/minio/admin/v3/kms/key/status?keyName=my-key" \
  -H "Authorization: AWS4-HMAC-SHA256 ..."
```

## Security Considerations

1. **Secure Communication**: Always use HTTPS in production environments
2. **Key Rotation**: Regularly rotate your KMS keys
3. **Access Control**: Implement proper IAM policies to restrict KMS access
4. **Audit Logging**: Enable audit logging for all KMS operations
5. **Backup**: Ensure your KMS backend (e.g., Vault) is properly backed up

## Supported KMS Backends

- **Vault**: HashiCorp Vault integration
- **Local**: Built-in local key management (for development only)

For production use, it's recommended to use external KMS solutions like HashiCorp Vault for enhanced security and key management capabilities.