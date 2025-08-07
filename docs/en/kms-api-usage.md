# KMS API Usage Guide

This document describes how to use the KMS (Key Management Service) API in RustFS with examples and best practices.

## Overview

RustFS KMS provides comprehensive key management services, supporting key creation, querying, enabling, disabling, and KMS service configuration and status monitoring.

## API Endpoints

All KMS APIs use `/rustfs/admin/v3/kms` as the base path.

### 1. Configure KMS Service

**Endpoint**: `POST /rustfs/admin/v3/kms/configure`

**Description**: Configure or reconfigure the KMS service

**Request Body**:
```json
{
  "kms_type": "vault",
  "vault_address": "https://vault.example.com:8200",
  "vault_token": "your-vault-token",
  "vault_namespace": "admin",
  "vault_mount_path": "transit",
  "vault_timeout_seconds": 30,
  "vault_app_role_id": "your-app-role-id",
  "vault_app_role_secret_id": "your-app-role-secret"
}
```

**Response**:
```json
{
  "success": true,
  "message": "KMS configured successfully",
  "kms_type": "vault"
}
```

**Example**:
```bash
curl -X POST "http://localhost:9000/rustfs/admin/v3/kms/configure" \
  -H "Content-Type: application/json" \
  -d '{
    "kms_type": "vault",
    "vault_address": "https://vault.example.com:8200",
    "vault_token": "your-vault-token"
  }'
```

### 2. Create KMS Key

**Endpoint**: `POST /rustfs/admin/v3/kms/key/create`

**Description**: Create a new KMS key

**Query Parameters**:
- `keyName` (optional): Key name, will be auto-generated if not provided

**Response**:
```json
{
  "keyId": "rustfs-key-12345678-1234-1234-1234-123456789abc",
  "keyName": "my-encryption-key",
  "status": "Enabled",
  "createdAt": "2024-01-15T10:30:00Z"
}
```

**Examples**:
```bash
# Create key with specified name
curl -X POST "http://localhost:9000/rustfs/admin/v3/kms/key/create?keyName=my-encryption-key"

# Create key with auto-generated name
curl -X POST "http://localhost:9000/rustfs/admin/v3/kms/key/create"
```

### 3. Get Key Status

**Endpoint**: `GET /rustfs/admin/v3/kms/key/status`

**Description**: Get detailed status information for a specific key

**Query Parameters**:
- `keyName` (required): Name of the key to query

**Response**:
```json
{
  "keyId": "rustfs-key-12345678-1234-1234-1234-123456789abc",
  "keyName": "my-encryption-key",
  "status": "Enabled",
  "createdAt": "2024-01-15T10:30:00Z",
  "algorithm": "AES-256"
}
```

**Example**:
```bash
curl "http://localhost:9000/rustfs/admin/v3/kms/key/status?keyName=my-encryption-key"
```

### 4. List All Keys

**Endpoint**: `GET /rustfs/admin/v3/kms/key/list`

**Description**: Get a list of all KMS keys

**Response**:
```json
{
  "keys": [
    {
      "keyId": "rustfs-key-12345678-1234-1234-1234-123456789abc",
      "keyName": "my-encryption-key",
      "status": "Enabled",
      "createdAt": "2024-01-15T10:30:00Z",
      "algorithm": "AES-256"
    },
    {
      "keyId": "rustfs-key-87654321-4321-4321-4321-cba987654321",
      "keyName": "backup-key",
      "status": "Disabled",
      "createdAt": "2024-01-14T15:20:00Z",
      "algorithm": "AES-256"
    }
  ]
}
```

**Example**:
```bash
curl "http://localhost:9000/rustfs/admin/v3/kms/key/list"
```

### 5. Enable Key

**Endpoint**: `PUT /rustfs/admin/v3/kms/key/enable`

**Description**: Enable a specific KMS key

**Query Parameters**:
- `keyName` (required): Name of the key to enable

**Response**:
```json
{
  "keyId": "rustfs-key-12345678-1234-1234-1234-123456789abc",
  "keyName": "my-encryption-key",
  "status": "Enabled",
  "createdAt": "2024-01-15T10:30:00Z",
  "algorithm": "AES-256"
}
```

**Example**:
```bash
curl -X PUT "http://localhost:9000/rustfs/admin/v3/kms/key/enable?keyName=my-encryption-key"
```

### 6. Disable Key

**Endpoint**: `PUT /rustfs/admin/v3/kms/key/disable`

**Description**: Disable a specific KMS key

**Query Parameters**:
- `keyName` (required): Name of the key to disable

**Response**:
```json
{
  "keyId": "rustfs-key-12345678-1234-1234-1234-123456789abc",
  "keyName": "my-encryption-key",
  "status": "Disabled",
  "createdAt": "2024-01-15T10:30:00Z",
  "algorithm": "AES-256"
}
```

**Example**:
```bash
curl -X PUT "http://localhost:9000/rustfs/admin/v3/kms/key/disable?keyName=my-encryption-key"
```

### 7. Get KMS Status

**Endpoint**: `GET /rustfs/admin/v3/kms/status`

**Description**: Get the overall status of the KMS service

**Response**:
```json
{
  "status": "Active",
  "backend": "vault",
  "healthy": true
}
```

**Example**:
```bash
curl "http://localhost:9000/rustfs/admin/v3/kms/status"
```

## Error Handling

When API calls fail, an error response is returned:

```json
{
  "code": "KMSNotConfigured",
  "message": "KMS is not configured",
  "description": "Key Management Service is not available"
}
```

Common error codes:
- `KMSNotConfigured`: KMS service is not configured
- `MissingParameter`: Required parameter is missing
- `KeyNotFound`: Specified key does not exist
- `InvalidConfiguration`: Configuration parameters are invalid

## Programming Examples

### Rust Example

```rust
use rustfs_kms::{get_global_kms, ListKeysRequest};

// Get global KMS instance
if let Some(kms) = get_global_kms() {
    // List all keys
    let keys = kms.list_keys(&ListKeysRequest::default(), None).await?;
    println!("Found {} keys", keys.keys.len());
    
    // Create new key
    let key_info = kms.create_key("my-new-key", "AES-256", None).await?;
    println!("Created key: {}", key_info.key_id);
    
    // Query key status
    let key_status = kms.describe_key("my-new-key", None).await?;
    println!("Key status: {:?}", key_status.status);
} else {
    println!("KMS not initialized");
}
```

### Python Example

```python
import requests
import json

base_url = "http://localhost:9000/rustfs/admin/v3/kms"

# Configure KMS
config_data = {
    "kms_type": "vault",
    "vault_address": "https://vault.example.com:8200",
    "vault_token": "your-vault-token"
}
response = requests.post(f"{base_url}/configure", json=config_data)
print(f"Configure KMS: {response.json()}")

# Create key
response = requests.post(f"{base_url}/key/create?keyName=python-test-key")
key_info = response.json()
print(f"Created key: {key_info}")

# List all keys
response = requests.get(f"{base_url}/key/list")
keys = response.json()
print(f"All keys: {keys}")

# Query key status
response = requests.get(f"{base_url}/key/status?keyName=python-test-key")
status = response.json()
print(f"Key status: {status}")
```

### JavaScript Example

```javascript
const baseUrl = 'http://localhost:9000/rustfs/admin/v3/kms';

// Configure KMS
const configData = {
  kms_type: 'vault',
  vault_address: 'https://vault.example.com:8200',
  vault_token: 'your-vault-token'
};

fetch(`${baseUrl}/configure`, {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify(configData)
})
.then(response => response.json())
.then(data => console.log('Configure KMS:', data));

// Create key
fetch(`${baseUrl}/key/create?keyName=js-test-key`, { method: 'POST' })
.then(response => response.json())
.then(data => console.log('Created key:', data));

// List all keys
fetch(`${baseUrl}/key/list`)
.then(response => response.json())
.then(data => console.log('All keys:', data));
```

## Best Practices

### 1. Authentication and Security
- Always use appropriate admin credentials for KMS API calls
- Use HTTPS in production environments to protect API communication
- Regularly rotate vault tokens and credentials
- Implement proper access controls and audit logging

### 2. Key Management
- Use descriptive names for keys to improve manageability
- Regularly rotate encryption keys according to your security policy
- Use different keys for different applications or environments
- Backup important customer-provided keys
- Monitor key usage and access patterns

### 3. Error Handling
- Implement proper retry logic for transient failures
- Log errors appropriately for debugging and monitoring
- Handle KMS unavailability gracefully in your applications
- Validate configuration parameters before making API calls

### 4. Performance Considerations
- Cache key information when appropriate to reduce API calls
- Use connection pooling for high-frequency operations
- Monitor KMS response times and set appropriate timeouts
- Consider using async/await patterns for better concurrency

### 5. Monitoring and Maintenance
- Use the `/kms/status` endpoint to monitor KMS service health
- Set up alerts for KMS service failures or degraded performance
- Regularly review and audit key usage
- Keep KMS configuration and dependencies up to date

### 6. Development and Testing
- Use separate KMS instances for development, testing, and production
- Implement comprehensive tests for KMS integration
- Use mock KMS services for unit testing when appropriate
- Document KMS configuration requirements for your team

## Integration Examples

### Spring Boot (Java) Integration

```java
@RestController
@RequestMapping("/api/kms")
public class KmsController {
    
    private final String kmsBaseUrl = "http://localhost:9000/rustfs/admin/v3/kms";
    private final RestTemplate restTemplate = new RestTemplate();
    
    @PostMapping("/keys")
    public ResponseEntity<String> createKey(@RequestParam String keyName) {
        String url = kmsBaseUrl + "/key/create?keyName=" + keyName;
        ResponseEntity<String> response = restTemplate.postForEntity(url, null, String.class);
        return response;
    }
    
    @GetMapping("/keys")
    public ResponseEntity<String> listKeys() {
        String url = kmsBaseUrl + "/key/list";
        ResponseEntity<String> response = restTemplate.getForEntity(url, String.class);
        return response;
    }
}
```

### Go Integration

```go
package main

import (
    "bytes"
    "encoding/json"
    "fmt"
    "net/http"
)

type KmsClient struct {
    BaseURL string
    Client  *http.Client
}

func NewKmsClient(baseURL string) *KmsClient {
    return &KmsClient{
        BaseURL: baseURL,
        Client:  &http.Client{},
    }
}

func (k *KmsClient) CreateKey(keyName string) error {
    url := fmt.Sprintf("%s/key/create?keyName=%s", k.BaseURL, keyName)
    resp, err := k.Client.Post(url, "application/json", nil)
    if err != nil {
        return err
    }
    defer resp.Body.Close()
    
    if resp.StatusCode != http.StatusOK {
        return fmt.Errorf("failed to create key: %s", resp.Status)
    }
    
    return nil
}

func (k *KmsClient) ListKeys() ([]byte, error) {
    url := fmt.Sprintf("%s/key/list", k.BaseURL)
    resp, err := k.Client.Get(url)
    if err != nil {
        return nil, err
    }
    defer resp.Body.Close()
    
    var result bytes.Buffer
    _, err = result.ReadFrom(resp.Body)
    return result.Bytes(), err
}
```

## Troubleshooting

### Common Issues

1. **KMS Service Not Available**
   ```
   Error: KMS is not configured
   ```
   Solution: Configure KMS using the `/configure` endpoint first

2. **Invalid Vault Configuration**
   ```
   Error: Failed to connect to Vault
   ```
   Solution: Verify vault address, token, and network connectivity

3. **Key Not Found**
   ```
   Error: The specified key does not exist
   ```
   Solution: Verify the key name and ensure it was created successfully

4. **Permission Denied**
   ```
   Error: Access denied
   ```
   Solution: Check admin credentials and IAM permissions

### Debugging Tips

1. Use the `/kms/status` endpoint to check KMS health
2. Check server logs for detailed error messages
3. Verify network connectivity to external KMS providers
4. Ensure proper authentication headers are included
5. Test with simple curl commands before integrating into applications

### Performance Optimization

1. **Connection Pooling**: Use HTTP connection pooling for better performance
2. **Caching**: Cache key metadata to reduce API calls
3. **Batch Operations**: Group multiple operations when possible
4. **Async Processing**: Use asynchronous patterns for non-blocking operations

## Reference

For complete test examples and implementation details, see:
- `crates/e2e_test/src/kms/s3_encryption.rs` - Complete encryption test cases
- `rustfs/src/admin/handlers/kms.rs` - KMS handler implementations
- `crates/kms/src/lib.rs` - KMS library API reference

These examples demonstrate how to use all KMS features in real-world scenarios.