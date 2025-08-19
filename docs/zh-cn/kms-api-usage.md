# KMS API 使用文档

本文档介绍 RustFS 中 KMS (Key Management Service) API 的使用方法和示例。

## 概述

RustFS KMS 提供了完整的密钥管理服务，支持密钥的创建、查询、启用、禁用等操作，以及 KMS 服务本身的配置和状态查询。

## API 端点

所有 KMS API 都使用 `/rustfs/admin/v3/kms` 作为基础路径。

### 1. 配置 KMS 服务

**端点**: `POST /rustfs/admin/v3/kms/configure`

**描述**: 配置或重新配置 KMS 服务

**请求体**:
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

**响应**:
```json
{
  "success": true,
  "message": "KMS configured successfully",
  "kms_type": "vault"
}
```

**示例**:
```bash
curl -X POST "http://localhost:9000/rustfs/admin/v3/kms/configure" \
  -H "Content-Type: application/json" \
  -d '{
    "kms_type": "vault",
    "vault_address": "https://vault.example.com:8200",
    "vault_token": "your-vault-token"
  }'
```

### 2. 创建 KMS 密钥

**端点**: `POST /rustfs/admin/v3/kms/key/create`

**描述**: 创建新的 KMS 密钥

**请求体（推荐）**:
```json
{
  "keyName": "my-encryption-key",
  "algorithm": "AES-256"
}
```

**兼容的查询参数（旧版）**:
- `keyName` (可选): 密钥名称
- `algorithm` (可选): 算法，默认 `AES-256`

**响应**:
```json
{
  "keyId": "rustfs-key-12345678-1234-1234-1234-123456789abc",
  "keyName": "my-encryption-key",
  "status": "Enabled",
  "createdAt": "2024-01-15T10:30:00Z"
}
```

**示例**:
```bash
# 使用 JSON 请求体（推荐）
curl -X POST "http://localhost:9000/rustfs/admin/v3/kms/key/create" \
  -H 'Content-Type: application/json' \
  -d '{"keyName":"my-encryption-key","algorithm":"AES-256"}'

# 兼容旧版查询参数
curl -X POST "http://localhost:9000/rustfs/admin/v3/kms/key/create?keyName=my-encryption-key&algorithm=AES-256"

# 自动命名
curl -X POST "http://localhost:9000/rustfs/admin/v3/kms/key/create"
```

### 3. 查询密钥状态

**端点**: `GET /rustfs/admin/v3/kms/key/status`

**描述**: 获取指定密钥的详细状态信息

**查询参数**:
- `keyName` (必需): 要查询的密钥名称

**响应**:
```json
{
  "keyId": "rustfs-key-12345678-1234-1234-1234-123456789abc",
  "keyName": "my-encryption-key",
  "status": "Enabled",
  "createdAt": "2024-01-15T10:30:00Z",
  "algorithm": "AES-256"
}
```

**示例**:
```bash
curl "http://localhost:9000/rustfs/admin/v3/kms/key/status?keyName=my-encryption-key"
```

### 4. 列出所有密钥

**端点**: `GET /rustfs/admin/v3/kms/key/list`

**描述**: 获取所有 KMS 密钥的列表

**响应**:
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

**示例**:
```bash
curl "http://localhost:9000/rustfs/admin/v3/kms/key/list"
```

### 5. 启用密钥

**端点**: `PUT /rustfs/admin/v3/kms/key/enable`

**描述**: 启用指定的 KMS 密钥

**查询参数**:
- `keyName` (必需): 要启用的密钥名称

**响应**:
```json
{
  "keyId": "rustfs-key-12345678-1234-1234-1234-123456789abc",
  "keyName": "my-encryption-key",
  "status": "Enabled",
  "createdAt": "2024-01-15T10:30:00Z",
  "algorithm": "AES-256"
}
```

**示例**:
```bash
curl -X PUT "http://localhost:9000/rustfs/admin/v3/kms/key/enable?keyName=my-encryption-key"
```

### 6. 禁用密钥

**端点**: `PUT /rustfs/admin/v3/kms/key/disable`

**描述**: 禁用指定的 KMS 密钥

**查询参数**:
- `keyName` (必需): 要禁用的密钥名称

**响应**:
```json
{
  "keyId": "rustfs-key-12345678-1234-1234-1234-123456789abc",
  "keyName": "my-encryption-key",
  "status": "Disabled",
  "createdAt": "2024-01-15T10:30:00Z",
  "algorithm": "AES-256"
}
```

**示例**:
```bash
curl -X PUT "http://localhost:9000/rustfs/admin/v3/kms/key/disable?keyName=my-encryption-key"
```

### 7. 查询 KMS 状态

**端点**: `GET /rustfs/admin/v3/kms/status`

**描述**: 获取 KMS 服务的整体状态

**响应**:
```json
{
  "status": "Active",
  "backend": "vault",
  "healthy": true
}
```

**示例**:
```bash
curl "http://localhost:9000/rustfs/admin/v3/kms/status"
```

## 错误处理

当 API 调用失败时，会返回错误响应：

```json
{
  "code": "KMSNotConfigured",
  "message": "KMS is not configured",
  "description": "Key Management Service is not available"
}
```

常见错误代码：
- `KMSNotConfigured`: KMS 服务未配置
- `MissingParameter`: 缺少必需参数
- `KeyNotFound`: 指定的密钥不存在
- `InvalidConfiguration`: 配置参数无效

## 编程示例

### Rust 示例

```rust
use rustfs_kms::{get_global_kms, ListKeysRequest};

// 获取全局 KMS 实例
if let Some(kms) = get_global_kms() {
    // 列出所有密钥
    let keys = kms.list_keys(&ListKeysRequest::default(), None).await?;
    println!("Found {} keys", keys.keys.len());
    
    // 创建新密钥
    let key_info = kms.create_key("my-new-key", "AES-256", None).await?;
    println!("Created key: {}", key_info.key_id);
    
    // 查询密钥状态
    let key_status = kms.describe_key("my-new-key", None).await?;
    println!("Key status: {:?}", key_status.status);
} else {
    println!("KMS not initialized");
}
```

### Python 示例

```python
import requests
import json

base_url = "http://localhost:9000/rustfs/admin/v3/kms"

# 配置 KMS
config_data = {
    "kms_type": "vault",
    "vault_address": "https://vault.example.com:8200",
    "vault_token": "your-vault-token"
}
response = requests.post(f"{base_url}/configure", json=config_data)
print(f"Configure KMS: {response.json()}")

# 创建密钥
response = requests.post(f"{base_url}/key/create?keyName=python-test-key")
key_info = response.json()
print(f"Created key: {key_info}")

# 列出所有密钥
response = requests.get(f"{base_url}/key/list")
keys = response.json()
print(f"All keys: {keys}")

# 查询密钥状态
response = requests.get(f"{base_url}/key/status?keyName=python-test-key")
status = response.json()
print(f"Key status: {status}")
```

## 注意事项

1. **认证**: 所有 KMS API 调用都需要适当的管理员权限
2. **HTTPS**: 在生产环境中建议使用 HTTPS 来保护 API 通信
3. **密钥管理**: 禁用的密钥无法用于加密操作，但仍可用于解密已加密的数据
4. **备份**: 建议定期备份 KMS 配置和密钥信息
5. **监控**: 使用 `/kms/status` 端点监控 KMS 服务健康状态