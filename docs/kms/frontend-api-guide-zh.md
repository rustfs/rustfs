# RustFS KMS 前端对接指南

本文档专为前端开发者编写，提供了与 RustFS 密钥管理系统（KMS）交互的完整 API 规范。

## 📋 目录

1. [快速开始](#快速开始)
2. [认证和权限](#认证和权限)
3. [完整接口列表](#完整接口列表)
4. [服务管理API](#服务管理api)
5. [密钥管理API](#密钥管理api)
6. [数据加密API](#数据加密api)
7. [Bucket加密配置API](#bucket加密配置api)
8. [监控和缓存API](#监控和缓存api)
9. [通用错误码](#通用错误码)
10. [数据类型定义](#数据类型定义)
11. [实现示例](#实现示例)

## 🚀 快速开始

### API 基础信息

| 配置项 | 值 |
|--------|-----|
| **基础URL** | `http://localhost:9000/rustfs/admin/v3` (本地开发) |
| **生产URL** | `https://your-rustfs-domain.com/rustfs/admin/v3` |
| **请求格式** | `application/json` |
| **响应格式** | `application/json` |
| **认证方式** | AWS SigV4 签名 |
| **字符编码** | UTF-8 |

### 通用请求头

| 头部字段 | 必需 | 值 |
|----------|------|-----|
| `Content-Type` | ✅ | `application/json` |
| `Authorization` | ✅ | `AWS4-HMAC-SHA256 Credential=...` |
| `X-Amz-Date` | ✅ | ISO8601 格式时间戳 |

## 🔐 认证和权限

### 权限要求

调用 KMS API 需要账户具有以下权限：
- `ServerInfoAdminAction` - 管理员操作权限

### AWS SigV4 签名

所有请求必须使用 AWS Signature Version 4 进行签名认证。

**签名参数**：
- **Access Key ID**: 账户的访问密钥ID
- **Secret Access Key**: 账户的私密访问密钥
- **Region**: `us-east-1` (固定值)
- **Service**: `execute-api`

## 📋 完整接口列表

### 服务管理接口

| 方法 | 接口路径 | 描述 | 状态 |
|------|----------|------|------|
| `POST` | `/kms/configure` | 配置 KMS 服务 | ✅ 可用 |
| `POST` | `/kms/start` | 启动 KMS 服务 | ✅ 可用 |
| `POST` | `/kms/stop` | 停止 KMS 服务 | ✅ 可用 |
| `GET` | `/kms/service-status` | 获取 KMS 服务状态 | ✅ 可用 |
| `POST` | `/kms/reconfigure` | 重新配置 KMS 服务 | ✅ 可用 |

### 密钥管理接口

| 方法 | 接口路径 | 描述 | 状态 |
|------|----------|------|------|
| `POST` | `/kms/keys` | 创建主密钥 | ✅ 可用 |
| `GET` | `/kms/keys` | 列出密钥 | ✅ 可用 |
| `GET` | `/kms/keys/{key_id}` | 获取密钥详情 | ✅ 可用 |
| `DELETE` | `/kms/keys/delete` | 计划删除密钥 | ✅ 可用 |
| `POST` | `/kms/keys/cancel-deletion` | 取消密钥删除 | ✅ 可用 |

### 数据加密接口

| 方法 | 接口路径 | 描述 | 状态 |
|------|----------|------|------|
| `POST` | `/kms/generate-data-key` | 生成数据密钥 | ✅ 可用 |
| `POST` | `/kms/decrypt` | 解密数据密钥 | ⚠️ **未实现** |

### Bucket加密配置接口

| 方法 | 接口路径 | 描述 | 状态 |
|------|----------|------|------|
| `GET` | `/api/v1/buckets` | 列出所有buckets | ✅ 可用 |
| `GET` | `/api/v1/bucket-encryption/{bucket}` | 获取bucket加密配置 | ✅ 可用 |
| `PUT` | `/api/v1/bucket-encryption/{bucket}` | 设置bucket加密配置 | ✅ 可用 |
| `DELETE` | `/api/v1/bucket-encryption/{bucket}` | 删除bucket加密配置 | ✅ 可用 |

### 监控和缓存接口

| 方法 | 接口路径 | 描述 | 状态 |
|------|----------|------|------|
| `GET` | `/kms/config` | 获取 KMS 配置 | ✅ 可用 |
| `POST` | `/kms/clear-cache` | 清除 KMS 缓存 | ✅ 可用 |

### 兼容性接口（旧版本）

| 方法 | 接口路径 | 描述 | 状态 |
|------|----------|------|------|
| `POST` | `/kms/create-key` | 创建密钥（旧版） | ✅ 可用 |
| `GET` | `/kms/describe-key` | 获取密钥详情（旧版） | ✅ 可用 |
| `GET` | `/kms/list-keys` | 列出密钥（旧版） | ✅ 可用 |
| `GET` | `/kms/status` | 获取 KMS 状态（旧版） | ✅ 可用 |

**重要说明**：
- ✅ **可用**：接口已实现且可正常使用
- ⚠️ **未实现**：接口规范已定义但后端未实现，需要联系后端开发团队
- 建议优先使用新版接口，旧版接口主要用于向后兼容

## 🔧 服务管理API

### 1. 配置 KMS 服务

**接口**: `POST /kms/configure`

**请求参数**:

| 参数名 | 类型 | 必需 | 说明 |
|--------|------|------|------|
| `backend_type` | string | ✅ | 后端类型：`"local"` 或 `"vault"` |
| `key_directory` | string | 条件 | Local后端：密钥存储目录路径 |
| `default_key_id` | string | ✅ | 默认主密钥ID |
| `enable_cache` | boolean | ❌ | 是否启用缓存，默认 `true` |
| `cache_ttl_seconds` | integer | ❌ | 缓存TTL秒数，默认 `600` |
| `timeout_seconds` | integer | ❌ | 操作超时秒数，默认 `30` |
| `retry_attempts` | integer | ❌ | 重试次数，默认 `3` |
| `address` | string | 条件 | Vault后端：Vault服务器地址 |
| `auth_method` | object | 条件 | Vault后端：认证方法配置 |
| `mount_path` | string | 条件 | Vault后端：Transit挂载路径 |
| `kv_mount` | string | 条件 | Vault后端：KV存储挂载路径 |
| `key_path_prefix` | string | 条件 | Vault后端：密钥路径前缀 |

**Vault auth_method 对象**:

| 参数名 | 类型 | 必需 | 说明 |
|--------|------|------|------|
| `token` | string | ✅ | Vault访问令牌 |

**响应格式**:

```json
{
  "success": boolean,
  "message": string,
  "config_id": string?
}
```

**响应字段说明**:

| 字段名 | 类型 | 说明 |
|--------|------|------|
| `success` | boolean | 配置是否成功 |
| `message` | string | 配置结果描述信息 |
| `config_id` | string | 配置ID（如果成功） |

**调用示例**:

```javascript
// 配置本地 KMS 后端
const localConfig = {
  backend_type: "local",
  key_directory: "/var/lib/rustfs/kms/keys",
  default_key_id: "default-master-key",
  enable_cache: true,
  cache_ttl_seconds: 600
};

const response = await callKMSAPI('POST', '/kms/configure', localConfig);
// 响应: { "success": true, "message": "KMS configured successfully", "config_id": "config-123" }

// 配置 Vault KMS 后端
const vaultConfig = {
  backend_type: "vault",
  address: "https://vault.example.com:8200",
  auth_method: {
    token: "s.your-vault-token"
  },
  mount_path: "transit",
  kv_mount: "secret",
  key_path_prefix: "rustfs/kms/keys",
  default_key_id: "rustfs-master"
};

const vaultResponse = await callKMSAPI('POST', '/kms/configure', vaultConfig);
```

### 2. 启动 KMS 服务

**接口**: `POST /kms/start`

**请求参数**: 无

**响应格式**:

```json
{
  "success": boolean,
  "message": string,
  "status": string
}
```

**响应字段说明**:

| 字段名 | 类型 | 可能值 | 说明 |
|--------|------|--------|------|
| `success` | boolean | `true`, `false` | 启动是否成功 |
| `message` | string | - | 启动结果描述信息 |
| `status` | string | `"Running"`, `"Stopped"`, `"Error"` | 服务当前状态 |

### 3. 停止 KMS 服务

**接口**: `POST /kms/stop`

**请求参数**: 无

**响应格式**:

```json
{
  "success": boolean,
  "message": string,
  "status": string
}
```

**响应字段说明**: 同启动接口

**调用示例**:

```javascript
// 启动 KMS 服务
const startResponse = await callKMSAPI('POST', '/kms/start');
// 响应: { "success": true, "message": "KMS service started successfully", "status": "Running" }

// 停止 KMS 服务
const stopResponse = await callKMSAPI('POST', '/kms/stop');
// 响应: { "success": true, "message": "KMS service stopped successfully", "status": "Stopped" }
```

### 4. 获取 KMS 服务状态

**接口**: `GET /kms/service-status`

**请求参数**: 无

**响应格式**:

```json
{
  "status": string,
  "backend_type": string,
  "healthy": boolean,
  "config_summary": {
    "backend_type": string,
    "default_key_id": string,
    "timeout_seconds": integer,
    "retry_attempts": integer,
    "enable_cache": boolean
  }
}
```

**响应字段说明**:

| 字段名 | 类型 | 可能值 | 说明 |
|--------|------|--------|------|
| `status` | string | `"Running"`, `"Stopped"`, `"NotConfigured"`, `"Error"` | 服务状态 |
| `backend_type` | string | `"local"`, `"vault"` | 后端类型 |
| `healthy` | boolean | `true`, `false` | 服务健康状态 |
| `config_summary` | object | - | 配置摘要信息 |

**调用示例**:

```javascript
// 获取 KMS 服务状态
const status = await callKMSAPI('GET', '/kms/service-status');
console.log('KMS状态:', status);

/* 响应示例:
{
  "status": "Running",
  "backend_type": "vault",
  "healthy": true,
  "config_summary": {
    "backend_type": "vault",
    "default_key_id": "rustfs-master",
    "timeout_seconds": 30,
    "retry_attempts": 3,
    "enable_cache": true
  }
}
*/
```

### 5. 重新配置 KMS 服务

**接口**: `POST /kms/reconfigure`

**请求参数**: 同配置接口的参数

**响应格式**:

```json
{
  "success": boolean,
  "message": string,
  "status": string
}
```

**调用示例**:

```javascript
// 重新配置 KMS 服务（会停止当前服务并重新启动）
const newConfig = {
  backend_type: "vault",
  address: "https://new-vault.example.com:8200",
  auth_method: {
    token: "s.new-vault-token"
  },
  mount_path: "transit",
  kv_mount: "secret",
  key_path_prefix: "rustfs/kms/keys",
  default_key_id: "new-master-key"
};

const reconfigureResponse = await callKMSAPI('POST', '/kms/reconfigure', newConfig);
// 响应: { "success": true, "message": "KMS reconfigured and restarted successfully", "status": "Running" }
```

## 🔑 密钥管理API

### 1. 创建主密钥

**接口**: `POST /kms/keys`

**请求参数**:

| 参数名 | 类型 | 必需 | 说明 |
|--------|------|------|------|
| `KeyUsage` | string | ✅ | 密钥用途，固定值：`"ENCRYPT_DECRYPT"` |
| `Description` | string | ❌ | 密钥描述，最长256字符 |
| `Tags` | object | ❌ | 密钥标签，键值对格式 |

**Tags 对象**: 任意键值对，值必须为字符串类型

**响应格式**:

```json
{
  "key_id": string,
  "key_metadata": {
    "key_id": string,
    "description": string,
    "enabled": boolean,
    "key_usage": string,
    "creation_date": string,
    "rotation_enabled": boolean,
    "deletion_date": string?
  }
}
```

**响应字段说明**:

| 字段名 | 类型 | 说明 |
|--------|------|------|
| `key_id` | string | 生成的密钥唯一标识符（UUID格式） |
| `key_metadata.key_id` | string | 密钥ID（与外层相同） |
| `key_metadata.description` | string | 密钥描述 |
| `key_metadata.enabled` | boolean | 密钥是否启用 |
| `key_metadata.key_usage` | string | 密钥用途 |
| `key_metadata.creation_date` | string | 创建时间（ISO8601格式） |
| `key_metadata.rotation_enabled` | boolean | 是否启用轮换 |
| `key_metadata.deletion_date` | string | 删除时间（如果已计划删除） |

**调用示例**:

```javascript
// 创建主密钥
const keyRequest = {
  KeyUsage: "ENCRYPT_DECRYPT",
  Description: "前端应用主密钥",
  Tags: {
    owner: "frontend-team",
    environment: "production",
    project: "user-data-encryption"
  }
};

const newKey = await callKMSAPI('POST', '/kms/keys', keyRequest);
console.log('创建的密钥ID:', newKey.key_id);

/* 响应示例:
{
  "key_id": "fa5bac0e-2a2c-4f9a-a09d-2f5b8a59ed85",
  "key_metadata": {
    "key_id": "fa5bac0e-2a2c-4f9a-a09d-2f5b8a59ed85",
    "description": "前端应用主密钥",
    "enabled": true,
    "key_usage": "ENCRYPT_DECRYPT",
    "creation_date": "2024-09-19T07:10:42.012345Z",
    "rotation_enabled": false
  }
}
*/
```

### 2. 获取密钥详情

**接口**: `GET /kms/keys/{key_id}`

**路径参数**:

| 参数名 | 类型 | 必需 | 说明 |
|--------|------|------|------|
| `key_id` | string | ✅ | 密钥ID（UUID格式） |

**响应格式**:

```json
{
  "key_metadata": {
    "key_id": string,
    "description": string,
    "enabled": boolean,
    "key_usage": string,
    "creation_date": string,
    "rotation_enabled": boolean,
    "deletion_date": string?
  }
}
```

**响应字段说明**: 同创建接口的 key_metadata 字段

**调用示例**:

```javascript
// 获取密钥详情
const keyId = "fa5bac0e-2a2c-4f9a-a09d-2f5b8a59ed85";
const keyDetails = await callKMSAPI('GET', `/kms/keys/${keyId}`);
console.log('密钥详情:', keyDetails.key_metadata);

/* 响应示例:
{
  "key_metadata": {
    "key_id": "fa5bac0e-2a2c-4f9a-a09d-2f5b8a59ed85",
    "description": "前端应用主密钥",
    "enabled": true,
    "key_usage": "ENCRYPT_DECRYPT",
    "creation_date": "2024-09-19T07:10:42.012345Z",
    "rotation_enabled": false,
    "deletion_date": null
  }
}
*/
```

### 3. 列出密钥

**接口**: `GET /kms/keys`

**查询参数**:

| 参数名 | 类型 | 必需 | 默认值 | 说明 |
|--------|------|------|--------|------|
| `limit` | integer | ❌ | `50` | 每页返回的密钥数量，最大1000 |
| `marker` | string | ❌ | - | 分页标记，用于获取下一页 |

**响应格式**:

```json
{
  "keys": [
    {
      "key_id": string,
      "description": string
    }
  ],
  "truncated": boolean,
  "next_marker": string?
}
```

**响应字段说明**:

| 字段名 | 类型 | 说明 |
|--------|------|------|
| `keys` | array | 密钥列表 |
| `keys[].key_id` | string | 密钥ID |
| `keys[].description` | string | 密钥描述 |
| `truncated` | boolean | 是否还有更多数据 |
| `next_marker` | string | 下一页的分页标记 |

**调用示例**:

```javascript
// 列出所有密钥（分页）
let allKeys = [];
let marker = null;

do {
  const params = new URLSearchParams({ limit: '50' });
  if (marker) params.append('marker', marker);

  const keysList = await callKMSAPI('GET', `/kms/keys?${params}`);
  allKeys.push(...keysList.keys);
  marker = keysList.next_marker;
} while (marker);

console.log('所有密钥:', allKeys);

/* 响应示例:
{
  "keys": [
    { "key_id": "fa5bac0e-2a2c-4f9a-a09d-2f5b8a59ed85", "description": "前端应用主密钥" },
    { "key_id": "bb2cd4f1-3e4d-4a5b-b6c7-8d9e0f1a2b3c", "description": "用户数据密钥" }
  ],
  "truncated": false,
  "next_marker": null
}
*/
```

### 4. 计划删除密钥

**接口**: `DELETE /kms/keys/delete`

**请求参数**:

| 参数名 | 类型 | 必需 | 说明 |
|--------|------|------|------|
| `key_id` | string | ✅ | 要删除的密钥ID |
| `pending_window_in_days` | integer | ❌ | 待删除天数，范围 7-30，默认 7 |

**响应格式**:

```json
{
  "key_id": string,
  "deletion_date": string,
  "pending_window_in_days": integer
}
```

**响应字段说明**:

| 字段名 | 类型 | 说明 |
|--------|------|------|
| `key_id` | string | 密钥ID |
| `deletion_date` | string | 计划删除时间（ISO8601格式） |
| `pending_window_in_days` | integer | 待删除天数 |

**调用示例**:

```javascript
// 计划删除密钥（7天后删除）
const deleteRequest = {
  key_id: "fa5bac0e-2a2c-4f9a-a09d-2f5b8a59ed85",
  pending_window_in_days: 7
};

const deleteResponse = await callKMSAPI('DELETE', '/kms/keys/delete', deleteRequest);
console.log('密钥已计划删除:', deleteResponse);

/* 响应示例:
{
  "key_id": "fa5bac0e-2a2c-4f9a-a09d-2f5b8a59ed85",
  "deletion_date": "2024-09-26T07:10:42.012345Z",
  "pending_window_in_days": 7
}
*/
```

### 5. 取消密钥删除

**接口**: `POST /kms/keys/cancel-deletion`

**请求参数**:

| 参数名 | 类型 | 必需 | 说明 |
|--------|------|------|------|
| `key_id` | string | ✅ | 要取消删除的密钥ID |

**响应格式**:

```json
{
  "key_id": string,
  "key_metadata": {
    "key_id": string,
    "description": string,
    "enabled": boolean,
    "key_usage": string,
    "creation_date": string,
    "rotation_enabled": boolean,
    "deletion_date": null
  }
}
```

**响应字段说明**: 同创建接口，注意 `deletion_date` 将为 `null`

**调用示例**:

```javascript
// 取消密钥删除
const cancelRequest = {
  key_id: "fa5bac0e-2a2c-4f9a-a09d-2f5b8a59ed85"
};

const cancelResponse = await callKMSAPI('POST', '/kms/keys/cancel-deletion', cancelRequest);
console.log('密钥删除已取消:', cancelResponse);

/* 响应示例:
{
  "key_id": "fa5bac0e-2a2c-4f9a-a09d-2f5b8a59ed85",
  "key_metadata": {
    "key_id": "fa5bac0e-2a2c-4f9a-a09d-2f5b8a59ed85",
    "description": "前端应用主密钥",
    "enabled": true,
    "key_usage": "ENCRYPT_DECRYPT",
    "creation_date": "2024-09-19T07:10:42.012345Z",
    "rotation_enabled": false,
    "deletion_date": null
  }
}
*/
```

## 🔒 数据加密API

### 1. 生成数据密钥

**接口**: `POST /kms/generate-data-key`

**请求参数**:

| 参数名 | 类型 | 必需 | 说明 |
|--------|------|------|------|
| `key_id` | string | ✅ | 主密钥ID（UUID格式） |
| `key_spec` | string | ❌ | 数据密钥规格，默认 `"AES_256"` |
| `encryption_context` | object | ❌ | 加密上下文，键值对格式 |

**key_spec 可能值**:
- `"AES_256"` - 256位AES密钥
- `"AES_128"` - 128位AES密钥

**encryption_context 对象**: 任意键值对，用于加密上下文，键和值都必须是字符串

**响应格式**:

```json
{
  "key_id": string,
  "plaintext_key": string,
  "ciphertext_blob": string
}
```

**响应字段说明**:

| 字段名 | 类型 | 说明 |
|--------|------|------|
| `key_id` | string | 主密钥ID |
| `plaintext_key` | string | 原始数据密钥（Base64编码） |
| `ciphertext_blob` | string | 加密后的数据密钥（Base64编码） |

**调用示例**:

```javascript
// 生成数据密钥用于文件加密
const dataKeyRequest = {
  key_id: "fa5bac0e-2a2c-4f9a-a09d-2f5b8a59ed85",
  key_spec: "AES_256",
  encryption_context: {
    bucket: "user-uploads",
    object_key: "documents/report.pdf",
    user_id: "user123",
    department: "finance"
  }
};

const dataKey = await callKMSAPI('POST', '/kms/generate-data-key', dataKeyRequest);
console.log('生成的数据密钥:', dataKey);

// 立即使用原始密钥进行数据加密
const encryptedData = await encryptFileWithKey(fileData, dataKey.plaintext_key);

// 安全地清理内存中的原始密钥
dataKey.plaintext_key = null;

// 保存加密后的密钥用于后续解密
localStorage.setItem('encrypted_key', dataKey.ciphertext_blob);

/* 响应示例:
{
  "key_id": "fa5bac0e-2a2c-4f9a-a09d-2f5b8a59ed85",
  "plaintext_key": "sQW6qt0yS7CqD6c8hY7GZg==",
  "ciphertext_blob": "gAAAAABlLK4xQ8..."
}
*/
```

### 2. 解密数据密钥

⚠️ **注意：此接口当前未实现**

根据代码分析，虽然底层 KMS 服务具有解密功能，但尚未暴露对应的 HTTP API 接口。这是一个重要的功能缺失。

**预期接口**: `POST /kms/decrypt`

**预期请求参数**:

| 参数名 | 类型 | 必需 | 说明 |
|--------|------|------|------|
| `ciphertext_blob` | string | ✅ | 加密的数据密钥（Base64编码） |
| `encryption_context` | object | ❌ | 解密上下文（必须与加密时相同） |

**预期响应格式**:

```json
{
  "key_id": string,
  "plaintext": string
}
```

**预期响应字段说明**:

| 字段名 | 类型 | 说明 |
|--------|------|------|
| `key_id` | string | 用于加密的主密钥ID |
| `plaintext` | string | 解密后的原始数据密钥（Base64编码） |

**临时解决方案**:

目前前端需要通过其他方式处理数据密钥解密：

```javascript
// 临时解决方案：建议联系后端开发团队添加此接口
console.error('解密数据密钥接口暂未实现，请联系后端开发团队');

// 或者考虑使用以下替代方案：
// 1. 在服务端完成数据加密/解密，前端只处理已解密的数据
// 2. 等待后端团队实现 /kms/decrypt 接口

/* 未来的调用示例:
const encryptedKey = localStorage.getItem('encrypted_key');

const decryptRequest = {
  ciphertext_blob: encryptedKey,
  encryption_context: {
    bucket: "user-uploads",
    object_key: "documents/report.pdf",
    user_id: "user123",
    department: "finance"
  }
};

const decryptedKey = await callKMSAPI('POST', '/kms/decrypt', decryptRequest);
console.log('解密成功，主密钥ID:', decryptedKey.key_id);

// 使用解密的密钥解密文件数据
const decryptedData = await decryptFileWithKey(encryptedFileData, decryptedKey.plaintext);

// 立即清理内存中的原始密钥
decryptedKey.plaintext = null;
*/
```

**建议**:

1. **联系后端团队**：建议尽快实现 `POST /kms/decrypt` 接口
2. **API 设计参考**：可参考 AWS KMS 的 Decrypt API 设计
3. **安全考虑**：确保接口包含适当的认证和授权检查

## 🪣 Bucket加密配置API

### 概述

Bucket加密配置API提供了对存储桶级别默认加密设置的管理功能。这些API基于AWS S3兼容的bucket加密接口，支持SSE-S3和SSE-KMS两种加密方式。

**重要说明**：这些接口使用AWS S3 SDK的标准接口，不是RustFS的自定义KMS接口。

### 1. 列出所有buckets

**接口**: AWS S3 `ListBuckets` 操作

**AWS SDK调用方式**:
```javascript
import { ListBucketsCommand } from '@aws-sdk/client-s3';

const listBuckets = async (s3Client) => {
  const command = new ListBucketsCommand({});
  return await s3Client.send(command);
};
```

**响应格式**:
```json
{
  "Buckets": [
    {
      "Name": "my-bucket",
      "CreationDate": "2024-09-19T10:30:00.000Z"
    }
  ],
  "Owner": {
    "DisplayName": "owner-name",
    "ID": "owner-id"
  }
}
```

**响应字段说明**:

| 字段名 | 类型 | 说明 |
|--------|------|------|
| `Buckets` | array | Bucket列表 |
| `Buckets[].Name` | string | Bucket名称 |
| `Buckets[].CreationDate` | string | 创建时间（ISO8601格式） |
| `Owner` | object | 所有者信息 |

### 2. 获取bucket加密配置

**接口**: AWS S3 `GetBucketEncryption` 操作

**AWS SDK调用方式**:
```javascript
import { GetBucketEncryptionCommand } from '@aws-sdk/client-s3';

const getBucketEncryption = async (s3Client, bucketName) => {
  const command = new GetBucketEncryptionCommand({
    Bucket: bucketName
  });
  return await s3Client.send(command);
};
```

**响应格式**:
```json
{
  "ServerSideEncryptionConfiguration": {
    "Rules": [
      {
        "ApplyServerSideEncryptionByDefault": {
          "SSEAlgorithm": "aws:kms",
          "KMSMasterKeyID": "key-id-here"
        }
      }
    ]
  }
}
```

**响应字段说明**:

| 字段名 | 类型 | 可能值 | 说明 |
|--------|------|--------|------|
| `ServerSideEncryptionConfiguration` | object | - | 服务端加密配置 |
| `Rules` | array | - | 加密规则列表 |
| `Rules[].ApplyServerSideEncryptionByDefault` | object | - | 默认加密设置 |
| `SSEAlgorithm` | string | `"aws:kms"`, `"AES256"` | 加密算法 |
| `KMSMasterKeyID` | string | - | KMS主密钥ID（仅SSE-KMS时存在） |

**错误处理**:
- **404错误**: 表示bucket未配置加密，应视为"未配置"状态
- **403错误**: 权限不足，无法访问bucket加密配置

### 3. 设置bucket加密配置

**接口**: AWS S3 `PutBucketEncryption` 操作

**AWS SDK调用方式**:

#### SSE-S3加密:
```javascript
import { PutBucketEncryptionCommand } from '@aws-sdk/client-s3';

const putBucketEncryptionSSE_S3 = async (s3Client, bucketName) => {
  const command = new PutBucketEncryptionCommand({
    Bucket: bucketName,
    ServerSideEncryptionConfiguration: {
      Rules: [
        {
          ApplyServerSideEncryptionByDefault: {
            SSEAlgorithm: 'AES256'
          }
        }
      ]
    }
  });
  return await s3Client.send(command);
};
```

#### SSE-KMS加密:
```javascript
const putBucketEncryptionSSE_KMS = async (s3Client, bucketName, kmsKeyId) => {
  const command = new PutBucketEncryptionCommand({
    Bucket: bucketName,
    ServerSideEncryptionConfiguration: {
      Rules: [
        {
          ApplyServerSideEncryptionByDefault: {
            SSEAlgorithm: 'aws:kms',
            KMSMasterKeyID: kmsKeyId
          }
        }
      ]
    }
  });
  return await s3Client.send(command);
};
```

**请求参数**:

| 参数名 | 类型 | 必需 | 说明 |
|--------|------|------|------|
| `Bucket` | string | ✅ | Bucket名称 |
| `ServerSideEncryptionConfiguration` | object | ✅ | 加密配置对象 |
| `Rules` | array | ✅ | 加密规则数组 |
| `SSEAlgorithm` | string | ✅ | `"AES256"` 或 `"aws:kms"` |
| `KMSMasterKeyID` | string | 条件 | KMS密钥ID（SSE-KMS时必需） |

**响应**: 成功时返回HTTP 200，无响应体

### 4. 删除bucket加密配置

**接口**: AWS S3 `DeleteBucketEncryption` 操作

**AWS SDK调用方式**:
```javascript
import { DeleteBucketEncryptionCommand } from '@aws-sdk/client-s3';

const deleteBucketEncryption = async (s3Client, bucketName) => {
  const command = new DeleteBucketEncryptionCommand({
    Bucket: bucketName
  });
  return await s3Client.send(command);
};
```

**请求参数**:

| 参数名 | 类型 | 必需 | 说明 |
|--------|------|------|------|
| `Bucket` | string | ✅ | Bucket名称 |

**响应**: 成功时返回HTTP 204，无响应体

### 前端集成示例

#### Vue.js Composable示例
```javascript
import { ref } from 'vue';

export function useBucketEncryption() {
  const { listBuckets, getBucketEncryption, putBucketEncryption, deleteBucketEncryption } = useBucket({});

  const buckets = ref([]);
  const loading = ref(false);
  const error = ref(null);

  // 加载bucket列表和加密状态
  const loadBucketList = async () => {
    loading.value = true;
    error.value = null;

    try {
      const response = await listBuckets();
      if (response?.Buckets) {
        // 并行获取加密配置
        const bucketList = await Promise.all(
          response.Buckets.map(async (bucket) => {
            try {
              const encryptionConfig = await getBucketEncryption(bucket.Name);

              let encryptionStatus = 'Disabled';
              let encryptionType = '';
              let kmsKeyId = '';

              if (encryptionConfig?.ServerSideEncryptionConfiguration?.Rules?.length > 0) {
                const rule = encryptionConfig.ServerSideEncryptionConfiguration.Rules[0];
                if (rule.ApplyServerSideEncryptionByDefault) {
                  encryptionStatus = 'Enabled';
                  const algorithm = rule.ApplyServerSideEncryptionByDefault.SSEAlgorithm;

                  if (algorithm === 'aws:kms') {
                    encryptionType = 'SSE-KMS';
                    kmsKeyId = rule.ApplyServerSideEncryptionByDefault.KMSMasterKeyID || '';
                  } else if (algorithm === 'AES256') {
                    encryptionType = 'SSE-S3';
                  }
                }
              }

              return {
                name: bucket.Name,
                creationDate: bucket.CreationDate,
                encryptionStatus,
                encryptionType,
                kmsKeyId
              };
            } catch (encryptionError) {
              // 404表示未配置加密
              return {
                name: bucket.Name,
                creationDate: bucket.CreationDate,
                encryptionStatus: 'Disabled',
                encryptionType: '',
                kmsKeyId: ''
              };
            }
          })
        );

        buckets.value = bucketList;
      }
    } catch (err) {
      error.value = err.message;
      throw err;
    } finally {
      loading.value = false;
    }
  };

  // 配置bucket加密
  const configureBucketEncryption = async (bucketName, encryptionType, kmsKeyId = '') => {
    const encryptionConfig = {
      Rules: [
        {
          ApplyServerSideEncryptionByDefault: {
            SSEAlgorithm: encryptionType === 'SSE-KMS' ? 'aws:kms' : 'AES256',
            ...(encryptionType === 'SSE-KMS' && kmsKeyId && { KMSMasterKeyID: kmsKeyId })
          }
        }
      ]
    };

    await putBucketEncryption(bucketName, encryptionConfig);
    await loadBucketList(); // 刷新列表
  };

  // 移除bucket加密
  const removeBucketEncryption = async (bucketName) => {
    await deleteBucketEncryption(bucketName);
    await loadBucketList(); // 刷新列表
  };

  return {
    buckets,
    loading,
    error,
    loadBucketList,
    configureBucketEncryption,
    removeBucketEncryption
  };
}
```

### 与KMS密钥管理的集成

结合KMS密钥管理API，可以实现完整的加密密钥生命周期管理：

```javascript
// 完整的加密管理示例
export function useEncryptionManagement() {
  const { loadBucketList, configureBucketEncryption } = useBucketEncryption();
  const { getKeyList, createKey } = useSSE();

  // 为bucket设置新的加密配置
  const setupBucketEncryption = async (bucketName, encryptionType, keyName) => {
    let kmsKeyId = null;

    if (encryptionType === 'SSE-KMS') {
      // 1. 获取现有KMS密钥列表
      const keysList = await getKeyList();
      let targetKey = keysList.keys.find(key =>
        key.tags?.name === keyName || key.description === keyName
      );

      // 2. 如果密钥不存在，创建新密钥
      if (!targetKey) {
        const newKeyResponse = await createKey({
          KeyUsage: 'ENCRYPT_DECRYPT',
          Description: `Bucket encryption key for ${bucketName}`,
          Tags: {
            name: keyName,
            bucket: bucketName,
            purpose: 'bucket-encryption'
          }
        });
        kmsKeyId = newKeyResponse.key_id;
      } else {
        kmsKeyId = targetKey.key_id;
      }
    }

    // 3. 配置bucket加密
    await configureBucketEncryption(bucketName, encryptionType, kmsKeyId);

    return { success: true, kmsKeyId };
  };

  return { setupBucketEncryption };
}
```

### 安全最佳实践

1. **权限控制**: 确保只有授权用户可以修改bucket加密配置
2. **加密算法选择**:
   - **SSE-S3**: 由S3服务管理密钥，适合一般用途
   - **SSE-KMS**: 使用KMS管理密钥，提供更细粒度的访问控制
3. **密钥管理**: 使用SSE-KMS时，确保KMS密钥具有适当的访问策略
4. **审计日志**: 记录所有加密配置变更操作

### 错误处理指南

| 错误类型 | HTTP状态 | 处理建议 |
|----------|----------|----------|
| `NoSuchBucket` | 404 | Bucket不存在，检查bucket名称 |
| `NoSuchBucketPolicy` | 404 | 未配置加密，视为正常状态 |
| `AccessDenied` | 403 | 权限不足，检查IAM策略 |
| `InvalidRequest` | 400 | 请求参数错误，检查加密配置格式 |
| `KMSKeyNotFound` | 400 | KMS密钥不存在，验证密钥ID |

## 📊 监控和缓存API

### 1. 获取 KMS 配置

**接口**: `GET /kms/config`

**请求参数**: 无

**响应格式**:

```json
{
  "backend": string,
  "cache_enabled": boolean,
  "cache_max_keys": integer,
  "cache_ttl_seconds": integer,
  "default_key_id": string?
}
```

**响应字段说明**:

| 字段名 | 类型 | 说明 |
|--------|------|------|
| `backend` | string | 后端类型 |
| `cache_enabled` | boolean | 是否启用缓存 |
| `cache_max_keys` | integer | 缓存最大密钥数量 |
| `cache_ttl_seconds` | integer | 缓存TTL（秒） |
| `default_key_id` | string | 默认密钥ID |

**调用示例**:

```javascript
// 获取 KMS 配置
const config = await callKMSAPI('GET', '/kms/config');
console.log('KMS配置:', config);

/* 响应示例:
{
  "backend": "vault",
  "cache_enabled": true,
  "cache_max_keys": 1000,
  "cache_ttl_seconds": 300,
  "default_key_id": "rustfs-master"
}
*/
```

### 2. 清除 KMS 缓存

**接口**: `POST /kms/clear-cache`

**请求参数**: 无

**响应格式**:

```json
{
  "status": string,
  "message": string
}
```

**调用示例**:

```javascript
// 清除 KMS 缓存
const clearResult = await callKMSAPI('POST', '/kms/clear-cache');
console.log('缓存清除结果:', clearResult);

/* 响应示例:
{
  "status": "success",
  "message": "cache cleared successfully"
}
*/
```

### 3. 获取缓存统计信息

**接口**: `GET /kms/status` (旧版接口，包含缓存统计)

**请求参数**: 无

**响应格式**:

```json
{
  "backend_type": string,
  "backend_status": string,
  "cache_enabled": boolean,
  "cache_stats": {
    "hit_count": integer,
    "miss_count": integer
  }?,
  "default_key_id": string?
}
```

**调用示例**:

```javascript
// 获取详细的 KMS 状态（包含缓存统计）
const detailedStatus = await callKMSAPI('GET', '/kms/status');
console.log('详细状态:', detailedStatus);

/* 响应示例:
{
  "backend_type": "vault",
  "backend_status": "healthy",
  "cache_enabled": true,
  "cache_stats": {
    "hit_count": 1250,
    "miss_count": 48
  },
  "default_key_id": "rustfs-master"
}
*/
```

## ❌ 通用错误码

### HTTP 状态码

| 状态码 | 错误类型 | 说明 |
|--------|----------|------|
| `200` | - | 请求成功 |
| `400` | `InvalidRequest` | 请求格式错误或参数无效 |
| `401` | `AccessDenied` | 认证失败 |
| `403` | `AccessDenied` | 权限不足 |
| `404` | `NotFound` | 资源不存在 |
| `409` | `Conflict` | 资源状态冲突 |
| `500` | `InternalError` | 服务器内部错误 |

### 错误响应格式

```json
{
  "error": {
    "code": string,
    "message": string,
    "request_id": string?
  }
}
```

### 具体错误码

| 错误码 | HTTP状态 | 说明 | 处理建议 |
|--------|----------|------|----------|
| `InvalidRequest` | 400 | 请求参数错误 | 检查请求格式和参数 |
| `AccessDenied` | 401/403 | 认证或授权失败 | 检查访问凭证和权限 |
| `KeyNotFound` | 404 | 密钥不存在 | 验证密钥ID是否正确 |
| `InvalidKeyState` | 400 | 密钥状态无效 | 检查密钥是否已启用 |
| `ServiceNotConfigured` | 409 | KMS服务未配置 | 先配置KMS服务 |
| `ServiceNotRunning` | 409 | KMS服务未运行 | 启动KMS服务 |
| `BackendError` | 500 | 后端存储错误 | 检查后端服务状态 |
| `EncryptionFailed` | 500 | 加密操作失败 | 重试操作或检查密钥状态 |
| `DecryptionFailed` | 500 | 解密操作失败 | 检查密文和加密上下文 |

## 📊 数据类型定义

### KeyMetadata 对象

| 字段名 | 类型 | 必需 | 说明 |
|--------|------|------|------|
| `key_id` | string | ✅ | 密钥唯一标识符（UUID格式） |
| `description` | string | ✅ | 密钥描述 |
| `enabled` | boolean | ✅ | 密钥是否启用 |
| `key_usage` | string | ✅ | 密钥用途，值为 `"ENCRYPT_DECRYPT"` |
| `creation_date` | string | ✅ | 创建时间（ISO8601格式） |
| `rotation_enabled` | boolean | ✅ | 是否启用自动轮换 |
| `deletion_date` | string | ❌ | 计划删除时间（如果已计划删除） |

### ConfigSummary 对象

| 字段名 | 类型 | 必需 | 说明 |
|--------|------|------|------|
| `backend_type` | string | ✅ | 后端类型 |
| `default_key_id` | string | ✅ | 默认主密钥ID |
| `timeout_seconds` | integer | ✅ | 操作超时时间 |
| `retry_attempts` | integer | ✅ | 重试次数 |
| `enable_cache` | boolean | ✅ | 是否启用缓存 |

### 枚举值定义

**ServiceStatus（服务状态）**:
- `"Running"` - 运行中
- `"Stopped"` - 已停止
- `"NotConfigured"` - 未配置
- `"Error"` - 错误状态

**BackendType（后端类型）**:
- `"local"` - 本地文件系统后端
- `"vault"` - Vault后端

**KeyUsage（密钥用途）**:
- `"ENCRYPT_DECRYPT"` - 加密解密

**KeySpec（数据密钥规格）**:
- `"AES_256"` - 256位AES密钥
- `"AES_128"` - 128位AES密钥

## 💡 实现示例

### Bucket加密管理完整示例

以下是一个完整的bucket加密管理实现，展示了如何在前端应用中集成KMS密钥管理和bucket加密配置：

```javascript
// BucketEncryptionManager.js - 完整的bucket加密管理类
import {
  ListBucketsCommand,
  GetBucketEncryptionCommand,
  PutBucketEncryptionCommand,
  DeleteBucketEncryptionCommand
} from '@aws-sdk/client-s3';

class BucketEncryptionManager {
  constructor(s3Client, kmsAPI) {
    this.s3Client = s3Client;
    this.kmsAPI = kmsAPI;
    this.buckets = [];
    this.kmsKeys = [];
  }

  // 初始化 - 加载buckets和KMS密钥
  async initialize() {
    try {
      await Promise.all([
        this.loadBuckets(),
        this.loadKMSKeys()
      ]);
      console.log('Bucket加密管理器初始化完成');
      return { success: true };
    } catch (error) {
      console.error('初始化失败:', error);
      throw error;
    }
  }

  // 加载所有buckets及其加密状态
  async loadBuckets() {
    try {
      const listResult = await this.s3Client.send(new ListBucketsCommand({}));

      // 并行获取每个bucket的加密配置
      this.buckets = await Promise.all(
        listResult.Buckets.map(async (bucket) => {
          const encryptionInfo = await this.getBucketEncryptionInfo(bucket.Name);
          return {
            name: bucket.Name,
            creationDate: bucket.CreationDate,
            ...encryptionInfo
          };
        })
      );

      console.log(`已加载 ${this.buckets.length} 个buckets`);
      return this.buckets;
    } catch (error) {
      console.error('加载buckets失败:', error);
      throw error;
    }
  }

  // 获取单个bucket的加密信息
  async getBucketEncryptionInfo(bucketName) {
    try {
      const encryptionResult = await this.s3Client.send(
        new GetBucketEncryptionCommand({ Bucket: bucketName })
      );

      const rule = encryptionResult.ServerSideEncryptionConfiguration?.Rules?.[0];
      const defaultEncryption = rule?.ApplyServerSideEncryptionByDefault;

      if (!defaultEncryption) {
        return {
          encryptionStatus: 'Disabled',
          encryptionType: null,
          encryptionAlgorithm: null,
          kmsKeyId: null,
          kmsKeyName: null
        };
      }

      const isKMS = defaultEncryption.SSEAlgorithm === 'aws:kms';
      const kmsKeyId = defaultEncryption.KMSMasterKeyID;
      const kmsKeyName = isKMS ? this.getKMSKeyName(kmsKeyId) : null;

      return {
        encryptionStatus: 'Enabled',
        encryptionType: isKMS ? 'SSE-KMS' : 'SSE-S3',
        encryptionAlgorithm: isKMS ? 'AES-256 (KMS)' : 'AES-256 (S3)',
        kmsKeyId: kmsKeyId || null,
        kmsKeyName: kmsKeyName || null
      };
    } catch (error) {
      // 404或NoSuchBucketPolicy表示未配置加密
      if (error.name === 'NoSuchBucketPolicy' || error.$metadata?.httpStatusCode === 404) {
        return {
          encryptionStatus: 'Disabled',
          encryptionType: null,
          encryptionAlgorithm: null,
          kmsKeyId: null,
          kmsKeyName: null
        };
      }
      throw error;
    }
  }

  // 加载KMS密钥列表
  async loadKMSKeys() {
    try {
      const keysList = await this.kmsAPI.getKeyList();
      this.kmsKeys = keysList.keys || [];
      console.log(`已加载 ${this.kmsKeys.length} 个KMS密钥`);
      return this.kmsKeys;
    } catch (error) {
      console.error('加载KMS密钥失败:', error);
      // KMS密钥加载失败不应该阻止bucket加载
      this.kmsKeys = [];
    }
  }

  // 根据KMS密钥ID获取密钥名称
  getKMSKeyName(keyId) {
    if (!keyId || !this.kmsKeys.length) return null;

    const key = this.kmsKeys.find(k => k.key_id === keyId);
    return key?.tags?.name || key?.description || keyId.substring(0, 8) + '...';
  }

  // 配置bucket加密
  async configureBucketEncryption(bucketName, encryptionType, kmsKeyId = null) {
    try {
      const encryptionConfig = {
        Bucket: bucketName,
        ServerSideEncryptionConfiguration: {
          Rules: [
            {
              ApplyServerSideEncryptionByDefault: {
                SSEAlgorithm: encryptionType === 'SSE-KMS' ? 'aws:kms' : 'AES256',
                ...(encryptionType === 'SSE-KMS' && kmsKeyId && { KMSMasterKeyID: kmsKeyId })
              }
            }
          ]
        }
      };

      await this.s3Client.send(new PutBucketEncryptionCommand(encryptionConfig));

      // 更新本地缓存
      await this.refreshBucketInfo(bucketName);

      console.log(`Bucket ${bucketName} 加密配置成功: ${encryptionType}`);
      return { success: true };
    } catch (error) {
      console.error(`配置bucket加密失败 (${bucketName}):`, error);
      throw error;
    }
  }

  // 移除bucket加密配置
  async removeBucketEncryption(bucketName) {
    try {
      await this.s3Client.send(new DeleteBucketEncryptionCommand({ Bucket: bucketName }));

      // 更新本地缓存
      await this.refreshBucketInfo(bucketName);

      console.log(`Bucket ${bucketName} 加密配置已移除`);
      return { success: true };
    } catch (error) {
      console.error(`移除bucket加密失败 (${bucketName}):`, error);
      throw error;
    }
  }

  // 为bucket创建专用KMS密钥并配置加密
  async setupDedicatedEncryption(bucketName, keyName, keyDescription) {
    try {
      // 1. 创建专用KMS密钥
      const newKey = await this.kmsAPI.createKey({
        KeyUsage: 'ENCRYPT_DECRYPT',
        Description: keyDescription || `Dedicated encryption key for bucket: ${bucketName}`,
        Tags: {
          name: keyName,
          bucket: bucketName,
          purpose: 'bucket-encryption',
          created_by: 'bucket-manager',
          created_at: new Date().toISOString()
        }
      });

      // 2. 配置bucket使用新密钥
      await this.configureBucketEncryption(bucketName, 'SSE-KMS', newKey.key_id);

      // 3. 更新KMS密钥缓存
      await this.loadKMSKeys();

      console.log(`为bucket ${bucketName} 创建并配置专用密钥: ${newKey.key_id}`);
      return {
        success: true,
        keyId: newKey.key_id,
        keyName: keyName
      };
    } catch (error) {
      console.error(`设置专用加密失败 (${bucketName}):`, error);
      throw error;
    }
  }

  // 批量配置多个bucket的加密
  async batchConfigureEncryption(configurations) {
    const results = [];

    for (const config of configurations) {
      try {
        await this.configureBucketEncryption(
          config.bucketName,
          config.encryptionType,
          config.kmsKeyId
        );
        results.push({ bucketName: config.bucketName, success: true });
      } catch (error) {
        results.push({
          bucketName: config.bucketName,
          success: false,
          error: error.message
        });
      }
    }

    const successCount = results.filter(r => r.success).length;
    console.log(`批量配置完成: ${successCount}/${configurations.length} 成功`);

    return results;
  }

  // 刷新单个bucket信息
  async refreshBucketInfo(bucketName) {
    try {
      const bucketIndex = this.buckets.findIndex(b => b.name === bucketName);
      if (bucketIndex !== -1) {
        const encryptionInfo = await this.getBucketEncryptionInfo(bucketName);
        this.buckets[bucketIndex] = {
          ...this.buckets[bucketIndex],
          ...encryptionInfo
        };
      }
    } catch (error) {
      console.error(`刷新bucket信息失败 (${bucketName}):`, error);
    }
  }

  // 获取加密统计信息
  getEncryptionStats() {
    const total = this.buckets.length;
    const encrypted = this.buckets.filter(b => b.encryptionStatus === 'Enabled').length;
    const sseS3 = this.buckets.filter(b => b.encryptionType === 'SSE-S3').length;
    const sseKMS = this.buckets.filter(b => b.encryptionType === 'SSE-KMS').length;
    const unencrypted = total - encrypted;

    return {
      total,
      encrypted,
      unencrypted,
      sseS3,
      sseKMS,
      encryptionRate: total > 0 ? (encrypted / total * 100).toFixed(1) + '%' : '0%'
    };
  }

  // 搜索和过滤功能
  searchBuckets(query, filters = {}) {
    let filtered = [...this.buckets];

    // 名称搜索
    if (query) {
      const lowerQuery = query.toLowerCase();
      filtered = filtered.filter(bucket =>
        bucket.name.toLowerCase().includes(lowerQuery)
      );
    }

    // 加密状态过滤
    if (filters.encryptionStatus) {
      filtered = filtered.filter(bucket =>
        bucket.encryptionStatus === filters.encryptionStatus
      );
    }

    // 加密类型过滤
    if (filters.encryptionType) {
      filtered = filtered.filter(bucket =>
        bucket.encryptionType === filters.encryptionType
      );
    }

    return filtered;
  }

  // 获取可用的KMS密钥选项
  getKMSKeyOptions() {
    return this.kmsKeys.map(key => ({
      value: key.key_id,
      label: key.tags?.name || key.description || `Key: ${key.key_id.substring(0, 8)}...`,
      description: key.description,
      enabled: key.enabled,
      creationDate: key.creation_date
    }));
  }
}

// 使用示例
async function bucketEncryptionExample() {
  // 1. 初始化S3客户端和KMS API
  const s3Client = new S3Client({
    region: 'us-east-1',
    endpoint: 'http://localhost:9000',
    forcePathStyle: true,
    credentials: {
      accessKeyId: 'your-access-key',
      secretAccessKey: 'your-secret-key'
    }
  });

  const kmsAPI = {
    createKey: async (params) => callKMSAPI('POST', '/kms/keys', params),
    getKeyList: async () => callKMSAPI('GET', '/kms/keys'),
    getKeyDetails: async (keyId) => callKMSAPI('GET', `/kms/keys/${keyId}`)
  };

  // 2. 创建管理器实例
  const bucketManager = new BucketEncryptionManager(s3Client, kmsAPI);

  try {
    // 3. 初始化
    await bucketManager.initialize();

    // 4. 查看当前加密状态
    const stats = bucketManager.getEncryptionStats();
    console.log('加密统计:', stats);

    // 5. 为特定bucket配置SSE-KMS加密
    await bucketManager.setupDedicatedEncryption(
      'sensitive-data-bucket',
      'sensitive-data-key',
      'Encryption key for sensitive data bucket'
    );

    // 6. 为其他buckets配置SSE-S3加密
    const bucketConfigs = [
      { bucketName: 'public-assets', encryptionType: 'SSE-S3' },
      { bucketName: 'user-uploads', encryptionType: 'SSE-S3' },
      { bucketName: 'backup-data', encryptionType: 'SSE-S3' }
    ];

    const batchResults = await bucketManager.batchConfigureEncryption(bucketConfigs);
    console.log('批量配置结果:', batchResults);

    // 7. 搜索未加密的buckets
    const unencryptedBuckets = bucketManager.searchBuckets('', {
      encryptionStatus: 'Disabled'
    });

    if (unencryptedBuckets.length > 0) {
      console.log('发现未加密的buckets:', unencryptedBuckets.map(b => b.name));
    }

    // 8. 获取最终加密统计
    const finalStats = bucketManager.getEncryptionStats();
    console.log('最终加密统计:', finalStats);

  } catch (error) {
    console.error('Bucket加密管理示例执行失败:', error);
  }
}

// 启动示例
bucketEncryptionExample();
```

### JavaScript 基础请求函数

```javascript
import AWS from 'aws-sdk';

// 配置 AWS SDK
const awsConfig = {
  accessKeyId: 'your-access-key',
  secretAccessKey: 'your-secret-key',
  region: 'us-east-1',
  endpoint: 'http://localhost:9000',
  s3ForcePathStyle: true
};

// 创建签名请求的函数
function createSignedRequest(method, path, body = null) {
  const endpoint = new AWS.Endpoint(awsConfig.endpoint);
  const request = new AWS.HttpRequest(endpoint, awsConfig.region);

  request.method = method;
  request.path = `/rustfs/admin/v3${path}`;
  request.headers['Content-Type'] = 'application/json';

  if (body) {
    request.body = JSON.stringify(body);
  }

  const signer = new AWS.Signers.V4(request, 'execute-api');
  signer.addAuthorization(awsConfig, new Date());

  return request;
}

// 基础的 KMS API 调用函数
async function callKMSAPI(method, path, body = null) {
  const signedRequest = createSignedRequest(method, path, body);

  const options = {
    method: signedRequest.method,
    headers: signedRequest.headers,
    body: signedRequest.body
  };

  const response = await fetch(signedRequest.endpoint.href + signedRequest.path, options);
  const data = await response.json();

  if (!response.ok) {
    throw new Error(`KMS API Error: ${data.error?.message || response.statusText}`);
  }

  return data;
}

// 文件加密函数（使用 Web Crypto API）
async function encryptFileWithKey(fileData, plaintextKey) {
  // 将 Base64 密钥转换为 ArrayBuffer
  const keyData = Uint8Array.from(atob(plaintextKey), c => c.charCodeAt(0));

  // 导入密钥
  const cryptoKey = await crypto.subtle.importKey(
    'raw',
    keyData,
    { name: 'AES-GCM' },
    false,
    ['encrypt']
  );

  // 生成随机 IV
  const iv = crypto.getRandomValues(new Uint8Array(12));

  // 加密数据
  const encryptedData = await crypto.subtle.encrypt(
    { name: 'AES-GCM', iv: iv },
    cryptoKey,
    fileData
  );

  return {
    encryptedData: new Uint8Array(encryptedData),
    iv: iv
  };
}

// 文件解密函数
async function decryptFileWithKey(encryptedData, iv, plaintextKey) {
  const keyData = Uint8Array.from(atob(plaintextKey), c => c.charCodeAt(0));

  const cryptoKey = await crypto.subtle.importKey(
    'raw',
    keyData,
    { name: 'AES-GCM' },
    false,
    ['decrypt']
  );

  const decryptedData = await crypto.subtle.decrypt(
    { name: 'AES-GCM', iv: iv },
    cryptoKey,
    encryptedData
  );

  return new Uint8Array(decryptedData);
}
```

### React Hook 示例

```javascript
import { useState, useCallback } from 'react';

export function useKMSService() {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const callAPI = useCallback(async (method, path, body) => {
    setLoading(true);
    setError(null);

    try {
      const result = await callKMSAPI(method, path, body);
      return result;
    } catch (err) {
      setError(err.message);
      throw err;
    } finally {
      setLoading(false);
    }
  }, []);

  return { callAPI, loading, error };
}
```

### Vue.js Composable 示例

```javascript
import { ref } from 'vue';

export function useKMSService() {
  const loading = ref(false);
  const error = ref(null);

  const callAPI = async (method, path, body) => {
    loading.value = true;
    error.value = null;

    try {
      return await callKMSAPI(method, path, body);
    } catch (err) {
      error.value = err.message;
      throw err;
    } finally {
      loading.value = false;
    }
  };

  return { callAPI, loading, error };
}
```

### 完整的端到端使用示例

#### 1. KMS 服务初始化

```javascript
// KMS 服务管理类
class KMSServiceManager {
  constructor() {
    this.isConfigured = false;
    this.isRunning = false;
  }

  // 初始化 KMS 服务
  async initialize(backendType = 'local') {
    try {
      // 1. 配置 KMS 服务
      const config = backendType === 'local' ? {
        backend_type: "local",
        key_directory: "/var/lib/rustfs/kms/keys",
        default_key_id: "default-master-key",
        enable_cache: true,
        cache_ttl_seconds: 600
      } : {
        backend_type: "vault",
        address: "https://vault.example.com:8200",
        auth_method: { token: "s.your-vault-token" },
        mount_path: "transit",
        kv_mount: "secret",
        key_path_prefix: "rustfs/kms/keys",
        default_key_id: "rustfs-master"
      };

      const configResult = await callKMSAPI('POST', '/kms/configure', config);
      console.log('KMS 配置成功:', configResult);
      this.isConfigured = true;

      // 2. 启动 KMS 服务
      const startResult = await callKMSAPI('POST', '/kms/start');
      console.log('KMS 启动成功:', startResult);
      this.isRunning = true;

      // 3. 验证服务状态
      const status = await callKMSAPI('GET', '/kms/status');
      console.log('KMS 状态:', status);

      return { success: true, status };
    } catch (error) {
      console.error('KMS 初始化失败:', error);
      throw error;
    }
  }

  // 检查服务健康状态
  async checkHealth() {
    try {
      const status = await callKMSAPI('GET', '/kms/status');
      return status.healthy;
    } catch (error) {
      console.error('健康检查失败:', error);
      return false;
    }
  }
}
```

#### 2. 密钥管理工具类

```javascript
// 密钥管理工具类
class KMSKeyManager {
  constructor() {
    this.keys = new Map();
  }

  // 创建应用主密钥
  async createApplicationKey(description, tags = {}) {
    try {
      const keyRequest = {
        KeyUsage: "ENCRYPT_DECRYPT",
        Description: description,
        Tags: {
          ...tags,
          created_by: "frontend-app",
          created_at: new Date().toISOString()
        }
      };

      const result = await callKMSAPI('POST', '/kms/keys', keyRequest);
      this.keys.set(result.key_id, result.key_metadata);

      console.log(`密钥创建成功: ${result.key_id}`);
      return result;
    } catch (error) {
      console.error('密钥创建失败:', error);
      throw error;
    }
  }

  // 列出所有应用密钥
  async listApplicationKeys() {
    try {
      let allKeys = [];
      let marker = null;

      do {
        const params = new URLSearchParams({ limit: '50' });
        if (marker) params.append('marker', marker);

        const keysList = await callKMSAPI('GET', `/kms/keys?${params}`);
        allKeys.push(...keysList.keys);
        marker = keysList.next_marker;
      } while (marker);

      // 更新本地缓存
      allKeys.forEach(key => {
        this.keys.set(key.key_id, key);
      });

      return allKeys;
    } catch (error) {
      console.error('密钥列表获取失败:', error);
      throw error;
    }
  }

  // 获取密钥详情
  async getKeyDetails(keyId) {
    try {
      const details = await callKMSAPI('GET', `/kms/keys/${keyId}`);
      this.keys.set(keyId, details.key_metadata);
      return details;
    } catch (error) {
      console.error(`密钥详情获取失败 (${keyId}):`, error);
      throw error;
    }
  }

  // 安全删除密钥
  async safeDeleteKey(keyId, pendingDays = 7) {
    try {
      const deleteRequest = {
        key_id: keyId,
        pending_window_in_days: pendingDays
      };

      const result = await callKMSAPI('DELETE', '/kms/keys/delete', deleteRequest);
      console.log(`密钥已计划删除: ${keyId}, 删除日期: ${result.deletion_date}`);
      return result;
    } catch (error) {
      console.error(`密钥删除失败 (${keyId}):`, error);
      throw error;
    }
  }
}
```

#### 3. 文件加密管理器

```javascript
// 文件加密管理器
class FileEncryptionManager {
  constructor(keyManager) {
    this.keyManager = keyManager;
    this.encryptionCache = new Map();
  }

  // 加密文件
  async encryptFile(file, masterKeyId, metadata = {}) {
    try {
      // 1. 生成数据密钥
      const encryptionContext = {
        file_name: file.name,
        file_size: file.size.toString(),
        file_type: file.type,
        user_id: metadata.userId || 'unknown',
        ...metadata
      };

      const dataKeyRequest = {
        key_id: masterKeyId,
        key_spec: "AES_256",
        encryption_context: encryptionContext
      };

      const dataKey = await callKMSAPI('POST', '/kms/generate-data-key', dataKeyRequest);

      // 2. 读取文件数据
      const fileData = await this.readFileAsArrayBuffer(file);

      // 3. 加密文件数据
      const { encryptedData, iv } = await encryptFileWithKey(fileData, dataKey.plaintext_key);

      // 4. 立即清理内存中的原始密钥
      dataKey.plaintext_key = null;

      // 5. 创建加密文件信息
      const encryptedFileInfo = {
        encryptedData: encryptedData,
        iv: iv,
        ciphertextBlob: dataKey.ciphertext_blob,
        keyId: dataKey.key_id,
        encryptionContext: encryptionContext,
        originalFileName: file.name,
        originalSize: file.size,
        encryptedAt: new Date().toISOString()
      };

      // 6. 缓存加密信息
      const fileId = this.generateFileId();
      this.encryptionCache.set(fileId, encryptedFileInfo);

      console.log(`文件加密成功: ${file.name} -> ${fileId}`);
      return { fileId, encryptedFileInfo };

    } catch (error) {
      console.error(`文件加密失败 (${file.name}):`, error);
      throw error;
    }
  }

  // 解密文件
  async decryptFile(fileId) {
    try {
      // 1. 获取加密文件信息
      const encryptedFileInfo = this.encryptionCache.get(fileId);
      if (!encryptedFileInfo) {
        throw new Error('加密文件信息不存在');
      }

      // 2. 解密数据密钥
      const decryptRequest = {
        ciphertext_blob: encryptedFileInfo.ciphertextBlob,
        encryption_context: encryptedFileInfo.encryptionContext
      };

      const decryptedKey = await callKMSAPI('POST', '/kms/decrypt', decryptRequest);

      // 3. 解密文件数据
      const decryptedData = await decryptFileWithKey(
        encryptedFileInfo.encryptedData,
        encryptedFileInfo.iv,
        decryptedKey.plaintext
      );

      // 4. 立即清理内存中的原始密钥
      decryptedKey.plaintext = null;

      // 5. 创建解密后的文件对象
      const decryptedFile = new File(
        [decryptedData],
        encryptedFileInfo.originalFileName,
        { type: encryptedFileInfo.encryptionContext.file_type }
      );

      console.log(`文件解密成功: ${fileId} -> ${encryptedFileInfo.originalFileName}`);
      return decryptedFile;

    } catch (error) {
      console.error(`文件解密失败 (${fileId}):`, error);
      throw error;
    }
  }

  // 批量加密文件
  async encryptFiles(files, masterKeyId, metadata = {}) {
    const results = [];

    for (const file of files) {
      try {
        const result = await this.encryptFile(file, masterKeyId, {
          ...metadata,
          batch_id: this.generateBatchId(),
          file_index: results.length
        });
        results.push({ success: true, file: file.name, ...result });
      } catch (error) {
        results.push({ success: false, file: file.name, error: error.message });
      }
    }

    return results;
  }

  // 工具方法
  readFileAsArrayBuffer(file) {
    return new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = () => resolve(reader.result);
      reader.onerror = () => reject(reader.error);
      reader.readAsArrayBuffer(file);
    });
  }

  generateFileId() {
    return 'file_' + Date.now() + '_' + Math.random().toString(36).substr(2, 9);
  }

  generateBatchId() {
    return 'batch_' + Date.now() + '_' + Math.random().toString(36).substr(2, 9);
  }
}
```

#### 4. 完整的应用示例

```javascript
// 完整的 KMS 应用示例
class KMSApplication {
  constructor() {
    this.serviceManager = new KMSServiceManager();
    this.keyManager = new KMSKeyManager();
    this.fileManager = null;
    this.appMasterKeyId = null;
  }

  // 初始化应用
  async initialize() {
    try {
      console.log('正在初始化 KMS 应用...');

      // 1. 初始化 KMS 服务
      await this.serviceManager.initialize('local');

      // 2. 创建应用主密钥
      const appKey = await this.keyManager.createApplicationKey(
        '文件加密应用主密钥',
        {
          application: 'file-encryption-app',
          version: '1.0.0',
          environment: 'production'
        }
      );
      this.appMasterKeyId = appKey.key_id;

      // 3. 初始化文件管理器
      this.fileManager = new FileEncryptionManager(this.keyManager);

      console.log('KMS 应用初始化完成');
      return { success: true, masterKeyId: this.appMasterKeyId };

    } catch (error) {
      console.error('KMS 应用初始化失败:', error);
      throw error;
    }
  }

  // 处理文件上传和加密
  async handleFileUpload(files, userMetadata = {}) {
    if (!this.fileManager || !this.appMasterKeyId) {
      throw new Error('应用未初始化');
    }

    try {
      console.log(`开始处理 ${files.length} 个文件的加密...`);

      const results = await this.fileManager.encryptFiles(
        files,
        this.appMasterKeyId,
        {
          ...userMetadata,
          upload_session: Date.now()
        }
      );

      const successCount = results.filter(r => r.success).length;
      console.log(`文件加密完成: ${successCount}/${files.length} 成功`);

      return results;

    } catch (error) {
      console.error('文件上传处理失败:', error);
      throw error;
    }
  }

  // 处理文件下载和解密
  async handleFileDownload(fileId) {
    if (!this.fileManager) {
      throw new Error('应用未初始化');
    }

    try {
      console.log(`开始解密文件: ${fileId}`);
      const decryptedFile = await this.fileManager.decryptFile(fileId);

      // 创建下载链接
      const url = URL.createObjectURL(decryptedFile);
      const a = document.createElement('a');
      a.href = url;
      a.download = decryptedFile.name;
      a.click();

      // 清理资源
      setTimeout(() => URL.revokeObjectURL(url), 100);

      console.log(`文件下载完成: ${decryptedFile.name}`);
      return decryptedFile;

    } catch (error) {
      console.error('文件下载处理失败:', error);
      throw error;
    }
  }

  // 健康检查
  async performHealthCheck() {
    try {
      const isHealthy = await this.serviceManager.checkHealth();
      const keyCount = this.keyManager.keys.size;

      return {
        kmsHealthy: isHealthy,
        keyCount: keyCount,
        masterKeyId: this.appMasterKeyId,
        timestamp: new Date().toISOString()
      };
    } catch (error) {
      console.error('健康检查失败:', error);
      return { kmsHealthy: false, error: error.message };
    }
  }
}

// 使用示例
async function main() {
  const app = new KMSApplication();

  try {
    // 初始化应用
    await app.initialize();

    // 模拟文件上传
    const fileInput = document.getElementById('file-input');
    fileInput.addEventListener('change', async (event) => {
      const files = Array.from(event.target.files);
      const results = await app.handleFileUpload(files, {
        userId: 'user123',
        department: 'finance'
      });

      console.log('上传结果:', results);
    });

    // 定期健康检查
    setInterval(async () => {
      const health = await app.performHealthCheck();
      console.log('健康状态:', health);
    }, 30000);

  } catch (error) {
    console.error('应用启动失败:', error);
  }
}

// 启动应用
main();
```

## 🔗 相关资源

- [KMS 配置指南](configuration.md)
- [服务端加密集成](sse-integration.md)
- [安全最佳实践](security.md)
- [故障排除指南](troubleshooting.md)

## 📞 技术支持

如果在对接过程中遇到问题，请：

1. **KMS服务问题**: 检查 [故障排除指南](troubleshooting.md)
2. **Bucket加密问题**: 验证S3客户端配置和权限设置
3. **查看日志**: 检查服务器日志以获取详细错误信息
4. **运行测试**: 验证KMS配置：`cargo test -p e2e_test kms:: -- --nocapture`
5. **API兼容性**: 确保使用的AWS SDK版本支持相关操作

### 常见问题解决

**Q: Bucket加密配置失败，提示权限不足**
A: 检查IAM策略是否包含以下权限：
- `s3:GetBucketEncryption`
- `s3:PutBucketEncryption`
- `s3:DeleteBucketEncryption`
- `kms:DescribeKey`（当使用SSE-KMS时）

**Q: KMS密钥在bucket加密中无法选择**
A: 确保：
1. KMS服务状态为Running且健康
2. 密钥状态为Enabled
3. 密钥的KeyUsage为ENCRYPT_DECRYPT

**Q: 前端显示加密状态错误**
A: 这通常是由于：
1. 获取bucket加密配置时发生404错误（正常，表示未配置）
2. 网络延迟导致状态更新不及时，手动刷新即可

---

*本文档版本：v1.1 | 最后更新：2024-09-22 | 新增：Bucket加密配置API指南*