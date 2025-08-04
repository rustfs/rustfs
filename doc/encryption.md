# RustFS 加密系统完整指南

本文档详细介绍RustFS的KMS（密钥管理服务）和加密系统的配置、使用方法及API接口。

## 目录

1. [系统概述](#系统概述)
2. [KMS配置](#kms配置)
3. [加密配置](#加密配置)
4. [API接口详解](#api接口详解)
5. [使用示例](#使用示例)
6. [故障排查](#故障排查)

## 系统概述

RustFS提供完整的加密解决方案，包括：
- **KMS集成**：支持多种密钥管理服务（HashiCorp Vault、AWS KMS等）
- **透明加密**：自动对存储数据进行加密/解密
- **密钥轮换**：支持自动和手动密钥轮换
- **访问控制**：基于IAM的细粒度权限管理

## KMS配置

### 支持的KMS类型

#### 1. HashiCorp Vault

**配置参数：**

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| `kms_type` | string | 是 | - | 固定值：`vault` |
| `vault_address` | string | 是 | - | Vault服务器地址，格式：`https://vault.example.com:8200` |
| `vault_token` | string | 是* | - | Vault访问令牌（与AppRole二选一） |
| `vault_namespace` | string | 否 | - | Vault命名空间 |
| `vault_mount_path` | string | 否 | `transit` | Transit引擎挂载路径 |
| `vault_timeout_seconds` | int | 否 | `30` | 请求超时时间（秒） |
| `vault_app_role_id` | string | 是* | - | AppRole认证ID |
| `vault_app_role_secret_id` | string | 是* | - | AppRole认证密钥 |

**示例配置：**
```json
{
  "kms_type": "vault",
  "vault_address": "https://vault.example.com:8200",
  "vault_token": "hvp.AAAAAQAABBBBcccc...",
  "vault_namespace": "rustfs",
  "vault_mount_path": "transit",
  "vault_timeout_seconds": 30
}
```

#### 2. AWS KMS

**配置参数：**

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| `kms_type` | string | 是 | - | 固定值：`aws` |
| `aws_region` | string | 是 | - | AWS区域，如：`us-east-1` |
| `aws_access_key_id` | string | 是* | - | AWS访问密钥ID |
| `aws_secret_access_key` | string | 是* | - | AWS秘密访问密钥 |
| `aws_session_token` | string | 否 | - | AWS会话令牌（临时凭证） |
| `key_id` | string | 是 | - | KMS密钥ID或ARN |

**示例配置：**
```json
{
  "kms_type": "aws",
  "aws_region": "us-east-1",
  "aws_access_key_id": "AKIA...",
  "aws_secret_access_key": "secret...",
  "key_id": "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012"
}
```

#### 3. 本地KMS（开发测试用）

**配置参数：**

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| `kms_type` | string | 是 | - | 固定值：`local` |
| `key_path` | string | 否 | `./keys` | 密钥存储目录 |

**示例配置：**
```json
{
  "kms_type": "local",
  "key_path": "./rustfs-keys"
}
```

## 加密配置

### 存储桶加密配置

存储桶级别加密支持以下模式：

#### 1. SSE-KMS（服务器端加密-KMS）

**配置参数：**

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| `algorithm` | string | 是 | `AES256` | 加密算法：`AES256`、`aws:kms` |
| `kms_key_id` | string | 是 | - | KMS密钥ID |
| `kms_context` | object | 否 | `{}` | 加密上下文键值对 |

**示例配置：**
```json
{
  "algorithm": "aws:kms",
  "kms_key_id": "rustfs-master-key",
  "kms_context": {
    "bucket": "my-bucket",
    "purpose": "data-at-rest"
  }
}
```

#### 2. SSE-S3（服务器端加密-S3）

**配置参数：**

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| `algorithm` | string | 是 | `AES256` | 固定值：`AES256` |

**示例配置：**
```json
{
  "algorithm": "AES256"
}
```

## API接口详解

### KMS管理接口

#### 1. 配置KMS

**接口地址：** `POST http://localhost:9000/rustfs/admin/v3/kms/configure`

**完整请求示例：**
```bash
curl -X POST "http://localhost:9000/rustfs/admin/v3/kms/configure" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <admin-token>" \
  -d '{
    "kms_type": "vault",
    "vault_address": "https://vault.example.com:8200",
    "vault_token": "hvp.AAAAAQAABBBBcccc...",
    "vault_namespace": "rustfs"
  }'
```

**请求头：**
```
Content-Type: application/json
Authorization: Bearer <admin-token>
```

**请求体：**
```json
{
  "kms_type": "vault",
  "vault_address": "https://vault.example.com:8200",
  "vault_token": "hvp.AAAAAQAABBBBcccc...",
  "vault_namespace": "rustfs"
}
```

**响应：**
```json
{
  "success": true,
  "message": "KMS configured successfully",
  "kms_type": "vault"
}
```

**错误响应：**
```json
{
  "success": false,
  "message": "Failed to connect to Vault: connection timeout",
  "kms_type": "vault"
}
```

#### 2. 获取KMS状态

**接口地址：** `GET http://localhost:9000/rustfs/admin/v3/kms/status`

**完整请求示例：**
```bash
curl -X GET "http://localhost:9000/rustfs/admin/v3/kms/status" \
  -H "Authorization: Bearer <admin-token>"
```

**响应：**
```json
{
  "status": "active",
  "backend": "vault",
  "healthy": true,
  "version": "1.15.0",
  "endpoint": "https://vault.example.com:8200"
}
```

#### 3. 获取KMS密钥列表

**接口地址：** `GET http://localhost:9000/rustfs/admin/v3/kms/key/list`

**完整请求示例：**
```bash
curl -X GET "http://localhost:9000/rustfs/admin/v3/kms/key/list" \
  -H "Authorization: Bearer <admin-token>"
```

**响应：**
```json
{
  "keys": [
    {
      "name": "rustfs-master-key",
      "type": "AES256",
      "created_at": "2024-01-15T10:30:00Z",
      "status": "enabled"
    },
    {
      "name": "my-app-key",
      "type": "AES256",
      "created_at": "2024-01-16T14:20:00Z",
      "status": "enabled"
    }
  ]
}
```

#### 4. 获取KMS密钥状态

**接口地址：** `GET http://localhost:9000/rustfs/admin/v3/kms/key/{key_name}/status`

**完整请求示例：**
```bash
curl -X GET "http://localhost:9000/rustfs/admin/v3/kms/key/rustfs-master-key/status" \
  -H "Authorization: Bearer <admin-token>"
```

**响应：**
```json
{
  "name": "rustfs-master-key",
  "type": "AES256",
  "status": "enabled",
  "created_at": "2024-01-15T10:30:00Z",
  "last_rotation": "2024-01-20T08:15:00Z"
}
```

#### 5. 启用KMS密钥

**接口地址：** `POST http://localhost:9000/rustfs/admin/v3/kms/key/{key_name}/enable`

**完整请求示例：**
```bash
curl -X POST "http://localhost:9000/rustfs/admin/v3/kms/key/rustfs-master-key/enable" \
  -H "Authorization: Bearer <admin-token>"
```

**响应：**
```json
{
  "success": true,
  "message": "Key rustfs-master-key enabled successfully"
}
```

#### 6. 禁用KMS密钥

**接口地址：** `POST http://localhost:9000/rustfs/admin/v3/kms/key/{key_name}/disable`

**完整请求示例：**
```bash
curl -X POST "http://localhost:9000/rustfs/admin/v3/kms/key/rustfs-master-key/disable" \
  -H "Authorization: Bearer <admin-token>"
```

**响应：**
```json
{
  "success": true,
  "message": "Key rustfs-master-key disabled successfully"
}
```
  "description": "Application encryption key"
}
```

**响应：**
```json
{
  "keyId": "key-12345678",
  "keyName": "my-app-key",
  "status": "enabled",
  "createdAt": "2024-01-15T10:30:00Z"
}
```

#### 4. 列出KMS密钥

**接口：** `GET /rustfs/admin/v3/kms/key/list`

**响应：**
```json
{
  "keys": [
    {
      "keyId": "key-12345678",
      "keyName": "my-app-key",
      "status": "enabled",
      "createdAt": "2024-01-15T10:30:00Z",
      "algorithm": "AES256"
    }
  ]
}
```

#### 5. 启用KMS密钥

**接口：** `PUT /rustfs/admin/v3/kms/key/enable?key_id=key-12345678`

**响应：**
```json
{
  "success": true,
  "message": "Key enabled successfully"
}
```

#### 6. 禁用KMS密钥

**接口：** `PUT /rustfs/admin/v3/kms/key/disable?key_id=key-12345678`

**响应：**
```json
{
  "success": true,
  "message": "Key disabled successfully"
}
```

### 存储桶加密接口

#### 1. 设置存储桶加密

**接口地址：** `PUT http://localhost:9000/rustfs/admin/v3/bucket-encryption/{bucket}`

**完整请求示例：**
```bash
curl -X PUT "http://localhost:9000/rustfs/admin/v3/bucket-encryption/my-bucket" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <admin-token>" \
  -d '{
      "algorithm": "aws:kms",
      "kms_key_id": "rustfs-master-key",
      "kms_context": {
        "bucket": "my-bucket"
      }
    }'
```

**请求体：**
```json
{
  "algorithm": "aws:kms",
  "kms_key_id": "rustfs-master-key",
  "kms_context": {
    "bucket": "my-bucket"
  }
}
```

**响应：**
```json
{
  "success": true,
  "message": "Bucket encryption configured successfully"
}
```

#### 2. 获取存储桶加密配置

**接口地址：** `GET http://localhost:9000/rustfs/admin/v3/bucket-encryption/{bucket}`

**完整请求示例：**
```bash
curl -X GET "http://localhost:9000/rustfs/admin/v3/bucket-encryption/my-bucket" \
  -H "Authorization: Bearer <admin-token>"
```

**响应：**
```json
{
  "algorithm": "aws:kms",
  "kms_key_id": "rustfs-master-key",
  "kms_context": {
    "bucket": "my-bucket"
  },
  "last_modified": "2024-01-20T10:30:00Z"
}
```

#### 3. 删除存储桶加密配置

**接口地址：** `DELETE http://localhost:9000/rustfs/admin/v3/bucket-encryption/{bucket}`

**完整请求示例：**
```bash
curl -X DELETE "http://localhost:9000/rustfs/admin/v3/bucket-encryption/my-bucket" \
  -H "Authorization: Bearer <admin-token>"
```

**响应：**
```json
{
  "success": true,
  "message": "Bucket encryption configuration removed"
}
```

#### 4. 列出所有存储桶加密配置

**接口地址：** `GET http://localhost:9000/rustfs/admin/v3/bucket-encryptions`

**完整请求示例：**
```bash
curl -X GET "http://localhost:9000/rustfs/admin/v3/bucket-encryptions" \
  -H "Authorization: Bearer <admin-token>"
```

**响应：**
```json
{
  "bucket_encryptions": [
    {
      "bucket": "my-bucket",
      "algorithm": "aws:kms",
      "kms_key_id": "rustfs-master-key",
      "last_modified": "2024-01-20T10:30:00Z"
    },
    {
      "bucket": "public-bucket",
      "algorithm": "AES256",
      "last_modified": "2024-01-19T15:45:00Z"
    }
  ]
}
```

## 使用示例

### 完整配置流程

#### 步骤1：配置KMS

```bash
# 配置HashiCorp Vault
 curl -X POST "http://localhost:9000/rustfs/admin/v3/kms/configure" \\
  -H "Authorization: Bearer YOUR_ADMIN_TOKEN" \\
  -H "Content-Type: application/json" \\
  -d '{
    "kms_type": "vault",
    "vault_address": "https://vault.example.com:8200",
    "vault_token": "hvp.AAAAAQAABBBBcccc...",
    "vault_namespace": "rustfs"
  }'
```

#### 步骤2：验证KMS状态

```bash
 curl -X GET "http://localhost:9000/rustfs/admin/v3/kms/status" \\
  -H "Authorization: Bearer YOUR_ADMIN_TOKEN"
```

#### 步骤3：创建加密密钥

```bash
 curl -X POST "http://localhost:9000/rustfs/admin/v3/kms/key/create" \\
  -H "Authorization: Bearer YOUR_ADMIN_TOKEN" \\
  -H "Content-Type: application/json" \\
  -d '{
    "key_name": "my-bucket-key",
    "key_type": "AES256",
    "description": "Encryption key for my-bucket"
  }'
```

#### 步骤4：配置存储桶加密

```bash
 curl -X PUT "http://localhost:9000/rustfs/admin/v3/bucket-encryption/my-bucket" \\
  -H "Authorization: Bearer YOUR_ADMIN_TOKEN" \\
  -H "Content-Type: application/json" \\
  -d '{
    "algorithm": "aws:kms",
    "kms_key_id": "my-bucket-key",
    "kms_context": {
      "bucket": "my-bucket"
    }
  }'
```

#### 步骤5：验证加密配置

```bash
 curl -X GET "http://localhost:9000/rustfs/admin/v3/bucket-encryption/my-bucket" \\
  -H "Authorization: Bearer YOUR_ADMIN_TOKEN"
```

### 使用S3 SDK上传加密文件

#### AWS CLI示例

```bash
# 上传文件到加密存储桶
 aws s3 cp myfile.txt s3://my-bucket/ \\
  --endpoint-url http://localhost:9000 \\
  --sse aws:kms \\
  --sse-kms-key-id my-bucket-key
```

#### Python示例

```python
import boto3

s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='YOUR_ACCESS_KEY',
    aws_secret_access_key='YOUR_SECRET_KEY'
)

# 上传加密文件
s3.upload_file(
    'myfile.txt',
    'my-bucket',
    'myfile.txt',
    ExtraArgs={
        'ServerSideEncryption': 'aws:kms',
        'SSEKMSKeyId': 'my-bucket-key'
    }
)
```

### 密钥轮换

#### 自动密钥轮换

```bash
# 启用自动轮换（每90天）
 curl -X POST "http://localhost:9000/rustfs/admin/v3/kms/key/rotate/auto" \\
  -H "Authorization: Bearer YOUR_ADMIN_TOKEN" \\
  -H "Content-Type: application/json" \\
  -d '{
    "key_id": "my-bucket-key",
    "rotation_period_days": 90,
    "enabled": true
  }'
```

#### 手动密钥轮换

```bash
# 立即轮换密钥
 curl -X POST "http://localhost:9000/rustfs/admin/v3/kms/key/rotate" \\
  -H "Authorization: Bearer YOUR_ADMIN_TOKEN" \\
  -H "Content-Type: application/json" \\
  -d '{
    "key_id": "my-bucket-key",
    "reason": "Security incident response"
  }'
```

## 故障排查

### 常见问题

#### 1. KMS连接失败

**症状：**
```json
{"error": "Failed to connect to KMS backend"}
```

**解决方案：**
1. 检查网络连接
2. 验证KMS凭证
3. 确认KMS服务状态

#### 2. 密钥权限不足

**症状：**
```json
{"error": "Access denied to key 'my-bucket-key'"}
```

**解决方案：**
1. 检查IAM策略
2. 确认密钥存在
3. 验证密钥状态（enabled/disabled）

#### 3. 加密配置错误

**症状：**
```json
{"error": "Invalid encryption configuration"}
```

**解决方案：**
1. 验证算法参数
2. 检查密钥ID格式
3. 确认KMS上下文格式

### 调试日志

启用调试日志：

```bash
export RUST_LOG=debug
./rustfs server /path/to/config
```

查看KMS相关日志：
```bash
grep "KMS" /var/log/rustfs/rustfs.log
```

### 性能调优

#### KMS缓存配置

在配置文件中添加：
```yaml
kms:
  cache:
    enabled: true
    ttl_seconds: 3600
    max_entries: 1000
```

#### 连接池配置

```yaml
kms:
  connection_pool:
    max_connections: 10
    timeout_seconds: 30
    idle_timeout_seconds: 300
```

## 安全最佳实践

### 1. 密钥管理
- 使用强密钥（256位以上）
- 定期轮换密钥（建议90天）
- 分离不同用途的密钥

### 2. 访问控制
- 最小权限原则
- 使用IAM角色而非长期凭证
- 启用访问日志

### 3. 监控告警
- 监控密钥使用频率
- 设置异常访问告警
- 定期审计密钥权限

### 4. 备份恢复
- 定期备份KMS配置
- 测试密钥恢复流程
- 维护离线密钥副本

## 版本兼容性

| RustFS版本 | KMS API版本 | 支持功能 |
|------------|-------------|----------|
| 1.0.x | v3 | 基础加密 |
| 1.1.x | v3 | 密钥轮换 |
| 1.2.x | v3 | 多KMS支持 |
| 2.0.x | v4 | 高级加密 |

## 参考链接

- [KMS API完整文档](./kms.md)
- [存储桶加密文档](./bucket-encryption.md)
- [IAM权限管理](../rustfs/src/admin/README.md)
- [RustFS配置指南](../README.md)

### S3兼容接口完整示例

#### 1. PutBucketEncryption (S3标准接口)

**接口地址：** `PUT http://localhost:9000/{bucket}?encryption`

**完整请求示例：**
```bash
curl -X PUT "http://localhost:9000/my-bucket?encryption" \
  -H "Authorization: Bearer <admin-token>" \
  -H "Content-Type: application/xml" \
  -d '<?xml version="1.0" encoding="UTF-8"?>
<ServerSideEncryptionConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Rule>
    <ApplyServerSideEncryptionByDefault>
      <SSEAlgorithm>aws:kms</SSEAlgorithm>
      <KMSMasterKeyID>rustfs-master-key</KMSMasterKeyID>
    </ApplyServerSideEncryptionByDefault>
  </Rule>
</ServerSideEncryptionConfiguration>'
```

#### 2. GetBucketEncryption (S3标准接口)

**接口地址：** `GET http://localhost:9000/{bucket}?encryption`

**完整请求示例：**
```bash
curl -X GET "http://localhost:9000/my-bucket?encryption" \
  -H "Authorization: Bearer <admin-token>"
```

**响应：**
```xml
<?xml version="1.0" encoding="UTF-8"?>
<ServerSideEncryptionConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Rule>
    <ApplyServerSideEncryptionByDefault>
      <SSEAlgorithm>aws:kms</SSEAlgorithm>
      <KMSMasterKeyID>rustfs-master-key</KMSMasterKeyID>
    </ApplyServerSideEncryptionByDefault>
  </Rule>
</ServerSideEncryptionConfiguration>
```

#### 3. DeleteBucketEncryption (S3标准接口)

**接口地址：** `DELETE http://localhost:9000/{bucket}?encryption`

**完整请求示例：**
```bash
curl -X DELETE "http://localhost:9000/my-bucket?encryption" \
  -H "Authorization: Bearer <admin-token>"
```

**响应：** HTTP 204 No Content

#### 4. PutObject加密上传 (S3标准接口)

**接口地址：** `PUT http://localhost:9000/{bucket}/{object_key}`

**完整请求示例：**
```bash
curl -X PUT "http://localhost:9000/my-bucket/secret-file.txt" \
  -H "Authorization: Bearer <admin-token>" \
  -H "x-amz-server-side-encryption: aws:kms" \
  -H "x-amz-server-side-encryption-aws-kms-key-id: rustfs-master-key" \
  -H "Content-Type: text/plain" \
  -d "This is sensitive data"
```

**响应：**
```xml
<?xml version="1.0" encoding="UTF-8"?>
<CopyObjectResult>
  <ETag>"d41d8cd98f00b204e9800998ecf8427e"</ETag>
  <LastModified>2024-01-20T10:30:00.000Z</LastModified>
</CopyObjectResult>
```

#### 5. 使用AWS CLI访问加密接口

**配置AWS CLI：**
```bash
aws configure set aws_access_key_id YOUR_ACCESS_KEY
aws configure set aws_secret_access_key YOUR_SECRET_KEY
aws configure set region us-east-1
```

**设置存储桶加密：**
```bash
aws s3api put-bucket-encryption \
  --bucket my-bucket \
  --endpoint-url http://localhost:9000 \
  --server-side-encryption-configuration '{
    "Rules": [
      {
        "ApplyServerSideEncryptionByDefault": {
          "SSEAlgorithm": "aws:kms",
          "KMSMasterKeyID": "rustfs-master-key"
        }
      }
    ]
  }'
```

**获取存储桶加密：**
```bash
aws s3api get-bucket-encryption \
  --bucket my-bucket \
  --endpoint-url http://localhost:9000
```

**删除存储桶加密：**
```bash
aws s3api delete-bucket-encryption \
  --bucket my-bucket \
  --endpoint-url http://localhost:9000
```

#### 6. 使用Postman测试接口

**基础配置：**
- URL: `http://localhost:9000`
- 认证: Bearer Token
- Content-Type: 根据接口选择 `application/json` 或 `application/xml`

**测试KMS配置接口：**
```
POST http://localhost:9000/rustfs/admin/v3/kms/configure
Headers:
  Authorization: Bearer <your-admin-token>
  Content-Type: application/json

Body (raw JSON):
{
  "kms_type": "vault",
  "vault_address": "https://vault.example.com:8200",
  "vault_token": "your-vault-token"
}
```

**测试存储桶加密接口：**
```
PUT http://localhost:9000/rustfs/admin/v3/bucket-encryption/my-bucket
Headers:
  Authorization: Bearer <your-admin-token>
  Content-Type: application/json

Body (raw JSON):
{
  "algorithm": "aws:kms",
  "kms_key_id": "rustfs-master-key"
}
```