# Bucket 加密使用文档

本文档介绍如何在 RustFS 中使用 S3 兼容的 bucket 加密功能。RustFS 支持多种服务端加密方式，包括 SSE-S3、SSE-KMS 和 SSE-C。

## 加密类型概述

### 1. SSE-S3 (Server-Side Encryption with S3-Managed Keys)
- 使用 S3 服务管理的密钥进行加密
- 最简单的加密方式，无需额外配置
- 算法：AES-256

### 2. SSE-KMS (Server-Side Encryption with KMS-Managed Keys)
- 使用 KMS 管理的密钥进行加密
- 提供更细粒度的密钥控制和审计
- 支持自定义 KMS 密钥

兼容说明：请求中若使用 `aws:kms:dsse` 算法将被接受并规范化为 `aws:kms`（响应与对象元数据中都会显示为 `aws:kms`）。

### 3. SSE-C (Server-Side Encryption with Customer-Provided Keys)
- 使用客户提供的密钥进行加密
- 客户完全控制加密密钥
- 需要在每次请求时提供密钥

## 使用方法

### SSE-S3 加密

#### 上传对象时指定加密

```rust
use aws_sdk_s3::{primitives::ByteStream, types::ServerSideEncryption};

// 创建 S3 客户端
let client = aws_sdk_s3::Client::new(&config);

// 上传对象并启用 SSE-S3 加密
let test_data = b"Hello, SSE-S3 encryption!";
client
    .put_object()
    .bucket("my-bucket")
    .key("my-object")
    .body(ByteStream::from(test_data.to_vec()))
    .server_side_encryption(ServerSideEncryption::Aes256)  // 启用 SSE-S3
    .send()
    .await?;
```

#### 使用 curl 命令

```bash
curl -X PUT "http://localhost:9000/my-bucket/my-object" \
  -H "x-amz-server-side-encryption: AES256" \
  -d "Hello, SSE-S3 encryption!"
```

### SSE-KMS 加密

#### 使用默认 KMS 密钥

```rust
use aws_sdk_s3::{primitives::ByteStream, types::ServerSideEncryption};

// 上传对象并启用 SSE-KMS 加密（使用默认密钥）
let test_data = b"Hello, SSE-KMS encryption!";
client
    .put_object()
    .bucket("my-bucket")
    .key("my-object")
    .body(ByteStream::from(test_data.to_vec()))
    .server_side_encryption(ServerSideEncryption::AwsKms)  // 启用 SSE-KMS
    .send()
    .await?;
```

#### 使用指定的 KMS 密钥

```rust
// 上传对象并使用指定的 KMS 密钥
let test_data = b"Hello, SSE-KMS encryption!";
let kms_key_id = "my-custom-kms-key";

client
    .put_object()
    .bucket("my-bucket")
    .key("my-object")
    .body(ByteStream::from(test_data.to_vec()))
    .server_side_encryption(ServerSideEncryption::AwsKms)
    .ssekms_key_id(kms_key_id)  // 指定 KMS 密钥 ID
    .send()
    .await?;
```

#### 使用 curl 命令

```bash
# 使用默认 KMS 密钥
curl -X PUT "http://localhost:9000/my-bucket/my-object" \
  -H "x-amz-server-side-encryption: aws:kms" \
  -d "Hello, SSE-KMS encryption!"

# 使用指定的 KMS 密钥
curl -X PUT "http://localhost:9000/my-bucket/my-object" \
  -H "x-amz-server-side-encryption: aws:kms" \
  -H "x-amz-server-side-encryption-aws-kms-key-id: my-custom-kms-key" \
  -d "Hello, SSE-KMS encryption!"
```

### SSE-C 加密

#### 使用客户提供的密钥

```rust
use base64::{Engine, engine::general_purpose::STANDARD};
use md5::{Digest, Md5};

// 准备客户密钥
let customer_key = b"1234567890abcdef1234567890abcdef"; // 32 字节密钥
let mut hasher = Md5::new();
hasher.update(customer_key);
let customer_key_md5 = STANDARD.encode(hasher.finalize().as_slice());

// 上传对象
let test_data = b"Hello, SSE-C encryption!";
client
    .put_object()
    .bucket("my-bucket")
    .key("my-object")
    .body(ByteStream::from(test_data.to_vec()))
    .sse_customer_algorithm("AES256")  // 指定加密算法
    .sse_customer_key(STANDARD.encode(customer_key))  // Base64 编码的密钥
    .sse_customer_key_md5(customer_key_md5)  // 密钥的 MD5 哈希
    .send()
    .await?;

// 下载对象时也需要提供相同的密钥
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

#### 使用 curl 命令

```bash
# 准备密钥和 MD5 哈希
KEY="MTIzNDU2Nzg5MGFiY2RlZjEyMzQ1Njc4OTBhYmNkZWY="  # Base64 编码的 32 字节密钥
KEY_MD5="$(echo -n "1234567890abcdef1234567890abcdef" | md5sum | cut -d' ' -f1 | xxd -r -p | base64)"

# 上传对象
curl -X PUT "http://localhost:9000/my-bucket/my-object" \
  -H "x-amz-server-side-encryption-customer-algorithm: AES256" \
  -H "x-amz-server-side-encryption-customer-key: $KEY" \
  -H "x-amz-server-side-encryption-customer-key-MD5: $KEY_MD5" \
  -d "Hello, SSE-C encryption!"

# 下载对象
curl "http://localhost:9000/my-bucket/my-object" \
  -H "x-amz-server-side-encryption-customer-algorithm: AES256" \
  -H "x-amz-server-side-encryption-customer-key: $KEY" \
  -H "x-amz-server-side-encryption-customer-key-MD5: $KEY_MD5"

### SSE-C 限制与注意事项
- 仅支持单次 PUT/GET 以及 COPY 的“源对象解密”与“目标对象按请求或桶默认策略加密”；不支持多部分上传（Multipart Upload）的 SSE-C。
- 客户密钥绝不会在服务端持久化；只存储必要的算法与随机向量等元信息。
- 强烈建议全程使用 HTTPS 以防止明文密钥在网络中泄露。
- GET/HEAD 访问 SSE-C 对象时，调用方必须提供与写入时相同的客户密钥与 MD5 校验头；否则会返回解密失败错误。
```

## Bucket 默认加密配置

### 设置 Bucket 默认加密

可以为 bucket 设置默认的加密配置，这样上传到该 bucket 的对象会自动应用加密设置。

```rust
use aws_sdk_s3::types::{
    ServerSideEncryption, ServerSideEncryptionByDefault, 
    ServerSideEncryptionConfiguration, ServerSideEncryptionRule
};

// 配置默认加密为 SSE-S3
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

// 应用加密配置到 bucket
client
    .put_bucket_encryption()
    .bucket("my-bucket")
    .server_side_encryption_configuration(encryption_config)
    .send()
    .await?;
```

### 配置 SSE-KMS 默认加密

```rust
// 配置默认加密为 SSE-KMS
let by_default = ServerSideEncryptionByDefault::builder()
    .sse_algorithm(ServerSideEncryption::AwsKms)
    .kms_master_key_id("my-default-kms-key")  // 可选：指定默认 KMS 密钥
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

### 使用 curl 设置默认加密

```bash
# 设置 SSE-S3 默认加密
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

# 设置 SSE-KMS 默认加密
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

## 多部分上传加密

对于大文件的多部分上传，也支持加密：

```rust
// 创建多部分上传并启用加密
let multipart_upload = client
    .create_multipart_upload()
    .bucket("my-bucket")
    .key("large-object")
    .server_side_encryption(ServerSideEncryption::Aes256)  // 启用加密
    .send()
    .await?;

let upload_id = multipart_upload.upload_id().unwrap();

// 上传分片
let part_data = vec![b'A'; 5 * 1024 * 1024]; // 5MB 分片
let upload_part = client
    .upload_part()
    .bucket("my-bucket")
    .key("large-object")
    .upload_id(upload_id)
    .part_number(1)
    .body(ByteStream::from(part_data))
    .send()
    .await?;

// 完成多部分上传
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
```

注意与行为说明：
- SSE-C：不支持用于多部分上传。若需要对大对象加密，请使用 SSE-S3 或 SSE-KMS。
- SSE-KMS/SSE-S3：
  - 如果在 CreateMultipartUpload 请求中显式指定算法，则按请求算法加密并在 CompleteMultipartUpload 的响应中返回相应的 SSE 头：
    - `x-amz-server-side-encryption: AES256 | aws:kms`
    - 当为 KMS 时，额外返回 `x-amz-server-side-encryption-aws-kms-key-id`。
  - 如果未在请求中指定算法但 Bucket 配置了默认加密，则在完成合并并进行最终加密后，同样会在 CompleteMultipartUpload 响应中返回上述 SSE 响应头。
  - 若客户端在请求中使用 `aws:kms:dsse`，服务端会接受但在响应中规范化为 `aws:kms`。

示例：使用 curl 创建带 SSE-KMS 的多部分上传（片段略），最终完成时会在响应头中看到 `x-amz-server-side-encryption: aws:kms` 与对应的 `x-amz-server-side-encryption-aws-kms-key-id`。

```bash
# 仅展示创建 MPU 与完成 MPU 的关键头；上传分片步骤从略
CREATE_XML='{"Bucket":"my-bucket","Key":"large-object"}'
curl -i -X POST "http://localhost:9000/my-bucket/large-object?uploads" \
  -H "x-amz-server-side-encryption: aws:kms" \
  -H "x-amz-server-side-encryption-aws-kms-key-id: my-kms-key-id"

# ... 上传若干分片后，完成 MPU：
curl -i -X POST "http://localhost:9000/my-bucket/large-object?uploadId=UPLOAD_ID" \
  -H "Content-Type: application/xml" \
  --data-binary @complete.xml
# 响应头将包含：
# x-amz-server-side-encryption: aws:kms
# x-amz-server-side-encryption-aws-kms-key-id: my-kms-key-id
```

## 对象拷贝（COPY）与 SSE

当从一个对象拷贝到另一个对象时：
- 若源对象使用 SSE-C 加密，必须提供源对象的 SSE-C 头以便解密：
  - `x-amz-copy-source-server-side-encryption-customer-algorithm`
  - `x-amz-copy-source-server-side-encryption-customer-key`
  - `x-amz-copy-source-server-side-encryption-customer-key-MD5`
- 目标对象的加密由请求头或 Bucket 默认策略决定（SSE-S3 或 SSE-KMS）。

示例：使用 SSE-C 源对象拷贝并将目标以 SSE-KMS 写入。

```bash
SRC_KEY="MTIzNDU2Nzg5MGFiY2RlZjEyMzQ1Njc4OTBhYmNkZWY="
SRC_KEY_MD5="$(echo -n "1234567890abcdef1234567890abcdef" | md5 | xxd -r -p | base64)"

curl -i -X PUT "http://localhost:9000/my-bucket/obj-copy" \
  -H "x-amz-copy-source: /my-bucket/obj-src" \
  -H "x-amz-copy-source-server-side-encryption-customer-algorithm: AES256" \
  -H "x-amz-copy-source-server-side-encryption-customer-key: $SRC_KEY" \
  -H "x-amz-copy-source-server-side-encryption-customer-key-MD5: $SRC_KEY_MD5" \
  -H "x-amz-server-side-encryption: aws:kms" \
  -H "x-amz-server-side-encryption-aws-kms-key-id: my-kms-key-id"
# 响应将回显目标对象的 SSE 头，算法为 aws:kms，且包含 KMS KeyId。
```

## 查看对象加密信息

```rust
// 获取对象元数据，包括加密信息
let response = client
    .head_object()
    .bucket("my-bucket")
    .key("my-object")
    .send()
    .await?;

// 检查加密类型
if let Some(encryption) = response.server_side_encryption() {
    println!("Encryption type: {:?}", encryption);
}

// 检查 KMS 密钥 ID（如果使用 SSE-KMS）
if let Some(key_id) = response.ssekms_key_id() {
    println!("KMS Key ID: {}", key_id);
}
```

## 最佳实践

### 1. 选择合适的加密方式
- **SSE-S3**: 适用于大多数场景，简单易用
- **SSE-KMS**: 需要密钥审计和细粒度控制时使用
- **SSE-C**: 需要完全控制密钥时使用

### 2. 密钥管理
- 定期轮换 KMS 密钥
- 为不同的应用或环境使用不同的密钥
- 备份重要的客户提供密钥

### 3. 性能考虑
- 加密会增加少量的 CPU 开销
- SSE-S3 和 SSE-KMS 的性能差异很小
- SSE-C 需要在每次请求时传输密钥

### 4. 安全建议
- 在生产环境中始终使用 HTTPS
- 定期审计加密配置
- 监控 KMS 密钥的使用情况
- 为敏感数据使用 SSE-KMS 或 SSE-C

### 5. 兼容性
- RustFS 完全兼容 AWS S3 的加密 API
- 可以使用任何 S3 兼容的客户端库
- 支持所有标准的 S3 加密头部
  - 接受 `aws:kms:dsse` 作为请求算法，但在响应与对象元数据中统一显示为 `aws:kms`

## 故障排除

### 常见错误

1. **KMS 密钥不存在**
   ```
   Error: The specified KMS key does not exist
   ```
   解决方案：确保 KMS 密钥已创建并且可访问

2. **SSE-C 密钥格式错误**
   ```
   Error: The encryption key provided is not valid
   ```
   解决方案：确保密钥是 32 字节且正确 Base64 编码

3. **权限不足**
   ```
   Error: Access denied to KMS key
   ```
   解决方案：检查 IAM 权限和 KMS 密钥策略

### 调试技巧

1. 使用 `head_object` 检查对象的加密状态
2. 检查服务器日志中的加密相关错误
3. 验证 KMS 服务的健康状态
4. 确保客户端和服务器的时间同步

## 示例代码仓库

完整的测试示例可以在以下位置找到：
- `crates/e2e_test/src/kms/s3_encryption.rs` - 包含所有加密方式的完整测试用例

这些示例展示了如何在实际应用中使用各种加密功能。