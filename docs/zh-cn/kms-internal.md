# RustFS KMS/SSE 内部设计与实现概要

本文档面向开发者，描述 RustFS 在对象加密（SSE）与 KMS 集成方面的内部设计、数据模型、关键流程与错误语义。与用户手册（kms.md）互补，帮助理解实现细节与后续演进。

## 设计目标

- 与 S3 行为对齐：SSE-S3、SSE-KMS（接受 aws:kms:dsse）、SSE-C；multipart 完成响应包含正确 SSE 头。
- 安全收敛：仅内部持久化“密封元数据”（sealed metadata），避免对外暴露敏感分量；HEAD/GET 不透出内部字段。
- 简化使用：客户端只需标准 SSE 头；GET 无需额外上下文；桶默认加密自动生效。
- 低耦合：加密/解密通过 ObjectEncryptionService 与存储层解耦；支持 Vault 与 Local 两种 KMS。

## 组件与职责

- ObjectEncryptionService（crates/kms）
  - 负责数据密钥（DEK）生成、包裹与解包、流式加/解密（AES-256-GCM）。
  - 统一 KMS 交互：Vault/Local 实现 KmsManager trait。
  - 规范化 DSSE：输入 aws:kms:dsse 归一化为 aws:kms。
- 存储层（rustfs/src/storage/ecfs.rs）
  - 在 PUT/COPY/multipart 完成时调用加密；GET/COPY 源端调用解密。
  - 负责元数据读写与过滤；透出标准 SSE 头，隐藏内部密封元数据。
- 管理端（rustfs/src/admin/handlers/kms.rs）
  - KMS 配置、键管理、批量 rewrap；rewrap 直接读取/写回内部密封字段。

## 数据模型

- 对外公共头（持久化且可见）
  - x-amz-server-side-encryption: AES256 | aws:kms（输入可接受 aws:kms:dsse，存储/返回为 aws:kms）
  - x-amz-server-side-encryption-aws-kms-key-id: <key-id>（SSE-KMS 时存在）
- 内部密封元数据（持久化且隐藏，前缀 x-rustfs-internal-）
  - x-rustfs-internal-sse-key: base64(包裹 DEK，含 key_id 头)
  - x-rustfs-internal-sse-iv: base64(IV)
  - x-rustfs-internal-sse-tag: base64(GCM TAG)
  - x-rustfs-internal-sse-context: JSON（最终 AAD）
    - 至少包含 bucket、key；可合并请求头提供的自定义字段。

说明
- SSE-C 不持久化用户密钥，仅持久化 IV 于内部字段；公开算法头为 AES256。
- HEAD/List 过滤所有内部前缀字段；仅保留公共 SSE 头对外。

## 关键流程

### PUT（单对象）
1) 解析 SSE 头：SSE-S3 或 SSE-KMS（可含 key-id、context JSON）；SSE-C 单独校验 key/MD5。
2) 构造 AAD：包含 bucket、key，合并 x-amz-server-side-encryption-context（如提供）。
3) 生成 DEK 并加密数据流（AES-256-GCM）。
4) 写入对象：
   - 对外：算法头 +（可选）KMS KeyId。
   - 内部：sse-key/iv/tag/context 四元组。

### GET
1) 判断 SSE-C：内部仅有 sse-iv 而无 sse-key（或遗留 IV 公开字段）→ 需要客户提供 SSE-C 头。
2) 否则从内部密封元数据获取包裹 DEK 与 AAD，通过 KMS 解包并解密数据流。

### COPY
- 源端：与 GET 相同逻辑解密（SSE-C 需 x-amz-copy-source-server-side-encryption-customer-*）。
- 目标端：与 PUT 相同逻辑加密，可重新选择 SSE 算法与 KMS KeyId。

### Multipart
- CreateMultipartUpload：记录算法与（可选）KMS KeyId，不持久化公开 context；标记 x-amz-multipart-encryption-pending。
- UploadPart：按常规写入分片内容（此实现最终按整对象密封）。
- CompleteMultipartUpload：合并对象→以 PUT 流程对“整对象”加密→写内部密封元数据→响应包含 SSE 头（及 KeyId，如适用）。
- SSE-C：当前不支持 multipart（与用户文档一致）。

## KMS 交互

- Vault Transit
  - 使用 datakey/plaintext 生成明文 DEK 与包裹密钥；decrypt/rewrap 用于回退或工具路径。
  - 惰性创建 default_key_id（默认 rustfs-default-key），策略禁止时将失败但不阻断非 KMS 路径。
- Local KMS
  - 开发/测试用途，接口与 Vault 对齐。

## 错误与边界

- 内部密封字段缺失：解密失败（GET/COPY 源）。
- SSE-C 缺少 key/MD5：请求无效。
- KMS 不可达：加密/解密/rewrap 相关操作失败；状态接口报告 Failed。
- AAD 不匹配：解包失败；检查 bucket/key 与自定义 AAD 的一致性。

## 兼容性

- 移除 legacy 公开字段回退：不再从 x-amz-server-side-encryption-{key,iv,tag,context-*} 解密。
- 批量 rewrap：读取内部 sse-key 与 sse-context，写回新的 sse-key（保持密文格式与 key_id 头）。

## 测试要点

- KMS 单测：内部密封往返成功；仅提供 legacy 字段时失败。
- 存储层：
  - PUT/GET/COPY SSE-C 与 SSE-KMS/S3 路径；
  - multipart 完成响应包含 SSE 头；
  - HEAD 不含内部密封字段。

## 后续演进

- KMS 调用增加退避与指标；
- SSE-C 的 multipart 支持（保持内部密封一致性）。
