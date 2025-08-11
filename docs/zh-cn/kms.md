# RustFS KMS 与服务端加密（SSE）

本文介绍如何在 RustFS 中配置 KMS、管理密钥，以及在对象上使用 S3 兼容的服务端加密（SSE）。包含 Vault Transit 与本地后端、加密上下文（AAD）以及常用 curl 示例。

## 总览

- 后端：Vault Transit（生产推荐）、Local（开发测试）。
- 默认：
  - Vault Transit 的挂载路径默认使用 transit。
  - 对象数据实际使用 AES-256-GCM 加密；KMS 管理数据密钥（DEK）。
- 加密上下文（AAD）：RustFS 会构建一个包含至少 bucket 与 key 的 JSON 上下文，将密文与对象身份绑定；也可通过请求头补充自定义上下文。

安全与认证
- 管理员接口（/rustfs/admin/v3/...）需要 AWS SigV4 签名认证。
- 请使用与 RustFS 服务端配置匹配的 AK/SK 对请求进行 SigV4 签名（服务名通常为 s3，区域可配置或使用默认）。
- 未签名或签名不合法会返回 403 AccessDenied。

## 配置 KMS

接口：
- POST /rustfs/admin/v3/kms/configure

### 请求体字段说明
- kms_type: 字符串，必填。可选值：
  - "vault"：使用 HashiCorp Vault Transit 引擎
  - "local"：使用内置本地 KMS（开发/测试）
- vault_address: 字符串，kms_type=vault 时必填。Vault 的 HTTP(S) 地址，例如 https://vault.example.com。
- vault_token: 字符串，可选。当提供该字段时使用 Token 认证。
- vault_app_role_id: 字符串，可选。与 vault_app_role_secret_id 一起提供时，使用 AppRole 认证。
- vault_app_role_secret_id: 字符串，可选。与 vault_app_role_id 一起提供时，使用 AppRole 认证。
- vault_namespace: 字符串，可选。Vault Enterprise 命名空间，不填表示根命名空间。
- vault_mount_path: 字符串，可选，默认 "transit"。Transit 引擎的挂载名（不是 KV 引擎路径）。
- vault_timeout_seconds: 整数，可选，默认 30。与 Vault 的请求超时时间（秒）。
- default_key_id: 字符串，可选。SSE-KMS 未显式指定密钥时的默认主密钥 ID；未设置时默认回退到 "rustfs-default-key" 并尽力惰性创建。

环境变量映射：
- RUSTFS_KMS_DEFAULT_KEY_ID → default_key_id

认证选择规则（自动）：
- 同时存在 vault_app_role_id 与 vault_app_role_secret_id → 使用 AppRole；
- 否则若存在 vault_token → 使用 Token；
- 其他情况 → 返回无效配置错误。

请求体（Vault + Token）：
```json
{
  "kms_type": "vault",
  "vault_address": "https://vault.example.com",
  "vault_token": "s.xxxxx",             
  "vault_namespace": "optional-namespace", 
  "vault_mount_path": "transit",        
  "vault_timeout_seconds": 30            
}
```

请求体（Vault + AppRole）：
```json
{
  "kms_type": "vault",
  "vault_address": "https://vault.example.com",
  "vault_app_role_id": "role-id",
  "vault_app_role_secret_id": "secret-id",
  "vault_mount_path": "transit"
}
```

请求体（Local）：
```json
{
  "kms_type": "local"
}
```

状态与健康检查：
- GET /rustfs/admin/v3/kms/status → { status: OK|Degraded|Failed, backend, healthy }
  - OK：KMS 可达，且能生成数据密钥
  - Degraded：KMS 可达，但加解密路径未完全验证
  - Failed：不可达
  - 说明：初次使用时即使尚无任何密钥，KMS 也会报告可用；Transit 未挂载或 Vault 被封存（sealed）时会报告失败。

响应示例：
```json
{
  "status": "OK",
  "backend": "vault",
  "healthy": true,
  "details": {
    "engine_type": "transit",
    "mount_path": "transit",
    "namespace": null
  }
}
```

## 密钥管理接口

- 创建密钥：POST /rustfs/admin/v3/kms/key/create?keyName=<id>[&algorithm=AES-256]
- 查询状态：GET /rustfs/admin/v3/kms/key/status?keyName=<id>
- 列表：GET /rustfs/admin/v3/kms/key/list
- 启用：PUT /rustfs/admin/v3/kms/key/enable?keyName=<id>
- 禁用：PUT /rustfs/admin/v3/kms/key/disable?keyName=<id>
  - Vault 限制：Transit 不支持禁用，RustFS 会返回 501 并给出说明。
- 轮换：POST /rustfs/admin/v3/kms/key/rotate?keyName=<id>
- 重包裹（rewrap）：POST /rustfs/admin/v3/kms/rewrap（请求体：{"ciphertext_b64":"...","context":{...}}）

### 参数与取值说明
- keyName: 字符串，必填。主密钥 ID（Transit key 名）。建议使用业务相关的可读 ID，例如 "app-default"。
- algorithm: 字符串，可选，默认 "AES-256"。可选值：
  - "AES-256"、"AES-128"、"RSA-2048"、"RSA-4096"
  - 提示：在 Vault Transit 中，实际的 key_type 由引擎配置/默认值决定（通常为 aes256-gcm96）。当前 RustFS 会尽力对齐，但以 Vault 的实际 key 定义为准；algorithm 主要用于元数据和一致性校验。

### 接口细节
- 创建密钥（key/create）
  - 成功返回创建的密钥信息；若密钥已存在，可能返回已存在错误或视后端而定。
  - Vault 后端可由策略限制创建权限；若无权限，请预先由管理员创建。
  - 方法与路径：POST /rustfs/admin/v3/kms/key/create
  - 查询参数：
    - keyName: 字符串，必填
    - algorithm: 字符串，可选，默认 "AES-256"
  - 响应字段：
    - key_id: 字符串，主密钥 ID
    - key_name: 字符串（同 key_id）
    - status: 字符串，Active 等
    - created_at: 字符串，ISO 时间
  - 成功示例：
    ```json
    {"key_id":"app-default","key_name":"app-default","status":"Active","created_at":"2025-08-11T09:30:21Z"}
    ```
- 查询状态（key/status）
  - 读取指定密钥的基本信息与状态；密钥不存在时返回 404。
  - 方法与路径：GET /rustfs/admin/v3/kms/key/status
  - 查询参数：
    - keyName: 字符串，必填
  - 响应字段：
    - key_id, key_name, status, created_at, algorithm
- 列表（key/list）
  - 返回当前挂载下的所有密钥；当 Vault 下尚无任何密钥时，返回空列表（而不是错误）。
  - 方法与路径：GET /rustfs/admin/v3/kms/key/list
  - 响应字段：
    - keys: 数组，元素为 { key_id, algorithm, status, created_at }
- 启用（key/enable）
  - Transit 不支持显式启用/禁用；该操作会确保目标密钥存在（必要时惰性创建）。
  - 方法与路径：PUT /rustfs/admin/v3/kms/key/enable
  - 查询参数：
    - keyName: 字符串，必填
- 禁用（key/disable）
  - Transit 不支持禁用，返回 501；可改用策略限制或轮换替代。
  - 方法与路径：PUT /rustfs/admin/v3/kms/key/disable
- 轮换（key/rotate）
  - 将密钥提升到下一版本；已有密文可通过 rewrap 升级到新版本。
  - 方法与路径：POST /rustfs/admin/v3/kms/key/rotate
  - 查询参数：
    - keyName: 字符串，必填
- 重包裹（rewrap）
  - 请求体字段：
    - ciphertext_b64: 字符串，必填。对象元数据中保存的包装 DEK（x-amz-server-side-encryption-key）的 base64 值。
    - context: 对象，可选。加密上下文（AAD），建议至少包含 bucket 与 key，与写入时一致。
  - 成功返回新的包装 DEK（base64），保持与旧格式兼容。
  - 方法与路径：POST /rustfs/admin/v3/kms/rewrap
  - 响应示例：
    ```json
    {"ciphertext_b64":"dmF1bHQ6djI6Li4u"}
    ```

### 错误返回格式
管理员接口错误均返回 JSON：
```json
{"code":"InvalidConfiguration","message":"Failed to create KMS manager","description":"Error: ..."}
```
常见 code：
- AccessDenied（签名错误或权限不足）
- InvalidConfiguration（配置参数不合法、缺失或后端不可达）
- NotFound（key/status 等查询的密钥不存在）
- NotImplemented（禁用等不被 Transit 支持的操作）

说明
- RustFS 使用 Vault Transit 引擎进行 KMS 操作（encrypt/decrypt/rewrap、datakey/plaintext）。请确保已启用并挂载 Transit（默认路径 transit）。
- 不支持 KV 引擎路径（例如 secret/data/...），也没有 vault_key_path 参数；若提供该字段会被忽略。
- 配置中无需显式提供 vault_auth_method；当包含 vault_token 时使用 Token 认证；当包含 vault_app_role_id 与 vault_app_role_secret_id 时使用 AppRole 认证。多余字段会被忽略。
- 包装的 DEK 会带一个小的 key_id 头，便于后续解密自动选取正确密钥。

## 对象上的 SSE 使用

RustFS 支持在 PUT 时使用 SSE-S3（AES256）与 SSE-KMS（aws:kms）请求头。对象数据由 DEK（AES-256-GCM）加密，相关参数与包装 DEK 存储在对象元数据中。

PUT 可选请求头：
- SSE-S3：x-amz-server-side-encryption: AES256
- SSE-KMS：x-amz-server-side-encryption: aws:kms
- 指定主密钥：x-amz-server-side-encryption-aws-kms-key-id: <key-id>
- 自定义加密上下文（JSON）：x-amz-server-side-encryption-context: {"project":"demo","tenant":"t1"}

SSE-C（客户提供密钥）请求头（单次 PUT/GET/COPY 支持）：
- x-amz-server-side-encryption-customer-algorithm: AES256
- x-amz-server-side-encryption-customer-key: <Base64-encoded 256-bit key>
- x-amz-server-side-encryption-customer-key-MD5: <Base64-encoded MD5(key)>

说明
- SSE-C 需要通过 HTTPS 传输（强烈建议，避免明文密钥泄露）。RustFS 不持久化用户提供的密钥，仅持久化算法与随机 IV；读取时必须再次通过请求头提供相同密钥。
- 对 COPY：可用 x-amz-copy-source-server-side-encryption-customer-* 头解密源对象，目标端可用 SSE-S3/SSE-KMS 或 SSE-C 重新加密。
- Multipart（分片上传）：当前不支持 SSE-C；请使用单次 PUT 或 COPY 流程。

约束与取值
- x-amz-server-side-encryption 可选值：AES256（SSE-S3）、aws:kms（SSE-KMS）。
- x-amz-server-side-encryption-aws-kms-key-id 建议使用已存在或可惰性创建的主密钥名。
- x-amz-server-side-encryption-context 为 JSON 文本，建议 UTF-8，无换行；过大上下文会增加元数据存储开销。

DSSE 兼容
- 接受 aws:kms:dsse 作为 x-amz-server-side-encryption 的值；服务端将其归一化为 aws:kms，并在响应/HEAD 中返回 aws:kms。

密钥选择
- 若提供 x-amz-server-side-encryption-aws-kms-key-id，则使用该密钥。
- 否则使用 KMS 配置中的 default_key_id；若未配置，回退到 “rustfs-default-key”，并尽力自动创建（失败不阻断写入流程）。

加密上下文（AAD）
- 若通过 x-amz-server-side-encryption-context 传入 JSON，将与默认上下文合并；RustFS 会以 x-amz-server-side-encryption-context-<k> 的形式逐项写入对象元数据。
- RustFS 始终包含 bucket 与 key 字段，使密文与对象身份绑定。
- GET 时，RustFS 会从元数据重建上下文并透明解密；客户端无需额外头即可读取。

持久化的加密元数据（由服务端管理）：
- x-amz-server-side-encryption-key：base64 的包装 DEK
- x-amz-server-side-encryption-iv：base64 IV
- x-amz-server-side-encryption-tag：base64 AEAD 标签（GCM）
- x-amz-server-side-encryption-context-*：逐项 AAD（例如 …-context-bucket、…-context-key、…-context-project）

桶默认加密与分片行为
- 若桶配置了默认加密（SSE-S3 或 SSE-KMS），当请求未显式携带 SSE 头时，将使用桶默认加密。
- 分片上传时：CreateMultipartUpload 会记录加密意图；CompleteMultipartUpload 将在响应中返回相应的 SSE 头（以及 KMS KeyId，如果适用），确保与 MinIO/S3 行为一致。
- 目前分片 + SSE-C 不支持。

## curl 示例

配置 Vault KMS（token）：
```bash
curl -sS -X POST \
  http://127.0.0.1:9000/rustfs/admin/v3/kms/configure \
  -H 'Content-Type: application/json' \
  -d '{
    "kms_type":"vault",
    "vault_address":"https://vault.example.com",
    "vault_token":"s.xxxxx",
  "vault_mount_path":"transit"
  }'
```

注意（参数校对）
- 支持字段：kms_type、vault_address、vault_token、vault_namespace、vault_mount_path、vault_timeout_seconds、vault_app_role_id、vault_app_role_secret_id。
- 不支持字段：vault_key_path、vault_auth_method（若出现会被忽略）。若你的 Transit 非默认挂载，请设置 vault_mount_path 为实际挂载名；例如 custom-transit。
- 若你的 Vault 只有 KV 引擎（如 secret/…），请先启用 Transit 引擎，再配置上述参数。

创建密钥：
```bash
curl -sS -X POST 'http://127.0.0.1:9000/rustfs/admin/v3/kms/key/create?keyName=app-default&algorithm=AES-256'
```

轮换密钥：
```bash
curl -sS -X POST 'http://127.0.0.1:9000/rustfs/admin/v3/kms/key/rotate?keyName=app-default'
```

SSE-S3（AES256）上传：
```bash
curl -sS -X PUT 'http://127.0.0.1:9000/bucket1/hello.txt' \
  -H 'x-amz-server-side-encryption: AES256' \
  --data-binary @./hello.txt
```

SSE-KMS 携带上下文上传：
```bash
curl -sS -X PUT 'http://127.0.0.1:9000/bucket1/secret.txt' \
  -H 'x-amz-server-side-encryption: aws:kms' \
  -H 'x-amz-server-side-encryption-aws-kms-key-id: app-default' \
  -H 'x-amz-server-side-encryption-context: {"project":"demo","env":"staging"}' \
  --data-binary @./secret.txt
```

SSE-C 上传（单次 PUT）：
```bash
curl -sS -X PUT 'http://127.0.0.1:9000/bucket1/private.txt' \
  -H 'x-amz-server-side-encryption-customer-algorithm: AES256' \
  -H "x-amz-server-side-encryption-customer-key: $(printf '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef' | xxd -r -p | base64)" \
  -H "x-amz-server-side-encryption-customer-key-MD5: $(printf '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef' | xxd -r -p | md5 | awk '{print $1}' | xxd -r -p | base64)" \
  --data-binary @./private.txt
```

SSE-C 读取（GET）：
```bash
curl -sS 'http://127.0.0.1:9000/bucket1/private.txt' \
  -H 'x-amz-server-side-encryption-customer-algorithm: AES256' \
  -H "x-amz-server-side-encryption-customer-key: <Base64Key>" \
  -H "x-amz-server-side-encryption-customer-key-MD5: <Base64MD5>" \
  -o ./private.out
```

SSE-C 源 + SSE-KMS 目标的 COPY：
```bash
curl -sS -X PUT 'http://127.0.0.1:9000/bucket1/copied.txt' \
  -H 'x-amz-copy-source: /bucket1/private.txt' \
  -H 'x-amz-copy-source-server-side-encryption-customer-algorithm: AES256' \
  -H "x-amz-copy-source-server-side-encryption-customer-key: <Base64Key>" \
  -H "x-amz-copy-source-server-side-encryption-customer-key-MD5: <Base64MD5>" \
  -H 'x-amz-server-side-encryption: aws:kms' \
  -H 'x-amz-server-side-encryption-aws-kms-key-id: app-default'
```

读取（自动解密）：
```bash
curl -sS 'http://127.0.0.1:9000/bucket1/secret.txt' -o ./secret.out
```

重包裹包装 DEK（管理员接口）：
```bash
curl -sS -X POST 'http://127.0.0.1:9000/rustfs/admin/v3/kms/rewrap' \
  -H 'Content-Type: application/json' \
  -d '{"ciphertext_b64":"<base64-of-wrapped-dek>","context":{"bucket":"bucket1","key":"secret.txt"}}'
```

## 批量重包裹（Batch Rewrap）

使用该管理员接口，可对指定桶/前缀范围内的所有对象包装 DEK 进行重包裹，以升级到 KMS 最新的密钥版本。支持干跑、分页与非递归列举。

- 接口：POST /rustfs/admin/v3/kms/rewrap-bucket
- 请求体字段：
  - bucket：字符串（必填）
  - prefix：字符串（可选）
  - recursive：布尔（默认 true）
  - page_size：整数 1..=1000（默认 1000）
  - max_objects：整数（可选，用于限制本次处理的对象数）
  - dry_run：布尔（默认 false）

约束与返回
- dry_run=true 时不落盘，仅返回统计信息，例如 { matched, would_rewrap, errors }。
- 实跑返回 { rewrapped, failed, errors }；errors 为数组，元素包含 { key, error }。
- 建议分批、前缀切片与限额执行，避免一次性处理过大数据集。

说明
- 重包裹会保留密文格式（包含嵌入的 key_id 头）。
- 对 Vault，会使用对象元数据中保存的加密上下文（AAD）进行验证。
- 当 dry_run=true 时不会写回元数据，仅统计本次将会重包裹的对象数。

示例（干跑，递归）：
```bash
curl -sS -X POST 'http://127.0.0.1:9000/rustfs/admin/v3/kms/rewrap-bucket' \
  -H 'Content-Type: application/json' \
  -d '{
    "bucket":"bucket1",
    "prefix":"tenant-a/",
    "recursive":true,
    "page_size":1000,
    "dry_run":true
  }'
```

示例（非递归、限量处理 200 个对象）：
```bash
curl -sS -X POST 'http://127.0.0.1:9000/rustfs/admin/v3/kms/rewrap-bucket' \
  -H 'Content-Type: application/json' \
  -d '{
    "bucket":"bucket1",
    "prefix":"tenant-a/",
    "recursive":false,
    "page_size":500,
    "max_objects":200,
    "dry_run":false
  }'
```

## 运行手册：密钥轮换 + 批量重包裹

1) 轮换主密钥版本（管理员）
```bash
curl -sS -X POST 'http://127.0.0.1:9000/rustfs/admin/v3/kms/key/rotate?keyName=app-default'
```
2) 干跑评估受影响对象
```bash
curl -sS -X POST 'http://127.0.0.1:9000/rustfs/admin/v3/kms/rewrap-bucket' \
  -H 'Content-Type: application/json' \
  -d '{"bucket":"bucket1","prefix":"tenant-a/","dry_run":true}'
```
3) 分批执行重包裹（可按前缀/限额分段）
```bash
curl -sS -X POST 'http://127.0.0.1:9000/rustfs/admin/v3/kms/rewrap-bucket' \
  -H 'Content-Type: application/json' \
  -d '{"bucket":"bucket1","prefix":"tenant-a/","page_size":1000,"max_objects":500}'
```
4) 采样验证：随机抽取对象 GET，核对内容与元数据（SSE 相关字段、wrapped DEK 已更新）。

注意
- 先 dry_run，再实跑；分段执行有利于控制风险与负载。
- 大规模时建议按业务前缀切分多次执行；page_size 500~1000 通常较稳。
- 执行前后监控 KMS 可用性（/v3/kms/status）与错误项列表（响应中的 errors）。

## 权限要求（Vault Transit）

对使用中的密钥（例如 app-default）至少需要以下能力：
- transit/datakey/plaintext（生成明文 DEK 与包装密钥）
- transit/encrypt、transit/decrypt（用于回退和工具路径）
- transit/rewrap（将密文就地更新到最新密钥版本）

示例（概念性策略片段，需按实际 mount 路径与 key 名替换）：
```hcl
path "transit/datakey/plaintext/app-default" { capabilities = ["update"] }
path "transit/encrypt/app-default"        { capabilities = ["update"] }
path "transit/decrypt/app-default"        { capabilities = ["update"] }
path "transit/rewrap/app-default"         { capabilities = ["update"] }
```

补充（最小权限与初始化）
- 初始状态无任何密钥时：
  - 列表接口会返回空列表；
  - 首次写入/生成数据密钥时，若策略允许，RustFS 会惰性创建指定的主密钥；
  - 若策略禁止创建，请管理员预先创建主密钥，或为特定 key 下发 create 权限。
- Transit 必须已挂载到配置的 vault_mount_path（默认 transit），否则会在状态/首次使用时报错。


## 故障排查

- KMS 状态 Failed：检查地址/认证（token 或 approle），确认 Transit 引擎已启用并挂载在正确路径（默认 transit）。
- datakey/plaintext 被拒：调整 Vault 策略允许对该 key 进行 transit generate。
- Vault 不支持禁用：可通过策略禁止使用、或轮换/移除密钥替代。
- rewrap-bucket 返回 errors：逐条查看 key 与 error 字段；可先缩小 prefix 或降低 page_size 重试。
- GET 失败（解密错误）：检查对象元数据中的 context-* 是否完整、bucket/key 是否存在于上下文，确认 KMS 策略允许带 AAD 的操作。

## 规划

- 为 KMS 调用增加有限重试/退避与指标上报。
- 更丰富的管理端示例与 UX。
