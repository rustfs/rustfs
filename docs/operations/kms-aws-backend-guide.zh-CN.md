# AWS KMS 后端：接入、验证与运维

**适用场景：** 首次把 RustFS 接入 AWS KMS、验证已有的 AWS 后端部署、从 1.0.4 或 1.0.5 升级，或者规划如何运维并保护 RustFS 所依赖的 AWS 密钥。
**事实来源：** `crates/kms/src/backends/aws.rs`（后端行为及其差异）、`crates/kms/src/config.rs`（`ENV_KMS_AWS_*`）、`crates/kms/src/policy.rs`（重试、超时与熔断）、`rustfs/src/storage/sse.rs`（SSE 数据面）。English: [kms-aws-backend-guide.md](kms-aws-backend-guide.md)。

本页关于行为的描述，均来自对真实 AWS KMS 服务（区域 `eu-north-1`）的实测；对于无法让 AWS 按需产生的故障，则使用一个实现了 AWS KMS 线上协议的本机替身来实测。属于未经实测的建议之处，文中会明确标注。AWS 与其它后端在契约层面的差异，见 [KMS backend security properties](kms-backend-security.md#aws-kms-deviations-from-the-shared-backend-contract)。

## RustFS 如何使用 AWS KMS

RustFS 采用信封加密。每个加密对象写入时，RustFS 向 AWS KMS 申请一把数据密钥（`GenerateDataKey`），用明文数据密钥加密对象，只把被包裹的数据密钥存在对象旁边。读取时再把被包裹的密钥发回 AWS（`Decrypt`）。KMS 密钥本身从不离开 AWS，RustFS 也从不保存 AWS 凭据。

由此得出贯穿全文的三点：

- **AWS KMS 在读路径上。** 密钥被禁用、被计划删除、被删除，或 AWS 不可达时，加密对象无法读取。RustFS 中存储的任何内容都替代不了这把密钥。
- **每次加密写入、每次加密读取，都是一次 AWS 请求。** 多段上传每上传一段还会多一次 `Decrypt`。请据此估算请求量与费用。
- **`aws:kms` 与 `AES256` 对象都依赖这把密钥。** 配置了 KMS 后端后，SSE-S3（`AES256`）的数据密钥由默认密钥包裹，因此 SSE-S3 对象对 AWS 的依赖与 SSE-KMS 对象完全相同。

## 开始之前

| 项目 | 要求 |
| --- | --- |
| RustFS 版本 | 包含 AWS 读路径修复的版本。1.0.4 与 1.0.5 能写不能读 AWS 后端的对象，见[从 1.0.4 或 1.0.5 升级](#从-104-或-105-升级)。 |
| KMS 密钥 | 对称密钥，用途为"加密和解密"，位于 RustFS 将要配置的区域。 |
| 网络 | 每个 RustFS 节点都要能通过 HTTPS 访问 `kms.<region>.amazonaws.com`（或 KMS 的 VPC 接口终端节点）。RustFS 内置的 AWS SDK 不使用 `HTTP_PROXY` / `HTTPS_PROXY`。 |
| 身份 | 每套部署一个 IAM 身份，且每个节点都能取得它的凭据。 |
| 时钟 | 各节点时钟同步；签名时间偏差超过几分钟的请求会被 AWS 拒绝。 |

## 第一步：创建 KMS 密钥

在 AWS 控制台：**KMS → 客户管理的密钥 → 创建密钥**。

1. 密钥类型选**对称**，密钥用途选**加密和解密**。"高级选项"保持默认（密钥材料来源 KMS、单区域），除非你已决定用多区域密钥做灾备。
2. 别名与描述：只给人看。RustFS 通过**密钥 ID 或 ARN** 寻址，从不使用别名。
3. **密钥管理员**：只勾选负责管理这把密钥（禁用、轮换、计划删除）的人员角色。不要勾选 AWS 服务关联角色（`AWSServiceRoleFor…`）和组织托管角色。只有当这些管理员确实应当拥有删除权时，才保留"允许密钥管理员删除此密钥"。
4. **密钥用户**：勾选 RustFS 运行所用的 IAM 身份（第二步创建，可以回头再添加）。这会通过密钥策略授予该密钥上的 `kms:Encrypt`、`kms:Decrypt`、`kms:ReEncrypt*`、`kms:GenerateDataKey*` 和 `kms:DescribeKey`。
5. 完成后记下**区域**和**密钥 ARN**。

管理员与用户应当是不同的身份。RustFS 运行所用的身份只需要使用密钥，不需要管理密钥。

## 第二步：创建 RustFS 的运行身份

优先使用短期的角色凭据，而不是长期访问密钥：

| RustFS 运行位置 | 推荐身份 |
| --- | --- |
| EC2 | 实例配置文件角色 |
| EKS | IRSA（服务账号 IAM 角色）或 EKS Pod Identity |
| ECS | 任务角色 |
| AWS 之外 | 专用 IAM 用户的访问密钥（定期轮换），或 IAM Roles Anywhere |

RustFS 通过标准的 AWS 凭据链解析凭据：环境变量、共享 profile（`AWS_PROFILE`、`~/.aws/credentials`），然后是容器与实例元数据。RustFS 不会持久化凭据，KMS admin API 也会拒绝配置体中的凭据字段。

为该身份附加下面的身份策略。`kms:ListKeys` 不针对某一把密钥，密钥策略无法授予；而 RustFS 启动时用它做健康检查，缺少它 KMS 后端无法启动。

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "RustfsKmsHealthCheck",
      "Effect": "Allow",
      "Action": "kms:ListKeys",
      "Resource": "*"
    }
  ]
}
```

如果不想依赖控制台的"密钥用户"列表，可以把数据面权限写进同一份身份策略，并限定到具体密钥：

```json
{
  "Sid": "RustfsKmsDataPath",
  "Effect": "Allow",
  "Action": ["kms:GenerateDataKey", "kms:Decrypt", "kms:DescribeKey"],
  "Resource": "arn:aws:kms:<region>:<account-id>:key/<key-id>"
}
```

只有当运维人员要**通过 RustFS 的 admin API** 管理 AWS 密钥时，该身份才需要下面的管理权限。多数部署应当不授予这些权限，直接在 AWS 中管理密钥，这样即使某个 RustFS 节点被攻破，也无法禁用或删除密钥。

| RustFS admin 操作 | AWS 权限 |
| --- | --- |
| 创建密钥 | `kms:CreateKey` |
| 启用 / 禁用 | `kms:EnableKey`、`kms:DisableKey` |
| 轮换 | `kms:RotateKeyOnDemand` |
| 计划 / 取消删除 | `kms:ScheduleKeyDeletion`、`kms:CancelKeyDeletion` |

## 第三步：配置 RustFS

在每个节点上设置并重启：

```bash
RUSTFS_KMS_ENABLE=true
RUSTFS_KMS_BACKEND=aws
RUSTFS_KMS_AWS_REGION=eu-north-1
RUSTFS_KMS_DEFAULT_KEY_ID=arn:aws:kms:eu-north-1:<account-id>:key/<key-id>
```

说明：

- `RUSTFS_KMS_DEFAULT_KEY_ID` 是请求要求加密但未指定密钥时使用的密钥，也是包裹所有 SSE-S3 数据密钥的密钥。密钥 ID 和 ARN 两种写法都可以。
- **不要**设置 `RUSTFS_SSE_S3_MASTER_KEY`。它配置的是无 KMS 时的回退方案，KMS 后端启用后不起任何作用。
- `RUSTFS_KMS_AWS_ENDPOINT_URL` 仅用于模拟器和非默认端点。明文 `http://` 端点会被拒绝，除非设置 `RUSTFS_KMS_ALLOW_INSECURE_DEV_DEFAULTS=true`，而该开关只用于开发环境。使用 VPC 接口终端节点并开启私有 DNS 时，默认的区域域名已经解析到终端节点，无需额外设置。
- 同样的配置也可以在运行时通过 admin API 下发（`POST /rustfs/admin/v3/kms/configure`，请求体 `{"backend_type": "AWS", "region": "...", "default_key_id": "..."}`）。凭据仍然来自各节点自己的环境。

启动是失败关闭的。如果健康检查访问不到 AWS，或端点是明文 HTTP 又没有开发开关，RustFS 进程会在启动阶段直接退出，而不是在没有加密的状态下提供服务。

## 第四步：验证

写入、查看并读回一个对象。任意 S3 客户端均可：

```bash
aws s3api put-object --endpoint-url https://rustfs.example.com --bucket demo --key kms-check \
  --body ./sample.bin --server-side-encryption aws:kms
aws s3api head-object --endpoint-url https://rustfs.example.com --bucket demo --key kms-check
aws s3api get-object  --endpoint-url https://rustfs.example.com --bucket demo --key kms-check ./readback.bin
cmp ./sample.bin ./readback.bin
```

预期：三个调用全部成功，`head-object` 显示 `ServerSideEncryption: aws:kms` 以及你配置的密钥，读回的内容逐字节一致。

然后在 AWS 侧确认这次往返，以证明 RustFS 确实使用了这把密钥，而不是某种本地回退：在 **CloudTrail → 事件历史记录**中按事件源 `kms.amazonaws.com` 筛选。写入会产生 `GenerateDataKey`，读取会产生 `Decrypt`。每个事件都带有 RustFS 发送的加密上下文，其中一项把桶名映射到 `桶名/对象键`，因此 CloudTrail 能看出每次 KMS 调用服务的是哪个对象。**如果读取返回了数据却没有对应的 `Decrypt` 事件，或者报错且没有任何 `Decrypt` 尝试，说明这次读取根本没有到达 AWS。**

检查服务状态：

```bash
GET /rustfs/admin/v3/kms/status
```

`backend_status` 应为 `healthy`；`capabilities` 中 `rotate`、`enable_disable`、`schedule_deletion` 为 true，`versioning`、`physical_delete`、`rewrap` 为 false。

## 行为参考

以下为 AWS 后端的实测行为。状态码是 S3 客户端收到的结果。

### 密钥标识

| 情形 | 结果 |
| --- | --- |
| 在 `x-amz-server-side-encryption-aws-kms-key-id` 中用 ID 或 ARN 指定密钥 | 均可。响应以及之后的 `HEAD`/`GET` 会原样回显写入时使用的写法。 |
| 不存在的密钥，或其它区域的 ARN | `400 KMS.NotFoundException`，不会创建对象。 |
| 通过 RustFS 用自定义名称创建密钥（含兼容 `mc` 的 `create-key?key-id=`） | `501 NotImplemented`。密钥 ID 由 AWS 分配，创建时不要带名称，使用返回的 ID。 |
| 通过 `GET /rustfs/admin/v3/kms/keys/{key_id}` 用 ARN 描述密钥 | 把 ARN 百分号编码后放入路径段即可；`describe-key?keyId=` 形式则把它作为查询参数。 |

### 站点复制下与 Vault 站点配对

**交付约束**：站点复制原样同步密钥名称，但从不同步密钥本身，所以每个站点都必须有同名密钥（[详见](site-replication-operations.md)）。密钥 ID 由 AWS 分配，本后端也不管理别名，因此 Vault 与 AWS 配对时，必须由 Vault 站点跟随 AWS 的命名：

1. 先在 AWS 创建密钥，记下它的密钥 ID。
2. 在每个 Vault 站点（`vault-transit` 或 `vault-kv2`）创建一个以该裸密钥 ID 命名的密钥，例如 `74e5921e-107a-4584-bfda-a5dadeb3e13e`。
3. 所有站点（包括 AWS 站点）的 `RUSTFS_KMS_DEFAULT_KEY_ID` 都设为同一个裸密钥 ID。AWS 站点的默认密钥不要配成 ARN：同步出去的就是配置里的写法，而只有裸 ID 形式经过验证可以用作 Vault 密钥名。
4. 桶默认加密或 IAM 策略还会点名其它密钥时，对每个密钥重复第 1、2 步。

按此布局在 `vault-transit` + AWS 与 `vault-kv2` + AWS 上实测：点名密钥的桶默认加密，以及不点名密钥的 `aws:kms` 默认加密，在两个站点上都能写入并双向复制，每个副本由接收站点自己的 KMS 封装。未对齐时，接收站点返回 `400 KMS.NotFoundException`，相关对象的复制状态停在 `FAILED`。

### 密钥状态

| AWS 密钥状态 | `PUT`（加密） | `GET` / Range / 拷贝源（解密） | `HEAD` |
| --- | --- | --- | --- |
| 已启用 | 200 | 200 | 200 |
| 已禁用 | `400 InvalidRequest`（`DisabledException`） | `400 InvalidRequest`（`DisabledException`） | 200 |
| 待删除 | `400 InvalidRequest`（`KMSInvalidStateException`） | `400 InvalidRequest`（`KMSInvalidStateException`） | 200 |
| 已删除 | `400 KMS.NotFoundException` | 失败，数据无法恢复 | 200 |

这是与 RustFS 自管后端差别最大的一点：自管后端在密钥禁用后仍允许读取。而在 AWS 上，**禁用一把密钥会让它名下的所有对象下线，直到密钥重新启用**；重新启用后访问即恢复，RustFS 一侧不需要任何操作。AWS 的密钥状态变更可能需要几秒才对所有请求生效。

取消计划删除后，密钥在 AWS 中处于**已禁用**状态。必须显式启用，读写才会恢复。

### AWS 后端不提供的操作

| 操作 | 结果 | 原因 |
| --- | --- | --- |
| 立即删除（`force_immediate`） | `501` | AWS 只提供 7–30 天的等待期。 |
| 打标签、去标签、修改描述 | `501` | 该后端未实现，请在 AWS 中管理标签。 |
| 批量 rekey / rewrap | 拒绝 | AWS 的密钥版本对外不可见，AWS 会自动用正确的底层密钥解密。 |
| 导出 KMS 备份 | 不适用 | 密钥材料在 AWS 中，无法导出。 |
| SSE-S3 密钥自动创建、KMS 合成探测 | 不可用 | 两者都要用 RustFS 自选的名称寻址密钥。 |

### AWS 异常时

| 情况 | 客户端看到 | 说明 |
| --- | --- | --- |
| 单次被限流 | 200 | 自动重试，对客户端透明。 |
| 持续限流、AWS 返回 `5xx`，或端点不可达 | 不到一秒内返回 `503 ServiceUnavailable` | 可重试。被拒绝的写入不会留下残缺对象。 |
| AWS 返回 `AccessDeniedException` | `403 AccessDenied` | 通常是 IAM 或密钥策略缺少授权。 |
| 上述任一情况期间 | 未加密对象的读写、加密对象的 `HEAD` 不受影响 | 只有需要密钥的操作失败。 |
| AWS 恢复后 | 加密读写**无需重启**即可恢复，约 30 秒后 | 后端熔断器会保持断开 30 秒，然后才重新探测。 |

### 按密钥授权

RustFS 可以按密钥对 `kms:GenerateDataKey` / `kms:Decrypt` 做身份策略校验（`RUSTFS_KMS_ENFORCE_SSE_KEY_POLICY=true`，见 [Per-key KMS authorization](kms-per-key-authorization.md)）。策略资源请用**密钥 ID** 书写（`arn:aws:kms:::key/<key-id>`）。请求或已存储的对象可以用密钥 ID，也可以用 ARN 来指定密钥，两者都会按同一把密钥来判定，因此针对密钥 ID 的 `Allow` 或 `Deny` 对两种写法都生效。用别名指定密钥的请求会被当作"可能是任何一把密钥"：所有限定到具体密钥的 `Deny` 都对它生效，而限定到具体密钥的 `Allow` 都不生效，所以在使用按密钥策略的环境里，请求中请使用密钥 ID 或 ARN。AWS 侧的控制（密钥策略、IAM、加密上下文条件）仍然是最外层的边界，即使某个 RustFS 节点被攻破也依然有效。

## 从 1.0.4 或 1.0.5 升级

1.0.4 与 1.0.5 存储 AWS 数据密钥的形式，它们自己的读路径无法识别。在这两个版本上使用 AWS 后端时：

- `PUT` 成功，`HEAD` 显示 `aws:kms`，但 `GET` 返回 `400 InvalidRequest`，错误信息要求设置 `RUSTFS_SSE_S3_MASTER_KEY`。
- SSE-KMS 多段上传的 `UploadPart` 以同样的方式失败。
- CloudTrail 中写入有 `GenerateDataKey`，读取没有 `Decrypt`。

设置 `RUSTFS_SSE_S3_MASTER_KEY` 没有用，也不应该设置。

这两个版本写入的对象是完好的。升级到已修复的版本后，沿用原来的数据目录、密钥和凭据：

- 已有对象可以原样读回，Range 读取同样正确。不需要迁移、重写或重新上传。
- 对这类对象做一次原地的服务端拷贝，会把它改写成当前格式；这一步是可选的。
- 新对象按当前格式写入。**早于修复的版本读不了这些新对象**，所以用修复版本写入之后不要回滚；不过旧版本本来也读不了它们自己写的对象。

验证升级的方法：读取一个升级前写入的对象，并在 CloudTrail 中确认出现了 `Decrypt` 事件。

## 安全地运维 AWS KMS

密钥就是数据。下面每一条建议都由此而来。

### 防止密钥被删除或禁用

- **删除 KMS 密钥，等于销毁它加密的所有对象。** 等待期结束后，RustFS 和 AWS 都没有任何恢复手段。对待 `kms:ScheduleKeyDeletion` 权限，应当与对待"删除全部数据"的权限同等严格。
- 使用最长的等待期（30 天），并对"发起请求"而不是"完成删除"告警：为你的密钥上的 CloudTrail 事件 `ScheduleKeyDeletion` 和 `DisableKey` 建立 EventBridge 规则，推送到值班渠道。等待期只有在有人注意到的情况下才有意义。
- 用组织 SCP 或密钥策略中的显式 `Deny`，对除少数应急角色以外的所有人拒绝 `kms:ScheduleKeyDeletion` 和 `kms:DisableKey`。RustFS 的运行身份永远不应持有这两个权限。
- 退役一把密钥之前，先证明没有任何东西依赖它：先禁用，并观察至少一个完整业务周期内是否出现读取报 `400 InvalidRequest`（`DisabledException`）。禁用可以撤销，删除不能。
- 通过 RustFS admin API 计划删除密钥时，RustFS 会报告哪些桶的默认加密配置引用了它。这份报告覆盖的是配置，**不是对象**：报告为空并不代表没有对象在使用这把密钥。

### 最小权限与职责分离

- 三种角色，三个身份：密钥管理员（管理密钥，但不能使用）、RustFS 服务身份（使用密钥，但不能管理）、审计员（读取 CloudTrail 与密钥元数据）。
- 把服务身份限定到具体的密钥 ARN。不要使用 `kms:*`；除 `kms:ListKeys` 之外，不要使用 `Resource: "*"`。
- 不要把账户根用户或某位管理员的个人凭据用于服务。
- 如果一把密钥只应服务于某些桶，可以加上加密上下文条件。RustFS 把桶名作为加密上下文的键发送，因此可以用 `kms:EncryptionContextKeys` 把密钥限定到指定的桶，即使服务身份泄露也有效。这一项不在本页所依据的测试范围内，请先在非生产账户中验证。
- 按环境区分 AWS 密钥，并考虑按租户或数据等级区分。独立的密钥意味着独立的影响范围和独立的"总开关"。

### 凭据

- 优先使用实例、Pod 或任务角色。它们自动轮换，不在磁盘上留下任何秘密。
- 如果只能使用静态访问密钥：每套部署一把，以 `0600` 权限的文件或密钥管理服务保存，定期轮换，绝不写进 RustFS 配置、镜像或版本库。
- 每个节点都需要相同的权限和区域。某个节点权限不全时，它自己的请求会失败而其它节点成功，表现为间歇性错误。

### 轮换

- AWS 的自动密钥轮换对 RustFS 透明：AWS 保留所有历史底层密钥，并自动用正确的那一把解密旧的数据密钥。RustFS 既不会开启它，也不会报告它的状态，请在 AWS 中开启。
- 通过 RustFS 做按需轮换时调用的是 `RotateKeyOnDemand`。AWS 对每把密钥的按需轮换次数有上限，超出免费额度后还会计费；自动化之前请先核对 AWS 当前的限制。
- 轮换保护的是今后的数据密钥，不会重新加密已有的数据密钥或对象，而 RustFS 的批量 rekey 在该后端不可用。如果怀疑密钥泄露，应对方式是换一把新密钥并重写受影响的对象，而不是轮换。

### 可用性与灾备

- AWS KMS 的区域可用性从此成为你的存储可用性的一部分。AWS KMS 故障期间，加密读写返回 `503`，AWS 恢复约 30 秒后自动恢复，无需重启。
- RustFS 运行在 VPC 中时，为 KMS 使用 VPC 接口终端节点：这样不再依赖公网出口，并且可以用终端节点策略限制从该网络能使用哪些密钥和主体。
- 对于另一个区域的灾备站点，AWS 多区域密钥允许副本密钥解密由主密钥包裹的数据密钥。这一项不在本页所依据的测试范围内，依赖它之前请在灾备区域完整演练一次恢复。
- RustFS 复制会在目标端重新加密，因此每个站点使用各自的 KMS 配置。源站点的密钥丢失不影响已经复制过去的对象。

### 监控与审计

- 在所有用到的区域开启 CloudTrail 并保留 KMS 事件。每条 `GenerateDataKey` 和 `Decrypt` 都记录了调用者和加密上下文，后者指明了桶和对象。
- 对你的密钥上 `AccessDeniedException`、`DisabledException`、`KMSInvalidStateException`、`ThrottlingException` 的持续上升告警。
- RustFS 一侧关注 KMS 后端指标和熔断器指标 `rustfs_kms_backend_circuit_open`，见 [KMS observability runbook](kms-observability-runbook.md)。
- 大规模迁移之前，先申请提高 AWS KMS 的请求配额。对称加解密操作在每个区域共用账户级的速率配额，而 RustFS 每个加密对象操作都会发出一次请求。

### 费用

- 每把客户管理的密钥有固定月费，每次 API 请求另外计费。加密对象的流量会一比一地变成 KMS 请求：每次写入一次、每次读取一次，多段上传每段再加一次。
- 处于待删除状态的密钥不计费。测试遗留的密钥会继续计费。

## 排障

| 现象 | 可能原因 | 处理 |
| --- | --- | --- |
| 已配置 KMS，`GET` 却返回 `400 InvalidRequest` 并提到 `RUSTFS_SSE_S3_MASTER_KEY` | 版本早于 AWS 读路径修复 | 升级。不要设置该变量。 |
| 使用 AWS 后端时 RustFS 启动即退出 | 健康检查失败：没有凭据、未授予 `kms:ListKeys`、区域不对，或到端点的网络不通 | 查看启动日志中的 KMS 错误，再依次核对凭据、身份策略、该节点的网络可达性。 |
| 写入返回 `400 KMS.NotFoundException` | 密钥 ID 写错、密钥在其它区域或账户，或密钥已被删除 | 把密钥 ARN 与 `RUSTFS_KMS_AWS_REGION` 及账户对照。 |
| 只有加密读写返回 `403 AccessDenied` | 服务身份在该密钥上缺少 `kms:GenerateDataKey` 或 `kms:Decrypt` | 修正密钥策略或身份策略，无需重启。 |
| `400 InvalidRequest` 并带有 `DisabledException` 或 `KMSInvalidStateException` | 密钥被禁用或处于待删除状态 | 启用密钥（如有需要先取消删除）。 |
| 加密操作返回 `503 ServiceUnavailable` | AWS KMS 不可达或被限流 | 检查 AWS 健康状态与配额。AWS 恢复响应约 30 秒后自动恢复。 |
| 只有某一个节点上的加密操作失败 | 该节点的凭据、权限或网络与其它节点不同 | 把该节点的身份和可达性与正常节点对比。 |
| 按名称创建密钥返回 `501 NotImplemented` | 密钥 ID 由 AWS 分配 | 创建时不带名称，或在 AWS 中创建后引用其 ID。 |

## 相关文档

- [KMS backend security properties](kms-backend-security.md)
- [KMS admin API contract](kms-admin-contract.md)
- [Per-key KMS authorization](kms-per-key-authorization.md)
- [KMS observability runbook](kms-observability-runbook.md)
