# 用 OpenBao 作为 Vault KMS 后端

**适用场景：** 评估能否用 OpenBao 代替 HashiCorp Vault 承载 RustFS 的服务端加密、为 RustFS 规划 OpenBao 高可用部署，或者排查对接 OpenBao 集群时创建密钥报 `Key not found`。
**事实来源：** `crates/kms/src/backends/vault.rs`（KV2 后端，`create_key` 与 `store_key_metadata`）、`crates/kms/src/backends/vault_transit.rs`（Transit 后端及其 KV2 元数据记录）、`crates/kms/src/config.rs`（`RUSTFS_KMS_VAULT_*`）。English: [kms-openbao-compatibility.md](kms-openbao-compatibility.md)。

本页关于行为的描述，均为 2026-11-13 对 OpenBao 2.7.2 的实测结果，其它 OpenBao 版本未测。属于推断而非实测之处，文中会明确标注。

## 结论

RustFS 没有专门的 OpenBao 后端。OpenBao 通过现有的 `vault-kv2` 与 `vault-transit` 后端接入，使用同一套 `RUSTFS_KMS_VAULT_*` 配置，详见 [Vault KMS authentication runbook](vault-kms-authentication.md) 与 [KMS backend security properties](kms-backend-security.md)。

- **单节点，或 RustFS 直连 active 节点：** 就 RustFS 用到的全部功能而言，OpenBao 与 Vault 行为一致，无需改任何配置。
- **高可用且 RustFS 连到 standby 节点：** standby 用本地副本应答读请求，而副本会比写入滞后一小段时间。RustFS 1.0.5 及更早版本对接这样的节点时，创建密钥和其它密钥管理调用会失败。之后的版本在发起变更的那个节点上可以容忍；另一个 RustFS 节点在这段时间内仍可能读到变更前的状态。见[高可用](#高可用standby-节点的读是滞后的)。

## 已验证的范围

| 范围 | OpenBao 2.7.2 上的结果 |
| --- | --- |
| `rustfs-kms` 测试套件（开启 KV2 与 Transit 实连后端） | 712 例全部通过 |
| 实连服务的契约、后端与故障注入测试 | 17 例全部通过 |
| AppRole 认证（KV2、Transit、自定义挂载路径） | 3 例全部通过 |
| 经真实 RustFS 服务的端到端 SSE（SSE-S3、SSE-KMS、多段、大对象、密钥操作、rekey 扫描） | 8 例全部通过 |
| KMS 与 SSE 全生命周期黑盒套件，KV2 与 Transit 后端 | 与同一 RustFS 二进制对接 HashiCorp Vault 1.21 的结果相同 |
| 通过 `RUSTFS_KMS_VAULT_NAMESPACE` 使用 namespace，KV2 与 Transit 后端 | 通过；已确认密钥落在该 namespace 内 |
| 三节点 Raft 集群并配置 `disable_standby_reads = true`，解密流量进行中杀掉 leader | 通过 |
| 三节点 Raft 集群，默认配置，经 standby 节点做密钥生命周期与 SSE | 见[高可用](#高可用standby-节点的读是滞后的) |

在 OpenBao 上，Transit 密文仍带 `vault:v1:` 前缀，Transit 加解密的 associated data 生效，删除 Transit 密钥同样需要先设置 `deletion_allowed`。

本次验证未覆盖：Kubernetes 认证、TLS 与双向 TLS 连接，以及 2.7.2 以外的 OpenBao 版本。

## 高可用：standby 节点的读是滞后的

在 OpenBao 高可用集群中，standby 节点用本地数据副本处理请求，只有必须写入时才转发给 active 节点。在本机三节点 Raft 集群上实测到两个后果：

- 经 standby 读，要等这个 standby 应用了变更之后才能读到经它自己写入的值。间隔为 60 到 100 毫秒。
- standby 凭本地副本就能拒绝的写请求，会被直接拒绝而不转发。设置 `deletion_allowed` 之后立刻删除 Transit 密钥，在同样的间隔内返回 `deletion is not allowed for this key`。

这是"由副本应答读请求"的存储的通常行为，不是 OpenBao 的缺陷。未实测：按 HashiCorp 的文档，Vault Enterprise 的 performance standby 同样在本地应答读请求，预计行为相同。Vault 社区版的 standby 转发全部请求，不会出现这种情况。

### RustFS 1.0.5 及更早版本

这些版本在密钥管理过程中会读回自己刚写入的数据，因此只要 `RUSTFS_KMS_VAULT_ADDRESS` 解析到 standby 节点（无论直连还是经负载均衡），就会出现下列失败。把同一个服务改连 active 节点后，这些用例全部通过。

- **KV2 后端：** 每次创建密钥都失败，返回 HTTP 404 与 `failed to create key: Key not found: <key-id>`。密钥记录其实已经写入并且可用，但缺少请求里的描述与标签。紧跟在创建或更新之后的操作会报 `Key not found` 或 `Concurrent modification of key <key-id> detected, retry the operation`。
- **Transit 后端：** disable 之后紧接着 enable 会失败，报 `Concurrent modification of transit key <key-id> metadata detected; retry the operation`。
- **对象读写：** 使用已存在密钥的 PUT、GET、多段上传与 Copy，在测试中未受影响。

### 之后的版本

RustFS 不再依赖"能读回自己刚写入的数据"：

- KV2 后端创建密钥改为一次仅创建的写入，描述与标签随同写入。
- 每个 RustFS 节点在五秒内记住自己对每条密钥记录最后写入的版本，读到更旧的结果就重读，包括自己已删除却仍被返回的记录。每次重读前暂停 25 到 400 毫秒，最多五次，之后原样返回后端的应答。
- check-and-set 写入失败后，同样先暂停再重读，尝试次数由三次改为五次。
- Transit 密钥的删除如果因为删除开关尚不可见而被拒绝，按同样的方式重试。

在 OpenBao 默认配置下经 standby 节点实测：密钥生命周期、加解密与轮换的行为套件在两个后端上连续三轮通过；SSE 数据面套件（34 例）在两个后端上通过；密钥管理套件在 Transit 后端上通过；在 KV2 后端上除下述列表用例外通过，该用例三轮中失败两轮。

仍然存在的情况，原因是只有发起变更的那个节点知道这次变更：

- **另一个 RustFS 节点，或任何其它客户端，读同一个 standby 时**，在 standby 追上之前看到的是变更前的状态。经一个节点创建的密钥，在这段时间内经另一个节点使用，可能被报告为不存在；disable 在其它节点上生效的时间，是这段间隔再加上它们自身的缓存时长。
- **KV2 后端创建密钥后立刻列出密钥**，结果里可能没有新密钥。
- **leader 故障期间读请求不受影响。** 能应答读请求的 standby 不需要 active 节点，所以杀掉 leader 期间 KV2 解密没有任何重试、持续成功。`scripts/test/vault_ha_kms_live.sh` 断言的是相反的情况（这是在 Vault 上观察到的行为），因此对默认配置的 OpenBao 集群会在这条断言上失败。

### 推荐配置

如果需要各 RustFS 节点之间严格一致，以下两种任选其一，均已实测有效。

1. 在每个 OpenBao 节点上关闭 standby 读，让 standby 把读请求转发给 active 节点：

   ```hcl
   disable_standby_reads = true
   ```

2. 只把 RustFS 的流量路由到 active 节点。`GET /v1/sys/health` 在 active 节点返回 200，在已解封的 standby 上返回 429，可用作负载均衡的健康检查。

RustFS 不发送 `X-Vault-Index` 请求头。测试中发送 `X-Vault-Forward: active-node` 或 `X-Vault-Inconsistent: forward-active-node` 都不能改变 standby 的行为。

## 与 Vault 的部署差异

以下差异影响的是 OpenBao 自身的部署方式，不影响 RustFS 的配置。

- **存储：** OpenBao 2.7.2 拒绝 `storage "file"`，报 `unknown storage type file`。请使用 `storage "raft"`，单节点也一样。
- **容器镜像：** 数据目录是 `/openbao/file`，配置目录是 `/openbao/config`，内联配置通过 `BAO_LOCAL_CONFIG` 传入，`VAULT_LOCAL_CONFIG` 不会被读取。镜像里保留了指向 `bao` 的 `vault` 命令。
- **审计设备：** `sys/audit` 默认拒绝通过 API 启用审计设备，要求改用配置文件声明。`unsafe_allow_api_audit_creation = true` 可以恢复 API 方式，仅适合测试集群。
- **Raft 成员：** 刚加入的节点先显示为 non-voter，测试中约十秒后才升为 voter。依赖故障切换之前，先确认所有节点都已是 voter。
- **命令行：** `bao` 接受 RustFS 测试脚本传给 `vault` 的同一组 `server -dev` 参数，因此设置 `RUSTFS_TEST_VAULT_BIN=bao` 即可原样运行 `scripts/test/vault_approle_kms_live.sh` 与端到端的 Vault KMS 测试。

## 故障排查

| 现象 | 原因 | 处理 |
| --- | --- | --- |
| OpenBao 高可用下，KV2 后端的 `POST /rustfs/admin/v3/kms/keys` 返回 `failed to create key: Key not found: <key-id>` | RustFS 1.0.5 或更早版本连到了尚未应用这次创建的 standby | 升级 RustFS 或采用[推荐配置](#推荐配置)之一，然后重发创建请求以补上描述与标签 |
| enable、disable、轮换或打标签时报 `Concurrent modification of ... detected` | standby 持续返回写入所依据的旧版本，时间超过了重试预算 | 重试该操作；反复出现时采用推荐配置之一 |
| 经一个 RustFS 节点刚创建的密钥，被另一个节点报告为不存在 | 另一个节点读到了尚未应用这次创建的 standby | 重试，或采用推荐配置之一 |
| OpenBao 启动失败，报 `unknown storage type file` | `file` 存储后端不可用 | 把配置改为 `storage "raft"` |
| `cannot enable audit device via API` | 默认禁止经 API 创建审计设备 | 在配置文件中声明审计设备 |
