# RustFS 整体框架结构走读

> 走读范围：仓库顶层、`rustfs/` 主 crate、`crates/` 核心库分层、启动序列、请求路径、ECStore 存储引擎骨架、元数据与 I/O 内存结构。
> 本文档是代码走读系列的总览；后续功能细节将另文展开。

---

## 1. 项目定位与 Bird's Eye View

RustFS 是用 Rust 编写的高性能、S3 兼容分布式对象存储系统，使用纠删码（Erasure Coding）保证数据持久性，通过 IAM/STS 支持多租户，并提供 Web 管理控制台。

一个运行中的 RustFS 节点对外暴露：

| 通道 | 端口/前缀 | 职责 |
|------|-----------|------|
| **S3 API** | 9000 | 对象 CRUD 主数据面 |
| **Admin API** | 9000, `/minio/` | 集群管理、IAM、指标 |
| **Console** | 9001 | Web UI，后端走 Admin API |
| **Inter-node RPC** | gRPC/tonic, `/rustfs/rpc/` | 分布式节点间通信 |
| 可选协议 | feature 门控 | FTPS / SFTP / WebDAV / Swift |

### 1.1 核心数据流（PUT 请求）

```
HTTP request
  → server/          (TLS, auth, routing, compression)
    → app/object_usecase  (validation, policy, lifecycle)
      → storage/ecfs      (erasure coding, encryption, checksums)
        → ecstore          (disk pool selection, data distribution)
          → rio            (reader pipeline: encrypt → compress → hash → write)
            → io-core      (buffer pool, storage profiling, admission control)
              → local disk / remote disk via RPC
```

---

## 2. 仓库布局与 Workspace 成员

仓库是 Cargo workspace（`Cargo.toml` 为权威成员列表），扁平 `crates/` 布局：

```
rustfs/                      # Workspace 根（virtual manifest）
├── rustfs/                  # 主二进制 + 库 crate
│   └── src/
│       ├── main.rs          # 进程入口、全局分配器
│       ├── lib.rs           # 模块树根
│       ├── server/          # HTTP 服务、TLS、路由、中间件
│       ├── admin/           # Admin API handlers 与 console
│       ├── app/             # 用例层（object / bucket / multipart）
│       ├── storage/         # 存储引擎接口与实现（ecfs）
│       ├── auth.rs          # S3 请求认证
│       ├── config/          # CLI 参数、配置解析、workload profiles
│       └── startup_*.rs     # 启动编排（约 25 个模块）
├── crates/                  # 库 crates
│   ├── ecstore/             # 纠删码存储引擎（架构中心）
│   ├── filemeta/            # 对象元数据（xl.meta）
│   ├── rio/                 # 读 I/O 管道（加密、压缩、哈希）
│   ├── io-core/             # 缓冲池、背压、死锁检测、锁优化
│   ├── storage-api/         # 存储 API 契约（trait 定义）
│   ├── common / config / utils / ...
│   └── e2e_test/            # 端到端集成测试
└── docs/                    # 架构契约、运维手册、测试规范
```

### 2.1 按领域划分的 Crate 分组

| 领域 | Workspace crates | 职责 |
|------|------------------|------|
| **Foundation** | `checksums`, `common`, `config`, `data-usage`, `heal-contracts`, `scanner-metrics`, `utils` | 共享配置、数据用量模型、heal 域契约、扫描遥测、工具、校验和 |
| **I/O 与存储** | `concurrency`, `ecstore`, `filemeta`, `heal`, `io-core`, `io-metrics`, `lifecycle`, `lock`, `object-capacity`, `object-data-cache`, `replication`, `rio`, `rio-v2`, `s3-client`, `scanner`, `storage-api` | 纠删码对象存储、元数据、恢复、生命周期、复制、锁、缓存、I/O 管道、远程 tier S3 客户端 |
| **安全与身份** | `credentials`, `crypto`, `iam`, `keystone`, `kms`, `policy`, `security-governance`, `signer`, `tls-runtime`, `trusted-proxies` | 凭据、认证、授权、加密、密钥管理、TLS |
| **协议与契约** | `extension-schema`, `madmin`, `protos`, `protocols`, `s3-ops`, `s3-types`, `s3select-api`, `s3select-query` | Admin、节点间、S3、S3 Select、可选协议契约 |
| **运维与集成** | `audit`, `notify`, `obs`, `targets`, `zip` | 审计、可观测、事件投递、通知目标、归档 |
| **测试支撑** | `e2e_test`, `test-utils` | 端到端验证与共享测试工具 |

> `ecstore` 是架构中心的存储引擎；`rio-v2` 是 feature 门控的 MinIO 磁盘格式兼容 I/O 层（默认不编入）。

---

## 3. 主 Crate 分层（`rustfs/src/`）

请求自上而下穿过各层，**不允许向上依赖**（storage 不得 import admin）。

| 层 | 目录 | 职责 |
|----|------|------|
| **Server** | `server/` | HTTP 监听、TLS、CORS、压缩、中间件、优雅关闭 |
| **Admin** | `admin/` | Admin API 路由、30+ handler 模块、Web console |
| **App** | `app/` | 用例编排：object（按 S3 操作拆在 `app/object/`）、bucket_usecase、multipart_usecase |
| **Storage** | `storage/` | S3 API 翻译、纠删码 FS、SSE 加密、RPC、并发 |
| **Auth** | `auth.rs` | S3 签名验证、凭据校验 |
| **Config** | `config/` | CLI 解析、配置结构、workload profiles |

### 3.1 依赖方向（简化）

```
                        ┌─────────┐
                        │  rustfs │  (binary + lib)
                        │  main   │
                        └────┬────┘
                             │
             ┌───────────────┼───────────────┐
             │               │               │
        ┌────▼────┐     ┌────▼────┐   ┌──────▼─────┐
        │ server  │     │  admin  │   │    app     │
        │ (HTTP)  │     │(console)│   │(use-cases) │
        └────┬────┘     └────┬────┘   └──────┬─────┘
             │               │               │
             └───────────────┼───────────────┘
                             │
                      ┌──────▼──────┐
                      │   storage   │
                      │ (ecfs, SSE, │
                      │  RPC, ACL)  │
                      └──────┬──────┘
                             │
          ┌──────────────────┼──────────────────┐
          │                  │                  │
    ┌─────▼──────┐    ┌──────▼──────┐    ┌──────▼──────┐
    │  ecstore   │    │     rio     │    │   io-core   │
    │   (core)   │    │  (readers)  │    │ (buffers)   │
    └─────┬──────┘    └─────────────┘    └─────────────┘
          │
 ┌─────┬──┼──┬─────┬──────┐
 │     │  │  │     │      │
common utils config policy filemeta ...
```

### 3.2 架构不变量（摘要）

1. **分层向下**。Server → Admin/App → Storage → ecstore → rio/io-core，无向上 import。
2. **叶子 crate 只依赖外部 crate**（少量有 guard 钉住的例外：`io-metrics→s3-ops`、`madmin→signer`）。
3. **每个类型只有一个定义**（跨 crate 共享类型在一个 crate 定义，其余 re-export）。
4. **ecstore 不服务 HTTP/S3 wire**；消费远程 S3 端点走 `rustfs-s3-client`。
5. **`rustfs` 二进制 crate 是唯一组装处**；各 crate 可独立测试。
6. **错误类型用 `thiserror`**，命名具描述性（`StorageError` 而非 `Error`）。

---

## 4. 启动序列

### 4.1 进程入口

```
main.rs::main()
  → 设置全局分配器（mimalloc / hotpath CountingAllocator）
  → rustfs::startup_entrypoint::run_process()
       → server::build_tokio_runtime()   // Tokio runtime + dial9 guard
       → runtime.block_on(async_main())
       → drop(dial9_guard)               // 显式封存 trace segment
       → 失败: emit_fatal_stderr + shutdown_observability_guard + exit(1)
```

### 4.2 `async_main()` — CLI 分发与 Preflight

1. e2e CAS 探针（feature `e2e-test-hooks`）
2. `cgroup_resources::log_container_resources()`
3. `bootstrap_external_prefix_compat()` — 兼容 `MINIO_*` 等外部环境变量
4. `Opt::parse_command(args)` → `CommandResult`：
   - `Info` / `Tls` / `Diagnose` / `Inspect` / `ConnectRegister` 短路
   - `Server(config)` → 继续服务器路径
5. `init_startup_server_preflight`：config snapshot、license、observability、crypto provider、TLS material
6. `run(*config)` 进入核心启动

### 4.3 核心启动顺序 `run(config)`

```
bootstrap_instance_ctx()                 // 单实例 InstanceContext
  → init_startup_listen_context          // 解析地址、发布 region/port
  → init_startup_storage_foundation      // EndpointServerPools、本地盘、锁客户端
  → init_startup_http_servers            // 先起 HTTP，readiness gate 挡请求
  → init_startup_storage_runtime         // ECStore::new、全局配置、StorageReady
  → init_capacity_management_managed     // 容量后台任务
  → init_startup_runtime_services        // KMS/IAM/Audit/Notification/Scanner/...
  → run_startup_runtime_lifecycle        // 就绪发布 + 等待关闭
```

| 步骤 | 函数 | 关键产物 |
|------|------|----------|
| 1 | `bootstrap_instance_ctx` | `Arc<InstanceContext>` |
| 2 | `init_startup_listen_context` | `StartupListenContext` |
| 3 | `init_startup_storage_foundation` | `EndpointServerPools` |
| 4 | `init_startup_http_servers` | `StartupHttpServers` + `ServerContextSlot` |
| 5 | `init_startup_storage_runtime` | `Arc<ECStore>` + `StorageReady` |
| 6 | `init_capacity_management_managed` | 容量后台任务 |
| 7 | `init_startup_runtime_services` | `StartupServiceRuntime` |
| 8 | `run_startup_runtime_lifecycle` | 就绪 + 等待 shutdown |

**设计要点**：HTTP 服务先于 ECStore 启动，但 `ReadinessGateLayer` 挡住业务请求，直到 `GlobalReadiness` 各 stage（含 `StorageReady`、IAM bootstrap）就绪。

### 4.4 服务初始化 `init_startup_runtime_services`

顺序：KMS → optional runtime services → heartbeat → buffer profile / deadlock detector → bucket metadata → IAM → audit → site-replication reconcile → auth integrations → notification → background services（scanner）→ observability → heartbeat/inventory。

---

## 5. Server 层与请求路径

### 5.1 HTTP 服务装配（`server/http.rs::start_http_server`）

1. socket2 建监听（IPv6 dual-stack / IPv4 fallback、TCP_NODELAY、keepalive、SO_REUSEPORT）
2. TLS：`load_tls_material` + `build_acceptor_from_loaded`（显式配置却无证书则 fail closed）+ 热更新
3. S3 服务装配：

```text
storage::ecfs::FS::with_server_ctx(server_ctx)   // S3 业务实现
S3ServiceBuilder::new(store)
  .set_auth(IAMAuth::with_server_context(...))   // 签名验证（s3s S3Auth）
  .set_access(store)                             // 授权（s3s S3Access）
  .set_route(metadata_route + admin::make_admin_route)
  .set_config(StaticConfigProvider)              // SigV2、5GiB put 上限等
  .set_host(MultiDomain)                         // virtual-hosted-style
  .build()
```

### 5.2 每连接服务栈（`process_connection`）

```
TCP accept → TLS handshake?
  → PathDispatchService: /rustfs/rpc/* ? internode : external
  → HybridService: gRPC (application/grpc) ? tonic : REST
  → S3Service (s3s crate)
```

**External 中间件栈（外→内）**：

```text
RemoteAddr → TrustedProxy → ExternalRequestContext
→ StsQueryApiCompat → EmptyBodyContentLengthCompat → CatchPanic
→ RateLimit → SsecTransport → ReadinessGate
→ KeystoneAuth → InFlight → Trace → RequestLogging
→ Compression → S3ErrorMessageCompat / IcebergRestErrorCompat / ObjectAttributesEtagFix
→ ConditionalCors → RedirectLayer(console) → BodylessStatusFix → HeadRequestBodyFix
→ PublicHealthEndpoint → VirtualHostStyleHint → DoubleSlashListBucketsCompat
→ [s3_service | hybrid]
```

### 5.3 HTTP → S3 Handler 路径

```text
TCP accept
  → process_connection
  → PathDispatchService: /rustfs/rpc/* ? internode : external
  → HybridService: gRPC ? tonic : REST
  → S3Service (s3s):
       1) S3Auth::get_secret_key  ← IAMAuth (auth.rs)  查密钥
       2) s3s 内部 SigV4/V2 验签
       3) S3Route::is_match/check_access/call
          ├─ MetadataRoute (app/metadata_route.rs)
          └─ admin::make_admin_route → admin/router.rs
       4) S3Access::check / 每操作授权  ← FS (storage/access.rs)
       5) S3 trait 实现                ← storage/ecfs.rs::FS
  → ECStore (crates/ecstore，经 storage_api 边界)
```

---

## 6. 认证与授权

### 6.1 关键结构体

| 结构体 | 位置 | 作用 |
|--------|------|------|
| `IAMAuth` | `auth.rs` | 实现 s3s `S3Auth`；持有 root ak/sk + `SimpleAuth` + 可选 `ServerContextSlot` |
| `AuthType` | `auth.rs` | `Anonymous / Presigned / PresignedV2 / PostPolicy / StreamingSigned / Signed / SignedV2 / JWT / STS / StreamingSignedTrailer / ...` |
| `VerifiedPresignedRequest` / `VerifiedSigV4Request` | `auth.rs` | 访问边界注入的 marker，下游能力解析必须要求此 marker |
| `ReqInfo` | — | 请求身份上下文（cred、is_owner、bucket/object/version_id） |

### 6.2 签名验证与凭据解析分层

密码学验签在 s3s crate 内完成；RustFS 提供密钥查找 + IAM/策略校验：

**`S3Auth::get_secret_key` 密钥查找顺序**：
1. Keystone task-local `KEYSTONE_CREDENTIALS` → 空 secret（token 认证）
2. `access_key` 为空 → `UnauthorizedAccess`
3. `"keystone:"` 前缀 → 空 secret
4. 匹配 root `access_key` → 返回配置的 `secret_key`
5. `SimpleAuth` 兜底
6. IAM handle → `iam_store.check_key(access_key)` → `secret_key`
7. 失败 → `InvalidAccessKeyId`

**`S3Access::check` 访问边界**（签名已验证后）：
1. 拒绝 presigned URL 上未签名的 `x-amz-*` 头
2. `check_key_valid_with_context` 解析完整 `Credentials` 与 `is_owner`
3. 组装 `ReqInfo`；插入 `Verified*` markers
4. 校验 RustFS 扩展 query 能力只作用于对应 op
5. `license_check()`

**每操作授权 `authorize_request`**：
1. 构造策略条件（claims、groups/roles、object-lock 头、client IP/region 等）
2. `iam_store.prepare_auth` + bucket policy
3. `PolicySys::try_is_allowed_for_store` / `iam_store.is_allowed` 合并 IAM + bucket policy
4. owner 绕过显式 Deny 仅限 Get/Put/DeleteBucketPolicy

---

## 7. ECStore 存储引擎

### 7.1 对象持有链（内存组织总图）

```
ECStore                          (store/mod.rs)
 ├─ pools: Vec<Arc<Sets>>        // 每个 pool 一个 Sets
 ├─ disk_map: HashMap<usize, Vec<Option<DiskStore>>>  // pool_idx → 全部盘（含离线槽位）
 ├─ pool_meta: RwLock<PoolMeta>  // pool.bin 持久化的拓扑/decommission 状态
 ├─ decommission_cancelers / rebalance_meta / start_gate / pool_meta_save_gate
 └─ ctx: Arc<InstanceContext>    // 实例级运行时上下文

Sets                             (core/sets.rs)
 ├─ disk_set: Vec<Arc<SetDisks>> // [set_count]，每项 = 一个 erasure set
 ├─ pool_idx: usize
 ├─ format: FormatV3             // 本 pool 的 format.json 快照
 ├─ set_count / set_drive_count / parity_count / default_parity_count
 ├─ distribution_algo: DistributionAlgoVersion
 └─ ctx: Arc<InstanceContext>

SetDisks                         (set_disk/mod.rs, ctx.rs 暴露字段)
 ├─ disks: Arc<RwLock<Vec<Option<DiskStore>>>>  // [set_drive_count]，None=离线
 ├─ set_index / pool_index / set_drive_count / default_parity_count
 ├─ set_endpoints: Vec<Endpoint>
 ├─ format: FormatV3
 ├─ lockers: [Arc<dyn LockClient>]
 └─ 内部缓存：ErasureCache、GetObjectMetadataCache(moka)、CapacityScopeCache

DiskStore = Arc<Disk>            (disk/mod.rs)
 └─ Disk::Local(Box<LocalDiskWrapper>) | Disk::Remote(Box<RemoteDisk>)
```

**扁平索引约定**：`flat_idx = set_idx * set_drive_count + disk_idx`，与 `FormatV3.erasure.sets[i][j]`、`Endpoint.{pool_idx,set_idx,disk_idx}` 一一对应。

### 7.2 对象定位：三层映射

| 映射 | 算法 | 位置 |
|------|------|------|
| **对象 → pool** | 优先已存在位置（`get_pool_info_existing`），否则按可用容量 / decommission 目标选 | `ECStore::get_pool_idx` / `select_put_object_pool_idx`（core/pools.rs） |
| **对象 → set** | `get_hashed_set_index(object_key)`：V1=CRCMOD，V2/V3=SIPMOD（部署 UUID 作种子） | `Sets::get_hashed_set_index`（core/sets.rs） |
| **shard → disk** | `FileInfo.erasure.distribution` 置换 + `shuffle_disks` | `SetDisks::put_object`（set_disk/ops/object.rs） |

```rust
fn get_hashed_set_index(&self, input: &str) -> usize {
    match self.distribution_algo {
        DistributionAlgoVersion::V1 => crc_hash(input, self.disk_set.len()),
        DistributionAlgoVersion::V2 | V3 => sip_hash(input, self.disk_set.len(), self.id.as_bytes()),
    }
}
```

`Sets::get_disks_by_key(object)` = `get_disks(get_hashed_set_index(key))`，几乎所有对象 API 都经它落到 `Arc<SetDisks>`。

### 7.3 关键结构体

#### `ECStore`（`store/mod.rs`）

| 字段 | 类型 | 作用 |
|------|------|------|
| `id` | `Uuid` | 实例 ID |
| `disk_map` | `HashMap<usize, Vec<Option<DiskStore>>>` | 磁盘索引映射 |
| `pools` | `Vec<Arc<Sets>>` | 磁盘池列表 |
| `peer_sys` | `S3PeerSys` | 节点间对等通信 |
| `pool_meta` | `RwLock<PoolMeta>` | 池元数据（`pool.bin`，CAS 写） |
| `rebalance_meta` | `RwLock<Option<RebalanceMeta>>` | 再平衡元数据 |
| `decommission_cancelers` | `RwLock<Vec<Option<DecommissionCanceler>>>` | 退役取消句柄 |
| `start_gate` | `Mutex<()>` | 串行化 rebalance/decommission 启动 |
| `pool_meta_save_gate` | `Mutex<PoolMetaWriteState>` | 串行化池元数据落盘（锁序：save_gate → pool.bin fence → pool_meta 短读锁） |
| `ctx` | `Arc<InstanceContext>` | 实例级运行时状态 |
| `bucket_fence_registry` | `Arc<BucketFenceRegistry>` | bucket incarnation 校验备忘 |

#### `Sets`（`core/sets.rs`）— 一个 pool 的全部 erasure set

- 实现 `ObjectIO` / `ObjectOperations` / `BucketOperations`，全部是薄路由：`self.get_disks_by_key(object).<op>(...)`
- bucket 级操作对每个 set 广播（make_bucket 全成功才 OK）
- 构造时启动 `monitor_and_connect_endpoints_task`，每 15s `connect_disks()` 重连离线盘
- 批量删除按 set 分组并发（`set_obj_map` 先按 hash 分桶再 `FuturesUnordered`）

#### `SetDisks`（`set_disk/mod.rs`）— 一个纠删集

历史 ~19.7k 行 God-Object 已拆分：

| 模块 | 职责 |
|------|------|
| `mod.rs` | 核心结构 + GET 优化门、metadata cache、ErasureCache、锁诊断 |
| `ctx.rs` | `SetDisksCtx<'a>` — `Copy` 借用句柄，操作族不克隆 `Arc` |
| `core/io_primitives.rs` | 元数据 fanout quorum、bitrot reader 调度、read-repair heal 去重 |
| `ops/object.rs` | `ObjectIO`+`ObjectOperations` 热路径（写） |
| `ops/heal.rs` / `multipart.rs` / `list.rs` / `bucket.rs` / `locking.rs` | 按契约拆分 |
| `read.rs` | `get_object_*` 读管线 + 元数据缓存 |
| `metadata.rs` | quorum 归并（`reduce_common_data_dir` 等） |

**内部缓存**：
- `ErasureCache`：key = `(data_shards, parity_shards, block_size, uses_legacy)`，上限 32，克隆 set 共享
- `GetObjectMetadataCache`（moka）：TTL 2s + 4096 条 + generation fence 分片失效

#### `Disk` / `DiskStore` / `DiskAPI`

```rust
pub type DiskStore = Arc<Disk>;
pub enum Disk {
    Local(Box<LocalDiskWrapper>),
    Remote(Box<RemoteDisk>),
}
```

`DiskAPI` trait：`make_volume` / `read_xl` / `read_version` / `write_metadata` / `rename_data` / `read_file_stream` / `walk_dir` / `check_parts` 等。

**`LocalDiskWrapper`**：`LocalDisk` + `DiskHealthTracker`（原子时间戳、状态、连续成败计数、容量探测）+ metrics epoch + 超时策略。

**盘上常量**：`.rustfs.sys`（RUSTFS_META_BUCKET）、`format.json`、`xl.meta`/`xl.meta.bkp`、`healing.bin`、`.part.N.rustfs-txn`。

### 7.4 盘上格式 `FormatV3`（`layout/format.rs`）

```rust
pub struct FormatV3 {
    pub version: FormatMetaVersion,     // 恒 "1"
    pub format: FormatBackend,          // "xl" (Erasure) / "xl-single" (单盘)
    pub id: Uuid,                       // 部署 ID（siphash 种子）
    pub erasure: FormatErasureV3,
    pub disk_info: Option<DiskInfo>,    // skip 序列化
}

pub struct FormatErasureV3 {
    pub version: FormatErasureVersion,       // V1/V2/V3
    pub this: Uuid,                          // 本盘 UUID
    pub sets: Vec<Vec<Uuid>>,                // [set_idx][disk_idx] = disk uuid（拓扑唯一真源）
    pub distribution_algo: DistributionAlgoVersion,  // CRCMOD / SIPMOD / SIPMOD+PARITY
}
```

- `find_disk_index_by_disk_id` → `(set_idx, disk_idx)`
- `shared_identity` 用于多盘一致性校验
- 新格式默认 `FormatErasureVersion::V3` + `DistributionAlgoVersion::V3`
- 兼容 MinIO `format.json`

### 7.5 layout/ — 端点与拓扑

| 结构 | 作用 |
|------|------|
| `Endpoint` | `url` + `is_local` + `pool_idx/set_idx/disk_idx` |
| `Endpoints` / `PoolEndpoints` | 解析 `DisksLayout` → `SetupType::{FS, ErasureSD, Erasure, DistErasure}` |
| `StaticSetLayoutSnapshot` | 从 `FormatV3` 派生的静态布局（set_count、drives_per_set、disk_ids） |
| `RuntimeSetLayoutPlan` | 运行时按 host 展开的 set 计划，含 `lock_hosts_for_set` |

格式初始化/仲裁：`store/init_format.rs` — `load_format_erasure_all`、`select_format_erasure_in_quorum`、`save_format_file`。

### 7.6 对象写入主路径（PUT）

```
ECStore::put_object
 └─ put_object_with_old_current_size
     └─ handle_put_object (store/object.rs)
         ├─ prepare_put_object（校验/encode_dir_object/对象锁配置快照）
         ├─ select_put_object_pool_idx → pool_idx          ← 对象→pool
         └─ pools[pool_idx].put_object...  (Sets)
             └─ get_disks_by_key(object)                    ← 对象→set（sip/crc hash）
                 └─ SetDisks::put_object...  (set_disk/ops/object.rs)
                     ├─ FileInfo::new + data_dir=Uuid::new_v4() + version_id
                     ├─ shuffle_disks_owned(distribution)   ← shard→disk 置换
                     ├─ classify_put_write_path:
                     │    Inline（小对象内联进 xl.meta）
                     │    | SingleBlockNonInline
                     │    | Pipeline / PipelineBatchedLarge
                     ├─ create_bitrot_writer × N
                     │    （写到 .rustfs.sys/tmp/<uuid>/<data_dir>/part.1）
                     ├─ erasure::coding::encode
                     │    （Erasure::encode_data → EncodedBlock → MultiWriter）
                     ├─ write quorum 校验（reduce_write_quorum_errs）
                     ├─ rename_data 原子提交到 bucket/object
                     │    （+ early-ACK tail heal）
                     └─ record_capacity_scope_if_needed（容量脏标记）
```

### 7.7 对象读取主路径（GET）

```
ECStore::get_object_reader
 └─ pools[?].get_object_reader  (Sets)
     └─ get_disks_by_key(object)          ← 同一 hash 定位 set
         └─ SetDisks::get_object_reader  (set_disk/read.rs)
             ├─ 元数据 fanout（read_version × N，MetadataQuorumAccumulator 归并）
             │    支持 early-stop、two-phase（失败数据 shard 后补 late parity）
             ├─ [可选] GetObjectMetadataCache 命中短路
             ├─ 读路径决策：
             │    DIRECT_MEMORY（小对象 inline 直拼）
             │    | INLINE_DIRECT | BODY_CACHE
             │    | CODEC_STREAMING（新流式解码，默认 rollout=off）
             │    | MID_SIZE_STREAMING | LEGACY_DUPLEX
             │    | REMOTE_TRANSITION | EMPTY
             ├─ 为每 shard 建 BitrotReader（校验 HighwayHash256S）
             ├─ decode::ParallelReader 按 stripe 并发读 → 达 quorum 解码
             │    └─ 缺 shard: Erasure::reconstruct_data + encode_parity
             └─ 输出 GetObjectReader{stream, offset/length}
```

**锁**：读走 `acquire_read_lock_diag`；lock optimization 下物化读提前放锁，流式读持锁至 body 结束（`SetDiskLockGuardedReader`）。

### 7.8 纠删码（`erasure/`）

#### `Erasure`（`coding/erasure.rs`）— 双编码器

```rust
pub struct Erasure {
    pub data_shards: usize,
    pub parity_shards: usize,
    pub block_size: usize,
    uses_legacy: bool,
    encoder: Option<ReedSolomonEncoder>,          // 现代：rustfs_erasure_codec::galois_8
    legacy_encoder: Option<Arc<LegacyReedSolomonEncoder>>,  // 旧：reed-solomon-simd
}
```

- **`EncodedBlock`**：编码后连续 buffer（`Bytes` + `shard_size`），`shards()` 按 `shard_size` 切片，`into_shards` 零拷贝 `split_to`
- **shard 尺寸**：现代 `ceil(block_size/data_shards)`；legacy `(block_size.div_ceil(data)+1) & !1`（MinIO 兼容）
- **`MultiWriter`**（encode.rs）：扇出写 shard 到 bitrot writer，带写 quorum、进度截止（`WriteProgressPolicy`）、inflight 字节配额（默认 32 MiB）
- **`ParallelReader`**（decode.rs）：stripe 级并行读，`DecodeReadPolicy::{Default, DemandBound}` 区分普通 GET 与 copy-source
- **Bitrot**：每 shard 独立 HighwayHash256S，读时校验，heal 时 `bitrot_verify`

#### 编解码工作区（`codec/`）

- `CodecStreamingDecodeEngine`：双引擎 `legacy` / `rustfs`
- `ShardBufferPool`：stripe shard 缓冲池（take 不清零）
- `BufferPool`：按 2 的幂分桶 `Vec<u8>` 池

### 7.9 对象存储 API 契约（`crates/storage-api`）

| Trait | 职责 |
|-------|------|
| `ObjectIO` | `get_object_reader` / `put_object_reader` 流式 I/O |
| `ObjectOperations` | `get_object_info` / `copy_object` / `delete_object` |
| `BucketOperations` | 桶生命周期 |
| `MultipartOperations` | 分段上传 |
| `ListOperations` | 列表/遍历 |
| `HealOperations` | 修复 |
| `NamespaceLocking` | 命名空间锁 |
| `StorageAdminApi` | 管理面 |

### 7.10 关键不变量

- **quorum**：读 = data_shards（可 reconstruct 时放宽），写 = `default_write_quorum`（通常 data+1）；`disk/error_reduce.rs` 的 `reduce_read_quorum_errs` / `reduce_write_quorum_errs` 是统一裁决点
- **bitrot**：每 shard 独立 HighwayHash256S
- **格式兼容**：`FormatV3` 读 MinIO `format.json`；`uses_legacy_checksum` 文件走旧编码器
- **写提交**：tmp 目录写入 → `rename_data` 原子发布；失败走 tail heal

### 7.6 对象元数据（`crates/filemeta`）

#### 7.6.1 运行时主结构 `FileInfo`（`fileinfo.rs:251`）

上层 API 与 RPC 使用的「解码后对象版本」视图：

| 字段 | 作用 |
|------|------|
| `volume` / `name` | 桶与对象键 |
| `version_id: Option<Uuid>` | 版本 ID；`None`/nil = null 版本 |
| `is_latest` / `deleted` / `mark_deleted` | 最新版本、删除标记、待清除标记 |
| `transition_*` | 分层/转储状态 |
| `data_dir: Option<Uuid>` | 对象体 shard 目录 UUID（每次写入重新生成，是 ODC 缓存键的 write-unique 锚点） |
| `mod_time` / `size` / `mode` / `written_by_version` | 修改时间、长度、模式位、写入版本时间戳 |
| `metadata: HashMap<String,String>` | 合并后的用户+内部元数据（含 SSE 密钥材料，Debug 脱敏） |
| `parts: Vec<ObjectPartInfo>` | 分片列表 |
| `erasure: ErasureInfo` | 纠删几何 |
| `data: Option<Bytes>` | **inline 对象体**（小对象直接嵌在 xl.meta） |
| `checksum: Option<Bytes>` | 上传合并校验和 |
| `versioned` / `uses_legacy_checksum` | 是否版本化桶、是否走 legacy 校验和算法 |

`Debug` 手写实现：`metadata` 走 `RedactedMetadata`，`data`/`checksum` 走 `ElidedBytes`，防止 inline 明文与密钥材料进日志。

#### 7.6.2 `ErasureInfo` / `ObjectPartInfo` / `ChecksumInfo`

```rust
pub struct ErasureInfo {
    pub algorithm: String,        // "rs-vandermonde"
    pub data_blocks: usize,
    pub parity_blocks: usize,
    pub block_size: usize,        // 1 MiB (BLOCK_SIZE_V2)
    pub index: usize,             // 当前盘索引
    pub distribution: Vec<usize>, // 数据/校验块分布
    pub checksums: Vec<ChecksumInfo>,
}
// shard_size = (block_size / data_blocks + 1) & !1  （偶数对齐）
```

`ObjectPartInfo`：`etag / number / size / actual_size(压缩前) / mod_time / index(压缩索引) / checksums`。
`ChecksumInfo`：`part_number / algorithm / hash` — 每分片 bitrot 校验。

#### 7.6.3 xl.meta 磁盘布局（`filemeta/codec.rs`）

```
[ "XL2 " 4B ][ major u16 LE ][ minor u16 LE ]     <- XL_FILE_HEADER + 版本 1.3
[ msgp bin32 长度前缀 5B ][ meta 载荷 ]
    meta = [ header_ver u8 ][ meta_ver u8 ][ versions_len ]
           repeated versions_len 次:
             [ bin: FileMetaVersionHeader.marshal_msg ]
             [ bin: FileMetaVersion.marshal_msg     ]
[ msgp u32 CRC 5B ]   // xxh64(meta) 低 32 位
[ InlineData 载荷 ]   // 可选，小对象体
```

- CRC 不符 → `Error::FileCorrupt`，供 heal 判定 bitrot。
- `versions_len > meta.len()` 直接拒绝，防止按损坏长度预分配。

#### 7.6.4 版本层级内存结构

```
FileMeta { versions: Vec<FileMetaShallowVersion>, data: InlineData, meta_ver: u8 }
  └── FileMetaShallowVersion                    // 浅版本：列表/合并热路径
        ├── header: FileMetaVersionHeader       // 固定小结构，可不解析 body 做排序/quorum
        │     ├── version_id: Option<Uuid>
        │     ├── mod_time: Option<OffsetDateTime>   // 排序主键
        │     ├── signature: [u8; 4]                 // body 内容签名（xxh64 折叠）
        │     ├── version_type: VersionType          // Object/Delete/Legacy/Invalid
        │     ├── flags: u8                          // bit0 FreeVersion, bit1 UsesDataDir, bit2 InlineData
        │     └── ec_n / ec_m: u8                    // write_quorum 快速推导
        └── meta: Vec<u8>                            // FileMetaVersion 的 msgp 字节
              └── FileMetaVersion
                    ├── version_type: VersionType
                    ├── legacy_object: Option<MetaObjectV1>   // 旧格式
                    ├── object: Option<MetaObject>            // V2 当前格式
                    ├── delete_marker: Option<MetaDeleteMarker>
                    └── write_version: u64
```

**`MetaObject`**（V2 对象体，msgp 字段名固定）：

| 字段 | 作用 |
|------|------|
| `ID` / `DDir` | 版本 UUID / 数据目录 UUID |
| `EcAlgo/EcM/EcN/EcBSize/EcIndex/EcDist` | 纠删几何 |
| `CSumAlgo` | bitrot 算法 |
| `PartNums/PartETags/PartSizes/PartASizes/PartIdx` | 并行五元组描述分片 |
| `Size/MTime` | 对象长度与修改时间 |
| `MetaSys: HashMap<String,Vec<u8>>` | 内部元数据（双前缀 `x-rustfs-internal-*` + `x-minio-internal-*`） |
| `MetaUsr: HashMap<String,String>` | 用户元数据 |

**排序谓词 `sorts_before`**：mod_time 新者优先 → type 小者优先 → signature/version_id/flags 兜底，保证多盘一致的「最新版本」。

**`get_signature`**：清零 per-disk 的 `erasure_index`，map 用 XOR 无关序哈希折叠，其余 body 走 xxh64，折成 4 字节 —— 同内容不同盘一致，任何 body 分歧可检。

#### 7.6.5 `InlineData`（`filemeta_inline.rs`）

```
InlineData(Vec<u8>)
// 布局: [ver:u8=1][ msgpack map: version_key(str) -> body(bin) ]
```

- key 规则：null 版本 → `"null"`，否则 UUID 字符串。
- `physical_data_dir` 判定某版本是否仍占用 `data_dir`。
- `shared_data_dir_count`：删除版本时判断能否安全删数据目录。

#### 7.6.6 metacache（`metacache.rs`）— 列表/扫描路径

| 结构 | 作用 |
|------|------|
| `MetaCacheEntry` | `name` + 原始 `metadata: Vec<u8>` + 惰性 `cached: Option<FileMeta>` + `reusable` |
| `MetaCacheEntries(Vec<Option<MetaCacheEntry>>)` | 每盘一个槽位；`resolve` / `resolve_with_write_quorum` 跨盘合并 |
| `MetadataResolutionParams` | `dir_quorum / obj_quorum / write_quorum_slack / candidates` |
| `MetacacheWriter` / `MetacacheReader` | 流式编解码：`[stream_ver][bool+name+metadata...][bool=false]` |

**msgp 安全解码**（`msgp_decode.rs`）：`MAX_MSGP_ELEMENT_SIZE = 16 MiB` 防损坏长度 OOM；`read_exact_vec` / `prealloc_hint(≤4096)` 受控分配。

---

## 8. I/O 与内存管理结构

### 8.1 分层缓冲池 `BytesPool`（`io-core/src/pool.rs`）

**设计目标**：零拷贝缓冲复用，按大小分层。

```
BytesPool (根)
├── small_pool  : PoolTier  4 KiB – 64 KiB,  max 1000
├── medium_pool : PoolTier  64 KiB – 512 KiB, max 500
├── large_pool  : PoolTier  512 KiB – 4 MiB,  max 100
├── xlarge_pool : PoolTier  > 4 MiB,          max 25
└── metrics: Arc<BytesPoolMetrics>
```

**`PoolTier`（中间层）**：

| 字段 | 作用 |
|------|------|
| `buffer_size` / `max_buffers` | 本层块大小与并发上限 |
| `semaphore: Arc<Semaphore>` | 并发准入 |
| `available_buffers: Mutex<Vec<BytesMut>>` | 空闲块复用队列 |
| `tier_total_acquires / tier_pool_hits / tier_current_allocated_bytes` | 原子统计 |

**`PooledBuffer`（叶子，持有块）**：`buffer: ManuallyDrop<BytesMut>` + `tier: Option<Arc<PoolTier>>` + `_permit: OwnedSemaphorePermit`；Drop 时归还块并释放信号量许可。

`select_tier`：≤64K→small，≤512K→medium，≤4M→large，否则 xlarge。

### 8.2 io-core 其他内存/并发结构

| 模块 | 关键结构 | 作用 |
|------|----------|------|
| `backpressure` | `BackpressureMonitor` | `current: AtomicUsize` + CAS 环保证不超 `max_concurrent=32`；high=0.8→Critical，low=0.5→Warning |
| `deadlock_detector` | `DeadlockDetector` / `LockInfo` / `WaitGraphEdge` | 等待图死锁检测 |
| `lock_optimizer` | `LockOptimizer` / `LockGuard` / `LockStats` | 自适应自旋锁优化，全原子统计 |
| `io_profile` | `StorageProfile` / `IoPatternDetector` | 存储介质画像（NVMe/SSD/HDD）+ 滑窗访问模式检测 |
| `config` | `IoSchedulerConfig` / `IoPriorityQueueConfig` | I/O 调度配置 |
| `progress` | `OperationProgress` | 长操作进度 |

### 8.3 纠删码路径缓冲

- **`ShardBufferPool`**（`erasure/codec/workspace.rs`）：解码路径按 slot 复用分片 `Vec<u8>`，take 时**不清零**（由 reader 覆写，避免 1MiB shard 的 memset 浪费 ~4.8% GET CPU）
- **`BufferPool`**（`erasure/codec/buffer_pool.rs`）：按 2 的幂分 32 桶的 `Vec<u8>` 池，全局 `EC_BUFFER_POOL`，当前用于 bitrot_verify
- **`ErasureCache`**（`SetDisks` 持有）：按布局维度 memo 编解码器，克隆 set 时共享

### 8.4 rio 读写管道（`crates/rio`）

全部是 `pin_project_lite` 的 `AsyncRead` 包装器，通过能力宏向下透传 `EtagResolvable` / `HashReaderDetector` / `TryGetIndex`。

```
上传:  source → HardLimitReader → EtagReader → [CompressReader] → [EncryptReader] → HashReader → EC 分片
下载:  shard readers → DecryptReader → DecompressReader → LimitReader(范围) → HTTP
```

| 组件 | 内存/状态要点 |
|------|---------------|
| `EncryptReader` | `cipher: Aes256Gcm\|XkunlunAes256Gcm`；帧格式 `header(8B)+uvarint+ciphertext`；v2 帧固定 8KiB 明文块，可闭式偏移映射；多段 nonce 派生 |
| `CompressReader` / `DecompressReader` | 压缩：`temp_buffer` 聚满 1MiB → 建块 → 记 `Index`；解压：跨 poll 状态机 + `poisoned` 粘性错误 |
| `EtagReader` | 流式 `Md5`，EOF 时 finalize 成 hex etag |
| `HashReader` | 构造期按能力包装 Limit/Etag |
| **`Index`（压缩偏移索引）** | **有序向量（非树）**：`Vec<IndexInfo{compressed_offset, uncompressed_offset}>`，按 uncompressed 升序，1MiB 抽稀，上限 65536 项；序列化头尾 `s2idx\x00` / `\x00xdi2s`；用于 Range GET 定位压缩块 |
| `Writer` | 三态出口：`Cursor(Cursor<Vec<u8>>)` / `Http` / `Other(Box<dyn AsyncWrite>)` |

### 8.5 对象体缓存（`crates/object-data-cache`）— 多级索引内存结构

```
ObjectDataCache (facade, cache.rs)
├── backend: MokaBackend
│     ├── moka::future::Cache<ObjectDataCacheKey, ObjectDataCacheEntry>   ← 权重 LRU 本体
│     ├── StarshardIdentityIndex  (identity → ObjectDataCacheKeySet)      ← 失效索引
│     ├── ObjectDataCacheSingleflight (HashSet<key> in-flight 去重)
│     ├── ObjectDataCacheMemoryGate (准入内存门)
│     └── ClearFence / FillGenerationGuard (clear 与 fill 的代际栅栏)
└── config / stats
```

**`ObjectDataCacheKey`（write-unique 缓存键）**：
```
{ bucket, object, version_id, etag, size,
  data_dir_u128,          // 主 write-unique 锚（每次写入的 data_dir）
  mod_time_unix_nanos,    // 次锚
  body_variant }          // FullObjectPlainV1
```
设计意图：仅 etag+size 会在 MD5 碰撞 + 无版本覆盖时命中旧体；`data_dir` 保证覆盖写必换键。

**`ObjectDataCacheEntry`**：`{ bytes: Bytes, generation: u64 }`，权重 = key_bytes + body_bytes + 64。

**失效索引 `StarshardIdentityIndex`（分片哈希索引，根/中间/叶）**：

```
StarshardIdentityIndex (根)
└── by_object: AsyncShardedHashMap<ObjectDataCacheIdentity, ObjectDataCacheKeySet>
      ├── shard[0] : HashMap<Identity, KeySet>   ← 中间：分片
      ├── shard[1]
      └── ...
            └── ObjectDataCacheKeySet { keys: Vec<TrackedKey> }   ← 叶子
                  TrackedKey { key: ObjectDataCacheKey, generation: u64 }
```

- `max_keys_per_identity` 限额，超出从 Vec 头部（最旧）驱逐。
- `remove_generation` 只删匹配 generation 的条目，防止过期驱逐通知误删新 fill 的键。

**内存门 `ObjectDataCacheMemoryGate`（seqlock 快照）**：
```
MemorySnapshotCell {
  sequence: AtomicU64,          // 偶=稳定，奇=写者占用
  total_bytes / available_bytes: AtomicU64,
  issued_sequence / live_reserved / pending_release / sampled_reserved: AtomicU64,
}
```
- `try_claim` 失败关闭（跳过 fill，不影响对象读）；遥测过期（>15s）拒绝。
- `ObjectDataCacheMemoryReservation` Drop 释放；`wrap_bytes` 绑定到 `Bytes`。

**Singleflight**：`fills: Mutex<HashSet<Key>>`；`try_acquire` 选主，其余 `Busy` 跳过冗余 fill。

**ClearFence / FillGenerationGuard**：clear 时 bump generation 并等 `active_fills==0`；旧 generation 的 fill 不能发布。

### 8.6 树形/哈希索引结构总表

**未发现 B+树 / B 树实现。** 当前内存管理索引形态：

| 结构 | 位置 | 形态 | 根/中间/叶 |
|------|------|------|------------|
| **Starshard 分片哈希** | `object-data-cache/starshard_index.rs` | 分片哈希表 | 根 `AsyncShardedHashMap` → 中间 `shard[i]: HashMap` → 叶 `KeySet{Vec<TrackedKey>}` |
| **Moka 权重并发缓存** | `moka_backend.rs` | 分段 LRU | 分段哈希 + 权重容量 |
| **压缩偏移索引 `Index`** | `rio/compress_index.rs` | **有序向量**（非树） | 单层 `Vec<IndexInfo>`，1MiB 抽稀，上限 65536 |
| **xl.meta 版本表** | `FileMeta.versions` | 线性表按 `sorts_before` 排序 | 无树；`find_version` 线性扫描 |
| **`BytesPool` 四层** | `io-core/pool.rs` | 尺寸分层池 | 根 BytesPool → 中间 PoolTier → 叶 PooledBuffer |
| **`ShardBufferPool`** | `erasure/codec/workspace.rs` | slot 复用向量 | 单层 `Vec<Option<Vec<u8>>>` |

若需要 B+树级磁盘索引，目前 metacache/列表路径依赖 walkdir 流式 + 跨盘 merge，而非页式树。

---

### 8.7 关键交叉关系（元数据 ↔ 缓存 ↔ I/O）

```
xl.meta 磁盘字节
  └─ FileMeta { versions[], InlineData }
       └─ FileMetaShallowVersion { header, meta[] }
            └─ FileMetaVersion { MetaObject | MetaDeleteMarker | MetaObjectV1 }
                 ├─ MetaObject.meta_sys / meta_user  ──► FileInfo.metadata
                 └─ data_dir ──► ObjectDataCacheKey.data_dir_u128

对象体 I/O 管道 (rio)
  source ─ HardLimit ─ Etag ─ [Compress→Index] ─ [Encrypt] ─ Writer

对象体缓存 (object-data-cache)
  ObjectDataCache ─ moka.Cache(key→entry)
                 ─ StarshardIdentityIndex(identity→KeySet)  // 失效
                 ─ MemoryGate(seqlock snapshot)              // 准入
                 ─ Singleflight + ClearFence                 // 并发

缓冲 (io-core)
  BytesPool{small|medium|large|xlarge} ← PooledBuffer
  BackpressureMonitor / LockOptimizer / IoPatternDetector
```

---

## 9. 横切关注点

| 关注点 | 方案 |
|--------|------|
| **错误处理** | `thiserror` 具名错误（`StorageError` 等）；库代码不用 `anyhow` |
| **日志/追踪** | `tracing`；结构化字段；span 传递请求上下文 |
| **指标** | Prometheus 风格（`rustfs-obs`）；I/O 计数（`rustfs-io-metrics`） |
| **测试** | 单元测试 `#[cfg(test)]` 同文件；集成测试在 crate `tests/`；E2E 在 `crates/e2e_test/` |
| **全局状态** | 向 `InstanceContext` 迁移中（backlog#939）；`GLOBAL_*` 详见 `docs/architecture/global-state-inventory.md` |
| **锁纪律** | 多锁须注释获取顺序；不在 `.await` 间持写锁 |

---

## 10. 代码导航速查

| 问题 | 入口 |
|------|------|
| S3 PutObject 走到哪？ | `server/` 路由 → `app/object/put.rs` → `storage/ecfs` → `ecstore` → `rio` → `io-core` |
| 桶策略在哪强制？ | `app/bucket_usecase` → `crates/policy/` |
| 复制配置在哪？ | `admin/handlers/replication.rs`、`rustfs/src/site_replication/`、`ecstore/src/bucket/replication/` |
| 怎么加 admin 端点？ | `admin/handlers/` 加 handler，`admin/router.rs` 注册 |
| 怎么加指标？ | `crates/obs/src/metrics/`，经 `/minio/v2/metrics` 暴露 |
| 对象元数据格式？ | `crates/filemeta/`（xl.meta = `XL2 ` + msgp） |
| 纠删码实现？ | `crates/ecstore/src/erasure/` |
| 缓冲池？ | `crates/io-core/src/pool.rs` |
| 盘上拓扑格式？ | `crates/ecstore/src/layout/format.rs`（FormatV3） |

---

## 11. 已知结构问题（摘自 ARCHITECTURE.md）

1. **scanner/data-usage 重复 `.usage-cache.bin` 序列化类型** — 两个 `DataUsageCacheInfo` 定义待收敛（backlog#1828）
2. **ecstore 是巨型 crate**（265 文件）— 拆分计划见 `docs/architecture/ecstore-module-split-plan.md`
3. **三层背压/死锁策略桥接**（io-core / concurrency / storage）— 需继续用 bridge
4. **6 个 crate 仍导出裸 `Error` 命名**（thiserror 已统一，命名未统一）

---

## 12. 后续走读计划

本文档为总览；按规则（结合代码流程、关键结构体、内存管理结构）将继续展开：

1. **对象 PUT 完整路径** — 从 `app/object/put.rs` 到 `SetDisks::put_object` 的逐步代码流程（eager/streaming 写路径、Inline/SingleBlock/Pipeline 分类）
2. **对象 GET 完整路径** — 元数据 fanout → 路径决策 → BitrotReader → ParallelReader 解码
3. **xl.meta 元数据读写与 quorum 合并** — FileMeta 版本表、`MetadataQuorumAccumulator`、`MetaCacheEntries::resolve`
4. **纠删码编码/解码/修复** — Erasure 双编码器、Bitrot、Heal 数据流
5. **ECStore FormatV3 初始化与格式仲裁** — `init_format.rs`、多盘一致性校验
6. **IAM / 策略 / STS** — `crates/iam`、`crates/policy`、`auth.rs` 深入
7. **生命周期 / 复制 / 分层存储** — `ecstore/src/bucket/lifecycle`、`replication`、tiering
8. **scanner / heal / data-usage** — 后台扫描、修复、用量统计
9. **decommission / rebalance** — `core/pools.rs` 状态机
10. **io-core 背压、锁优化、缓冲池细节** — `BackpressureMonitor`、`LockOptimizer`、`BytesPool` 调优

---

*走读记录目录：`learning-record/`。后续各专题文档将按同一规范编写（功能流程 + 关键结构体 + 内存管理结构）。*
