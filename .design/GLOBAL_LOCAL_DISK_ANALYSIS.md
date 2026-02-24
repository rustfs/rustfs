# GLOBAL_LOCAL_DISK 分析报告

## 执行摘要

本报告分析了 RustFS 中 `GLOBAL_LOCAL_DISK` 的使用情况，发现该全局变量从未被赋值使用，实际功能已被 `GLOBAL_LOCAL_DISK_MAP`
替代。同时分析了 RustFS 在多机多盘环境下的数据副本处理机制。

## 1. GLOBAL_LOCAL_DISK 问题分析

### 1.1 问题发现

**位置**: `crates/ecstore/src/global.rs:43`

```rust
pub static ref GLOBAL_LOCAL_DISK: Arc<RwLock<Vec<Option<DiskStore> > > >
= Arc::new(RwLock::new(Vec::new()));
```

**关键发现**:

- ✅ **已定义**: 在 global.rs 中声明
- ❌ **未赋值**: 整个项目中无任何写操作
- ❌ **仅一处使用**: `rustfs/src/storage/metadata/mod.rs:76` (已修复)
- ❌ **永远为空**: 始终是空的 `Vec::new()`

### 1.2 实际使用的数据结构

项目中实际存储本地磁盘信息的是三个相关的全局变量：

#### 1.2.1 GLOBAL_LOCAL_DISK_MAP (主要使用)

```rust
pub static ref GLOBAL_LOCAL_DISK_MAP: Arc<RwLock<HashMap<String, Option<DiskStore> > > >
= Arc::new(RwLock::new(HashMap::new()));
```

**用途**: 按路径（endpoint）映射的磁盘存储
**赋值位置**:

- `store.rs:287` - ECStore 初始化
- `store.rs:1116` - init_local_disks()
- `set_disk.rs:1873` - renew_disk() 更新

**使用场景**:

- `store.rs:1068` - 查找磁盘
- `store.rs:1086` - 获取磁盘信息
- `peer_s3_client.rs:1002` - RPC 通信

#### 1.2.2 GLOBAL_LOCAL_DISK_SET_DRIVES (分布式场景)

```rust
pub static ref GLOBAL_LOCAL_DISK_SET_DRIVES: Arc<RwLock<TypeLocalDiskSetDrives> >
= Arc::new(RwLock::new(Vec::new()));

// 类型定义
type TypeLocalDiskSetDrives = Vec<Vec<Vec<Option<DiskStore>>>>;
// 结构：[Pool][Set][Drive]
```

**用途**: 按 Pool/Set/Drive 三级索引组织的磁盘存储
**使用场景**:

- 分布式擦除编码模式 (`is_dist_erasure()`)
- `set_disk.rs:1878` - 更新本地磁盘
- `sets.rs:129` - 获取本地磁盘驱动器

### 1.3 设计意图推测

从数据结构对比可以看出设计演进：

| 特性   | GLOBAL_LOCAL_DISK | GLOBAL_LOCAL_DISK_MAP | GLOBAL_LOCAL_DISK_SET_DRIVES |
|------|-------------------|-----------------------|------------------------------|
| 数据结构 | Vec (索引访问)        | HashMap (路径访问)        | Vec[Vec[Vec]] (3 级索引)         |
| 访问方式 | 按序号               | 按 endpoint 路径         | 按 Pool/Set/Drive             |
| 使用场景 | ❌ 未使用             | ✅ 通用场景                | ✅ 分布式擦除编码                    |
| 历史意义 | 早期设计              | 当前主要实现                | 分布式优化                        |

## 2. 多机多盘数据副本处理机制

### 2.1 架构概述

RustFS 采用 **擦除编码 (Erasure Coding)** 而非传统的副本复制来实现数据冗余和可靠性。

#### 核心概念

```
┌─────────────────────────────────────────────────────────────┐
│                    Endpoint Server Pools                     │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │   Pool 0     │  │   Pool 1     │  │   Pool N     │      │
│  │  ┌────────┐  │  │  ┌────────┐  │  │  ┌────────┐  │      │
│  │  │ Set 0  │  │  │  │ Set 0  │  │  │  │ Set 0  │  │      │
│  │  ├────────┤  │  │  ├────────┤  │  │  ├────────┤  │      │
│  │  │ Set 1  │  │  │  │ Set 1  │  │  │  │ Set 1  │  │      │
│  │  └────────┘  │  │  └────────┘  │  │  └────────┘  │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
└─────────────────────────────────────────────────────────────┘
                           ↓
              每个 Set 包含多个 Disk (Drive)
              ┌─────────────────────────────┐
              │  Data Disks  │ Parity Disks │
              │    D0 D1 D2  │    P0 P1     │
              └─────────────────────────────┘
```

### 2.2 擦除编码实现

#### 2.2.1 Reed-Solomon 编码

**位置**: `crates/ecstore/src/erasure_coding/erasure.rs`

```rust
pub struct Erasure {
    pub data_shards: usize,      // 数据分片数
    pub parity_shards: usize,    // 校验分片数
    encoder: Option<ReedSolomonEncoder>,
    pub block_size: usize,       // 块大小
}

impl Erasure {
    pub fn new(data_shards: usize, parity_shards: usize, block_size: usize) -> Self {
        let encoder = if parity_shards > 0 {
            Some(ReedSolomonEncoder::new(data_shards, parity_shards).unwrap())
        } else {
            None
        };
        // ...
    }
}
```

**特点**:

- 使用 SIMD 优化的 `reed-solomon-simd` 库
- 支持任意数据块和校验块组合
- 高性能编码/解码

#### 2.2.2 数据分布算法

**典型配置示例**:

```rust
// 4 磁盘配置：2 数据盘 + 2 校验盘
let erasure = Erasure::new(2, 2, 1024 * 1024); // 1MB block_size

// 容错能力：可容忍 2 个磁盘同时故障
// 存储效率：50% (实际数据 / 总存储)
```

**常见配置**:

| 总磁盘数 | 数据盘 | 校验盘 | 容错能力  | 存储效率  |
|------|-----|-----|-------|-------|
| 4    | 2   | 2   | 2 块磁盘 | 50%   |
| 6    | 4   | 2   | 2 块磁盘 | 66.7% |
| 8    | 4   | 4   | 4 块磁盘 | 50%   |
| 12   | 8   | 4   | 4 块磁盘 | 66.7% |
| 16   | 12  | 4   | 4 块磁盘 | 75%   |

### 2.3 写入流程 (Put Object)

**位置**: `crates/ecstore/src/set_disk.rs:3742+`

#### 写入步骤

```rust
async fn put_object(...) -> Result<ObjectInfo> {
    // 1. 计算写入仲裁数
    let write_quorum = data_drives - parity_drives;
    if data_drives == parity_drives {
        write_quorum += 1;
    }

    // 2. 创建擦除编码实例
    let erasure = erasure_coding::Erasure::new(
        fi.erasure.data_blocks,
        fi.erasure.parity_blocks,
        fi.erasure.block_size
    );

    // 3. 为每个磁盘创建 writer
    let mut writers = Vec::with_capacity(shuffle_disks.len());
    for disk in shuffle_disks.iter() {
        if disk.is_online().await {
            let writer = create_bitrot_writer(
                is_inline_buffer,
                Some(disk),
                RUSTFS_META_TMP_BUCKET,
                &tmp_object,
                erasure.shard_file_size(data.size()),
                erasure.shard_size(),
                HashAlgorithm::HighwayHash256,
            ).await?;
            writers.push(Some(writer));
        } else {
            writers.push(None);
        }
    }

    // 4. 检查是否满足写入仲裁数
    let nil_count = errors.iter().filter(|&e| e.is_none()).count();
    if nil_count < write_quorum {
        return Err(Error::other("not enough disks to write"));
    }

    // 5. 执行擦除编码写入
    let (reader, w_size) = erasure.encode(stream, &mut writers, write_quorum).await?;

    // 6. 写入元数据
    Self::write_unique_file_info(disks, org_bucket, bucket, prefix, &files, write_quorum).await?;

    // 7. 提交数据（移动临时文件到最终位置）
    // ...
}
```

#### 写入仲裁 (Write Quorum)

**计算规则** (`crates/filemeta/src/fileinfo.rs:291`):

```rust
pub fn write_quorum(&self, quorum: usize) -> usize {
    if self.erasure.data_blocks == self.erasure.parity_blocks {
        return self.erasure.data_blocks + 1; // 需要超过半数
    }
    // 标准情况：data_blocks 个磁盘即可
    self.erasure.data_blocks
}
```

**示例**:

- 配置 4:2 (4 数据 +2 校验) → write_quorum = 4
- 配置 2:2 (2 数据 +2 校验) → write_quorum = 3 (需要多数派)
- 配置 8:4 (8 数据 +4 校验) → write_quorum = 8

### 2.4 读取流程 (Get Object)

**位置**: `crates/ecstore/src/set_disk.rs:2338+`

#### 读取步骤

```rust
async fn get_object_with_fileinfo<W>(...) -> Result<()> {
    // 1. 创建擦除编码实例
    let erasure = erasure_coding::Erasure::new(
        fi.erasure.data_blocks,
        fi.erasure.parity_blocks,
        fi.erasure.block_size
    );

    // 2. 从可用磁盘创建 readers
    for disk in disks.iter() {
        if disk.is_some() && disk.is_online().await {
            let reader = disk.read_file(...).await?;
            readers.push(Some(reader));
        } else {
            readers.push(None);
        }
    }

    // 3. 解码并写入到输出流
    let (written, err) = erasure.decode(
        writer,
        readers,
        part_offset,
        part_length,
        part_size
    ).await;

    // 4. 如果解码失败，尝试从其他可用分片恢复
    // Reed-Solomon 可以从任意 data_shards 个完整分片恢复原始数据
}
```

**读取仲裁 (Read Quorum)**:

- 最少需要 `data_blocks` 个可用分片
- 可以是任意 data_blocks 个分片组合（数据盘或校验盘）
- 如果有更多分片可用，可以选择最快响应的

### 2.5 分布式场景处理

#### 2.5.1 多节点架构

```
Node 1 (192.168.1.101)          Node 2 (192.168.1.102)          Node 3 (192.168.1.103)
├── /mnt/disk1                  ├── /mnt/disk1                  ├── /mnt/disk1
├── /mnt/disk2                  ├── /mnt/disk2                  ├── /mnt/disk2
└── /mnt/disk3                  └── /mnt/disk3                  └── /mnt/disk3

                ↓ 组织为 Set (Pool 0, Set 0)
        
        [D0:Node1/disk1] [D1:Node1/disk2] [D2:Node2/disk1]
        [P0:Node2/disk2] [P1:Node3/disk1] [P2:Node3/disk2]
```

#### 2.5.2 本地与远程磁盘管理

**位置**: `crates/ecstore/src/sets.rs:91+`

```rust
pub async fn new(
    disks: Vec<Option<DiskStore>>,
    endpoints: &PoolEndpoints,
    fm: &FormatV3,
    pool_idx: usize,
    parity_count: usize,
) -> Result<Arc<Self>> {
    // 遍历每个 Set
    for i in 0..set_count {
        for j in 0..set_drive_count {
            let disk_id = fm.erasure.sets[i][j];
            let disk = find_disk_by_id(&disks, disk_id);

            // 如果是本地磁盘且为分布式模式
            if disk.is_local() && is_dist_erasure().await {
                // 从 GLOBAL_LOCAL_DISK_SET_DRIVES 获取
                let local_set_drives = GLOBAL_LOCAL_DISK_SET_DRIVES.read().await;
                disk = local_set_drives[pool_idx][i][j].clone();
            }

            set_drive.push(disk);
        }
        // ...
    }
}
```

#### 2.5.3 跨节点通信

**位置**: `crates/ecstore/src/rpc/peer_s3_client.rs`

- 使用 RPC 与远程节点通信
- 每个节点维护本地磁盘列表
- 通过 `GLOBAL_LOCAL_DISK_MAP` 快速查找本地磁盘
- 通过 Peer S3 Client 访问远程节点磁盘

### 2.6 自动修复 (Auto-Heal)

当检测到磁盘故障或数据损坏时，系统自动执行修复：

**位置**: `crates/ecstore/src/erasure_coding/heal.rs`

```rust
// 从可用分片重建丢失的分片
pub async fn heal_shard(
    erasure: &Erasure,
    available_shards: Vec<Option<Bytes>>,
    missing_index: usize,
) -> Result<Bytes> {
    // 1. 确保有足够的可用分片（至少 data_shards 个）
    let available_count = available_shards.iter().filter(|s| s.is_some()).count();
    if available_count < erasure.data_shards {
        return Err(Error::NotEnoughShards);
    }

    // 2. 使用 Reed-Solomon 解码重建
    let reconstructed = erasure.reconstruct_shard(available_shards, missing_index)?;

    // 3. 写回修复后的分片到故障磁盘
    // ...
}
```

### 2.7 数据分布策略

#### 2.7.1 分布算法

**位置**: `crates/ecstore/src/disk/format.rs`

```rust
pub struct FormatErasureV3 {
    pub version: FormatErasureVersion,
    pub this: Uuid,
    pub sets: Vec<Vec<Uuid>>,  // [Set][Disk] -> Disk UUID
    pub distribution_algo: DistributionAlgoVersion,
}
```

**V3 分布算法特点**:

- 每个磁盘分配唯一 UUID
- Set 间均匀分布数据
- 支持动态添加/移除磁盘（扩容缩容）

#### 2.7.2 Shuffle 策略

在写入时会 shuffle 磁盘顺序以实现负载均衡：

```rust
fn shuffle_disks_and_parts_metadata(
    disks: &[Option<DiskStore>],
    parts_metadata: &[FileInfo],
    fi: &FileInfo,
) -> (Vec<Option<DiskStore>>, Vec<FileInfo>) {
    // 基于对象名的哈希确定性打乱磁盘顺序
    // 确保同一对象总是使用相同的磁盘顺序
    // 不同对象使用不同的磁盘顺序实现负载均衡
}
```

## 3. 关键数据结构对比

### 3.1 磁盘存储层次

```
ECStore (全局存储引擎)
├── pools: Vec<Arc<Sets>>          // 存储池数组
│   └── Sets                        // 一组擦除编码集合
│       ├── set_disks: Vec<Arc<SetDisks>>  // Set 数组
│       │   └── SetDisks            // 一个擦除编码单元
│       │       └── disks: Vec<Option<DiskStore>>  // 物理磁盘
│       └── format: FormatV3        // 格式配置
└── disk_map: HashMap<usize, Vec<Option<DiskStore>>>  // Pool索引->磁盘映射
```

### 3.2 全局变量使用

| 全局变量                         | 数据结构                  | 用途    | 使用场景   |
|------------------------------|-----------------------|-------|--------|
| GLOBAL_LOCAL_DISK            | Vec                   | ❌ 未使用 | 早期设计遗留 |
| GLOBAL_LOCAL_DISK_MAP        | HashMap<String, Disk> | 路径查找  | 通用场景   |
| GLOBAL_LOCAL_DISK_SET_DRIVES | Vec[Vec[Vec]]         | 3级索引  | 分布式场景  |
| GLOBAL_OBJECT_API            | Arc\<ECStore\>        | 存储引擎  | 全局访问   |

## 4. 修复方案

### 4.1 已完成修复

✅ 修改 `rustfs/src/storage/metadata/mod.rs:76`

**修改前**:

```rust
let disks = rustfs_ecstore::global::GLOBAL_LOCAL_DISK.read().await;
```

**修改后**:

```rust
let disk_map = rustfs_ecstore::global::GLOBAL_LOCAL_DISK_MAP.read().await;
let mut legacy_fs = None;
for disk in disk_map.values().flatten() {
// ...
}
```

### 4.2 待执行清理

🔧 删除未使用的 `GLOBAL_LOCAL_DISK` 定义

**位置**: `crates/ecstore/src/global.rs:43`

**操作**: 删除以下代码

```rust
pub static ref GLOBAL_LOCAL_DISK: Arc<RwLock<Vec<Option<DiskStore> > > >
= Arc::new(RwLock::new(Vec::new()));
```

**影响范围**: 无影响（该变量未被使用）

## 5. 总结

### 5.1 GLOBAL_LOCAL_DISK 问题

- ❌ **从未被赋值**: 整个项目中无写操作
- ❌ **无法获取真实磁盘**: 永远为空
- ✅ **已被替代**: `GLOBAL_LOCAL_DISK_MAP` 实现相同功能
- 🗑️ **建议删除**: 属于技术债务

### 5.2 数据副本机制

RustFS 采用**擦除编码**而非传统副本：

**优势**:

- ✅ 更高的存储效率（66.7% vs 33.3% for 3 副本）
- ✅ 更强的容错能力（可配置任意数量校验块）
- ✅ 自动数据修复能力
- ✅ 跨节点数据分布
- ✅ SIMD 优化的高性能编解码

**实现关键点**:

1. **Reed-Solomon 编码**: 数学上保证从任意 N 个分片恢复原始数据
2. **Write Quorum**: 确保足够多的分片写入成功
3. **Read Quorum**: 至少需要 data_blocks 个分片读取
4. **分布式架构**: 支持跨节点数据分布和访问
5. **自动修复**: 后台任务持续检查和修复数据

### 5.3 多机多盘场景

**数据流向**:

```
Client Request
    ↓
ECStore (选择 Pool 和 Set)
    ↓
SetDisks (擦除编码处理)
    ↓
Multiple DiskStore (并行写入多个磁盘)
    ├── Local Disks (本地磁盘，直接写入)
    └── Remote Disks (远程磁盘，RPC 调用)
```

**关键组件**:

1. **Endpoint Server Pools**: 节点和磁盘的逻辑组织
2. **Erasure Coding**: 数据编码和解码
3. **Peer S3 Client**: 跨节点通信
4. **Lock Clients**: 分布式锁协调
5. **Auto Heal**: 自动数据修复

## 6. 推荐阅读

- `crates/ecstore/src/erasure_coding/erasure.rs` - 擦除编码核心实现
- `crates/ecstore/src/set_disk.rs` - Set 级别的对象读写
- `crates/ecstore/src/store.rs` - ECStore 存储引擎
- `crates/ecstore/src/rpc/peer_s3_client.rs` - 节点间通信
- `crates/heal/` - 自动修复系统

---

**报告日期**: 2026-02-24  
**版本**: RustFS v0.0.5  
**作者**: AI Assistant

