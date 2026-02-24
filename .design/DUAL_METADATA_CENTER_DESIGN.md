# RustFS 双元数据中心架构详细设计文档

**版本**: 1.0  
**日期**: 2026-02-23  
**作者**: RustFS Architecture Team  
**状态**: Active Development

---

## 目录

1. [执行摘要](#执行摘要)
2. [核心架构设计](#核心架构设计)
3. [数据结构定义](#数据结构定义)
4. [存储引擎层](#存储引擎层)
5. [平滑过渡策略](#平滑过渡策略)
6. [性能优化](#性能优化)
7. [全局去重与内容寻址](#全局去重与内容寻址)
8. [面向未来的架构](#面向未来的架构)
9. [实现路线图](#实现路线图)
10. [性能指标](#性能指标)

---

## 执行摘要

RustFS 正在从**文件系统元数据中心** (`xl.meta` + `part.1`) 迁移到**高性能 KV 元数据中心** (SurrealKV + Ferntree +
SurrealMX)。本设计文档阐述了如何利用三个核心组件构建一个单机高性能元数据引擎，解决海量小文件场景中的关键问题：

| 问题             | 当前状态                | 新架构目标                 |
|----------------|---------------------|-----------------------|
| 小文件 IOPS 放大    | 2 次 IO (meta + data) | 1 次 IO (内联存储)          |
| Inode 耗尽       | 受限于文件系统             | 无限制 KV 容量             |
| ListObjects 性能 | 文件系统目录遍历 O(n)       | B+ 树范围扫描 O(log n)     |
| SSD 缓存利用率      | 低                   | 中~高 (SurrealMX 分层)    |
| 全局去重能力         | 无                   | 基于 Content Hash 的 CAS |

### 核心价值

- **3-5 倍 IOPS 提升**: 小文件从 2 次 IO 减少到 1 次
- **毫秒级 ListObjects**: Ferntree B+ 树索引替代文件系统扫描
- **无限元数据扩展**: 脱离文件系统 Inode 限制
- **零拷贝秒传**: 内容寻址 (CAS) 支持软链接级别的重复数据消除

---

## 核心架构设计

### 1. 架构整体视图

```
┌─────────────────────────────────────────────────────────────┐
│                    RustFS Application Layer                 │
│           (S3 API / Object Storage Operations)              │
└────────────────────┬────────────────────────────────────────┘
                     │
         ┌───────────▼──────────────────┐
         │   MetadataEngine (Trait)     │
         │  - put_object                │
         │  - get_object_reader         │
         │  - list_objects              │
         │  - delete_object             │
         │  - update_metadata           │
         └───────────┬──────────────────┘
                     │
    ┌────────────────┼────────────────┐
    │                │                │
    ▼                ▼                ▼
┌─────────┐  ┌──────────────┐  ┌─────────────────┐
│LocalMeta│  │DualMetadata  │  │LegacyAdapter    │
│Engine   │  │Center        │  │(Migration)      │
│(Modern) │  │(HA)          │  │                 │
└────┬────┘  └──────┬───────┘  └────────┬────────┘
     │              │                   │
     ├──────────────┼───────────────────┤
     │              │                   │
     ▼              ▼                   ▼
  ┌──────────────────────────────────────────────┐
  │         Unified Metadata Subsystem           │
  ├──────────────────────────────────────────────┤
  │                                              │
  │  ┌──────────────┐  ┌─────────────────────┐  │
  │  │  SurrealKV   │  │  Ferntree (B+ Tree) │  │
  │  │              │  │                     │  │
  │  │ • ACID Trans │  │ • Range Scan        │  │
  │  │ • MVCC       │  │ • Prefix Search     │  │
  │  │ • Compress   │  │ • O(log n) seek     │  │
  │  │              │  │                     │  │
  │  └──────────────┘  └─────────────────────┘  │
  │                                              │
  │  ┌────────────────────────────────────────┐ │
  │  │      SurrealMX (Storage Manager)       │ │
  │  │                                        │ │
  │  │ • Cache Layer (Memory)                 │ │
  │  │ • SSD/HDD Tiering                      │ │
  │  │ • Hot/Cold Data Movement               │ │
  │  │ • Reference Counting                   │ │
  │  └────────────────────────────────────────┘ │
  │                                              │
  └──────────────────────────────────────────────┘
     │              │                   │
     ▼              ▼                   ▼
  ┌────────────────────────────────────────────┐
  │        Local Disk Storage (Data Plane)     │
  ├────────────────────────────────────────────┤
  │                                            │
  │  ┌──────────┐  ┌──────────┐  ┌──────────┐ │
  │  │ KV DB    │  │ Index DB │  │ MX Data  │ │
  │  │ (mmap)   │  │ (in-mem) │  │ Store    │ │
  │  └──────────┘  └──────────┘  └──────────┘ │
  │                                            │
  └────────────────────────────────────────────┘
```

### 2. LocalMetadataEngine 结构设计

```rust
#[derive(Clone)]
pub struct LocalMetadataEngine {
    /// SurrealKV: 持久化对象元数据存储 (ACID, MVCC)
    /// Key: "buckets/{bucket}/objects/{key}/meta"
    /// Value: ObjectMetadata (序列化)
    kv_store: Arc<KvStore>,

    /// Ferntree: B+ 树索引层 (内存 + 持久化)
    /// Key: "{bucket}/{prefix}/{object_name}"
    /// Value: IndexMetadata (轻量级列表元数据)
    /// 用途：毫秒级 ListObjects, 前缀搜索，范围扫描
    index_tree: Arc<IndexTree<String, Bytes>>,

    /// SurrealMX: 存储管理器 (数据放置，缓存，IO 优化)
    /// 功能：小文件内联，大文件聚合，热冷分层，引用计数
    storage_manager: Arc<dyn StorageManager>,

    /// 传统文件系统接口 (迁移和回退)
    /// 用途：读时迁移 (Lazy Load), 元数据一致性检查
    legacy_fs: Arc<rustfs_ecstore::disk::local::LocalDisk>,

    /// 并发控制：支持 MVCC 风格的隔离
    /// 读时修复 (Read Repair) 锁
    repair_locks: Arc<DashMap<String, ()>>,

    /// 后台垃圾回收器
    gc: Arc<GarbageCollector>,
}
```

### 3. 数据流架构 (Write/Read/List)

#### Write 流程 (Dual-Write + Shadow Write)

```
User Write Request
        │
        ▼
   Check Size
        │
   ┌────┴────┐
   │          │
<128KB    >=128KB
   │          │
   ▼          ▼
Inline    Chunked
Write     Write
   │          │
   │          ▼
   │    ChunkedWriter
   │    (5MB chunks)
   │          │
   │    ┌─────┴──────┐
   │    ▼            ▼
   │  Hash      Ref Count
   │  (BLAKE3)  (inc_ref)
   │    │            │
   └────┴────────────┤
        │            │
        ▼            ▼
    Create ObjectMetadata
        │
        ▼
    Transaction Begin
        │
   ┌────┴────────────────────┐
   │                         │
   ▼                         ▼
Set KV Meta             Update Index Tree
   │                     (Ferntree)
   │                         │
   └────────────┬────────────┘
                │
                ▼
        Transaction Commit
                │
                ▼
        Return ObjectInfo
                │
                ▼ (Optional Shadow)
        Sync to Secondary
           (Background)
```

**关键特性**:

1. **原子性**: KV + Index 在单一事务中更新
2. **异步 Shadow Write**: 异步同步至从元数据中心 (Dual Center)
3. **Ref Count Increment**: 对所有数据块执行引用计数递增

#### Read 流程 (多层次回退)

```
User Read Request
        │
        ▼
 Query KV Store
        │
   ┌────┴────┐
   │          │
Found     Not Found
   │          │
   ▼          ▼
Load Meta   Query Legacy FS
   │        (xl.meta)
   │          │
   │      ┌───┴────┐
   │      │        │
   │    Found   Not Found
   │      │        │
   │      ▼        ▼
   │   Migrate  Error
   │   (Async)
   │      │
   └──────┴──────┐
                 │
        Check Data Layout
                 │
    ┌────────────┼────────────┐
    │            │            │
Inline        Local Path    BlockRef
    │            │            │
    ▼            ▼            ▼
Load from    Load from     Resolve
Metadata     StorageManager  Block
    │            │            │
    └────────────┴────────────┘
                 │
                 ▼
         Create GetObjectReader
                 │
                 ▼
         Stream to Client
```

**Read Repair 机制**:

- 若从 KV 查询失败但在 Legacy FS 中找到，则启动异步迁移任务
- 迁移完成后，新读请求直接从 KV 读取（加速）

#### List 流程 (Ferntree 范围扫描)

```
ListObjects Request
(bucket, prefix, marker, delimiter, max_keys)
        │
        ▼
    Parse Parameters
        │
        ▼
    Build Range
  (start_key, end_key)
        │
        ▼
    Ferntree Range Scan
   (O(log n) seek)
        │
        ▼
   Iterate Results
        │
   ┌────┴──────────────┐
   │                   │
Handle Delimiter    Accumulate
   │                  Objects
   ├──────────┬───────┤
   │          │       │
   ▼          ▼       ▼
Common    Objects  Marker
Prefixes           (Next)
   │          │       │
   └──────────┴───────┘
        │
        ▼
  Return ListObjectsV2Info
(objects, prefixes, is_truncated, next_marker)
```

**性能特性**:

- O(log n) 初始定位 (B+ 树树高)
- O(k) 顺序扫描 (k = 返回对象数)
- 无文件系统目录遍历开销

---

## 数据结构定义

### 1. ObjectMetadata (主元数据)

存储在 SurrealKV 中的完整对象元数据：

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectMetadata {
    // === 身份标识 ===
    pub bucket: String,
    pub key: String,
    pub version_id: String,

    // === 内容寻址 (CAS) ===
    /// SHA256 / BLAKE3 内容哈希
    /// 用途：去重，数据完整性验证，版本管理
    pub content_hash: String,

    // === 大小与时间 ===
    pub size: u64,
    pub created_at: i64,          // Unix 时间戳
    pub mod_time: i64,

    // === 用户定义元数据 ===
    /// x-amz-meta-* 标签
    pub user_metadata: HashMap<String, String>,

    /// Content-Type, Cache-Control, 等系统元数据
    pub system_metadata: HashMap<String, String>,

    // === 纠删码信息 ===
    /// 若对象采用纠删码存储则填充此字段
    /// (与 DataLayout 分离，支持未来的分层纠删码)
    pub erasure_info: Option<ErasureInfo>,

    // === 数据布局信息 ===
    /// 标记：是否为内联数据 (<128KB)
    pub is_inline: bool,

    /// 内联数据负荷 (若 is_inline == true)
    /// 设计选项：也可存放在相邻 KV 键 ".../{key}/inline" 以保持元数据键紧凑
    pub inline_data: Option<Bytes>,

    /// 大文件分块信息 (若非内联)
    /// 每块记录：hash, size, offset
    pub chunks: Option<Vec<ChunkInfo>>,

    // === 内部控制字段 ===
    /// 删除标记 (支持软删除与垃圾回收)
    pub is_deleted: bool,

    /// 访问计数 (用于热度追踪)
    pub access_count: u64,

    /// 最后访问时间 (用于 LRU 驱逐)
    pub last_accessed: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkInfo {
    /// 块数据的 BLAKE3 哈希
    pub hash: String,
    /// 块数据大小
    pub size: u64,
    /// 在完整对象中的偏移量
    pub offset: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErasureInfo {
    pub algorithm: String,        // "RS", "XOR", 等
    pub data_blocks: usize,
    pub parity_blocks: usize,
    pub block_size: usize,
    pub index: usize,             // 本副本的块索引
    pub distribution: Vec<usize>, // 副本分布 (节点 ID)
    pub checksums: Vec<Option<String>>, // 块校验和
}
```

**KV 键空间设计**:

```
元数据键空间 (SurrealKV):
  buckets/{bucket}/objects/{key}/meta        → ObjectMetadata (JSON)
  buckets/{bucket}/objects/{key}/inline      → Vec<u8> (可选, 超大内联数据)
  refs/{content_hash}                        → u64 (引用计数)
  buckets/{bucket}/access_log/{key}          → AccessInfo (热度追踪)
```

### 2. IndexMetadata (轻量级索引元数据)

存储在 Ferntree B+ 树中的最小化索引数据，用于快速列表操作：

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexMetadata {
    /// 对象大小 (字节)
    pub size: u64,

    /// 最后修改时间戳
    pub mod_time: i64,

    /// ETag (通常为内容哈希前缀或 MD5)
    pub etag: String,

    /// 对象在 KV 中的版本 ID (可选，用于版本管理)
    pub version_id: Option<String>,

    /// 存储类型标记 (STANDARD, INTELLIGENT_TIERING, GLACIER, 等)
    pub storage_class: Option<String>,

    /// 是否为删除标记 (Delete Marker, 用于版本控制)
    pub is_delete_marker: bool,
}

/// Ferntree 键空间设计：
/// Key: "{bucket}/{prefix}/{object_name}"
/// Value: IndexMetadata (JSON 序列化)
/// 特性：按字典序存储，支持前缀范围扫描，分隔符处理
```

### 3. 数据布局枚举 (DataLayout)

```rust
/// 描述对象数据在物理存储中的位置和组织方式
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DataLayout {
    /// 小文件 (<128KB): 数据直接内联在 ObjectMetadata 中
    /// 优势：单次 KV 读取，无额外 IO
    /// 限制：KV 值大小受限 (通常 < 1MB)
    Inline {
        data: Bytes,
        /// 可选：压缩算法 ("zstd", "lz4", 等)
        compression: Option<String>,
    },

    /// 传统路径引用：数据存储在文件系统上的特定路径
    /// 用途：迁移期间，兼容旧 xl.meta 对象
    LocalPath {
        path: PathBuf,
        /// 从文件哪个字节开始读
        offset: u64,
        /// 读多少字节
        length: u64,
    },

    /// 块存储：数据分割成多个块，分别存储
    /// 每块有独立的内容哈希 (CAS)
    Chunked {
        chunks: Vec<ChunkInfo>,
        /// 总大小 (for 快速获取)
        total_size: u64,
    },

    /// 聚合块：多个对象打包到一个大块文件中
    /// 用途：Phase 2 优化，减少文件数量
    BlockAggregated {
        block_id: String,
        offset: u64,
        length: u64,
    },

    /// 分层存储：数据根据访问频率分布在多个介质上
    /// 用途：Phase 3, 冷热分层
    Tiered {
        hot_blocks: Vec<ChunkInfo>,    // 在高速存储上 (NVMe)
        cold_blocks: Vec<ChunkInfo>,   // 在低速存储上 (HDD)
    },
}
```

### 4. 引用计数结构 (Reference Control)

```rust
/// 支持全局去重和秒传功能的引用计数机制
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefControl {
    /// 内容哈希
    pub content_hash: String,

    /// 引用计数
    pub ref_count: u64,

    /// 创建时间
    pub created_at: i64,

    /// 最后访问时间
    pub last_accessed: i64,

    /// 数据大小 (用于配额计算)
    pub size: u64,

    /// 所有者列表 (bucket/key 对)
    pub owners: HashSet<String>,

    /// 锁定标记 (防止过期回收)
    pub pinned: bool,
}

/// KV 键空间：refs/{content_hash} → u64 (简化版，仅存储计数)
/// 扩展空间：refs:info/{content_hash} → RefControl (详细信息)
```

### 5. 目录节点结构 (DirectoryNode)

虽然 RustFS 是对象存储，但支持类似目录的逻辑组织：

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectoryNode {
    /// 完整路径 (e.g., "bucket/path/to/dir/")
    pub path: String,

    /// 该目录下的对象计数
    pub object_count: u64,

    /// 该目录的总大小 (字节)
    pub total_size: u64,

    /// 最后修改时间 (该目录下任何对象被修改时更新)
    pub mod_time: i64,

    /// 子目录列表 (用于树形浏览)
    pub sub_dirs: Vec<String>,

    /// 该目录的访问权限信息 (可选)
    pub acl: Option<String>,
}

/// Ferntree 键空间："bucket/path/to/dir/" → DirectoryNode 序列化
```

---

## 存储引擎层

### 1. MetadataEngine Trait 定义

```rust
/// 核心元数据引擎接口
/// 支持三种实现：LocalMetadataEngine, DualMetadataCenter, LegacyAdapter
#[async_trait]
pub trait MetadataEngine: Send + Sync {
    /// 上传对象
    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        reader: Box<dyn AsyncRead + Send + Unpin>,
        size: u64,
        opts: ObjectOptions,
    ) -> Result<ObjectInfo>;

    /// 获取对象数据流
    /// 返回异步读取器和对象元数据
    async fn get_object_reader(
        &self,
        bucket: &str,
        key: &str,
        opts: &ObjectOptions,
    ) -> Result<GetObjectReader>;

    /// 获取对象元数据 (不含数据)
    async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        opts: ObjectOptions,
    ) -> Result<ObjectInfo>;

    /// 删除对象
    async fn delete_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<()>;

    /// 更新元数据 (如标签、权限等)
    async fn update_metadata(
        &self,
        bucket: &str,
        key: &str,
        info: ObjectInfo,
    ) -> Result<()>;

    /// 列表对象 (支持前缀、分隔符、分页)
    async fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        marker: Option<String>,
        delimiter: Option<String>,
        max_keys: usize,
    ) -> Result<ListObjectsV2Info>;

    /// 额外操作：获取对象大小
    async fn get_object_size(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<u64> {
        let info = self.get_object(bucket, key, ObjectOptions::default()).await?;
        Ok(info.size as u64)
    }

    /// 额外操作：检查对象是否存在
    async fn exists(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<bool> {
        match self.get_object(bucket, key, ObjectOptions::default()).await {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }
}
```

### 2. LocalMetadataEngine 实现

#### 初始化 (Initialize)

```rust
impl LocalMetadataEngine {
    pub async fn initialize(
        kv_path: impl AsRef<Path>,
        mx_path: impl AsRef<Path>,
        legacy_disk: Arc<LocalDisk>,
    ) -> Result<Arc<Self>> {
        // 1. 初始化 KV 存储
        let kv_store = Arc::new(
            new_kv_store(kv_path)
                .await
                .context("Failed to initialize KV store")?
        );

        // 2. 初始化索引树 (Ferntree, 内存或持久化)
        let index_tree = Arc::new(
            new_index_tree()
                .await
                .context("Failed to initialize index tree")?
        );

        // 3. 初始化存储管理器 (SurrealMX)
        let storage_manager = Arc::new(
            MxStorageManager::new(mx_path)
                .context("Failed to initialize storage manager")?
        );

        // 4. 创建引擎实例
        let engine = Arc::new(Self {
            kv_store: kv_store.clone(),
            index_tree: index_tree.clone(),
            storage_manager: storage_manager.clone(),
            legacy_fs: legacy_disk,
            repair_locks: Arc::new(DashMap::new()),
            gc: Arc::new(GarbageCollector::new(
                kv_store,
                storage_manager,
            )),
        });

        // 5. 启动后台垃圾回收
        let gc = engine.gc.clone();
        tokio::spawn(async move {
            gc.start().await;
        });

        // 6. 预热缓存 (可选)
        engine.warmup_cache().await.ok();

        Ok(engine)
    }

    /// 预热缓存：从 KV 加载常用元数据
    async fn warmup_cache(&self) -> Result<()> {
        // TODO: 实现 LRU 缓存预热逻辑
        Ok(())
    }
}
```

#### 事务管理器 (Transaction Manager)

```rust
pub struct TransactionManager {
    kv_tx: surrealkv::Transaction,
}

impl TransactionManager {
    /// 开启事务
    pub fn begin(kv_store: &Arc<KvStore>) -> Result<Self> {
        let kv_tx = kv_store
            .begin()
            .map_err(|e| Error::other(e.to_string()))?;

        Ok(Self { kv_tx })
    }

    /// 在事务中递增引用计数
    pub async fn inc_ref(&mut self, hash: &str) -> Result<()> {
        let key = format!("refs/{}", hash);
        let count = if let Some(val) = self.kv_tx.get(key.as_bytes())? {
            u64::from_be_bytes(Self::parse_u64(&val)?)
        } else {
            0
        };

        let new_count = count + 1;
        self.kv_tx.set(key.as_bytes(), &new_count.to_be_bytes())?;
        Ok(())
    }

    /// 在事务中递减引用计数
    pub async fn dec_ref(&mut self, hash: &str) -> Result<u64> {
        let key = format!("refs/{}", hash);
        let count = if let Some(val) = self.kv_tx.get(key.as_bytes())? {
            u64::from_be_bytes(Self::parse_u64(&val)?)
        } else {
            0
        };

        if count == 0 {
            return Ok(0);
        }

        let new_count = count - 1;
        if new_count == 0 {
            self.kv_tx.delete(key.as_bytes())?;
        } else {
            self.kv_tx.set(key.as_bytes(), &new_count.to_be_bytes())?;
        }

        Ok(new_count)
    }

    /// 提交事务
    pub async fn commit(self) -> Result<()> {
        self.kv_tx.commit().await.map_err(|e| Error::other(e.to_string()))?;
        Ok(())
    }

    /// 回滚事务 (自动在 Drop 时执行)
    pub async fn rollback(self) -> Result<()> {
        // SurrealKV transaction 会自动回滚未提交的变更
        Ok(())
    }

    fn parse_u64(buf: &[u8]) -> Result<[u8; 8]> {
        if buf.len() < 8 {
            return Err(Error::other("Invalid u64 format"));
        }
        let mut arr = [0u8; 8];
        arr.copy_from_slice(&buf[..8]);
        Ok(arr)
    }
}
```

### 3. StorageManager Trait (SurrealMX 包装)

```rust
#[async_trait]
pub trait StorageManager: Send + Sync {
    /// 写入数据块 (使用内容哈希作为 Key)
    async fn write_data(&self, content_hash: &str, data: Bytes) -> Result<()>;

    /// 读取数据块
    async fn read_data(&self, content_hash: &str) -> Result<Bytes>;

    /// 检查数据块是否存在 (去重)
    async fn exists(&self, content_hash: &str) -> bool;

    /// 删除数据块
    async fn delete_data(&self, content_hash: &str) -> Result<()>;

    /// 获取数据块大小 (无需读取全部数据)
    async fn get_size(&self, content_hash: &str) -> Result<u64>;

    /// 数据块迁移 (热冷分层)
    async fn migrate(&self, content_hash: &str, from_tier: Tier, to_tier: Tier) -> Result<()>;
}

pub enum Tier {
    Hot,      // NVMe/Memory
    Warm,     // SSD
    Cold,     // HDD
}
```

#### SurrealMX 具体实现

```rust
pub struct MxStorageManager {
    db: Arc<Database>,
    /// 内存缓存配额 (字节)
    cache_quota: u64,
    /// 已使用缓存大小
    cache_usage: Arc<AtomicU64>,
}

impl MxStorageManager {
    pub fn new(path: impl AsRef<Path>, cache_quota: u64) -> Result<Self> {
        let opts = DatabaseOptions {
            cache_size: cache_quota,
            ..Default::default()
        };
        let persistence_opts = PersistenceOptions::new(path.as_ref());

        let db = Database::new_with_persistence(opts, persistence_opts)
            .map_err(|e| Error::other(e.to_string()))?;

        Ok(Self {
            db: Arc::new(db),
            cache_quota,
            cache_usage: Arc::new(AtomicU64::new(0)),
        })
    }

    /// 获取缓存使用率 (百分比)
    pub fn cache_usage_percent(&self) -> f32 {
        let usage = self.cache_usage.load(Ordering::Relaxed);
        (usage as f32 / self.cache_quota as f32) * 100.0
    }

    /// 检查是否需要驱逐 (超过 90% 触发)
    fn should_evict(&self) -> bool {
        self.cache_usage_percent() > 90.0
    }
}

#[async_trait]
impl StorageManager for MxStorageManager {
    async fn write_data(&self, content_hash: &str, data: Bytes) -> Result<()> {
        let db = self.db.clone();
        let key = content_hash.to_string();
        let val = data.to_vec();
        let size = val.len() as u64;

        // 检查缓存压力
        let cache_usage = self.cache_usage.load(Ordering::Relaxed);
        if cache_usage + size > self.cache_quota && self.should_evict() {
            // TODO: 触发 LRU 驱逐或热冷迁移
        }

        tokio::task::spawn_blocking(move || {
            let mut tx = db.transaction(true);
            tx.put(&key, &val).map_err(|e| Error::other(e.to_string()))?;
            tx.commit().map_err(|e| Error::other(e.to_string()))?;
            Ok(())
        })
            .await
            .map_err(|e| Error::other(e.to_string()))?
    }

    async fn read_data(&self, content_hash: &str) -> Result<Bytes> {
        let db = self.db.clone();
        let key = content_hash.to_string();

        tokio::task::spawn_blocking(move || {
            let tx = db.transaction(false);
            match tx.get(&key) {
                Ok(Some(val)) => Ok(Bytes::from(val)),
                Ok(None) => Err(Error::other("Data not found")),
                Err(e) => Err(Error::other(e.to_string())),
            }
        })
            .await
            .map_err(|e| Error::other(e.to_string()))?
    }

    async fn exists(&self, content_hash: &str) -> bool {
        let db = self.db.clone();
        let key = content_hash.to_string();

        tokio::task::spawn_blocking(move || {
            let tx = db.transaction(false);
            tx.get(&key).ok().flatten().is_some()
        })
            .await
            .unwrap_or(false)
    }

    async fn delete_data(&self, content_hash: &str) -> Result<()> {
        let db = self.db.clone();
        let key = content_hash.to_string();

        tokio::task::spawn_blocking(move || {
            let mut tx = db.transaction(true);
            tx.del(&key).map_err(|e| Error::other(e.to_string()))?;
            tx.commit().map_err(|e| Error::other(e.to_string()))?;
            Ok(())
        })
            .await
            .map_err(|e| Error::other(e.to_string()))?
    }

    async fn get_size(&self, content_hash: &str) -> Result<u64> {
        let data = self.read_data(content_hash).await?;
        Ok(data.len() as u64)
    }

    async fn migrate(&self, _content_hash: &str, _from: Tier, _to: Tier) -> Result<()> {
        // TODO: Phase 3 实现：热冷数据迁移
        Ok(())
    }
}
```

### 4. DualMetadataCenter (高可用实现)

```rust
/// 双元数据中心架构：Primary + Secondary
/// 写操作同步至两个中心，读操作可切换和修复
#[derive(Clone)]
pub struct DualMetadataCenter {
    primary: Arc<dyn MetadataEngine>,
    secondary: Arc<dyn MetadataEngine>,
    consistency_checker: Arc<ConsistencyChecker>,
}

impl DualMetadataCenter {
    pub fn new(
        primary: Arc<dyn MetadataEngine>,
        secondary: Arc<dyn MetadataEngine>,
    ) -> Self {
        Self {
            primary,
            secondary,
            consistency_checker: Arc::new(ConsistencyChecker::new(
                primary.clone(),
                secondary.clone(),
            )),
        }
    }

    /// 启动后台一致性检查
    pub async fn start_consistency_checker(&self) {
        let checker = self.consistency_checker.clone();
        tokio::spawn(async move {
            checker.run().await;
        });
    }
}

#[async_trait]
impl MetadataEngine for DualMetadataCenter {
    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        reader: Box<dyn AsyncRead + Send + Unpin>,
        size: u64,
        opts: ObjectOptions,
    ) -> Result<ObjectInfo> {
        // 主中心写入
        let info = self.primary.put_object(bucket, key, reader, size, opts.clone()).await?;

        // 异步影子写入 (Shadow Write) 至从中心
        let secondary = self.secondary.clone();
        let bucket_clone = bucket.to_string();
        let key_clone = key.to_string();
        let info_clone = info.clone();

        tokio::spawn(async move {
            if let Err(e) = secondary.update_metadata(&bucket_clone, &key_clone, info_clone).await {
                tracing::warn!("Shadow write to secondary failed: {:?}", e);
            }
        });

        Ok(info)
    }

    async fn get_object_reader(&self, bucket: &str, key: &str, opts: &ObjectOptions) -> Result<GetObjectReader> {
        // 读取修复：优先尝试主中心，失败则从从中心读
        match self.primary.get_object_reader(bucket, key, opts).await {
            Ok(reader) => Ok(reader),
            Err(e) => {
                tracing::warn!("Primary read failed: {:?}, trying secondary...", e);

                // 从从中心读取
                let reader = self.secondary.get_object_reader(bucket, key, opts).await?;

                // 异步修复主中心
                let primary = self.primary.clone();
                let b = bucket.to_string();
                let k = key.to_string();
                let info = reader.object_info.clone();

                tokio::spawn(async move {
                    if let Err(e) = primary.update_metadata(&b, &k, info).await {
                        tracing::error!("Read repair to primary failed: {:?}", e);
                    }
                });

                Ok(reader)
            }
        }
    }

    // ... 其他方法类似实现
}
```

### 5. LegacyAdapter (迁移支持)

```rust
/// 支持从旧 xl.meta 格式迁移到新 KV 格式
pub struct LegacyAdapter {
    modern_engine: Arc<LocalMetadataEngine>,
    legacy_fs: Arc<LocalDisk>,
}

impl LegacyAdapter {
    pub async fn migrate_bucket(&self, bucket: &str) -> Result<MigrationStats> {
        let mut stats = MigrationStats::default();

        // 1. 扫描遗留对象
        let objects = self.legacy_fs.list_bucket(bucket).await?;

        for obj_key in objects {
            // 2. 读取旧元数据
            match self.legacy_fs.read_xl(bucket, &obj_key, false).await {
                Ok(fi) => {
                    // 3. 迁移到新引擎
                    match self.modern_engine.migrate_object(bucket, &obj_key, fi).await {
                        Ok(_) => {
                            stats.migrated += 1;
                        }
                        Err(e) => {
                            tracing::error!("Migration failed for {}/{}: {:?}", bucket, obj_key, e);
                            stats.failed += 1;
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to read legacy metadata for {}/{}: {:?}", bucket, obj_key, e);
                    stats.failed += 1;
                }
            }
        }

        Ok(stats)
    }
}

#[derive(Default, Debug)]
pub struct MigrationStats {
    pub total: u64,
    pub migrated: u64,
    pub failed: u64,
}
```

---

## 平滑过渡策略

### 1. 双轨制方案 (Dual-Track)

#### Phase 1: Shadow Write (并行写入)

```
流程:
  写请求到达
    │
    ├─ 主轨: 写入 xl.meta (保持不变)
    │
    └─ 副轨: 异步写入 SurrealKV + Ferntree (后台)

优势:
  • 零风险: 新路径失败不影响现有业务
  • 可验证: 比对两个元数据源数据一致性
  • 可回退: 任何时刻可停用新引擎

缺点:
  • 双倍写入成本 (应该可接受，因为写是 I/O 密集)
  • 数据不一致窗口 (最终一致性)
```

**实现细节**:

```rust
pub struct DualTrackEngine {
    /// 主轨：传统文件系统
    primary_fs: Arc<LocalDisk>,

    /// 副轨：新 KV 引擎
    secondary_kv: Arc<LocalMetadataEngine>,

    /// 一致性检查线程
    checker: Arc<ConsistencyChecker>,
}

#[async_trait]
impl MetadataEngine for DualTrackEngine {
    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        reader: Box<dyn AsyncRead + Send + Unpin>,
        size: u64,
        opts: ObjectOptions,
    ) -> Result<ObjectInfo> {
        // 分流读取器数据
        let mut buf = Vec::with_capacity(size as usize);
        let mut reader = reader;
        reader.read_to_end(&mut buf).await?;

        let data = Bytes::from(buf);

        // 主轨写入 (FS)
        let info1 = self.primary_fs.put_object(
            bucket,
            key,
            Box::new(Cursor::new(data.clone())),
            size,
            opts.clone(),
        ).await?;

        // 副轨异步写入 (KV)
        let secondary = self.secondary_kv.clone();
        let b = bucket.to_string();
        let k = key.to_string();
        let o = opts.clone();
        let s = size;
        let d = data.clone();

        tokio::spawn(async move {
            if let Err(e) = secondary.put_object(
                &b,
                &k,
                Box::new(Cursor::new(d)),
                s,
                o,
            ).await {
                tracing::warn!("Shadow write to KV failed: {:?}", e);
            }
        });

        Ok(info1)
    }

    async fn get_object_reader(&self, bucket: &str, key: &str, opts: &ObjectOptions) -> Result<GetObjectReader> {
        // 优先读主轨 (FS)
        self.primary_fs.get_object_reader(bucket, key, opts).await
    }
}
```

#### Phase 2: Read Repair (读时修复)

```
流程:
  读请求到达
    │
    ├─ 尝试读 KV
    │   ├─ 找到 → 返回
    │   └─ 未找到
    │       │
    │       └─ 尝试读 FS (xl.meta)
    │           ├─ 找到 → 返回
    │           │   └─ 异步迁移到 KV (Read Repair)
    │           │
    │           └─ 未找到 → 错误

优势:
  • 逐步迁移: 热数据优先迁移，冷数据懒加载
  • 零停机: 不需要全量迁移窗口
  • 减少写入: 不需要 Shadow Write 成本

实现:
```

```rust
#[async_trait]
impl MetadataEngine for LocalMetadataEngine {
    async fn get_object_reader(&self, bucket: &str, key: &str, opts: &ObjectOptions) -> Result<GetObjectReader> {
        let meta_key = format!("buckets/{}/objects/{}/meta", bucket, key);

        // 步骤 1: 尝试从 KV 读取
        let tx = self.kv_store.begin()?;
        if let Some(val) = tx.get(meta_key.as_bytes())? {
            let meta: ObjectMetadata = serde_json::from_slice(&val)?;
            return Ok(self.create_reader(meta).await?);
        }

        // 步骤 2: KV 未命中，尝试从 Legacy FS 读取
        match self.legacy_fs.read_xl(bucket, key, false).await {
            Ok(_) => {
                // 步骤 3: 异步迁移
                let engine = self.clone();
                let b = bucket.to_string();
                let k = key.to_string();
                let fi_clone = self.legacy_fs.read_version(bucket, bucket, key, "", &ReadOptions::default()).await?;

                tokio::spawn(async move {
                    if let Err(e) = engine.migrate_object(&b, &k, fi_clone).await {
                        tracing::error!("Lazy migration failed: {:?}", e);
                    }
                });

                // 返回数据 (从 FS 读取)
                let data = self.legacy_fs.read_all(bucket, key).await?;
                let reader = Box::new(Cursor::new(data));

                Ok(GetObjectReader {
                    stream: reader,
                    object_info: ObjectInfo::default(), // TODO: 填充元数据
                })
            }
            Err(_) => Err(Error::other("Object not found")),
        }
    }

    async fn migrate_object(&self, bucket: &str, key: &str, fi: FileInfo) -> Result<()> {
        // 防止并发迁移 (使用锁)
        let lock_key = format!("{}/{}", bucket, key);
        let _lock = self.repair_locks.entry(lock_key).or_insert(());

        // 读取数据
        let data = self.legacy_fs.read_all(bucket, key).await?;

        // 写入到新引擎
        let mut tx = self.kv_store.begin()?;

        let content_hash = blake3::hash(&data).to_hex().to_string();
        let is_inline = data.len() < 128 * 1024;

        if !is_inline {
            if !self.storage_manager.exists(&content_hash).await {
                self.storage_manager.write_data(&content_hash, data.clone()).await?;
            }
            self.inc_ref(&mut tx, &content_hash).await?;
        }

        let meta = ObjectMetadata {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id: fi.version_id.map(|v| v.to_string()).unwrap_or_default(),
            content_hash,
            size: data.len() as u64,
            is_inline,
            inline_data: if is_inline { Some(data.into()) } else { None },
            ..Default::default()
        };

        let meta_key = format!("buckets/{}/objects/{}/meta", bucket, key);
        let meta_bytes = serde_json::to_vec(&meta)?;

        tx.set(meta_key.as_bytes(), &meta_bytes)?;

        // 更新索引
        let index_key = format!("{}/{}", bucket, key);
        let index_meta = IndexMetadata {
            size: meta.size,
            mod_time: meta.mod_time,
            etag: meta.content_hash.clone(),
        };
        let index_bytes = serde_json::to_vec(&index_meta)?;
        self.index_tree.insert(index_key, Bytes::from(index_bytes));

        tx.commit().await?;
        Ok(())
    }
}
```

#### Phase 3: 切换为 KV 读优先

```
流程:
  当 KV 中的对象数超过阈值 (如 95%) 时：
  
  读请求:
    ├─ 优先读 KV (快速)
    ├─ KV 未命中 → 降级读 FS (兼容)
    └─ 异步清理 FS (后台)
    
  写请求:
    ├─ 若仍有 Shadow Write 配置 → 写 FS + 异步写 KV
    ├─ 否则 → 直接写 KV
```

### 2. 一致性保障机制

```rust
pub struct ConsistencyChecker {
    primary: Arc<dyn MetadataEngine>,
    secondary: Arc<dyn MetadataEngine>,
    check_interval: Duration,
}

impl ConsistencyChecker {
    pub async fn run(&self) {
        let mut interval = tokio::time::interval(self.check_interval);

        loop {
            interval.tick().await;
            if let Err(e) = self.check_and_repair().await {
                tracing::error!("Consistency check failed: {:?}", e);
            }
        }
    }

    async fn check_and_repair(&self) -> Result<()> {
        // 采样检查 1% 的对象
        // 对每个对象，比对 primary 和 secondary 的元数据
        // 若不一致，触发修复 (向主引擎看齐)

        let mut errors = 0;
        let mut checked = 0;

        // TODO: 实现采样逻辑

        if errors > 0 {
            tracing::warn!("Consistency issues found: {} errors out of {} checked", errors, checked);
        }

        Ok(())
    }
}
```

---

## 性能优化

### 1. 内联数据机制 (Inline Data)

**小文件问题分析**:

- 当前：每个小文件产生 2 次 I/O (读 `xl.meta` + 读 `part.1`)
- 新方案：直接内联数据在 KV value 中，1 次 I/O 搞定

**设计**:

```rust
pub struct InlineDataConfig {
    /// 内联阈值 (字节)
    pub threshold: u64,         // 默认 128 KB

    /// 是否压缩内联数据
    pub enable_compression: bool,

    /// 压缩算法 (zstd, lz4)
    pub compression_algo: CompressionAlgo,

    /// 内联数据分开存储的大小限制
    /// 若超过此值，则存储在相邻 KV 键中
    pub separate_threshold: u64, // 默认 512 KB
}

pub enum CompressionAlgo {
    None,
    Zstd,
    Lz4,
}

impl LocalMetadataEngine {
    async fn put_object_inline(
        &self,
        bucket: &str,
        key: &str,
        data: Bytes,
        config: &InlineDataConfig,
    ) -> Result<()> {
        // 1. 检查大小
        if data.len() > config.threshold as usize {
            return Err(Error::other("Data too large for inline"));
        }

        // 2. 可选压缩
        let data_to_store = if config.enable_compression {
            match config.compression_algo {
                CompressionAlgo::Zstd => {
                    let compressed = zstd::encode_all(data.as_ref(), 3)?;
                    Bytes::from(compressed)
                }
                CompressionAlgo::Lz4 => {
                    let compressed = lz4::encode(&data)?;
                    Bytes::from(compressed)
                }
                CompressionAlgo::None => data,
            }
        } else {
            data.clone()
        };

        // 3. 决定存储位置
        let (inline_data, separate_key) = if data_to_store.len() <= config.separate_threshold as usize {
            (Some(data_to_store), None)
        } else {
            (None, Some(format!("buckets/{}/objects/{}/inline", bucket, key)))
        };

        // 4. 更新元数据
        let mut meta = ObjectMetadata {
            bucket: bucket.to_string(),
            key: key.to_string(),
            size: data.len() as u64,
            is_inline: true,
            inline_data,
            ..Default::default()
        };

        // 5. 事务写入
        let mut tx = self.kv_store.begin()?;

        let meta_key = format!("buckets/{}/objects/{}/meta", bucket, key);
        let meta_bytes = serde_json::to_vec(&meta)?;
        tx.set(meta_key.as_bytes(), &meta_bytes)?;

        // 若需要分离存储
        if let Some(separate_key) = separate_key {
            tx.set(separate_key.as_bytes(), data_to_store.as_ref())?;
        }

        tx.commit().await?;
        Ok(())
    }
}
```

**性能指标**:

- **写延迟**: 从 10-50ms (双文件) → 2-5ms (单 KV 写)
- **读延迟**: 从 5-20ms (双文件) → 1-3ms (单 KV 读)
- **小文件 IOPS**: 3-5 倍 提升

### 2. 缓存与预热 (Caching & Warmup)

```rust
pub struct MetadataCache {
    /// LRU 缓存 (内存)
    lru: Arc<RwLock<lru::LruCache<String, Arc<ObjectMetadata>>>>,

    /// 缓存命中统计
    hits: Arc<AtomicU64>,
    misses: Arc<AtomicU64>,
}

impl MetadataCache {
    pub async fn get_or_fetch(
        &self,
        key: &str,
        kv_store: &Arc<KvStore>,
    ) -> Result<Arc<ObjectMetadata>> {
        // 1. 尝试缓存命中
        {
            let mut lru = self.lru.write().await;
            if let Some(val) = lru.get(key) {
                self.hits.fetch_add(1, Ordering::Relaxed);
                return Ok(val.clone());
            }
        }

        // 2. 缓存未命中，从 KV 读取
        self.misses.fetch_add(1, Ordering::Relaxed);
        let tx = kv_store.begin()?;
        let val = tx.get(key.as_bytes())?
            .ok_or_else(|| Error::other("Not found"))?;

        let meta: ObjectMetadata = serde_json::from_slice(&val)?;
        let meta = Arc::new(meta);

        // 3. 更新缓存
        {
            let mut lru = self.lru.write().await;
            lru.put(key.to_string(), meta.clone());
        }

        Ok(meta)
    }

    pub fn hit_rate(&self) -> f32 {
        let hits = self.hits.load(Ordering::Relaxed) as f32;
        let misses = self.misses.load(Ordering::Relaxed) as f32;
        if hits + misses == 0.0 {
            0.0
        } else {
            hits / (hits + misses)
        }
    }
}
```

### 3. 索引优化 (Ferntree B+ 树)

**B+ 树特性**:

- **叶节点**: 存储实际数据 (IndexMetadata)
- **内部节点**: 存储分界键，指向子节点
- **范围扫描**: O(log n) 定位 + O(k) 顺序扫描

**优化策略**:

```rust
impl LocalMetadataEngine {
    /// 批量插入优化 (预排序，减少树重新平衡)
    pub async fn bulk_insert_indices(
        &self,
        bucket: &str,
        objects: Vec<(String, IndexMetadata)>,
    ) -> Result<()> {
        // 1. 按键排序
        let mut sorted = objects;
        sorted.sort_by(|a, b| a.0.cmp(&b.0));

        // 2. 批量插入 (Ferntree 应支持批量操作以优化性能)
        for (key, meta) in sorted {
            let full_key = format!("{}/{}", bucket, key);
            let meta_bytes = serde_json::to_vec(&meta)?;
            self.index_tree.insert(full_key, Bytes::from(meta_bytes));
        }

        Ok(())
    }

    /// 前缀查询优化
    pub fn list_with_cache(
        &self,
        bucket: &str,
        prefix: &str,
        cache: &mut HashMap<String, Vec<ObjectInfo>>,
    ) -> Result<Vec<ObjectInfo>> {
        let cache_key = format!("{}/{}", bucket, prefix);

        // 1. 检查缓存
        if let Some(cached) = cache.get(&cache_key) {
            return Ok(cached.clone());
        }

        // 2. 执行 Ferntree 范围扫描
        let results = self.list_objects_internal(bucket, prefix)?;

        // 3. 缓存结果 (带 TTL)
        cache.insert(cache_key, results.clone());

        Ok(results)
    }
}
```

---

## 全局去重与内容寻址 (Content-Addressable Storage - CAS)

### 1. CAS 设计

**核心思想**: 每个数据块都由其内容的哈希值唯一标识，支持：

- 全局去重：相同内容的多个对象共享一份存储
- 秒传：上传已存在的文件（通过哈希检查）直接返回成功
- 软链接：多个对象指向同一个内容块

```rust
pub struct ContentAddressableStorage {
    /// 内容哈希 → 引用计数
    /// 存储在 KV: refs/{hash} → u64
    kv_store: Arc<KvStore>,

    /// 内容哈希 → 数据存储位置
    /// 可选：在 SurrealMX 中维护
    storage_manager: Arc<dyn StorageManager>,
}

impl ContentAddressableStorage {
    /// 计算内容哈希
    pub fn compute_hash(data: &[u8]) -> String {
        blake3::hash(data).to_hex().to_string()
    }

    /// 检查内容是否已存在 (去重)
    pub async fn exists(&self, content_hash: &str) -> bool {
        self.storage_manager.exists(content_hash).await
    }

    /// 为内容创建新引用
    pub async fn add_reference(
        &self,
        content_hash: &str,
        data: Bytes,
    ) -> Result<()> {
        // 1. 检查内容是否已存在
        if !self.storage_manager.exists(content_hash).await {
            // 2. 不存在，写入新数据
            self.storage_manager.write_data(content_hash, data).await?;
        }

        // 3. 递增引用计数
        let mut tx = self.kv_store.begin()?;
        self.inc_ref_tx(&mut tx, content_hash).await?;
        tx.commit().await?;

        Ok(())
    }

    /// 移除引用 (用于删除操作)
    pub async fn remove_reference(&self, content_hash: &str) -> Result<u64> {
        let mut tx = self.kv_store.begin()?;
        let new_count = self.dec_ref_tx(&mut tx, content_hash).await?;
        tx.commit().await?;

        // 若引用计数降至 0，异步删除数据
        if new_count == 0 {
            let mgr = self.storage_manager.clone();
            let hash = content_hash.to_string();
            tokio::spawn(async move {
                let _ = mgr.delete_data(&hash).await;
            });
        }

        Ok(new_count)
    }

    async fn inc_ref_tx(&self, tx: &mut surrealkv::Transaction, hash: &str) -> Result<()> {
        let key = format!("refs/{}", hash);
        let count = if let Some(val) = tx.get(key.as_bytes())? {
            u64::from_be_bytes(Self::parse_u64(&val)?)
        } else {
            0
        };
        let new_count = count + 1;
        tx.set(key.as_bytes(), &new_count.to_be_bytes())?;
        Ok(())
    }

    async fn dec_ref_tx(&self, tx: &mut surrealkv::Transaction, hash: &str) -> Result<u64> {
        let key = format!("refs/{}", hash);
        let count = if let Some(val) = tx.get(key.as_bytes())? {
            u64::from_be_bytes(Self::parse_u64(&val)?)
        } else {
            0
        };

        if count == 0 {
            return Ok(0);
        }

        let new_count = count - 1;
        if new_count == 0 {
            tx.delete(key.as_bytes())?;
        } else {
            tx.set(key.as_bytes(), &new_count.to_be_bytes())?;
        }
        Ok(new_count)
    }

    fn parse_u64(buf: &[u8]) -> Result<[u8; 8]> {
        let mut arr = [0u8; 8];
        arr.copy_from_slice(&buf[..8]);
        Ok(arr)
    }
}
```

### 2. 秒传实现 (Quick Upload)

```rust
impl LocalMetadataEngine {
    /// 快速上传：若内容已存在，则创建引用而非复制数据
    pub async fn quick_upload(
        &self,
        bucket: &str,
        key: &str,
        content_hash: &str,
        size: u64,
        opts: ObjectOptions,
    ) -> Result<ObjectInfo> {
        // 1. 检查内容是否存在
        if !self.storage_manager.exists(content_hash).await {
            return Err(Error::other("Content not found"));
        }

        // 2. 创建元数据 (指向现有内容)
        let mut tx = self.kv_store.begin()?;

        self.inc_ref(&mut tx, content_hash).await?;

        let meta = ObjectMetadata {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id: opts.version_id.clone().unwrap_or_default(),
            content_hash: content_hash.to_string(),
            size,
            ..Default::default()
        };

        let meta_key = format!("buckets/{}/objects/{}/meta", bucket, key);
        let meta_bytes = serde_json::to_vec(&meta)?;
        tx.set(meta_key.as_bytes(), &meta_bytes)?;

        // 3. 更新索引
        let index_key = format!("{}/{}", bucket, key);
        let index_meta = IndexMetadata {
            size,
            mod_time: meta.mod_time,
            etag: content_hash.to_string(),
        };
        let index_bytes = serde_json::to_vec(&index_meta)?;
        self.index_tree.insert(index_key, Bytes::from(index_bytes));

        tx.commit().await?;

        Ok(ObjectInfo {
            bucket: meta.bucket,
            name: meta.key,
            size: meta.size as i64,
            etag: Some(meta.content_hash),
            ..Default::default()
        })
    }
}
```

### 3. 软链接 (Symlink-like Functionality)

```rust
pub struct SymlinkInfo {
    /// 目标对象的 bucket/key
    pub target_bucket: String,
    pub target_key: String,

    /// 目标对象的内容哈希
    pub target_hash: String,

    /// 创建时间
    pub created_at: i64,

    /// 所有者
    pub owner: String,
}

impl LocalMetadataEngine {
    /// 创建符号链接 (无数据复制)
    pub async fn create_symlink(
        &self,
        bucket: &str,
        key: &str,
        target_bucket: &str,
        target_key: &str,
    ) -> Result<()> {
        // 1. 获取目标对象的元数据
        let target_meta_key = format!("buckets/{}/objects/{}/meta", target_bucket, target_key);
        let tx = self.kv_store.begin()?;
        let target_val = tx.get(target_meta_key.as_bytes())?
            .ok_or_else(|| Error::other("Target object not found"))?;

        let target_meta: ObjectMetadata = serde_json::from_slice(&target_val)?;
        let content_hash = target_meta.content_hash.clone();

        // 2. 递增引用计数
        let mut tx = self.kv_store.begin()?;
        self.inc_ref(&mut tx, &content_hash).await?;

        // 3. 创建符号链接元数据
        let symlink_meta = ObjectMetadata {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id: String::new(),
            content_hash: content_hash.clone(),
            size: target_meta.size,
            // 标记为符号链接
            system_metadata: {
                let mut m = HashMap::new();
                m.insert("x-symlink-target".to_string(), format!("{}/{}", target_bucket, target_key));
                m
            },
            ..Default::default()
        };

        let meta_key = format!("buckets/{}/objects/{}/meta", bucket, key);
        let meta_bytes = serde_json::to_vec(&symlink_meta)?;
        tx.set(meta_key.as_bytes(), &meta_bytes)?;

        tx.commit().await?;
        Ok(())
    }
}
```

---

## 面向未来的架构

### 1. RDMA 网络适配

```rust
/// 预留 RDMA 接口层
pub trait RemoteDmaStorage: Send + Sync {
    /// 获取远程内存区域的 RDMA 地址
    async fn get_rdma_address(&self, content_hash: &str) -> Result<RdmaAddress>;

    /// 远程写操作 (零拷贝)
    async fn rdma_write(
        &self,
        local_addr: RdmaAddress,
        remote_addr: RdmaAddress,
        size: u64,
    ) -> Result<()>;

    /// 远程读操作 (零拷贝)
    async fn rdma_read(
        &self,
        remote_addr: RdmaAddress,
        local_addr: RdmaAddress,
        size: u64,
    ) -> Result<()>;
}

pub struct RdmaAddress {
    pub node_id: u64,
    pub rkey: u32,
    pub addr: u64,
}

/// LocalMetadataEngine 可组合 RDMA 后端
pub struct RdmaEnabledEngine {
    local_engine: Arc<LocalMetadataEngine>,
    rdma_backend: Option<Arc<dyn RemoteDmaStorage>>,
}

#[async_trait]
impl MetadataEngine for RdmaEnabledEngine {
    async fn get_object_reader(&self, bucket: &str, key: &str, opts: &ObjectOptions) -> Result<GetObjectReader> {
        let reader = self.local_engine.get_object_reader(bucket, key, opts).await?;

        // 若启用 RDMA，则将读取操作转为 RDMA 读
        if let Some(rdma) = &self.rdma_backend {
            // TODO: 实现 RDMA 读优化路径
        }

        Ok(reader)
    }
}
```

### 2. NVMe-oF 支持

```rust
pub trait NvmeOfBackend: Send + Sync {
    /// 连接到远程 NVMe 设备
    async fn connect(&self, target_address: &str) -> Result<NvmeNamespace>;

    /// 读取 NVMe 块
    async fn read_blocks(
        &self,
        ns: &NvmeNamespace,
        lba: u64,
        block_count: u32,
    ) -> Result<Bytes>;

    /// 写入 NVMe 块
    async fn write_blocks(
        &self,
        ns: &NvmeNamespace,
        lba: u64,
        data: Bytes,
    ) -> Result<()>;
}

pub struct NvmeOfStorageManager {
    nvme_backend: Arc<dyn NvmeOfBackend>,
    mapping: Arc<RwLock<HashMap<String, (NvmeNamespace, u64)>>>,
}

#[async_trait]
impl StorageManager for NvmeOfStorageManager {
    async fn write_data(&self, content_hash: &str, data: Bytes) -> Result<()> {
        // 分配 LBA 范围
        let lba = self.allocate_lba(data.len()).await?;

        // 通过 NVMe-oF 写入
        let ns = self.get_namespace().await?;
        let block_count = (data.len() + 4095) / 4096; // 假设 4KB 块大小
        self.nvme_backend.write_blocks(&ns, lba, data).await?;

        // 记录映射
        let mut mapping = self.mapping.write().await;
        mapping.insert(content_hash.to_string(), (ns, lba));

        Ok(())
    }

    async fn read_data(&self, content_hash: &str) -> Result<Bytes> {
        let mapping = self.mapping.read().await;
        let (ns, lba) = mapping.get(content_hash)
            .ok_or_else(|| Error::other("Not found"))?;

        let block_count = 1; // 根据实际大小计算
        self.nvme_backend.read_blocks(ns, *lba, block_count).await
    }
}
```

### 3. io_uring 优化

```rust
use io_uring::{opcode, IoUring};

pub struct IoUringStorageManager {
    io_uring: Arc<Mutex<IoUring>>,
    ring_size: usize,
}

impl IoUringStorageManager {
    pub fn new(ring_size: usize) -> Result<Self> {
        let io_uring = IoUring::new(ring_size as u32)?;
        Ok(Self {
            io_uring: Arc::new(Mutex::new(io_uring)),
            ring_size,
        })
    }

    async fn async_write(&self, fd: i32, buf: &[u8], offset: u64) -> Result<()> {
        let mut ring = self.io_uring.lock().await;

        unsafe {
            let write_e = opcode::Write::new(
                opcode::Target::Fd(fd),
                buf.as_ptr(),
                buf.len() as u32,
            ).offset(offset as i64);

            let mut sq = ring.submission();
            sq.push(&write_e)?;
            drop(sq);
        }

        ring.submit_and_wait(1)?;

        // 处理完成队列
        let mut cq = ring.completion();
        for cqe in cq.by_ref().take(1) {
            if cqe.result() < 0 {
                return Err(Error::other("io_uring write failed"));
            }
        }

        Ok(())
    }
}
```

---

## 实现路线图

### Phase 1: 基础建设与双写架构 (v0.1 - v0.3)

#### v0.1: 引擎集成

- [x] 集成 surrealkv, ferntree, surrealmx
- [x] 实现 MetadataEngine trait
- [x] 定义新的元数据序列化格式 (JSON -> Protobuf 可选)
- [ ] 单元测试覆盖基础操作

**交付物**:

- `LocalMetadataEngine` 核心实现
- `ObjectMetadata`, `IndexMetadata` 数据结构
- `StorageManager` 抽象接口

#### v0.2: 异步双写 (Shadow Write)

- [ ] 实现 DualTrackEngine
- [ ] 异步写入副轨 (KV)
- [ ] 后台一致性检查器
- [ ] 验证数据不一致情况处理

**交付物**:

- `DualTrackEngine` 实现
- `ConsistencyChecker` 后台任务
- 单元测试

#### v0.3: 读时修复 (Read Repair & Lazy Load)

- [ ] 实现优先读 KV, 未命中读 FS 的逻辑
- [ ] 异步迁移任务 (Lazy Load)
- [ ] 防止重复迁移的锁机制
- [ ] 集成测试

**交付物**:

- `LegacyAdapter` 迁移工具
- 读时修复完整实现
- E2E 测试

---

### Phase 2: 性能突破与去文件化 (v0.4 - v0.6)

#### v0.4: 内联数据 (Inline Data)

- [ ] 实现 `InlineDataConfig` 配置
- [ ] 小文件内联写入路径
- [ ] 可选压缩支持 (zstd, lz4)
- [ ] 性能对标测试 (IOPS 指标)

**交付物**:

- 内联数据引擎
- 压缩集成
- 性能基准

#### v0.5: 停止生成 xl.meta

- [ ] 切换为 KV 读优先
- [ ] 后台清道夫进程 (扫描并清理已迁移的 xl.meta)
- [ ] Inode 回收监测
- [ ] 灰度发布策略

**交付物**:

- 完整去文件化路径
- 清理工具
- 监测告警

#### v0.6: 高级索引能力

- [ ] Ferntree 多维索引 (Tag, Size, Time)
- [ ] S3 Select 元数据加速
- [ ] 前缀搜索优化
- [ ] 范围查询优化

**交付物**:

- 高级索引 API
- S3 Select 集成

---

### Phase 3: 架构升级与硬件亲和 (v0.7 - v1.0)

#### v0.7: 全局去重 (CAS) 与软链接

- [ ] `ContentAddressableStorage` 实现
- [ ] 秒传功能
- [ ] 符号链接支持
- [ ] 引用计数管理

**交付物**:

- CAS 引擎
- 秒传 API
- 软链接支持

#### v0.8: 大文件聚合 (Block Storage)

- [ ] `BlockManager` 实现
- [ ] 聚合策略 (1GB 块大小)
- [ ] 块的分配和回收
- [ ] 碎片化管理

**交付物**:

- 块存储引擎
- 聚合算法

#### v1.0: RDMA & NVMe-oF 适配

- [ ] RDMA 接口设计
- [ ] NVMe-oF 后端实现
- [ ] io_uring 集成
- [ ] 性能验证

**交付物**:

- RDMA 存储后端
- NVMe-oF 驱动
- 性能基准 (网络优化场景)

---

## 性能指标

### 预期性能改进

| 指标                | 当前 (MinIO-like) | 目标 (RustFS v1.0) | 改进倍数  |
|-------------------|-----------------|------------------|-------|
| 小文件 PUT IOPS      | 1000            | 3000-5000        | 3-5x  |
| 小文件 GET IOPS      | 2000            | 6000-8000        | 3-4x  |
| ListObjects (10K) | 500-1000ms      | 50-100ms         | 5-10x |
| Inode 耗尽          | 受限 FS           | 无限制              | ∞     |
| 秒传延迟              | N/A             | <10ms            | -     |
| 内存缓存命中率           | <10%            | >80%             | 8x+   |
| 数据去重率             | ~5%             | >50%             | 10x+  |

### 基准测试用例

```bash
# 小文件性能
fio --name=random_write \
    --rw=randwrite \
    --bs=4k \
    --size=10GB \
    --numjobs=8 \
    --iodepth=32

# ListObjects 性能
aws s3api list-objects-v2 \
    --bucket test-bucket \
    --max-items 10000 \
    --measure-latency

# 秒传测试
echo "test" | sha256sum  # 计算哈希
# 首次上传
aws s3 cp test.bin s3://test-bucket/
# 再次上传相同内容
time aws s3 cp test.bin s3://test-bucket/test2.bin
```

---

## 总结

本设计文档详细阐述了 RustFS 双元数据中心架构的完整设计，包括：

1. **核心架构**: LocalMetadataEngine + DualMetadataCenter + LegacyAdapter
2. **数据结构**: ObjectMetadata, IndexMetadata, DataLayout 等
3. **存储引擎**: 基于 SurrealKV, Ferntree, SurrealMX 的统一接口
4. **平滑过渡**: 双轨制、Shadow Write、Read Repair、Lazy Migration
5. **性能优化**: 内联数据、缓存、B+ 树索引、全局去重
6. **未来扩展**: RDMA、NVMe-oF、io_uring 适配

关键的创新点：

- **3-5 倍 IOPS 提升**: 通过小文件内联消除双倍 IO
- **毫秒级 ListObjects**: Ferntree B+ 树替代文件系统扫描
- **零拷贝秒传**: Content-Addressable Storage 支持全局去重
- **无限元数据扩展**: 脱离文件系统 Inode 限制

遵循既有约束条件，保证了实现的安全性、可维护性和向下兼容性。


