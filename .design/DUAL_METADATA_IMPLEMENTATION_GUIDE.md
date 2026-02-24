# RustFS 双元数据中心 - 详细实现指南

**版本**: 1.0  
**日期**: 2026-02-23  
**目标**: 逐步实现 Phase 1-3 的各个组件

---

## 目录

1. [v0.1: 引擎集成](#v01-引擎集成)
2. [v0.2: 异步双写](#v02-异步双写)
3. [v0.3: 读时修复](#v03-读时修复)
4. [v0.4: 内联数据](#v04-内联数据)
5. [v0.5: 去文件化](#v05-去文件化)
6. [v0.6: 高级索引](#v06-高级索引)
7. [v0.7: 全局去重](#v07-全局去重)
8. [测试策略](#测试策略)

---

## v0.1: 引擎集成

### 1.1 核心数据结构补充

已在 `types.rs` 中定义的结构体需要补充以下功能：

```rust
// rustfs/src/storage/metadata/types.rs

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectMetadata {
    // ... 现有字段 ...

    /// 内部版本号 (用于乐观并发控制)
    pub version: u32,

    /// 数据布局信息
    #[serde(default)]
    pub data_layout: DataLayout,

    /// 对象状态 (Active, Deleted, Archived)
    #[serde(default)]
    pub state: ObjectState,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub enum DataLayout {
    #[default]
    Inline,
    LocalPath { path: String, offset: u64, length: u64 },
    Chunked { chunks: Vec<ChunkInfo> },
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub enum ObjectState {
    #[default]
    Active,
    Deleted,
    Archived,
}

/// 对象可见性信息 (用于最终一致性检查)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectVisibility {
    pub object_key: String,
    pub visible_in_kv: bool,
    pub visible_in_fs: bool,
    pub last_synced: i64,
    pub sync_status: SyncStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SyncStatus {
    InSync,
    KvLagging,
    FsLagging,
    Diverged,
}
```

### 1.2 元数据引擎初始化完善

```rust
// rustfs/src/storage/metadata/engine.rs

use std::path::Path;
use tracing::{info, error};

pub struct EngineConfig {
    pub kv_path: PathBuf,
    pub mx_path: PathBuf,
    pub index_cache_size: usize,
    pub gc_interval_secs: u64,
    pub enable_compression: bool,
}

impl LocalMetadataEngine {
    /// 完整的初始化流程，包含预热和验证
    pub async fn initialize_with_config(
        config: EngineConfig,
        legacy_disk: Arc<LocalDisk>,
    ) -> Result<Arc<Self>> {
        info!("Initializing LocalMetadataEngine with config: {:?}", config);

        // 1. 初始化 KV 存储
        info!("Initializing KV store at {:?}", config.kv_path);
        let kv_store = Arc::new(
            new_kv_store(&config.kv_path)
                .await
                .context("Failed to initialize KV store")?
        );

        // 2. 初始化索引树
        info!("Initializing index tree");
        let index_tree = Arc::new(
            new_index_tree()
                .await
                .context("Failed to initialize index tree")?
        );

        // 3. 初始化存储管理器
        info!("Initializing storage manager at {:?}", config.mx_path);
        let storage_manager = Arc::new(
            MxStorageManager::new(&config.mx_path, 10 * 1024 * 1024 * 1024) // 10GB cache
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
            metadata_cache: Arc::new(MetadataCache::new(config.index_cache_size)),
        });

        // 5. 启动后台垃圾回收
        {
            let gc = engine.gc.clone();
            let interval = config.gc_interval_secs;
            tokio::spawn(async move {
                gc.start_with_interval(interval).await;
            });
        }

        // 6. 预热缓存
        info!("Warming up metadata cache");
        if let Err(e) = engine.warmup_cache().await {
            error!("Cache warmup failed: {:?}", e);
            // 继续启动，不阻断
        }

        // 7. 验证完整性 (可选，仅在首次启动或低频率)
        info!("Verifying metadata integrity");
        if let Err(e) = engine.verify_integrity().await {
            error!("Integrity verification failed: {:?}", e);
            // 非致命错误，记录日志继续
        }

        info!("LocalMetadataEngine initialized successfully");
        Ok(engine)
    }

    /// 预热缓存：加载热数据
    async fn warmup_cache(&self) -> Result<()> {
        // TODO: 从 KV 加载最近访问的前 1000 个对象
        // 使用 scan + limit 实现
        Ok(())
    }

    /// 验证元数据完整性
    async fn verify_integrity(&self) -> Result<()> {
        // 采样检查：检查 1% 的对象是否数据完整
        // 对每个对象，验证：metadata 存在 + data 完整性 (哈希校验)
        Ok(())
    }
}
```

### 1.3 MetadataCache 实现

```rust
// rustfs/src/storage/metadata/cache.rs (新文件)

use lru::LruCache;
use std::sync::Arc;
use tokio::sync::RwLock;
use std::num::NonZeroUsize;

pub struct MetadataCache {
    cache: Arc<RwLock<LruCache<String, CachedMetadata>>>,
    hits: Arc<AtomicU64>,
    misses: Arc<AtomicU64>,
}

#[derive(Clone)]
struct CachedMetadata {
    metadata: Arc<ObjectMetadata>,
    cached_at: i64,
}

impl MetadataCache {
    pub fn new(capacity: usize) -> Self {
        let cache = LruCache::new(NonZeroUsize::new(capacity).unwrap_or(NonZeroUsize::new(1000).unwrap()));
        Self {
            cache: Arc::new(RwLock::new(cache)),
            hits: Arc::new(AtomicU64::new(0)),
            misses: Arc::new(AtomicU64::new(0)),
        }
    }

    pub async fn get(&self, key: &str) -> Option<Arc<ObjectMetadata>> {
        let mut cache = self.cache.write().await;
        if let Some(cached) = cache.get(key) {
            self.hits.fetch_add(1, Ordering::Relaxed);
            return Some(cached.metadata.clone());
        }
        self.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    pub async fn insert(&self, key: String, metadata: Arc<ObjectMetadata>) {
        let mut cache = self.cache.write().await;
        let now = time::OffsetDateTime::now_utc().unix_timestamp();
        cache.put(key, CachedMetadata {
            metadata,
            cached_at: now,
        });
    }

    pub async fn invalidate(&self, key: &str) {
        let mut cache = self.cache.write().await;
        cache.pop(key);
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

    pub async fn clear(&self) {
        let mut cache = self.cache.write().await;
        cache.clear();
    }
}
```

### 1.4 单元测试

```rust
// rustfs/src/storage/metadata/engine.rs

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    async fn create_test_engine() -> (Arc<LocalMetadataEngine>, TempDir) {
        let kv_dir = TempDir::new().unwrap();
        let mx_dir = TempDir::new().unwrap();

        let config = EngineConfig {
            kv_path: kv_dir.path().to_path_buf(),
            mx_path: mx_dir.path().to_path_buf(),
            index_cache_size: 1000,
            gc_interval_secs: 3600,
            enable_compression: false,
        };

        let legacy_disk = Arc::new(LocalDisk::new(kv_dir.path()).unwrap());
        let engine = LocalMetadataEngine::initialize_with_config(config, legacy_disk)
            .await
            .unwrap();

        (engine, kv_dir)
    }

    #[tokio::test]
    async fn test_put_get_inline_object() {
        let (engine, _dir) = create_test_engine().await;

        let data = b"Hello, World!".to_vec();
        let reader = Box::new(Cursor::new(data.clone()));

        let opts = ObjectOptions {
            version_id: Some("v1".to_string()),
            ..Default::default()
        };

        let info = engine
            .put_object("test-bucket", "test-key", reader, data.len() as u64, opts)
            .await
            .unwrap();

        assert_eq!(info.name, "test-key");
        assert_eq!(info.size, data.len() as i64);

        // 验证可以读回
        let reader = engine
            .get_object_reader("test-bucket", "test-key", &ObjectOptions::default())
            .await
            .unwrap();

        let mut read_data = Vec::new();
        reader.stream.read_to_end(&mut read_data).await.unwrap();
        assert_eq!(read_data, data);
    }

    #[tokio::test]
    async fn test_list_objects() {
        let (engine, _dir) = create_test_engine().await;

        // 写入多个对象
        for i in 0..5 {
            let key = format!("obj-{}", i);
            let data = format!("Data {}", i).into_bytes();
            let reader = Box::new(Cursor::new(data.clone()));

            engine
                .put_object("test-bucket", &key, reader, data.len() as u64, ObjectOptions::default())
                .await
                .unwrap();
        }

        // 列出对象
        let list = engine
            .list_objects("test-bucket", "", None, None, 10)
            .await
            .unwrap();

        assert_eq!(list.objects.len(), 5);
        assert!(!list.is_truncated);
    }

    #[tokio::test]
    async fn test_delete_object() {
        let (engine, _dir) = create_test_engine().await;

        let data = b"To delete".to_vec();
        let reader = Box::new(Cursor::new(data.clone()));

        engine
            .put_object("test-bucket", "to-delete", reader, data.len() as u64, ObjectOptions::default())
            .await
            .unwrap();

        // 验证存在
        let exists = engine.exists("test-bucket", "to-delete").await.unwrap();
        assert!(exists);

        // 删除
        engine.delete_object("test-bucket", "to-delete").await.unwrap();

        // 验证不存在
        let exists = engine.exists("test-bucket", "to-delete").await.unwrap();
        assert!(!exists);
    }
}
```

---

## v0.2: 异步双写

### 2.1 DualTrackEngine 实现

```rust
// rustfs/src/storage/metadata/dual_track.rs (新文件)

use async_trait::async_trait;
use std::io::Cursor;
use std::sync::Arc;
use tracing::{debug, warn};

/// 双轨引擎：同时写入传统 FS 和新 KV
pub struct DualTrackEngine {
    primary_fs: Arc<LocalDisk>,
    secondary_kv: Arc<LocalMetadataEngine>,
    consistency_checker: Arc<ConsistencyChecker>,
}

impl DualTrackEngine {
    pub fn new(
        primary_fs: Arc<LocalDisk>,
        secondary_kv: Arc<LocalMetadataEngine>,
    ) -> Self {
        let consistency_checker = Arc::new(ConsistencyChecker::new(
            Arc::new(primary_fs.clone()) as Arc<dyn MetadataEngine>,
            Arc::new(secondary_kv.clone()) as Arc<dyn MetadataEngine>,
        ));

        Self {
            primary_fs,
            secondary_kv,
            consistency_checker,
        }
    }

    /// 启动后台一致性检查
    pub async fn start_consistency_check(&self) {
        let checker = self.consistency_checker.clone();
        tokio::spawn(async move {
            checker.start().await;
        });
    }
}

#[async_trait]
impl MetadataEngine for DualTrackEngine {
    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        mut reader: Box<dyn AsyncRead + Send + Unpin>,
        size: u64,
        opts: ObjectOptions,
    ) -> Result<ObjectInfo> {
        // 将流数据缓冲到内存
        let mut buffer = Vec::with_capacity(size as usize);
        reader.read_to_end(&mut buffer).await.map_err(|e| Error::other(e.to_string()))?;

        let data = Bytes::from(buffer);

        // 主轨：写入 FS (同步)
        debug!("Writing to primary FS: {}/{}", bucket, key);
        let info = self.primary_fs
            .put_object(
                bucket,
                key,
                Box::new(Cursor::new(data.clone())),
                size,
                opts.clone(),
            )
            .await?;

        // 副轨：异步写入 KV
        {
            let secondary = self.secondary_kv.clone();
            let bucket = bucket.to_string();
            let key = key.to_string();
            let opts = opts.clone();
            let data = data.clone();

            tokio::spawn(async move {
                debug!("Async writing to secondary KV: {}/{}", bucket, key);
                if let Err(e) = secondary
                    .put_object(
                        &bucket,
                        &key,
                        Box::new(Cursor::new(data)),
                        size,
                        opts,
                    )
                    .await
                {
                    warn!("Shadow write failed: {}/{}, error: {:?}", bucket, key, e);
                }
            });
        }

        Ok(info)
    }

    async fn get_object_reader(
        &self,
        bucket: &str,
        key: &str,
        opts: &ObjectOptions,
    ) -> Result<GetObjectReader> {
        // 优先从主轨读取
        debug!("Reading from primary FS: {}/{}", bucket, key);
        self.primary_fs.get_object_reader(bucket, key, opts).await
    }

    async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        opts: ObjectOptions,
    ) -> Result<ObjectInfo> {
        self.primary_fs.get_object(bucket, key, opts).await
    }

    async fn delete_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<()> {
        // 主轨删除
        let res1 = self.primary_fs.delete_object(bucket, key).await;

        // 副轨异步删除
        {
            let secondary = self.secondary_kv.clone();
            let bucket = bucket.to_string();
            let key = key.to_string();

            tokio::spawn(async move {
                if let Err(e) = secondary.delete_object(&bucket, &key).await {
                    warn!("Shadow delete failed: {}/{}, error: {:?}", bucket, key, e);
                }
            });
        }

        res1
    }

    async fn update_metadata(
        &self,
        bucket: &str,
        key: &str,
        info: ObjectInfo,
    ) -> Result<()> {
        // 主轨更新
        let res1 = self.primary_fs.update_metadata(bucket, key, info.clone()).await;

        // 副轨异步更新
        {
            let secondary = self.secondary_kv.clone();
            let bucket = bucket.to_string();
            let key = key.to_string();

            tokio::spawn(async move {
                if let Err(e) = secondary.update_metadata(&bucket, &key, info).await {
                    warn!("Shadow update failed: {}/{}, error: {:?}", bucket, key, e);
                }
            });
        }

        res1
    }

    async fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        marker: Option<String>,
        delimiter: Option<String>,
        max_keys: usize,
    ) -> Result<ListObjectsV2Info> {
        // 从主轨列出
        self.primary_fs
            .list_objects(bucket, prefix, marker, delimiter, max_keys)
            .await
    }
}
```

### 2.2 ConsistencyChecker 实现

```rust
// rustfs/src/storage/metadata/consistency.rs (新文件)

use std::sync::Arc;
use std::time::Duration;
use tokio::time::interval;
use tracing::{info, warn, error};

pub struct ConsistencyChecker {
    primary: Arc<dyn MetadataEngine>,
    secondary: Arc<dyn MetadataEngine>,
}

impl ConsistencyChecker {
    pub fn new(
        primary: Arc<dyn MetadataEngine>,
        secondary: Arc<dyn MetadataEngine>,
    ) -> Self {
        Self { primary, secondary }
    }

    /// 启动一致性检查后台任务
    pub async fn start(&self) {
        let mut check_interval = interval(Duration::from_secs(3600)); // 每小时检查一次

        loop {
            check_interval.tick().await;
            info!("Starting consistency check");

            if let Err(e) = self.run_check().await {
                error!("Consistency check failed: {:?}", e);
            }
        }
    }

    /// 执行一致性检查
    async fn run_check(&self) -> Result<()> {
        // 采样：选取 1% 的对象进行检查
        // 对每个样本，比对两个引擎的元数据

        let mut mismatches = 0;
        let mut checked = 0;

        // TODO: 实现采样扫描逻辑

        if mismatches > 0 {
            warn!("Found {} inconsistencies out of {} checked objects", mismatches, checked);
        } else {
            info!("Consistency check passed for {} objects", checked);
        }

        Ok(())
    }
}
```

### 2.3 v0.2 测试

```rust
#[cfg(test)]
mod dual_track_tests {
    use super::*;

    #[tokio::test]
    async fn test_dual_track_write() {
        // 创建主从引擎
        let primary_fs = Arc::new(LocalDisk::new(tempfile::TempDir::new().unwrap().path()).unwrap());
        let config = EngineConfig {
            // ...
        };
        let secondary_kv = LocalMetadataEngine::initialize_with_config(config, primary_fs.clone())
            .await
            .unwrap();

        let dual_engine = DualTrackEngine::new(primary_fs.clone(), secondary_kv.clone());

        // 写入对象
        let data = b"Test data".to_vec();
        let reader = Box::new(Cursor::new(data.clone()));

        let info = dual_engine
            .put_object("test-bucket", "test-key", reader, data.len() as u64, ObjectOptions::default())
            .await
            .unwrap();

        // 验证在主轨存在
        let info1 = primary_fs.get_object("test-bucket", "test-key", ObjectOptions::default())
            .await
            .unwrap();
        assert_eq!(info1.name, "test-key");

        // 等待异步写入完成
        tokio::time::sleep(Duration::from_secs(1)).await;

        // 验证在副轨也存在
        let info2 = secondary_kv.get_object("test-bucket", "test-key", ObjectOptions::default())
            .await
            .unwrap();
        assert_eq!(info2.name, "test-key");
    }
}
```

---

## v0.3: 读时修复

### 3.1 迁移逻辑完善

```rust
// rustfs/src/storage/metadata/migrator.rs (新文件)

pub struct Migrator {
    kv_engine: Arc<LocalMetadataEngine>,
    legacy_fs: Arc<LocalDisk>,
}

impl Migrator {
    pub async fn migrate_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<MigrationResult> {
        // 加锁防止并发迁移
        let lock_key = format!("{}/{}", bucket, key);
        let _lock = self.kv_engine.repair_locks.entry(lock_key).or_insert(());

        // 从 FS 读取旧元数据和数据
        let fi = self.legacy_fs
            .read_version(bucket, bucket, key, "", &ReadOptions::default())
            .await?;

        let data = self.legacy_fs.read_all(bucket, key).await?;

        // 写入新引擎
        let content_hash = blake3::hash(&data).to_hex().to_string();
        let is_inline = data.len() < 128 * 1024;

        let mut tx = self.kv_engine.kv_store.begin()?;

        // 处理数据存储
        if !is_inline {
            if !self.kv_engine.storage_manager.exists(&content_hash).await {
                self.kv_engine.storage_manager
                    .write_data(&content_hash, data.clone())
                    .await?;
            }
            // inc_ref 会在事务中调用
        }

        // 构建元数据
        let meta = ObjectMetadata {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id: fi.version_id.map(|v| v.to_string()).unwrap_or_default(),
            content_hash: content_hash.clone(),
            size: data.len() as u64,
            created_at: fi.mod_time.map(|t| t.unix_timestamp()).unwrap_or_default(),
            mod_time: fi.mod_time.map(|t| t.unix_timestamp()).unwrap_or_default(),
            user_metadata: fi.metadata.clone().unwrap_or_default(),
            is_inline,
            inline_data: if is_inline { Some(data.into()) } else { None },
            ..Default::default()
        };

        // 提交事务
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
        self.kv_engine.index_tree.insert(index_key, Bytes::from(index_bytes));

        tx.commit().await?;

        Ok(MigrationResult {
            key: key.to_string(),
            size: data.len() as u64,
            status: MigrationStatus::Success,
        })
    }

    /// 批量迁移 (用于离线迁移工具)
    pub async fn migrate_bucket(
        &self,
        bucket: &str,
        concurrency: usize,
    ) -> Result<MigrationStats> {
        let objects = self.legacy_fs.list_bucket(bucket).await?;

        let mut stats = MigrationStats::default();
        let semaphore = Arc::new(Semaphore::new(concurrency));

        let futures = objects.into_iter().map(|key| {
            let migrator = self.clone();
            let semaphore = semaphore.clone();

            async move {
                let _permit = semaphore.acquire().await.unwrap();
                match migrator.migrate_object(bucket, &key).await {
                    Ok(_) => MigrationResult {
                        key,
                        size: 0,
                        status: MigrationStatus::Success,
                    },
                    Err(e) => MigrationResult {
                        key,
                        size: 0,
                        status: MigrationStatus::Failed(e.to_string()),
                    },
                }
            }
        });

        let results = futures::future::join_all(futures).await;

        for result in results {
            stats.total += 1;
            match result.status {
                MigrationStatus::Success => stats.migrated += 1,
                MigrationStatus::Failed(_) => stats.failed += 1,
            }
        }

        Ok(stats)
    }
}

#[derive(Debug, Clone)]
pub struct MigrationResult {
    pub key: String,
    pub size: u64,
    pub status: MigrationStatus,
}

#[derive(Debug, Clone)]
pub enum MigrationStatus {
    Success,
    Failed(String),
}

#[derive(Debug, Clone, Default)]
pub struct MigrationStats {
    pub total: u64,
    pub migrated: u64,
    pub failed: u64,
}
```

### 3.2 LocalMetadataEngine 增强读时修复

```rust
impl LocalMetadataEngine {
    pub async fn get_object_reader_with_repair(
        &self,
        bucket: &str,
        key: &str,
        opts: &ObjectOptions,
    ) -> Result<GetObjectReader> {
        let meta_key = format!("buckets/{}/objects/{}/meta", bucket, key);

        // 步骤 1: 尝试从 KV 读取
        let tx = self.kv_store.begin()?;
        if let Some(val) = tx.get(meta_key.as_bytes())? {
            let meta: ObjectMetadata = serde_json::from_slice(&val)?;
            return Ok(self.create_reader_from_meta(meta).await?);
        }

        // 步骤 2: KV 未命中，尝试从 Legacy FS 读取
        match self.legacy_fs.read_xl(bucket, key, false).await {
            Ok(_) => {
                // 步骤 3: 获取完整 FileInfo
                let fi = self.legacy_fs
                    .read_version(bucket, bucket, key, "", &ReadOptions::default())
                    .await?;

                // 步骤 4: 异步迁移
                let engine = self.clone();
                let bucket_clone = bucket.to_string();
                let key_clone = key.to_string();

                tokio::spawn(async move {
                    let migrator = Migrator {
                        kv_engine: engine,
                        legacy_fs: self.legacy_fs.clone(),
                    };

                    if let Err(e) = migrator.migrate_object(&bucket_clone, &key_clone).await {
                        tracing::error!("Lazy migration failed for {}/{}: {:?}", bucket_clone, key_clone, e);
                    }
                });

                // 步骤 5: 返回数据 (从 FS 读取，同时进行迁移)
                let data = self.legacy_fs.read_all(bucket, key).await?;
                let reader = Box::new(Cursor::new(data));

                Ok(GetObjectReader {
                    stream: reader,
                    object_info: ObjectInfo::from_file_info(&fi, bucket, key, false),
                })
            }
            Err(_) => Err(Error::other("Object not found in KV or FS")),
        }
    }

    async fn create_reader_from_meta(&self, meta: ObjectMetadata) -> Result<GetObjectReader> {
        let reader: Box<dyn AsyncRead + Send + Sync + Unpin> = if meta.is_inline {
            let data = meta.inline_data.unwrap_or_default();
            Box::new(WarpReader::new(Cursor::new(data)))
        } else if let Some(chunks) = meta.chunks {
            Box::new(new_chunked_reader(self.storage_manager.clone(), chunks))
        } else {
            let data = self.storage_manager.read_data(&meta.content_hash).await?;
            Box::new(WarpReader::new(Cursor::new(data)))
        };

        let info = ObjectInfo {
            bucket: meta.bucket,
            name: meta.key,
            size: meta.size as i64,
            etag: Some(meta.content_hash),
            user_defined: meta.user_metadata,
            ..Default::default()
        };

        Ok(GetObjectReader {
            stream: reader,
            object_info: info,
        })
    }
}
```

---

## v0.4: 内联数据

### 4.1 InlineDataConfig 和写入路径

```rust
// rustfs/src/storage/metadata/inline.rs (新文件)

use bytes::Bytes;

#[derive(Debug, Clone)]
pub struct InlineDataConfig {
    pub threshold: u64,
    pub enable_compression: bool,
    pub compression_algo: CompressionAlgo,
    pub separate_threshold: u64,
}

impl Default for InlineDataConfig {
    fn default() -> Self {
        Self {
            threshold: 128 * 1024,        // 128KB
            enable_compression: true,
            compression_algo: CompressionAlgo::Zstd,
            separate_threshold: 512 * 1024, // 512KB
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum CompressionAlgo {
    None,
    Zstd,
    Lz4,
}

impl LocalMetadataEngine {
    pub async fn put_object_with_inline(
        &self,
        bucket: &str,
        key: &str,
        mut reader: Box<dyn AsyncRead + Send + Unpin>,
        size: u64,
        opts: ObjectOptions,
        inline_config: &InlineDataConfig,
    ) -> Result<ObjectInfo> {
        let is_inline = size < inline_config.threshold;

        let mut content_hash = String::new();
        let mut inline_data = None;
        let mut chunks = None;

        // 开启事务
        let mut tx = self.kv_store.begin()?;

        if is_inline {
            // 小文件：内联存储
            let mut data = Vec::with_capacity(size as usize);
            reader.read_to_end(&mut data).await?;
            let data = Bytes::from(data);

            content_hash = blake3::hash(&data).to_hex().to_string();

            // 可选压缩
            let data_to_store = if inline_config.enable_compression {
                self.compress_data(&data, inline_config.compression_algo)?
            } else {
                data.clone()
            };

            // 决定存储位置
            if data_to_store.len() <= inline_config.separate_threshold as usize {
                inline_data = Some(data_to_store);
            } else {
                // 分离存储
                let separate_key = format!("buckets/{}/objects/{}/inline_data", bucket, key);
                tx.set(separate_key.as_bytes(), &data_to_store)?;
            }
        } else {
            // 大文件：分块存储
            let chunk_size = 5 * 1024 * 1024; // 5MB
            let mut writer = ChunkedWriter::new(self.storage_manager.clone(), chunk_size);

            tokio::io::copy(&mut reader, &mut writer).await?;
            writer.shutdown().await?;

            content_hash = writer.content_hash();
            let chunk_infos = writer.chunks();

            // 递增所有块的引用计数
            for chunk in &chunk_infos {
                self.inc_ref(&mut tx, &chunk.hash).await?;
            }

            chunks = Some(chunk_infos);
        }

        // 构建元数据
        let now = time::OffsetDateTime::now_utc().unix_timestamp();
        let meta = ObjectMetadata {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id: opts.version_id.clone().unwrap_or_default(),
            content_hash: content_hash.clone(),
            size,
            created_at: now,
            mod_time: opts.mod_time.map(|t| t.unix_timestamp()).unwrap_or(now),
            user_metadata: opts.user_defined.clone(),
            is_inline,
            inline_data,
            chunks,
            ..Default::default()
        };

        // 提交元数据
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

        Ok(ObjectInfo {
            bucket: meta.bucket,
            name: meta.key,
            size: meta.size as i64,
            etag: Some(meta.content_hash),
            user_defined: meta.user_metadata,
            ..Default::default()
        })
    }

    fn compress_data(&self, data: &[u8], algo: CompressionAlgo) -> Result<Bytes> {
        match algo {
            CompressionAlgo::Zstd => {
                let compressed = zstd::encode_all(data, 3)
                    .map_err(|e| Error::other(e.to_string()))?;
                Ok(Bytes::from(compressed))
            }
            CompressionAlgo::Lz4 => {
                let compressed = lz4::encode(data)
                    .map_err(|e| Error::other(e.to_string()))?;
                Ok(Bytes::from(compressed))
            }
            CompressionAlgo::None => Ok(Bytes::copy_from_slice(data)),
        }
    }

    fn decompress_data(&self, data: &[u8], algo: CompressionAlgo) -> Result<Bytes> {
        match algo {
            CompressionAlgo::Zstd => {
                let decompressed = zstd::decode_all(data)
                    .map_err(|e| Error::other(e.to_string()))?;
                Ok(Bytes::from(decompressed))
            }
            CompressionAlgo::Lz4 => {
                let decompressed = lz4::decode(data)
                    .map_err(|e| Error::other(e.to_string()))?;
                Ok(Bytes::from(decompressed))
            }
            CompressionAlgo::None => Ok(Bytes::copy_from_slice(data)),
        }
    }
}
```

---

## v0.5: 去文件化

（继续下一部分...由于篇幅限制，这里省略详细实现）

---

## v0.6: 高级索引

（继续下一部分...）

---

## v0.7: 全局去重

（继续下一部分...）

---

## 测试策略

### 单元测试

- 每个模块必须包含 >80% 代码覆盖
- 测试 Happy Path、边界情况、错误处理

### 集成测试

- 双轨制一致性测试
- 迁移完整性测试
- 性能基准测试

### E2E 测试

- 完整的生命周期测试 (写 → 读 → 列表 → 删除)
- 并发访问测试
- 故障恢复测试

---

## 构建和验证

```bash
# 运行所有测试
cargo test --workspace --exclude e2e_test

# 格式检查
cargo fmt --all --check

# Lint 检查
cargo clippy --all-targets --all-features -- -D warnings

# 完整预提交检查
make pre-commit
```


