# RustFS 双元数据中心 - 源码集成与扩展指南

**版本**: 1.0  
**日期**: 2026-02-23  
**面向**: 想要深入理解和扩展双元数据中心的开发者

---

## 目录

1. [与现有代码库的集成点](#与现有代码库的集成点)
2. [object_usecase 集成](#object_usecase-集成)
3. [policy 模块集成](#policy-模块集成)
4. [扩展点与插件化](#扩展点与插件化)
5. [性能优化建议](#性能优化建议)

---

## 与现有代码库的集成点

### 1. 从 object_usecase 到 LocalMetadataEngine

**现状**: `object_usecase.rs` (2702 行) 是 RustFS 的核心业务逻辑层，处理对象的 CRUD 操作。

**集成路径**:

```
object_usecase.rs
    ↓
    ├─ put_object()
    │  └→ LocalMetadataEngine::put_object()
    │
    ├─ get_object()
    │  └→ LocalMetadataEngine::get_object_reader()
    │
    ├─ delete_object()
    │  └→ LocalMetadataEngine::delete_object()
    │
    └─ list_objects()
       └→ LocalMetadataEngine::list_objects()
```

**改造步骤**:

```rust
// rustfs/src/app/object_usecase.rs

// 1. 注入 MetadataEngine
pub struct ObjectUsecase {
    // 现有字段...

    // 新增：元数据引擎 (通过 DI)
    metadata_engine: Arc<dyn MetadataEngine>,
}

impl ObjectUsecase {
    // 2. 修改 put_object
    pub async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        reader: Box<dyn AsyncRead + Send + Unpin>,
        size: u64,
        opts: ObjectOptions,
    ) -> Result<ObjectInfo> {
        // 原有权限检查、验证等逻辑保留
        self.check_permissions(bucket)?;
        self.validate_object_size(size)?;

        // 委托给元数据引擎
        self.metadata_engine
            .put_object(bucket, key, reader, size, opts)
            .await
    }

    // 3. 修改 get_object
    pub async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        opts: ObjectOptions,
    ) -> Result<GetObjectReader> {
        // 权限检查
        self.check_read_permission(bucket, key)?;

        // 委托给元数据引擎
        self.metadata_engine
            .get_object_reader(bucket, key, &opts)
            .await
    }

    // ... 类似修改其他方法
}
```

### 2. 权限检查与 IAM 集成

**关键**: ObjectUsecase 中的权限检查必须在调用 MetadataEngine 之前执行。

```rust
impl ObjectUsecase {
    async fn check_permissions(&self, bucket: &str) -> Result<()> {
        // 现有 IAM 检查逻辑
        // 这层不变，只是提前到 put_object 之前执行

        let user = self.extract_user_from_context()?;
        let action = Action::PutObject; // 来自 crates/policy

        self.iam_engine
            .check_permission(&user, bucket, &action)
            .await?;

        Ok(())
    }

    // 访问日志 (审计)
    async fn log_access(&self, bucket: &str, key: &str, action: &str) -> Result<()> {
        self.audit_logger
            .log(AuditEvent {
                timestamp: chrono::Utc::now(),
                bucket: bucket.to_string(),
                key: key.to_string(),
                action: action.to_string(),
                // ...
            })
            .await?;
        Ok(())
    }
}
```

### 3. 与 ecstore 接口适配

**现状**: RustFS 存储层通过 `ecstore::DiskAPI` 进行 I/O 操作。

**适配策略**: LocalMetadataEngine 应该兼容 DiskAPI 接口，或者提供适配层。

```rust
// rustfs/src/storage/metadata/disk_adapter.rs (新文件)

use rustfs_ecstore::disk::DiskAPI;

/// 适配器：将 LocalMetadataEngine 适配为 DiskAPI
pub struct MetadataEngineDiskAdapter {
    engine: Arc<LocalMetadataEngine>,
}

#[async_trait]
impl DiskAPI for MetadataEngineDiskAdapter {
    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        reader: Box<dyn AsyncRead + Send + Unpin>,
        size: u64,
        opts: ObjectOptions,
    ) -> Result<ObjectInfo> {
        self.engine.put_object(bucket, key, reader, size, opts).await
    }

    async fn get_object_reader(
        &self,
        bucket: &str,
        key: &str,
        opts: &ObjectOptions,
    ) -> Result<GetObjectReader> {
        self.engine.get_object_reader(bucket, key, opts).await
    }

    // ... 其他方法的适配
}
```

---

## object_usecase 集成

### 深入分析现有代码

```rust
// rustfs/src/app/object_usecase.rs 关键节点

pub async fn put_object(&self, req: PutObjectRequest) -> Result<PutObjectResponse> {
    // 1. 验证桶存在
    let bucket_info = self.bucket_manager.get_bucket(&req.bucket).await?;

    // 2. 权限检查 (IAM)
    self.iam_manager
        .check_permission(&req.user, &req.bucket, Action::PutObject)
        .await?;

    // 3. 版本管理检查
    let version_id = if bucket_info.is_versioned {
        Some(uuid::Uuid::new_v4().to_string())
    } else {
        None
    };

    // 4. 加密处理 (KMS)
    let encryption = self.kms_manager
        .prepare_encryption(&req.bucket, &req.key)
        .await?;

    // === 在这里集成 MetadataEngine ===
    // 5. 写入元数据和数据
    let object_info = self.metadata_engine
        .put_object(
            &req.bucket,
            &req.key,
            req.body,
            req.size,
            ObjectOptions {
                version_id: version_id.clone(),
                user_defined: req.metadata.clone(),
                encryption: Some(encryption),
                ..Default::default()
            },
        )
        .await?;

    // 6. 事件通知 (异步)
    self.notification_manager
        .notify_object_created(&req.bucket, &req.key, &object_info)
        .await
        .ok(); // 不阻断主流程

    // 7. 返回响应
    Ok(PutObjectResponse {
        etag: object_info.etag,
        version_id,
    })
}
```

### 集成步骤

```rust
// Step 1: 添加依赖注入
pub struct ObjectUsecase {
    // 现有字段...
    bucket_manager: Arc<dyn BucketManager>,
    iam_manager: Arc<dyn IamManager>,
    kms_manager: Arc<dyn KmsManager>,
    notification_manager: Arc<dyn NotificationManager>,

    // 新增
    metadata_engine: Arc<dyn MetadataEngine>,
}

// Step 2: 在构造函数中初始化
impl ObjectUsecase {
    pub async fn new(
        config: ObjectUsecaseConfig,
        metadata_engine: Arc<dyn MetadataEngine>,
    ) -> Result<Self> {
        Ok(Self {
            bucket_manager: config.bucket_manager,
            iam_manager: config.iam_manager,
            kms_manager: config.kms_manager,
            notification_manager: config.notification_manager,
            metadata_engine,
            // ...
        })
    }
}

// Step 3: 修改 put_object 方法
pub async fn put_object(&self, req: PutObjectRequest) -> Result<PutObjectResponse> {
    // 权限和验证逻辑保持不变
    let bucket_info = self.bucket_manager.get_bucket(&req.bucket).await?;
    self.iam_manager
        .check_permission(&req.user, &req.bucket, Action::PutObject)
        .await?;

    // 版本 ID 生成
    let version_id = if bucket_info.is_versioned {
        Some(uuid::Uuid::new_v4().to_string())
    } else {
        None
    };

    // 加密准备
    let encryption = self.kms_manager
        .prepare_encryption(&req.bucket, &req.key)
        .await?;

    // === 使用新的 MetadataEngine ===
    let object_info = self.metadata_engine
        .put_object(
            &req.bucket,
            &req.key,
            req.body,
            req.size,
            ObjectOptions {
                version_id: version_id.clone(),
                user_defined: req.metadata,
                encryption: Some(encryption),
                mod_time: Some(chrono::Utc::now()),
            },
        )
        .await?;

    // 事件通知
    self.notification_manager
        .notify_object_created(&req.bucket, &req.key, &object_info)
        .await
        .ok();

    Ok(PutObjectResponse {
        etag: object_info.etag,
        version_id,
    })
}

// Step 4: 类似修改 get_object, delete_object, list_objects
pub async fn get_object(&self, req: GetObjectRequest) -> Result<GetObjectResponse> {
    // 权限检查
    self.iam_manager
        .check_permission(&req.user, &req.bucket, Action::GetObject)
        .await?;

    // 使用 MetadataEngine
    let reader = self.metadata_engine
        .get_object_reader(&req.bucket, &req.key, &ObjectOptions::default())
        .await?;

    Ok(GetObjectResponse {
        body: reader.stream,
        metadata: reader.object_info,
    })
}

pub async fn list_objects(&self, req: ListObjectsRequest) -> Result<ListObjectsResponse> {
    // 权限检查
    self.iam_manager
        .check_permission(&req.user, &req.bucket, Action::ListBucket)
        .await?;

    // 使用 MetadataEngine
    let list = self.metadata_engine
        .list_objects(
            &req.bucket,
            &req.prefix,
            req.marker,
            req.delimiter,
            req.max_keys,
        )
        .await?;

    Ok(ListObjectsResponse {
        objects: list.objects,
        prefixes: list.prefixes,
        is_truncated: list.is_truncated,
        next_marker: list.next_continuation_token,
    })
}
```

---

## policy 模块集成

### 权限模型映射

RustFS 的权限系统 (`crates/policy`) 定义了一套 Action 和 Policy。双元数据中心需要在这些权限检查之前执行。

```rust
// rustfs/src/app/object_usecase.rs

use rustfs_policy::action::{Action, ActionContext};

impl ObjectUsecase {
    async fn enforce_policy(
        &self,
        user: &User,
        bucket: &str,
        action: &Action,
    ) -> Result<()> {
        // 1. 获取用户的权限策略
        let policies = self.iam_manager.get_user_policies(user).await?;

        // 2. 评估 Action
        let ctx = ActionContext {
            bucket: bucket.to_string(),
            action: action.clone(),
            user: user.clone(),
            timestamp: chrono::Utc::now(),
        };

        // 3. 检查是否允许
        for policy in policies {
            if policy.allows(&ctx) {
                return Ok(());
            }
        }

        Err(Error::AccessDenied(format!(
            "User {} not allowed to perform {:?} on bucket {}",
            user.id, action, bucket
        )))
    }

    // 在所有 MetadataEngine 操作前调用此方法
    pub async fn put_object(&self, req: PutObjectRequest) -> Result<PutObjectResponse> {
        // 权限检查 (使用 policy)
        self.enforce_policy(
            &req.user,
            &req.bucket,
            &Action::PutObject,
        )
            .await?;

        // 然后调用 MetadataEngine
        // ... (如上所示)
    }
}
```

### 审计日志集成

```rust
use rustfs_audit_logger::AuditEvent;

impl ObjectUsecase {
    async fn log_metadata_operation(
        &self,
        user: &User,
        bucket: &str,
        key: &str,
        action: &str,
        result: &OperationResult,
    ) -> Result<()> {
        let event = AuditEvent {
            timestamp: chrono::Utc::now(),
            user_id: user.id.clone(),
            action: action.to_string(),
            resource: format!("s3://{}/{}", bucket, key),
            result: format!("{:?}", result),
            details: serde_json::json!({
                "bucket": bucket,
                "key": key,
                "metadata_engine": "LocalMetadataEngine",
            }),
        };

        self.audit_logger.log(event).await?;
        Ok(())
    }

    pub async fn put_object(&self, req: PutObjectRequest) -> Result<PutObjectResponse> {
        // ... (前面的逻辑)

        let result = self.metadata_engine
            .put_object(&req.bucket, &req.key, req.body, req.size, opts)
            .await;

        // 记录审计日志
        self.log_metadata_operation(
            &req.user,
            &req.bucket,
            &req.key,
            "PutObject",
            &result,
        )
            .await
            .ok(); // 不阻断

        result.map(|info| PutObjectResponse {
            etag: info.etag,
            version_id: opts.version_id,
        })
    }
}
```

---

## 扩展点与插件化

### 1. 自定义 StorageManager

若要支持特殊的存储后端 (如 S3、HDFS、OSS)，可实现 StorageManager trait：

```rust
pub struct S3StorageManager {
    s3_client: Arc<S3Client>,
    bucket_prefix: String,
}

#[async_trait]
impl StorageManager for S3StorageManager {
    async fn write_data(&self, content_hash: &str, data: Bytes) -> Result<()> {
        let key = format!("{}{}", self.bucket_prefix, content_hash);
        self.s3_client
            .put_object(&key, data)
            .await
            .map_err(|e| Error::other(e.to_string()))?;
        Ok(())
    }

    async fn read_data(&self, content_hash: &str) -> Result<Bytes> {
        let key = format!("{}{}", self.bucket_prefix, content_hash);
        self.s3_client
            .get_object(&key)
            .await
            .map_err(|e| Error::other(e.to_string()))
    }

    // ... 其他方法
}
```

### 2. 自定义 MetadataEngine

用于特殊场景 (如多租户隔离、地域限制等)：

```rust
pub struct MultiTenantMetadataEngine {
    engines: HashMap<String, Arc<LocalMetadataEngine>>,
}

#[async_trait]
impl MetadataEngine for MultiTenantMetadataEngine {
    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        reader: Box<dyn AsyncRead + Send + Unpin>,
        size: u64,
        opts: ObjectOptions,
    ) -> Result<ObjectInfo> {
        // 从 bucket 名称提取租户
        let tenant = Self::extract_tenant(bucket)?;

        // 获取租户的专用引擎
        let engine = self.engines.get(tenant)
            .ok_or_else(|| Error::other("Tenant not found"))?;

        // 委托给租户的引擎
        engine.put_object(bucket, key, reader, size, opts).await
    }

    // ... 其他方法类似
}
```

### 3. 链式调用 (Middleware Pattern)

```rust
pub struct LoggingMetadataEngine {
    inner: Arc<dyn MetadataEngine>,
}

#[async_trait]
impl MetadataEngine for LoggingMetadataEngine {
    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        reader: Box<dyn AsyncRead + Send + Unpin>,
        size: u64,
        opts: ObjectOptions,
    ) -> Result<ObjectInfo> {
        let start = Instant::now();

        let result = self.inner
            .put_object(bucket, key, reader, size, opts)
            .await;

        let elapsed = start.elapsed();
        tracing::info!(
            "put_object({}/{}) took {:.2}ms, success: {}",
            bucket,
            key,
            elapsed.as_secs_f64() * 1000.0,
            result.is_ok()
        );

        result
    }

    // ... 其他方法类似包装
}

// 使用：包装多层
let base = Arc::new(LocalMetadataEngine::new(...));
let with_logging = Arc::new(LoggingMetadataEngine { inner: base });
let with_dual_track = Arc::new(DualTrackEngine::new(fs, with_logging));
```

---

## 性能优化建议

### 1. 批量操作优化

```rust
pub trait MetadataEngine {
    // 新增批量操作接口
    async fn put_objects_batch(
        &self,
        bucket: &str,
        objects: Vec<(String, Bytes, ObjectOptions)>,
    ) -> Result<Vec<ObjectInfo>> {
        // 默认实现：逐个调用 put_object
        // 但具体实现可以优化为：
        // - 合并多个事务
        // - 批量构建索引
        // - 减少 I/O 次数

        let mut results = Vec::new();
        for (key, data, opts) in objects {
            let reader = Box::new(Cursor::new(data));
            let info = self
                .put_object(bucket, &key, reader, data.len() as u64, opts)
                .await?;
            results.push(info);
        }
        Ok(results)
    }
}

impl LocalMetadataEngine {
    async fn put_objects_batch(
        &self,
        bucket: &str,
        objects: Vec<(String, Bytes, ObjectOptions)>,
    ) -> Result<Vec<ObjectInfo>> {
        // 优化实现：单个事务中批量写入
        let mut tx = self.kv_store.begin()?;
        let mut results = Vec::new();

        for (key, data, opts) in objects {
            // ... 处理每个对象，全部在同一事务中
        }

        tx.commit().await?;
        Ok(results)
    }
}
```

### 2. 缓存预热

```rust
impl LocalMetadataEngine {
    pub async fn warmup_cache_for_prefix(
        &self,
        bucket: &str,
        prefix: &str,
        limit: usize,
    ) -> Result<()> {
        // 1. 从索引树扫描前缀
        let keys = self.index_tree
            .range_keys(
                &format!("{}/{}", bucket, prefix),
                limit,
            );

        // 2. 批量加载到缓存
        for key in keys {
            let _ = self.get_object(bucket, &key, ObjectOptions::default()).await;
        }

        Ok(())
    }
}
```

### 3. 异步后台任务优化

```rust
// 使用专用的后台任务队列，而不是 tokio::spawn
pub struct BackgroundTaskQueue {
    sender: mpsc::Sender<BackgroundTask>,
}

pub enum BackgroundTask {
    Migrate { bucket: String, key: String },
    GarbageCollect { content_hash: String },
    Repair { bucket: String, key: String },
}

impl BackgroundTaskQueue {
    pub async fn enqueue(&self, task: BackgroundTask) -> Result<()> {
        self.sender.send(task)
            .await
            .map_err(|e| Error::other(e.to_string()))?;
        Ok(())
    }

    // 启动后台工作线程池
    pub async fn start_workers(&self, num_workers: usize) {
        for _ in 0..num_workers {
            let receiver = self.sender.subscribe();
            tokio::spawn(async move {
                while let Ok(task) = receiver.recv().await {
                    // 处理任务
                }
            });
        }
    }
}
```

---

## 总结

双元数据中心与现有 RustFS 代码库的集成关键点：

1. **object_usecase** 是主要入口，需要注入 MetadataEngine
2. **权限检查** 必须在 MetadataEngine 操作之前执行
3. **审计日志** 需要记录所有元数据操作
4. **通知系统** 应该异步处理，不阻断主流程
5. **扩展点** 包括自定义 StorageManager、MetadataEngine、Middleware

遵循这些集成点，可以平滑地将双元数据中心架构集成到现有 RustFS 代码库中。


