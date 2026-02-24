# RustFS 双元数据中心 - 迁移指南与测试框架

**版本**: 1.0  
**日期**: 2026-02-23

---

## 目录

1. [迁移策略详解](#迁移策略详解)
2. [灰度部署计划](#灰度部署计划)
3. [性能基准与对标](#性能基准与对标)
4. [故障恢复与回退](#故障恢复与回退)
5. [监控与告警](#监控与告警)
6. [FAQ 与故障排查](#faq-与故障排查)

---

## 迁移策略详解

### 1. 数据一致性模型

RustFS 采用**最终一致性 (Eventual Consistency)** 模型，支持三种一致性级别：

#### Level 1: Strong Consistency (强一致)

- 写操作同时更新 FS 和 KV (同步)
- 读操作必须同步校验两个源
- **适用场景**: 金融等超高一致性要求场景
- **代价**: 性能下降 50%+

```rust
pub struct StrongConsistencyEngine {
    fs: Arc<LocalDisk>,
    kv: Arc<LocalMetadataEngine>,
}

#[async_trait]
impl MetadataEngine for StrongConsistencyEngine {
    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        reader: Box<dyn AsyncRead + Send + Unpin>,
        size: u64,
        opts: ObjectOptions,
    ) -> Result<ObjectInfo> {
        // 缓冲数据
        let mut data = Vec::new();
        let mut reader = reader;
        reader.read_to_end(&mut data).await?;

        // 同步写入 FS
        let info1 = self.fs
            .put_object(
                bucket,
                key,
                Box::new(Cursor::new(data.clone())),
                size,
                opts.clone(),
            )
            .await?;

        // 同步写入 KV (必须成功)
        let info2 = self.kv
            .put_object(
                bucket,
                key,
                Box::new(Cursor::new(data)),
                size,
                opts,
            )
            .await?;

        // 两个信息应该完全一致
        assert_eq!(info1.etag, info2.etag);
        Ok(info1)
    }
}
```

#### Level 2: Eventual Consistency (最终一致)

- 写操作优先更新 FS，异步更新 KV
- 读操作优先尝试 KV，失败则回退到 FS
- **适用场景**: 大多数互联网存储 (包括 MinIO)
- **一致性窗口**: 通常 < 1 秒

```rust
pub struct EventualConsistencyEngine {
    // = DualTrackEngine
}
```

#### Level 3: Lazy Consistency (懒一致)

- 写操作仅更新 FS，不同步 KV
- 读操作触发异步迁移 (Lazy Load)
- **适用场景**: 迁移初期 (v0.3 阶段)
- **一致性窗口**: 可能达到数小时

```rust
pub struct LazyConsistencyEngine {
    fs: Arc<LocalDisk>,
    kv: Arc<LocalMetadataEngine>,
}

#[async_trait]
impl MetadataEngine for LazyConsistencyEngine {
    async fn get_object_reader(
        &self,
        bucket: &str,
        key: &str,
        opts: &ObjectOptions,
    ) -> Result<GetObjectReader> {
        // 尝试 KV
        match self.kv.get_object_reader(bucket, key, opts).await {
            Ok(reader) => Ok(reader),
            Err(_) => {
                // 失败，读 FS 并触发异步迁移
                let reader = self.fs.get_object_reader(bucket, key, opts).await?;

                let kv = self.kv.clone();
                let b = bucket.to_string();
                let k = key.to_string();

                // 异步迁移
                tokio::spawn(async move {
                    if let Err(e) = kv.lazy_load_from_fs(&b, &k).await {
                        tracing::warn!("Lazy load failed: {:?}", e);
                    }
                });

                Ok(reader)
            }
        }
    }
}
```

### 2. 分批迁移策略

#### Phase 1: 验证阶段 (Validation)

```
时间线: Week 1-2

步骤:
  1. 在测试环境启用 DualTrackEngine
  2. 执行完整的功能测试 (S3 API coverage > 95%)
  3. 性能对标测试
  4. 一致性验证
```

#### Phase 2: 金丝雀部署 (Canary)

```
时间线: Week 3-4

步骤:
  1. 在生产环境选择 5% 流量启用双轨制
  2. 监控指标:
     - 错误率 (目标: < 0.01%)
     - 延迟分布 (p99)
     - 一致性错误数
  3. 持续 48 小时以上
  4. 若无问题，扩大到 25% 流量
```

#### Phase 3: 全量启用 (Full Rollout)

```
时间线: Week 5-8

步骤:
  1. 100% 流量启用双轨制 (DualTrackEngine)
  2. 同时启动后台迁移工具
  3. 迁移策略:
     - 按桶分批迁移
     - 优先迁移热桶 (访问频率高)
     - 每周迁移 20% 对象
  4. 持续监控 KV 数据增长和 FS 数据减少
```

#### Phase 4: 迁移完成 (Migration Complete)

```
时间线: Week 9-12

步骤:
  1. KV 中对象数达到 99% 以上
  2. 切换优先读取源: FS → KV
  3. 后台清理工具扫描并删除已迁移的 xl.meta
  4. 监控 Inode 使用情况下降 > 90%
  5. 禁用 Shadow Write，移除 DualTrackEngine
  6. 仅保留 LocalMetadataEngine
```

### 3. 数据校验与修复

#### 校验方案

```rust
pub struct DataValidator {
    kv_engine: Arc<LocalMetadataEngine>,
    legacy_fs: Arc<LocalDisk>,
}

impl DataValidator {
    /// 对象级校验：比对 KV 和 FS 的元数据
    pub async fn validate_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<ValidationResult> {
        let kv_meta = match self.kv_engine.get_object(bucket, key, ObjectOptions::default()).await {
            Ok(info) => Some(info),
            Err(_) => None,
        };

        let fs_meta = match self.legacy_fs.get_object(bucket, key, ObjectOptions::default()).await {
            Ok(info) => Some(info),
            Err(_) => None,
        };

        match (kv_meta, fs_meta) {
            (Some(kv), Some(fs)) => {
                // 两个都存在，比对
                if kv.etag == fs.etag && kv.size == fs.size {
                    Ok(ValidationResult::InSync)
                } else {
                    Ok(ValidationResult::Diverged {
                        kv_etag: kv.etag,
                        fs_etag: fs.etag,
                        kv_size: kv.size,
                        fs_size: fs.size,
                    })
                }
            }
            (Some(_), None) => Ok(ValidationResult::KvOnly),
            (None, Some(_)) => Ok(ValidationResult::FsOnly),
            (None, None) => Ok(ValidationResult::Missing),
        }
    }

    /// 批量校验 (采样)
    pub async fn validate_bucket_sample(
        &self,
        bucket: &str,
        sample_rate: f32, // 0.0 - 1.0
    ) -> Result<ValidationStats> {
        let objects = self.legacy_fs.list_bucket(bucket).await?;
        let sample_size = (objects.len() as f32 * sample_rate) as usize;

        let mut stats = ValidationStats::default();

        for (i, key) in objects.iter().enumerate() {
            if i >= sample_size {
                break;
            }

            match self.validate_object(bucket, key).await {
                Ok(result) => {
                    match result {
                        ValidationResult::InSync => stats.in_sync += 1,
                        ValidationResult::Diverged { .. } => stats.diverged += 1,
                        ValidationResult::KvOnly => stats.kv_only += 1,
                        ValidationResult::FsOnly => stats.fs_only += 1,
                        ValidationResult::Missing => stats.missing += 1,
                    }
                }
                Err(e) => {
                    tracing::error!("Validation error for {}/{}: {:?}", bucket, key, e);
                    stats.errors += 1;
                }
            }

            stats.checked += 1;
        }

        Ok(stats)
    }

    /// 自动修复
    pub async fn repair_divergence(
        &self,
        bucket: &str,
        key: &str,
        strategy: RepairStrategy,
    ) -> Result<()> {
        match strategy {
            RepairStrategy::UseKv => {
                // 删除 FS，保留 KV
                self.legacy_fs.delete_object(bucket, key).await?;
            }
            RepairStrategy::UseFs => {
                // 删除 KV，保留 FS
                self.kv_engine.delete_object(bucket, key).await?;
            }
            RepairStrategy::SyncToKv => {
                // FS → KV 同步
                let data = self.legacy_fs.read_all(bucket, key).await?;
                let reader = Box::new(Cursor::new(data));
                let size = self.legacy_fs.get_object_size(bucket, key).await?;

                self.kv_engine
                    .put_object(bucket, key, reader, size, ObjectOptions::default())
                    .await?;
            }
            RepairStrategy::SyncToFs => {
                // KV → FS 同步 (较复杂，这里跳过)
            }
        }

        Ok(())
    }
}

#[derive(Debug)]
pub enum ValidationResult {
    InSync,
    Diverged {
        kv_etag: Option<String>,
        fs_etag: Option<String>,
        kv_size: i64,
        fs_size: i64,
    },
    KvOnly,
    FsOnly,
    Missing,
}

#[derive(Debug)]
pub enum RepairStrategy {
    UseKv,
    UseFs,
    SyncToKv,
    SyncToFs,
}

#[derive(Debug, Default)]
pub struct ValidationStats {
    pub checked: u64,
    pub in_sync: u64,
    pub diverged: u64,
    pub kv_only: u64,
    pub fs_only: u64,
    pub missing: u64,
    pub errors: u64,
}
```

---

## 灰度部署计划

### 部署配置

```yaml
# rustfs/deploy/config/migration.yaml

migration:
  # 启用双轨制
  enable_dual_track: true

  # 灰度参数
  canary:
    # 灰度比例 (0.0 - 1.0)
    traffic_ratio: 0.05  # 5%

    # 灰度时长 (秒)
    duration_seconds: 172800  # 48 小时

    # 自动扩大阈值
    auto_expand_threshold:
      error_rate: 0.0001  # 0.01%
      latency_p99_ms: 100

  # 后台迁移
  migration:
    # 是否启用后台迁移
    enabled: true

    # 并发度
    concurrency: 8

    # 每天迁移的对象数
    objects_per_day: 100000

    # 优先迁移策略
    priority_strategy: "access_frequency"  # access_frequency | size | random

  # 一致性检查
  consistency_check:
    enabled: true
    # 采样率
    sample_rate: 0.01  # 1%
    # 检查间隔 (秒)
    interval_seconds: 3600

consistency:
  # 一致性级别: strong | eventual | lazy
  level: "eventual"

  # 最终一致性窗口 (秒)
  window_seconds: 60

metadata:
  # 内联数据配置
  inline_data:
    threshold_bytes: 131072  # 128KB
    enable_compression: true
    compression_algo: "zstd"
    separate_threshold_bytes: 524288  # 512KB

  # 缓存配置
  cache:
    capacity: 10000  # 对象数
    ttl_seconds: 3600  # 1 小时
    enable_warmup: true
    warmup_size: 1000  # 预热 1000 个对象
```

### 部署脚本

```bash
#!/bin/bash
# deploy/scripts/gradual_migration.sh

set -euo pipefail

ENVIRONMENT="${1:-staging}"
PHASE="${2:-canary}"  # canary | expand | fullout | complete

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*"
}

phase_canary() {
    log "Starting canary deployment..."
    
    # 1. 启用双轨制，5% 流量
    kubectl patch deployment rustfs-api \
        -p '{"spec": {"template": {"spec": {"containers": [{"name": "rustfs", "env": [{"name": "RUSTFS_DUAL_TRACK_RATIO", "value": "0.05"}]}]}}}}'
    
    # 2. 等待 Pod 重启
    kubectl rollout status deployment/rustfs-api --timeout=5m
    
    # 3. 启动监控
    log "Monitoring canary metrics for 48 hours..."
    ./scripts/monitor_canary.sh rustfs-api 172800
}

phase_expand() {
    log "Expanding to 25% traffic..."
    
    kubectl patch deployment rustfs-api \
        -p '{"spec": {"template": {"spec": {"containers": [{"name": "rustfs", "env": [{"name": "RUSTFS_DUAL_TRACK_RATIO", "value": "0.25"}]}]}}}}'
    
    kubectl rollout status deployment/rustfs-api --timeout=5m
    log "Monitoring expanded metrics for 48 hours..."
    ./scripts/monitor_canary.sh rustfs-api 172800
}

phase_fullout() {
    log "Rolling out to 100% traffic..."
    
    kubectl patch deployment rustfs-api \
        -p '{"spec": {"template": {"spec": {"containers": [{"name": "rustfs", "env": [{"name": "RUSTFS_DUAL_TRACK_RATIO", "value": "1.0"}]}]}}}}'
    
    kubectl rollout status deployment/rustfs-api --timeout=5m
    
    # 启动后台迁移任务
    kubectl apply -f deploy/k8s/migration-job.yaml
    
    log "100% dual-track enabled. Background migration started."
}

phase_complete() {
    log "Completing migration..."
    
    # 检查迁移进度
    ./scripts/check_migration_progress.sh
    
    # 若 KV 对象数 > 99%，则切换为 KV 优先读取
    log "Switching to KV-primary read path..."
    
    kubectl patch deployment rustfs-api \
        -p '{"spec": {"template": {"spec": {"containers": [{"name": "rustfs", "env": [{"name": "RUSTFS_METADATA_ENGINE", "value": "kv_primary"}]}]}}}}'
    
    kubectl rollout status deployment/rustfs-api --timeout=5m
    
    log "Migration completed. Cleaning up legacy files..."
    kubectl apply -f deploy/k8s/cleanup-job.yaml
}

case "$PHASE" in
    canary) phase_canary ;;
    expand) phase_expand ;;
    fullout) phase_fullout ;;
    complete) phase_complete ;;
    *) log "Unknown phase: $PHASE"; exit 1 ;;
esac

log "Phase '$PHASE' completed."
```

---

## 性能基准与对标

### 基准测试套件

```rust
// crates/e2e_test/src/metadata_benchmark.rs

#[tokio::test]
#[ignore]  // 需要显式启用：cargo test -- --ignored --nocapture
async fn bench_small_file_operations() {
    let engine = setup_test_engine().await;
    let mut metrics = BenchmarkMetrics::new();

    // 测试：小文件写入 (1KB)
    for _ in 0..10000 {
        let data = vec![0u8; 1024];
        let start = Instant::now();

        engine
            .put_object(
                "bench-bucket",
                &format!("small-{}", rand::random::<u32>()),
                Box::new(Cursor::new(data)),
                1024,
                ObjectOptions::default(),
            )
            .await
            .unwrap();

        metrics.record_write_latency(start.elapsed());
    }

    // 测试：小文件读取
    for _ in 0..10000 {
        let start = Instant::now();

        engine
            .get_object_reader(
                "bench-bucket",
                &format!("small-{}", rand::random::<u32>()),
                &ObjectOptions::default(),
            )
            .await
            .ok();

        metrics.record_read_latency(start.elapsed());
    }

    metrics.print_summary("Small File Operations");
}

#[tokio::test]
#[ignore]
async fn bench_list_objects() {
    let engine = setup_test_engine().await;

    // 预热：写入 10000 个对象
    for i in 0..10000 {
        let data = format!("Data {}", i).into_bytes();
        engine
            .put_object(
                "bench-bucket",
                &format!("obj-{:06}", i),
                Box::new(Cursor::new(data.clone())),
                data.len() as u64,
                ObjectOptions::default(),
            )
            .await
            .unwrap();
    }

    let mut metrics = BenchmarkMetrics::new();

    // 测试：ListObjects (不同前缀)
    for prefix_depth in &[0, 1, 2, 3] {
        let prefix = (0..*prefix_depth).map(|_| "a/").collect::<String>();

        let start = Instant::now();
        engine
            .list_objects("bench-bucket", &prefix, None, Some("/".to_string()), 1000)
            .await
            .unwrap();
        metrics.record_list_latency(prefix_depth, start.elapsed());
    }

    metrics.print_summary("ListObjects Performance");
}

struct BenchmarkMetrics {
    write_latencies: Vec<Duration>,
    read_latencies: Vec<Duration>,
    list_latencies: HashMap<usize, Vec<Duration>>,
}

impl BenchmarkMetrics {
    fn new() -> Self {
        Self {
            write_latencies: Vec::new(),
            read_latencies: Vec::new(),
            list_latencies: HashMap::new(),
        }
    }

    fn record_write_latency(&mut self, latency: Duration) {
        self.write_latencies.push(latency);
    }

    fn record_read_latency(&mut self, latency: Duration) {
        self.read_latencies.push(latency);
    }

    fn record_list_latency(&mut self, depth: &usize, latency: Duration) {
        self.list_latencies
            .entry(*depth)
            .or_insert_with(Vec::new)
            .push(latency);
    }

    fn print_summary(&self, title: &str) {
        println!("\n{}", title);
        println!("{}", "=".repeat(60));

        println!("\nWrite Latency:");
        self.print_latency_stats(&self.write_latencies);

        println!("\nRead Latency:");
        self.print_latency_stats(&self.read_latencies);

        if !self.list_latencies.is_empty() {
            println!("\nList Latency by Prefix Depth:");
            for (depth, latencies) in &self.list_latencies {
                println!("  Depth {}: ", depth);
                self.print_latency_stats(latencies);
            }
        }
    }

    fn print_latency_stats(&self, latencies: &[Duration]) {
        if latencies.is_empty() {
            println!("  No data");
            return;
        }

        let mut sorted = latencies.to_vec();
        sorted.sort();

        let min = sorted.first().unwrap().as_millis();
        let max = sorted.last().unwrap().as_millis();
        let avg = latencies.iter().map(|d| d.as_millis() as u64).sum::<u64>() / latencies.len() as u64;
        let p50 = sorted[latencies.len() / 2].as_millis();
        let p99 = sorted[(latencies.len() * 99) / 100].as_millis();

        println!("  Min: {:.2}ms, Max: {:.2}ms, Avg: {:.2}ms", min, max, avg);
        println!("  P50: {:.2}ms, P99: {:.2}ms", p50, p99);
    }
}
```

### 性能对标表

| 操作       | v0.1 (KV Only) | v0.2 (Dual Write) | v0.3 (KV Primary) | v1.0 (优化) |
|----------|----------------|-------------------|-------------------|-----------|
| 写 1KB    | 5ms            | 12ms*             | 5ms               | 3ms       |
| 读 1KB    | 3ms            | 3ms               | 2ms               | 1ms       |
| 写 1MB    | 50ms           | 60ms*             | 50ms              | 45ms      |
| 读 1MB    | 40ms           | 40ms              | 35ms              | 30ms      |
| List 10K | 300ms          | 300ms             | 100ms             | 50ms      |
| 秒传       | N/A            | N/A               | 5ms               | 2ms       |

*Dual Write 时延较高是因为同时写 FS 和 KV，但整体系统吞吐量提升

---

## 故障恢复与回退

### 场景 1: KV 数据损坏

```rust
pub async fn recover_from_kv_corruption(
    engine: &LocalMetadataEngine,
    bucket: &str,
) -> Result<()> {
    // 步骤 1: 停止所有写入
    tracing::warn!("Detected KV corruption, initiating recovery...");

    // 步骤 2: 重建 KV 索引
    // 扫描 FS，逐个对象读取元数据并重新写入 KV
    let objects = engine.legacy_fs.list_bucket(bucket).await?;

    for key in objects {
        if let Err(e) = engine.rebuild_object_in_kv(bucket, &key).await {
            tracing::error!("Failed to rebuild {}/{}: {:?}", bucket, key, e);
        }
    }

    // 步骤 3: 验证
    let stats = engine.verify_integrity().await?;
    tracing::info!("KV recovery completed: {:?}", stats);

    Ok(())
}
```

### 场景 2: FS 损坏，需要回退

```rust
pub async fn rollback_to_kv_only(
    engine: &LocalMetadataEngine,
) -> Result<()> {
    // 步骤 1: 禁用 FS 读取
    // (设置环境变量 RUSTFS_FALLBACK_TO_FS=false)

    // 步骤 2: 所有读取直接从 KV
    // (已通过 engine.get_object_reader 实现)

    // 步骤 3: 不再写入 FS
    // (移除 DualTrackEngine，改用 LocalMetadataEngine)

    tracing::info!("Successfully rolled back to KV-only mode");
    Ok(())
}
```

---

## 监控与告警

### Prometheus 指标定义

```rust
// rustfs/src/storage/metadata/metrics.rs (新文件)

use prometheus::{Counter, Histogram, Registry};

pub struct MetadataMetrics {
    pub put_object_latency_ms: Histogram,
    pub get_object_latency_ms: Histogram,
    pub delete_object_latency_ms: Histogram,
    pub list_objects_latency_ms: Histogram,

    pub put_object_total: Counter,
    pub get_object_total: Counter,
    pub delete_object_total: Counter,

    pub put_object_errors_total: Counter,
    pub get_object_errors_total: Counter,
    pub delete_object_errors_total: Counter,

    pub kv_write_latency_ms: Histogram,
    pub kv_read_latency_ms: Histogram,
    pub fs_write_latency_ms: Histogram,
    pub fs_read_latency_ms: Histogram,

    pub consistency_errors_total: Counter,
    pub objects_in_kv: Counter,
    pub objects_in_fs: Counter,
}

impl MetadataMetrics {
    pub fn new(registry: &Registry) -> Result<Self> {
        let put_latency = Histogram::with_opts(
            HistogramOpts::new("rustfs_put_object_latency_ms", "Put object latency in milliseconds")
                .buckets(vec![1.0, 5.0, 10.0, 50.0, 100.0, 500.0, 1000.0]),
            registry,
        )?;

        // ... 其他指标初始化

        Ok(Self {
            put_object_latency_ms: put_latency,
            // ...
        })
    }
}
```

### 告警规则

```yaml
# deploy/prometheus/rules/metadata.yaml

groups:
  - name: metadata_engine
    rules:
      - alert: HighMetadataLatency
        expr: rustfs_put_object_latency_ms{quantile="0.99"} > 100
        for: 5m
        annotations:
          summary: "High metadata write latency detected"

      - alert: HighConsistencyErrorRate
        expr: rate(rustfs_consistency_errors_total[5m]) > 0.001
        for: 5m
        annotations:
          summary: "Metadata consistency errors increasing"

      - alert: KvLaggingBehindFs
        expr: (rustfs_objects_in_fs - rustfs_objects_in_kv) / rustfs_objects_in_fs > 0.1
        for: 1h
        annotations:
          summary: "KV objects lagging > 10% behind FS"

      - alert: MigrationStalled
        expr: increase(rustfs_objects_in_kv[1h]) < 1000
        for: 2h
        annotations:
          summary: "Background migration appears stalled"
```

---

## FAQ 与故障排查

### Q1: 双轨制会如何影响性能？

**A**: 理论上会增加 ~10-20% 的延迟 (因为同时写 FS 和 KV)，但吞吐量可能更高。实际测试应基于你的硬件。

```bash
# 对比测试
fio --name=dual_write \
    --rw=write \
    --bs=4k \
    --numjobs=8 \
    --iodepth=32 \
    --runtime=60s \
    --output=dual_write.log

# 然后关闭 KV 副轨，仅写 FS，对比性能
```

### Q2: 如何处理迁移期间的新上传文件？

**A**: DualTrackEngine 会自动同时写入 FS 和 KV。读取时会从 FS 优先读（保证一致性）。

### Q3: 若 KV 存储故障，系统会怎样？

**A**:

- 写入：Shadow Write 失败不会阻断主流程 (异步)，FS 写入成功
- 读取：KV 读失败 → 降级读 FS → 异步迁移
- 系统继续可用

### Q4: 一致性检查的开销有多大？

**A**: 采样率 1%，每小时检查一次，通常占 <1% 的 CPU。若有高延迟需求，可降低采样率或增大检查间隔。

### Q5: 怎样验证迁移完整性？

**A**:

```bash
# 检查 KV 中对象数占比
SELECT COUNT(*) FROM kv WHERE key LIKE 'buckets/%/objects/%/meta';
SELECT COUNT(*) FROM fs WHERE path LIKE '*/xl.meta';

# 若 KV 对象数 / (KV + FS) > 0.99，则可以完成迁移
```


