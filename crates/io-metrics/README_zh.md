# rustfs-io-metrics

<p align="center">
  <a href="https://github.com/rustfs/rustfs/actions/workflows/ci.yml">
    <img src="https://github.com/rustfs/rustfs/actions/workflows/ci.yml/badge.svg" alt="CI Status" />
  </a>
  <a href="https://crates.io/crates/rustfs-io-metrics">
    <img src="https://img.shields.io/crates/v/rustfs-io-metrics.svg" alt="Crates.io" />
  </a>
</p>

<p align="center">
  · <a href="https://github.com/rustfs/rustfs">🏠 主页</a>
  · <a href="#-文档">📚 文档</a>
  · <a href="https://github.com/rustfs/rustfs/issues">🐛 问题</a>
  · <a href="https://github.com/rustfs/rustfs/discussions">💬 讨论</a>
</p>

---

## 📖 概述

**rustfs-io-metrics** 是 [RustFS](https://rustfs.com) 分布式对象存储系统的指标和配置模块。它提供了：

- **缓存配置**：L1/L2 分层缓存配置管理
- **自适应 TTL**：基于访问频率的动态 TTL 调整
- **指标收集**：统一的指标记录和上报
- **带宽监控**：实时带宽观测和分析
- **性能指标**：I/O 性能指标收集
- **统一配置**：集中式配置管理
- **导出边界**：通过 `metrics` 主动上报，由 `rustfs-obs` 负责 OTEL 导出，不提供 Prometheus HTTP 端点

## ✨ 核心功能

### 缓存配置 (CacheConfig)

分层缓存配置管理：

```rust
use rustfs_io_metrics::{CacheConfig, CacheConfigError};

// 从环境变量加载配置
let config = CacheConfig::from_env();

// 验证配置
if let Err(e) = config.validate() {
    println!("配置无效: {}", e);
}

// 创建自定义配置
let config = CacheConfig {
    max_capacity: 10_000,
    default_ttl_secs: 300,
    max_memory: 100 * 1024 * 1024,  // 100 MB
    ..Default::default()
};
```

### 自适应 TTL (AdaptiveTTL)

基于访问频率动态调整 TTL：

```rust
use rustfs_io_metrics::{AdaptiveTTL, AdaptiveTTLStats};
use std::time::Duration;

let ttl = AdaptiveTTL::new(
    Duration::from_secs(60),    // 最小 TTL: 60 秒
    Duration::from_secs(3600),  // 最大 TTL: 1 小时
    5,                          // 热点阈值: 5 次访问
    2.0,                        // TTL 扩展因子
);

// 冷对象（访问次数少）
let cold_ttl = ttl.calculate(1, Duration::from_secs(60));
println!("冷对象 TTL: {:?}", cold_ttl);

// 热对象（访问次数多）
let hot_ttl = ttl.calculate(100, Duration::from_secs(60));
println!("热对象 TTL: {:?}", hot_ttl);

// 获取统计信息
let stats = ttl.stats();
println!("TTL 调整次数: {}", stats.adjustments);
```

### 访问追踪 (AccessTracker)

追踪缓存项的访问模式：

```rust
use rustfs_io_metrics::{AccessTracker, AccessRecord};
use std::time::Duration;

let mut tracker = AccessTracker::new(1000, Duration::from_secs(300));

// 记录访问
tracker.record_access("object-key-1", 1024);
tracker.record_access("object-key-1", 1024);
tracker.record_access("object-key-2", 2048);

// 获取访问计数
let count = tracker.get_access_count("object-key-1");
println!("访问次数: {}", count);

// 检测热点/冷点
if tracker.is_hot("object-key-1", 1) {
    println!("热点对象");
}

// 获取热门键
let top_keys = tracker.top_keys(10);
for (key, count) in top_keys {
    println!("{}: {} 次访问", key, count);
}
```

### 指标记录

统一的指标记录函数：

```rust
use rustfs_io_metrics::{
    // I/O 调度指标
    record_io_scheduler_decision,
    record_io_strategy_change,
    record_io_load_level,
    
    // 缓存指标
    record_cache_hit,
    record_cache_miss,
    record_cache_eviction,
    
    // 背压指标
    record_backpressure_event,
    record_backpressure_state,
    
    // 超时指标
    record_timeout_event,
    record_operation_duration,
};

// 记录 I/O 调度决策
record_io_scheduler_decision("sequential", "high_priority");

// 记录缓存命中
record_cache_hit("L1");

// 记录背压事件
record_backpressure_event("warning", 0.85);

// 记录操作超时
record_timeout_event("GetObject", Duration::from_secs(30));
```

### 带宽监控 (BandwidthMonitor)

实时带宽观测：

```rust
use rustfs_io_metrics::bandwidth::{BandwidthMonitor, BandwidthSnapshot};

let monitor = BandwidthMonitor::new();

// 记录传输
monitor.record_read(1024 * 1024);  // 1 MB 读取
monitor.record_write(512 * 1024);  // 512 KB 写入

// 获取快照
let snapshot = monitor.snapshot();
println!("读取速率: {} bytes/s", snapshot.read_bytes_per_sec);
println!("写入速率: {} bytes/s", snapshot.write_bytes_per_sec);
```

### 统一配置 (IoConfig)

集中式配置管理：

```rust
use rustfs_io_metrics::{
    IoConfig, CacheSettings, IoSchedulerSettings,
    BackpressureSettings, TimeoutSettings,
};

let config = IoConfig::new()
    .with_cache(CacheSettings::new()
        .with_max_capacity(10_000)
        .with_ttl(std::time::Duration::from_secs(300)))
    .with_scheduler(IoSchedulerSettings::new()
        .with_max_concurrent_reads(64))
    .with_backpressure(BackpressureSettings::new())
    .with_timeout(TimeoutSettings::new());

// 访问配置
println!("缓存容量: {}", config.cache.max_capacity);
println!("最大并发读: {}", config.scheduler.max_concurrent_reads);
```

## 📊 指标类型

### I/O 调度指标

| 指标名 | 描述 | 类型 |
|--------|------|------|
| `io_scheduler_decision_total` | 调度决策次数 | Counter |
| `io_strategy_change_total` | 策略变更次数 | Counter |
| `io_load_level` | 当前负载级别 | Gauge |
| `io_buffer_size_bytes` | 缓冲区大小 | Histogram |

### 缓存指标

| 指标名 | 描述 | 类型 |
|--------|------|------|
| `cache_hit_total` | 缓存命中次数 | Counter |
| `cache_miss_total` | 缓存未命中次数 | Counter |
| `cache_eviction_total` | 缓存驱逐次数 | Counter |
| `cache_size_bytes` | 缓存大小 | Gauge |
| `cache_entries` | 缓存条目数 | Gauge |

### 背压指标

| 指标名 | 描述 | 类型 |
|--------|------|------|
| `backpressure_event_total` | 背压事件次数 | Counter |
| `backpressure_state` | 当前背压状态 | Gauge |
| `backpressure_wait_duration_secs` | 等待时长 | Histogram |

### 超时指标

| 指标名 | 描述 | 类型 |
|--------|------|------|
| `timeout_event_total` | 超时事件次数 | Counter |
| `operation_duration_secs` | 操作时长 | Histogram |
| `operation_progress` | 操作进度 | Gauge |

## 🔧 配置

### 代码配置

```rust
use rustfs_io_metrics::{CacheSettings, IoConfig};

let settings = CacheSettings::new()
    .with_max_capacity(5000)
    .with_ttl(std::time::Duration::from_secs(600))
    .with_max_memory(200 * 1024 * 1024);

let config = IoConfig::new().with_cache(settings);
```

## 📁 模块结构

```
rustfs-io-metrics/
├── src/
│   ├── lib.rs               # 模块入口
│   ├── cache_config.rs      # 缓存配置
│   ├── adaptive_ttl.rs      # 自适应 TTL
│   ├── config.rs            # 统一配置
│   ├── io_metrics.rs        # I/O 指标
│   ├── backpressure_metrics.rs # 背压指标
│   ├── deadlock_metrics.rs  # 死锁指标
│   ├── lock_metrics.rs      # 锁指标
│   ├── timeout_metrics.rs   # 超时指标
│   ├── bandwidth.rs         # 带宽监控
│   ├── global_metrics.rs    # 全局指标
│   └── performance.rs       # 性能指标
└── Cargo.toml
```

## 🧪 测试

```bash
# 运行所有测试
cargo test --package rustfs-io-metrics

# 运行特定测试
cargo test --package rustfs-io-metrics --lib adaptive_ttl

# 运行基准测试
cargo bench --package rustfs-io-metrics --bench metrics_pipeline
```

## 📚 文档

此 crate 通过 Rust `metrics` crate 记录指标，并由 `rustfs-obs` 或应用层可观测性管线负责导出。
它本身不提供 Prometheus 兼容的 HTTP 端点，例如 `/rustfs/v2/metrics/cluster`
或 `/rustfs/v2/metrics/node`。

可以在本地生成 API 文档：

```bash
cargo doc --package rustfs-io-metrics --no-deps --open
```

相关源码入口：

- [Crate API 概览](./src/lib.rs)
- [指标示例](./examples/metrics_example.rs)
- [配置模块](./src/config.rs)
- [自适应 TTL 模块](./src/adaptive_ttl.rs)

## 🔗 相关模块

- **rustfs-io-core**: 核心 I/O 调度
- **rustfs**: 主存储服务

## 📄 许可证

Apache License 2.0
