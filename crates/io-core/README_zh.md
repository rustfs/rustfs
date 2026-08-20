# rustfs-io-core

<p align="center">
  <a href="https://github.com/rustfs/rustfs/actions/workflows/ci.yml">
    <img src="https://github.com/rustfs/rustfs/actions/workflows/ci.yml/badge.svg" alt="CI Status" />
  </a>
  <a href="https://docs.rs/rustfs-io-core">
    <img src="https://docs.rs/rustfs-io-core/badge.svg" alt="Documentation" />
  </a>
  <a href="https://crates.io/crates/rustfs-io-core">
    <img src="https://img.shields.io/crates/v/rustfs-io-core.svg" alt="Crates.io" />
  </a>
</p>

<p align="center">
  · <a href="https://github.com/rustfs/rustfs">🏠 主页</a>
  · <a href="https://docs.rs/rustfs-io-core">📚 文档</a>
  · <a href="https://github.com/rustfs/rustfs/issues">🐛 问题</a>
  · <a href="https://github.com/rustfs/rustfs/discussions">💬 讨论</a>
</p>

---

## 📖 概述

**rustfs-io-core** 是 [RustFS](https://rustfs.com) 分布式对象存储系统的共享 I/O 基础组件。它提供了：

- **缓冲池**：分级复用的 `BytesPool`
- **存储画像**：存储介质与访问模式模型（`io_profile`）
- **调度配置**：存储层投影使用的 `IoSchedulerConfig` / `IoPriorityQueueConfig`
- **背压控制**：系统过载保护和优雅降级
- **死锁检测**：基于等待图的死锁检测算法
- **锁优化**：自适应自旋锁优化
- **进度追踪**：长耗时操作的字节进度与停滞判定

调度算法本身位于 `rustfs/src/storage/concurrency/io_schedule.rs`；本 crate 只承载它投影使用的配置形状，不是第二套实现。

## ✨ 核心功能

### 背压控制 (BackpressureMonitor)

系统过载保护：

```rust
use rustfs_io_core::{BackpressureMonitor, BackpressureState, BackpressureConfig};

let config = BackpressureConfig {
    high_watermark: 0.8,  // 80% 触发背压
    low_watermark: 0.5,   // 50% 解除背压
    ..Default::default()
};
let monitor = BackpressureMonitor::new(config);

// 检查状态
match monitor.state() {
    BackpressureState::Normal => println!("系统正常"),
    BackpressureState::Warning => println!("系统警告"),
    BackpressureState::Critical => println!("系统过载"),
}

// 更新负载
monitor.update_load(75, 100);  // 当前 75，最大 100
```

### 死锁检测 (DeadlockDetector)

基于等待图的死锁检测：

```rust
use rustfs_io_core::{DeadlockDetector, LockType};

let detector = DeadlockDetector::with_defaults();

// 注册锁
let lock1 = detector.register_lock(LockType::Mutex);
let lock2 = detector.register_lock(LockType::RwLockWrite);

// 记录锁获取
detector.record_acquire(lock1, 1);  // 线程 1 获取 lock1
detector.record_wait(lock2, 1);     // 线程 1 等待 lock2

// 检测死锁
if let Some(deadlock) = detector.detect_deadlock() {
    println!("检测到死锁: {:?}", deadlock);
}

// 清理
detector.unregister_lock(lock1);
detector.unregister_lock(lock2);
```

### 锁优化 (LockOptimizer)

自适应自旋锁优化：

```rust
use rustfs_io_core::{LockOptimizer, LockOptimizeConfig};

let config = LockOptimizeConfig {
    max_spin_iterations: 1000,
    spin_backoff_factor: 2.0,
    ..Default::default()
};
let optimizer = LockOptimizer::new(config);

// 获取锁守卫
let guard = optimizer.acquire_lock("my_lock");

// 守卫释放时自动记录统计
drop(guard);

// 查看统计
let stats = optimizer.stats();
println!("获取锁次数: {}", stats.locks_acquired.load(std::sync::atomic::Ordering::Relaxed));
```

### 进度追踪 (OperationProgress)

长耗时操作的字节进度与停滞判定：

```rust
use rustfs_io_core::OperationProgress;
use std::time::Duration;

let progress = OperationProgress::new(Some(1000), Duration::from_secs(5));

progress.update(500);
assert_eq!(progress.progress_percent(), Some(50.0));
assert!(!progress.is_stale());
```

## 🔧 配置

### 代码配置

```rust
use rustfs_io_core::IoSchedulerConfig;

let config = IoSchedulerConfig {
    max_concurrent_reads: 128,
    base_buffer_size: 128 * 1024,
    max_buffer_size: 4 * 1024 * 1024,
    high_priority_threshold: 64 * 1024,
    low_priority_threshold: 4 * 1024 * 1024,
    ..Default::default()
};

// 验证配置
if let Err(e) = config.validate() {
    panic!("配置无效: {}", e);
}
```

## 📁 模块结构

```
rustfs-io-core/
├── src/
│   ├── lib.rs              # 模块入口
│   ├── config.rs           # 配置类型
│   ├── pool.rs             # 分级缓冲池
│   ├── backpressure.rs     # 背压控制
│   ├── deadlock_detector.rs # 死锁检测
│   ├── lock_optimizer.rs   # 锁优化
│   ├── progress.rs         # 操作进度追踪
│   └── io_profile.rs       # I/O 配置文件
└── Cargo.toml
```

## 🧪 测试

```bash
# 运行所有测试
cargo nextest run --package rustfs-io-core

# 运行特定测试
cargo nextest run --package rustfs-io-core -E 'test(backpressure)'
```

## 📚 文档

- [API 文档](https://docs.rs/rustfs-io-core)

## 🔗 相关模块

- **rustfs-io-metrics**: 指标收集和配置管理
- **rustfs**: 主存储服务

## 📄 许可证

Apache License 2.0
