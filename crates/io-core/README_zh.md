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

**rustfs-io-core** 是 [RustFS](https://rustfs.com) 分布式对象存储系统的核心 I/O 调度模块。它提供了：

- **I/O 调度器**：自适应缓冲区大小计算和负载管理
- **优先级队列**：支持饥饿预防的请求优先级调度
- **背压控制**：系统过载保护和优雅降级
- **死锁检测**：基于等待图的死锁检测算法
- **锁优化**：自适应自旋锁优化
- **超时包装器**：动态超时计算和操作进度追踪

## ✨ 核心功能

### I/O 调度器 (IoScheduler)

自适应 I/O 调度，根据文件大小、访问模式和系统负载动态调整缓冲区大小：

```rust
use rustfs_io_core::{IoScheduler, IoSchedulerConfig, IoLoadLevel};
use rustfs_io_core::io_profile::{StorageMedia, AccessPattern};

// 创建调度器
let config = IoSchedulerConfig {
    max_concurrent_reads: 64,
    base_buffer_size: 64 * 1024,  // 64 KB
    max_buffer_size: 1024 * 1024, // 1 MB
    ..Default::default()
};
let scheduler = IoScheduler::new(config);

// 计算最优缓冲区大小
let buffer_size = scheduler.calculate_buffer_size(
    10 * 1024 * 1024,  // 10 MB 文件
    true,              // 顺序访问
    StorageMedia::Ssd,
    IoLoadLevel::Low,
);
println!("缓冲区大小: {} bytes", buffer_size);
```

### 优先级队列 (IoPriorityQueue)

支持饥饿预防的优先级队列：

```rust
use rustfs_io_core::{IoPriorityQueue, IoPriority, IoQueueStatus};

let queue = IoPriorityQueue::<()>::new(100);

// 入队请求
let request_id = queue.enqueue(
    IoPriority::High,
    (),  // 请求数据
    1024, // 请求大小
);

// 出队请求
if let Some((priority, data)) = queue.dequeue() {
    println!("处理优先级 {:?} 的请求", priority);
}

// 检查队列状态
let status = queue.status();
println!("高优先级等待: {}", status.high_priority_waiting);
println!("低优先级等待: {}", status.low_priority_waiting);
```

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

### 超时包装器 (RequestTimeoutWrapper)

动态超时计算：

```rust
use rustfs_io_core::{RequestTimeoutWrapper, TimeoutConfig};
use std::time::Duration;

let config = TimeoutConfig {
    base_timeout: Duration::from_secs(5),
    timeout_per_mb: Duration::from_millis(100),
    max_timeout: Duration::from_secs(300),
    ..Default::default()
};
let wrapper = RequestTimeoutWrapper::new(config);

// 计算操作超时
let timeout = wrapper.calculate_timeout(10 * 1024 * 1024);  // 10 MB
println!("超时时间: {:?}", timeout);

// 执行带超时的操作
let result = wrapper.execute_with_timeout(async {
    // 异步操作
    Ok::<_, std::io::Error>(())
}, timeout).await;
```

## 📊 缓冲区大小计算

模块提供了多种缓冲区大小计算函数：

```rust
use rustfs_io_core::{
    get_concurrency_aware_buffer_size,
    get_advanced_buffer_size,
    get_buffer_size_for_media,
    calculate_optimal_buffer_size,
    KI_B, MI_B,
};
use rustfs_io_core::io_profile::StorageMedia;

// 基础计算
let size1 = get_concurrency_aware_buffer_size(1024 * 1024, 64 * 1024);

// 高级计算（考虑访问模式）
let size2 = get_advanced_buffer_size(10 * 1024 * 1024, 64 * 1024, true);

// 媒体类型优化
let size3 = get_buffer_size_for_media(64 * 1024, StorageMedia::Ssd);

// 综合计算
let size4 = calculate_optimal_buffer_size(
    100 * 1024 * 1024,  // 100 MB 文件
    64 * 1024,          // 基础缓冲区
    true,               // 顺序访问
    4,                  // 并发请求数
    StorageMedia::Nvme,
    IoLoadLevel::Low,
);
```

## 🔧 配置

### 环境变量

| 变量名 | 描述 | 默认值 |
|--------|------|--------|
| `RUSTFS_MAX_CONCURRENT_READS` | 最大并发读数 | 64 |
| `RUSTFS_BASE_BUFFER_SIZE` | 基础缓冲区大小 | 65536 |
| `RUSTFS_MAX_BUFFER_SIZE` | 最大缓冲区大小 | 1048576 |
| `RUSTFS_IO_TIMEOUT_SECS` | I/O 超时秒数 | 30 |

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
│   ├── scheduler.rs        # I/O 调度器
│   ├── io_priority_queue.rs # 优先级队列
│   ├── backpressure.rs     # 背压控制
│   ├── deadlock_detector.rs # 死锁检测
│   ├── lock_optimizer.rs   # 锁优化
│   ├── timeout_wrapper.rs  # 超时包装器
│   └── io_profile.rs       # I/O 配置文件
└── Cargo.toml
```

## 🧪 测试

```bash
# 运行所有测试
cargo test --package rustfs-io-core

# 运行特定测试
cargo test --package rustfs-io-core --lib scheduler

# 运行基准测试
cargo bench --package rustfs-io-core
```

## 📚 文档

- [API 文档](https://docs.rs/rustfs-io-core)
- [I/O 调度器设计](./docs/scheduler-design.md)
- [背压控制原理](./docs/backpressure-design.md)
- [死锁检测算法](./docs/deadlock-detection.md)

## 🔗 相关模块

- **rustfs-io-metrics**: 指标收集和配置管理
- **rustfs**: 主存储服务

## 📄 许可证

Apache License 2.0
