# rustfs-object-capacity

`rustfs-object-capacity` 是 RustFS 的对象容量统计核心组件，负责扫描本地数据目录、维护容量缓存、在写入后触发增量刷新，并为上层管理接口提供尽量便宜且可恢复的 used-capacity 结果。

这个 crate 的目标不是做“磁盘总容量”探测，而是回答“RustFS 当前对象数据大约占用了多少字节”，并在精确性、实时性、扫描成本之间做工程化折中。

## 核心职责

- 扫描一个或多个本地数据盘目录，汇总对象数据占用字节数与文件数。
- 在目录规模较大时使用“前缀精确统计 + 尾部采样估算”降低扫描成本。
- 在扫描超时、遍历卡住、部分目录失败时尽量保留可用结果，而不是直接让上层完全失效。
- 维护全局 `HybridCapacityManager` 缓存，支持定时刷新、写触发刷新、前台阻塞刷新和后台异步刷新。
- 记录写入涉及的脏盘范围，在完整磁盘缓存可用时只刷新 dirty subset，而不是每次全量扫描所有盘。
- 输出容量相关 metrics，供运行时观测与基准测试使用。

## 模块划分

- `src/lib.rs`
  对外导出 `scan_used_capacity_disks`、`CapacityDiskRef`、`CapacityScanSummary`。
- `src/types.rs`
  定义扫描输入输出类型，包括 `CapacityDiskRef`、内部 `CapacityScanResult` 和公开 `CapacityScanSummary`。
- `src/scan.rs`
  负责真实目录遍历、采样估算、超时/卡顿检测、多盘并发扫描，以及把扫描结果转换成 `CapacityUpdate`。
- `src/capacity_manager.rs`
  负责缓存、写频率统计、singleflight 刷新协调、后台定时任务、dirty subset 合并和全局单例管理。
- `src/capacity_scope.rs`
  负责“写操作影响了哪些磁盘”的范围传播，包括 token 绑定的局部 scope 和全局 dirty scope 注册表。
- `benches/capacity_scan.rs`
  使用公开扫描 API 做基准，覆盖单盘精确扫描、单盘采样扫描和多盘扫描。

## 数据模型

### `CapacityDiskRef`

```rust
pub struct CapacityDiskRef {
    pub endpoint: String,
    pub drive_path: String,
}
```

它是扫描入口的最小描述单元：

- `endpoint` 用于指标标签和日志区分。
- `drive_path` 是本地磁盘根目录。

### `CapacityScanSummary`

```rust
pub struct CapacityScanSummary {
    pub used_bytes: u64,
    pub file_count: usize,
    pub sampled_count: usize,
    pub is_estimated: bool,
    pub had_partial_errors: bool,
    pub scan_duration: Duration,
}
```

字段语义：

- `used_bytes`：本次扫描或估算得到的容量。
- `file_count`：遍历到的普通文件数量。
- `sampled_count`：超过阈值后被抽样统计的 overflow 文件数。
- `is_estimated`：是否为估算值。
- `had_partial_errors`：遍历中是否出现局部错误但整体仍返回了结果。
- `scan_duration`：扫描耗时。

## 扫描算法

目录扫描实现在 `scan.rs::get_dir_size_async`，核心逻辑如下：

1. 用 `tokio::task::spawn_blocking` 包裹阻塞型目录遍历，避免阻塞 async runtime。
2. 通过 `WalkDir` 遍历目录树，只统计普通文件大小。
3. 当文件数未超过 `DEFAULT_MAX_FILES_THRESHOLD`（默认 `200_000`）时，逐文件精确累加。
4. 超过阈值后：
   - 前 `max_files_threshold` 个文件继续作为精确前缀保留。
   - 之后每隔 `sample_rate` 个文件采样一次，基于 sampled bytes 估算 overflow 部分。
5. 周期性做进度检查：
   - 若总耗时超过 timeout，则尝试退化为采样估算结果。
   - 若在 `stall_timeout` 内没有任何新文件进展，则判定为 stall。
6. 若遍历中部分目录或元数据读取失败：
   - 只要仍有至少一个磁盘成功，就返回部分成功结果。
   - 同时设置 `had_partial_errors = true`。

### 扫描并发

- 多盘扫描使用 `buffer_unordered` 并发执行。
- 当前硬编码最大并发为 `4` 个磁盘。
- 单次磁盘扫描失败不会立即中断其它磁盘。

### 超时与估算退化

crate 不是“超时就直接失败”的设计：

- 如果已经收集到足够的采样数据，超时或 stall 时会返回估算值。
- 如果还没有可用采样，才会真正报错。
- 这保证了大目录、慢盘、暂时抖动场景下，上层依然能拿到近似可用的容量值。

### 符号链接处理

- 默认不跟随符号链接：`RUSTFS_CAPACITY_FOLLOW_SYMLINKS=false`。
- 开启后会做循环引用检测和最大深度限制。
- 默认最大深度是 `3`。

## 容量缓存与刷新策略

`HybridCapacityManager` 是这个 crate 的状态中心。

### 缓存内容

- 最近一次容量值 `total_used`
- 更新时间 `last_update`
- 文件数 `file_count`
- 是否估算值 `is_estimated`
- 数据来源 `DataSource`
- 每盘缓存 `disk_cache`
- dirty disk 集合
- 最近 60 秒写入桶统计

### `DataSource`

- `RealTime`
  首次无缓存时的前台实时刷新。
- `Scheduled`
  定时后台刷新。
- `WriteTriggered`
  写入频率高且缓存偏旧时触发的刷新。
- `Fallback`
  全部扫描失败时，回退到外部传入的磁盘 used capacity。

### 刷新入口

- `refresh_or_join`
  singleflight 前台刷新。若已有刷新进行中，调用方加入等待，不重复扫描。
- `spawn_refresh_if_needed`
  后台异步刷新。若已有刷新在进行，则直接跳过。
- `start_background_task`
  启动两个后台任务：
  - 定时容量刷新任务
  - 定时 runtime summary 日志任务

### singleflight 语义

`refresh_or_join` / `spawn_refresh_if_needed` 通过 `watch` channel 协调刷新：

- 同一时刻只允许一个 leader 真正执行 refresh。
- joiner 在 leader 完成后共享同一份结果。
- refresh panic 会被捕获并转换为错误，避免把调用者一起打崩。

## Dirty Scope 与子集刷新

这个 crate 的一个关键优化是“写后只刷新脏盘”。

### Scope 传播

`capacity_scope.rs` 提供两种脏盘传播方式：

- token scope
  - 调用方先用 `record_capacity_scope(token, scope)` 把一次写操作关联到一组磁盘。
  - 后续 `record_write_operation_with_scope_token(Some(token))` 会取出该 scope，并把磁盘标记为 dirty。
- global dirty scope
  - 通过 `record_global_dirty_scope(scope)` 直接记录全局脏盘。
  - manager 在 `get_dirty_disks()` 时会 drain 这些全局脏盘并合并。

### 何时允许 dirty subset refresh

不是所有时候都能只刷脏盘。前提是：

- `disk_cache_complete == true`
- 也就是系统已经完成过一次“无部分错误”的全盘刷新
- 并且成功拿到了每盘缓存

若当前还没有完整 per-disk cache，或者脏盘集合为空，就会回退到全盘刷新。

### 子集刷新后的合并规则

- 全盘刷新成功时，`per_disk` 会完整替换 `disk_cache`。
- dirty subset 刷新成功时，只更新对应脏盘的缓存条目。
- 总容量会基于更新后的 `disk_cache` 重新求和，而不是盲信子集扫描返回的局部和。
- 若 dirty subset 刷新出现 partial errors，则当前轮次失败，并回退到全盘刷新恢复一致性。

## 与 RustFS 主流程的关系

这个 crate 本身只提供容量能力，真正把它接到 RustFS 主流程的是 `rustfs/src/capacity/service.rs`。

上层使用方式大致如下：

1. 启动时调用 `init_capacity_management_for_local_disks()`。
2. 它收集所有本地盘，调用 `capacity_manager::start_background_task(...)`。
3. 管理接口查询 used capacity 时，优先读 `HybridCapacityManager` 缓存。
4. 缓存足够新时直接返回。
5. 缓存过旧但还可容忍时，先返回 stale cache，再后台刷新。
6. 缓存极旧且写入频率高时，前台阻塞刷新。
7. 若首次实时扫描失败，则退回外部已有的磁盘 used capacity，并写入 `Fallback` 缓存。

`crates/ecstore/src/set_disk.rs` 中则负责在对象写入、heal、data movement 等流程里记录 capacity scope，把“这次写影响了哪些盘”传播给本 crate。

## 公开 API

### 1. 直接扫描

适合 benchmark、运维工具或独立验证路径。

```rust
use rustfs_object_capacity::{CapacityDiskRef, scan_used_capacity_disks};

let disks = vec![
    CapacityDiskRef {
        endpoint: "node-a".to_string(),
        drive_path: "/data/disk1".to_string(),
    },
];

let summary = scan_used_capacity_disks(&disks).await?;
println!(
    "used={} files={} estimated={}",
    summary.used_bytes, summary.file_count, summary.is_estimated
);
# Ok::<(), Box<dyn std::error::Error>>(())
```

### 2. 使用全局 manager

适合服务内缓存与刷新控制。

```rust
use rustfs_object_capacity::capacity_manager::{DataSource, get_capacity_manager};

let manager = get_capacity_manager();

if let Some(cached) = manager.get_capacity().await {
    println!("cached bytes={}", cached.total_used);
}

manager.record_write_operation().await;

let _ = manager
    .refresh_or_join(DataSource::Scheduled, || async {
        rustfs_object_capacity::scan::refresh_capacity_with_scope(
            vec![rustfs_object_capacity::CapacityDiskRef {
                endpoint: "node-a".to_string(),
                drive_path: "/data/disk1".to_string(),
            }],
            false,
        )
        .await
    })
    .await;
```

### 3. 传播 dirty scope

```rust
use rustfs_object_capacity::capacity_scope::{
    CapacityScope, CapacityScopeDisk, record_capacity_scope,
};
use rustfs_object_capacity::capacity_manager::get_capacity_manager;
use uuid::Uuid;

let token = Uuid::new_v4();
record_capacity_scope(
    token,
    CapacityScope {
        disks: vec![CapacityScopeDisk {
            endpoint: "node-a".to_string(),
            drive_path: "/data/disk1".to_string(),
        }],
    },
);

get_capacity_manager()
    .record_write_operation_with_scope_token(Some(token))
    .await;
```

## 环境变量与默认值

配置常量定义在 `crates/config/src/constants/capacity.rs`。

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `RUSTFS_CAPACITY_SCHEDULED_INTERVAL` | `120s` | 定时刷新间隔 |
| `RUSTFS_CAPACITY_WRITE_TRIGGER_DELAY` | `5s` | 写后防抖延迟 |
| `RUSTFS_CAPACITY_WRITE_FREQUENCY_THRESHOLD` | `5` | 最近 60 秒写频率阈值 |
| `RUSTFS_CAPACITY_FAST_UPDATE_THRESHOLD` | `30s` | 缓存超过该年龄后才考虑快速刷新 |
| `RUSTFS_CAPACITY_MAX_FILES_THRESHOLD` | `200000` | 精确统计文件数阈值 |
| `RUSTFS_CAPACITY_STAT_TIMEOUT` | `3s` | 基础扫描超时 |
| `RUSTFS_CAPACITY_SAMPLE_RATE` | `200` | overflow 文件采样间隔 |
| `RUSTFS_CAPACITY_METRICS_INTERVAL` | `600s` | runtime summary 打点间隔 |
| `RUSTFS_CAPACITY_FOLLOW_SYMLINKS` | `false` | 是否跟随符号链接 |
| `RUSTFS_CAPACITY_MAX_SYMLINK_DEPTH` | `3` | 符号链接最大跟随深度 |
| `RUSTFS_CAPACITY_ENABLE_DYNAMIC_TIMEOUT` | `true` | 是否启用动态超时 |
| `RUSTFS_CAPACITY_MIN_TIMEOUT` | `2s` | 动态超时下界 |
| `RUSTFS_CAPACITY_MAX_TIMEOUT` | `15s` | 动态超时上界 |
| `RUSTFS_CAPACITY_STALL_TIMEOUT` | `20s` | 无进展 stall 判定阈值 |

### 配置缓存注意事项

在非测试构建中，配置通过 `OnceLock` 缓存：

- 环境变量只在首次读取时生效。
- 运行中修改 `RUSTFS_CAPACITY_*` 通常不会即时生效。
- 需要重启进程才能稳定应用新配置。

## Metrics

这个 crate 会向 `rustfs-io-metrics::capacity_metrics` 上报多类指标，包括但不限于：

- cache hit / miss / served 状态
- refresh inflight、joiner、success / error
- 当前容量字节数
- 写频率
- dirty disk 数量
- 单盘扫描耗时、采样模式、timeout fallback、stall、symlink 统计

因此它既是容量计算模块，也是容量观测数据的重要生产者。

## 基准测试

运行基准：

```bash
cargo bench -p rustfs-object-capacity --bench capacity_scan
```

当前 bench 场景：

- `capacity_scan_exact`
  单盘 10k 文件精确扫描。
- `capacity_scan_sampled`
  单盘 202,048 文件，触发采样估算。
- `capacity_scan_multi_disk`
  四盘混合规模精确扫描。

## 已知边界与设计取舍

- 它统计的是对象数据目录中文件大小之和，不是文件系统 `du` 的完全等价替代。
- 估算模式优先保证成本可控和结果可用，不保证逐次完全精确。
- dirty subset refresh 只有在完整 per-disk cache 已建立后才安全。
- 部分错误会尽量返回 degraded result，这对可用性更友好，但也意味着调用方需要识别 `had_partial_errors`。
- symlink 默认关闭，是出于安全性和结果确定性考虑。

## 相关源码入口

- [src/lib.rs](./src/lib.rs)
- [src/scan.rs](./src/scan.rs)
- [src/capacity_manager.rs](./src/capacity_manager.rs)
- [src/capacity_scope.rs](./src/capacity_scope.rs)
- [src/types.rs](./src/types.rs)
- [benches/capacity_scan.rs](./benches/capacity_scan.rs)
- [../../rustfs/src/capacity/service.rs](../../rustfs/src/capacity/service.rs)
- [../config/src/constants/capacity.rs](../config/src/constants/capacity.rs)
