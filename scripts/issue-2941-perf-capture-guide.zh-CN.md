# Issue 2941 复测与对比指引

本文用于在默认 `musl` 运行形态下，重新验证 `#2941` 是否仍然存在，并为后续是否需要第三阶段代码改造提供统一证据。

## 目标

我们希望把下面三类运行形态放到同一套采样标准下做横向比较：

1. 默认 `musl` 形态
2. `glibc` 形态
3. 当前分支本地二进制

重点观察：

- RustFS 进程总 CPU 是否仍会在空闲窗口持续升到约 `200%`
- `perf` 热点里是否还会出现 `list_path_raw` / scanner / metacache 聚合路径
- `perf` 热点里是否已经不再出现 generic profiling allocator 相关符号
- 在磁盘 I/O 接近空闲时，是否仍然存在高频 wake/sleep / futex 类特征

## 脚本

复测脚本：

- [scripts/run_issue_2941_perf_capture.sh](/Users/zhi/Documents/code/rust/rustfs/rustfs/scripts/run_issue_2941_perf_capture.sh)

该脚本面向“已经运行中的 RustFS 实例”，不会强绑定某一种部署方式。你可以分别对 `musl`、`glibc`、本地二进制运行中的实例各执行一次，最后对比 artifact。

脚本默认采集：

- `uname` / `lscpu` / `free` / `df`
- `/proc/<pid>/status`、`io`、`sched`、`smaps_rollup`
- 线程快照与 `top -H`
- `pidstat`
- 容器 `inspect` / `logs` / `docker stats`（如果传入 `--container`）
- `perf record` + `perf report`（如果环境允许）

## 推荐跑法

### 1. 默认 musl 形态

先确保默认镜像实例已经运行，并拿到容器名，例如 `rustfs`。

```bash
scripts/run_issue_2941_perf_capture.sh \
  --label issue-2941-musl \
  --container rustfs \
  --endpoint http://127.0.0.1:9000 \
  --perf auto \
  --sudo-cmd sudo
```

### 2. glibc 形态

如果使用 `Dockerfile.glibc` 或等价的 `gnu` 产物运行：

```bash
scripts/run_issue_2941_perf_capture.sh \
  --label issue-2941-glibc \
  --container rustfs-glibc \
  --endpoint http://127.0.0.1:9000 \
  --perf auto \
  --sudo-cmd sudo
```

### 3. 当前分支本地二进制

如果是直接运行本地 `rustfs` 进程：

```bash
scripts/run_issue_2941_perf_capture.sh \
  --label issue-2941-local-bin \
  --pid "$(pgrep -n rustfs)" \
  --endpoint http://127.0.0.1:9000 \
  --perf auto \
  --sudo-cmd sudo
```

## 建议对比项

优先对比下面这些 artifact：

- `capture-meta.txt`
- `pidstat.txt`
- `start.top.txt` / `end.top.txt`
- `start.threads.txt` / `end.threads.txt`
- `docker-stats-loop.jsonl`
- `perf-report.txt`

### 预期变化

如果当前分支修复有效，理想上应看到：

1. 默认 `musl` 形态下，不再出现 generic profiling allocator 相关热点。
2. `perf-report.txt` 中 `starshard` 热点占比明显下降，尤其是不再由 allocator 路径触发。
3. 若仍有剩余热点，更可能集中在：
   - `crates/ecstore/src/cache_value/metacache_set.rs`
   - `crates/ecstore/src/store_list_objects.rs`
   - `crates/scanner/src/scanner_folder.rs`

### 需要继续第三阶段改造的信号

如果下面现象仍然成立，则说明还需要继续深入 scanner / merge 主路径：

- CPU 仍长期接近 `200%`
- 物理磁盘 I/O 仍接近空闲
- `perf-report.txt` 中明显出现：
  - `list_path_raw`
  - `peek_with_timeout`
  - callback channel / task wakeup 相关路径
  - futex / wake_up / schedule 类特征

## 当前结论边界

这套脚本的目的不是直接给出根因，而是把“默认 `musl` allocator 干扰已去除后，剩余热点是否仍存在”这件事变成可重复验证的流程。

在这组 artifact 到位前，不建议继续对 scanner / metacache 主路径做更大范围重构。
