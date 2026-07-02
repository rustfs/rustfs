# RustFS jemalloc 到 mimalloc 全面替换可行性与执行方案

日期：2026-07-02

分支：`houseme/mimalloc-allocator-baseline`

基线提交：`b57820a48`

## 1. 结论

RustFS 全面移除 `jemalloc` 并在 Linux 上统一切换到 `mimalloc` 是可行的，推荐分两阶段执行。

第一阶段是默认推荐路径：RustFS 主进程在所有平台使用 `mimalloc`，保留 Pyroscope CPU profiling，Linux 下基于 `jemalloc` 的 `dump_memory_pprof_now` 明确改为不支持，同时移除 RustFS 对 `tikv-jemallocator`、`tikv-jemalloc-ctl`、`jemalloc_pprof` 的直接依赖。

第二阶段是可选增强路径：等 `pyroscope-rs` 的 mimalloc backend 合并发布，或临时使用受控 fork，再恢复 Pyroscope memory profiling。这个阶段不应和 allocator 替换混在同一个性能归因周期里。

当前不建议在第一阶段强行引入 pyroscope-rs fork。原因是上游 mimalloc backend 仍未合并，且它提供的是 allocation profile，不等价于 jemalloc backend 的 live heap/in-use profile。如果把 allocator 替换和 profiling backend 语义变化同时落地，后续性能与观测差异很难归因。

## 2. 当前代码事实

当前 Linux x86_64 gnu 目标使用 `tikv_jemallocator::Jemalloc` 作为全局 allocator；其他平台使用 `mimalloc::MiMalloc`。

相关入口：

- `rustfs/src/main.rs`：全局 allocator cfg 选择。
- `rustfs/src/allocator_reclaim.rs`：Linux x86_64 使用 `tikv_jemalloc_ctl` 触发 jemalloc epoch/background thread；非 Linux jemalloc 平台使用 `libmimalloc_sys::mi_collect`。
- `rustfs/src/profiling.rs`：Linux x86_64 的 memory dump、周期性 memory dump、jemalloc 状态检查依赖 `jemalloc_pprof::PROF_CTL` 和 `tikv_jemalloc_ctl`。
- `rustfs/src/admin/handlers/profile.rs`：admin memory profile endpoint 在 Linux x86_64 调用 `dump_memory_pprof_now`。
- `crates/obs/src/telemetry/otel.rs`：Pyroscope CPU backend 使用 `pprof_backend`；Pyroscope memory backend 使用 `jemalloc_backend`。
- `crates/obs/src/telemetry/guard.rs`：持有 CPU profiling agent 和 memory profiling agent 的生命周期。
- `rustfs/src/runtime_capabilities.rs`：memory profiling capability 目前声明为 `jemalloc memory profiling target`。
- `Cargo.toml`、`rustfs/Cargo.toml`、`crates/obs/Cargo.toml`：workspace 与 crate 级 jemalloc/pyroscope feature 声明。

## 3. 五个专家 review 结论

### 3.1 Allocator 专家

收益：

- Linux、macOS、其他非 Windows 平台的 allocator 行为更一致，减少平台分支。
- 避免 jemalloc page-size、arena、`MALLOC_CONF`、profiling build feature 的长期维护复杂度。
- `mimalloc` 的 `mi_collect(force)` 可以统一 allocator reclaim service 的实现路径。
- 对 Tokio-heavy、高并发小对象/中对象请求，mimalloc 有机会降低分配锁竞争与 RSS 抖动。

风险：

- RustFS 的大对象 PUT/GET、erasure coding、multipart、scanner/heal 并发路径可能受 allocator 行为影响。
- mimalloc 与 jemalloc 的 retained memory、page purge、thread cache 策略不同，RSS 峰值和 P99 必须实测。
- 当前 Linux 生产路径已有 jemalloc 调优假设，切换后这些参数会失效。

结论：可以替换，但必须以远程 Linux 基线和改造后同机 A/B 为准，不接受无 benchmark 的默认切换。

### 3.2 Profiling 专家

收益：

- 保留 Pyroscope CPU profiling 风险低，因为它基于 `pprof-rs`，不依赖 jemalloc。
- 允许 Linux 下 `dump_memory_pprof_now` 改为不支持，可以切断 RustFS 内部对 `jemalloc_pprof::PROF_CTL.dump_pprof()` 的强耦合。

影响：

- 第一阶段会失去本地 admin memory pprof dump。
- 第一阶段会失去 Pyroscope jemalloc memory profiling。
- 即使后续接入 pyroscope-rs mimalloc backend，也应明确它是 allocation profile，不是 live heap/in-use profile。

结论：第一阶段保留 Pyroscope CPU，禁用 memory profiling；第二阶段单独评估 mimalloc allocation profiling。

### 3.3 性能专家

必须固定的指标：

- warp 吞吐：`median_throughput_bps`。
- warp 请求速率：`median_reqps`。
- warp 平均延迟、P90、P99：`median_latency_ms`、`median_req_p90_ms`、`median_req_p99_ms`。
- 错误率：failed rounds、warp log 中的 error。
- 进程资源：RSS、VmHWM、CPU%、线程数、FD 数。
- cgroup/container memory：`memory.current`、`memory.peak`，如环境支持。
- RustFS metrics：GET/PUT stage、EC in-flight、bytespool、allocator reclaim backend/result。

验收建议：

- 吞吐 median 退化不超过 3%-5%。
- P95/P99 退化不超过 10%。
- failed rounds 必须为 0。
- RSS/VmHWM 不高于 jemalloc 基线 5%，理想情况应下降。
- allocator reclaim metrics 能正确标记 `backend="mimalloc"`。

结论：先跑 jemalloc baseline，再编码，最后同机同参数跑 mimalloc 对比。

### 3.4 构建与发布专家

第一阶段改动范围：

- `rustfs/src/main.rs`：Linux 也使用 `mimalloc::MiMalloc`。
- `rustfs/Cargo.toml`：将 `mimalloc`、`libmimalloc-sys` 依赖范围扩大到 Linux；移除 Linux x86_64 的 jemalloc 依赖。
- workspace `Cargo.toml`：移除 `tikv-jemallocator`、`tikv-jemalloc-ctl`、`jemalloc_pprof`，调整 `pyroscope` feature。
- `crates/obs/Cargo.toml`：`pyroscope` feature 不再拉 `jemalloc_pprof`；保留 `backend-pprof-rs`。
- `rustfs/src/profiling.rs`：删除或 cfg-disable jemalloc memory dump、周期性 dump、状态检查。
- `crates/obs/src/telemetry/otel.rs` 与 `guard.rs`：移除 jemalloc memory profiling agent。
- `runtime_capabilities.rs`、config 常量注释、README/obs 文档：更新行为说明。

结论：这是跨 crate 行为变更，不能只改 allocator 一行。

### 3.5 运维/API 专家

外部可见影响：

- Linux admin memory profile endpoint 从“尝试导出 memory pprof”变成明确 `NOT_IMPLEMENTED`。
- `RUSTFS_PROF_MEM_PERIODIC` 第一阶段应无效或返回清晰跳过日志。
- `MALLOC_CONF` 相关运维调优不再适用。
- `RUSTFS_ALLOCATOR_RECLAIM_FORCE` 在 mimalloc 下可生效，之前 jemalloc 下会被忽略。

需要更新：

- runtime capabilities 的 memory profiling 状态与 reason。
- profile endpoint 错误文案。
- observability README 中 Pyroscope memory profiling 描述。
- PR 描述中必须写清楚 memory pprof dump 的兼容性变化。

结论：这是有意的可见行为变更，不应伪装成无兼容影响。

## 4. 推荐执行阶段

### Phase 0：远程 jemalloc baseline

目标：在改代码前固定 Linux jemalloc 性能指标。

输出目录：

```bash
target/bench/allocator-ab/jemalloc-<YYYYMMDD-HHMMSS>
```

必须保存：

- `run_manifest.env`
- `round_results.csv`
- `median_summary.csv`
- benchmark logs
- RustFS server logs
- host info：`uname -a`、`lscpu`、`free -h`、`rustc --version`、`cargo --version`
- process snapshots：`ps`、`/proc/<pid>/status` 或 container stats

推荐 benchmark 矩阵：

```bash
scripts/run_object_batch_bench_enhanced.sh \
  --tool warp \
  --endpoint http://127.0.0.1:9000 \
  --access-key rustfsadmin \
  --secret-key rustfsadmin \
  --bucket rustfs-allocator-jemalloc \
  --bucket-size-suffix \
  --warp-mode mixed \
  --sizes 1KiB,4KiB,32KiB,1MiB,10MiB \
  --concurrency 128 \
  --duration 60s \
  --rounds 5 \
  --retry-per-round 1 \
  --round-cooldown-secs 30 \
  --out-dir target/bench/allocator-ab/jemalloc-<YYYYMMDD-HHMMSS>/mixed
```

如果时间允许，补充 GET/PUT 拆分：

```bash
scripts/run_object_batch_bench_enhanced.sh ... --warp-mode get ...
scripts/run_object_batch_bench_enhanced.sh ... --warp-mode put ...
```

### Phase 1：第一阶段代码替换

目标：完成 allocator 统一切换，不接入 pyroscope-rs fork。

实现原则：

- 只做 allocator/profiling/reclaim/capability 相关最小改动。
- 不重构 unrelated profiling CPU 逻辑。
- Linux `dump_memory_pprof_now` 返回明确 unsupported。
- `RUSTFS_PROF_MEM_PERIODIC` 在 mimalloc 第一阶段不启动后台 dump。
- 保留 Pyroscope CPU profiling。

### Phase 2：本地验证

必须运行：

```bash
cargo fmt --all
cargo fmt --all --check
cargo check -p rustfs --features pyroscope
cargo test -p rustfs allocator_reclaim
cargo test -p rustfs-obs --features pyroscope
```

如代码变更编译通过，再跑：

```bash
make pre-commit
```

最终 PR 前跑：

```bash
make pre-pr
```

### Phase 3：远程 mimalloc A/B

目标：同一台 Linux 机器、同一 workload、同一参数，对比 jemalloc baseline。

输出目录：

```bash
target/bench/allocator-ab/mimalloc-<YYYYMMDD-HHMMSS>
```

必须传入 baseline：

```bash
--baseline-csv target/bench/allocator-ab/jemalloc-<YYYYMMDD-HHMMSS>/mixed/median_summary.csv
```

生成：

- `baseline_compare.csv`
- mimalloc `run_manifest.env`
- mimalloc `round_results.csv`
- mimalloc `median_summary.csv`
- 资源指标快照

### Phase 4：Pyroscope memory profiling 决策

只有当第一阶段性能结果可接受后，才进入此阶段。

可选路径：

1. 等 pyroscope-rs mimalloc backend 合并发布，再升级依赖。
2. 临时使用受控 fork，仅在单独 PR 中接入。
3. 长期只保留 Pyroscope CPU profiling，不恢复 memory profiling。

推荐默认选择 1 或 3，不推荐把 fork 接入和 allocator 切换绑定在一个 PR 中。

## 5. 远程 baseline 操作清单

### 5.1 远程准备

确认远程机器：

```bash
ssh azure-20780104 'hostname; uname -a; pwd'
```

本次已确认远程机器基础信息：

- SSH alias：`azure-20780104`
- hostname：`rustfs-jumpbox`
- OS：Ubuntu Linux x86_64 gnu，kernel `6.17.0-1015-azure`
- 源码主 checkout：`/data/rustfs/rustfs`
- Rust toolchain：`/root/.cargo/env`，非交互 SSH shell 需要先 `source /root/.cargo/env`
- warp：`/usr/local/bin/warp`，版本 `v1.5.0`
- `origin/main` 已 fetch 到基线提交 `b57820a486a27ccd263f2b4a8e329d9c37615a8c`

确认仓库状态：

```bash
ssh azure-20780104 'cd /data/rustfs/rustfs && git status --short --branch && git rev-parse --short HEAD'
```

为避免破坏远程主 checkout，本次 baseline 使用独立 worktree，不在 `/data/rustfs/rustfs` 上执行 `reset --hard`：

```bash
ssh azure-20780104 'cd /data/rustfs/rustfs && git fetch origin main && git worktree add -B houseme/mimalloc-allocator-baseline /data/rustfs/rustfs-allocator-baseline b57820a486a27ccd263f2b4a8e329d9c37615a8c'
```

注意：如果 `/data/rustfs/rustfs-allocator-baseline` 已存在，应先检查它是否是本任务 worktree；只允许清理本任务创建的 worktree。

### 5.2 构建

```bash
ssh azure-20780104 'source /root/.cargo/env && cd /data/rustfs/rustfs-allocator-baseline && cargo build --release -p rustfs --bin rustfs'
```

### 5.3 启动服务

根据远程已有 benchmark 拓扑选择以下之一：

1. 使用已有 Docker/compose 4 节点集群。
2. 使用已有本地单机多盘 RustFS。
3. 使用 `scripts/run_get_metrics_gate_smoke.sh` 自动启动单机多盘服务，仅作为 smoke baseline。

allocator A/B 推荐使用和历史性能测试一致的 4 节点/16 盘或 32C64G 环境；如果远程暂时只有单机多盘，也可以先固定 smoke baseline，但文档和 PR 中必须标注环境限制。

### 5.4 固定 jemalloc 结果

```bash
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_DIR="target/bench/allocator-ab/jemalloc-${RUN_ID}"
mkdir -p "${OUT_DIR}"

uname -a > "${OUT_DIR}/host-uname.txt"
lscpu > "${OUT_DIR}/host-lscpu.txt"
free -h > "${OUT_DIR}/host-free.txt"
rustc --version > "${OUT_DIR}/rustc-version.txt"
cargo --version > "${OUT_DIR}/cargo-version.txt"
git status --short --branch > "${OUT_DIR}/git-status.txt"
git rev-parse HEAD > "${OUT_DIR}/git-head.txt"

scripts/run_object_batch_bench_enhanced.sh \
  --tool warp \
  --endpoint http://127.0.0.1:9000 \
  --access-key rustfsadmin \
  --secret-key rustfsadmin \
  --bucket rustfs-allocator-jemalloc \
  --bucket-size-suffix \
  --warp-mode mixed \
  --sizes 1KiB,4KiB,32KiB,1MiB,10MiB \
  --concurrency 128 \
  --duration 60s \
  --rounds 5 \
  --retry-per-round 1 \
  --round-cooldown-secs 30 \
  --out-dir "${OUT_DIR}/mixed"
```

## 6. 风险与回滚

主要风险：

- P99 抖动扩大。
- RSS 峰值高于 jemalloc。
- Pyroscope memory profiling 语义变化被误认为兼容保留。
- Admin memory dump 行为变化影响排障流程。
- jemalloc 相关依赖删除后，某些 cfg 组合编译失败。

回滚策略：

- 代码回滚：恢复 `main.rs` allocator cfg、jemalloc dependencies、profiling memory path、obs memory agent。
- 运行时回滚：第一阶段没有新增 runtime 开关；若 benchmark 不通过，不合并 PR。
- 运维回滚：继续使用 jemalloc 基线版本，保留现有 `MALLOC_CONF` 调优。

## 7. 最终验收

合并前必须满足：

- jemalloc baseline 已固定在 `target/bench/allocator-ab/jemalloc-*`。
- mimalloc 对比结果已固定在 `target/bench/allocator-ab/mimalloc-*`。
- `baseline_compare.csv` 无不可接受退化。
- Linux 下 memory dump unsupported 行为有测试或明确验证。
- Pyroscope CPU profiling 编译通过。
- `cargo fmt --all --check` 通过。
- `make pre-commit` 通过。
- PR 描述明确列出 memory profiling 行为变化。

## 8. 当前状态与下一步

当前状态：

- Linux 远程 jemalloc baseline 已固定。
- 第一阶段代码改造已完成：Linux/macOS/Unix 主路径统一使用 mimalloc，Pyroscope 仅保留 CPU profiling，jemalloc memory pprof dump 路径改为 unsupported。
- Linux 远程 mimalloc 对比已固定，结果见第 10 节。

下一步：

- 将本轮 A/B 作为第一阶段准入证据，但不把它视为最终性能背书。
- 在 PR 前补一轮低噪声复测，优先扩大到 `rounds=5` 或固定远程机空闲窗口。
- 如后续复测仍显示 1KiB/32KiB latency 明显退化，需要先定位写入/删除阶段抖动，再决定是否合并。

## 9. 已固定的 jemalloc baseline

远程路径：

```bash
/data/rustfs/rustfs-allocator-baseline/target/bench/allocator-ab/jemalloc-20260702T023842Z
```

baseline worktree：

```bash
/data/rustfs/rustfs-allocator-baseline
```

基线提交：

```bash
b57820a486a27ccd263f2b4a8e329d9c37615a8c
```

实际 baseline 说明：

- 远程现有 `:9000` 集群在 baseline 前 `/health` 返回 503，且同机存在 `rustfs-uninstall.yml` ansible 进程；因此没有使用现有集群。
- 本次使用隔离单节点多盘 RustFS：`127.0.0.1:19031`，四个本地数据目录，release binary 来自 `b57820a48`。
- 完整完成的可比较矩阵：`mixed`、`concurrency=128`、`duration=60s`、`rounds=3`、sizes=`1KiB,4KiB,32KiB,1MiB`。
- `10MiB mixed concurrency=128` 第 1 轮超过 8 分钟未收敛，已终止并保留日志。
- 补充 `10MiB mixed concurrency=16` 同样未在预期窗口内收敛，已终止并保留日志。
- 后续 mimalloc 对比必须先使用相同隔离单节点入口和相同完成矩阵；`10MiB mixed` 不作为第一阶段硬性 A/B 门槛，只作为风险观察项。

可比较 baseline CSV：

```bash
target/bench/allocator-ab/jemalloc-20260702T023842Z/mixed/median_summary.completed.csv
```

当前 median：

| size | mode | concurrency | successful rounds | median throughput bps | median req/s | median latency ms | median p90 ms | median p99 ms |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| `1KiB` | mixed | 128 | 3 | `2810183.680000` | `609.790000` | `410.800000` | `407.400000` | `1585.800000` |
| `4KiB` | mixed | 128 | 3 | `5148508.160000` | `279.560000` | `1330.000000` | `1241.900000` | `7443.800000` |
| `32KiB` | mixed | 128 | 3 | `39541800.960000` | `268.310000` | `131.800000` | `54.800000` | `4601.100000` |
| `1MiB` | mixed | 128 | 3 | `49356472.320000` | `10.440000` | `169.600000` | `601.100000` | `1096.800000` |

已保存的关键文件：

- `baseline-status.md`
- `host-uname.txt`
- `host-lscpu.txt`
- `host-free.txt`
- `rustc-version.txt`
- `cargo-version.txt`
- `git-head.txt`
- `git-status.txt`
- `binary-ls.txt`
- `rustfs.log`
- `rustfs-ps-before.txt`
- `rustfs-proc-status-before.txt`
- `rustfs-ps-final.txt`
- `rustfs-proc-status-final.txt`
- `mixed/run_manifest.env`
- `mixed/round_results.csv`
- `mixed/median_summary.csv`
- `mixed/median_summary.completed.csv`
- `mixed/logs/*.log`

编码后的 mimalloc A/B 对比命令应引用：

```bash
--baseline-csv target/bench/allocator-ab/jemalloc-20260702T023842Z/mixed/median_summary.completed.csv
```

## 10. 已固定的 mimalloc 对比结果

远程路径：

```bash
/data/rustfs/rustfs-allocator-baseline/target/bench/allocator-ab/mimalloc-20260702T035429Z
```

对比代码状态：

- 基线提交仍为 `b57820a486a27ccd263f2b4a8e329d9c37615a8c`。
- 远程 worktree 应用了当前 mimalloc migration patch，但未提交。
- 单节点多目录压测显式使用 `RUSTFS_UNSAFE_BYPASS_DISK_CHECK=true`，原因是四个测试数据目录位于同一物理设备；第一次未设置该开关的失败证据保留在 `mimalloc-20260702T035156Z`。

可比较 mimalloc CSV：

```bash
target/bench/allocator-ab/mimalloc-20260702T035429Z/mixed/median_summary.csv
```

当前 median：

| size | mode | concurrency | successful rounds | median throughput bps | median req/s | median latency ms | median p90 ms | median p99 ms |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| `1KiB` | mixed | 128 | 3 | `2401239.040000` | `521.340000` | `2386.100000` | `2377.200000` | `3940.200000` |
| `4KiB` | mixed | 128 | 3 | `5200936.960000` | `282.540000` | `1133.500000` | `1056.700000` | `6930.700000` |
| `32KiB` | mixed | 128 | 3 | `33334231.040000` | `226.330000` | `229.100000` | `120.000000` | `4713.500000` |
| `1MiB` | mixed | 128 | 3 | `53991178.240000` | `11.360000` | `49.900000` | `230.900000` | `673.100000` |

A/B 对比：

| size | throughput vs jemalloc | req/s vs jemalloc | latency vs jemalloc | p90 vs jemalloc | p99 vs jemalloc | 判读 |
|---|---:|---:|---:|---:|---:|---|
| `1KiB` | `0.85x` | `0.86x` | `5.81x` | `5.84x` | `2.49x` | 吞吐下降且延迟显著变差，需要复测确认是否为噪声或 allocator 退化。 |
| `4KiB` | `1.01x` | `1.01x` | `0.85x` | `0.85x` | `0.93x` | 基本持平到小幅改善。 |
| `32KiB` | `0.84x` | `0.84x` | `1.74x` | `2.19x` | `1.02x` | 吞吐下降且 p90 变差，需要复测。 |
| `1MiB` | `1.09x` | `1.09x` | `0.29x` | `0.38x` | `0.61x` | 吞吐和延迟均改善，尤其 tail latency 有收益。 |

本轮实战结论：

- 依赖和功能层面：第一阶段替换可行，且收益明确：移除 jemalloc 专用依赖、减少 allocator 分叉、避免继续维护 jemalloc memory pprof dump 路径，Pyroscope CPU profiling 仍保留。
- 行为层面：Linux `dump_memory_pprof_now` 改为 unsupported 是可接受边界，但必须在 PR、release notes、运维文档中明确。
- 性能层面：本轮单机远程 A/B 不支持“全面性能提升”的结论，只支持“可继续推进到 PR 前复测”。4KiB 和 1MiB 改善，1KiB 和 32KiB 存在退化信号。
- 风险层面：远程机同一时段存在 ansible 任务和明显 burst 型负载表现，latency 数据必须保守解读。
- 准入建议：可以保留当前代码方向，但合并前必须执行低噪声复测；若 1KiB/32KiB 退化重复出现，应先定位 allocator collect、write/delete 路径和后台任务交互，再决定是否合并。
