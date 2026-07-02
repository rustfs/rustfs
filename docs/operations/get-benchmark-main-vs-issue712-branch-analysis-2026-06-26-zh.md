# `main` vs `houseme/issue-712-gt1g-multipart-server-path` GET 压测回退分析

## 1. 结论摘要

本次对比的核心结论可以先直接收敛为一句话：

`main` 不是“整体 GET 退化”，而是“在引入部分正向优化的同时，又叠加了新的固定开销”，最终表现为：

1. 小对象 `1KiB`/`4KiB`/`10KiB` 相比 `06-25` 有轻微回退
2. 中等对象里 `32KiB`/`1MiB`/`4MiB` 反而有提升
3. `10MiB` 出现最明显的回退，属于本轮最需要优先处理的异常点

从代码面看，最强相关的回退来源不是 `main` 上的那些大对象 GET 优化本身，而是 `06-26` 新增的 GET 可观测性埋点采用了“默认始终执行”的方式，且没有像 PUT 一样做启停门控；这部分固定成本在 metrics/export 关闭时仍然存在。  
同时，`10MiB` 恰好又踩在默认的 in-memory seek-support 阈值上，导致它成为“最容易放大额外固定成本”的尺寸点。

## 2. 本次比对基线

### 2.1 分支关系

本地代码关系显示：

1. `houseme/issue-712-gt1g-multipart-server-path` 是当前 `main` 的祖先
2. 二者 merge-base 为 `efebcb66be412fa2d11f5e9dcfd7b027aa005b3e`
3. 因此本次分析不应理解为“两条完全不同的实现线”，而应理解为：
   - `06-25` 是老基线
   - `06-26` 是在该基线上继续叠加了一批提交后的结果

### 2.2 用户提供的关键数据

按你给出的表，直接看 `06-25` vs `06-26`：

| Size | 06-25 | 06-26 | 变化 |
|---|---:|---:|---:|
| `1KiB` | `7792` | `7373` | `-5.4%` |
| `4KiB` | `7740` | `7274` | `-6.0%` |
| `10KiB` | `7549` | `7125` | `-5.6%` |
| `32KiB` | `5388` | `6670` | `+23.8%` |
| `1MiB` | `1252` | `1283` | `+2.5%` |
| `4MiB` | `1040` | `1136` | `+9.2%` |
| `10MiB` | `624` | `476` | `-23.7%` |

所以这次不是“全线退化”，而是明显的“分尺寸分层表现”。

## 3. `main` 相对 issue-712 分支新增的 GET 相关代码面

从 branch 分叉点到当前 `main`，真正和本次 GET 结果强相关的提交主要是三类：

### 3.1 正向优化：`96b1f5c37`

提交：

1. `96b1f5c37 feat(storage): optimize gt1g get read path`

关键变化：

1. 顺序 GET 的 `sequential hint` 真正参与 I/O 策略计算
2. media cap 不再被统一 `1MiB` clamp 二次压回
3. 对 `>= 1GiB`、非 range、允许 readahead 的 GET 扩大 `ReaderStream` buffer

代码证据：

1. `rustfs/src/app/object_usecase.rs:1581`
2. `rustfs/src/app/object_usecase.rs:1587`
3. `rustfs/src/app/object_usecase.rs:2162`

判断：

1. 这部分主要影响 `>1GiB` 顺序 GET
2. 对你这次重点异常的 `10MiB` 并不是直接解释项
3. 它更可能解释“大对象类场景为什么没有整体变差”

### 3.2 PUT/ingest 侧实验：`ef0dcec47`

提交：

1. `ef0dcec47 feat(ecstore): add deeper zero-copy ingest experiment`

关键变化：

1. `zero_copy_eager` PUT 路径
2. `bytesmut` erasure ingest gate
3. 重点在写入/编码入口，而不是 GET 读路径

判断：

1. 它不是本轮 GET 回退的第一嫌疑项
2. 但如果 `06-25` 和 `06-26` 的 benchmark 对象是“各自重新生成”的，而不是复用同一批对象，那么它属于需要单独排除的次级变量
3. 更严格的结论需要在“完全相同对象集”上复测一次

### 3.3 最强相关新增成本：`7bf411f46`

提交：

1. `7bf411f46 obs(get): add GET read pipeline metrics`

这批改动同时做了三件事：

1. GET handler 分阶段耗时打点
2. GET reader/stream strategy 与 handoff 打点
3. legacy duplex / codec streaming 路径增加读链路诊断和失败分类

最关键的问题不在“有埋点”，而在“埋点是默认执行、未按 exporter 状态门控”。

## 4. 为什么我判断回退主因在 GET 埋点，而不是其它提交

### 4.1 PUT 已经有门控，GET 没有

`crates/io-metrics/src/lib.rs:54` 到 `crates/io-metrics/src/lib.rs:74` 已经明确给 PUT 做了门控：

1. `PUT_STAGE_METRICS_ENABLED`
2. 注释明确写了：当关闭时，应避免 `Instant::now()` 等额外系统调用

但 GET 这批新增埋点并没有对应的 `GET_STAGE_METRICS_ENABLED`。

对应代码：

1. `crates/io-metrics/src/lib.rs:276`
2. `crates/io-metrics/src/lib.rs:286`
3. `crates/io-metrics/src/lib.rs:351`
4. `crates/io-metrics/src/lib.rs:483`
5. `crates/io-metrics/src/lib.rs:489`

也就是说：

1. PUT 路径已经承认“埋点默认常开会有成本”
2. GET 路径却把同类成本直接放进了热路径

这是本次最强的代码层面证据。

### 4.2 即使 metrics export 关闭，GET 额外成本仍然存在

你本地 `06-26` 的单机 GET 记录里，metrics export 是关闭的：

1. `target/bench/issue714-local-single-node-get-20260626-112352/rustfs/meta.env:15`

也就是：

1. `RUSTFS_OBS_METRICS_EXPORT_ENABLED=false`

但 GET 打点仍然在热路径上无条件执行：

1. `rustfs/src/app/object_usecase.rs:1794` 到 `rustfs/src/app/object_usecase.rs:1800`
2. `rustfs/src/app/object_usecase.rs:1826` 到 `rustfs/src/app/object_usecase.rs:1830`
3. `rustfs/src/app/object_usecase.rs:2975` 到 `rustfs/src/app/object_usecase.rs:2981`
4. `rustfs/src/app/object_usecase.rs:3026` 到 `rustfs/src/app/object_usecase.rs:3032`
5. `rustfs/src/app/object_usecase.rs:3061` 到 `rustfs/src/app/object_usecase.rs:3065`
6. `rustfs/src/app/object_usecase.rs:1610` 到 `rustfs/src/app/object_usecase.rs:1643`

尤其 `record_get_object_stream_strategy()` 和 `record_get_object_response_handoff()` 里还做了：

1. `strategy.to_string()`
2. `buffer_source.to_string()`

对应：

1. `crates/io-metrics/src/lib.rs:276` 到 `crates/io-metrics/src/lib.rs:316`

也就是说，即使 exporter 关闭，这些：

1. `Instant::now()`
2. label 组装
3. counter/histogram 调用

仍然会成为每个 GET 请求的固定成本。

### 4.3 这些新增成本与当前数据形态吻合

如果是真正的“底层读路径坏了”，一般会更像：

1. `4MiB`、`10MiB`、`16MiB`、`32MiB` 一起向下
2. 或所有尺寸统一退化

但你给的数据不是这样：

1. `32KiB` 提升很大
2. `1MiB`、`4MiB` 也有提升
3. 最差点集中在 `10MiB`

这更像：

1. 一部分路径被优化了
2. 但请求级固定成本增加了
3. 某个尺寸阈值把这个固定成本放大了

也就是现在看到的“局部退化而不是全线退化”。

## 5. 为什么 `10MiB` 会成为最差点

`10MiB` 的异常，并不是因为 `main` 新引入了一个 10MiB 阈值，而是因为已有阈值与新增成本在这个点发生了叠加。

### 5.1 `10MiB` 正好踩中 in-memory seek-support 阈值

当前默认：

1. `RUSTFS_OBJECT_SEEK_SUPPORT_THRESHOLD = 10 MiB`

代码：

1. `crates/config/src/constants/runtime.rs:110` 到 `crates/config/src/constants/runtime.rs:112`

GET body 构建逻辑里，小于等于该阈值、非 range、非 part-number 时，会先整对象读入内存：

1. `rustfs/src/app/object_usecase.rs:2139` 到 `rustfs/src/app/object_usecase.rs:2155`

这意味着：

1. `4MiB` 也会走内存缓冲
2. 但 `10MiB` 是“仍走内存缓冲的最大默认尺寸”
3. 在 `concurrency=32` 下，它的瞬时内存与复制压力会明显高于 `4MiB`

### 5.2 `10MiB` 是最容易把“固定成本 + 内存复制成本”同时放大的尺寸

在这个点上会同时叠加：

1. GET handler 新增的一批阶段打点
2. ReaderStream strategy/handoff 新增打点
3. `Vec::with_capacity(10MiB)` + `read_to_end()` 的整对象内存聚合
4. 之后再把聚合结果包装成响应体

因此 `10MiB` 很容易出现：

1. 比 `4MiB` 更明显的延迟放大
2. 又还没有进入 `>10MiB` 之后更偏 streaming 的自然形态

换句话说：

1. `10MiB` 不是“主因”
2. 它是“最容易暴露主因的尺寸”

### 5.3 这也解释了为什么 `4MiB` 没有同步恶化

因为：

1. `4MiB` 同样有 seek-support buffering
2. 但其内存放大和复制量远小于 `10MiB`
3. 同时 `main` 的其它局部正向收益还足以把它托起来

所以最终表现成：

1. `4MiB` 仍有提升
2. `10MiB` 却明显回退

## 6. 次级因素：哪些改动“看上去像嫌疑项”，但我认为不是主因

### 6.1 codec streaming reader

`main` 新增了单 part plain object 的 codec streaming reader：

1. `crates/ecstore/src/set_disk.rs:359` 到 `crates/ecstore/src/set_disk.rs:360`
2. `crates/ecstore/src/set_disk.rs:402` 到 `crates/ecstore/src/set_disk.rs:429`
3. `crates/ecstore/src/set_disk.rs:1152` 到 `crates/ecstore/src/set_disk.rs:1167`

但默认值是：

1. `DEFAULT_RUSTFS_GET_CODEC_STREAMING_ENABLE = false`

也就是说，只要你没有额外开 env，它默认不会启用。  
所以它不是这次 `06-25` vs `06-26` 的直接主因。

### 6.2 GET metadata cache

`main` 在 `get_object_fileinfo()` 增加了 metadata cache：

1. `crates/ecstore/src/set_disk/read.rs:674` 到 `crates/ecstore/src/set_disk/read.rs:676`
2. `crates/ecstore/src/set_disk/read.rs:722` 到 `crates/ecstore/src/set_disk/read.rs:724`

这更像是正向项，可能帮助：

1. `32KiB`
2. `100KiB`
3. `1MiB`

这类更容易被 metadata / lock / fileinfo 读取成本支配的对象尺寸。

所以它解释的是“为什么有些点变好”，不是“为什么 10MiB 变差”。

### 6.3 大对象 sequential GET buffer 优化

`main` 对 `>=1GiB` 的 sequential GET 做了 buffer 扩张：

1. `rustfs/src/app/object_usecase.rs:1587` 到 `rustfs/src/app/object_usecase.rs:1592`

这不作用于 `10MiB`，因此也不是本轮 10MiB 回退原因。

## 7. 高标准结论口径

如果把本轮结论压缩成适合对外同步的高标准表述，我建议直接使用下面这版：

1. `main` 相比 `houseme/issue-712-gt1g-multipart-server-path` 并未出现全局性 GET 退化，而是呈现明显的分尺寸分层结果
2. `>1GiB` 相关的顺序 GET 调度与 buffer 优化不是本轮回退原因
3. 本轮最强相关的退化来源是 `obs(get)` 提交引入的 GET 热路径埋点默认常开，且没有像 PUT 一样按 observability/export 状态做门控
4. 在 `metrics export=false` 的运行条件下，这些 GET 埋点依然产生 `Instant::now()`、label 组装和 histogram/counter 调用成本
5. `10MiB` 又恰好踩中默认 `seek-support` 内存缓冲阈值，导致请求级固定成本和整对象内存聚合成本在该点被共同放大，因此出现 `-23.7%` 的最明显回退
6. `32KiB`/`1MiB`/`4MiB` 等点的提升，说明 `main` 上并非只有负向变化，metadata cache 等局部优化已开始带来收益

## 8. 优化改进建议

### 8.1 P0：给 GET 埋点补“和 PUT 同级别”的开关门控

这是优先级最高、收益最确定的一项。

建议：

1. 新增 `GET_STAGE_METRICS_ENABLED`
2. 在 exporter/metrics 关闭时，直接跳过：
   - `Instant::now()`
   - `record_get_object_stage_duration()`
   - `record_get_object_stream_strategy()`
   - `record_get_object_response_handoff()`
   - 以及 legacy duplex 下的 decode/reader-setup 时长上报

目标：

1. 先把“metrics 关闭但仍付埋点成本”的问题切掉
2. 这一步对 `1KiB` 到 `10MiB` 的改善最确定

### 8.2 P0：去掉 GET 热路径上的动态 label 分配

当前最典型的是：

1. `strategy.to_string()`
2. `buffer_source.to_string()`

建议：

1. 改成静态 label 直传
2. 或在门控打开后才构造动态 label

因为这类字符串构造在高 QPS、小对象场景下没有必要。

### 8.3 P1：把 `seek-support` 阈值从固定值改成“并发感知”

当前 `10MiB` 的 cliff 太明显，不建议继续使用静态 `10MiB` 作为所有并发级别下的统一阈值。

建议：

1. 当 `ACTIVE_GET_REQUESTS >= 16` 或更高时
2. 把 `RUSTFS_OBJECT_SEEK_SUPPORT_THRESHOLD` 的有效值动态下压到 `1MiB` 或 `4MiB`
3. 让 `10MiB` 默认回归 streaming，而不是整对象先读入内存

这项对修复 `10MiB` 单点回退最直接。

### 8.4 P1：把 `10MiB` 单独做 A/B 验证，不要再混在全量矩阵里猜

建议最小验证矩阵：

1. `10MiB`
2. `concurrency=32`
3. 同一批对象
4. 同一台机器
5. 同一套环境变量

只做三组：

1. 当前 `main`
2. `main + 关闭 GET metrics`
3. `main + 关闭 GET metrics + 降低 seek-support threshold`

如果第二组就明显回升，说明主因就是埋点。  
如果第三组再继续回升，说明 `10MiB` cliff 也被证实。

### 8.5 P2：codec streaming 作为后续收益项，不建议混进这次回退修复

因为它默认关闭，而且这次的目标不是“再引入一个实验路径”，而是先把 `main` 的回退因素拿掉。

建议：

1. 先修 GET metrics 门控
2. 再单独做 codec streaming 实验
3. 避免两个变量混在一起影响判断

## 9. 建议的后续验证顺序

建议按这个顺序推进：

1. 先复测 `main` 当前版本，保持同一批对象
2. 关闭 GET metrics，再复测 `1KiB`/`4KiB`/`10KiB`/`10MiB`
3. 仅调整 `seek-support threshold`，再复测 `10MiB`
4. 如果对象是重新生成的，再补一轮“同对象集复测”，彻底排除 PUT-path 变量

## 10. 最终结论

这次回退的最可信定因不是：

1. `main` 的大对象 GET 优化失败
2. 也不是 codec streaming 默认启用
3. 更不是一次全面性的 GET 读路径退化

而是：

1. `06-26` 新增的 GET 可观测性埋点把固定成本直接放进了热路径
2. 这部分成本没有像 PUT 一样在 metrics 关闭时被门控掉
3. `10MiB` 又正好是默认 seek-support 内存缓冲的上边界，于是成为最明显的退化点

如果只做一件事，我建议先做：

1. `给 GET metrics 加门控，并复测 10MiB`

如果要做第二件事，再做：

1. `把 seek-support threshold 改成并发感知，而不是固定 10MiB`

这两刀最有机会把当前这次 `main` 上的 GET 回退问题快速、清晰、可验证地收回来。
