# Issue #3519 Fuzzing 改进与完善方案

## 1. 结论

`#3519` 的方向应当采纳，但不能只停留在“补几个 fuzz target”。

基于当前 `1.0.0-beta.8` 工作树核实后，我的判断是：

- issue 对“仓库缺少 coverage-guided fuzzing”的判断成立。
- issue 给出的目标列表大体正确，但还需要按**攻击面类型、仓库边界、CI 成本、已有防线成熟度**重新分层。
- 最适合的落地方式不是一次性把所有目标都接进来，而是按 **P0 基础设施 + P0 路径约束 + P0/P1 解析面 + P1 本地元数据/压缩解析 + P2 扩面** 分阶段推进。

## 2. 现状核实

### 2.1 仓库当前没有 fuzz 基础设施

已核实当前仓库没有下列内容：

- `fuzz/` 或 `fuzz_targets/` 目录
- `cargo-fuzz` / `libfuzzer-sys` / `honggfuzz` / `afl`
- 面向 fuzz 的 `arbitrary` 输入模型
- GitHub Actions 中的 fuzz job

同时，现有 CI 主路径仍以常规测试为主：

- `.github/workflows/ci.yml` 当前核心仍是 `cargo nextest`、`cargo test --doc`、`cargo fmt --check`、`cargo clippy`
- `Makefile` / `.config/make/` 也没有 fuzz 入口

### 2.2 当前“随机化测试”只覆盖局部性质校验

目前仓库里可见的随机化测试主要是 `proptest`：

- `crates/protocols/src/sftp/paths.rs`
- `crates/protocols/src/sftp/write.rs`
- 依赖位于 `crates/protocols/Cargo.toml`

这说明项目不是完全没有性质测试，但这些测试：

- 只覆盖局部模块
- 不做 coverage-guided 输入演化
- 不能替代对攻击面 parser/path 入口的持续 fuzz

## 3. 对 issue 原方案的补充判断

### 3.1 总体方向正确

issue 将优先级放在以下几类面上，是合理的：

- 路径归一化与目录逃逸
- S3/XML/JSON 解析入口
- 范围头/数值边界解析
- 解压/归档处理
- 本地元数据与压缩块解析

### 3.2 但还需要三个关键修正

#### 修正一：要按“外部攻击面”和“本地稳健性面”分开

当前目标混合了两类问题：

- **外部攻击面**：HTTP/S3 请求体、Header、路径、归档上传
- **本地稳健性面**：`xl.meta`、压缩块、磁盘元数据读取

这两类都值得做，但不应放在同一批次里：

- 外部攻击面应优先进入 PR/夜间 fuzz 主线
- 本地稳健性面更适合作为第二阶段补强

#### 修正二：要按仓库边界拆开

RustFS 的 S3 协议解析很大一部分依赖 `s3s`：

- `Cargo.toml` 中 `s3s` 来自 `https://github.com/rustfs/s3s`

这意味着：

- **S3 XML body / aws-chunked / 某些 header 语法解析** 的最佳 fuzz 落点其实在 `s3s` 仓库
- RustFS 仓库更适合 fuzz：
  - RustFS 自己的路径约束与落盘逻辑
  - RustFS 自己的策略/配置兼容层
  - RustFS 自己的归档处理、文件元数据、压缩读取器

如果把所有解析目标都塞进本仓库，会让目标边界变得模糊，也不利于后续修复归属。

#### 修正三：已有局部防线要转化为“可持续不变量”

例如：

- `crates/ecstore/src/bucket/utils.rs` 已经有 `has_bad_path_component`、`is_valid_object_prefix`
- `crates/ecstore/src/disk/local.rs` 已经有 `check_valid_path`、`normalize_path_components`
- `rustfs/src/app/object_usecase.rs` 已经对 archive 提取路径做了 `validate_extract_relative_path`

这说明部分防线已经存在。
下一步不只是“再测一遍”，而是要把这些规则升级成**统一、持续执行的不变量 fuzz**。

## 4. 代码层面的重点观察

### 4.1 路径安全目前是“多层防线”，但缺少跨层联合验证

当前路径相关逻辑分布在多个层次：

- `crates/ecstore/src/bucket/utils.rs:118`
  - `has_bad_path_component`
- `crates/ecstore/src/bucket/utils.rs:170`
  - `is_valid_object_prefix`
- `crates/ecstore/src/disk/local.rs:992`
  - `check_valid_path`
- `crates/ecstore/src/disk/local.rs:1946`
  - `normalize_path_components`
- `crates/utils/src/path.rs:440`
  - `clean`

问题不在于单个函数完全没防护，而在于：

- 这些函数由不同调用链组合使用
- 当前缺少“输入先校验、再拼接、再归一化、再判断 root containment”的联合性质验证

这正是最值得先做 fuzz 的地方。

### 4.2 archive 提取路径已有限制，但 fuzz 目标要从“路径逃逸”扩展到“资源滥用”

当前 archive 提取已经有明显防线：

- `rustfs/src/app/object_usecase.rs:856`
  - `validate_extract_relative_path`
- `rustfs/src/app/object_usecase.rs:880`
  - `normalize_extract_entry_key`
- `rustfs/src/app/object_usecase.rs:4821`
  - 已有拒绝 bucket escape 的测试

因此这块不应只重复验证 `../`。
更值得增强的是：

- path decoding 异常组合
- pax/header 衍生路径
- 超大 entry 数量
- 超大累计解压体积
- `ignore_errors` 分支下的持续推进是否会放大资源消耗

### 4.3 `parse_copy_source_range` 值得 fuzz，但 issue 里的溢出描述需要更精确

当前实现位于：

- `rustfs/src/storage/options.rs:598`

该函数确实适合做边界 fuzz，但本地这段实现里更值得关注的是：

- 非法负值
- 超长数字
- 多个 `-`
- 空区间
- `start > end`

issue 提到的 `-i64::MIN` 类风险，更可能是一个**更广义的 range parser 家族问题**，不应只把焦点落在当前 helper 上。

### 4.4 `compress_reader` 存在非常直接的 panic 型 fuzz 价值

当前代码位于：

- `crates/rio/src/compress_reader.rs:307`

这里直接对 `compressed_buf[0..16]` 做切片，再调用 `uvarint`。
如果输入不足 16 字节，这类代码天然适合 fuzz：

- 很容易触发越界 panic
- 触发条件明确
- 修复收益高

这是一个比“泛泛而谈的 parser fuzz”更容易快速产出价值的 P1 入口。

### 4.5 Serde unknown-field 治理已有抽象，但还没转成实际 fuzz 策略

仓库里已经有：

- `crates/security-governance/src/serde_policy.rs`

而且测试里已经表达了治理意图：

- `StrictIngress` 应该 `deny_unknown_fields`
- `TolerantCompat` 则不能一刀切 deny

这意味着配置/策略解析不只是“能不能 parse”，还带有**兼容性与安全性治理约束**。
因此 parser fuzz 方案里应加入：

- 对 strict ingress 模型验证“未知字段必须拒绝”
- 对 tolerant compat 模型验证“未知字段不 panic，且产生预期行为”

## 5. 建议的总体实施方案

### 5.1 先补基础设施，不要直接堆 target

建议先做一个单独的 fuzz 基础设施 PR，只解决以下问题：

1. 新建顶层 `fuzz/` 目录，作为独立 fuzz harness
2. 明确它**不进入主 workspace 测试路径**
3. 建立最小运行脚本与文档
4. 定义 corpus / artifacts / crash triage 目录规范

建议目录结构：

```text
fuzz/
  Cargo.toml
  fuzz_targets/
    path_containment.rs
    s3_ingress_parsers.rs
    archive_extract.rs
    local_metadata.rs
  corpus/
    path_containment/
    s3_ingress_parsers/
    archive_extract/
    local_metadata/
  artifacts/
scripts/fuzz/
  run_ci_targets.sh
  run_nightly_targets.sh
docs/
  issue-3519-fuzzing-improvement-plan-zh.md
```

关键原则：

- 不做一个“万能大 target”
- 每个 target 只覆盖一类不变量
- 每个 target 都能独立 seed / 独立裁剪 / 独立 crash triage

### 5.2 优先级重排

#### P0-1：路径归一化与 root containment

首个 P0 target 建议覆盖：

- `has_bad_path_component`
- `is_valid_object_prefix`
- `clean`
- `normalize_path_components`
- 与 root 拼接后的 containment 结果

要验证的不变量不是“返回 true/false 是否符合直觉”，而是：

- 对任何输入，**只要被接受**，最终拼接到 root 后都必须仍在 root 内
- 任何被接受的 key，规范化后都不能形成 `..` 逃逸语义
- Windows 分隔符、连续分隔符、空段、前后空白段都不能破坏该不变量

建议输入模型：

- `root: String`
- `bucket: String`
- `object: String`
- `prefix: Option<String>`
- `use_backslash: bool`

建议初始语料：

- 当前单元测试里的合法/非法路径样本
- issue 中提到的 `..` / `.` / `//` / `\\` / `\0`
- 长路径、空路径、只含空白段的路径

#### P0-2：S3 ingress parser 面

这一块建议拆成两个仓库维度：

- `s3s` 仓库：
  - `DeleteObjects`
  - `CompleteMultipartUpload`
  - aws-chunked
  - 直接 XML body 解析入口
- RustFS 仓库：
  - `BucketPolicy` JSON 兼容解析
  - RustFS 自己持久化/治理相关配置兼容层
  - Notification / Replication 等 RustFS 自身补充逻辑

RustFS 仓库内的目标不应重复 fuzz `s3s` 已经负责的 XML 语法细节，而应聚焦：

- 解析后对象进入 RustFS 逻辑时是否 panic
- strict/tolerant 语义是否符合治理约束
- 恶意嵌套、重复字段、超大数组、异常 unicode 是否导致高资源消耗

#### P1-1：archive extract 路径与资源约束

虽然路径逃逸已有一定保护，但仍建议保留独立 target，重点验证：

- 提取后 key 始终受限于目标 bucket 命名空间
- `prefix` 不能把安全路径重新变成不安全路径
- 目录项与文件项后缀处理一致
- 异常路径编码、PAX 扩展、空路径、超长路径不会 panic
- 后续若补 entry-count / total-size 限制，要把这些限制写成 fuzz 可验证不变量

#### P1-2：本地元数据与压缩块解析

建议把下列内容放到同一批次：

- `crates/rio/src/compress_reader.rs`
- `crates/utils/src/compress.rs`
- `crates/filemeta` 中 `xl.meta` / filemeta 解码路径

目标是：

- 截断输入返回错误而不是 panic
- 错误块头返回错误而不是 panic
- 长度字段、版本字段、压缩算法字段异常时不会出现死循环或过量分配

这类 target 的 seed 应直接来自现有 fixtures：

- `crates/filemeta/tests/fixtures/*`

#### P2：补充面

放到后续扩面的目标：

- bucket-name validator
- 其他 header 语法
- 复制/通知/标签等配置对象的细粒度语义 fuzz
- 其他本地格式兼容解析器

## 6. CI 与交付策略

### 6.1 不建议一开始就把 fuzz 纳入 `make pre-commit`

当前仓库规则要求代码 PR 在提交前通过 `make pre-commit`，而现有 CI 也偏重常规测试。
如果一开始就把 fuzz 强行并入 `pre-commit`，会有几个问题：

- 本地开发门槛突然升高
- CI 时间不稳定
- corpus 未稳定前容易出现噪声失败

更稳妥的策略：

- `make pre-commit` 先不变
- 新增独立命令，例如 `make fuzz-smoke`
- PR 上先跑 bounded smoke fuzz
- nightly 再跑长时间 fuzz

### 6.2 建议的 CI 分层

#### PR smoke job

目标：

- 只跑 2 到 3 个最高价值 target
- 每个 target `60` 到 `120` 秒
- 用于快速发现显著回归

建议首批：

- `path_containment`
- `archive_extract`
- `local_metadata` 或 `compress_reader`

#### Nightly fuzz job

目标：

- 跑全量 target
- 保存 corpus
- 收集 crash artifact
- 允许更长运行时间

建议增加：

- 统一 seed 同步脚本
- crash 最小化脚本
- 按 target 单独归档 artifact

## 7. 建议的实施顺序

### 第一阶段：基础设施 PR

输出物：

- `fuzz/` 骨架
- 运行脚本
- README / 使用说明
- 本文档

验收标准：

- 本地可运行至少一个空 target
- 不影响现有 workspace 常规测试路径

### 第二阶段：路径 containment PR

输出物：

- `path_containment` target
- 初始 corpus
- 至少一组来自现有单元测试的 seed

验收标准：

- 可稳定运行
- 明确表达 accepted-path containment invariant

### 第三阶段：archive + compress/local metadata PR

输出物：

- `archive_extract` target
- `local_metadata` target
- 使用现有 fixture 做 corpus seed

验收标准：

- 截断/畸形输入不 panic
- 能复现并守住已知边界

### 第四阶段：parser 分仓推进

输出物：

- RustFS 仓库侧 parser fuzz
- `s3s` 仓库侧独立 issue / PR / fuzz target

验收标准：

- 解析职责边界清晰
- 不把 `s3s` 的问题长期堆在 RustFS 仓库本地兜底

### 第五阶段：CI 接入

输出物：

- PR smoke fuzz workflow
- nightly fuzz workflow
- artifact/corpus 保留策略

验收标准：

- PR 成本可接受
- nightly 能长期积累 corpus

## 8. 不建议的做法

- 不建议一个 target 同时覆盖路径、XML、压缩、archive 四类问题
- 不建议一开始就把所有 issue 中提到的目标一次性接入
- 不建议把 `s3s` parser 问题全部下沉到 RustFS 本仓库处理
- 不建议只写“never panic”断言，不写 containment / bounded-resource 这类语义不变量
- 不建议在 fuzz 基础设施尚未稳定前把它并进 `make pre-commit`

## 9. 最终建议

如果要把 `#3519` 真正做成高质量改进，我建议按下面的 issue/PR 拆分执行：

1. `fuzz: scaffold standalone cargo-fuzz harness`
2. `fuzz: add path containment invariant target`
3. `fuzz: add archive extraction safety target`
4. `fuzz: add local metadata and compress reader target`
5. `ci: add bounded PR fuzz smoke job`
6. `ci: add nightly fuzz corpus job`
7. `s3s: add parser fuzz targets in upstream fork`

一句话总结：

`#3519` 应采纳，但要从“补 fuzz target”升级成“建立分层 fuzz 体系”，并且把 **路径 containment 不变量** 作为首个 P0 目标，把 **archive / local metadata / compress reader** 作为最快产生实效的第二批目标，把 **S3 parser fuzz** 按 RustFS / `s3s` 仓库边界拆开推进。
