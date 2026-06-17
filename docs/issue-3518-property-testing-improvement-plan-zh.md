# Issue #3518 性质测试改进与落地方案

## 1. 结论

`#3518` 的方向应当采纳，但不建议原样按 issue 中的优先级直接推进。

基于当前 `1.0.0-beta.8` 工作树核实后，我的判断是：

- issue 对“关键路径仍缺少系统化 property testing”的判断成立。
- issue 对候选目标的选择大体正确，但对仓库现状略有低估。
- 最适合的落地方式不是一次性把所有目标都补成 `proptest`，而是按 **P0 协议解析与 range 边界 -> P1 filemeta 序列化边界 -> P1/P2 erasure 不变量 -> P2 通用路径工具** 分阶段推进。

## 2. 现状核实

### 2.1 `proptest` 已经不是“只存在一处”

当前仓库里至少已经有以下 property test：

- `crates/protocols/src/sftp/paths.rs`
  - 已有 `10_000` case 的 `parse_s3_path` 不变量测试
- `crates/protocols/src/sftp/write.rs`
  - 已有写路径相关数值边界性质测试

这说明项目当前缺的不是“从 0 到 1 引入 property testing”，而是：

- 把已经验证过的安全/语义不变量扩散到相邻解析器
- 把关键模块从例子型测试升级为系统化性质测试

### 2.2 Erasure 不是“没有测试”，但还缺少统一性质层

`crates/ecstore/src/erasure_coding/erasure.rs` 已经存在较多测试，包括：

- round-trip
- 丢失 data/parity shard 的恢复
- legacy/non-legacy 分支

当前短板不在“完全没有覆盖”，而在：

- 现有测试多是样例驱动
- 缺少统一的不变量表达：
  - 任意输入数据
  - 任意丢失模式
  - 只要 `loss_count <= parity_shards`
  - 恢复结果必须与原始字节完全一致

### 2.3 Filemeta 已经补过一个关键回归点

`crates/filemeta/src/filemeta/version.rs` 已经覆盖了 `transition_version_id` 的已知回归风险，包括：

- absent -> `None`
- empty bytes -> `None`
- nil UUID -> `None`
- valid UUID -> 正常 round-trip

因此 `filemeta` 当前的重点不是重复回归测试，而是继续向外扩展：

- `FileInfo` msgpack round-trip
- arbitrary bytes 输入下不 panic

### 2.4 Path / validation 工具已有大量表驱动测试

以下模块已经有明显的样例覆盖：

- `crates/utils/src/path.rs`
- `crates/ecstore/src/bucket/utils.rs`
- `rustfs/src/storage/options.rs`
- `crates/protocols/src/swift/object.rs`
- `crates/protocols/src/webdav/driver.rs`

因此这些模块更适合补“性质”，而不是继续堆更多同类型样例。

## 3. 对 issue 原优先级的修正

### 3.1 P0 应优先落在协议解析族与 range parser

最值得先做的是：

- `crates/protocols/src/webdav/driver.rs`
  - `parse_path`
- `crates/protocols/src/ftps/driver.rs`
  - `parse_s3_path`
- `crates/protocols/src/swift/object.rs`
  - `validate_object_name`
  - `decode_object_from_url`
  - `normalize_path`
- `rustfs/src/storage/options.rs`
  - `parse_copy_source_range`

原因：

- 这些点离外部输入最近
- 现有实现偏样例测试，性质测试收益高
- 改动集中、依赖少、CI 成本低
- 可以直接复用 SFTP 已有的路径不变量模型

### 3.2 Erasure 应从 “P0 correctness panic” 调整为 “P1 correctness strengthening”

纠删码当然是核心正确性路径，但当前它并不是“几乎无测试”的状态。

更准确的定位应当是：

- 先保证协议解析和 range 这些更容易被外部输入直接击穿的边界
- 再把 erasure 的正确性从样例测试升级为系统化性质测试

### 3.3 Filemeta 应优先做边界稳健性，而不是只做字段回归

`filemeta` 当前最值得补的是两类性质：

1. `unmarshal(marshal(x)) == x`
2. `unmarshal(arbitrary_bytes)` 不 panic

这两类测试比继续增加单字段回归 case 更有放大收益。

## 4. 关键风险点与代码观察

### 4.1 WebDAV / FTPS 当前更偏“清洗后接受”，不是“严格拒绝”

例如：

- `crates/protocols/src/webdav/driver.rs:768`
- `crates/protocols/src/ftps/driver.rs:151`

它们都依赖 `path::clean(...)` 和 `path_to_bucket_object(...)`。

因此 property test 的首目标不应是“强推 reject 语义”，而应先验证：

- 成功输出时 bucket 不包含 `/`
- key 不包含控制字符
- key 不包含 `..` 段
- key 不以前导 `/` 开头

也就是先把 **输出安全不变量** 固化下来。

### 4.2 Swift 文档约束与实现之间有轻微漂移

`crates/protocols/src/swift/object.rs` 的注释写着：

- object name 不应以前导 `/` 开头

但当前 `validate_object_name(...)` 实现并没有显式拒绝前导 `/`。

因此在给 Swift 加 property test 之前，应先明确两件事：

1. 当前实现是否要保持兼容
2. 文档是否要与实现重新对齐

否则测试会把当前漂移状态直接固化下来。

### 4.3 `parse_copy_source_range` 存在明显的边界测试价值

`rustfs/src/storage/options.rs:598` 当前主要风险不在复杂逻辑，而在边界输入：

- `bytes=`
- `bytes=-`
- `bytes=abc-def`
- 超长数字
- 多个 `-`
- `start > end`
- suffix range 的极值

另外，issue 提到的 `-i64::MIN` 风险是值得认真对待的：

- suffix range 分支会把解析出来的 `length` 变成 `-length`
- 如果 `length == i64::MIN`，理论上就存在取负溢出风险

即使当前解析路径未必总能稳定走到 panic，测试层也应该明确锁定：

- 非法边界输入必须返回 `Err`
- 绝不能 panic

## 5. 建议的实施方案

### 5.1 PR1: 协议解析与 range 边界

目标：

- 为 `WebDAV`、`FTPS`、`Swift`、`parse_copy_source_range` 建立第一批性质测试

重点不变量：

- 路径成功输出不泄漏 `..` / 控制字符 / 前导 `/`
- URL decode 成功后输出必须满足对象名校验约束
- `normalize_path(normalize_path(x)) == normalize_path(x)`
- range parser 对非法输入只返回 `Err`，不 panic

建议收益：

- 成本最低
- 回报最快
- 能立刻把 SFTP 已有模式推广到 sibling parser

### 5.2 PR2: Filemeta round-trip 与 no-panic

目标：

- 为 `crates/filemeta` 建立结构化的随机输入模型

重点不变量：

- 对受限 `FileInfo` 模型：`unmarshal(marshal(x)) == x`
- 对 arbitrary bytes：`unmarshal(...)` 返回 `Ok/Err` 都可以，但不能 panic

约束建议：

- 控制 metadata / parts / checksum 数量，避免 CI 时间失控
- 优先覆盖 optional fields、UUID、time、version-related 字段

### 5.3 PR3: Erasure correctness property tests

目标：

- 把纠删码测试从样例驱动升级为真正的不变量校验

重点不变量：

- 任意数据输入
- 任意 `loss_count <= parity_shards`
- 恢复后字节流与原始输入完全一致

建议约束：

- `data_shards` 控制在 `2..=8`
- `parity_shards` 控制在 `1..=4`
- 数据大小控制在 `0..=64KiB`
- 普通编码与 legacy 编码各保留一组性质测试

### 5.4 PR4: 通用 path / validator 工具补强

目标：

- 为通用工具补上“规律型”测试，而不是更多样例

候选点：

- `crates/utils/src/path.rs`
- `crates/ecstore/src/bucket/utils.rs`

重点不变量：

- `clean(clean(x)) == clean(x)`
- `normalize/clean` 输出不产生额外双斜杠或异常段
- object prefix / object name 的 grammar 不变量稳定成立

## 6. 实施细节建议

### 6.1 复用现有 `proptest` 依赖策略

当前 `crates/protocols` 已经具备 `proptest` 依赖。

因此第一阶段建议：

- 先在已有依赖的模块中扩散 property tests
- `filemeta`、`ecstore`、`utils` 再按需补充 dev-dependency

这样可以减少一次性大范围改 `Cargo.toml` 带来的噪音。

### 6.2 避免一次性引入过大的输入空间

这批工作目标是提高稳定性，不是制造 CI 噪声。

建议：

- 单个 property test 不要盲目设置超高 case 数
- 对结构化输入设置上限
- 对高成本测试放在独立模块，必要时单独降低 case 数

### 6.3 先锁定不变量，再决定是否调整语义

对于 WebDAV / FTPS / Swift 这类解析器，第一阶段建议：

- 先验证“当前实现成功返回时，输出必须安全”
- 不先大规模改变兼容行为

这样可以先建立回归护栏，再根据 issue 反馈决定是否收紧语义。

## 7. 验收标准

本轮工作的完成标准建议定义为：

1. `protocols` 至少新增一组跨协议 sibling parser 的 property tests。
2. `parse_copy_source_range` 新增边界性质测试，覆盖非法 suffix 极值输入不 panic。
3. `filemeta` 至少新增一组 round-trip 性质测试，和一组 arbitrary bytes no-panic 测试。
4. `erasure` 至少新增一组 “任意可恢复丢失模式 -> 数据完全恢复” 的性质测试。
5. 所有新增测试都能稳定进入 CI，不显著拉长主线耗时。

## 8. 最终建议

如果只允许先做一件事，我建议先做：

- **PR1: 协议解析族 + `parse_copy_source_range` 的性质测试补齐**

原因很直接：

- 离外部输入最近
- 当前最缺“系统化不变量”
- 改动小、验证快、回归价值高

之后再依次推进：

- `PR2: filemeta`
- `PR3: erasure`
- `PR4: utils/ecstore path validators`

这样能在控制风险和 CI 成本的前提下，把 `#3518` 从“方向建议”落成一条清晰、可拆分、可交付的测试改进路线。
