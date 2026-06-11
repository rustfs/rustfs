# RustFS 日志标准、脱敏规则与最小 Guard Rails 2026-06-11

本文件定义 RustFS 运行时日志的最小统一标准，供后续日志治理批次和
新增代码直接遵循。

## 适用范围

优先适用于：

- `rustfs/src`
- `crates/obs`
- `crates/audit`
- `crates/notify`
- `crates/ecstore`
- `crates/targets`

测试代码、一次性脚本、临时调试输出不在本标准的第一轮强约束范围内，
但新增生产路径代码默认应遵守。

## 一、事件模型

运行时日志默认采用：

- 短 message
- 稳定字段优先
- 动态 prose 只做补充，不做主查询面

推荐公共字段：

- `event`
- `component`
- `subsystem`
- `request_id`
- `trace_id`
- `span_id`
- `bucket`
- `object`
- `target_id`
- `peer_addr`
- `status_code`
- `duration_ms`
- `result`
- `error`
- `retry_count`

### 推荐写法

```rust
warn!(
    event = "iam_bootstrap_retry_failed",
    component = "startup_iam",
    attempts,
    next_retry_secs = next_interval.as_secs(),
    error = %err,
    "IAM bootstrap retry failed; service remains degraded"
);
```

### 不推荐写法

```rust
warn!("IAM bootstrap retry failed after {} attempts: {:?}", attempts, err);
```

原因：

- 不利于统一检索
- 字段不可稳定聚合
- 不利于跨 sink 输出一致

## 二、级别语义

### `error`

只用于：

- 当前操作失败
- 需要值班/运维关注
- 无法按预期继续当前动作

不应用于：

- 预期内重试
- 已知降级分支
- 可恢复的普通探测失败

### `warn`

用于：

- 降级状态
- 非预期但仍可继续
- 重试中
- 外部依赖异常但系统未完全失败

### `info`

用于：

- 一次性启动/关闭摘要
- 明确状态迁移
- 重要业务结果
- 运维需要默认看到的关键事件

### `debug`

用于：

- 诊断细节
- 分支路径
- 正常流程中的附加上下文

### `trace`

用于：

- 高频明细
- 每请求内部步骤
- 热路径超细粒度排障信息

## 三、请求日志标准

默认请求路径应遵守：

1. 正常请求只保留一条主 completion 事件
2. started / body chunk / eos 这类细节默认放在 `debug` 或 feature gate
3. request span 上的字段应足够做关联，不依赖动态 prose

主请求日志应可回答：

- 谁发起：`request_id` / `trace_id` / `peer_addr`
- 请求什么：`method` / `uri` / `bucket` / `object`
- 结果如何：`status_code` / `result`
- 花了多久：`duration_ms`

## 四、后台任务与重试日志标准

循环、worker、重试路径应遵守：

1. 不在每次成功循环都打 `info`
2. 不对“expected idle / disabled / no config”长期刷 `warn`
3. 将 repeated retry 日志收敛为：
   - 状态变化时输出
   - 周期摘要输出
   - 达到阈值时升级级别

推荐模式：

- 第一次失败：`warn`
- 达到阈值后：`error`
- 恢复时：一条 `info`

`startup_iam` 当前做法可作为优先参考模型。

## 五、敏感字段脱敏规则

### 严禁直接记录

- `secret_key`
- `session_token`
- `Authorization`
- bearer token
- client secret
- 原始认证 header
- 原始签名串

### 默认掩码后记录

- `access_key`
- 外部 target credential identifier
- 可能映射到账户主体的 key id

### 脱敏建议

- 保留前后缀，中间掩码
- 长度信息可保留
- 原值不可恢复

示例：

- `AKIAIOSFODNN7EXAMPLE` -> `AKIA***MPLE`
- `debug-secret-key` -> 不记录，或只记录 `masked`

## 六、最小 Guard Rails

第一轮 guard rails 只做最小集，不做复杂 lint 框架。

### GR-001 敏感字段禁止回退

新增或修改代码时，不应再出现以下模式进入生产日志：

- `access_key={}`
- `secret_key={}`
- `Authorization={}`
- `token={}`

说明：

- `access_key` 允许保留，但必须走掩码化 helper 或统一展示函数
- 其他字段默认不允许直接打印原值

### GR-002 关键路径禁止新增纯句子型 `warn!/error!`

这些目录新增的 `warn!/error!` 应优先带字段：

- `rustfs/src/server`
- `rustfs/src/main.rs`
- `rustfs/src/startup_iam.rs`
- `rustfs/src/auth.rs`
- `crates/audit/src`
- `crates/notify/src`

允许短 message。
不鼓励只有字符串插值、没有任何结构化字段的新增写法。

### GR-003 高频路径避免默认逐步日志

以下路径默认不新增逐 step `info!`：

- request handling
- body chunk loop
- retry loop
- worker poll loop
- replication / lifecycle inner loop

如果必须保留，优先：

- `debug!`
- feature gate
- 周期摘要

## 七、命名建议

事件名建议采用稳定 snake_case：

- `http_request_completed`
- `iam_bootstrap_retry_failed`
- `audit_target_dispatch_failed`
- `notify_target_replay_started`
- `tls_handshake_failed`

避免：

- 句子型 event 名
- 临时英文口语
- 把动态值嵌入 `event`

## 八、实施顺序建议

规范落地顺序建议如下：

1. 先新增本规范文档
2. 再补 redaction helper / format helper
3. 再补最小 guard rails
4. 最后做大面积调用点收敛

原因：

- 先有规则，后有重构目标
- 先卡住敏感字段回退，再做风格统一
- 先统一底座，再改热点调用点

## 九、验收口径

后续任何日志治理 PR 至少应满足：

1. 查询性提升，而非只改措辞
2. 默认 `info` / `warn` 噪音下降
3. 敏感字段不再裸露
4. request / trace / target 关联性不下降
5. 业务语义与运行时边界不漂移
