# Issue #3552 执行方案

## 概览

- issue: `#3552`
- 目标：为 `snowball auto-extract` 接入统一的归档限制与累计解包配额检查
- 当前执行分支：
  - `houseme/issue-3552-snowball-limits`

本轮只处理 `snowball auto-extract`，不把 `#3557` 的跨模块 egress guard 混进来。

## 实现边界

- 仅修改：
  - `rustfs/src/app/object_usecase.rs`
  - 必要时补充相关测试
- 复用已有：
  - `crates/zip/src/lib.rs` 中的 `ArchiveLimits`
- 不做：
  - 不修改外连 URL 校验
  - 不修改 admin authz
  - 不扩展到其他非 archive 上传路径

## 关键实现点

1. 在 `execute_put_object_extract()` 中引入统一 `ArchiveLimits`
2. 在 archive entry 迭代期间累计：
   - entry count
   - total unpacked size
3. 在进入对象写入前执行：
   - entry 数量上限检查
   - 单 entry 大小上限检查
   - 累计解包体积上限检查
   - 归一化后路径长度上限检查
4. 将 bucket quota 从“只检查原始压缩上传体积”扩展为：
   - 保留入口总上传体积检查
   - 基于初始 usage + 累计解包体积执行额外 quota 检查
5. `ignore-errors` 语义保持收敛：
   - 单 entry 读取失败或非法路径仍可按现有逻辑跳过
   - 超限类错误直接失败整个请求，不允许继续解包

## 测试矩阵

- 单元/集成：
  - 超 entry count 失败
  - 超单 entry 大小失败
  - 超累计解包体积失败
  - 超路径长度失败
  - quota exceeded 错误形态保持一致
- 基线：
  - `cargo fmt --all`
  - `cargo fmt --all --check`
  - 精准 `cargo test`
  - 完成前 `make pre-commit`

## 默认约束

- `ArchiveLimits::default()` 是第一版默认值来源，不新增新的用户配置项
- 第一版不额外引入新的 runtime config 或 admin API 开关
- 优先补失败路径测试，复用现有 extract 成功路径覆盖
