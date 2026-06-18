# Issue #3520 子 Issue 执行方案（第一批）

## 概览

- 父 issue: `#3520`
- 第一批实现目标：
  - `#3551` `security: fail closed SSE-S3 local fallback without valid master key`
  - `#3553` `security: randomize SSE-S3 DEK wrap nonce and keep legacy read compatibility`
- 当前执行分支：
  - `houseme/issue-3551-3553-sse-p0`

本轮只收口 SSE P0，不把 `metrics`、`snowball`、`local KMS hardening`、`s3s fork` 混入同一补丁。

## Issue 拆分与依赖

- 第一批直接实现：
  - `#3551`
  - `#3553`
- 第一批并行候选，但不与本分支混改：
  - `#3556` `admin metrics authz`
  - 备选 `#3552` `snowball limits`
- 后续独立推进：
  - `#3554` `local KMS hardening`
  - `#3555` `rustfs/s3s fork`
  - `#3557` `outbound URL egress guard`
  - `#3558` `blocking-in-async`

依赖关系：

- `#3553` 可以与 `#3551` 同一分支实现，因为都集中在 `rustfs/src/storage/sse.rs`
- `#3555` 依赖外部 `rustfs/s3s` fork，不进入第一轮本仓库实现
- `#3556` 如果静态分析发现 admin route/action 耦合过大，则切换到 `#3552`

## 并行边界

- 并行的定义是：
  - 独立分支
  - 独立 worktree
  - 独立 PR
- 不允许在同一工作树中交叉实现多个子 issue
- 当前工作树只允许承载 `#3551/#3553`

推荐的双轨组织：

1. 轨道 A：`houseme/issue-3551-3553-sse-p0`
2. 轨道 B：`houseme/issue-3556-admin-metrics`
3. 若 `#3556` 耦合超预期，则轨道 B 改为 `houseme/issue-3552-snowball-limits`

## 第一批实现内容

### 轨道 A：`#3551 + #3553`

修改目标：

- `new_for_local_sse()` 显式失败，不再接受缺失或非法 `RUSTFS_SSE_S3_MASTER_KEY`
- `get_sse_dek_provider()` 透传本地 provider 初始化失败
- `encrypt_dek()` 每次生成新的随机 12-byte nonce
- `decrypt_dek()` 继续兼容历史 `nonce:ciphertext` 格式

明确不做：

- 不改 `local KMS` KDF
- 不改 `snowball auto-extract`
- 不改 `admin metrics`
- 不改 `rustfs/s3s` fork

## 分支与 PR 规则

- PR-1：
  - 分支：`houseme/issue-3551-3553-sse-p0`
  - 范围：仅 `#3551 + #3553`
- PR-2：
  - 分支：`houseme/issue-3556-admin-metrics`
  - 若耦合过大则改为 `houseme/issue-3552-snowball-limits`
  - 范围：一个分支只对应一个 issue

不创建汇总 PR，不将多条安全线打包到一个大补丁中。

## 测试矩阵

轨道 A 必跑：

- `cargo fmt --all`
- `cargo fmt --all --check`
- `cargo test -p rustfs encrypt_dek_uses_random_nonce_prefixes`
- `cargo test -p rustfs decrypt_dek_accepts_legacy_zero_nonce_payload`
- `cargo test -p rustfs sse_encryption_fails_closed_without_local_sse_master_key`
- `cargo test -p rustfs sse_encryption_fails_closed_with_invalid_local_sse_master_key`
- `cargo test -p rustfs sse_encryption_omits_kms_header_for_sse_s3_objects`
- `cargo test -p rustfs test_managed_sse_rio_v2_uses_object_key_metadata_roundtrip --features rio-v2`
- `make pre-commit`

轨道 B 若是 `#3556`：

- 精准的 admin metrics 授权测试
- `make pre-commit`

轨道 B 若是 `#3552`：

- 超条目数归档
- 超累计解包体积归档
- 正常 bounded archive
- `make pre-commit`

## 默认约束

- 当前 `rustfs/src/storage/sse.rs` 的未提交改动全部归入 `#3551/#3553`
- 第一批只做 SSE P0 收口
- 只有在轨道 A 稳定后才启动轨道 B 的编码实现
- 若 `#3556` 静态分析发现存在明显的非局部 admin action 牵连，则不硬上，直接切换到 `#3552`
