# feat/xlmeta-compat 分支 MinIO 兼容性差异报告

> 对比基准：`main` vs `feat/xlmeta-compat`  
> 生成时间：2025-03-06

## 一、概述

`feat/xlmeta-compat` 分支为实现与 MinIO 的存储格式兼容，对 xl.meta、bucket metadata、erasure coding、checksum 等进行了多处修改。主要目标包括：

1. **xl.meta 格式**：对齐 MinIO 的 MessagePack 编码格式
2. **跨存储兼容**：更换 Reed-Solomon 实现以兼容 MinIO 数据
3. **Checksum**：HighwayHash256S 与 Blake2b512 与 MinIO 一致
4. **Bucket 元数据**：支持 MinIO 格式读写与迁移
5. **生命周期**：实现 DelMarkerExpiration 及规则校验

---

## 二、提交历史（11 个提交）

| 提交 | 描述 |
|------|------|
| `078035a6` | feat(filemeta): align xl.meta msgpack format |
| `36cd8ece` | fix xlmeta decode |
| `390eb482` | fix download compat |
| `3bb6cbf1` | to HighwayHash256S |
| `3d259480` | feat(ecstore): switch to reed-solomon-erasure for cross-storage compatibility |
| `809b4612` | feat(ecstore): enable simd-accel for reed-solomon-erasure |
| `57e98593` | refactor(ecstore): extract bucket metadata tests to separate file |
| `bbfef77e` | feat: add migration from legacy format and bucket metadata |
| `cdd787e6` | refactor: improve bucket metadata migration and related fixes |
| `5601af8e` | Merge origin/main into feat/xlmeta-compat |
| `6b4c3656` | feat(lifecycle): implement DelMarkerExpiration and rule validation |

---

## 三、变更统计

- **修改文件**：48 个
- **新增行数**：约 2434 行
- **删除行数**：约 1298 行
- **净增**：约 1136 行

---

## 四、核心模块变更

### 4.1 xl.meta (FileMeta) 格式兼容

**涉及文件**：`crates/filemeta/`

#### 4.1.1 MessagePack 编解码重写

- **`version.rs`**：由 `rmp_serde` 自动序列化改为手写 `decode_from` / `encode_to`
  - 使用 MinIO 字段名：`Type`, `V1Obj`, `V2Obj`, `DelObj`, `v`
  - 支持 `V1Obj` 跳过（legacy）
  - 支持 `V2Obj` / `DelObj` 为 nil
  - `MetaObject` 字段：`ID`, `DDir`, `EcAlgo`, `EcM`, `EcN`, `EcBSize`, `EcIndex`, `EcDist`, `CSumAlgo`, `PartNums`, `PartETags`, `PartSizes`, `PartASizes`, `PartIdx`, `Size`, `MTime`, `MetaSys`, `MetaUsr`
  - 空数组/空 map 按 nil 或省略处理，与 MinIO 一致

- **新增 `msgp_decode.rs`**：
  - `PrependByteReader`：在流前预读一个字节
  - `read_nil_or_array_len` / `read_nil_or_map_len`
  - `skip_msgp_value`：跳过未知字段

#### 4.1.2 版本与格式

- **`XL_META_VERSION`**：2 → 3
- **`codec.rs`**：xl header 由 `write_uint8` 改为 `write_uint`，与 MinIO 编码一致

#### 4.1.3 Null 版本处理

- **新增 `data_key_for_version()`**：`None` / `nil` 映射为 `"null"`
- **`add_version`**：统一用 `data_key_for_version` 作为 inline data 的 key
- **`shared_data_dir_count`**：使用 `data_key_for_version(version_id)` 替代 `version_id.to_string()`
- **版本匹配**：`None` 与 `Some(nil)` 视为等价

#### 4.1.4 版本数量限制

- 版本上限由 1000 改为 `DEFAULT_OBJECT_MAX_VERSIONS`（10000）

---

### 4.2 校验算法 (Checksum / Bitrot)

**涉及文件**：`crates/utils/src/hash.rs`

| 变更项 | main | feat/xlmeta-compat |
|--------|------|---------------------|
| **HighwayHash256 密钥** | 固定 `[3,4,2,1]` | MinIO 魔法密钥（π 前 100 位 UTF-8 的 HH-256） |
| **BLAKE2b512** | 使用 blake3 (32 字节) | 使用 blake2 (64 字节) |
| **Bitrot 默认** | HighwayHash256 | HighwayHash256S |

- 新增 `test_bitrot_selftest`、`test_highwayhash_compat` 用于与 MinIO 结果对比

---

### 4.3 Erasure Coding 实现

**涉及文件**：`crates/ecstore/src/erasure_coding/erasure.rs`

| 变更项 | main | feat/xlmeta-compat |
|--------|------|---------------------|
| **库** | `reed-solomon-simd` | `reed-solomon-erasure` (GF(2^8)) |
| **SIMD** | 内置 | `simd-accel` feature |
| **API** | 自定义 encoder/decoder 缓存 | 直接使用 `ReedSolomon::encode` / `reconstruct_data` |

- 目的：与 MinIO 使用相同 GF(2^8) 实现，实现跨存储数据兼容

---

### 4.4 Bucket 元数据

**涉及文件**：`crates/ecstore/src/bucket/`

#### 4.4.1 序列化格式

- **`metadata.rs`**：移除 `rmp_serde`，改为手写 `decode_from` / `encode_to`
- 字段顺序与 MinIO `BucketMetadata` 一致
- 时间使用 MessagePack Ext8 格式（`read_msgp_ext8_time` / `write_msgp_time`）

#### 4.4.2 新增模块

- **`msgp_decode.rs`**：MessagePack 编解码辅助（`skip_msgp_value`, `read_msgp_ext8_time`, `write_msgp_time`）
- **`migration.rs`**：从 `.minio.sys` 迁移 bucket metadata 到 `.rustfs.sys`
- **`metadata_test.rs`**：bucket metadata 单元测试

#### 4.4.3 迁移流程

- 启动时调用 `try_migrate_bucket_metadata`
- 从 `MIGRATING_META_BUCKET` (`.minio.sys`) 读取 `.metadata.bin`，写入 `RUSTFS_META_BUCKET` (`.rustfs.sys`)

---

### 4.5 存储初始化与格式迁移

**涉及文件**：`crates/ecstore/src/store_init.rs`

- **`try_migrate_format`**：在未格式化磁盘上尝试从 MinIO 的 `format.json` 迁移
- 读取 `MIGRATING_META_BUCKET` 下的 `format.json`，解析为 `FormatV3`
- 校验 set 数量与 drive 数量后写入 RustFS 格式

---

### 4.6 生命周期 (Lifecycle)

**涉及文件**：`crates/ecstore/src/bucket/lifecycle/lifecycle.rs`

- **DelMarkerExpiration**：支持删除标记在 N 天后过期
- **规则校验**：
  - DelMarkerExpiration 不能与 tag 过滤同时使用
  - 规则必须至少包含一种动作（Expiration / Transition / NoncurrentVersionExpiration / NoncurrentVersionTransition / DelMarkerExpiration）

---

### 4.7 策略 (Policy)

**涉及文件**：`crates/policy/src/policy/policy.rs`

- **deny_only 行为**：由“仅校验 Deny，无 Deny 则允许”改为“无 Allow 则拒绝”
- 测试名：`test_deny_only_checks_only_deny_statements` → `test_deny_only_security_fix`

---

## 五、依赖变更

| 包 | main | feat/xlmeta-compat |
|----|------|---------------------|
| `reed-solomon-simd` | 3.1.0 | 移除 |
| `reed-solomon-erasure` | - | 6.0 (std, simd-accel) |
| `blake2` | - | 0.11.0-rc.5 |
| `serde_bytes` | - | 0.11 |
| `s3s` | s3s-project/s3s (rev) | weisd/s3s (feature/minio-compatibility) |

---

## 六、其他变更

- **AGENTS.md**：多处 AGENTS.md 删除或调整（与兼容性无关）
- **bitrot 测试**：默认 checksum 由 `HighwayHash256` 改为 `HighwayHash256S`
- **scanner**：`data_usage_define.rs` 中部分定义移除
- **user handler**：`rustfs/src/admin/handlers/user.rs` 有调整

---

## 七、兼容性要点总结

1. **xl.meta**：字段名、类型、nil 处理与 MinIO 一致
2. **Erasure Coding**：使用 reed-solomon-erasure，与 MinIO 数据可互读
3. **Checksum**：HighwayHash256S 密钥与 Blake2b512 实现与 MinIO 一致
4. **Bucket 元数据**：MessagePack 格式与 MinIO 兼容，支持从 `.minio.sys` 迁移
5. **Format**：支持从 MinIO `format.json` 迁移到 RustFS 格式
