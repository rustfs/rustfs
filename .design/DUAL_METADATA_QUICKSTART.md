# RustFS 双元数据中心 - 架构总结与快速开始指南

**版本**: 1.0  
**日期**: 2026-02-23  
**状态**: Active Development (v0.1 阶段)

---

## 快速导航

本项目包含 **4 份核心设计文档**：

| 文档                                                                             | 用途        | 目标读者      |
|--------------------------------------------------------------------------------|-----------|-----------|
| [DUAL_METADATA_CENTER_DESIGN.md](DUAL_METADATA_CENTER_DESIGN.md)               | 架构设计与数据结构 | 架构师、技术负责人 |
| [DUAL_METADATA_IMPLEMENTATION_GUIDE.md](DUAL_METADATA_IMPLEMENTATION_GUIDE.md) | 逐步实现指南    | 工程师、开发者   |
| [DUAL_METADATA_MIGRATION_GUIDE.md](DUAL_METADATA_MIGRATION_GUIDE.md)           | 迁移与测试策略   | 运维、测试工程师  |
| **本文**                                                                         | 快速入门与总结   | 所有人       |

---

## 核心创新点

### 1. 架构创新：三层元数据系统

```
Application Layer (S3 API)
        │
        ▼
┌───────────────────────────┐
│   MetadataEngine Trait    │  (统一接口)
├───────────────────────────┤
│  LocalMetadataEngine      │  (新的 KV 引擎)
│  DualMetadataCenter       │  (高可用)
│  DualTrackEngine          │  (迁移辅助)
│  LegacyAdapter            │  (兼容)
└──────┬──────────┬──────────┘
       │          │
       ▼          ▼
┌─────────────────────────────────────┐
│    Surreal 组件层                    │
├──────────────┬──────────┬────────────┤
│ SurrealKV    │Ferntree  │SurrealMX   │
│ (KV Store)   │(B+ Tree) │(Storage)   │
└──────────────┴──────────┴────────────┘
```

### 2. 性能创新：3-5 倍 IOPS 提升

**小文件优化**: 128KB 以下内联存储

- **当前**: 1 个小文件 = 2 次 IO (meta + data)
- **优化后**: 1 个小文件 = 1 次 IO (meta+data 一起存)

**ListObjects 加速**: Ferntree B+ 树

- **当前**: 遍历文件系统目录，O(n) 复杂度
- **优化后**: 范围扫描 B+ 树，O(log n) 查询 + O(k) 顺序读

### 3. 功能创新：全局去重与秒传

**Content-Addressable Storage (CAS)**:

```
Upload File A (Hash = ABC123) → 存储
Upload File B (相同内容)       → 检查 Hash → 秒传 (无数据复制)
Copy File A → File C           → 仅增加引用计数 → 零拷贝
Delete File A                  → 引用计数 -1
Delete File B, C               → 引用计数为 0 时才物理删除
```

### 4. 架构创新：平滑迁移策略

不需要停机，3 个阶段完成从 FS 到 KV 的无缝过渡：

```
Phase 1: 双轨制 (Dual Track)
  FS (Primary) + KV (Shadow) 并行写入
  读优先 FS，异步迁移触发

         ↓ (1-2 周)

Phase 2: 读优先切换 (Read Primary Switch)
  读优先尝试 KV，失败降级 FS
  后台迁移工具扫描 FS，批量迁移到 KV

         ↓ (2-4 周)

Phase 3: 完全迁移 (Migration Complete)
  KV 中对象数 > 99%
  禁用 FS 读取，仅用 KV
  删除 FS 中的旧 xl.meta 文件
```

---

## 快速开始

### Step 1: 理解核心概念 (5 分钟)

关键术语：

- **ObjectMetadata**: 存储在 KV 中的完整对象元数据 (包括内容哈希、大小、用户元数据等)
- **IndexMetadata**: 存储在 Ferntree 中的轻量级列表元数据 (用于快速 ListObjects)
- **DataLayout**: 数据存储方式枚举 (Inline / LocalPath / Chunked / BlockAggregated)
- **RefControl**: 引用计数 (支持全局去重和秒传)

### Step 2: 本地环境搭建 (10 分钟)

```bash
# 1. 克隆代码
git clone https://github.com/rustfs/rustfs.git
cd rustfs

# 2. 检查依赖
cargo tree | grep -E "surrealkv|ferntree|surrealmx"

# 3. 编译
cargo build --release

# 4. 运行单元测试
cargo test --workspace --exclude e2e_test
```

### Step 3: 查看现有实现 (20 分钟)

关键文件位置：

```
rustfs/src/storage/metadata/
├── engine.rs          ← LocalMetadataEngine 核心实现
├── types.rs           ← 数据结构定义
├── mx.rs              ← StorageManager (SurrealMX 包装)
├── kv.rs              ← KV 存储初始化
├── ferntree.rs        ← 索引树初始化
├── writer.rs          ← 分块写入
├── reader.rs          ← 分块读取
├── gc.rs              ← 垃圾回收
└── mod.rs             ← 模块公开接口
```

### Step 4: 运行演示 (15 分钟)

```bash
# 启用新元数据引擎
export RUSTFS_NEW_METADATA_ENGINE=true

# 启动服务
cargo run --release --bin rustfs -- --server

# 在另一个终端测试
# 1. 创建 bucket
aws s3 mb s3://test-bucket

# 2. 上传小文件 (会被内联)
echo "Hello" > /tmp/small.txt
aws s3 cp /tmp/small.txt s3://test-bucket/

# 3. 上传大文件 (会分块)
dd if=/dev/zero of=/tmp/large.bin bs=1M count=100
aws s3 cp /tmp/large.bin s3://test-bucket/

# 4. 列出对象 (使用 B+ 树)
aws s3 ls s3://test-bucket/

# 5. 读取对象
aws s3 cp s3://test-bucket/small.txt /tmp/

# 6. 监控指标
curl http://localhost:9000/metrics | grep rustfs_metadata
```

---

## 开发指南

### 当前状态 (v0.1)

✅ **已完成**:

- [x] LocalMetadataEngine 核心设计
- [x] ObjectMetadata / IndexMetadata 数据结构
- [x] StorageManager 抽象接口 (MxStorageManager)
- [x] 事务管理 (KV 事务)
- [x] 基础 CRUD 操作 (put, get, list, delete)
- [x] 垃圾回收 (基于引用计数)
- [x] 单元测试框架

📋 **计划中** (v0.2-v0.3):

- [ ] DualTrackEngine (双轨制)
- [ ] ConsistencyChecker (后台一致性检查)
- [ ] Migrator (懒迁移)
- [ ] MetadataCache (内存缓存)
- [ ] 集成测试

### 代码规范

遵循 Copilot Instructions 中的要求：

```bash
# 提交前必须通过
make pre-commit

# 包括:
# 1. 格式检查 (rustfmt)
cargo fmt --all --check

# 2. Lint 检查 (clippy)
cargo clippy --all-targets --all-features -- -D warnings

# 3. 单元测试
cargo test --workspace --exclude e2e_test

# 4. 编译检查
cargo check --all-targets
```

### 贡献流程

```bash
# 1. 创建功能分支
git checkout -b feat/dual-track-engine

# 2. 实现功能 (遵循代码规范)
# 3. 添加单元测试 (覆盖率 > 80%)
# 4. 提交前验证
make pre-commit

# 5. Push 并创建 PR
git push origin feat/dual-track-engine

# PR 模板会自动生成，填入:
# - What: 实现内容
# - Why: 为什么需要
# - How: 如何测试
# - References: 相关 Issue/Design Doc
```

---

## 性能指标对标

### 预期改进 (基于设计目标)

| 指标                   | MinIO       | RustFS v0.1 | RustFS v1.0 | 改进    |
|----------------------|-------------|-------------|-------------|-------|
| 小文件写 IOPS            | 1K          | 3K          | 5K          | 3-5x  |
| 小文件读 IOPS            | 2K          | 6K          | 8K          | 3-4x  |
| ListObjects 延迟 (10K) | 500-1000ms  | 300-500ms   | 50-100ms    | 5-10x |
| Inode 容量             | ~1M (FS 限制) | 无限制         | 无限制         | ∞     |
| 数据去重率                | <5%         | <5%         | >50%        | 10x+  |
| 秒传延迟                 | N/A         | N/A         | <10ms       | -     |

### 如何验证

```bash
# 1. 小文件 IOPS 测试
fio --name=small_write \
    --rw=write \
    --bs=4k \
    --size=10GB \
    --numjobs=8 \
    --iodepth=32 \
    --output=bench_small_write.txt

# 2. ListObjects 性能
# 上传 10000 个对象
for i in {0..9999}; do
  echo "data" | aws s3 cp - s3://test-bucket/obj-$(printf "%06d" $i)
done

# 列出并计时
time aws s3 ls s3://test-bucket/ --recursive | wc -l

# 3. 去重率检测
# 上传相同内容的多个文件
sha256sum /tmp/data.bin
# ... 上传多个副本到 S3
# ... 检查存储空间占用，应该只占用一份数据空间
```

---

## 故障排查

### 问题 1: KV 写入失败

```
错误: "Failed to commit KV transaction"

排查:
1. 检查 KV 存储路径权限: ls -la metadata/kv/
2. 检查磁盘空间: df -h
3. 检查 SurrealKV 进程是否正常
4. 查看日志: tail -f logs/rustfs.log | grep "kv"
```

### 问题 2: ListObjects 性能低

```
症状: ListObjects 耗时 > 500ms

排查:
1. 检查 Ferntree 索引是否构建完整
   SELECT COUNT(*) FROM ferntree;
2. 检查是否有大量新增对象未更新索引
3. 考虑启用缓存预热
4. 检查 CPU/内存是否瓶颈
```

### 问题 3: 一致性错误

```
症状: "Object exists in KV but not in FS" or vice versa

处理:
1. 查看一致性检查日志
   kubectl logs -l app=rustfs -c consistency-checker
2. 运行手动修复
   cargo run --bin consistency-repair -- --bucket <bucket>
3. 若问题严重，回退到 KV-only 模式
   export RUSTFS_FALLBACK_TO_FS=false
```

---

## 项目里程碑

```
2026 Q1:
  v0.1 ✅ (Feb)  - 引擎集成，基础 CRUD
  v0.2 🔄 (Mar)  - 双轨制，一致性检查
  v0.3 📅 (Apr)  - 读时修复，懒迁移

2026 Q2:
  v0.4 📅 (May)  - 内联数据，压缩
  v0.5 📅 (Jun)  - 去文件化，清理工具
  v0.6 📅 (Jul)  - 高级索引，多维查询

2026 Q3:
  v0.7 📅 (Aug)  - 全局去重，秒传
  v0.8 📅 (Sep)  - 块聚合，存储优化
  v1.0 📅 (Oct)  - RDMA/NVMe-oF, GA Release
```

---

## 相关资源

- **设计文档**: [DUAL_METADATA_CENTER_DESIGN.md](DUAL_METADATA_CENTER_DESIGN.md)
- **实现指南**: [DUAL_METADATA_IMPLEMENTATION_GUIDE.md](DUAL_METADATA_IMPLEMENTATION_GUIDE.md)
- **迁移指南**: [DUAL_METADATA_MIGRATION_GUIDE.md](DUAL_METADATA_MIGRATION_GUIDE.md)
- **Copilot 规则**: [.github/copilot-instructions.md](../.github/copilot-instructions.md)
- **Issue Tracker**: https://github.com/rustfs/rustfs/issues
- **Design RFC**: 见 GitHub Discussions

---

## 联系与反馈

- **技术讨论**: GitHub Issues / Discussions
- **设计评审**: 见 RFC / Design Doc 评论
- **Bug 报告**: Issues with label `bug`
- **功能建议**: Issues with label `enhancement`

---

## 许可证

Apache License 2.0 - 见 [LICENSE](../LICENSE)

---

**Last Updated**: 2026-02-23  
**Maintainers**: RustFS Architecture Team  
**Status**: Active Development


