# Heal 并发安全说明（对象级 healing 标记对标审计结论）

对应 backlog rustfs/backlog#1874（父 #1862，HS-12）。本文回答一个问题：MinIO 在 heal
期间对对象打 `x-minio-healing:true` 元数据标记以防"heal 提交与并发删除/版本清理互毁"
（cmd/xl-storage.go RenameData 的 healing 分支），RustFS 是否需要同款防御。

**结论：不需要。** RustFS 不存在 MinIO 用 healing 标记防御的那类竞争：所有会触达同一
`(bucket, object)` 提交面的路径都在同一把对象级 namespace 写锁上互斥，且 heal 的锁
guard 覆盖 rename 提交全程；MinIO 需要标记的根因（RenameData 提交内部与版本清理逻辑
交错）在 RustFS 的提交模型中不存在。RustFS 已有一个瞬态 healing 旗标用于另一目的
（见下文 §2），并有并发不变量回归测试锁定本结论（§5）。

## 1. 两个防御模型的对照

MinIO：heal 时对对象写 `x-minio-healing:true`（持久元数据标记），后续任何 RenameData
提交看到该标记就跳过版本清理/legacy purge 逻辑——防御发生在锁外，靠元数据让路。

RustFS：三层防御，全部不依赖持久对象标记：

1. **锁内互斥**：heal 与一切前台/后台写路径的提交点在同一把 `(bucket, object)` ns 写锁
   上串行（分布式部署为 quorum 锁 RPC，单机为进程内锁管理器；锁粒度是对象级，version
   恒为 None）。
2. **提交模型隔离**：rename_data 提交内没有会与 heal 交错的版本清理逻辑；被替换旧版本
   的 data_dir 物理删除被移出提交临界区（commit tail），且只删已被新提交替换的 unshared
   目录。
3. **瞬态 healing 旗标**：`FileInfo::set_healing`（crates/filemeta/src/fileinfo.rs）在
   heal 提交的内存 FileInfo 上打 `"healing"` 内部键，rename_data 据此允许先清空 stale
   目标 data_dir 再 rename——解决 heal 复用 data_dir 做 in-place 修复时 rename(2) 无法
   替换非空目录的文件系统语义冲突（EEXIST/ENOTEMPTY）。该键是瞬态的，不落盘
   （`is_skip_meta_key`），与 MinIO 的持久标记目的不同。非 heal 提交撞上非空目标
   data_dir 会显式失败，有测试锁定两个方向的行为。

## 2. 交点矩阵

中心路径：`heal_object_with_explicit_version_regen`（crates/ecstore/src/set_disk/ops/heal.rs，
下称 heal.rs）在入口取 `(bucket, object)` ns 写锁，guard 绑定到函数作用域末尾，覆盖
quorum 元数据读取 → EC 重建 → 逐盘 rename 提交 → tmp 清理 → HEAL_RENAME_INCOMPLETE
部分提交返回 → 孤儿 data_dir 回收的全过程。并发侧逐交点判定：

| # | 并发路径 | 并发侧锁 | 判定 | 关键证据 |
|---|---|---|---|---|
| 1 | PUT 对象提交 | `put_object_commit` 对象写锁，rename_data 在锁内 | 同锁串行 | ops/object.rs 提交锁段 + rename 调用点 |
| 2 | PUT 旧 data_dir tail 清理 | drop 对象锁后的 `commit_rename_data_dir`，无锁 | 无锁并发，语义安全（见 §3.1） | object.rs drop 后 tail 段；io_primitives.rs |
| 3 | DELETE 单对象/版本 | `delete_object` 对象写锁，delete_version 在锁内 | 同锁串行 | object.rs delete_object 锁段 |
| 4 | DELETE 批量 | 批量逐对象写锁（dist 走批量锁 RPC） | 同锁串行 | object.rs delete_objects 锁段 |
| 5 | CompleteMultipart | 对象写锁 + upload 路径锁双锁，rename 在锁内 | 同锁串行 | ops/multipart.rs 提交锁段 |
| 6 | CompleteMultipart tail 清理 | drop 对象锁后的旧 data_dir 删除 | 无锁并发，语义安全（见 §3.1） | multipart.rs drop 后 tail 段 |
| 7 | AbortMultipart | 仅 multipart bucket 的 upload 路径锁 | 锁 key 不相交，但资源不相交（abort 不触对象 data_dir/xl.meta）→ 无实际交点 | multipart.rs abort 锁段 |
| 8 | ILM expiry（含 DeleteAllVersions） | DeleteAllVersions 走 `delete_prefix_object=true` → 仍取对象锁；FreeVersionTask 显式取锁；noncurrent 批量走批量锁 | 同锁串行 | bucket_lifecycle_ops.rs 消费端链路 |
| 9 | 纯 prefix 删除（绕锁能力面） | `delete_prefix`-only 不取子对象锁 | 无锁并发，但生产调用方为零（见 §3.2） | object.rs delete_object 锁条件 |
| 10 | 孤儿 data_dir 回收 reclaim_orphan_data_dirs | 函数本体无锁；唯一生产调用方在 heal 锁内 | heal 流程内=锁内串行 | heal.rs 收尾调用；io_primitives.rs |
| 11 | 旧清理 receipt 对账 reconcile_old_data_cleanup_receipts | 函数本体无锁；调用点在 heal 锁内 + epoch fence 防误删 | 锁内串行 | object.rs 对账函数 |
| 12 | replication | 数据面为远端 HTTP 写（不落本地盘）；本地元数据回写走对象锁 | 同锁串行 / 无交点 | replication_resyncer.rs 链路 |
| 13 | data_movement / rebalance / decommission 源清理 | 显式取对象锁 + 版本未变复核 + guard 复用（no_lock 只是复用已持锁） | 同锁串行 | data_movement/mod.rs 源清理 |
| 14 | copy_object | 目标对象锁 / 走 put 链锁 | 同锁串行 | object.rs copy_object 锁段 |
| 15 | 另一 heal 任务（跨 HealType/force_start） | dedup key 跨类型不相交 + force_start 跳过去重 → 任务级可并发 | 最终在 ns 写锁上串行 | heal/manager.rs dedup key 构成 |
| 16 | admin `no_lock=true` heal | 客户端可控绕锁 | 无锁并发，明示运维选项（见 §3.3） | admin/handlers/heal.rs 透传 |
| 17 | stale multipart 清理 | multipart bucket 的 upload 路径锁 | 资源不相交 → 无交点 | bucket_lifecycle_ops.rs 清理链路 |

## 3. 残留窗口定性

### 3.1 PUT/CompleteMultipart commit tail（交点 2/6）

写路径提交成功、释放对象锁之后，才 best-effort 删除被替换的旧 data_dir（注释明示有意
不阻塞下一操作）。该删除与并发 heal 对同一旧 data_dir 的读取/重建存在竞态窗口，但语义
安全：

- 删除目标是已被新提交替换的 unshared data_dir；heal 的 canonical 元数据来自 quorum
  仲裁（ETag/mod_time），此时 quorum 已指向新版本，heal 不会把已替换版本当作 canonical
  复活；
- 竞态最坏后果 = heal 当轮对旧版本的一次 transient 失败/空转，重试轮自然收敛；清理
  residue 会上报并重新入队 heal（`report_old_data_dir_cleanup`）；
- 换盘重建等长 heal 走 per-version 显式版本请求，quorum 元数据在锁内读取，不受 tail
  影响。

### 3.2 纯 prefix 删除（交点 9）

`delete_prefix && !delete_prefix_object` 的路径不取子对象锁（对象名空间锁无法保护前缀
递归删除），与并发 heal 存在理论复活窗口（heal 在 prefix 删除进行中依据旧 quorum 元
数据重建某版本）。全仓库核对结论：该路径的**生产调用方为零**——所有生产 `delete_prefix:
true` 调用点均同时设置 `delete_prefix_object: true`（从而取对象锁）或在测试模块内。这
是 API 能力面的暴露而非行为风险。若未来有调用方需要纯 prefix 删除，须在调用点证明与
heal/scanner 的隔离（例如 bucket 级停扫围栏）。

### 3.3 admin `no_lock=true`（交点 16）

admin heal 请求可透传客户端 `nolock` 参数绕过 ns 锁（与 MinIO madmin 的同名选项对齐）。
这是运维明示选项：使用即自负与并发写的竞争责任。文档化即可，不建议收紧。

## 4. heal 侧自身的不变量保障

- dedup key 跨 HealType 不相交（object/metadata/mrf/ecdecode/prefix 各自键面）+ admin
  `force_start` 可跳过去重 → 同对象可能同时存在多个 heal 任务，但它们的执行体全部在
  `heal_object` 入口的 ns 写锁上串行（生产入口均 `no_lock=false`）；
- read-repair 的本地 TTL 预留只去重自身来源，不拦截其他来源的 heal——同样由 ns 锁兜底；
- healing 旗标不落盘，故不存在"标记残留导致后续提交错误让路"的反向风险。

## 5. 回归测试

以下两个并发不变量测试随本审计加入 `crates/ecstore/src/set_disk/ops/heal.rs` 测试模块：

- `heal_racing_version_delete_never_resurrects_the_deleted_version`：注入 doomed 版本
  shard 损坏后，版本化 DELETE 与 Deep heal 真并发（同一把锁争用），断言已删除版本不被
  复活、存活版本完好；
- `heal_racing_unversioned_overwrites_preserves_the_last_commit`：非版本化覆盖提交（激活
  commit tail 旧 data_dir 删除）与 Deep heal 循环竞态，断言最终 current 恰为最后一次
  提交（etag 级一致）。

## 6. 结论

MinIO 的 `x-minio-healing` 是锁外元数据防御，前提是其 RenameData 提交内部存在与 heal
交错的版本清理逻辑；RustFS 的提交模型把这类交错从根上消除（提交面锁内互斥 + 清理外
移到 tail + tail 只删 unshared 旧目录），因此引入持久对象级 healing 标记没有对应的竞争
可防，反而会引入 FileInfo 落盘格式变更与标记残留清理两类新成本。维持现状，本对标疑点
关闭。
