# Spike 0: io_uring 取消安全原型(backlog#894 P2 前置)

## 这是什么

rustfs/backlog#897 路线图中 P2(io_uring 读后端)被 P1.5 基准判 NO-GO 而 defer。本 spike 是 #894 明确要求先行的**取消安全原型**——P2 中风险最高、最容易随时间流失的知识,按"只实现原型、不进主干、不启用"的方案 B 存档。重启条件满足前,P2 主体不动工。

**本 crate 是独立 workspace**(Cargo.toml 内含空 `[workspace]` 表),io-uring 依赖不进入 rustfs 主 Cargo.lock、不参与主工程构建与 CI。这与守卫脚本 `scripts/check_no_tokio_io_uring.sh` 的约束一致:禁的是 tokio 的 io-uring runtime feature,应用层显式 io-uring 集成必须走运行时探测的独立后端(即本原型验证的模型)。

## 要证明的问题

EC quorum 达成 / 断连 / 超时会 drop 在途的 `BitrotReaderTask` future(main 上位于 `crates/ecstore/src/set_disk/core/io_primitives.rs:1455` 的 `FuturesUnordered`)。若该 future 已向内核提交了 read SQE,内核在 CQE 之前始终可能向目标 buffer 写入。**future 的 drop 不能回收 buffer,否则是 use-after-free。**

## 验证的所有权模型

```
caller                    driver thread                     kernel
------                    -------------                     ------
read_at() ──Msg::Read──▶  分配 buf,登记 pending 表
                          (buf + Arc<File> + oneshot tx)
                          push SQE(user_data=id) ─submit──▶ 开始随时可能写 buf
await ◀───oneshot────────                                    │
                                                             │
future drop(任意时刻)                                        │
  └─(可选)Msg::Cancel ──▶ push ASYNC_CANCEL ────────────────▶│ 加速 CQE
  └─绝不触碰 buf                                             │
                          CQE 到达 ◀─────────────────────────┘
                          pending.remove(id)  ← 全程唯一的 buf 回收点
                          send 结果:成功=delivered
                                    失败(接收方已 drop)=orphan_reclaimed
```

关键不变量:

1. **buffer 与 fd 归 pending 表所有,不归 future。** SQE 里的裸指针指向表项 `Vec` 的堆块;`Vec` 结构体可随 HashMap 移动(堆块地址不变),但在 CQE 前绝不 resize/drop。
2. **fd 也必须由表项持有**(`Arc<File>`)。只保 buffer 不保 fd 的话,future drop 会关闭 fd,fd 号被复用后内核的写落到别人的文件上——这是本 spike 落笔前未在 #894 文字中显式化的坑,已验证必须纳入 P2 设计。
3. **future drop 只放弃结果领取**,默认附带提交 `IORING_OP_ASYNC_CANCEL`(best-effort 加速),也可以不提交(裸 drop)——两种情况下回收都只发生在 CQE。
4. **shutdown 顺序**:停收新 SQE → 对所有在途 op 提交 cancel → drain 到 `in_flight == 0` → 线程退出 → ring drop(unmap)。ring 决不能在内核仍持有 buffer 引用时 unmap。
5. **探测必须提交真实 read op**:`io_uring_setup` 成功不代表 op 可用(gVisor/seccomp 可以建 ring 但 op ENOSYS/EINVAL);探测失败按 EACCES/EPERM/ENOSYS/EINVAL/EOPNOTSUPP 分类,命中即优雅降级(测试中表现为 skip),其余 errno 视为真 bug 直接断言失败。

## 测试矩阵

| 测试 | 验证点 |
|---|---|
| `read_matches_std` | 完成路径正确性:64 次变长/变偏移读与文件内容逐字节一致 |
| `dropped_future_buffer_lives_until_cqe` | **核心断言**:阻塞的 pipe 读上裸 drop future(不提交 cancel),300ms 后 op 仍 in-flight、buffer 未回收;向 pipe 写入触发 CQE 后才回收(orphan_reclaimed=1) |
| `async_cancel_accelerates_reclaim` | 默认 drop 路径:ASYNC_CANCEL 使孤儿 op 在无数据到达的情况下经 ECANCELED CQE 及时回收 |
| `cancel_stress_accounts_for_every_buffer` | 压力:256 并发读、一半立即 drop;`delivered + orphan_reclaimed == submitted`,幸存读逐字节正确 |
| `shutdown_drains_in_flight_ops` | 关停:两个阻塞在途 op 被 cancel + drain 到 0 后线程才退出,持有的 future 解析为 ECANCELED |

## 如何运行

需要 Docker(Linux 内核)。macOS 宿主上 `cargo check` 只验证非 Linux 桩编译。

```bash
./run-docker.sh
```

- **leg 1(默认 seccomp)**:多数 Docker 版本默认禁 io_uring(即 #4313 事故环境),探测失败 → 全部测试走优雅降级 skip,套件仍绿。若宿主 Docker 放行 io_uring,则此腿等同 leg 2。
- **leg 2(seccomp=unconfined)**:真实 io_uring,完整跑取消安全套件。

## 运行结果

两腿一次通过,详见"实测记录"。

## 对 P2 主体实现的遗留项(本 spike 不覆盖)

- eventfd + tokio `AsyncFd` 收割替换轮询驱动循环;SQ 深度 `Semaphore` 背压。
- 进程级单例 ring 的生命周期管理(本 spike 每测试一个 ring)。
- O_DIRECT 对齐 buffer(P1 的 statx 探测复用)、三条读形态接入 `LocalIoBackend`。
- per-disk 探测缓存与运行期 errno 降级闩锁(参照 main 上 `DirectIoReadState`,local.rs:278)。
- registered buffers(P3)/写路径(P4)完全不涉及。

## 实测记录

2026-07-07,宿主 macOS + OrbStack Docker(Linux arm64,内核 7.0.11-orbstack),镜像 `rust:1-bookworm`,`cargo test --release`:

- **leg 1(默认 seccomp)**:`io_uring_setup` 失败 `EPERM (Operation not permitted)`——与 #4313 事故环境同类。`ProbeFailure::is_expected_restriction()` 命中,5 个测试全部优雅降级 skip,套件绿。证明探测 + errno 分类降级契约按设计工作。
- **leg 2(seccomp=unconfined)**:5 个测试全部通过(0.45s):
  - `read_matches_std` ok — 64 次读逐字节正确;
  - `dropped_future_buffer_lives_until_cqe` ok — 裸 drop 后 op 保持 in-flight 300ms、buffer 未回收,写 pipe 触发 CQE 后 `orphan_reclaimed=1`;
  - `async_cancel_accelerates_reclaim` ok — ECANCELED CQE 路径回收;
  - `cancel_stress_accounts_for_every_buffer` ok — 256 op、128 drop,`delivered(128) + orphan_reclaimed(128) == submitted(256)`;
  - `shutdown_drains_in_flight_ops` ok — drain 到 0 后退出,持有 future 解析为 ECANCELED。

**结论:GO(模型可行)。** buffer/fd 归驱动 pending 表、CQE 唯一回收点、ASYNC_CANCEL 加速、shutdown drain 的组合在真实内核上成立,且降级契约在受限环境下按设计生效。P2 主体重启时可直接沿用此所有权模型。
