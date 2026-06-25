# Issue #712 更深一层 zero-copy 下一阶段说明

## 1. 当前结论

当前分支已经具备：

1. `rename_data` 的 `msgpack named-map + JSON fallback`
2. ordinary PUT 的 `zero_copy_eager` 实验路径

其中 `zero_copy_eager` 已经是实际命中的业务路径，但它当前仍然不是端到端严格意义上的 zero-copy write path。

## 2. 当前 copy 还存在的层

当前 plain PUT 即使命中了 `zero_copy_eager`，仍然会在后续链路里发生复制，主要位置在：

1. `HashReader::from_stream(...)`
2. `Erasure::encode(...)` 的 block ingest

从当前代码 review 看，优先级更高的是：

1. `Erasure::encode`

而不是：

1. `HashReader`

## 3. 为什么先看 `Erasure::encode`

原因：

1. `HashReader` 主要负责包装 `AsyncRead` 与 checksum 语义，不是最直接的热点
2. `Erasure::encode` 目前仍然是每个 block 读入 `Vec<u8>` 后再进入编码
3. 这更像是 plain PUT 的下一层实际 copy 热点

## 4. 这轮已经尝试过但不保留的方向

本轮已经试过一个很小的局部改动：

1. 对 full block 优先走 `encode_data_owned(...)`

结果：

1. 对某些 ordinary PUT 面有正向迹象
2. 但不同 size 下收益不稳定
3. 因此当前不建议直接基于这个 patch 继续往前推

## 5. 下一阶段更稳妥的技术方向

如果要继续做更深一层 zero-copy，建议优先考虑：

1. 为 `Erasure::encode` 设计更稳定的 block buffer 生命周期
2. 让 full block ingest 更接近 `Bytes` / owned buffer 复用
3. 避免在当前函数内继续做更多“局部替换一个调用”的小补丁

换句话说，下一阶段更适合做：

1. block buffer 生命周期设计
2. owned block ring / reusable block pool
3. encode/write 之间更明确的 buffer ownership

而不是先做：

1. `HashReader` 级别的大改
2. 更多无设计托底的局部 `Vec<u8>` 调整

## 6. 当前建议

当前最稳妥的推进顺序：

1. 先把当前 `zero_copy_eager` 路径继续作为实验性 ordinary PUT 路径保留
2. 如果要继续做 deeper zero-copy，单独开一个新阶段
3. 该阶段优先聚焦 `Erasure::encode` ingest / buffer lifecycle，而不是先改 `HashReader`
