# Issue #713 `>1GiB GET` baseline v1 小结

## 1. 执行范围

本轮是 `#713` 的第一轮 plain baseline，先不碰 encrypted / compressed，只看：

1. `1GiB`
2. `2GiB`
3. `sequential`
4. `ranged_parallel`
5. `concurrency=1,4,8`
6. `range_workers=4`
7. `rounds=1`

结果目录：

1. `rustfs/target/bench/issue713-gt1g-get-baseline-v1-20260625T105149Z`

## 2. 结果总表

### 2.1 `1GiB`

`sequential`

1. `c1`: `99.36 MiB/s`, `10807.0ms`
2. `c4`: `518.53 MiB/s`, `8267.5ms`
3. `c8`: `708.04 MiB/s`, `11362.4ms`

`ranged_parallel`

1. `c1`: `481.71 MiB/s`, `2229.0ms`
2. `c4`: `1536.66 MiB/s`, `2728.5ms`
3. `c8`: `1548.29 MiB/s`, `5010.9ms`

### 2.2 `2GiB`

`sequential`

1. `c1`: `116.43 MiB/s`, `18444.0ms`
2. `c4`: `522.06 MiB/s`, `16442.5ms`
3. `c8`: `712.24 MiB/s`, `21842.1ms`

`ranged_parallel`

1. `c1`: `503.28 MiB/s`, `4267.0ms`
2. `c4`: `1817.59 MiB/s`, `4707.3ms`
3. `c8`: `1681.50 MiB/s`, `9502.1ms`

## 3. 当前第一结论

本轮 plain baseline 可以先收敛出三点：

1. `ranged_parallel` 在当前单机多盘场景下，明显优于单流 `sequential`
2. 这种优势在 `1GiB` 和 `2GiB` 两个点上都成立
3. 即使 `sequential` 已经过了当前这轮 `>1GiB GET` buffer / readahead 优化，当前仍然没有追平 `4-way ranged_parallel`

换句话说：

1. 这轮服务端优化是必要的
2. 但对当前这组 plain `>1GiB GET` workload 来说，客户端并行 range 下载仍然是更强的模式

## 4. 当前推荐

在这轮 baseline 之后，最自然的下一步是：

1. 先补一轮更稳妥的确认性复测，只保留重点点位：
   - `1GiB c1`
   - `1GiB c4`
   - `2GiB c1`
   - `2GiB c4`
2. 如果结论仍稳定，再继续补：
   - larger `range_workers` 对比
   - encrypted `>1GiB GET`
   - compressed `>1GiB GET`

## 5. 当前边界说明

这轮还不能直接下最终结论的地方：

1. 只有 `1 round`
2. `ranged_parallel` 当前只测了 `range_workers=4`
3. 还没有拆 plain / encrypted / compressed

因此当前更准确的表述是：

1. plain `>1GiB GET` 的第一轮 baseline 已显示 `ranged_parallel` 明显领先
2. 但还需要更小面复测来确认这个结论的稳定性
