# PR-35: default switch readiness evaluation

## 1. 任务目标

评估当前 GET V2 路径是否已经满足“默认启用”条件。

本任务是评估任务，不是必须切默认的任务。

## 2. 硬门槛

以下条件必须全部满足：

1. 多轮 cooled A/B 稳定。
2. 1MiB / 4MiB / 10MiB 均不低于 legacy。
3. 至少两个尺寸提升超过 5%。
4. p95 / p99 不劣化。
5. CPU / RSS 可接受。
6. correctness matrix 全部通过。
7. response headers 完全兼容。
8. range / encrypted / compressed / multipart / remote fallback 已证明。
9. kill switch 已验证。

只要任意一项不满足，就不得默认启用，只能继续保持 opt-in。

## 3. 当前 PR 结论

本 PR 的目标是把 default-switch 评估材料整理成可复现的证据集，并把“不默认启用”的判定流程固化。

当前默认结论应为：

```text
keep opt-in only
```

除非正式 A/B 与配套证据同时满足全部硬门槛，否则不允许修改默认 gate。

### 3.1 当前 formal A/B 结果摘要

在 `1MiB / 4MiB / 10MiB`、`concurrency=16`、`duration=30s`、`rounds=3`、`round-cooldown-secs=20` 的 formal A/B 下，
当前 candidate（`codec-legacy`, rollout=`benchmark`）相对 legacy 的中位数结果为：

1. `1MiB`: throughput `-29.64%`, latency `+42.03%`
2. `4MiB`: throughput `-45.16%`, latency `+83.17%`
3. `10MiB`: throughput `-53.23%`, latency `+106.15%`

因此：

1. “1MiB / 4MiB / 10MiB 均不低于 legacy” 不满足。
2. “至少两个尺寸提升超过 5%” 不满足。
3. 当前阶段不具备任何默认启用的证据基础。

## 4. 证据文件

本轮要求输出并保留：

1. `baseline_compare.csv`
2. `compat_summary.csv`
3. `metrics_summary.csv`
4. `cpu_rss_notes.txt`
5. `fallback_coverage.txt`
6. `tracking_issues.txt`
7. `raw_output_paths.txt`
8. `default_switch_readiness.md`

其中 `default_switch_readiness.md` 是评估入口文件，用于汇总是否达到默认切换条件。

当前 formal A/B 证据目录为：

```text
/private/tmp/pr35-default-switch-evaluation
```

## 5. 实施原则

1. 默认 gate 不得被提前打开。
2. 如果证据不完整，也要明确给出“不默认启用”的结论，而不是模糊处理。
3. 脚本输出应优先服务评估与回溯，不允许吞掉 benchmark 失败。
4. legacy fallback 与 env kill switch 必须保留。

## 6. 通过标准

1. 若硬门槛未全部满足，PR 必须显式记录 no-default decision。
2. 若未来满足硬门槛，默认切换也只能在限定对象类型与尺寸后逐步推进。
3. 评估材料必须足以回溯 benchmark 原始输出路径与 compatibility 结果。
