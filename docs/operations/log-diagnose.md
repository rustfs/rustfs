# 日志故障诊断(`rustfs diagnose`)

对客户/现场提供的日志文件做离线故障归因:解析 RustFS 的 JSON 日志
(含 `kubectl logs` / docker compose / journald 采集前缀与 stderr panic 块),
匹配内置故障规则库,输出按严重度排序的诊断报告。设计与规则清单见
rustfs/backlog#1281(总纲)。

不启动存储、不联网、跑完即退,任何装有 `rustfs` 二进制的机器都可用。

## 用法

```bash
# 分析一个目录(自动处理 .zst/.gz 轮转归档)
rustfs diagnose /var/log/rustfs/

# 客户打包的多节点日志(zip/tar.gz 自动递归展开,第一层目录名作为节点标签)
rustfs diagnose customer-logs.zip

# 从 stdin 读(容器场景)
kubectl logs rustfs-0 | rustfs diagnose -

# 只看最近 24 小时,输出 Markdown 直接贴工单
rustfs diagnose logs.tar.gz --since 24h --format md > report.md
```

常用参数:`--format text|json|md`(默认 text;JSON 带稳定 `schema_version`)、
`--since/--until`(RFC-3339 或相对时间 `30m`/`24h`/`7d`)、`--min-level`、
`--top N`(未识别模式条数)、`--samples N`(每个 finding 的样本行数)。

## 报告解读

- **发现(按严重度)**:P0 数据风险 → P1 服务不可用 → P2 降级 → P3 客户端侧
  → P4 提示。每条带诊断结论、建议动作、证据字段与样本行。
- **因果折叠**:当症状类发现(如 quorum 刷屏)在时间上跟随其已知根因
  (如盘 faulty)出现时,报告把症状折叠进根因块的"级联症状"行,根因块
  提升到两者中更高的严重度位置——报告首块直接回答"最可能的原因"。
  JSON 输出保留全部 findings(`collapsed_into` / `caused` 字段标注关系)。
- **时间线异常(提示)**:三个确定性启发,均为提示不定罪——混合 UTC 偏移
  (伴签名错误时点名时钟偏移)、节点时间范围完全不重叠、时间线断档
  (断档后紧跟 startup 类发现时升级为"重启证据")。
- **低频提示**:命中数低于规则阈值的匹配(如零星的签名错误),仅供参考。
- **未识别的高频错误**:规则库未覆盖的 WARN/ERROR 消息模板聚类。这一节是
  规则库迭代的输入——反复出现的新模板请提给维护者补规则
  (`crates/log-analyzer/src/rules/seed/`)。
- **跳过的输入与时区提示**:所有被跳过的文件(二进制/超限)逐条披露;
  混合 UTC 偏移会显式提醒(时钟偏移本身就是 `SignatureDoesNotMatch`
  的常见根因)。

## `--redact`(报告需要转发时)

把 bucket/object/AK、IPv4/IPv6、peer 与磁盘路径、节点标签、来源文件路径等客户标识替换为稳定哈希(同值同哈希,保持可关联);规则 id、诊断文本、模块 target 与 panic 源码位置(RustFS 自身代码,非客户数据)保留。样本会连同其完整 `fields` 一起脱敏。

覆盖是尽力而为,不是绝对保证:结构化字段与 `key=value`/IP 形态的文本都会被处理,但散落在自由文本里、既非字段形态也非 IP 的标识(例如句子里顺带提到的一个 bucket 名)可能仍有残留。转发前建议再抽查一遍。

## 自定义规则(`--rules <file.json>`)

支持团队可以在不等发版的情况下补规则,或热修一条误报的内置规则:

```bash
rustfs diagnose customer.zip --rules extra-rules.json
```

文件格式(`Rule` 的 JSON 表示与内置规则完全一致):

```json
{
  "schema_version": 1,
  "rules": [
    {
      "id": "custom-oom-killer",
      "severity": "p1_unavailable",
      "category": "process",
      "title": "内核 OOM killer 终止了进程",
      "matcher": { "message_contains": "Out of memory: Killed process" },
      "diagnosis": "内核因内存不足杀掉了 rustfs 进程。",
      "suggestion": "检查内存限制与其他驻留进程;考虑调大内存或加节点。"
    }
  ]
}
```

- `severity`:`p0_data_risk | p1_unavailable | p2_degraded | p3_client_side | p4_info`;
- `matcher`:`message_prefix` / `message_contains` / `message_regex` /
  `field_equals {name, value}` / `target_prefix` / `is_panic` / `min_level` /
  `all [..]` / `any [..]`,与内置规则同一套类型;
- 可选字段:`evidence_fields`、`min_count`(默认 1)、`implies_root_cause`
  (参与因果折叠)、`anchors`;
- **同 id 覆盖内置规则**(用于热修误报);合并后的规则集整体校验,任何错误
  (坏 regex、重复 id、空 matcher 组)会逐条打印并以退出码 2 失败——不会
  带着半坏的规则集分析;
- 外部规则的 `anchors` 不受 CI 锚点守卫约束,质量由文件作者自担。

## 已知边界

- panic 只出现在 stderr:若客户只采集了 stdout,panic 不会出现在日志里,
  但 `rwlock ... poisoned` 类发现会提示"曾发生 panic"。
- 审计日志(camelCase 外发流)不在本工具范围内。
- 规则锚点由 CI 守卫(rustfs/backlog#1289)保证与源码日志文案同步。
