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
  → P4 提示。每条带诊断结论、建议动作、证据字段与样本行。quorum 类发现
  通常是级联症状,优先处理同时段的 disk/peer 类 P2 发现。
- **低频提示**:命中数低于规则阈值的匹配(如零星的签名错误),仅供参考。
- **未识别的高频错误**:规则库未覆盖的 WARN/ERROR 消息模板聚类。这一节是
  规则库迭代的输入——反复出现的新模板请提给维护者补规则
  (`crates/log-analyzer/src/rules/seed/`)。
- **跳过的输入与时区提示**:所有被跳过的文件(二进制/超限)逐条披露;
  混合 UTC 偏移会显式提醒(时钟偏移本身就是 `SignatureDoesNotMatch`
  的常见根因)。

## `--redact`(报告需要转发时)

把 bucket/object/AK/IP 等客户标识替换为稳定哈希(同值同哈希,保持可关联),
规则 id、诊断文本与 panic 源码位置保留。适合把报告转发到工单系统或群聊。

## 已知边界

- panic 只出现在 stderr:若客户只采集了 stdout,panic 不会出现在日志里,
  但 `rwlock ... poisoned` 类发现会提示"曾发生 panic"。
- 审计日志(camelCase 外发流)不在本工具范围内。
- 规则锚点由 CI 守卫(rustfs/backlog#1289)保证与源码日志文案同步。
