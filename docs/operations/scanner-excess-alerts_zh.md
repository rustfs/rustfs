# Scanner 超限告警：指标、S3 事件与阈值

> English version: [scanner-excess-alerts.md](scanner-excess-alerts.md)

日期：2026-08-18（rustfs/backlog#1868 / HS-04，含 HS-15 阈值差异说明）

后台 scanner 在扫描过程中检测三类"超限"状态并对外告警。本文说明每类告警的触发条件、可订阅的 S3 事件、冷却语义，以及与 MinIO 的阈值差异，供运维排障与事件消费方对接。

## 三类告警

| 告警 | 触发条件（任一扫描周期） | 指标 | S3 事件（RustFS wire 名） | MinIO 对应事件名 |
|---|---|---|---|---|
| 版本数超限 | 单对象保留版本数 ≥ `scanner:alert_excess_versions` | `rustfs_scanner_excess_object_versions_total{bucket}` | `s3:Scanner:ManyVersions` | `s3:ObjectManyVersions` |
| 版本总大小超限 | 单对象全部版本累计字节 ≥ `scanner:alert_excess_version_size` | `rustfs_scanner_excess_object_version_size_total{bucket}` | `s3:Scanner:LargeVersions` | `s3:ObjectLargeVersions` |
| 子目录数超限 | 单目录直接子目录数 > `scanner:alert_excess_folders` | `rustfs_scanner_excess_folders_total{root}` | `s3:Scanner:BigPrefix` | `s3:PrefixManyFolders` |

订阅方式与普通桶通知一致：对目标桶配置 notification，事件名填上表 RustFS wire 名（或通配 `s3:Scanner:*`）。事件以 `UserAgent: Scanner` 标记来源，`req_params` 携带实际值与阈值（`versions` / `cumulativeSize` / `folders` / `threshold`），便于消费方直接判断严重程度。

## 指标与事件的触发节奏不同

- **指标与结构化日志是电平触发**：只要对象仍在阈值之上，每个扫描周期都会计数/打日志（默认周期约 60s，见 `scanner:speed`）。
- **S3 事件是边沿触发 + 冷却**：同一 (告警类型， 桶, 对象) 在冷却窗口内只发一次，默认 24 小时（`RUSTFS_SCANNER_ALERT_COOLDOWN_SECS`，设 0 表示每周期都发）。窗口过后对象仍超限会再次发出。冷却表在进程内有 4096 条硬顶，超限清空重建（最坏情况是每个仍超限的 key 多发一次）。
- 进程重启会重置冷却（重启后每个仍超限的对象会再发一次）——这是有意为之：重启常伴随排障，重发提供可见性。

## 阈值默认值与 MinIO 差异（HS-15）

| 配置键 | ENV | RustFS 默认 | MinIO 默认 | 差异说明 |
|---|---|---|---|---|
| `scanner:alert_excess_versions` | `RUSTFS_SCANNER_ALERT_EXCESS_VERSIONS` | 100 | 100 | 一致 |
| `scanner:alert_excess_version_size` | `RUSTFS_SCANNER_ALERT_EXCESS_VERSION_SIZE` | 1 TiB | 1 TB | 语义同量级，单位口径不同（TiB vs TB） |
| `scanner:alert_excess_folders` | `RUSTFS_SCANNER_ALERT_EXCESS_FOLDERS` | 65538 | 50000 | **有意差异**：65538 兼容 Proxmox Backup Server 的 chunk 布局（每目录 65536 个 chunk + 目录自身条目），按 MinIO 的 50000 会对 PBS 用户持续误报。如需与 MinIO 行为一致可显式配置为 50000 |

三个键均支持 env 与 admin config（`PUT /rustfs/admin/v3/config` 的 `scanner` 子系统）双通道，热更新即时生效。

## 事件名映射的由来

RustFS 的事件枚举（`rustfs_s3_types::EventName::ScannerManyVersions/LargeVersions/BigPrefix`）沿用仓库既有 wire 名 `s3:Scanner:*`（与 MinIO 的 `s3:ObjectManyVersions` 字面不同，枚举注释中保留了映射关系）。订阅方应以本文的 RustFS wire 名为准；如需 MinIO 字面兼容，请在 console/消费侧做名称映射，不要修改已发布的 wire 名。
