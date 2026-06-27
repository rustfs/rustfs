# RustFS 可观测性技术栈

本目录包含 RustFS 的全面可观测性技术栈，旨在提供对应用程序性能、日志和追踪的深入洞察。

## 组件

该技术栈由以下一流的开源组件组成：

- **Prometheus** (v2.53.1): 行业标准的指标收集和告警工具。
- **Grafana** (v11.1.0): 领先的可观测性可视化平台。
- **Loki** (v3.1.0): 水平可扩展、高可用、多租户的日志聚合系统。
- **Tempo** (v2.5.0): 高吞吐量、最小依赖的分布式追踪后端。
- **Jaeger** (v1.59.0): 分布式追踪系统（配置为辅助 UI/存储）。
- **OpenTelemetry Collector** (v0.104.0): 接收、处理和导出遥测数据的供应商无关实现。

默认情况下，这套技术栈使用 Tempo 单二进制模式，不依赖 Kafka/Redpanda。
如果需要基于 Kafka 的 HA Tempo 路径，请使用 `docker-compose-example-for-rustfs.yml` 配合 `docker-compose-tempo-ha-override.yml`。

## 架构

1.  **遥测收集**: 应用程序将 OTLP (OpenTelemetry Protocol) 数据（指标、日志、追踪）发送到 **OpenTelemetry Collector**。
2.  **处理与导出**: Collector 处理数据（批处理、内存限制）并将其导出到相应的后端：
    -   **追踪** -> **Tempo** (主要) & **Jaeger** (辅助/可选)
    -   **指标** -> **Prometheus** (通过抓取 Collector 的导出器)
    -   **日志** -> **Loki**
3.  **可视化**: **Grafana** 连接到所有后端（Prometheus, Tempo, Loki, Jaeger），提供统一的仪表盘体验。

## 特性

-   **完全持久化**: 所有数据（指标、日志、追踪）都持久化到 Docker 卷，确保重启后无数据丢失。
-   **关联性**: 在 Grafana 中实现指标、日志和追踪之间的无缝导航。
    -   从指标峰值跳转到相关追踪。
    -   从追踪跳转到相关日志。
-   **高性能**: 针对批处理、压缩和内存管理进行了优化配置。
-   **标准化协议**: 完全基于 OpenTelemetry 标准构建。

## GET 性能优化仪表盘

包含三个预构建的 Grafana 仪表盘，用于监控 RustFS GET 性能优化发布：

### 可用仪表盘

| 仪表盘 | 文件 | 描述 |
|--------|------|------|
| **GET 发布健康度** | `grafana-get-rollout-health.json` | 监控优化发布：按 reader path 的延迟、early-stop 命中率、codec streaming 使用率、pipeline 失败率 |
| **GET 数据完整性** | `grafana-get-data-integrity.json` | 监控数据安全：bitrot 校验失败、decode 错误、short read、shard 读取结果 |
| **GET 资源影响** | `grafana-get-resource-impact.json` | 监控资源使用：并发请求数、IO 队列利用率、disk permit 等待、RSS 趋势 |

### Prometheus 告警规则

文件 `prometheus-rules/rustfs-get-optimization-alerts.yaml` 包含预配置的告警规则：

| 告警 | 级别 | 条件 |
|------|------|------|
| `GetP99Regression` | 严重 | GET p99 延迟 > 2x 基线，持续 10 分钟 |
| `PipelineFailureSpike` | 严重 | Pipeline 失败率 > 5x 基线，持续 5 分钟 |
| `BitrotMismatchSpike` | 严重 | Bitrot 不匹配率 > 3x 基线，持续 5 分钟 |
| `EarlyStopInsufficientQuorum` | 警告 | Early-stop quorum 不足率 > 0.1/s，持续 5 分钟 |
| `CodecStreamingFallbackSpike` | 警告 | Codec streaming 回退 > 10x 基线，持续 10 分钟 |
| `IoQueueSaturation` | 警告 | IO 队列利用率 > 90%，持续 5 分钟 |

### 启用告警规则

在 Prometheus 配置中添加告警规则文件：

```yaml
# prometheus.yml
rule_files:
  - "/etc/prometheus/rules/*.yml"

# 或在 docker-compose.yml 中挂载文件：
# volumes:
#   - ./prometheus-rules:/etc/prometheus/rules
```

### 仪表盘使用

仪表盘在 Grafana 启动时自动预置。它们使用 `${DS_PROMETHEUS}` 数据源变量，因此需要在 Grafana 中配置 Prometheus 数据源。

优化发布期间需要关注的关键面板：

1. **GET 延迟按 Reader Path** - 对比 `codec_streaming` vs `legacy_duplex` 延迟
2. **Early-Stop 命中率** - 验证 early-stop 是否有效触发
3. **Pipeline 失败率** - 检测优化引入的新故障模式
4. **Bitrot 校验失败** - 确保数据完整性

## 快速开始

### 前置条件

-   Docker
-   Docker Compose

### 部署

运行以下命令启动整个技术栈：

```bash
docker compose up -d
```

### Tempo 高可用模式

默认的 `docker-compose.yml` 对应单机栈。
如果需要基于 Kafka 的 HA Tempo 配置，请使用：

```bash
docker compose -f docker-compose-example-for-rustfs.yml -f docker-compose-tempo-ha-override.yml up -d
```

### 访问仪表盘

| 服务 | URL | 凭据 | 描述 |
| :--- | :--- | :--- | :--- |
| **Grafana** | [http://localhost:3000](http://localhost:3000) | `admin` / `admin` | 主要可视化中心。 |
| **Prometheus** | [http://localhost:9090](http://localhost:9090) | - | 指标查询和状态。 |
| **Jaeger UI** | [http://localhost:16686](http://localhost:16686) | - | 辅助追踪可视化。 |
| **Tempo** | [http://localhost:3200](http://localhost:3200) | - | Tempo 状态/指标。 |

## 配置

### 数据持久化

数据存储在以下 Docker 卷中：

-   `prometheus-data`: Prometheus 指标
-   `tempo-data`: Tempo 追踪 (WAL 和 Blocks)
-   `loki-data`: Loki 日志 (Chunks 和 Rules)
-   `jaeger-data`: Jaeger 追踪 (Badger DB)

要清除所有数据：

```bash
docker compose down -v
```

### 自定义

-   **Prometheus**: 编辑 `prometheus.yml` 以添加抓取目标或告警规则。
-   **Grafana**: 仪表盘和数据源从 `grafana/` 目录预置。
-   **Collector**: 编辑 `otel-collector-config.yaml` 以修改管道、处理器或导出器。

### 验证 RustFS Trace

当 RustFS 将 `RUSTFS_OBS_ENDPOINT` 指向这套技术栈时，应将该值视为
OTLP/HTTP 的基础 URL，例如：

```bash
export RUSTFS_OBS_ENDPOINT=http://host.docker.internal:4318
```

RustFS 会自动在该基础 URL 后补全：

- `/v1/traces`
- `/v1/metrics`
- `/v1/logs`

需要注意：

- 启动阶段通常会先看到日志和指标，因此“先有日志/指标、后有 trace”是正常现象。
- 可见的 trace 数据通常依赖启动后的真实 HTTP/S3/gRPC 请求流量，因为请求路径上的 span 是按需创建的。
- `RUSTFS_OBS_LOGGER_LEVEL=info` 会保留顶层请求 span，但会过滤掉很多 `debug` 级别的嵌套 span。
  如果 Tempo 或 Jaeger 中的 trace 看起来很稀疏，建议先改成 `RUSTFS_OBS_LOGGER_LEVEL=debug`，再判断是否是 collector 或 Tempo 问题。

最小验证流程：

```bash
# 1. 启动本目录下的可观测性技术栈。
docker compose up -d

# 2. 以 OTLP/HTTP 导出方式启动 RustFS，并提高 span 可见性。
export RUSTFS_OBS_ENDPOINT=http://host.docker.internal:4318
export RUSTFS_OBS_LOGGER_LEVEL=debug

# 3. 产生真实请求流量。
curl -I http://127.0.0.1:9000/health
curl -I http://127.0.0.1:9000/health/ready

# 4. 到 Grafana 或 Jaeger 中检查。
# Grafana: http://localhost:3000
# Jaeger:  http://localhost:16686
```

如果日志和指标已经正常，但 trace 仍然稀疏，最常见的原因通常是
“还没有真实请求流量”或“`info` 级别过滤了嵌套 span”，而不是 OTLP 路由失败。

## 故障排除

-   **服务健康**: 使用 `docker compose ps` 检查服务健康状况。
-   **日志**: 使用 `docker compose logs -f <service_name>` 查看特定服务的日志。
-   **Otel Collector**: 检查 `http://localhost:13133` 获取健康状态，检查 `http://localhost:1888/debug/pprof/` 进行性能分析。
