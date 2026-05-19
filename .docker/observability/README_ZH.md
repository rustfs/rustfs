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
