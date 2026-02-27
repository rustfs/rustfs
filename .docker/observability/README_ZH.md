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

## 故障排除

-   **服务健康**: 使用 `docker compose ps` 检查服务健康状况。
-   **日志**: 使用 `docker compose logs -f <service_name>` 查看特定服务的日志。
-   **Otel Collector**: 检查 `http://localhost:13133` 获取健康状态，检查 `http://localhost:1888/debug/pprof/` 进行性能分析。
