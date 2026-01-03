## 部署可观测性系统

OpenTelemetry Collector 提供了一个厂商中立的遥测数据处理方案，用于接收、处理和导出遥测数据。它消除了为支持多种开源可观测性数据格式（如
Jaeger、Prometheus 等）而需要运行和维护多个代理/收集器的必要性。

## 指标收集方式

RustFS 支持两种方式暴露 Prometheus 指标：

1. **通过 OpenTelemetry Collector**（默认）- 指标通过 OTLP 流向收集器，由收集器暴露 Prometheus 端点
2. **原生 Prometheus 端点** - 直接从 RustFS 抓取，需要 JWT 认证

### 原生 Prometheus 端点

RustFS 暴露原生 Prometheus 抓取端点，可以在不使用 OpenTelemetry Collector 的情况下直接抓取：

| 端点 | 描述 |
|------|------|
| `/rustfs/v2/metrics/cluster` | 集群级别指标 |
| `/rustfs/v2/metrics/bucket` | 按存储桶指标 |
| `/rustfs/v2/metrics/node` | 按节点/磁盘指标 |
| `/rustfs/v2/metrics/resource` | 系统资源指标 |

这些端点需要 JWT Bearer token 认证。使用以下命令生成配置：

```bash
curl -X GET "http://localhost:9000/rustfs/admin/v3/prometheus/config" \
  -H "Authorization: <S3-signature>"
```

Prometheus 配置示例：

```yaml
scrape_configs:
  - job_name: rustfs-cluster
    bearer_token: <generated-jwt-token>
    metrics_path: /rustfs/v2/metrics/cluster
    static_configs:
      - targets: ['rustfs:9000']
```

### 快速部署

1. 进入 `.docker/observability` 目录
2. 执行以下命令启动服务：

```bash
docker compose -f docker-compose.yml  up -d
```

### 访问监控面板

服务启动后，可通过以下地址访问各个监控面板：

- Grafana: `http://localhost:3000` (默认账号/密码：`admin`/`admin`)
- Jaeger: `http://localhost:16686`
- Prometheus: `http://localhost:9090`

## 配置可观测性

```shell
export RUSTFS_OBS_ENDPOINT="http://localhost:4317" # OpenTelemetry Collector 地址
```
