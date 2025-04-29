## 部署可观测性系统

OpenTelemetry Collector 提供了一个厂商中立的遥测数据处理方案，用于接收、处理和导出遥测数据。它消除了为支持多种开源可观测性数据格式（如
Jaeger、Prometheus 等）而需要运行和维护多个代理/收集器的必要性。

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

### 创建配置文件

1. 进入 `deploy/config` 目录
2. 复制示例配置：`cp obs.toml.example obs.toml`
3. 编辑 `obs.toml` 配置文件，修改以下关键参数：

| 配置项             | 说明                         | 示例值                   |
|-----------------|----------------------------|-----------------------|
| endpoint        | OpenTelemetry Collector 地址 | http://localhost:4317 |
| service_name    | 服务名称                       | rustfs                |
| service_version | 服务版本                       | 1.0.0                 |
| environment     | 运行环境                       | production            |
| meter_interval  | 指标导出间隔 (秒)                 | 30                    |
| sample_ratio    | 采样率                        | 1.0                   |
| use_stdout      | 是否输出到控制台                   | true/false            |
| logger_level    | 日志级别                       | info                  |

```