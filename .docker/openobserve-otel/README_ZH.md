# OpenObserve + OpenTelemetry Collector

[![OpenObserve](https://img.shields.io/badge/OpenObserve-OpenSource-blue.svg)](https://openobserve.org)
[![OpenTelemetry](https://img.shields.io/badge/OpenTelemetry-Collector-green.svg)](https://opentelemetry.io/)

[English](README.md) | 中文

## 中文

本目录包含搭建 OpenObserve 和 OpenTelemetry Collector 可观测性栈的配置文件。

### 概述

此设置为应用程序提供了完整的可观测性解决方案：

- **OpenObserve**：现代化、开源的可观测性平台，用于日志、指标和追踪。
- **OpenTelemetry Collector**：收集和处理遥测数据，然后将其发送到 OpenObserve。

### 设置说明

1. **前提条件**：
    - 已安装 Docker 和 Docker Compose
    - 足够的内存资源（建议至少 2GB）

2. **启动服务**：
   ```bash
   cd .docker/openobserve-otel
   docker compose -f docker-compose.yml  up -d
   ```

3. **访问仪表板**：
    - OpenObserve UI：http://localhost:5080
    - 默认凭据：
        - 用户名：root@rustfs.com
        - 密码：rustfs123

### 配置

#### OpenObserve 配置

OpenObserve 服务配置：

- 根用户凭据
- 通过卷挂载实现数据持久化
- 启用内存缓存
- 健康检查
- 暴露端口：
    - 5080：HTTP API 和 UI
    - 5081：OTLP gRPC

#### OpenTelemetry Collector 配置

收集器配置为：

- 通过 OTLP（HTTP 和 gRPC）接收遥测数据
- 从文件中收集日志
- 批处理数据
- 将数据导出到 OpenObserve
- 管理内存使用

### 与应用程序集成

要从应用程序发送遥测数据，将 OpenTelemetry SDK 配置为发送数据到：

- OTLP gRPC:`localhost:4317`
- OTLP HTTP:`localhost:4318`

例如，在使用 `rustfs-obs` 库的 Rust 应用程序中：

```bash
export RUSTFS_OBS_ENDPOINT=http://localhost:4317
export RUSTFS_OBS_SERVICE_NAME=yourservice
export RUSTFS_OBS_SERVICE_VERSION=1.0.0
export RUSTFS_OBS_ENVIRONMENT=development
```