# OpenObserve + OpenTelemetry Collector

[![OpenObserve](https://img.shields.io/badge/OpenObserve-OpenSource-blue.svg)](https://openobserve.org)
[![OpenTelemetry](https://img.shields.io/badge/OpenTelemetry-Collector-green.svg)](https://opentelemetry.io/)

[English](README.md) | 中文

本目录包含使用 OpenObserve 的**替代**可观测性技术栈配置。

## ⚠️ 注意

对于**推荐**的可观测性技术栈（Prometheus, Grafana, Tempo, Loki），请参阅 `../observability/`。

## 🌟 概览

OpenObserve 是一个轻量级、一体化的可观测性平台，在一个二进制文件中处理日志、指标和追踪。此设置非常适合：
- 资源受限的环境。
- 快速设置和测试。
- 喜欢统一 UI 的用户。

## 🚀 快速开始

### 1. 启动服务

```bash
cd .docker/openobserve-otel
docker compose up -d
```

### 2. 访问仪表盘

- **URL**: [http://localhost:5080](http://localhost:5080)
- **用户名**: `root@rustfs.com`
- **密码**: `rustfs123`

## 🛠️ 配置

### OpenObserve

- **持久化**: 数据持久化到 Docker 卷。
- **端口**:
    - `5080`: HTTP API 和 UI
    - `5081`: OTLP gRPC

### OpenTelemetry Collector

- **接收器**: OTLP (gRPC `4317`, HTTP `4318`)
- **导出器**: 将数据发送到 OpenObserve。

## 🔗 集成

配置您的应用程序将 OTLP 数据发送到收集器：

- **端点**: `http://localhost:4318` (HTTP) 或 `localhost:4317` (gRPC)

RustFS 示例：

```bash
export RUSTFS_OBS_ENDPOINT=http://localhost:4318
export RUSTFS_OBS_SERVICE_NAME=rustfs-node-1
```
