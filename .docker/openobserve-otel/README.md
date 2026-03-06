# OpenObserve + OpenTelemetry Collector

[![OpenObserve](https://img.shields.io/badge/OpenObserve-OpenSource-blue.svg)](https://openobserve.org)
[![OpenTelemetry](https://img.shields.io/badge/OpenTelemetry-Collector-green.svg)](https://opentelemetry.io/)

English | [中文](README_ZH.md)

This directory contains the configuration for an **alternative** observability stack using OpenObserve.

## ⚠️ Note

For the **recommended** observability stack (Prometheus, Grafana, Tempo, Loki), please see `../observability/`.

## 🌟 Overview

OpenObserve is a lightweight, all-in-one observability platform that handles logs, metrics, and traces in a single binary. This setup is ideal for:
- Resource-constrained environments.
- Quick setup and testing.
- Users who prefer a unified UI.

## 🚀 Quick Start

### 1. Start Services

```bash
cd .docker/openobserve-otel
docker compose up -d
```

### 2. Access Dashboard

- **URL**: [http://localhost:5080](http://localhost:5080)
- **Username**: `root@rustfs.com`
- **Password**: `rustfs123`

## 🛠️ Configuration

### OpenObserve

- **Persistence**: Data is persisted to a Docker volume.
- **Ports**:
    - `5080`: HTTP API and UI
    - `5081`: OTLP gRPC

### OpenTelemetry Collector

- **Receivers**: OTLP (gRPC `4317`, HTTP `4318`)
- **Exporters**: Sends data to OpenObserve.

## 🔗 Integration

Configure your application to send OTLP data to the collector:

- **Endpoint**: `http://localhost:4318` (HTTP) or `localhost:4317` (gRPC)

Example for RustFS:

```bash
export RUSTFS_OBS_ENDPOINT=http://localhost:4318
export RUSTFS_OBS_SERVICE_NAME=rustfs-node-1
```
