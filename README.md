# RustFS

## English Documentation |[中文文档](README_ZH.md)

### Prerequisites

| Package | Version | Download Link                                                                                                                    |
|---------|---------|----------------------------------------------------------------------------------------------------------------------------------|
| Rust    | 1.8.5+  | [rust-lang.org/tools/install](https://www.rust-lang.org/tools/install)                                                           |
| protoc  | 31.1+   | [protoc-31.1-linux-x86_64.zip](https://github.com/protocolbuffers/protobuf/releases/download/v31.1/protoc-31.1-linux-x86_64.zip) |
| flatc   | 24.0+   | [Linux.flatc.binary.g++-13.zip](https://github.com/google/flatbuffers/releases/download/v25.2.10/Linux.flatc.binary.g++-13.zip)  |

### Building RustFS

#### Generate Protobuf Code

```bash
cargo run --bin gproto
```

#### Using Docker for Prerequisites

```yaml
- uses: arduino/setup-protoc@v3
  with:
    version: "30.2"

- uses: Nugine/setup-flatc@v1
  with:
    version: "25.2.10"
```

#### Adding Console Web UI

1. Download the latest console UI:
   ```bash
   wget https://dl.rustfs.com/artifacts/console/rustfs-console-latest.zip
   ```
2. Create the static directory:
   ```bash
   mkdir -p ./rustfs/static
   ```
3. Extract and compile RustFS:
   ```bash
   unzip rustfs-console-latest.zip -d ./rustfs/static
   cargo build
   ```

### Running RustFS

#### Configuration

Set the required environment variables:

```bash
# Basic config
export RUSTFS_VOLUMES="./target/volume/test"
export RUSTFS_ADDRESS="0.0.0.0:9000"
export RUSTFS_CONSOLE_ENABLE=true
export RUSTFS_CONSOLE_ADDRESS="0.0.0.0:9001"

# Observability config
export RUSTFS_OBS_ENDPOINT="http://localhost:4317"

# Event message configuration
#export RUSTFS_EVENT_CONFIG="./deploy/config/event.toml"

```

#### Start the service

```bash
./rustfs /data/rustfs
```

## Observability Stack Otel and OpenObserve

### OpenTelemetry Collector 和 Jaeger、Grafana、Prometheus、Loki

1. Navigate to the observability directory:
   ```bash
   cd .docker/observability
   ```

2. Start the observability stack:
   ```bash
   docker compose -f docker-compose.yml  up -d
   ```

#### Access Monitoring Dashboards

- Grafana: `http://localhost:3000` (credentials: `admin`/`admin`)
- Jaeger: `http://localhost:16686`
- Prometheus: `http://localhost:9090`

#### Configure observability

```
OpenTelemetry Collector address(endpoint):  http://localhost:4317 
```

---

### OpenObserve and OpenTelemetry Collector

1. Navigate to the OpenObserve and OpenTelemetry directory:
   ```bash
   cd .docker/openobserve-otel
   ```
2. Start the OpenObserve and OpenTelemetry Collector services:
   ```bash
    docker compose -f docker-compose.yml up -d
    ```
3. Access the OpenObserve UI:
   OpenObserve UI: `http://localhost:5080`
    - Default credentials:
        - Username: `root@rustfs.com`
        - Password: `rustfs123`
    - Exposed ports:
        - 5080: HTTP API and UI
        - 5081: OTLP gRPC
