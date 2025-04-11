# How to compile RustFS

| Must package | Version | download link                                                                                                                    |
|--------------|---------|----------------------------------------------------------------------------------------------------------------------------------|
| Rust         | 1.8.5   | https://www.rust-lang.org/tools/install                                                                                          |
| protoc       | 30.2    | [protoc-30.2-linux-x86_64.zip](https://github.com/protocolbuffers/protobuf/releases/download/v30.2/protoc-30.2-linux-x86_64.zip) |
| flatc        | 24.0+   | [Linux.flatc.binary.g++-13.zip](https://github.com/google/flatbuffers/releases/download/v25.2.10/Linux.flatc.binary.g++-13.zip)  |

Download Links:

https://github.com/google/flatbuffers/releases/download/v25.2.10/Linux.flatc.binary.g++-13.zip

https://github.com/protocolbuffers/protobuf/releases/download/v30.2/protoc-30.2-linux-x86_64.zip

Or use Docker:

- uses: arduino/setup-protoc@v3
  with:
  version: "27.0"

- uses: Nugine/setup-flatc@v1
  with:
  version: "24.3.25"

# How to add Console web

1. `wget https://dl.rustfs.com/artifacts/console/rustfs-console-latest.zip`

2. mkdir in this repos folder `./rustfs/static`

3. Compile RustFS

# Star RustFS

Add Env infomation:

```
export RUSTFS_VOLUMES="./target/volume/test"
export RUSTFS_ADDRESS="0.0.0.0:9000"
export RUSTFS_CONSOLE_ENABLE=true
export RUSTFS_CONSOLE_ADDRESS="0.0.0.0:9001"
# 具体路径修改为配置文件真实路径，obs.example.toml 仅供参考 其中`RUSTFS_OBS_CONFIG` 和下面变量二选一
export RUSTFS_OBS_CONFIG="./deploy/config/obs.example.toml"

# 如下变量需要必须参数都有值才可以，以及会覆盖配置文件`obs.example.toml`中的值
export RUSTFS__OBSERVABILITY__ENDPOINT=http://localhost:4317
export RUSTFS__OBSERVABILITY__USE_STDOUT=true
export RUSTFS__OBSERVABILITY__SAMPLE_RATIO=2.0
export RUSTFS__OBSERVABILITY__METER_INTERVAL=30
export RUSTFS__OBSERVABILITY__SERVICE_NAME=rustfs
export RUSTFS__OBSERVABILITY__SERVICE_VERSION=0.1.0
export RUSTFS__OBSERVABILITY__ENVIRONMENT=develop
export RUSTFS__OBSERVABILITY__LOGGER_LEVEL=info
export RUSTFS__SINKS__FILE__ENABLED=true
export RUSTFS__SINKS__FILE__PATH="./deploy/logs/rustfs.log"
export RUSTFS__SINKS__WEBHOOK__ENABLED=false
export RUSTFS__SINKS__WEBHOOK__ENDPOINT=""
export RUSTFS__SINKS__WEBHOOK__AUTH_TOKEN=""
export RUSTFS__SINKS__KAFKA__ENABLED=false
export RUSTFS__SINKS__KAFKA__BOOTSTRAP_SERVERS=""
export RUSTFS__SINKS__KAFKA__TOPIC=""
export RUSTFS__LOGGER__QUEUE_CAPACITY=10
```

You need replace your real data folder:

```
./rustfs /data/rustfs
```

## How to deploy the observability stack

The OpenTelemetry Collector offers a vendor-agnostic implementation on how to receive, process, and export telemetry
data. It removes the need to run, operate, and maintain multiple agents/collectors in order to support open-source
observability data formats (e.g. Jaeger, Prometheus, etc.) sending to one or more open-source or commercial back-ends.

1. Enter the `.docker/observability` directory,
2. Run the following command:

```bash
docker-compose -f docker-compose.yml up -d
```

3. Access the Grafana dashboard by navigating to `http://localhost:3000` in your browser. The default username and
   password are `admin` and `admin`, respectively.

4. Access the Jaeger dashboard by navigating to `http://localhost:16686` in your browser.

5. Access the Prometheus dashboard by navigating to `http://localhost:9090` in your browser.

## Create a new Observability configuration file

#### 1. Enter the `deploy/config` directory,

#### 2. Copy `obs.toml.example` to `obs.toml`

#### 3. Modify the `obs.toml` configuration file

##### 3.1. Modify the `endpoint` value to the address of the OpenTelemetry Collector

##### 3.2. Modify the `service_name` value to the name of the service

##### 3.3. Modify the `service_version` value to the version of the service

##### 3.4. Modify the `environment` value to the environment of the service

##### 3.5. Modify the `meter_interval` value to export interval

##### 3.6. Modify the `sample_ratio` value to the sample ratio

##### 3.7. Modify the `use_stdout` value to export to stdout

##### 3.8. Modify the `logger_level` value to the logger level


