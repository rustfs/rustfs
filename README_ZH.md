# RustFS

## [English Documentation](README.md) ｜中文文档

### 前置要求

| 软件包    | 版本     | 下载链接                                                                                                                             |
|--------|--------|----------------------------------------------------------------------------------------------------------------------------------|
| Rust   | 1.8.5+ | [rust-lang.org/tools/install](https://www.rust-lang.org/tools/install)                                                           |
| protoc | 30.2+  | [protoc-30.2-linux-x86_64.zip](https://github.com/protocolbuffers/protobuf/releases/download/v30.2/protoc-30.2-linux-x86_64.zip) |
| flatc  | 24.0+  | [Linux.flatc.binary.g++-13.zip](https://github.com/google/flatbuffers/releases/download/v25.2.10/Linux.flatc.binary.g++-13.zip)  |

### 构建 RustFS

#### 生成 Protobuf 代码

```bash
cargo run --bin gproto
```

#### 使用 Docker 安装依赖

```yaml
- uses: arduino/setup-protoc@v3
  with:
    version: "30.2"

- uses: Nugine/setup-flatc@v1
  with:
    version: "25.2.10"
```

#### 添加控制台 Web UI

1. 下载最新的控制台 UI：
   ```bash
   wget https://dl.rustfs.com/artifacts/console/rustfs-console-latest.zip
   ```
2. 创建静态资源目录：
   ```bash
   mkdir -p ./rustfs/static
   ```
3. 解压并编译 RustFS：
   ```bash
   unzip rustfs-console-latest.zip -d ./rustfs/static
   cargo build
   ```

### 运行 RustFS

#### 配置

设置必要的环境变量：

```bash
# 基础配置
export RUSTFS_VOLUMES="./target/volume/test"
export RUSTFS_ADDRESS="0.0.0.0:9000"
export RUSTFS_CONSOLE_ENABLE=true
export RUSTFS_CONSOLE_ADDRESS="0.0.0.0:9001"

# 可观测性配置
export RUSTFS_OBS_CONFIG="./deploy/config/obs.toml"

# 事件消息配置
#export RUSTFS_EVENT_CONFIG="./deploy/config/event.toml"
```

#### 启动服务

```bash
./rustfs /data/rustfs
```

### 可观测性系统

#### 部署

1. 进入可观测性目录：
   ```bash
   cd .docker/observability
   ```

2. 启动可观测性系统：
   ```bash
   docker compose -f docker-compose.yml  up -d
   ```

#### 访问监控面板

- Grafana: `http://localhost:3000` (默认账号/密码：`admin`/`admin`)
- Jaeger: `http://localhost:16686`
- Prometheus: `http://localhost:9090`

#### 配置可观测性

1. 复制示例配置：
   ```bash
   cd deploy/config
   cp obs.toml.example obs.toml
   ```

2. 编辑 `obs.toml` 配置文件，参数如下：

| 配置项                  | 说明                         | 示例值                   |
|----------------------|----------------------------|-----------------------|
| endpoint             | OpenTelemetry Collector 地址 | http://localhost:4317 |
| service_name         | 服务名称                       | rustfs                |
| service_version      | 服务版本                       | 1.0.0                 |
| environment          | 运行环境                       | production            |
| meter_interval       | 指标导出间隔 (秒)                 | 30                    |
| sample_ratio         | 采样率                        | 1.0                   |
| use_stdout           | 是否输出到控制台                   | true/false            |
| logger_level         | 日志级别                       | info                  |
| local_logging_enable | 控制台是否答应日志                  | true/false            |

```