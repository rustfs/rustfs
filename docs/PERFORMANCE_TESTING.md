# RustFS 性能测试指南

本文档提供了对 RustFS 进行性能测试和性能分析的完整方法和工具。

## 概览

RustFS 提供了多种性能测试和分析工具：

1. **性能分析（Profiling）** - 使用内置的 pprof 接口收集 CPU 性能数据
2. **负载测试（Load Testing）** - 使用多种客户端工具模拟高并发请求
3. **监控和分析** - 查看性能指标和识别性能瓶颈

## 前置条件

### 1. 启用性能分析

在启动 RustFS 时，需要设置环境变量启用性能分析功能：

```bash
export RUSTFS_ENABLE_PROFILING=true
./rustfs
```

### 2. 安装依赖工具

确保系统中安装了以下工具：

```bash
# 基础工具
curl       # HTTP 请求
jq         # JSON 处理 (可选)

# 分析工具
go         # Go pprof 工具 (可选，用于 protobuf 格式)
python3    # Python 负载测试脚本

# macOS 用户
brew install curl jq go python3

# Ubuntu/Debian 用户  
sudo apt-get install curl jq golang-go python3
```

## 性能测试方法

### 方法 1：使用专业脚本（推荐）

项目提供了完整的性能分析脚本：

```bash
# 查看脚本帮助
./scripts/profile_rustfs.sh help

# 检查性能分析状态
./scripts/profile_rustfs.sh status

# 收集火焰图（30秒）
./scripts/profile_rustfs.sh flamegraph

# 收集 protobuf 格式性能数据
./scripts/profile_rustfs.sh protobuf

# 收集两种格式的性能数据
./scripts/profile_rustfs.sh both

# 自定义参数
./scripts/profile_rustfs.sh -d 60 -u http://192.168.1.100:9000 both
```

### 方法 2：使用 Python 综合测试

Python 脚本提供了负载测试和性能分析的一体化解决方案：

```bash
# 运行综合性能分析
python3 test_load.py
```

此脚本会：
1. 启动后台负载测试（多线程 S3 操作）
2. 并行收集性能分析数据
3. 生成火焰图用于分析

### 方法 3：使用简单负载测试

对于快速测试，可以使用 bash 脚本：

```bash
# 运行简单负载测试
./simple_load_test.sh
```

## 性能分析输出格式

### 1. 火焰图（SVG 格式）

- **用途**: 可视化 CPU 使用情况
- **文件**: `rustfs_profile_TIMESTAMP.svg`
- **查看方式**: 使用浏览器打开 SVG 文件
- **分析要点**:
  - 宽度表示 CPU 使用时间
  - 高度表示调用栈深度
  - 点击可以放大特定函数

```bash
# 在浏览器中打开
open profiles/rustfs_profile_20240911_143000.svg
```

### 2. Protobuf 格式

- **用途**: 使用 Go pprof 工具进行详细分析
- **文件**: `rustfs_profile_TIMESTAMP.pb` 
- **分析工具**: `go tool pprof`

```bash
# 使用 Go pprof 分析
go tool pprof profiles/rustfs_profile_20240911_143000.pb

# pprof 常用命令
(pprof) top        # 显示 CPU 使用率最高的函数
(pprof) list func  # 显示指定函数的源代码
(pprof) web        # 生成 web 界面（需要 graphviz）
(pprof) png        # 生成 PNG 图片
(pprof) help       # 查看所有命令
```

## API 接口使用

### 检查性能分析状态

```bash
curl "http://127.0.0.1:9000/rustfs/admin/debug/pprof/status"
```

返回示例：
```json
{
  "enabled": "true",
  "sampling_rate": "100"
}
```

### 收集性能数据

```bash
# 收集 30 秒的火焰图
curl "http://127.0.0.1:9000/rustfs/admin/debug/pprof/profile?seconds=30&format=flamegraph" \
  -o profile.svg

# 收集 protobuf 格式数据
curl "http://127.0.0.1:9000/rustfs/admin/debug/pprof/profile?seconds=30&format=protobuf" \
  -o profile.pb
```

**参数说明**:
- `seconds`: 收集时长（1-300 秒）
- `format`: 输出格式（`flamegraph`/`svg` 或 `protobuf`/`pb`）

## 负载测试场景

### 1. S3 API 负载测试

使用 Python 脚本进行完整的 S3 操作负载测试：

```python
# 基本配置
tester = S3LoadTester(
    endpoint="http://127.0.0.1:9000",
    access_key="rustfsadmin", 
    secret_key="rustfsadmin"
)

# 运行负载测试
# 4 个线程，每个线程执行 10 次操作
tester.run_load_test(num_threads=4, operations_per_thread=10)
```

每次操作包括：
1. 上传 1MB 对象
2. 下载对象
3. 删除对象

### 2. 自定义负载测试

```bash
# 创建测试桶
curl -X PUT "http://127.0.0.1:9000/test-bucket"

# 并发上传测试
for i in {1..10}; do
  echo "test data $i" | curl -X PUT "http://127.0.0.1:9000/test-bucket/object-$i" -d @- &
done
wait

# 并发下载测试  
for i in {1..10}; do
  curl "http://127.0.0.1:9000/test-bucket/object-$i" > /dev/null &
done
wait
```

## 性能分析最佳实践

### 1. 测试环境准备

- 确保 RustFS 已启用性能分析: `RUSTFS_ENABLE_PROFILING=true`
- 使用独立的测试环境，避免其他程序干扰
- 确保有足够的磁盘空间存储分析文件

### 2. 数据收集建议

- **预热阶段**: 先运行 5-10 分钟的轻量负载
- **数据收集**: 在稳定负载下收集 30-60 秒的性能数据
- **多次采样**: 收集多个样本进行对比分析

### 3. 分析重点

在火焰图中重点关注：

1. **宽度最大的函数** - CPU 使用时间最长
2. **平顶函数** - 可能的性能瓶颈
3. **深度调用栈** - 可能的递归或复杂逻辑
4. **意外的系统调用** - I/O 或内存分配问题

### 4. 常见性能问题

- **锁竞争**: 查找 `std::sync` 相关函数
- **内存分配**: 查找 `alloc` 相关函数
- **I/O 等待**: 查找文件系统或网络 I/O 函数
- **序列化开销**: 查找 JSON/XML 解析函数

## 故障排除

### 1. 性能分析未启用

错误信息：`{"enabled":"false"}`

解决方案：
```bash
export RUSTFS_ENABLE_PROFILING=true
# 重启 RustFS
```

### 2. 连接被拒绝

错误信息：`Connection refused`

检查项：
- RustFS 是否正在运行
- 端口是否正确（默认 9000）
- 防火墙设置

### 3. 分析文件过大

如果生成的分析文件过大：
- 减少收集时间（如 15-30 秒）
- 降低负载测试的并发度
- 使用 protobuf 格式而非 SVG

## 配置参数

### 环境变量

| 变量 | 默认值 | 描述 |
|------|--------|------|
| `RUSTFS_ENABLE_PROFILING` | `false` | 启用性能分析 |
| `RUSTFS_URL` | `http://127.0.0.1:9000` | RustFS 服务器地址 |
| `PROFILE_DURATION` | `30` | 性能数据收集时长（秒） |
| `OUTPUT_DIR` | `./profiles` | 输出文件目录 |

### 脚本参数

```bash
./scripts/profile_rustfs.sh [OPTIONS] [COMMAND]

OPTIONS:
  -u, --url URL           RustFS URL
  -d, --duration SECONDS  Profile duration  
  -o, --output DIR        Output directory

COMMANDS:
  status      检查状态
  flamegraph  收集火焰图
  protobuf    收集 protobuf 数据
  both        收集两种格式（默认）
```

## 输出文件位置

- **脚本输出**: `./profiles/` 目录
- **Python 脚本**: `/tmp/rustfs_profiles/` 目录
- **文件命名**: `rustfs_profile_TIMESTAMP.{svg|pb}`

## 示例工作流程

1. **启动 RustFS**:
   ```bash
   RUSTFS_ENABLE_PROFILING=true ./rustfs
   ```

2. **验证性能分析可用**:
   ```bash
   ./scripts/profile_rustfs.sh status
   ```

3. **开始负载测试**:
   ```bash
   python3 test_load.py &
   ```

4. **收集性能数据**:
   ```bash
   ./scripts/profile_rustfs.sh -d 60 both
   ```

5. **分析结果**:
   ```bash
   # 查看火焰图
   open profiles/rustfs_profile_*.svg
   
   # 或使用 pprof 分析
   go tool pprof profiles/rustfs_profile_*.pb
   ```

通过这个完整的性能测试流程，你可以系统地分析 RustFS 的性能特征，识别瓶颈，并进行有针对性的优化。