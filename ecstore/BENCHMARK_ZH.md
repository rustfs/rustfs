# Reed-Solomon 纠删码性能基准测试

本目录包含了比较不同 Reed-Solomon 实现性能的综合基准测试套件。

## 📊 测试概述

### 支持的实现模式

#### 🏛️ 纯 Erasure 模式（默认，推荐）
- **稳定可靠**: 使用成熟的 reed-solomon-erasure 实现
- **广泛兼容**: 支持任意分片大小
- **内存高效**: 优化的内存使用模式
- **可预测性**: 性能对分片大小不敏感
- **使用场景**: 生产环境默认选择，适合大多数应用场景

#### 🎯 SIMD模式（`reed-solomon-simd` feature）
- **高性能优化**: 使用SIMD指令集进行高性能编码解码
- **性能导向**: 专注于最大化处理性能
- **适用场景**: 大数据量处理的高性能场景
- **使用场景**: 需要最大化性能的场景，适合处理大量数据

### 测试维度

- **编码性能** - 数据编码成纠删码分片的速度
- **解码性能** - 从纠删码分片恢复原始数据的速度
- **分片大小敏感性** - 不同分片大小对性能的影响
- **纠删码配置** - 不同数据/奇偶分片比例的性能影响
- **SIMD模式性能** - SIMD优化的性能表现
- **并发性能** - 多线程环境下的性能表现
- **内存效率** - 内存使用模式和效率
- **错误恢复能力** - 不同丢失分片数量下的恢复性能

## 🚀 快速开始

### 运行快速测试

```bash
# 运行快速性能对比测试（默认纯Erasure模式）
./run_benchmarks.sh quick
```

### 运行完整对比测试

```bash
# 运行详细的实现对比测试
./run_benchmarks.sh comparison
```

### 运行特定模式的测试

```bash
# 测试默认纯 erasure 模式（推荐）
./run_benchmarks.sh erasure

# 测试SIMD模式
./run_benchmarks.sh simd
```

## 📈 手动运行基准测试

### 基本使用

```bash
# 运行所有基准测试（默认纯 erasure 模式）
cargo bench

# 运行特定的基准测试文件
cargo bench --bench erasure_benchmark
cargo bench --bench comparison_benchmark
```

### 对比不同实现模式

```bash
# 测试默认纯 erasure 模式
cargo bench --bench comparison_benchmark

# 测试SIMD模式
cargo bench --bench comparison_benchmark \
    --features reed-solomon-simd

# 保存基线进行对比
cargo bench --bench comparison_benchmark \
    -- --save-baseline erasure_baseline

# 与基线比较SIMD模式性能
cargo bench --bench comparison_benchmark \
    --features reed-solomon-simd \
    -- --baseline erasure_baseline
```

### 过滤特定测试

```bash
# 只运行编码测试
cargo bench encode

# 只运行解码测试  
cargo bench decode

# 只运行特定数据大小的测试
cargo bench 1MB

# 只运行特定配置的测试
cargo bench "4+2"
```

## 📊 查看结果

### HTML 报告

基准测试结果会自动生成 HTML 报告：

```bash
# 启动本地服务器查看报告
cd target/criterion
python3 -m http.server 8080

# 在浏览器中访问
open http://localhost:8080/report/index.html
```

### 命令行输出

基准测试会在终端显示：
- 每秒操作数 (ops/sec)
- 吞吐量 (MB/s)
- 延迟统计 (平均值、标准差、百分位数)
- 性能变化趋势

## 🔧 测试配置

### 数据大小

- **小数据**: 1KB, 8KB - 测试小文件场景
- **中等数据**: 64KB, 256KB - 测试常见文件大小
- **大数据**: 1MB, 4MB - 测试大文件处理和 SIMD 优化
- **超大数据**: 16MB+ - 测试高吞吐量场景

### 纠删码配置

- **(4,2)** - 常用配置，33% 冗余
- **(6,3)** - 50% 冗余，平衡性能和可靠性
- **(8,4)** - 50% 冗余，更多并行度
- **(10,5)**, **(12,6)** - 高并行度配置

### 分片大小

测试从 32 字节到 8KB 的不同分片大小，特别关注：
- **内存对齐**: 64, 128, 256 字节 - 内存对齐对性能的影响
- **Cache 友好**: 1KB, 2KB, 4KB - CPU 缓存友好的大小

## 📝 解读测试结果

### 性能指标

1. **吞吐量 (Throughput)**
   - 单位: MB/s 或 GB/s
   - 衡量数据处理速度
   - 越高越好

2. **延迟 (Latency)**
   - 单位: 微秒 (μs) 或毫秒 (ms)
   - 衡量单次操作时间
   - 越低越好

3. **CPU 效率**
   - 每 CPU 周期处理的字节数
   - 反映算法效率

### 预期结果

**纯 Erasure 模式（默认）**:
- 性能稳定，对分片大小不敏感
- 兼容性最佳，支持所有配置
- 内存使用稳定可预测

**SIMD模式（`reed-solomon-simd` feature）**:
- 高性能SIMD优化实现
- 适合大数据量处理场景
- 专注于最大化性能

**分片大小敏感性**:
- SIMD模式对分片大小可能更敏感
- 纯 Erasure 模式对分片大小相对不敏感

**内存使用**:
- SIMD模式可能有特定的内存对齐要求
- 纯 Erasure 模式内存使用更稳定

## 🛠️ 自定义测试

### 添加新的测试场景

编辑 `benches/erasure_benchmark.rs` 或 `benches/comparison_benchmark.rs`：

```rust
// 添加新的测试配置
let configs = vec![
    // 你的自定义配置
    BenchConfig::new(10, 4, 2048 * 1024, 2048 * 1024), // 10+4, 2MB
];
```

### 调整测试参数

```rust
// 修改采样和测试时间
group.sample_size(20);  // 样本数量
group.measurement_time(Duration::from_secs(10));  // 测试时间
```

## 🐛 故障排除

### 常见问题

1. **编译错误**: 确保安装了正确的依赖
```bash
cargo update
cargo build --all-features
```

2. **性能异常**: 检查是否在正确的模式下运行
```bash
# 检查当前配置
cargo bench --bench comparison_benchmark -- --help
```

3. **测试时间过长**: 调整测试参数
```bash
# 使用更短的测试时间
cargo bench -- --quick
```

### 性能分析

使用 `perf` 等工具进行更详细的性能分析：

```bash
# 分析 CPU 使用情况
cargo bench --bench comparison_benchmark & 
perf record -p $(pgrep -f comparison_benchmark)
perf report
```

## 🤝 贡献

欢迎提交新的基准测试场景或优化建议：

1. Fork 项目
2. 创建特性分支: `git checkout -b feature/new-benchmark`
3. 添加测试用例
4. 提交更改: `git commit -m 'Add new benchmark for XYZ'`
5. 推送到分支: `git push origin feature/new-benchmark`
6. 创建 Pull Request

## 📚 参考资料

- [reed-solomon-erasure crate](https://crates.io/crates/reed-solomon-erasure)
- [reed-solomon-simd crate](https://crates.io/crates/reed-solomon-simd)
- [Criterion.rs 基准测试框架](https://bheisler.github.io/criterion.rs/book/)
- [Reed-Solomon 纠删码原理](https://en.wikipedia.org/wiki/Reed%E2%80%93Solomon_error_correction)

---

💡 **提示**: 
- 推荐使用默认的纯Erasure模式，它在各种场景下都有稳定的表现
- 对于高性能需求可以考虑SIMD模式
- 基准测试结果可能因硬件、操作系统和编译器版本而异
- 建议在目标部署环境中运行测试以获得最准确的性能数据 