# Scanner Benchmark 基准测试

这个目录包含了Scanner性能优化的基准测试套件，用于验证优化效果和监控性能回归。

## 测试类型

### 1. 业务IO性能基准测试 (Business IO Benchmarks)
- **目的**: 测量Scanner对业务IO操作的影响
- **指标**: 吞吐量、延迟、IOPS
- **场景**: PUT/GET/DELETE操作在不同Scanner负载下的性能

### 2. Scanner扫描性能基准测试 (Scanner Performance Benchmarks)  
- **目的**: 测量Scanner本身的扫描效率
- **指标**: 扫描速度、内存使用、CPU使用
- **场景**: 不同数据量和文件分布下的扫描性能

### 3. 资源竞争基准测试 (Resource Contention Benchmarks)
- **目的**: 测量Scanner与业务IO的资源竞争情况
- **指标**: 磁盘队列深度、IO等待时间、系统负载
- **场景**: 并发业务请求与Scanner同时运行

### 4. 智能调度基准测试 (Adaptive Scheduling Benchmarks)
- **目的**: 验证IO感知调度算法的效果
- **指标**: 动态调节响应时间、负载感知准确性
- **场景**: 不同业务负载模式下的Scanner行为

## 运行方式

```bash
# 运行所有基准测试
cargo bench --package rustfs-ahm

# 运行特定测试组
cargo bench --package rustfs-ahm business_io
cargo bench --package rustfs-ahm scanner_perf  
cargo bench --package rustfs-ahm resource_contention
cargo bench --package rustfs-ahm adaptive_scheduling

# 生成HTML报告
cargo bench --package rustfs-ahm -- --output-format html

# 对比测试（优化前后）
cargo bench --package rustfs-ahm -- --save-baseline before_optimization
# ... 应用优化 ...
cargo bench --package rustfs-ahm -- --baseline before_optimization
```

## 性能目标

基于设计文档的优化目标：

| 测试场景 | 优化前 | 优化目标 | 验收标准 |
|---------|-------|---------|----------|
| 业务性能影响 | 6倍下降 | ≤10%下降 | ✓ 通过基准验证 |
| Scanner CPU使用 | >50% | <15% | ✓ 资源监控验证 |
| 磁盘IO竞争 | 严重冲突 | 最小化冲突 | ✓ 队列深度<2 |
| 动态调节响应 | 无 | <30秒 | ✓ 负载变化响应时间 |

## 测试环境要求

- 至少4个磁盘设备
- 内存：8GB+
- 网络：低延迟环境
- 数据：预置100GB+测试数据