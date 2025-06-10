# Reed-Solomon Erasure Coding Performance Benchmark

This directory contains a comprehensive benchmark suite for comparing the performance of different Reed-Solomon implementations.

## 📊 Test Overview

### Supported Implementation Modes

#### 🏛️ Pure Erasure Mode (Default, Recommended)
- **Stable and Reliable**: Uses mature reed-solomon-erasure implementation
- **Wide Compatibility**: Supports arbitrary shard sizes
- **Memory Efficient**: Optimized memory usage patterns
- **Predictable**: Performance insensitive to shard size
- **Use Case**: Default choice for production environments, suitable for most application scenarios

#### 🎯 SIMD Mode (`reed-solomon-simd` feature)
- **High Performance Optimization**: Uses SIMD instruction sets for high-performance encoding/decoding
- **Performance Oriented**: Focuses on maximizing processing performance
- **Target Scenarios**: High-performance scenarios for large data processing
- **Use Case**: Scenarios requiring maximum performance, suitable for handling large amounts of data

### Test Dimensions

- **Encoding Performance** - Speed of encoding data into erasure code shards
- **Decoding Performance** - Speed of recovering original data from erasure code shards
- **Shard Size Sensitivity** - Impact of different shard sizes on performance
- **Erasure Code Configuration** - Performance impact of different data/parity shard ratios
- **SIMD Mode Performance** - Performance characteristics of SIMD optimization
- **Concurrency Performance** - Performance in multi-threaded environments
- **Memory Efficiency** - Memory usage patterns and efficiency
- **Error Recovery Capability** - Recovery performance under different numbers of lost shards

## 🚀 Quick Start

### Run Quick Tests

```bash
# Run quick performance comparison tests (default pure Erasure mode)
./run_benchmarks.sh quick
```

### Run Complete Comparison Tests

```bash
# Run detailed implementation comparison tests
./run_benchmarks.sh comparison
```

### Run Specific Mode Tests

```bash
# Test default pure erasure mode (recommended)
./run_benchmarks.sh erasure

# Test SIMD mode
./run_benchmarks.sh simd
```

## 📈 Manual Benchmark Execution

### Basic Usage

```bash
# Run all benchmarks (default pure erasure mode)
cargo bench

# Run specific benchmark files
cargo bench --bench erasure_benchmark
cargo bench --bench comparison_benchmark
```

### Compare Different Implementation Modes

```bash
# Test default pure erasure mode
cargo bench --bench comparison_benchmark

# Test SIMD mode
cargo bench --bench comparison_benchmark \
    --features reed-solomon-simd

# Save baseline for comparison
cargo bench --bench comparison_benchmark \
    -- --save-baseline erasure_baseline

# Compare SIMD mode performance with baseline
cargo bench --bench comparison_benchmark \
    --features reed-solomon-simd \
    -- --baseline erasure_baseline
```

### Filter Specific Tests

```bash
# Run only encoding tests
cargo bench encode

# Run only decoding tests  
cargo bench decode

# Run tests for specific data sizes
cargo bench 1MB

# Run tests for specific configurations
cargo bench "4+2"
```

## 📊 View Results

### HTML Reports

Benchmark results automatically generate HTML reports:

```bash
# Start local server to view reports
cd target/criterion
python3 -m http.server 8080

# Access in browser
open http://localhost:8080/report/index.html
```

### Command Line Output

Benchmarks display in terminal:
- Operations per second (ops/sec)
- Throughput (MB/s)
- Latency statistics (mean, standard deviation, percentiles)
- Performance trend changes

## 🔧 Test Configuration

### Data Sizes

- **Small Data**: 1KB, 8KB - Test small file scenarios
- **Medium Data**: 64KB, 256KB - Test common file sizes
- **Large Data**: 1MB, 4MB - Test large file processing and SIMD optimization
- **Very Large Data**: 16MB+ - Test high throughput scenarios

### Erasure Code Configurations

- **(4,2)** - Common configuration, 33% redundancy
- **(6,3)** - 50% redundancy, balanced performance and reliability
- **(8,4)** - 50% redundancy, more parallelism
- **(10,5)**, **(12,6)** - High parallelism configurations

### Shard Sizes

Test different shard sizes from 32 bytes to 8KB, with special focus on:
- **Memory Alignment**: 64, 128, 256 bytes - Impact of memory alignment on performance
- **Cache Friendly**: 1KB, 2KB, 4KB - CPU cache-friendly sizes

## 📝 Interpreting Test Results

### Performance Metrics

1. **Throughput**
   - Unit: MB/s or GB/s
   - Measures data processing speed
   - Higher is better

2. **Latency**
   - Unit: microseconds (μs) or milliseconds (ms)
   - Measures single operation time
   - Lower is better

3. **CPU Efficiency**
   - Bytes processed per CPU cycle
   - Reflects algorithm efficiency

### Expected Results

**Pure Erasure Mode (Default)**:
- Stable performance, insensitive to shard size
- Best compatibility, supports all configurations
- Stable and predictable memory usage

**SIMD Mode (`reed-solomon-simd` feature)**:
- High-performance SIMD optimized implementation
- Suitable for large data processing scenarios
- Focuses on maximizing performance

**Shard Size Sensitivity**:
- SIMD mode may be more sensitive to shard sizes
- Pure Erasure mode relatively insensitive to shard size

**Memory Usage**:
- SIMD mode may have specific memory alignment requirements
- Pure Erasure mode has more stable memory usage

## 🛠️ Custom Testing

### Adding New Test Scenarios

Edit `benches/erasure_benchmark.rs` or `benches/comparison_benchmark.rs`:

```rust
// Add new test configuration
let configs = vec![
    // Your custom configuration
    BenchConfig::new(10, 4, 2048 * 1024, 2048 * 1024), // 10+4, 2MB
];
```

### Adjust Test Parameters

```rust
// Modify sampling and test time
group.sample_size(20);  // Sample count
group.measurement_time(Duration::from_secs(10));  // Test duration
```

## 🐛 Troubleshooting

### Common Issues

1. **Compilation Errors**: Ensure correct dependencies are installed
```bash
cargo update
cargo build --all-features
```

2. **Performance Anomalies**: Check if running in correct mode
```bash
# Check current configuration
cargo bench --bench comparison_benchmark -- --help
```

3. **Tests Taking Too Long**: Adjust test parameters
```bash
# Use shorter test duration
cargo bench -- --quick
```

### Performance Analysis

Use tools like `perf` for detailed performance analysis:

```bash
# Analyze CPU usage
cargo bench --bench comparison_benchmark & 
perf record -p $(pgrep -f comparison_benchmark)
perf report
```

## 🤝 Contributing

Welcome to submit new benchmark scenarios or optimization suggestions:

1. Fork the project
2. Create feature branch: `git checkout -b feature/new-benchmark`
3. Add test cases
4. Commit changes: `git commit -m 'Add new benchmark for XYZ'`
5. Push to branch: `git push origin feature/new-benchmark`
6. Create Pull Request

## 📚 References

- [reed-solomon-erasure crate](https://crates.io/crates/reed-solomon-erasure)
- [reed-solomon-simd crate](https://crates.io/crates/reed-solomon-simd)
- [Criterion.rs benchmark framework](https://bheisler.github.io/criterion.rs/book/)
- [Reed-Solomon error correction principles](https://en.wikipedia.org/wiki/Reed%E2%80%93Solomon_error_correction)

---

💡 **Tips**: 
- Recommend using the default pure Erasure mode, which provides stable performance across various scenarios
- Consider SIMD mode for high-performance requirements
- Benchmark results may vary based on hardware, operating system, and compiler versions
- Suggest running tests in target deployment environment for most accurate performance data 