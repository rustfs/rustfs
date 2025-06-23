# ECStore - Erasure Coding Storage

ECStore provides erasure coding functionality for the RustFS project, using high-performance Reed-Solomon SIMD implementation for optimal performance.

## Reed-Solomon Implementation

### SIMD Backend (Only)

- **Performance**: Uses SIMD optimization for high-performance encoding/decoding
- **Compatibility**: Works with any shard size through SIMD implementation
- **Reliability**: High-performance SIMD implementation for large data processing
- **Use case**: Optimized for maximum performance in large data processing scenarios

### Usage Example

```rust
use ecstore::erasure_coding::Erasure;

// Create erasure coding instance
// 4 data shards, 2 parity shards, 1KB block size
let erasure = Erasure::new(4, 2, 1024);

// Encode data
let data = b"hello world from rustfs erasure coding";
let shards = erasure.encode_data(data)?;

// Simulate loss of one shard
let mut shards_opt: Vec<Option<Vec<u8>>> = shards
    .iter()
    .map(|b| Some(b.to_vec()))
    .collect();
shards_opt[2] = None; // Lose shard 2

// Reconstruct missing data
erasure.decode_data(&mut shards_opt)?;

// Recover original data
let mut recovered = Vec::new();
for shard in shards_opt.iter().take(4) { // Only data shards
    recovered.extend_from_slice(shard.as_ref().unwrap());
}
recovered.truncate(data.len());
assert_eq!(&recovered, data);
```

## Performance Considerations

### SIMD Implementation Benefits
- **High Throughput**: Optimized for large block sizes (>= 1KB recommended)
- **CPU Optimization**: Leverages modern CPU SIMD instructions
- **Scalability**: Excellent performance for high-throughput scenarios

### Implementation Details

#### `reed-solomon-simd`
- **Instance Caching**: Encoder/decoder instances are cached and reused for optimal performance
- **Thread Safety**: Thread-safe with RwLock-based caching
- **SIMD Optimization**: Leverages CPU SIMD instructions for maximum performance
- **Reset Capability**: Cached instances are reset for different parameters, avoiding unnecessary allocations

### Performance Tips

1. **Batch Operations**: When possible, batch multiple small operations into larger blocks
2. **Block Size Optimization**: Use block sizes that are multiples of 64 bytes for optimal SIMD performance
3. **Memory Allocation**: Pre-allocate buffers when processing multiple blocks
4. **Cache Warming**: Initial operations may be slower due to cache setup, subsequent operations benefit from caching

## Cross-Platform Compatibility

The SIMD implementation supports:
- x86_64 with advanced SIMD instructions (AVX2, SSE)
- aarch64 (ARM64) with NEON SIMD optimizations
- Other architectures with fallback implementations

The implementation automatically selects the best available SIMD instructions for the target platform, providing optimal performance across different architectures.

## Testing and Benchmarking

Run performance benchmarks:
```bash
# Run erasure coding benchmarks
cargo bench --bench erasure_benchmark

# Run comparison benchmarks
cargo bench --bench comparison_benchmark

# Generate benchmark reports
./run_benchmarks.sh
```

## Error Handling

All operations return `Result` types with comprehensive error information:
- Encoding errors: Invalid parameters, insufficient memory
- Decoding errors: Too many missing shards, corrupted data
- Configuration errors: Invalid shard counts, unsupported parameters 