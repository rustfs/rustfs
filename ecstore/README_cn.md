# ECStore - Erasure Coding Storage

ECStore provides erasure coding functionality for the RustFS project, supporting multiple Reed-Solomon implementations for optimal performance and compatibility.

## Reed-Solomon Implementations

### Available Backends

#### `reed-solomon-erasure` (Default)
- **Stability**: Mature and well-tested implementation
- **Performance**: Good performance with SIMD acceleration when available
- **Compatibility**: Works with any shard size
- **Memory**: Efficient memory usage
- **Use case**: Recommended for production use

#### `reed-solomon-simd` (Optional)
- **Performance**: Optimized SIMD implementation for maximum speed
- **Limitations**: Has restrictions on shard sizes (must be >= 64 bytes typically)
- **Memory**: May use more memory for small shards
- **Use case**: Best for large data blocks where performance is critical

### Feature Flags

Configure the Reed-Solomon implementation using Cargo features:

```toml
# Use default implementation (reed-solomon-erasure)
ecstore = "0.0.1"

# Use SIMD implementation for maximum performance
ecstore = { version = "0.0.1", features = ["reed-solomon-simd"], default-features = false }

# Use traditional implementation explicitly
ecstore = { version = "0.0.1", features = ["reed-solomon-erasure"], default-features = false }
```

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

### When to use `reed-solomon-simd`
- Large block sizes (>= 1KB recommended)
- High-throughput scenarios
- CPU-intensive workloads where encoding/decoding is the bottleneck

### When to use `reed-solomon-erasure`
- Small block sizes
- Memory-constrained environments
- General-purpose usage
- Production deployments requiring maximum stability

### Implementation Details

#### `reed-solomon-erasure`
- **Instance Reuse**: The encoder instance is cached and reused across multiple operations
- **Thread Safety**: Thread-safe with interior mutability
- **Memory Efficiency**: Lower memory footprint for small data

#### `reed-solomon-simd`
- **Instance Creation**: New encoder/decoder instances are created for each operation
- **API Design**: The SIMD implementation's API is designed for single-use instances
- **Performance Trade-off**: While instances are created per operation, the SIMD optimizations provide significant performance benefits for large data blocks
- **Optimization**: Future versions may implement instance pooling if the underlying API supports reuse

### Performance Tips

1. **Batch Operations**: When possible, batch multiple small operations into larger blocks
2. **Block Size Optimization**: Use block sizes that are multiples of 64 bytes for SIMD implementations
3. **Memory Allocation**: Pre-allocate buffers when processing multiple blocks
4. **Feature Selection**: Choose the appropriate feature based on your data size and performance requirements

## Cross-Platform Compatibility

Both implementations support:
- x86_64 with SIMD acceleration
- aarch64 (ARM64) with optimizations
- Other architectures with fallback implementations

The `reed-solomon-erasure` implementation provides better cross-platform compatibility and is recommended for most use cases. 