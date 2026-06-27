# GET Optimization Stress Test Scripts

## Quick Start

### Prerequisites

- `warp` installed (https://github.com/minio/warp)
- `mc` configured with access to the RustFS server
- RustFS server running with GET optimizations enabled

### Quick Validation (5 minutes)

```bash
./scripts/quick-validate-get-optimization.sh localhost:9000
```

This runs basic functional tests:
- Data integrity verification
- Concurrent GET stability
- Early-stop behavior validation

### Full Stress Test (30+ minutes)

```bash
./scripts/stress-test-get-optimization.sh localhost:9000 ./stress-results
```

This runs comprehensive tests:
- Data correctness validation (1KB, 1MB, 10MB objects)
- Concurrent GET stress test (16, 64, 256 concurrency)
- Mixed read/write workload
- Early-stop behavior under load

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `WARP_ACCESS_KEY` | rustfsadmin | S3 access key |
| `WARP_SECRET_KEY` | rustfsadmin | S3 secret key |
| `MC_ALIAS` | rustfs | mc alias for the server |
| `TEST_BUCKET` | auto-generated | Test bucket name |
| `TEST_DURATION` | 300s | Duration for stress tests |
| `CONCURRENCY` | 64 | Default concurrency level |

## Test Scenarios

### 1. Data Correctness Validation

Verifies that GET returns correct data with early-stop enabled:
- Uploads random data
- Downloads and compares MD5 hash
- Tests different object sizes (1KB, 1MB, 10MB)

### 2. Concurrent GET Stress Test

Tests performance under high concurrency:
- Object sizes: 1KiB, 1MiB, 4MiB, 10MiB
- Concurrency levels: 16, 64, 256
- Duration: configurable (default 300s)

### 3. Mixed Read/Write Stress Test

Tests stability under concurrent read/write:
- 50% reads, 50% writes
- 1MB objects
- 64 concurrent operations

### 4. Early-Stop Behavior Validation

Verifies early-stop works correctly:
- 100 sequential reads of the same object
- Verifies data size matches expected
- Checks for any download failures

## Expected Results

### Success Criteria

- **Data Correctness**: 100% pass rate (no data corruption)
- **Concurrent GET**: No errors, consistent latency
- **Mixed Workload**: No deadlocks or data corruption
- **Early-Stop**: All reads return correct data size

### Performance Baselines

| Object Size | Expected Throughput | Expected p95 Latency |
|-------------|--------------------|--------------------|
| 1KiB | > 5 MiB/s | < 10ms |
| 1MiB | > 500 MiB/s | < 20ms |
| 4MiB | > 1000 MiB/s | < 30ms |
| 10MiB | > 2000 MiB/s | < 50ms |

## Troubleshooting

### Common Issues

1. **warp not found**: Install with `go install github.com/minio/warp@latest`
2. **mc not configured**: Run `mc alias set rustfs http://localhost:9000 admin password`
3. **Connection refused**: Verify RustFS is running and accessible
4. **Permission denied**: Check S3 credentials

### Debug Mode

Enable debug logging:
```bash
RUST_LOG=rustfs_ecstore::bucket::lifecycle=debug ./scripts/stress-test-get-optimization.sh
```

## Output Files

| File | Description |
|------|-------------|
| `test-config.txt` | Test configuration |
| `correctness-results.txt` | Data correctness results |
| `early-stop-results.txt` | Early-stop validation results |
| `get-*.json` | Concurrent GET performance (warp JSON) |
| `mixed-*.json` | Mixed workload performance (warp JSON) |
| `correctness-errors.log` | Data correctness errors (if any) |
| `early-stop-errors.log` | Early-stop errors (if any) |
