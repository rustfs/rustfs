# S3 Compatibility Tests Configuration

This directory contains the configuration for running [Ceph S3 compatibility tests](https://github.com/ceph/s3-tests) against RustFS.

## Configuration File

The `s3tests.conf` file is based on the official `s3tests.conf.SAMPLE` from the ceph/s3-tests repository. It uses environment variable substitution via `envsubst` to configure the endpoint and credentials.

### Key Configuration Points

- **Host**: Set via `${S3_HOST}` environment variable (e.g., `rustfs-single` for single-node, `lb` for multi-node)
- **Port**: 9000 (standard RustFS port)
- **Credentials**: Uses `${S3_ACCESS_KEY}` and `${S3_SECRET_KEY}` from workflow environment
- **TLS**: Disabled (`is_secure = False`)

## Test Execution Strategy

### Network Connectivity Fix

Tests run inside a Docker container on the `rustfs-net` network, which allows them to resolve and connect to the RustFS container hostnames. This fixes the "Temporary failure in name resolution" error that occurred when tests ran on the GitHub runner host.

### Performance Optimizations

1. **Parallel Execution**: Uses `pytest-xdist` with `-n 4` to run tests in parallel across 4 workers
2. **Load Distribution**: Uses `--dist=loadgroup` to distribute test groups across workers
3. **Fail-Fast**: Uses `--maxfail=50` to stop after 50 failures, saving time on catastrophic failures

### Feature Filtering

Tests are filtered using pytest markers (`-m`) to skip features not yet supported by RustFS:

- `lifecycle` - Bucket lifecycle policies
- `versioning` - Object versioning
- `s3website` - Static website hosting
- `bucket_logging` - Bucket logging
- `encryption` / `sse_s3` - Server-side encryption
- `cloud_transition` / `cloud_restore` - Cloud storage transitions
- `lifecycle_expiration` / `lifecycle_transition` - Lifecycle operations

This filtering:
1. Reduces test execution time significantly (from 1+ hour to ~10-15 minutes)
2. Focuses on features RustFS currently supports
3. Avoids hundreds of expected failures

## Running Tests Locally

### Single-Node Test

```bash
# Set credentials
export S3_ACCESS_KEY=rustfsadmin
export S3_SECRET_KEY=rustfsadmin

# Start RustFS container
docker run -d --name rustfs-single \
  --network rustfs-net \
  -e RUSTFS_ADDRESS=0.0.0.0:9000 \
  -e RUSTFS_ACCESS_KEY=$S3_ACCESS_KEY \
  -e RUSTFS_SECRET_KEY=$S3_SECRET_KEY \
  -e RUSTFS_VOLUMES="/data/rustfs0 /data/rustfs1 /data/rustfs2 /data/rustfs3" \
  rustfs-ci

# Generate config
export S3_HOST=rustfs-single
envsubst < .github/s3tests/s3tests.conf > /tmp/s3tests.conf

# Run tests
docker run --rm \
  --network rustfs-net \
  -v /tmp/s3tests.conf:/etc/s3tests.conf:ro \
  python:3.12-slim \
  bash -c '
    apt-get update -qq && apt-get install -y -qq git
    git clone --depth 1 https://github.com/ceph/s3-tests.git /s3-tests
    cd /s3-tests
    pip install -q -r requirements.txt pytest-xdist
    S3TEST_CONF=/etc/s3tests.conf pytest -v -n 4 \
      s3tests/functional/test_s3.py \
      -m "not lifecycle and not versioning and not s3website and not bucket_logging and not encryption and not sse_s3"
  '
```

## Test Results Interpretation

- **PASSED**: Test succeeded, feature works correctly
- **FAILED**: Test failed, indicates a potential bug or incompatibility
- **ERROR**: Test setup failed (e.g., network issues, missing dependencies)
- **SKIPPED**: Test skipped due to marker filtering

## Adding New Feature Support

When adding support for a new S3 feature to RustFS:

1. Remove the corresponding marker from the filter in `.github/workflows/e2e-s3tests.yml`
2. Run the tests to verify compatibility
3. Fix any failing tests
4. Update this README to reflect the newly supported feature

## References

- [Ceph S3 Tests Repository](https://github.com/ceph/s3-tests)
- [S3 API Compatibility](https://docs.aws.amazon.com/AmazonS3/latest/API/)
- [pytest-xdist Documentation](https://pytest-xdist.readthedocs.io/)
