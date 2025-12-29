# S3 Compatibility Tests

This directory contains scripts for running S3 compatibility tests against RustFS.

## Quick Start

Run the local S3 compatibility test script:

```bash
./scripts/s3-tests/run.sh
```

The script will automatically:

1. Build RustFS (if needed)
2. Start the service
3. Wait for it to be ready
4. Run S3 compatibility tests
5. Collect results in `artifacts/s3tests-single/`

## Deployment Modes

The script supports four deployment modes, controlled via the `DEPLOY_MODE` environment variable:

### 1. Build Mode (Default)

Compile with `cargo build --release` and run:

```bash
DEPLOY_MODE=build ./scripts/s3-tests/run.sh
# Or simply (build is the default)
./scripts/s3-tests/run.sh

# Force rebuild even if binary exists and is recent
./scripts/s3-tests/run.sh --no-cache
```

**Note**: In build mode, if the binary exists and was compiled less than 30 minutes ago, compilation will be skipped unless `--no-cache` is specified.

### 2. Binary File Mode

Use pre-compiled binary file:

```bash
# Use default path (./target/release/rustfs)
DEPLOY_MODE=binary ./scripts/s3-tests/run.sh

# Specify custom binary path
DEPLOY_MODE=binary RUSTFS_BINARY=./target/release/rustfs ./scripts/s3-tests/run.sh
```

### 3. Docker Mode

Build Docker image and run in container:

```bash
DEPLOY_MODE=docker ./scripts/s3-tests/run.sh
```

### 4. Existing Service Mode

Connect to an already running RustFS service:

```bash
DEPLOY_MODE=existing S3_HOST=127.0.0.1 S3_PORT=9000 ./scripts/s3-tests/run.sh

# Connect to remote service
DEPLOY_MODE=existing S3_HOST=192.168.1.100 S3_PORT=9000 ./scripts/s3-tests/run.sh
```

## Configuration Options

### Command Line Options

- `-h, --help`: Show help message
- `--no-cache`: Force rebuild even if binary exists and is recent (for build mode)

### Deployment Configuration

- `DEPLOY_MODE`: Deployment mode, options:
  - `build`: Compile with `cargo build --release` and run (default)
  - `binary`: Use pre-compiled binary file
  - `docker`: Build Docker image and run in container
  - `existing`: Use already running service
- `RUSTFS_BINARY`: Path to binary file (for binary mode, default: `./target/release/rustfs`)
- `DATA_ROOT`: Root directory for test data storage (default: `target`)
  - Final path: `${DATA_ROOT}/test-data/${CONTAINER_NAME}`
  - Example: `DATA_ROOT=/tmp` stores data in `/tmp/test-data/rustfs-single/`

### Service Configuration

- `S3_ACCESS_KEY`: Main user access key (default: `rustfsadmin`)
- `S3_SECRET_KEY`: Main user secret key (default: `rustfsadmin`)
- `S3_ALT_ACCESS_KEY`: Alt user access key (default: `rustfsalt`)
- `S3_ALT_SECRET_KEY`: Alt user secret key (default: `rustfsalt`)
- `S3_REGION`: S3 region (default: `us-east-1`)
- `S3_HOST`: S3 service host (default: `127.0.0.1`)
- `S3_PORT`: S3 service port (default: `9000`)

### Test Parameters

- `TEST_MODE`: Test mode (default: `single`)
- `MAXFAIL`: Stop after N failures (default: `1`)
- `XDIST`: Enable parallel execution with N workers (default: `0`, disabled)
- `MARKEXPR`: pytest marker expression for filtering tests (default: exclude unsupported features)

### Configuration Files

- `S3TESTS_CONF_TEMPLATE`: Path to s3tests config template (default: `.github/s3tests/s3tests.conf`)
  - Relative to project root
  - Uses `envsubst` to substitute variables (e.g., `${S3_HOST}`)
- `S3TESTS_CONF`: Path to generated s3tests config (default: `s3tests.conf`)
  - Relative to project root
  - This file is generated from the template before running tests

## Examples

### Build Mode (Default)

```bash
# Basic usage - compiles and runs automatically
./scripts/s3-tests/run.sh

# Force rebuild (skip cache check)
./scripts/s3-tests/run.sh --no-cache

# Run all tests, stop after 50 failures
MAXFAIL=50 ./scripts/s3-tests/run.sh

# Enable parallel execution (4 worker processes)
XDIST=4 ./scripts/s3-tests/run.sh

# Use custom data storage location
DATA_ROOT=/tmp ./scripts/s3-tests/run.sh
```

### Binary File Mode

```bash
# First compile the binary
cargo build --release

# Run with default path
DEPLOY_MODE=binary ./scripts/s3-tests/run.sh

# Specify custom path
DEPLOY_MODE=binary RUSTFS_BINARY=/path/to/rustfs ./scripts/s3-tests/run.sh

# Use binary with parallel tests
DEPLOY_MODE=binary XDIST=4 ./scripts/s3-tests/run.sh
```

### Docker Mode

```bash
# Build Docker image and run in container
DEPLOY_MODE=docker ./scripts/s3-tests/run.sh

# Run with parallel tests
DEPLOY_MODE=docker XDIST=4 ./scripts/s3-tests/run.sh
```

### Existing Service Mode

```bash
# Connect to locally running service
DEPLOY_MODE=existing ./scripts/s3-tests/run.sh

# Connect to remote service
DEPLOY_MODE=existing S3_HOST=192.168.1.100 S3_PORT=9000 ./scripts/s3-tests/run.sh

# Test specific features
DEPLOY_MODE=existing MARKEXPR="not lifecycle and not versioning" ./scripts/s3-tests/run.sh
```

### Custom Configuration Files

```bash
# Use custom config template and output path
S3TESTS_CONF_TEMPLATE=my-configs/s3tests.conf.template \
S3TESTS_CONF=my-s3tests.conf \
./scripts/s3-tests/run.sh
```

## Test Results

Test results are saved in the `artifacts/s3tests-${TEST_MODE}/` directory (default: `artifacts/s3tests-single/`):

- `junit.xml`: Test results in JUnit format (compatible with CI/CD systems)
- `pytest.log`: Detailed pytest logs with full test output
- `rustfs-${TEST_MODE}/rustfs.log`: RustFS service logs
- `rustfs-${TEST_MODE}/inspect.json`: Service metadata (PID, binary path, mode, etc.)

View results:

```bash
# Check test summary
cat artifacts/s3tests-single/junit.xml | grep -E "testsuite|testcase"

# View test logs
less artifacts/s3tests-single/pytest.log

# View service logs
less artifacts/s3tests-single/rustfs-single/rustfs.log
```

## Prerequisites

### Required

- Docker (for docker mode only)
- Python 3 (for running s3-tests)
- `nc` (netcat) or `timeout` command (for port checking)

### Auto-installed

The script will automatically install the following dependencies if missing:

- `awscurl` (for S3 API calls and user provisioning)
- `tox` (for running s3-tests in isolated environment)
- `gettext-base` (for `envsubst` - config file generation)
  - On macOS: `brew install gettext`
  - On Linux: `apt-get install gettext-base`

### Proxy Configuration

The script automatically disables proxy for localhost requests to avoid interference. All proxy environment variables (`http_proxy`, `https_proxy`, etc.) are unset at script startup.

## Troubleshooting

### Port Already in Use

If port 9000 is already in use, change the port:

```bash
S3_PORT=9001 ./scripts/s3-tests/run.sh
```

**Note**: The script automatically checks if the port is available before starting (except in `existing` mode). If the port is in use, the script will exit with an error message.

### Container Start Failure

Check Docker logs:

```bash
docker logs rustfs-single
```

### Binary Not Found

For binary mode, ensure the binary is compiled:

```bash
cargo build --release
```

Or specify the correct path:

```bash
DEPLOY_MODE=binary RUSTFS_BINARY=/path/to/rustfs ./scripts/s3-tests/run.sh
```

### Test Timeout

Increase wait time or check service status:

```bash
curl http://127.0.0.1:9000/health
```

### Existing Service Not Accessible

For existing mode, ensure the service is running and accessible:

```bash
# Check if service is reachable
curl http://192.168.1.100:9000/health

# Verify S3 API is responding
awscurl --service s3 --region us-east-1 \
  --access_key rustfsadmin \
  --secret_key rustfsadmin \
  -X GET "http://192.168.1.100:9000/"
```

## Workflow Integration

This script mirrors the GitHub Actions workflow defined in `.github/workflows/e2e-s3tests.yml`.

The script follows the same steps:

1. Check port availability (skip for existing mode)
2. Build/start RustFS service (varies by deployment mode)
3. Wait for service to be fully ready:
   - Check process/container status
   - Check port is listening
   - Wait for "server started successfully" log message
   - Verify S3 API is responding
4. Generate s3tests configuration from template
5. Provision alt user for s3-tests via admin API
6. Run ceph s3-tests with tox
7. Collect logs and results

### Key Improvements Over Workflow

- **Smart compilation**: Skips rebuild if binary is recent (< 30 minutes)
- **Better health checks**: Log-based readiness detection instead of blind waiting
- **Port conflict detection**: Prevents conflicts before starting service
- **Proxy handling**: Automatically disables proxy for localhost
- **Configurable paths**: All paths (data, configs, artifacts) can be customized

## See Also

- [GitHub Actions Workflow](../.github/workflows/e2e-s3tests.yml)
- [S3 Tests Configuration](../.github/s3tests/s3tests.conf)
- [Ceph S3 Tests Repository](https://github.com/ceph/s3-tests)
