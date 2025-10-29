# RustFS Performance Testing Guide

This document describes the recommended tools and workflows for benchmarking RustFS and analyzing performance bottlenecks.

## Overview

RustFS exposes several complementary tooling options:

1. **Profiling** – collect CPU samples through the built-in `pprof` endpoints.
2. **Load testing** – drive concurrent requests with dedicated client utilities.
3. **Monitoring and analysis** – inspect collected metrics to locate hotspots.

## Prerequisites

### 1. Enable profiling support

Set the profiling environment variable before launching RustFS:

```bash
export RUSTFS_ENABLE_PROFILING=true
./rustfs
```

### 2. Install required tooling

Make sure the following dependencies are available:

```bash
# Base tools
curl       # HTTP requests
jq         # JSON processing (optional)

# Analysis tools
go         # Go pprof CLI (optional, required for protobuf output)
python3    # Python load-testing scripts

# macOS users
brew install curl jq go python3

# Ubuntu/Debian users
sudo apt-get install curl jq golang-go python3
```

## Performance Testing Methods

### Method 1: Use the dedicated profiling script (recommended)

The repository ships with a helper script for common profiling flows:

```bash
# Show command help
./scripts/profile_rustfs.sh help

# Check profiler status
./scripts/profile_rustfs.sh status

# Capture a 30 second flame graph
./scripts/profile_rustfs.sh flamegraph

# Download protobuf-formatted samples
./scripts/profile_rustfs.sh protobuf

# Collect both formats
./scripts/profile_rustfs.sh both

# Provide custom arguments
./scripts/profile_rustfs.sh -d 60 -u http://192.168.1.100:9000 both
```

### Method 2: Run the Python end-to-end tester

A Python utility combines background load generation with profiling:

```bash
# Launch the integrated test harness
python3 test_load.py
```

The script will:

1. Launch multi-threaded S3 operations as load.
2. Pull profiling samples in parallel.
3. Produce a flame graph for investigation.

### Method 3: Simple shell-based load test

For quick smoke checks, a lightweight bash script is also provided:

```bash
# Execute a lightweight benchmark
./simple_load_test.sh
```

## Profiling Output Formats

### 1. Flame graph (SVG)

- **Purpose**: Visualize CPU time distribution.
- **File name**: `rustfs_profile_TIMESTAMP.svg`
- **How to view**: Open the SVG in a browser.
- **Interpretation tips**:
  - Width reflects CPU time per function.
  - Height illustrates call-stack depth.
  - Click to zoom into specific frames.

```bash
# Example: open the file in a browser
open profiles/rustfs_profile_20240911_143000.svg
```

### 2. Protobuf samples

- **Purpose**: Feed data to the `go tool pprof` command.
- **File name**: `rustfs_profile_TIMESTAMP.pb`
- **Tooling**: `go tool pprof`

```bash
# Analyze the protobuf output
go tool pprof profiles/rustfs_profile_20240911_143000.pb

# Common pprof commands
(pprof) top        # Show hottest call sites
(pprof) list func  # Display annotated source for a function
(pprof) web        # Launch the web UI (requires graphviz)
(pprof) png        # Render a PNG flame chart
(pprof) help       # List available commands
```

## API Usage

### Check profiling status

```bash
curl "http://127.0.0.1:9000/rustfs/admin/debug/pprof/status"
```

Sample response:

```json
{
  "enabled": "true",
  "sampling_rate": "100"
}
```

### Capture profiling data

```bash
# Fetch a 30-second flame graph
curl "http://127.0.0.1:9000/rustfs/admin/debug/pprof/profile?seconds=30&format=flamegraph" \
  -o profile.svg

# Fetch protobuf output
curl "http://127.0.0.1:9000/rustfs/admin/debug/pprof/profile?seconds=30&format=protobuf" \
  -o profile.pb
```

**Parameters**
- `seconds`: Duration between 1 and 300 seconds.
- `format`: Output format (`flamegraph`/`svg` or `protobuf`/`pb`).

## Load Testing Scenarios

### 1. S3 API workload

Use the Python harness to exercise a complete S3 workflow:

```python
# Basic configuration
tester = S3LoadTester(
    endpoint="http://127.0.0.1:9000",
    access_key="rustfsadmin",
    secret_key="rustfsadmin"
)

# Execute the load test
# Four threads, ten operations each
tester.run_load_test(num_threads=4, operations_per_thread=10)
```

Each iteration performs:
1. Upload a 1 MB object.
2. Download the object.
3. Delete the object.

### 2. Custom load scenarios

```bash
# Create a test bucket
curl -X PUT "http://127.0.0.1:9000/test-bucket"

# Concurrent uploads
for i in {1..10}; do
  echo "test data $i" | curl -X PUT "http://127.0.0.1:9000/test-bucket/object-$i" -d @- &
done
wait

# Concurrent downloads
for i in {1..10}; do
  curl "http://127.0.0.1:9000/test-bucket/object-$i" > /dev/null &
done
wait
```

## Profiling Best Practices

### 1. Environment preparation

- Confirm that `RUSTFS_ENABLE_PROFILING=true` is set.
- Use an isolated benchmark environment to avoid interference.
- Reserve disk space for generated profile artifacts.

### 2. Data collection tips

- **Warm-up**: Run a light workload for 5–10 minutes before sampling.
- **Sampling window**: Capture 30–60 seconds under steady load.
- **Multiple samples**: Take several runs to compare results.

### 3. Analysis focus areas

When inspecting flame graphs, pay attention to:

1. **The widest frames** – most CPU time consumed.
2. **Flat plateaus** – likely bottlenecks.
3. **Deep call stacks** – recursion or complex logic.
4. **Unexpected syscalls** – I/O stalls or allocation churn.

### 4. Common issues

- **Lock contention**: Investigate frames under `std::sync`.
- **Memory allocation**: Search for `alloc`-related frames.
- **I/O wait**: Review filesystem or network call stacks.
- **Serialization overhead**: Look for JSON/XML parsing hotspots.

## Troubleshooting

### 1. Profiling disabled

Error: `{"enabled":"false"}`

**Fix**:

```bash
export RUSTFS_ENABLE_PROFILING=true
# Restart RustFS
```

### 2. Connection refused

Error: `Connection refused`

**Checklist**:
- Confirm RustFS is running.
- Ensure the port number is correct (default 9000).
- Verify firewall rules.

### 3. Oversized profile output

If artifacts become too large:
- Shorten the capture window (e.g., 15–30 seconds).
- Reduce load-test concurrency.
- Prefer protobuf output instead of SVG.

## Configuration Parameters

### Environment variables

| Variable | Default | Description |
|------|--------|------|
| `RUSTFS_ENABLE_PROFILING` | `false` | Enable profiling support |
| `RUSTFS_URL` | `http://127.0.0.1:9000` | RustFS endpoint |
| `PROFILE_DURATION` | `30` | Profiling duration in seconds |
| `OUTPUT_DIR` | `./profiles` | Output directory |

### Script arguments

```bash
./scripts/profile_rustfs.sh [OPTIONS] [COMMAND]

OPTIONS:
  -u, --url URL           RustFS URL
  -d, --duration SECONDS  Profile duration
  -o, --output DIR        Output directory

COMMANDS:
  status      Check profiler status
  flamegraph  Collect a flame graph
  protobuf    Collect protobuf samples
  both        Collect both formats (default)
```

## Output Locations

- **Script output**: `./profiles/`
- **Python script**: `/tmp/rustfs_profiles/`
- **File naming**: `rustfs_profile_TIMESTAMP.{svg|pb}`

## Example Workflow

1. **Launch RustFS**
   ```bash
   RUSTFS_ENABLE_PROFILING=true ./rustfs
   ```

2. **Verify profiling availability**
   ```bash
   ./scripts/profile_rustfs.sh status
   ```

3. **Start a load test**
   ```bash
   python3 test_load.py &
   ```

4. **Collect samples**
   ```bash
   ./scripts/profile_rustfs.sh -d 60 both
   ```

5. **Inspect the results**
   ```bash
   # Review the flame graph
   open profiles/rustfs_profile_*.svg

   # Or analyze the protobuf output
   go tool pprof profiles/rustfs_profile_*.pb
   ```

Following this workflow helps you understand RustFS performance characteristics, locate bottlenecks, and implement targeted optimizations.
