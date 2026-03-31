# Concurrent Download Tool (tests)

This tool downloads multiple URLs concurrently and saves files to a target directory.

Saved filename format:

`<nanoseconds>_<index>_<original_filename>`

All downloaded files are written into one output directory.

## Environment variables

- `DOWNLOAD_URLS` (required): comma-separated URLs.
- `DOWNLOAD_OUTPUT_DIR` (optional): output directory, default `target/tmp/concurrent_downloads`.
- `DOWNLOAD_CONCURRENCY` (optional): max concurrent downloads, default `8`.
- `DOWNLOAD_REPEAT` (optional): repeat count per URL, default `1`.
- `DOWNLOAD_MAX_RETRIES` (optional): retry count per task after first failure, default `0`.
- `DOWNLOAD_RETRY_BACKOFF_MS` (optional): fixed backoff between retries, default `200`.

## Statistics output

After run, the tool prints:

- total tasks
- succeeded
- failed
- total bytes
- elapsed ms
- throughput bps
- total attempts
- retried tasks
- retry attempts
- latency p50 ms
- latency p95 ms
- failure details (`[index] url => error`) when failures exist

If any task fails, the test returns error after printing the summary.

Retry is triggered only for recoverable cases:

- network/request timeout/connect errors
- HTTP `429`
- HTTP `5xx`

## Compile check

```bash
cargo test -p rustfs --test concurrent_download_tool --no-run
```

## Manual run example

The commands below are for manual execution only.
They are not part of automated test runs.

```bash
DOWNLOAD_URLS="http://127.0.0.1:9001/demo/google-cloud-aiplugin-1.46.1-253.zip?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Content-Sha256=UNSIGNED-PAYLOAD&X-Amz-Credential=HAXVOTZK9MLBJT8KWI4E%2F20260329%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20260329T105159Z&X-Amz-Expires=86400&X-Amz-Security-Token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJwYXJlbnQiOiJydXN0ZnNhZG1pbiIsImV4cCI6MTc3NDgyMDgyMX0.tYhQoPRcg0Ysx4KVw9ez7ZpYxsqGgqomtsP_iaeTsKzoii8EVNt74BZm2wbUjXW-FbGXc1pqEYX6wZ5Ncpk9Iw&X-Amz-Signature=15f47b19832f53b34f9e0fe1862d53d71660bbf8f1a512669bb2d041ac8d0697&X-Amz-SignedHeaders=host&x-amz-checksum-mode=ENABLED&x-id=GetObject" \
DOWNLOAD_OUTPUT_DIR="/Users/zhi/Documents/code/rust/rustfs/rustfs/target/tmp/concurrent_downloads" \
DOWNLOAD_CONCURRENCY="40" \
DOWNLOAD_REPEAT="40" \
DOWNLOAD_MAX_RETRIES="2" \
DOWNLOAD_RETRY_BACKOFF_MS="300" \
cargo test -p rustfs --test concurrent_download_tool -- --ignored --nocapture
```

