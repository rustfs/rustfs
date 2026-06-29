# Issue 784 S3 GET Output Stage Breakdown

Date: 2026-06-29

Branch: `houseme/issue-779-small-get-observability`

Backlog: <https://github.com/rustfs/backlog/issues/784>

## Scope

This change implements the Phase 0 observability step from issue 784. It keeps
GET response construction behavior unchanged and only splits the existing
`s3_handler/output_build` stage into bounded sub-stages:

- `output_strategy`
- `body_build`
- `checksum_headers`
- `lifecycle_expiration`
- `metadata_filter`

No bucket name, object key, request id, version id, or user metadata is used as
a metric label.

## Implementation

File changed:

- `rustfs/src/app/object_usecase.rs`

The implementation adds static label constants and a small helper for recording
`s3_handler` stage duration when `GET_STAGE_METRICS_ENABLED` is enabled. The
existing execution order is preserved:

1. `finalize_get_object_strategy`
2. `build_get_object_body`
3. `build_get_object_checksums`
4. `resolve_put_object_expiration`
5. `filter_object_metadata`
6. `GetObjectOutput` construction

## Verification

Commands:

```bash
cargo fmt --all --check
cargo check -p rustfs
cargo test -p rustfs --lib build_get_object_output_context_returns_content_disposition
cargo test -p rustfs-io-metrics record_get_object --lib
cargo build -p rustfs --bins --release
RUSTFS_OBS_ENDPOINT=http://localhost:4318 RUSTFS_OBS_METER_INTERVAL=1 \
  bash scripts/run_get_codec_streaming_smoke.sh \
    --mode legacy \
    --sizes 1KiB,4KiB,100KiB,1MiB \
    --concurrency 16 \
    --duration 10s \
    --rounds 2 \
    --round-cooldown-secs 20 \
    --retry-per-round 1 \
    --skip-build \
    --diagnostic-metrics \
    --diagnostic-obs-endpoint http://localhost:4318 \
    --diagnostic-obs-meter-interval 1 \
    --handoff-attribution \
    --out-dir target/bench/issue784-s3-output-stage-breakdown-20260629
```

The first benchmark attempt failed inside the sandbox while starting the local
RustFS runtime with `Operation not permitted (os error 1)`. The same command was
rerun with elevated local-service permission and completed successfully.

Artifacts:

- `target/bench/issue784-s3-output-stage-breakdown-20260629/metrics_summary.csv`
- `target/bench/issue784-s3-output-stage-breakdown-20260629/legacy/warp/median_summary.csv`
- `target/bench/issue784-s3-output-stage-breakdown-20260629/legacy/service-metrics/after.prom`
- `target/bench/issue784-s3-output-stage-breakdown-20260629/legacy/compat/compat_summary.csv`

Compatibility probe:

- `body_sha256_match=true`
- `content_length_match=true`
- `etag_match=true`
- `content_range_match=true`
- `checksum_headers_match=true`
- `sse_headers_match=true`
- `status_code_match=true`
- `error_count=0`

## Benchmark Summary

Median GET results, release build, legacy path, concurrency 16, 2 rounds, 20s
cooldown:

| Size | Req/s | p50 | p90 | p99 |
| --- | ---: | ---: | ---: | ---: |
| 1KiB | 2815.01 | 6.30ms | 7.65ms | 10.25ms |
| 4KiB | 2723.67 | 6.25ms | 7.50ms | 9.20ms |
| 100KiB | 2714.15 | 6.10ms | 7.05ms | 8.25ms |
| 1MiB | 1937.94 | 9.80ms | 12.45ms | 15.15ms |

Raw service metrics confirm the new sub-stage labels are emitted under
`path="s3_handler"`.

Average stage duration from `after.prom`:

| Stage | Count | Avg |
| --- | ---: | ---: |
| `store_reader_setup` | 202784 | 4.568462ms |
| `output_build` | 202784 | 1.328529ms |
| `body_build` | 202784 | 1.317643ms |
| `output_strategy` | 202784 | 0.005691ms |
| `request_context` | 202784 | 0.003361ms |
| `store_lookup` | 202784 | 0.001177ms |
| `versioning_lookup` | 202784 | 0.000842ms |
| `lifecycle_expiration` | 202784 | 0.000722ms |
| `response_handoff` | 202784 | 0.000516ms |
| `metadata_filter` | 202784 | 0.000296ms |
| `checksum_headers` | 202784 | 0.000206ms |

## Conclusion

The added metrics show that `body_build` accounts for almost all of
`s3_handler/output_build` in this run:

- `output_build`: 1.328529ms/request
- `body_build`: 1.317643ms/request

This means the next optimization should not prioritize tiny response-header
helpers first. `output_strategy`, `checksum_headers`, `lifecycle_expiration`,
`metadata_filter`, and `versioning_lookup` are measurable but microsecond-level
in this run.

Recommended next step:

1. Investigate why `build_get_object_body` costs about 1.3ms/request even when
   the response body is already available to the S3 layer.
2. Split `body_build` further into memory blob construction, streaming blob
   construction, reader wrapper setup, and any eager read path.
3. Revisit the buffered small GET output fast path only after the sub-breakdown
   identifies the exact body handoff cost. The previous broad output-strategy
   skip experiment was not enough because `output_strategy` itself is only about
   0.006ms/request here.
4. Keep Phase 1.1 versioning reuse as a correctness-preserving cleanup, but do
   not expect it to move throughput by itself based on the current
   `versioning_lookup` cost.
