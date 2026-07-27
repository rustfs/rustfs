#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNNER="${SCRIPT_DIR}/run_get_1mib_abba_stage_metrics.sh"
TMP_DIR="$(mktemp -d)"
OUT_DIR="${TMP_DIR}/run"

cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

"$RUNNER" \
  --outer-reader-stream-buffer-sizes 65536,1048576 \
  --profile-orders normal,reverse \
  --concurrency 1 \
  --duration 1s \
  --rounds 1 \
  --retry-per-round 1 \
  --round-cooldown-secs 0 \
  --out-dir "$OUT_DIR" \
  --warp-bin true \
  --compressed-fallback-probe \
  --skip-build \
  --dry-run >/dev/null

rg -qx 'issue=rustfs/backlog#1434' "${OUT_DIR}/manifest.env"
rg -qx 'exact_size=1MiB' "${OUT_DIR}/manifest.env"
rg -qx 'exact_size_bytes=1048576' "${OUT_DIR}/manifest.env"
rg -qx 'read_path_profiles=legacy,codec-legacy' "${OUT_DIR}/manifest.env"
rg -qx 'abba_profile_orders=normal,reverse' "${OUT_DIR}/manifest.env"
rg -qx 'outer_reader_stream_buffer_sizes=65536,1048576' "${OUT_DIR}/manifest.env"
rg -qx 'outer_reader_stream_env=RUSTFS_GET_READER_STREAM_BUFFER_SIZE' "${OUT_DIR}/manifest.env"
rg -qx 'inner_legacy_duplex_buffer_policy=adaptive_duplex_buffer_size' "${OUT_DIR}/manifest.env"
rg -qx 'exact_1mib_legacy_duplex_buffer_bytes=65536' "${OUT_DIR}/manifest.env"
rg -qx 'handoff_attribution=true' "${OUT_DIR}/manifest.env"
rg -qx 'diagnostic_metrics=true' "${OUT_DIR}/manifest.env"
rg -qx 'diagnostic_obs_endpoint=http://127.0.0.1:4318' "${OUT_DIR}/manifest.env"
rg -qx 'diagnostic_obs_metric_endpoint=http://127.0.0.1:4318/v1/metrics' "${OUT_DIR}/manifest.env"
rg -qx 'diagnostic_obs_meter_interval=1' "${OUT_DIR}/manifest.env"
rg -qx 'compressed_fallback_probe=true' "${OUT_DIR}/manifest.env"
rg -qx 'performance_conclusion=not_encoded_by_harness_collect_raw_abba_stage_metrics_first' "${OUT_DIR}/manifest.env"
rg -Fq '("service.name", "service_name", "job", "otel_scope_name")' "${SCRIPT_DIR}/run_get_codec_streaming_smoke.sh"
rg -Fq '("service_name", "service.name", "job", "otel_scope_name")' "${SCRIPT_DIR}/run_get_codec_streaming_smoke.sh"
rg -Fq 'compressed_size = max(object_size, codec_min_size, 128 * 1024)' "${SCRIPT_DIR}/run_get_codec_streaming_smoke.sh"
rg -Fq 'encrypted_probe_body = payload(max(object_size, codec_min_size, 1))' "${SCRIPT_DIR}/run_get_codec_streaming_smoke.sh"

matrix_rows="$(awk -F',' 'NR > 1 { count++ } END { print count + 0 }' "${OUT_DIR}/abba_matrix.csv")"
if [[ "$matrix_rows" != "4" ]]; then
  echo "expected 4 ABBA matrix rows, got ${matrix_rows}" >&2
  exit 1
fi

for outer_size in 65536 1048576; do
  for profile_order in normal reverse; do
    cell_dir="${OUT_DIR}/${profile_order}-outer-${outer_size}"
    rg -qx "profile_order=${profile_order}" "${cell_dir}/manifest.env"
    rg -qx 'sizes=1MiB' "${cell_dir}/manifest.env"
    rg -qx 'mode=both' "${cell_dir}/manifest.env"
    rg -qx 'codec_engines=legacy' "${cell_dir}/manifest.env"
    rg -qx 'codec_min_size=1048576' "${cell_dir}/manifest.env"
    rg -qx 'output_handoff_attribution=true' "${cell_dir}/manifest.env"
    rg -qx 'diagnostic_metrics_enabled=true' "${cell_dir}/manifest.env"
    rg -qx 'dry_run=true' "${cell_dir}/manifest.env"

    if [[ "$profile_order" == "normal" ]]; then
      rg -qx 'profiles=legacy,codec-legacy' "${cell_dir}/manifest.env"
      rg -q "^normal,${outer_size},.*,legacy;codec-legacy,legacy>codec-legacy,.*,ok$" "${OUT_DIR}/abba_matrix.csv"
    else
      rg -qx 'profiles=codec-legacy,legacy' "${cell_dir}/manifest.env"
      rg -q "^reverse,${outer_size},.*,legacy;codec-legacy,codec-legacy>legacy,.*,ok$" "${OUT_DIR}/abba_matrix.csv"
    fi

    for profile in legacy codec-legacy; do
      profile_manifest="${cell_dir}/${profile}/manifest.env"
      rg -qx 'sizes=1MiB' "$profile_manifest"
      rg -qx 'RUSTFS_GET_OUTPUT_HANDOFF_ATTRIBUTION_ENABLE=true' "$profile_manifest"
      rg -qx 'RUSTFS_OBS_METRICS_EXPORT_ENABLED=true' "$profile_manifest"
      rg -qx 'RUSTFS_OBS_ENDPOINT=http://127.0.0.1:4318' "$profile_manifest"
      rg -qx 'RUSTFS_OBS_METRIC_ENDPOINT=http://127.0.0.1:4318/v1/metrics' "$profile_manifest"
      rg -qx 'RUSTFS_GET_CODEC_STREAMING_MIN_SIZE=1048576' "$profile_manifest"
      rg -qx 'RUSTFS_COMPRESSION_ENABLED=true' "$profile_manifest"
      rg -qx 'RUSTFS_COMPRESSION_EXTENSIONS=.compressed-probe.txt' "$profile_manifest"
      rg -qx 'RUSTFS_COMPRESSION_MIME_TYPES=text/plain' "$profile_manifest"
      test -f "${cell_dir}/${profile}/service_metrics_round_summary.csv"
      test -f "${cell_dir}/${profile}/service_metrics_stage_distribution.csv"
      test -f "${cell_dir}/${profile}/service_metrics_round_percentiles.csv"
    done
  done
done

if "$RUNNER" --outer-reader-stream-buffer-sizes 0 --dry-run >/dev/null 2>&1; then
  echo "expected zero outer ReaderStream buffer to be rejected" >&2
  exit 1
fi

if "$RUNNER" --profile-orders normal,sideways --dry-run >/dev/null 2>&1; then
  echo "expected invalid profile order to be rejected" >&2
  exit 1
fi
