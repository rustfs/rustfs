#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENHANCED_BENCH="${PROJECT_ROOT}/scripts/run_object_batch_bench_enhanced.sh"

SOURCE_DIR=""
OUT_DIR=""
ARCHIVE_PATH=""

usage() {
  cat <<'USAGE'
Usage:
  scripts/rebuild_pinned_paired_abba_artifact.sh --source-dir <paired-abba-dir> --out-dir <corrected-dir> [--archive <tar.gz>]

Rebuilds #1432 corrected CSVs from a pinned paired ABBA run's raw logs. The output records ACK contract comparability explicitly so RustFS relaxed ACK and MinIO strict ACK results are not treated as directly comparable.
USAGE
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --source-dir) SOURCE_DIR="$2"; shift 2 ;;
      --out-dir) OUT_DIR="$2"; shift 2 ;;
      --archive) ARCHIVE_PATH="$2"; shift 2 ;;
      -h|--help) usage; exit 0 ;;
      *) die "unknown arg: $1" ;;
    esac
  done
}

read_env_value() {
  local file="$1"
  local key="$2"
  awk -F= -v key="$key" '$1 == key {print substr($0, length(key) + 2); exit}' "$file" | sed -E "s/^'//; s/'$//"
}

resolve_log_path() {
  local log_path="$1"
  if [[ -f "$log_path" ]]; then
    printf '%s\n' "$log_path"
    return
  fi

  local relative_path="${log_path#"$SOURCE_DIR"/}"
  if [[ -f "$SOURCE_DIR/$relative_path" ]]; then
    printf '%s\n' "$SOURCE_DIR/$relative_path"
    return
  fi

  printf '%s\n' "$log_path"
}

median_from_file() {
  local file="$1"
  if [[ ! -s "$file" ]]; then
    echo "N/A"
    return
  fi

  sort -n "$file" | awk '
    { values[++n] = $1 }
    END {
      if (n == 0) {
        print "N/A"
      } else if (n % 2 == 1) {
        printf "%.6f\n", values[(n + 1) / 2]
      } else {
        printf "%.6f\n", (values[n / 2] + values[n / 2 + 1]) / 2
      }
    }'
}

pct_delta() {
  local new_value="$1"
  local old_value="$2"
  if [[ "$new_value" == "N/A" || "$old_value" == "N/A" ]]; then
    echo "N/A"
    return
  fi
  awk -v n="$new_value" -v o="$old_value" 'BEGIN { if (o == 0) print "N/A"; else printf "%.2f\n", ((n - o) / o) * 100 }'
}

validate_inputs() {
  [[ -n "$SOURCE_DIR" ]] || die "--source-dir is required"
  [[ -n "$OUT_DIR" ]] || die "--out-dir is required"
  [[ -d "$SOURCE_DIR" ]] || die "--source-dir does not exist: $SOURCE_DIR"
  [[ -f "$SOURCE_DIR/paired_manifest.env" ]] || die "missing paired_manifest.env under $SOURCE_DIR"
  [[ -f "$SOURCE_DIR/abba_schedule.csv" ]] || die "missing abba_schedule.csv under $SOURCE_DIR"
  [[ -x "$ENHANCED_BENCH" ]] || die "missing executable enhanced bench helper: $ENHANCED_BENCH"
}

write_corrected_rows() {
  local round_csv="$OUT_DIR/corrected_round_results.csv"
  echo "leg,product,ack_contract,concurrency,size,tool,round,attempt,status,exit_code,throughput_human,throughput_bps,reqps,latency_human,latency_ms,req_p90_human,req_p90_ms,req_p99_human,req_p99_ms,log_file" > "$round_csv"

  while IFS=',' read -r leg product _ _ _ _ ack_contract leg_out_dir; do
    [[ "$leg" != "leg" ]] || continue
    local relative_leg_dir="${leg_out_dir#"$SOURCE_DIR"/}"
    local actual_leg_dir="$SOURCE_DIR/$relative_leg_dir"
    [[ -d "$actual_leg_dir" ]] || actual_leg_dir="$leg_out_dir"
    [[ -d "$actual_leg_dir" ]] || die "missing leg directory for $leg: $leg_out_dir"

    local cell_dir
    while IFS= read -r cell_dir; do
      local source_round_csv="$cell_dir/round_results.csv"
      [[ -f "$source_round_csv" ]] || continue
      tail -n +2 "$source_round_csv" | while IFS=',' read -r size tool round attempt concurrency status exit_code _ _ _ _ _ _ _ log_file _ _ _ _; do
        [[ -n "$size" ]] || continue
        local resolved_log metrics throughput_human throughput_bps reqps latency_human latency_ms req_p90_human req_p90_ms req_p99_human req_p99_ms
        resolved_log="$(resolve_log_path "$log_file")"
        if [[ "$status" == "ok" && -f "$resolved_log" ]]; then
          metrics="$("$ENHANCED_BENCH" --extract-metrics-from-log "$resolved_log" | tail -n 1)"
          throughput_human="$(echo "$metrics" | cut -d',' -f1)"
          throughput_bps="$(echo "$metrics" | cut -d',' -f2)"
          reqps="$(echo "$metrics" | cut -d',' -f3)"
          latency_human="$(echo "$metrics" | cut -d',' -f4)"
          latency_ms="$(echo "$metrics" | cut -d',' -f5)"
          req_p90_human="$(echo "$metrics" | cut -d',' -f6)"
          req_p90_ms="$(echo "$metrics" | cut -d',' -f7)"
          req_p99_human="$(echo "$metrics" | cut -d',' -f8)"
          req_p99_ms="$(echo "$metrics" | cut -d',' -f9)"
        else
          throughput_human="N/A"
          throughput_bps="N/A"
          reqps="N/A"
          latency_human="N/A"
          latency_ms="N/A"
          req_p90_human="N/A"
          req_p90_ms="N/A"
          req_p99_human="N/A"
          req_p99_ms="N/A"
        fi
        echo "$leg,$product,$ack_contract,$concurrency,$size,$tool,$round,$attempt,$status,$exit_code,$throughput_human,$throughput_bps,$reqps,$latency_human,$latency_ms,$req_p90_human,$req_p90_ms,$req_p99_human,$req_p99_ms,$resolved_log" >> "$round_csv"
      done
    done < <(find "$actual_leg_dir" -maxdepth 1 -type d -name 'c*' | sort)
  done < "$SOURCE_DIR/abba_schedule.csv"
}

write_summary_rows() {
  local round_csv="$OUT_DIR/corrected_round_results.csv"
  local summary_csv="$OUT_DIR/corrected_ack_contract_summary.csv"
  local tmp_dir="$OUT_DIR/.summary-tmp"
  mkdir -p "$tmp_dir"

  echo "product,ack_contract,concurrency,size,ok_rounds,failed_rounds,median_throughput_bps,median_reqps,median_latency_ms,median_req_p90_ms,median_req_p99_ms" > "$summary_csv"

  tail -n +2 "$round_csv" | awk -F',' '{print $2","$3","$4","$5}' | sort -u | while IFS=',' read -r product ack_contract concurrency size; do
    local safe_key ok_rounds failed_rounds throughput_values reqps_values latency_values p90_values p99_values
    safe_key="$(printf '%s_%s_%s_%s' "$product" "$ack_contract" "$concurrency" "$size" | tr -c '[:alnum:]_.-' '_')"
    throughput_values="$tmp_dir/${safe_key}.throughput"
    reqps_values="$tmp_dir/${safe_key}.reqps"
    latency_values="$tmp_dir/${safe_key}.latency"
    p90_values="$tmp_dir/${safe_key}.p90"
    p99_values="$tmp_dir/${safe_key}.p99"
    : > "$throughput_values"
    : > "$reqps_values"
    : > "$latency_values"
    : > "$p90_values"
    : > "$p99_values"

    ok_rounds="$(awk -F',' -v p="$product" -v a="$ack_contract" -v c="$concurrency" -v s="$size" 'NR>1 && $2==p && $3==a && $4==c && $5==s && $9=="ok" {count++} END{print count+0}' "$round_csv")"
    failed_rounds="$(awk -F',' -v p="$product" -v a="$ack_contract" -v c="$concurrency" -v s="$size" 'NR>1 && $2==p && $3==a && $4==c && $5==s && $9!="ok" {count++} END{print count+0}' "$round_csv")"
    awk -F',' -v p="$product" -v a="$ack_contract" -v c="$concurrency" -v s="$size" 'NR>1 && $2==p && $3==a && $4==c && $5==s && $9=="ok" && $12!="N/A" {print $12}' "$round_csv" > "$throughput_values"
    awk -F',' -v p="$product" -v a="$ack_contract" -v c="$concurrency" -v s="$size" 'NR>1 && $2==p && $3==a && $4==c && $5==s && $9=="ok" && $13!="N/A" {print $13}' "$round_csv" > "$reqps_values"
    awk -F',' -v p="$product" -v a="$ack_contract" -v c="$concurrency" -v s="$size" 'NR>1 && $2==p && $3==a && $4==c && $5==s && $9=="ok" && $15!="N/A" {print $15}' "$round_csv" > "$latency_values"
    awk -F',' -v p="$product" -v a="$ack_contract" -v c="$concurrency" -v s="$size" 'NR>1 && $2==p && $3==a && $4==c && $5==s && $9=="ok" && $17!="N/A" {print $17}' "$round_csv" > "$p90_values"
    awk -F',' -v p="$product" -v a="$ack_contract" -v c="$concurrency" -v s="$size" 'NR>1 && $2==p && $3==a && $4==c && $5==s && $9=="ok" && $19!="N/A" {print $19}' "$round_csv" > "$p99_values"

    echo "$product,$ack_contract,$concurrency,$size,$ok_rounds,$failed_rounds,$(median_from_file "$throughput_values"),$(median_from_file "$reqps_values"),$(median_from_file "$latency_values"),$(median_from_file "$p90_values"),$(median_from_file "$p99_values")" >> "$summary_csv"
  done
}

write_comparison_rows() {
  local summary_csv="$OUT_DIR/corrected_ack_contract_summary.csv"
  local comparison_csv="$OUT_DIR/corrected_product_comparison.csv"
  echo "ack_comparable,rustfs_ack_contract,minio_ack_contract,concurrency,size,rustfs_median_throughput_bps,minio_median_throughput_bps,delta_throughput_pct,rustfs_median_reqps,minio_median_reqps,delta_reqps_pct" > "$comparison_csv"

  awk -F',' 'NR>1 {print $3","$4}' "$summary_csv" | sort -u | while IFS=',' read -r concurrency size; do
    local rustfs_rows minio_rows
    rustfs_rows="$(awk -F',' -v c="$concurrency" -v s="$size" 'NR>1 && $1=="rustfs" && $3==c && $4==s {print}' "$summary_csv")"
    minio_rows="$(awk -F',' -v c="$concurrency" -v s="$size" 'NR>1 && $1=="minio" && $3==c && $4==s {print}' "$summary_csv")"
    [[ -n "$rustfs_rows" && -n "$minio_rows" ]] || continue
    while IFS=',' read -r _ rustfs_ack _ _ _ _ rustfs_thr rustfs_reqps _ _ _; do
      while IFS=',' read -r _ minio_ack _ _ _ _ minio_thr minio_reqps _ _ _; do
        local comparable delta_thr delta_reqps
        if [[ "$rustfs_ack" == "$minio_ack" ]]; then
          comparable="true"
        else
          comparable="false"
        fi
        delta_thr="$(pct_delta "$rustfs_thr" "$minio_thr")"
        delta_reqps="$(pct_delta "$rustfs_reqps" "$minio_reqps")"
        echo "$comparable,$rustfs_ack,$minio_ack,$concurrency,$size,$rustfs_thr,$minio_thr,$delta_thr,$rustfs_reqps,$minio_reqps,$delta_reqps" >> "$comparison_csv"
      done <<< "$minio_rows"
    done <<< "$rustfs_rows"
  done
}

write_manifest_and_checksums() {
  cp "$SOURCE_DIR/paired_manifest.env" "$OUT_DIR/source_paired_manifest.env"
  cp "$SOURCE_DIR/abba_schedule.csv" "$OUT_DIR/source_abba_schedule.csv"

  local manifest="$OUT_DIR/corrected_artifact_manifest.env"
  {
    printf 'created_utc=%q\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf 'benchmark_issue=%q\n' "rustfs/backlog#1432"
    printf 'source_dir=%q\n' "$SOURCE_DIR"
    printf 'parser_revision=%q\n' "$(git -C "$PROJECT_ROOT" rev-parse HEAD)"
    printf 'artifact_format=%q\n' "corrected-pinned-paired-abba-v1"
    printf 'ack_comparability_rule=%q\n' "Only rows with ack_comparable=true are direct RustFS/MinIO ACK-contract comparisons."
    printf 'source_rustfs_ack_contract=%q\n' "$(read_env_value "$SOURCE_DIR/paired_manifest.env" rustfs_ack_contract)"
    printf 'source_minio_ack_contract=%q\n' "$(read_env_value "$SOURCE_DIR/paired_manifest.env" minio_ack_contract)"
  } > "$manifest"

  (
    cd "$OUT_DIR"
    find . -type f ! -name sha256sums.txt | LC_ALL=C sort | while IFS= read -r file; do
      shasum -a 256 "${file#./}"
    done > sha256sums.txt
  )
}

copy_source_evidence() {
  local source_evidence_dir="$OUT_DIR/source_evidence"
  mkdir -p "$source_evidence_dir"

  find "$SOURCE_DIR" \( -path '*/logs/*' -o -name round_results.csv \) -type f | while IFS= read -r source_file; do
    local relative_path target_file
    relative_path="${source_file#"$SOURCE_DIR"/}"
    target_file="$source_evidence_dir/$relative_path"
    mkdir -p "$(dirname "$target_file")"
    cp "$source_file" "$target_file"
  done
}

create_archive() {
  [[ -n "$ARCHIVE_PATH" ]] || return
  local archive_dir archive_name
  archive_dir="$(dirname "$ARCHIVE_PATH")"
  archive_name="$(basename "$ARCHIVE_PATH")"
  mkdir -p "$archive_dir"
  tar -C "$OUT_DIR" -czf "$archive_dir/$archive_name" .
  shasum -a 256 "$archive_dir/$archive_name" > "$archive_dir/$archive_name.sha256"
}

main() {
  parse_args "$@"
  validate_inputs
  mkdir -p "$OUT_DIR"
  write_corrected_rows
  write_summary_rows
  write_comparison_rows
  rm -rf "$OUT_DIR/.summary-tmp"
  copy_source_evidence
  write_manifest_and_checksums
  create_archive
  echo "corrected artifact root: $OUT_DIR"
  if [[ -n "$ARCHIVE_PATH" ]]; then
    echo "corrected artifact archive: $ARCHIVE_PATH"
  fi
}

main "$@"
