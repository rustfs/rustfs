#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
SCRIPT="$ROOT_DIR/scripts/run_scanner_validation_harness.sh"
TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

BIN_DIR="$TMP_DIR/bin"
OUT_DIR="$TMP_DIR/out"
mkdir -p "$BIN_DIR"

cat >"$BIN_DIR/mc" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' "$*" >>"${MC_LOG:?}"
if [[ "$*" == "admin config get rustfs-local scanner" ]]; then
  printf 'scanner delay="30" max_wait="15"\n'
elif [[ "$*" == "admin config get rustfs-local heal" ]]; then
  printf 'heal bitrot_cycle="2592000"\n'
else
  printf 'unexpected mc args: %s\n' "$*" >&2
  exit 1
fi
STUB

cat >"$BIN_DIR/awscurl" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' "$*" >>"${AWSCURL_LOG:?}"
cat <<'JSON'
{
  "runtime_config": {
    "delay": { "value": 30, "source": "config" }
  },
  "metrics": {
    "pacing_pressure": {
      "primary_pressure": "active_scans"
    },
    "current_cycle_objects_scanned": 5,
    "current_cycle_directories_scanned": 2,
    "last_cycle_result": "success",
    "last_cycle_partial_reason": "",
    "last_cycle_partial_source": "",
    "lifecycle_transition": {
      "scanner_missed": 0
    },
    "source_work": [
      { "source": "usage", "missed": 0 },
      { "source": "lifecycle", "missed": 0 }
    ]
  }
}
JSON
STUB

cat >"$BIN_DIR/jq" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail

if [[ "$1" == "." ]]; then
  cat
  exit 0
fi

if [[ "$1" == "-r" ]]; then
  while [[ $# -gt 0 ]]; do
    if [[ "$1" == "--arg" && "${2:-}" == "ts" ]]; then
      printf '"%s","active_scans",5,2,"success","","",0,0\n' "$3"
      exit 0
    fi
    shift
  done
fi

printf 'unexpected jq args: %s\n' "$*" >&2
exit 1
STUB

chmod +x "$BIN_DIR/mc" "$BIN_DIR/awscurl" "$BIN_DIR/jq"

mc_log="$TMP_DIR/mc.log"
awscurl_log="$TMP_DIR/awscurl.log"
MC_LOG="$mc_log" AWSCURL_LOG="$awscurl_log" PATH="$BIN_DIR:$PATH" "$SCRIPT" \
  --alias rustfs-local \
  --endpoint http://127.0.0.1:9000 \
  --access-key rustfsadmin \
  --secret-key rustfsadmin \
  --deployment single-disk \
  --workload-label small-object-idle \
  --samples 2 \
  --interval-secs 0 \
  --out-dir "$OUT_DIR" \
  --skip-host-telemetry

test -s "$OUT_DIR/scanner-config.before.txt"
test -s "$OUT_DIR/heal-config.before.txt"
grep -q 'scanner delay="30" max_wait="15"' "$OUT_DIR/scanner-config.before.txt"
grep -q 'heal bitrot_cycle="2592000"' "$OUT_DIR/heal-config.before.txt"

status_count=$(find "$OUT_DIR/status" -type f -name 'scanner-status.*.json' | wc -l | tr -d ' ')
if [[ "$status_count" != "2" ]]; then
  echo "Expected 2 scanner status snapshots, got $status_count" >&2
  exit 1
fi

test -s "$OUT_DIR/scanner-summary.csv"
if [[ "$(wc -l <"$OUT_DIR/scanner-summary.csv" | tr -d ' ')" != "3" ]]; then
  echo "Expected scanner summary header plus 2 rows" >&2
  exit 1
fi

grep -q '^deployment=single-disk$' "$OUT_DIR/run-metadata.env"
grep -q '^workload_label=small-object-idle$' "$OUT_DIR/run-metadata.env"
grep -q 'admin config get rustfs-local scanner' "$mc_log"
grep -q 'admin config get rustfs-local heal' "$mc_log"
grep -q -- '--request GET' "$awscurl_log"
grep -q -- 'http://127.0.0.1:9000/rustfs/admin/v3/scanner/status' "$awscurl_log"

missing_args_log="$TMP_DIR/missing-args.log"
if PATH="$BIN_DIR:$PATH" "$SCRIPT" --alias rustfs-local >"$missing_args_log" 2>&1; then
  echo "Expected missing required arguments to fail" >&2
  exit 1
fi

grep -q -- '--endpoint, --access-key, and --secret-key are required' "$missing_args_log"

missing_value_log="$TMP_DIR/missing-value.log"
if PATH="$BIN_DIR:$PATH" "$SCRIPT" --alias >"$missing_value_log" 2>&1; then
  echo "Expected --alias without a value to fail" >&2
  exit 1
fi

grep -q -- 'missing value for --alias' "$missing_value_log"
