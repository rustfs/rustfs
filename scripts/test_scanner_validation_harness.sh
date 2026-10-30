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
if [[ "${CONFIG_EXIT_CODE:-0}" != "0" ]]; then
  exit "$CONFIG_EXIT_CODE"
fi
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

if [[ -n "${SCANNER_CAPTURE_CHILD_MODE:-}" && "$*" == */scanner/status* ]]; then
  exec "${SCANNER_TEST_PYTHON:?}" - <<'PY'
import json, os, pathlib, signal, subprocess, sys, time
root = pathlib.Path(os.environ["SCANNER_CAPTURE_CHILD_DIR"])
signal.signal(signal.SIGTERM, signal.SIG_IGN)
child = subprocess.Popen([sys.executable, "-c", "import pathlib,signal,sys,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); pathlib.Path(sys.argv[1]).touch(); time.sleep(120)", str(root / "ready")])
while not (root / "ready").exists():
    time.sleep(0.01)
(root / "pids.json").write_text(json.dumps({"owner": os.getppid(), "leader": os.getpid(), "child": child.pid}))
if os.environ["SCANNER_CAPTURE_CHILD_MODE"] == "failure":
    raise SystemExit(74)
time.sleep(120)
PY
fi

if [[ "${SCANNER_STATUS_EXIT_CODE:-0}" != "0" && "$*" == */scanner/status* ]]; then
  for attempt in {1..100}; do
    [[ -s "${IOSTAT_PID_FILE:?}" ]] && break
    sleep 0.05
  done
  test -s "$IOSTAT_PID_FILE"
  if [[ -n "${SCANNER_STATUS_WRITE_FAIL_FILE:-}" ]]; then
    rm -f "$SCANNER_STATUS_WRITE_FAIL_FILE"
    mkdir "$SCANNER_STATUS_WRITE_FAIL_FILE"
  fi
  exit "$SCANNER_STATUS_EXIT_CODE"
fi

printf '%s\n' "$*" >>"${AWSCURL_LOG:?}"
printf 'access-env-present=%s\n' "${AWS_ACCESS_KEY_ID:+yes}" >>"${AWSCURL_LOG:?}"
printf 'secret-env-present=%s\n' "${AWS_SECRET_ACCESS_KEY:+yes}" >>"${AWSCURL_LOG:?}"

url="${*: -1}"
if [[ "$url" == *"/rustfs/admin/v3/background-heal/status" ]]; then
  cat <<'JSON'
{
  "healQueueLength": 4,
  "healActiveTasks": 1,
  "healOperations": {
    "queueLength": 4,
    "activeTasks": 1,
    "queuedBySource": {
      "scanner": 2,
      "admin": 1,
      "autoHeal": 1,
      "internal": 0
    },
    "activeBySource": {
      "scanner": 1,
      "admin": 0,
      "autoHeal": 0,
      "internal": 0
    },
    "queuedByPriority": {
      "low": 2,
      "normal": 1,
      "high": 1,
      "urgent": 0
    },
    "activeByPriority": {
      "low": 1,
      "normal": 0,
      "high": 0,
      "urgent": 0
    }
  }
}
JSON
  exit 0
fi

if [[ "$url" == *"/rustfs/admin/v3/realtime?types=1&by-host=true&n=1" ]]; then
  if [[ -n "${SCANNER_CAPTURE_BARRIER:-}" ]]; then
    case "$url" in
      *node-a*) touch "$SCANNER_CAPTURE_BARRIER/node-a" ;;
      *node-b*) touch "$SCANNER_CAPTURE_BARRIER/node-b" ;;
    esac
    for attempt in {1..100}; do
      [[ -f "$SCANNER_CAPTURE_BARRIER/node-a" && -f "$SCANNER_CAPTURE_BARRIER/node-b" ]] && break
      sleep 0.05
    done
    if [[ ! -f "$SCANNER_CAPTURE_BARRIER/node-a" || ! -f "$SCANNER_CAPTURE_BARRIER/node-b" ]]; then
      echo "distributed requests were serialized" >&2
      exit 73
    fi
    if [[ "${SCANNER_CAPTURE_FAIL:-}" == true ]]; then
      if [[ "$url" == *node-a* ]]; then exit 74; fi
      sleep 0.1
      touch "$SCANNER_CAPTURE_BARRIER/node-b-finished"
    fi
  fi
  cat <<'JSON'
{"node":"node-a","metrics":{"scanner":{"maintenanceControl":{"primaryControl":"active"}}}}
JSON
  exit 0
fi

if [[ "$url" != *"/rustfs/admin/v3/scanner/status" ]]; then
  printf 'unexpected admin URL: %s\n' "$url" >&2
  exit 1
fi

cat <<'JSON'
{
  "runtime_config": {
    "delay": { "value": 30, "source": "config" }
  },
  "metrics": {
    "life_time_ops": {
      "scan_cycle": 9,
      "scan_bucket_drive": 11,
      "scan_object": 13,
      "save_usage": 5
    },
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
    "current_cycle_usage_saves": 2,
    "last_cycle_usage_saves": 3,
    "usage_freshness": {
      "dirty_pending_buckets": 1,
      "last_dirty_mark_unix_secs": 1800000001,
      "last_dirty_clear_unix_secs": 1800000002,
      "last_cycle_dirty_buckets": 1,
      "last_cycle_cleared_dirty_buckets": 1,
      "last_usage_save_unix_secs": 1800000003,
      "last_usage_save_result": "success",
      "last_usage_save_result_code": 1
    },
    "source_work": [
      { "source": "usage", "missed": 0 },
      { "source": "lifecycle", "missed": 0 }
    ]
  }
}
JSON
STUB

cat >"$BIN_DIR/pidof" <<'STUB'
#!/usr/bin/env bash
exit 1
STUB

cat >"$BIN_DIR/uname" <<'STUB'
#!/usr/bin/env bash
printf '%s\n' "${TEST_UNAME:-Linux}"
STUB

cat >"$BIN_DIR/iostat" <<'STUB'
#!/usr/bin/env bash
printf 'iostat args: %s\n' "$*"
if [[ -n "${IOSTAT_PID_FILE:-}" ]]; then
  printf '%s\n' "$$" >"$IOSTAT_PID_FILE"
  exec sleep 60
fi
exit "${IOSTAT_EXIT_CODE:-0}"
STUB

cat >"$BIN_DIR/mpstat" <<'STUB'
#!/usr/bin/env bash
printf 'mpstat sample\n'
STUB

cat >"$BIN_DIR/jq" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail

if [[ "$1" == "." ]]; then
  cat
  exit 0
fi

if [[ "$1" == "-s" ]]; then
  cat <<'JSON'
{
  "healOperations": {
    "queueLength": 8,
    "activeTasks": 2,
    "queuedBySource": {
      "scanner": 4,
      "admin": 2,
      "autoHeal": 2,
      "internal": 0
    }
  }
}
JSON
  exit 0
fi

if [[ "$1" == "-r" ]]; then
  while [[ $# -gt 0 ]]; do
    if [[ "$1" == "--arg" && "${2:-}" == "ts" ]]; then
      printf '"%s","active_scans",5,2,"success","","",0,0,2,3,1,1,1,"success",1800000003,9,11,13,5,8,2,4,2,2\n' "$3"
      exit 0
    fi
    shift
  done
fi

printf 'unexpected jq args: %s\n' "$*" >&2
exit 1
STUB

chmod +x "$BIN_DIR/uname" "$BIN_DIR/mc" "$BIN_DIR/awscurl" "$BIN_DIR/pidof" "$BIN_DIR/iostat" "$BIN_DIR/mpstat" "$BIN_DIR/jq"

mc_log="$TMP_DIR/mc.log"
awscurl_log="$TMP_DIR/awscurl.log"
RUSTFS_SECRET_KEY=rustfsadmin MC_LOG="$mc_log" AWSCURL_LOG="$awscurl_log" PATH="$BIN_DIR:$PATH" "$SCRIPT" \
  --alias rustfs-local \
  --endpoint http://127.0.0.1:9000 \
  --access-key rustfsadmin \
  --deployment single-disk \
  --workload-label small-object-idle \
  --metrics-endpoints http://node-a:9000,http://node-b:9000 \
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

heal_status_count=$(find "$OUT_DIR/heal" -type f -name 'background-heal-status.*.json' | wc -l | tr -d ' ')
if [[ "$heal_status_count" != "4" ]]; then
  echo "Expected 4 background heal status snapshots, got $heal_status_count" >&2
  exit 1
fi

metrics_count=$(find "$OUT_DIR/metrics" -type f -name 'admin-metrics.*.ndjson' | wc -l | tr -d ' ')
if [[ "$metrics_count" != "4" ]]; then
  echo "Expected 4 distributed admin metrics snapshots, got $metrics_count" >&2
  exit 1
fi

test -s "$OUT_DIR/scanner-summary.csv"
if [[ "$(wc -l <"$OUT_DIR/scanner-summary.csv" | tr -d ' ')" != "3" ]]; then
  echo "Expected scanner summary header plus 2 rows" >&2
  exit 1
fi
grep -q 'current_cycle_usage_saves,last_cycle_usage_saves,usage_dirty_pending_buckets' "$OUT_DIR/scanner-summary.csv"
grep -q 'life_time_scan_cycle,life_time_scan_bucket_drive,life_time_scan_object,life_time_save_usage' "$OUT_DIR/scanner-summary.csv"
grep -q 'heal_queue_length,heal_active_tasks,heal_scanner_queued,heal_admin_queued,heal_auto_heal_queued' "$OUT_DIR/scanner-summary.csv"
grep -q '"active_scans",5,2,"success","","",0,0,2,3,1,1,1,"success",1800000003,9,11,13,5,8,2,4,2,2' "$OUT_DIR/scanner-summary.csv"

grep -q '^deployment=single-disk$' "$OUT_DIR/run-metadata.env"
grep -q '^workload_label=small-object-idle$' "$OUT_DIR/run-metadata.env"
grep -q '^metrics_endpoints=http://node-a:9000,http://node-b:9000$' "$OUT_DIR/run-metadata.env"
grep -q '## Scanner Validation Report' "$OUT_DIR/scanner-validation-report.md"
grep -q 'Distributed admin metrics snapshots: 4' "$OUT_DIR/scanner-validation-report.md"
grep -q 'Background heal status snapshots: 4' "$OUT_DIR/scanner-validation-report.md"
grep -q 'Usage freshness summary:' "$OUT_DIR/scanner-validation-report.md"
grep -q 'admin config get rustfs-local scanner' "$mc_log"
grep -q 'admin config get rustfs-local heal' "$mc_log"
grep -q -- '--request GET' "$awscurl_log"
grep -q -- '--request POST' "$awscurl_log"
grep -q -- 'access-env-present=yes' "$awscurl_log"
grep -q -- 'secret-env-present=yes' "$awscurl_log"
if grep -q -- '--secret_key' "$awscurl_log"; then
  echo "Expected awscurl to receive the secret through the environment, not argv" >&2
  exit 1
fi
grep -q -- 'http://127.0.0.1:9000/rustfs/admin/v3/scanner/status' "$awscurl_log"
grep -q -- 'http://node-a:9000/rustfs/admin/v3/background-heal/status' "$awscurl_log"
grep -q -- 'http://node-b:9000/rustfs/admin/v3/background-heal/status' "$awscurl_log"
grep -q -- 'http://node-a:9000/rustfs/admin/v3/realtime?types=1&by-host=true&n=1' "$awscurl_log"
grep -q -- 'http://node-b:9000/rustfs/admin/v3/realtime?types=1&by-host=true&n=1' "$awscurl_log"

parallel_out="$TMP_DIR/parallel-out"
parallel_barrier="$TMP_DIR/parallel-barrier"
mkdir "$parallel_barrier"
SCANNER_CAPTURE_BARRIER="$parallel_barrier" RUSTFS_SECRET_KEY=rustfsadmin MC_LOG="$mc_log" \
  AWSCURL_LOG="$awscurl_log" PATH="$BIN_DIR:$PATH" "$SCRIPT" \
  --alias rustfs-local --endpoint http://127.0.0.1:9000 --access-key rustfsadmin \
  --metrics-endpoints http://node-a:9000,http://node-b:9000 \
  --samples 1 --interval-secs 0 --out-dir "$parallel_out" --skip-host-telemetry
"$ROOT_DIR/scripts/python_bin.sh" - "$parallel_out" <<'PY'
import json
from pathlib import Path
import sys

root = Path(sys.argv[1])
records = [(path, json.loads(path.read_text())) for path in (root / "requests").glob("*.timing.json")]
assert len(records) == 5, records
assert len({row["clock_id"] for _, row in records}) == 1
assert len({row["start_ns"] for _, row in records}) == 5
for _, row in records:
    assert 0 < row["start_ns"] < row["end_ns"]
    assert row["start_unix_ns"] > 0 and row["end_unix_ns"] > 0
    assert row["exit_status"] == 0
    assert row["synchronized_source_window"] is False
metrics = [row for path, row in records if path.name.startswith("admin-metrics.")]
assert len(metrics) == 2
assert max(row["start_ns"] for row in metrics) < min(row["end_ns"] for row in metrics)
PY

failed_parallel_out="$TMP_DIR/failed-parallel-out"
failed_parallel_barrier="$TMP_DIR/failed-parallel-barrier"
mkdir "$failed_parallel_barrier"
parallel_status=0
SCANNER_CAPTURE_BARRIER="$failed_parallel_barrier" SCANNER_CAPTURE_FAIL=true \
  RUSTFS_SECRET_KEY=rustfsadmin MC_LOG="$mc_log" AWSCURL_LOG="$awscurl_log" PATH="$BIN_DIR:$PATH" \
  "$SCRIPT" --alias rustfs-local --endpoint http://127.0.0.1:9000 --access-key rustfsadmin \
  --metrics-endpoints http://node-a:9000,http://node-b:9000 --samples 1 --interval-secs 0 \
  --out-dir "$failed_parallel_out" --skip-host-telemetry >"$TMP_DIR/failed-parallel.log" 2>&1 || parallel_status=$?
if [[ "$parallel_status" != 74 ]]; then
  cat "$TMP_DIR/failed-parallel.log" >&2
  echo "Expected failed parallel request status 74, got $parallel_status" >&2
  exit 1
fi
test -f "$failed_parallel_barrier/node-b-finished"
"$ROOT_DIR/scripts/python_bin.sh" - "$failed_parallel_out" <<'PY'
import json
from pathlib import Path
import sys

records = [json.loads(path.read_text()) for path in (Path(sys.argv[1]) / "requests").glob("*.timing.json")]
assert len(records) == 5, records
assert sorted(row["exit_status"] for row in records) == [0, 0, 0, 0, 74]
PY

RUSTFS_ACCESS_KEY=rustfsadmin RUSTFS_SECRET_KEY=rustfsadmin MC_LOG="$mc_log" AWSCURL_LOG="$awscurl_log" \
  SCANNER_TEST_PYTHON="$ROOT_DIR/scripts/python_bin.sh" PATH="$BIN_DIR:$PATH" \
  "$ROOT_DIR/scripts/python_bin.sh" - "$ROOT_DIR" "$TMP_DIR" <<'PY'
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from unittest.mock import patch

root, temporary = map(Path, sys.argv[1:])
sys.path.insert(0, str(root / "scripts"))
import scanner_abba as harness

def running(pid):
    result = subprocess.run(["ps", "-o", "stat=", "-p", str(pid)], capture_output=True, text=True)
    return result.returncode == 0 and result.stdout.strip() and not result.stdout.lstrip().startswith("Z")

failures = []
for mode in ("failure", "cancel"):
    folder = temporary / ("http-child-" + mode)
    folder.mkdir()
    os.environ.update(SCANNER_CAPTURE_CHILD_MODE=mode, SCANNER_CAPTURE_CHILD_DIR=str(folder))
    pids = {}
    resumers = []
    try:
        if mode == "failure":
            with (folder / "collector.log").open("w") as log:
                result = subprocess.run([str(root / "scripts/run_scanner_validation_harness.sh"),
                    "--alias", "rustfs-local", "--endpoint", "http://127.0.0.1:9000", "--samples", "1",
                    "--interval-secs", "0", "--out-dir", str(folder / "telemetry"), "--skip-host-telemetry"],
                    stdout=log, stderr=log, timeout=30)
            assert result.returncode == 74, result.returncode
        else:
            def cancel_measurement(*_args):
                deadline = time.monotonic() + 20
                while not (folder / "pids.json").exists():
                    assert time.monotonic() < deadline, "HTTP fixture did not become ready"
                    time.sleep(0.01)
                # Control scheduling so the outer cancellation deadline starts
                # before this request owner can begin its nested cleanup.
                owner = json.loads((folder / "pids.json").read_text())["owner"]
                os.kill(owner, signal.SIGSTOP)
                resumers.append(subprocess.Popen([sys.executable, "-c",
                    "import os,signal,sys,time; time.sleep(0.25); os.kill(int(sys.argv[1]),signal.SIGCONT)", str(owner)]))
                raise RuntimeError("cancel measurement fixture")
            prepared = {"collector": {"alias": "rustfs-local", "endpoint": "http://127.0.0.1:9000",
                                      "metrics_endpoints": "http://node-a:9000,http://node-b:9000"}}
            with patch.object(harness, "invoke", side_effect=cancel_measurement):
                try:
                    harness.collect_live(prepared, {"duration_seconds": 0}, folder / "request.json", Path("unused"))
                except RuntimeError as error:
                    assert str(error) == "cancel measurement fixture"
                else:
                    raise AssertionError("expected outer collect_live cancellation")
        pids = json.loads((folder / "pids.json").read_text())
        receipts = list((folder / "telemetry/requests").glob("scanner-status.*.timing.json"))
        receipt = json.loads(receipts[0].read_text()) if len(receipts) == 1 else None
        alive = {name: bool(running(pid)) for name, pid in pids.items()}
        evidence = {"mode": mode, "alive_after_cleanup": alive, "receipt": receipt}
        print(json.dumps(evidence), flush=True)
        if any(alive.values()) or receipt is None or receipt["exit_status"] != (74 if mode == "failure" else 143):
            failures.append(evidence)
    finally:
        for resumer in resumers:
            resumer.wait(timeout=2)
        if not pids and (folder / "pids.json").exists():
            pids = json.loads((folder / "pids.json").read_text())
        if pids:
            try:
                os.killpg(pids["leader"], signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass
        os.environ.pop("SCANNER_CAPTURE_CHILD_MODE", None)
        os.environ.pop("SCANNER_CAPTURE_CHILD_DIR", None)
assert not failures, failures
PY

missing_pid_out="$TMP_DIR/out-missing-pid"
RUSTFS_SECRET_KEY=rustfsadmin MC_LOG="$mc_log" AWSCURL_LOG="$awscurl_log" PATH="$BIN_DIR:$PATH" "$SCRIPT" \
  --alias rustfs-local \
  --endpoint http://127.0.0.1:9000 \
  --access-key rustfsadmin \
  --deployment single-disk \
  --workload-label missing-pid \
  --samples 1 \
  --interval-secs 1 \
  --out-dir "$missing_pid_out"

test -s "$missing_pid_out/status/scanner-status.1."*.json
test -s "$missing_pid_out/iostat.txt"
test -s "$missing_pid_out/mpstat.txt"

grep -q 'iostat args: -xz 1 1' "$missing_pid_out/iostat.txt"

for platform in Darwin Linux; do
  platform_out="$TMP_DIR/out-$platform"
  TEST_UNAME="$platform" RUSTFS_SECRET_KEY=rustfsadmin MC_LOG="$mc_log" AWSCURL_LOG="$awscurl_log" \
    PATH="$BIN_DIR:$PATH" "$SCRIPT" --alias rustfs-local --endpoint http://127.0.0.1:9000 \
    --access-key rustfsadmin --samples 1 --interval-secs 1 --out-dir "$platform_out"
  if [[ "$platform" == Darwin ]]; then
    grep -q 'iostat args: -d -w 1 -c 1' "$platform_out/iostat.txt"
  else
    grep -q 'iostat args: -xz 1 1' "$platform_out/iostat.txt"
  fi
  test "$(grep -cx $'iostat\t0' "$platform_out/host-telemetry-status.tsv")" -eq 1
done

failed_config_out="$TMP_DIR/out-failed-config"
mkdir -p "$failed_config_out"
cp "$TMP_DIR/out-Linux/iostat.txt" "$TMP_DIR/out-Linux/mpstat.txt" \
  "$TMP_DIR/out-Linux/host-telemetry-status.tsv" "$failed_config_out/"
printf 'previous pidstat sample\n' >"$failed_config_out/pidstat.txt"
config_status=0
CONFIG_EXIT_CODE=43 RUSTFS_SECRET_KEY=rustfsadmin MC_LOG="$mc_log" AWSCURL_LOG="$awscurl_log" \
  PATH="$BIN_DIR:$PATH" "$SCRIPT" --alias rustfs-local --endpoint http://127.0.0.1:9000 \
  --access-key rustfsadmin --samples 1 --interval-secs 1 --out-dir "$failed_config_out" \
  >"$TMP_DIR/failed-config.log" 2>&1 || config_status=$?
test "$config_status" -eq 43
if [[ "$(cat "$failed_config_out/host-telemetry-status.tsv")" != $'collector\texit_status' ]]; then
  echo "Expected output initialization to reset host telemetry before config capture" >&2
  exit 1
fi
test ! -e "$failed_config_out/pidstat.txt"
test ! -e "$failed_config_out/iostat.txt"
test ! -e "$failed_config_out/mpstat.txt"

for status_sink in file directory; do
  failed_request_out="$TMP_DIR/out-failed-request-$status_sink"
  collector_pid_file="$TMP_DIR/collector-$status_sink.pid"
  status_write_fail_file=""
  if [[ "$status_sink" == directory ]]; then
    status_write_fail_file="$failed_request_out/host-telemetry-status.tsv"
  fi
  request_status=0
  SCANNER_STATUS_EXIT_CODE=42 IOSTAT_PID_FILE="$collector_pid_file" \
    SCANNER_STATUS_WRITE_FAIL_FILE="$status_write_fail_file" \
    RUSTFS_SECRET_KEY=rustfsadmin MC_LOG="$mc_log" AWSCURL_LOG="$awscurl_log" \
    PATH="$BIN_DIR:$PATH" "$SCRIPT" --alias rustfs-local --endpoint http://127.0.0.1:9000 \
    --access-key rustfsadmin --samples 1 --interval-secs 1 --out-dir "$failed_request_out" \
    >"$TMP_DIR/failed-request-$status_sink.log" 2>&1 || request_status=$?
  test "$request_status" -eq 42
  collector_pid="$(cat "$collector_pid_file")"
  if kill -0 "$collector_pid" 2>/dev/null; then
    kill "$collector_pid" 2>/dev/null || true
    echo "Expected a failed status request to stop and reap its host collector" >&2
    exit 1
  fi
  if [[ "$status_sink" == file ]]; then
    grep -q $'iostat\t143' "$failed_request_out/host-telemetry-status.tsv"
  else
    test -d "$failed_request_out/host-telemetry-status.tsv"
  fi
  grep -q 'host telemetry collector iostat exited with status 143' "$TMP_DIR/failed-request-$status_sink.log"
done

# Reuse both populated OS outputs with collection disabled in different ways.
for platform in Darwin Linux; do
  platform_out="$TMP_DIR/out-$platform"
  if [[ "$platform" == Darwin ]]; then
    disabled_args=(--interval-secs 1 --skip-host-telemetry)
  else
    disabled_args=(--interval-secs 0)
  fi
  printf 'previous pidstat sample\n' >"$platform_out/pidstat.txt"
  TEST_UNAME="$platform" RUSTFS_SECRET_KEY=rustfsadmin MC_LOG="$mc_log" AWSCURL_LOG="$awscurl_log" \
    PATH="$BIN_DIR:$PATH" "$SCRIPT" --alias rustfs-local --endpoint http://127.0.0.1:9000 \
    --access-key rustfsadmin --samples 1 "${disabled_args[@]}" --out-dir "$platform_out"
  if [[ "$(cat "$platform_out/host-telemetry-status.tsv")" != $'collector\texit_status' ]]; then
    echo "Expected disabled collection to reset the previous run's collector statuses" >&2
    exit 1
  fi
  test ! -e "$platform_out/pidstat.txt"
  test ! -e "$platform_out/iostat.txt"
  test ! -e "$platform_out/mpstat.txt"
  test -s "$platform_out/scanner-summary.csv"
done

failed_collector_out="$TMP_DIR/out-failed-collector"
IOSTAT_EXIT_CODE=7 RUSTFS_SECRET_KEY=rustfsadmin MC_LOG="$mc_log" AWSCURL_LOG="$awscurl_log" \
  PATH="$BIN_DIR:$PATH" "$SCRIPT" --alias rustfs-local --endpoint http://127.0.0.1:9000 \
  --access-key rustfsadmin --samples 1 --interval-secs 1 --out-dir "$failed_collector_out" \
  2>"$TMP_DIR/collector-warning.txt"
grep -q $'iostat\t7' "$failed_collector_out/host-telemetry-status.tsv"
grep -q 'host telemetry collector iostat exited with status 7' "$TMP_DIR/collector-warning.txt"
test -s "$failed_collector_out/scanner-summary.csv"

missing_args_log="$TMP_DIR/missing-args.log"
if PATH="$BIN_DIR:$PATH" "$SCRIPT" --alias rustfs-local >"$missing_args_log" 2>&1; then
  echo "Expected missing required arguments to fail" >&2
  exit 1
fi

grep -q -- '--alias, --endpoint, and RUSTFS_ACCESS_KEY (or --access-key) are required' "$missing_args_log"

missing_secret_log="$TMP_DIR/missing-secret.log"
if PATH="$BIN_DIR:$PATH" "$SCRIPT" \
  --alias rustfs-local \
  --endpoint http://127.0.0.1:9000 \
  --access-key rustfsadmin >"$missing_secret_log" 2>&1; then
  echo "Expected missing RUSTFS_SECRET_KEY to fail" >&2
  exit 1
fi

grep -q -- 'RUSTFS_SECRET_KEY is required for scanner status requests' "$missing_secret_log"

invalid_secret_env_log="$TMP_DIR/invalid-secret-env.log"
if PATH="$BIN_DIR:$PATH" "$SCRIPT" \
  --alias rustfs-local \
  --endpoint http://127.0.0.1:9000 \
  --access-key rustfsadmin \
  --secret-key-env 'not-valid!' >"$invalid_secret_env_log" 2>&1; then
  echo "Expected invalid --secret-key-env to fail" >&2
  exit 1
fi

grep -q -- '--secret-key-env must be a valid environment variable name' "$invalid_secret_env_log"

missing_value_log="$TMP_DIR/missing-value.log"
if PATH="$BIN_DIR:$PATH" "$SCRIPT" --alias >"$missing_value_log" 2>&1; then
  echo "Expected --alias without a value to fail" >&2
  exit 1
fi

grep -q -- 'missing value for --alias' "$missing_value_log"

"$ROOT_DIR/scripts/python_bin.sh" "$ROOT_DIR/scripts/test_scanner_version_oracle.py"

secret_arg_log="$TMP_DIR/secret-arg.log"
if PATH="$BIN_DIR:$PATH" "$SCRIPT" --secret-key rustfsadmin >"$secret_arg_log" 2>&1; then
  echo "Expected --secret-key argv to be rejected" >&2
  exit 1
fi

grep -q -- 'unknown arg: --secret-key' "$secret_arg_log"

"$ROOT_DIR/scripts/python_bin.sh" "$ROOT_DIR/scripts/test_scanner_service.py"
"$ROOT_DIR/scripts/python_bin.sh" "$ROOT_DIR/scripts/test_scanner_process_receipt.py"
"$ROOT_DIR/scripts/python_bin.sh" "$ROOT_DIR/scripts/test_scanner_abba.py"
"$ROOT_DIR/scripts/python_bin.sh" "$ROOT_DIR/scripts/test_scanner_client_trace.py"
"$ROOT_DIR/scripts/python_bin.sh" "$ROOT_DIR/scripts/test_summarize_scanner_heal_perf.py"
"$ROOT_DIR/scripts/python_bin.sh" "$ROOT_DIR/scripts/test_scanner_otlp_window.py"
