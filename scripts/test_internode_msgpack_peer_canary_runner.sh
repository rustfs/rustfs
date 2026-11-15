#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUNNER="$PROJECT_ROOT/scripts/run_internode_msgpack_peer_canary.sh"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT
FAKE_BIN="$TMP_DIR/bin"
mkdir -p "$FAKE_BIN"

cat >"$FAKE_BIN/docker" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$FAKE_DOCKER_LOG"
exit 0
SH

cat >"$FAKE_BIN/curl" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
query=""
at="0"
for arg in "$@"; do
  case "$arg" in
    http://127.0.0.1:*/health) printf '{}\n'; exit 0 ;;
    query=*) query="${arg#query=}" ;;
    time=*) at="${arg#time=}" ;;
  esac
done
if [[ "$query" == *msgpack_send_decisions_total* ]]; then
  index=0
  [[ -f "$FAKE_DECISION_INDEX" ]] && index="$(cat "$FAKE_DECISION_INDEX")"
  case "$index" in
    0) rows='[{"metric":{"decision":"msgpack_only","reason":"peer_capable"},"value":['"$at"',"4"]},{"metric":{"decision":"dual_write","reason":"peer_unknown"},"value":['"$at"',"2"]}]' ;;
    1) rows='[{"metric":{"decision":"msgpack_only","reason":"peer_capable"},"value":['"$at"',"8"]}]' ;;
    *) rows='[{"metric":{"decision":"msgpack_only","reason":"peer_capable"},"value":['"$at"',"5"]},{"metric":{"decision":"dual_write","reason":"peer_revoked"},"value":['"$at"',"3"]}]' ;;
  esac
  printf '%s\n' "$((index + 1))" >"$FAKE_DECISION_INDEX"
  printf '{"status":"success","data":{"resultType":"vector","result":%s}}\n' "$rows"
  exit 0
fi
value=0
if [[ "${FAKE_NONZERO_ERRORS:-false}" == "true" && "$query" == *decode_error_total* ]]; then
  value=1
fi
printf '{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[%s,"%s"]}]}}\n' "$at" "$value"
SH

cat >"$FAKE_BIN/aws" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
operation=""
body=""
destination=""
for ((i = 1; i <= $#; i++)); do
  arg="${!i}"
  case "$arg" in
    head-bucket|create-bucket|put-object|get-object) operation="$arg" ;;
    --body) next=$((i + 1)); body="${!next}" ;;
  esac
done
case "$operation" in
  head-bucket|create-bucket) exit 0 ;;
  put-object) cp "$body" "$FAKE_AWS_STATE" ;;
  get-object) destination="${!#}"; cp "$FAKE_AWS_STATE" "$destination" ;;
  *) echo "unexpected fake aws invocation: $*" >&2; exit 1 ;;
esac
SH

cat >"$TMP_DIR/workload" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
out=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --out-dir) out="$2"; shift 2 ;;
    *) shift ;;
  esac
done
[[ -n "$out" ]]
mkdir -p "$out"
cat >"$out/summary.csv" <<'CSV'
size,tool,concurrency,status,throughput,requests_per_sec,avg_latency,error_count,log_file
4KiB,warp,2,ok,1 MiB/s,10 req/s,1 ms,0,fake.log
CSV
SH

chmod +x "$FAKE_BIN/docker" "$FAKE_BIN/curl" "$FAKE_BIN/aws" "$TMP_DIR/workload"

CANDIDATE_REVISION="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
BASELINE_REVISION="bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

plan="$TMP_DIR/plan.txt"
"$RUNNER" \
  --candidate-image rustfs/candidate:test \
  --baseline-image rustfs/baseline:test \
  --candidate-revision "$CANDIDATE_REVISION" \
  --baseline-revision "$BASELINE_REVISION" >"$plan"
rg -q '^phases=mixed-old-peer all-new rollback-old-peer$' "$plan"
rg -q '^execute=false$' "$plan"
if rg -q 'rustfssecret-local' "$plan"; then
  echo "plan leaked the default secret" >&2
  exit 1
fi

run_once() {
  local name="$1"
  local nonzero="$2"
  local out="$TMP_DIR/$name"
  : >"$TMP_DIR/docker.log"
  rm -f "$TMP_DIR/decision.index" "$TMP_DIR/aws.object"
  set +e
  PATH="$FAKE_BIN:$PATH" \
    FAKE_DOCKER_LOG="$TMP_DIR/docker.log" \
    FAKE_DECISION_INDEX="$TMP_DIR/decision.index" \
    FAKE_AWS_STATE="$TMP_DIR/aws.object" \
    FAKE_NONZERO_ERRORS="$nonzero" \
    METRICS_SETTLE_SECS=0 \
    WORKLOAD_DURATION=1s \
    WORKLOAD_CONCURRENCY=2 \
    WORKLOAD_SIZES=4KiB \
    "$RUNNER" \
      --candidate-image rustfs/candidate:test \
      --baseline-image rustfs/baseline:test \
      --candidate-revision "$CANDIDATE_REVISION" \
      --baseline-revision "$BASELINE_REVISION" \
      --project-name "msgpack-${name}" \
      --workload-runner "$TMP_DIR/workload" \
      --out-dir "$out" \
      --execute
  status=$?
  set -e
  printf '%s\n' "$status" >"$TMP_DIR/$name.status"
}

run_once pass false
[[ "$(cat "$TMP_DIR/pass.status")" == "0" ]]
python3 - "$TMP_DIR/pass/verification.json" <<'PY'
import json
import sys
data = json.load(open(sys.argv[1], encoding="utf-8"))
assert data["observed_pass"] is True
assert len(data["artifacts"]) == 18
PY
rg -q 'compose .*up -d --no-build --force-recreate node4' "$TMP_DIR/docker.log"
rg -q 'compose .*up -d --no-build --force-recreate node2' "$TMP_DIR/docker.log"
rg -q 'compose .*down --volumes --remove-orphans' "$TMP_DIR/docker.log"

run_once fail true
[[ "$(cat "$TMP_DIR/fail.status")" == "1" ]]
python3 - "$TMP_DIR/fail/verification.json" <<'PY'
import json
import sys
data = json.load(open(sys.argv[1], encoding="utf-8"))
assert data["observed_pass"] is False
assert any("decode_errors" in item for item in data["violations"])
PY

bash -n "$RUNNER"
echo "internode msgpack peer canary runner tests passed"
