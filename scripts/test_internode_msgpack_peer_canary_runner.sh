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
if [[ "${1:-}" == "info" || "${1:-} ${2:-}" == "ps -aq" || "${1:-} ${2:-}" == "volume ls" ]]; then
  exit 0
fi
if [[ "${1:-} ${2:-}" == "image inspect" ]]; then
  image="${!#}"
  if [[ "$image" == "$FAKE_CANDIDATE_IMAGE" ]]; then
    revision="${FAKE_CANDIDATE_REVISION_OVERRIDE:-$FAKE_CANDIDATE_REVISION}"
    printf '%s|%s\n' "$FAKE_CANDIDATE_IMAGE_ID" "$revision"
  else
    image_id="$FAKE_BASELINE_IMAGE_ID"
    [[ "${FAKE_SAME_IMAGE_ID:-false}" == "true" ]] && image_id="$FAKE_CANDIDATE_IMAGE_ID"
    printf '%s|%s\n' "$image_id" "$FAKE_BASELINE_REVISION"
  fi
  exit 0
fi
if [[ "${1:-}" == "compose" ]]; then
  for arg in "$@"; do
    if [[ "$arg" == "up" ]]; then
      for path in "$@"; do
        if [[ "$path" == */compose.override.yml ]]; then
          basename "$(dirname "$path")" >"$FAKE_PHASE_STATE"
        fi
      done
      exit 0
    fi
  done
  for ((i = 1; i <= $#; i++)); do
    if [[ "${!i}" == "ps" ]]; then
      node="${!#}"
      printf 'container-%s\n' "$node"
      exit 0
    fi
  done
  exit 0
fi
if [[ "${1:-}" == "inspect" ]]; then
  node="${!#}"
  node="${node#container-}"
  phase="$(cat "$FAKE_PHASE_STATE")"
  old=false
  [[ "$phase" == "mixed-old-peer" && "$node" == "node4" ]] && old=true
  [[ "$phase" == "rollback-old-peer" && "$node" == "node2" ]] && old=true
  if [[ "${FAKE_PHASE_IMAGE_MISMATCH:-false}" == "true" && "$phase" == "mixed-old-peer" && "$node" == "node4" ]]; then
    old=false
  fi
  if [[ "$old" == "true" ]]; then
    printf '%s|%s|%s\n' "$FAKE_BASELINE_IMAGE_ID" "$FAKE_BASELINE_IMAGE" "$FAKE_BASELINE_REVISION"
  else
    printf '%s|%s|%s\n' "$FAKE_CANDIDATE_IMAGE_ID" "$FAKE_CANDIDATE_IMAGE" "$FAKE_CANDIDATE_REVISION"
  fi
  exit 0
fi
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
  [[ "$query" == *"rustfs_cluster_id=\"${FAKE_CLUSTER_ID}\""* ]]
  index=0
  [[ -f "$FAKE_DECISION_INDEX" ]] && index="$(cat "$FAKE_DECISION_INDEX")"
  case "$index" in
    0) rows='[{"metric":{"decision":"msgpack_only","reason":"peer_capable","rustfs_cluster_id":"'"$FAKE_CLUSTER_ID"'"},"value":['"$at"',"4"]},{"metric":{"decision":"dual_write","reason":"peer_unknown","rustfs_cluster_id":"'"$FAKE_CLUSTER_ID"'"},"value":['"$at"',"2"]}]' ;;
    1) rows='[{"metric":{"decision":"msgpack_only","reason":"peer_capable","rustfs_cluster_id":"'"$FAKE_CLUSTER_ID"'"},"value":['"$at"',"8"]}]' ;;
    *) rows='[{"metric":{"decision":"msgpack_only","reason":"peer_capable","rustfs_cluster_id":"'"$FAKE_CLUSTER_ID"'"},"value":['"$at"',"5"]},{"metric":{"decision":"dual_write","reason":"peer_revoked","rustfs_cluster_id":"'"$FAKE_CLUSTER_ID"'"},"value":['"$at"',"3"]}]' ;;
  esac
  printf '%s\n' "$((index + 1))" >"$FAKE_DECISION_INDEX"
  printf '{"status":"success","data":{"resultType":"vector","result":%s}}\n' "$rows"
  exit 0
fi
if [[ "$query" == "vector(0)" ]]; then
  printf '{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[%s,"0"]}]}}\n' "$at"
  exit 0
fi
value=0
[[ "$query" == *"rustfs_cluster_id=\"${FAKE_CLUSTER_ID}\""* ]]
if [[ "${FAKE_NONZERO_ERRORS:-false}" == "true" && "$query" == *decode_error_total* ]]; then
  value=1
fi
printf '{"status":"success","data":{"resultType":"vector","result":[{"metric":{"rustfs_cluster_id":"%s"},"value":[%s,"%s"]}]}}\n' "$FAKE_CLUSTER_ID" "$at" "$value"
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
CANDIDATE_IMAGE_ID="sha256:$(printf '1%.0s' {1..64})"
BASELINE_IMAGE_ID="sha256:$(printf '2%.0s' {1..64})"

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
    FAKE_CLUSTER_ID="msgpack-${name}-cluster" \
    FAKE_PHASE_STATE="$TMP_DIR/$name.phase" \
    FAKE_CANDIDATE_IMAGE="rustfs/candidate:test" \
    FAKE_BASELINE_IMAGE="rustfs/baseline:test" \
    FAKE_CANDIDATE_IMAGE_ID="$CANDIDATE_IMAGE_ID" \
    FAKE_BASELINE_IMAGE_ID="$BASELINE_IMAGE_ID" \
    FAKE_CANDIDATE_REVISION="$CANDIDATE_REVISION" \
    FAKE_BASELINE_REVISION="$BASELINE_REVISION" \
    METRICS_SETTLE_SECS=0 \
    WORKLOAD_DURATION=1s \
    WORKLOAD_CONCURRENCY=2 \
    WORKLOAD_SIZES=4KiB \
    "$RUNNER" \
      --candidate-image rustfs/candidate:test \
      --baseline-image rustfs/baseline:test \
      --candidate-revision "$CANDIDATE_REVISION" \
      --baseline-revision "$BASELINE_REVISION" \
      --telemetry-cluster-id "msgpack-${name}-cluster" \
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

run_preflight_failure() {
  local name="$1" same_id="$2" revision_override="$3" phase_mismatch="$4"
  local out="$TMP_DIR/$name"
  : >"$TMP_DIR/docker.log"
  set +e
  PATH="$FAKE_BIN:$PATH" \
    FAKE_DOCKER_LOG="$TMP_DIR/docker.log" \
    FAKE_DECISION_INDEX="$TMP_DIR/decision.index" \
    FAKE_AWS_STATE="$TMP_DIR/aws.object" \
    FAKE_NONZERO_ERRORS=false \
    FAKE_CLUSTER_ID="msgpack-${name}-cluster" \
    FAKE_PHASE_STATE="$TMP_DIR/$name.phase" \
    FAKE_CANDIDATE_IMAGE="rustfs/candidate:test" \
    FAKE_BASELINE_IMAGE="rustfs/baseline:test" \
    FAKE_CANDIDATE_IMAGE_ID="$CANDIDATE_IMAGE_ID" \
    FAKE_BASELINE_IMAGE_ID="$BASELINE_IMAGE_ID" \
    FAKE_CANDIDATE_REVISION="$CANDIDATE_REVISION" \
    FAKE_BASELINE_REVISION="$BASELINE_REVISION" \
    FAKE_SAME_IMAGE_ID="$same_id" \
    FAKE_CANDIDATE_REVISION_OVERRIDE="$revision_override" \
    FAKE_PHASE_IMAGE_MISMATCH="$phase_mismatch" \
    METRICS_SETTLE_SECS=0 WORKLOAD_DURATION=1s WORKLOAD_CONCURRENCY=2 WORKLOAD_SIZES=4KiB \
    "$RUNNER" \
      --candidate-image rustfs/candidate:test \
      --baseline-image rustfs/baseline:test \
      --candidate-revision "$CANDIDATE_REVISION" \
      --baseline-revision "$BASELINE_REVISION" \
      --telemetry-cluster-id "msgpack-${name}-cluster" \
      --project-name "msgpack-${name}" \
      --workload-runner "$TMP_DIR/workload" \
      --out-dir "$out" --execute >"$TMP_DIR/$name.out" 2>"$TMP_DIR/$name.err"
  status=$?
  set -e
  [[ "$status" == "2" ]]
}

run_preflight_failure same-image true "" false
rg -q 'same immutable image' "$TMP_DIR/same-image.err"
run_preflight_failure wrong-revision false "cccccccccccccccccccccccccccccccccccccccc" false
rg -q 'candidate image revision mismatch' "$TMP_DIR/wrong-revision.err"
run_preflight_failure phase-mismatch false "" true
rg -q 'mixed-old-peer/node4 image ref mismatch' "$TMP_DIR/phase-mismatch.err"

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
