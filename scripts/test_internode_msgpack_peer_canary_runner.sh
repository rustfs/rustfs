#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUNNER="$PROJECT_ROOT/scripts/run_internode_msgpack_peer_canary.sh"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT
FAKE_BIN="$TMP_DIR/bin"
mkdir -p "$FAKE_BIN"

cat >"$FAKE_BIN/docker" <<'PYTHON'
#!/usr/bin/env python3
import json
import os
from pathlib import Path
import re
import sys
args = sys.argv[1:]
with open(os.environ["FAKE_DOCKER_LOG"], "a") as log:
    log.write(" ".join(args) + "\n")
mode = os.environ.get("FAKE_IDENTITY_FAILURE", "")
if args[:2] == ["image", "inspect"]:
    char = "a" if "candidate" in args[-1] else "b"
    image = {"Id": "sha256:" + char * 64, "Config": {"Labels": {"vcs-ref": char * 40}}}
    if mode == "same" and char == "b":
        image["Id"] = "sha256:" + "a" * 64
    if mode == "revision" and char == "b":
        image["Config"]["Labels"]["vcs-ref"] = "c" * 40
    if mode in ("history", "history-revision", "history-noncanonical", "history-conflict"):
        image["Config"]["Labels"] = None
    image["Config"]["Env"] = ["RUSTFS_SECRET_KEY=identity-secret-sentinel"]
    print(json.dumps([image]))
elif args[:2] == ["image", "history"]:
    image_id = args[-1]
    revision = ("a" if image_id == "sha256:" + "a" * 64 else "b") * 40
    if mode == "history-revision" and image_id == "sha256:" + "b" * 64:
        revision = "c" * 40
    created_by = f"LABEL vcs-ref={revision}"
    if mode == "history-noncanonical" and image_id == "sha256:" + "b" * 64:
        print(json.dumps({"ID": image_id, "CreatedBy": "LABEL vcs-ref latest"}))
    if mode == "history-conflict" and image_id == "sha256:" + "b" * 64:
        created_by += " org.opencontainers.image.revision=" + "c" * 40
    print(json.dumps({"ID": image_id, "CreatedBy": created_by}))
elif args[:1] == ["compose"]:
    override = Path(args[args.index("-f", args.index("-f") + 1) + 1])
    if "ps" in args:
        print(str(override) + "@" + args[-1])
    elif "up" in args:
        text = override.read_text()
        assert 'OTEL_RESOURCE_ATTRIBUTES: "rustfs.cluster.id=msgpack-' in text
        assert 'image: "sha256:' in text
        cluster = re.search(r'rustfs.cluster.id=(msgpack-[0-9a-f]{32})', text).group(1)
        Path(os.environ["FAKE_CLUSTER_STATE"]).write_text(cluster)
elif args[:1] == ["inspect"]:
    override, node = args[1].rsplit("@", 1)
    text = Path(override).read_text().split("  " + node + ":\n")[1].split("\n  node", 1)[0]
    image_id = re.search(r'image: "([^"]+)"', text).group(1)
    cluster = re.search(r'OTEL_RESOURCE_ATTRIBUTES: "([^"]+)"', text).group(1)
    if mode == "running":
        image_id = "sha256:" + "c" * 64
    print(json.dumps([{"Image": image_id, "State": {"Running": True}, "Config": {"Env": ["RUSTFS_SECRET_KEY=identity-secret-sentinel", "OTEL_RESOURCE_ATTRIBUTES=" + cluster]}}]))
PYTHON

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
if [[ "$query" != "vector(0)" && "$query" != *"{rustfs_cluster_id=\"$(cat "$FAKE_CLUSTER_STATE")\""* ]]; then
  echo 'query lacks canary selector' >&2
  exit 1
fi
if [[ "$query" == *msgpack_send_decisions_total* ]]; then
  if [[ "${FAKE_FOREIGN_METRICS:-false}" == true ]]; then
    # Only foreign-cluster traffic exists; an exact canary selector returns no rows.
    printf '{"status":"success","data":{"resultType":"vector","result":[]}}\n'
    exit 0
  fi
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
    FAKE_CLUSTER_STATE="$TMP_DIR/cluster.id" \
    FAKE_DECISION_INDEX="$TMP_DIR/decision.index" \
    FAKE_AWS_STATE="$TMP_DIR/aws.object" \
    FAKE_NONZERO_ERRORS="$nonzero" \
    FAKE_IDENTITY_FAILURE="${3:-}" \
    FAKE_FOREIGN_METRICS="${4:-false}" \
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
assert len(data["artifacts"]) == 22
PY
rg -q 'compose .*up -d --no-build --force-recreate node4' "$TMP_DIR/docker.log"
rg -q 'compose .*up -d --no-build --force-recreate node2' "$TMP_DIR/docker.log"
rg -q 'compose .*down --volumes --remove-orphans' "$TMP_DIR/docker.log"
if rg -q 'identity-secret-sentinel' "$TMP_DIR/pass"; then
  echo "verification artifacts leaked a secret sentinel" >&2
  exit 1
fi

run_once fail true
[[ "$(cat "$TMP_DIR/fail.status")" == "1" ]]
python3 - "$TMP_DIR/fail/verification.json" <<'PY'
import json
import sys
data = json.load(open(sys.argv[1], encoding="utf-8"))
assert data["observed_pass"] is False
assert any("decode_errors" in item for item in data["violations"])
PY

run_once same-image false same
[[ "$(cat "$TMP_DIR/same-image.status")" != 0 ]]
if rg -q 'compose .*up ' "$TMP_DIR/docker.log"; then
  echo "same-image preflight started the Compose project" >&2
  exit 1
fi
run_once stale-revision false revision
[[ "$(cat "$TMP_DIR/stale-revision.status")" != 0 ]]
if rg -q 'image history ' "$TMP_DIR/docker.log"; then
  echo "mismatched config label fell back to image history" >&2
  exit 1
fi
if rg -q 'compose .*up ' "$TMP_DIR/docker.log"; then
  echo "stale-revision preflight started the Compose project" >&2
  exit 1
fi
run_once history-labels false history
[[ "$(cat "$TMP_DIR/history-labels.status")" == 0 ]]
rg -Fq "image history --no-trunc --format {{json .}} sha256:$(printf 'a%.0s' {1..64})" "$TMP_DIR/docker.log"
rg -Fq "image history --no-trunc --format {{json .}} sha256:$(printf 'b%.0s' {1..64})" "$TMP_DIR/docker.log"
python3 - "$TMP_DIR/history-labels/images.json" <<'PY'
import json
import sys
images = json.load(open(sys.argv[1], encoding="utf-8"))
assert all(image["RevisionEvidence"] == "immutable-history" for image in images)
PY
run_once stale-history false history-revision
[[ "$(cat "$TMP_DIR/stale-history.status")" != 0 ]]
if rg -q 'compose .*up ' "$TMP_DIR/docker.log"; then
  echo "stale-history preflight started the Compose project" >&2
  exit 1
fi
run_once noncanonical-history false history-noncanonical
[[ "$(cat "$TMP_DIR/noncanonical-history.status")" != 0 ]]
if rg -q 'compose .*up ' "$TMP_DIR/docker.log"; then
  echo "noncanonical-history preflight started the Compose project" >&2
  exit 1
fi
run_once conflicting-history false history-conflict
[[ "$(cat "$TMP_DIR/conflicting-history.status")" != 0 ]]
if rg -q 'compose .*up ' "$TMP_DIR/docker.log"; then
  echo "conflicting-history preflight started the Compose project" >&2
  exit 1
fi
run_once wrong-running-image false running
[[ "$(cat "$TMP_DIR/wrong-running-image.status")" != 0 ]]
run_once foreign-metrics false "" true
[[ "$(cat "$TMP_DIR/foreign-metrics.status")" == 1 ]]
python3 - "$TMP_DIR/foreign-metrics/verification.json" <<'PY'
import json
import sys
result = json.load(open(sys.argv[1], encoding="utf-8"))
assert not result["observed_pass"]
assert any("no peer_capable" in violation for violation in result["violations"])
PY

bash -n "$RUNNER"
echo "internode msgpack peer canary runner tests passed"
