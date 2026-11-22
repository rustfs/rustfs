#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLUSTER_COMPOSE="${CLUSTER_COMPOSE:-${PROJECT_ROOT}/.docker/compose/docker-compose.cluster.local-build.yml}"
PROJECT_NAME="${PROJECT_NAME:-rustfs-msgpack-peer-canary}"
OUT_DIR="${OUT_DIR:-${PROJECT_ROOT}/target/validation/internode-msgpack-peer-$(date +%Y%m%d-%H%M%S)}"
PROMETHEUS_QUERY_URL="${PROMETHEUS_QUERY_URL:-http://127.0.0.1:9090/api/v1/query}"
WORKLOAD_RUNNER="${WORKLOAD_RUNNER:-${PROJECT_ROOT}/scripts/run_object_batch_bench.sh}"
PYTHON_BIN="${RUSTFS_PYTHON_BIN:-python3}"
CANDIDATE_IMAGE=""
BASELINE_IMAGE=""
CANDIDATE_REVISION=""
BASELINE_REVISION=""
EXECUTE=false
KEEP_UP=false
WAIT_TIMEOUT_SECS="${WAIT_TIMEOUT_SECS:-180}"
METRICS_SETTLE_SECS="${METRICS_SETTLE_SECS:-15}"
WORKLOAD_DURATION="${WORKLOAD_DURATION:-20s}"
WORKLOAD_CONCURRENCY="${WORKLOAD_CONCURRENCY:-8}"
WORKLOAD_SIZES="${WORKLOAD_SIZES:-4KiB,1MiB}"
RUSTFS_ACCESS_KEY="${RUSTFS_ACCESS_KEY:-rustfsadmin-local}"
RUSTFS_SECRET_KEY="${RUSTFS_SECRET_KEY:-rustfssecret-local}"
S3_ENDPOINT="${S3_ENDPOINT:-http://127.0.0.1:9000}"
CANARY_BUCKET="${CANARY_BUCKET:-rustfs-msgpack-peer-canary}"
LAST_PHASE_END=0
ACTIVE_OVERRIDE=""
CLUSTER_ID=""
CANDIDATE_IMAGE_ID=""
BASELINE_IMAGE_ID=""

usage() {
  cat <<'USAGE'
Usage:
  scripts/run_internode_msgpack_peer_canary.sh \
    --candidate-image IMAGE --baseline-image IMAGE \
    --candidate-revision 40HEX --baseline-revision 40HEX [--execute]

Options:
  --candidate-image IMAGE       Candidate RustFS image.
  --baseline-image IMAGE        Previous-version image lacking the peer capability.
  --candidate-revision 40HEX    Exact source revision used by the candidate image.
  --baseline-revision 40HEX     Exact source revision used by the baseline image.
  --cluster-compose PATH        Four-node compose file.
  --project-name NAME           Isolated Docker Compose project name.
  --prometheus-query-url URL    Prometheus /api/v1/query endpoint.
  --workload-runner PATH        Object batch workload runner.
  --out-dir PATH                Evidence directory.
  --execute                     Run Docker, workload and Prometheus capture. Without this, print the plan.
  --keep-up                     Leave the Compose project running after execution.
  -h, --help                    Show this help.

Environment tuning:
  RUSTFS_ACCESS_KEY RUSTFS_SECRET_KEY S3_ENDPOINT CANARY_BUCKET
  WAIT_TIMEOUT_SECS METRICS_SETTLE_SECS WORKLOAD_DURATION
  WORKLOAD_CONCURRENCY WORKLOAD_SIZES RUSTFS_PYTHON_BIN

Phases:
  mixed-old-peer     node1/2/3 candidate, node4 baseline
  all-new            node1/2/3/4 candidate
  rollback-old-peer  node1/3/4 candidate, node2 baseline, known-object readback required
USAGE
}

die() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 2
}

log() {
  printf '[msgpack-peer-canary] %s\n' "$*"
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "command not found: $1"
}

validate_token() {
  local name="$1"
  local value="$2"
  [[ "$value" =~ ^[A-Za-z0-9][A-Za-z0-9._/:@-]*$ ]] || die "invalid ${name}: ${value}"
}

validate_revision() {
  local name="$1"
  local value="$2"
  [[ "$value" =~ ^[0-9a-f]{40}$ ]] || die "${name} must be a lowercase 40-hex revision"
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --candidate-image) CANDIDATE_IMAGE="${2:-}"; shift 2 ;;
      --baseline-image) BASELINE_IMAGE="${2:-}"; shift 2 ;;
      --candidate-revision) CANDIDATE_REVISION="${2:-}"; shift 2 ;;
      --baseline-revision) BASELINE_REVISION="${2:-}"; shift 2 ;;
      --cluster-compose) CLUSTER_COMPOSE="${2:-}"; shift 2 ;;
      --project-name) PROJECT_NAME="${2:-}"; shift 2 ;;
      --prometheus-query-url) PROMETHEUS_QUERY_URL="${2:-}"; shift 2 ;;
      --workload-runner) WORKLOAD_RUNNER="${2:-}"; shift 2 ;;
      --out-dir) OUT_DIR="${2:-}"; shift 2 ;;
      --execute) EXECUTE=true; shift ;;
      --keep-up) KEEP_UP=true; shift ;;
      -h|--help) usage; exit 0 ;;
      *) die "unknown argument: $1" ;;
    esac
  done
}

validate() {
  [[ -n "$CANDIDATE_IMAGE" && -n "$BASELINE_IMAGE" ]] || die "candidate and baseline images are required"
  [[ "$CANDIDATE_IMAGE" != "$BASELINE_IMAGE" ]] || die "candidate and baseline images must differ"
  validate_token candidate-image "$CANDIDATE_IMAGE"
  validate_token baseline-image "$BASELINE_IMAGE"
  validate_token project-name "$PROJECT_NAME"
  [[ "$CANARY_BUCKET" =~ ^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$ ]] || die "invalid S3 canary bucket: $CANARY_BUCKET"
  validate_revision candidate-revision "$CANDIDATE_REVISION"
  validate_revision baseline-revision "$BASELINE_REVISION"
  [[ "$CANDIDATE_REVISION" != "$BASELINE_REVISION" ]] || die "candidate and baseline revisions must differ"
  [[ -f "$CLUSTER_COMPOSE" ]] || die "cluster compose file not found: $CLUSTER_COMPOSE"
  [[ -x "$WORKLOAD_RUNNER" ]] || die "workload runner is not executable: $WORKLOAD_RUNNER"
  [[ "$WAIT_TIMEOUT_SECS" =~ ^[0-9]+$ && "$WAIT_TIMEOUT_SECS" -gt 0 ]] || die "invalid WAIT_TIMEOUT_SECS"
  [[ "$METRICS_SETTLE_SECS" =~ ^[0-9]+$ ]] || die "invalid METRICS_SETTLE_SECS"
  [[ "$WORKLOAD_CONCURRENCY" =~ ^[0-9]+$ && "$WORKLOAD_CONCURRENCY" -gt 0 ]] || die "invalid WORKLOAD_CONCURRENCY"
  [[ "$WORKLOAD_DURATION" =~ ^[1-9][0-9]*(s|m)$ ]] || die "invalid WORKLOAD_DURATION"
}

phase_is_old() {
  local phase="$1"
  local node="$2"
  [[ "$phase" == "mixed-old-peer" && "$node" == "node4" ]] ||
    [[ "$phase" == "rollback-old-peer" && "$node" == "node2" ]]
}

phase_image() {
  if phase_is_old "$1" "$2"; then
    printf '%s\n' "$BASELINE_IMAGE_ID"
  else
    printf '%s\n' "$CANDIDATE_IMAGE_ID"
  fi
}

write_override() {
  local phase="$1"
  local path="$2"
  local node image requested peer
  {
    printf 'services:\n'
    for node in node1 node2 node3 node4; do
      image="$(phase_image "$phase" "$node")"
      if phase_is_old "$phase" "$node"; then
        requested=false
        peer=false
      else
        requested=true
        peer=true
      fi
      printf '  %s:\n' "$node"
      printf '    image: "%s"\n' "$image"
      printf '    environment:\n'
      printf '      OTEL_RESOURCE_ATTRIBUTES: "rustfs.cluster.id=%s"\n' "$CLUSTER_ID"
      printf '      RUSTFS_INTERNODE_RPC_MSGPACK_ONLY: "%s"\n' "$requested"
      printf '      RUSTFS_INTERNODE_RPC_MSGPACK_PEER_CAPABILITY: "%s"\n' "$peer"
      printf '      RUSTFS_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED: "false"\n'
    done
  } >"$path"
}

compose() {
  docker compose --project-name "$PROJECT_NAME" -f "$CLUSTER_COMPOSE" -f "$ACTIVE_OVERRIDE" "$@"
}

cleanup() {
  if [[ "$EXECUTE" == "true" && "$KEEP_UP" != "true" && -n "$ACTIVE_OVERRIDE" ]]; then
    compose down --volumes --remove-orphans >/dev/null 2>&1 || true
  fi
}

preflight_isolated_project() {
  local containers volumes
  containers="$(docker ps -aq --filter "label=com.docker.compose.project=${PROJECT_NAME}")"
  volumes="$(docker volume ls -q --filter "label=com.docker.compose.project=${PROJECT_NAME}")"
  [[ -z "$containers" ]] || die "Compose project already has containers: ${PROJECT_NAME}"
  [[ -z "$volumes" ]] || die "Compose project already has volumes: ${PROJECT_NAME}"
}

verify_images() {
  local identities
  identities="$("$PYTHON_BIN" - "$OUT_DIR/images.json" "$CANDIDATE_IMAGE" "$BASELINE_IMAGE" \
    "$CANDIDATE_REVISION" "$BASELINE_REVISION" <<'PYTHON'
import json
from pathlib import Path
import re
import subprocess
import sys

LABEL_KEYS = ("vcs-ref", "org.opencontainers.image.revision")
IMAGE_ID = re.compile(r"sha256:[0-9a-f]{64}")
REVISION = re.compile(r"[0-9a-f]{40}")


def docker_output(*args: str) -> str:
    result = subprocess.run(["docker", *args], check=False, capture_output=True, text=True)
    if result.returncode != 0:
        raise SystemExit(f"docker {' '.join(args[:2])} failed")
    return result.stdout


def history_labels(image_id: str) -> dict[str, str]:
    labels: dict[str, str] = {}
    history = docker_output("image", "history", "--no-trunc", "--format", "{{json .}}", image_id)
    for line in history.splitlines():
        try:
            created_by = json.loads(line).get("CreatedBy", "")
        except (AttributeError, json.JSONDecodeError) as error:
            raise SystemExit("invalid immutable image history") from error
        if not isinstance(created_by, str):
            raise SystemExit("invalid immutable image history")
        instruction = re.search(r"(?:^|\s)LABEL\s+(.+)$", created_by)
        if instruction is None:
            continue
        payload = instruction.group(1)
        for key in LABEL_KEYS:
            if key in labels:
                continue
            match = re.search(rf"(?:^|\s){re.escape(key)}=([^\s]+)", payload)
            if match is None:
                if key in payload:
                    raise SystemExit("image history revision label is not canonical")
                continue
            value = match.group(1)
            if REVISION.fullmatch(value) is None:
                raise SystemExit("image history revision label is not canonical")
            labels[key] = value
    return labels


output, candidate, baseline, candidate_revision, baseline_revision = sys.argv[1:]
sanitized = []
ids = []
for reference, revision in ((candidate, candidate_revision), (baseline, baseline_revision)):
    try:
        images = json.loads(docker_output("image", "inspect", reference))
    except json.JSONDecodeError as error:
        raise SystemExit("invalid image inspection") from error
    if not isinstance(images, list) or len(images) != 1 or not isinstance(images[0], dict):
        raise SystemExit("expected one inspected image")
    image = images[0]
    image_id = image.get("Id", "")
    if IMAGE_ID.fullmatch(image_id) is None:
        raise SystemExit("invalid immutable image ID")
    config = image.get("Config")
    configured = config.get("Labels") if isinstance(config, dict) else {}
    if configured is None:
        configured = {}
    if not isinstance(configured, dict):
        raise SystemExit("invalid image labels")
    labels = {key: configured[key] for key in LABEL_KEYS if key in configured}
    evidence = "config-labels"
    if not labels:
        labels = history_labels(image_id)
        evidence = "immutable-history"
    observed = [labels[key] for key in ("vcs-ref", "org.opencontainers.image.revision") if key in labels]
    if not observed or any(value != revision for value in observed):
        raise SystemExit("image identity/revision does not match requested revision")
    sanitized.append({"Id": image_id, "Config": {"Labels": labels}, "RevisionEvidence": evidence})
    ids.append(image_id)
if ids[0] == ids[1]:
    raise SystemExit("candidate and baseline resolve to the same image")
Path(output).write_text(json.dumps(sanitized, sort_keys=True) + "\n", encoding="utf-8")
print(*ids)
PYTHON
)"
  read -r CANDIDATE_IMAGE_ID BASELINE_IMAGE_ID <<<"$identities"
}

record_running_images() {
  local phase="$1" phase_dir="$2" node container
  for node in node1 node2 node3 node4; do
    container="$(compose ps -q "$node")"
    [[ -n "$container" && "$container" != *$'\n'* ]] || die "expected one running container for ${node}"
    docker inspect "$container" | "$PYTHON_BIN" -c '
import json, sys
print(json.dumps([{"Image": container.get("Image"), "State": {"Running": container.get("State", {}).get("Running")},
    "Config": {"Env": [item for item in container.get("Config", {}).get("Env", [])
                       if item.startswith("OTEL_RESOURCE_ATTRIBUTES=")]}}
    for container in json.load(sys.stdin)]))
' >"$phase_dir/${node}-container.json"
  done
  "$PYTHON_BIN" - "$phase_dir" "$phase" "$CANDIDATE_IMAGE_ID" "$BASELINE_IMAGE_ID" \
    "$CANDIDATE_REVISION" "$BASELINE_REVISION" "$CLUSTER_ID" <<'PYTHON'
import json
from pathlib import Path
import sys
root, phase, candidate, baseline, candidate_revision, baseline_revision, cluster = sys.argv[1:]
root = Path(root)
nodes = {}
for node in ("node1", "node2", "node3", "node4"):
    old = (phase == "mixed-old-peer" and node == "node4") or (phase == "rollback-old-peer" and node == "node2")
    expected = baseline if old else candidate
    inspected = json.loads((root / f"{node}-container.json").read_text())
    if len(inspected) != 1:
        raise SystemExit(f"expected one inspected container for {node}")
    container = inspected[0]
    if container.get("Image") != expected or not container.get("State", {}).get("Running"):
        raise SystemExit(f"running image mismatch for {node}")
    if f"OTEL_RESOURCE_ATTRIBUTES=rustfs.cluster.id={cluster}" not in container.get("Config", {}).get("Env", []):
        raise SystemExit(f"telemetry identity mismatch for {node}")
    nodes[node] = {"image_id": container["Image"], "revision": baseline_revision if old else candidate_revision}
(root / "nodes.json").write_text(json.dumps(nodes, sort_keys=True) + "\n")
PYTHON
}

preflight_prometheus() {
  local output="$OUT_DIR/prometheus-preflight.json"
  prometheus_query "vector(0)" "$(date +%s)" "$output"
  "$PYTHON_BIN" - "$output" <<'PY'
import json
from pathlib import Path
import sys
document = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
if document.get("status") != "success" or document.get("data", {}).get("resultType") != "vector":
    raise SystemExit("Prometheus preflight did not return a successful vector")
PY
}

node_port() {
  case "$1" in
    node1) printf '9000\n' ;;
    node2) printf '9001\n' ;;
    node3) printf '9002\n' ;;
    node4) printf '9003\n' ;;
    *) die "unknown node: $1" ;;
  esac
}

wait_node() {
  local node="$1"
  local port start now
  port="$(node_port "$node")"
  start="$(date +%s)"
  while ! curl -fsS --connect-timeout 2 --max-time 3 "http://127.0.0.1:${port}/health" >/dev/null 2>&1; do
    now="$(date +%s)"
    (( now - start < WAIT_TIMEOUT_SECS )) || die "timed out waiting for ${node}"
    sleep 2
  done
}

wait_cluster() {
  local node
  for node in node1 node2 node3 node4; do
    wait_node "$node"
  done
}

aws_s3() {
  AWS_ACCESS_KEY_ID="$RUSTFS_ACCESS_KEY" AWS_SECRET_ACCESS_KEY="$RUSTFS_SECRET_KEY" \
    AWS_EC2_METADATA_DISABLED=true aws --endpoint-url "$S3_ENDPOINT" --region us-east-1 "$@"
}

prepare_readback_object() {
  local payload="$OUT_DIR/readback-source.bin"
  printf 'rustfs-msgpack-peer-canary\nsource=%s\nbaseline=%s\n' "$CANDIDATE_REVISION" "$BASELINE_REVISION" >"$payload"
  if ! aws_s3 s3api head-bucket --bucket "$CANARY_BUCKET" >/dev/null 2>&1; then
    aws_s3 s3api create-bucket --bucket "$CANARY_BUCKET" >/dev/null
  fi
  aws_s3 s3api put-object --bucket "$CANARY_BUCKET" --key rollback/readback.bin --body "$payload" >/dev/null
}

verify_readback_object() {
  local downloaded="$OUT_DIR/rollback-readback.bin"
  aws_s3 s3api get-object --bucket "$CANARY_BUCKET" --key rollback/readback.bin "$downloaded" >/dev/null
  cmp -s "$OUT_DIR/readback-source.bin" "$downloaded"
}

duration_seconds() {
  "$PYTHON_BIN" - "$WORKLOAD_DURATION" <<'PY'
import re
import sys
value = sys.argv[1]
match = re.fullmatch(r"([1-9][0-9]*)(s|m)", value)
if not match:
    raise SystemExit("duration must be positive integer seconds or minutes")
amount = int(match.group(1))
print(amount if match.group(2) == "s" else amount * 60)
PY
}

write_workload_evidence() {
  local summary="$1"
  local rollback="$2"
  local output="$3"
  local seconds
  seconds="$(duration_seconds)"
  "$PYTHON_BIN" - "$OUT_DIR" "$summary" "$seconds" "$rollback" "$output" <<'PY'
import csv
import hashlib
import json
from pathlib import Path
import re
import sys

root, summary = Path(sys.argv[1]).resolve(), Path(sys.argv[2]).resolve()
seconds, rollback, output = int(sys.argv[3]), sys.argv[4] == "true", Path(sys.argv[5])
rows = list(csv.DictReader(summary.open(newline="", encoding="utf-8")))
if not rows or any(row.get("status") != "ok" or row.get("error_count") != "0" for row in rows):
    raise SystemExit("workload summary contains failed or error rows")
requests = 0
for row in rows:
    match = re.match(r"([0-9]+(?:\.[0-9]+)?)", row.get("requests_per_sec", ""))
    if not match:
        raise SystemExit("workload summary lacks requests_per_sec")
    requests += max(1, int(float(match.group(1)) * seconds))
summary_bytes = summary.read_bytes()
source_digest = downloaded_digest = None
if rollback:
    source_digest = hashlib.sha256((root / "readback-source.bin").read_bytes()).hexdigest()
    downloaded_digest = hashlib.sha256((root / "rollback-readback.bin").read_bytes()).hexdigest()
output.write_text(json.dumps({
    "status": "success",
    "requests": requests,
    "rollback_readback": rollback,
    "summary": summary.relative_to(root).as_posix(),
    "summary_sha256": hashlib.sha256(summary_bytes).hexdigest(),
    "readback_source_sha256": source_digest,
    "readback_download_sha256": downloaded_digest,
}, sort_keys=True) + "\n", encoding="utf-8")
PY
}

run_workload() {
  local phase="$1"
  local phase_dir="$2"
  local bench_dir="$phase_dir/workload-bench"
  if ! "$WORKLOAD_RUNNER" \
    --tool warp \
    --endpoint "http://127.0.0.1:9000,http://127.0.0.1:9001,http://127.0.0.1:9002,http://127.0.0.1:9003" \
    --access-key "$RUSTFS_ACCESS_KEY" \
    --secret-key "$RUSTFS_SECRET_KEY" \
    --bucket-prefix "msgpack-peer-${phase}" \
    --auto-new-bucket \
    --warp-mode mixed \
    --concurrency "$WORKLOAD_CONCURRENCY" \
    --duration "$WORKLOAD_DURATION" \
    --sizes "$WORKLOAD_SIZES" \
    --out-dir "$bench_dir" >"$phase_dir/workload-runner.log" 2>&1; then
    cat "$phase_dir/workload-runner.log" >&2
    die "workload runner failed for ${phase}"
  fi
  [[ -f "$bench_dir/summary.csv" ]] || die "workload did not produce summary.csv for ${phase}"
  printf '%s\n' "$bench_dir/summary.csv"
}

prometheus_query() {
  local query="$1"
  local at="$2"
  local output="$3"
  local tmp="${output}.tmp"
  curl -fsS -G "$PROMETHEUS_QUERY_URL" \
    --data-urlencode "query=${query}" \
    --data-urlencode "time=${at}" >"$tmp"
  mv "$tmp" "$output"
}

wait_next_phase_second() {
  local now
  while true; do
    now="$(date +%s)"
    if (( now > LAST_PHASE_END )); then
      printf '%s\n' "$now"
      return
    fi
    sleep 1
  done
}

capture_metrics() {
  local phase_dir="$1"
  local start="$2"
  local end="$3"
  local window=$((end - start))
  local decisions fallback decode_errors rpc_errors
  decisions="sum by (decision, reason) (increase(rustfs_system_network_internode_msgpack_send_decisions_total{rustfs_cluster_id=\"${CLUSTER_ID}\"}[${window}s]))"
  fallback="sum(increase(rustfs_system_network_internode_msgpack_json_fallback_total{rustfs_cluster_id=\"${CLUSTER_ID}\"}[${window}s])) or vector(0)"
  decode_errors="sum(increase(rustfs_system_network_internode_msgpack_json_decode_error_total{rustfs_cluster_id=\"${CLUSTER_ID}\"}[${window}s])) or vector(0)"
  rpc_errors="(sum(increase(rustfs_system_network_internode_operation_errors_total{rustfs_cluster_id=\"${CLUSTER_ID}\",backend=\"grpc\",operation!=\"grpc_read_version\"}[${window}s])) or vector(0)) + (sum(increase(rustfs_system_network_internode_operation_classified_errors_total{rustfs_cluster_id=\"${CLUSTER_ID}\",backend=\"grpc\",operation=\"grpc_read_version\",classification!=\"remote_response\"}[${window}s])) or vector(0))"
  prometheus_query "$decisions" "$end" "$phase_dir/decisions.json"
  prometheus_query "$fallback" "$end" "$phase_dir/fallback.json"
  prometheus_query "$decode_errors" "$end" "$phase_dir/decode_errors.json"
  prometheus_query "$rpc_errors" "$end" "$phase_dir/rpc_errors.json"
  "$PYTHON_BIN" - "$phase_dir/queries.json" "$decisions" "$fallback" "$decode_errors" "$rpc_errors" <<'PY'
import json
from pathlib import Path
import sys
Path(sys.argv[1]).write_text(json.dumps({"decisions": sys.argv[2], "fallback": sys.argv[3],
                                        "decode_errors": sys.argv[4], "rpc_errors": sys.argv[5]},
                                       sort_keys=True) + "\n", encoding="utf-8")
PY
}

phase_nodes() {
  case "$1" in
    mixed-old-peer) printf 'node1 node2 node3 node4\n' ;;
    all-new) printf 'node4\n' ;;
    rollback-old-peer) printf 'node2\n' ;;
    *) die "unknown phase: $1" ;;
  esac
}

run_phase() {
  local phase="$1"
  local phase_dir="$OUT_DIR/$phase"
  local override="$phase_dir/compose.override.yml"
  local summary start end rollback=false
  local -a nodes
  mkdir -p "$phase_dir"
  write_override "$phase" "$override"
  ACTIVE_OVERRIDE="$override"
  read -r -a nodes <<<"$(phase_nodes "$phase")"
  log "applying ${phase}: ${nodes[*]}"
  compose up -d --no-build --force-recreate "${nodes[@]}"
  wait_cluster
  record_running_images "$phase" "$phase_dir"
  if [[ "$phase" == "mixed-old-peer" ]]; then
    prepare_readback_object
  fi
  start="$(wait_next_phase_second)"
  summary="$(run_workload "$phase" "$phase_dir")"
  if [[ "$phase" == "rollback-old-peer" ]]; then
    verify_readback_object || die "rollback readback mismatch"
    rollback=true
  fi
  sleep "$METRICS_SETTLE_SECS"
  end="$(date +%s)"
  while (( end <= start )); do sleep 1; end="$(date +%s)"; done
  LAST_PHASE_END="$end"
  write_workload_evidence "$summary" "$rollback" "$phase_dir/workload.json"
  capture_metrics "$phase_dir" "$start" "$end"
  printf '%s\t%s\t%s\n' "$phase" "$start" "$end" >>"$OUT_DIR/phases.tsv"
}

write_manifest() {
  PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$PROJECT_ROOT/scripts" "$PYTHON_BIN" - \
    "$OUT_DIR" "$CANDIDATE_REVISION" "$BASELINE_REVISION" "$CLUSTER_ID" <<'PY'
import json
from pathlib import Path
import sys
from check_internode_msgpack_peer_canary import expected_queries, OLD_NODES

root, candidate, baseline, cluster = Path(sys.argv[1]), sys.argv[2], sys.argv[3], sys.argv[4]
phases = []
for line in (root / "phases.tsv").read_text(encoding="utf-8").splitlines():
    name, start, end = line.split("\t")
    start, end = int(start), int(end)
    old = OLD_NODES[name]
    nodes = {node: (baseline if node in old else candidate) for node in ("node1", "node2", "node3", "node4")}
    phases.append({
        "name": name,
        "start_unix": start,
        "end_unix": end,
        "topology": {"new_nodes": 4 - len(old), "old_nodes": len(old), "nodes": nodes},
        "environment": {
            "new_nodes": {"msgpack_only": True, "peer_capability": True, "fleet_confirmed": False},
            "old_nodes": {"msgpack_only": False, "peer_capability": False, "fleet_confirmed": False},
        },
        "queries": expected_queries(end - start, cluster),
        "images": f"{name}/nodes.json",
        "workload": f"{name}/workload.json",
        "artifacts": {kind: f"{name}/{kind}.json" for kind in
                      ("decisions", "fallback", "decode_errors", "rpc_errors")},
    })
manifest = {
    "schema": 1,
    "scope": "internode-msgpack-peer-canary",
    "source_revision": candidate,
    "baseline_revision": baseline,
    "stable_connection_affinity": True,
    "cluster_id": cluster,
    "images": "images.json",
    "max_metric_lag_secs": 120,
    "phases": phases,
}
(root / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
}

print_plan() {
  printf 'candidate_image=%s\n' "$CANDIDATE_IMAGE"
  printf 'baseline_image=%s\n' "$BASELINE_IMAGE"
  printf 'candidate_revision=%s\n' "$CANDIDATE_REVISION"
  printf 'baseline_revision=%s\n' "$BASELINE_REVISION"
  printf 'project_name=%s\n' "$PROJECT_NAME"
  printf 'out_dir=%s\n' "$OUT_DIR"
  printf 'phases=mixed-old-peer all-new rollback-old-peer\n'
  printf 'verifier=scripts/check_internode_msgpack_peer_canary.py\n'
  printf 'execute=false\n'
}

main() {
  parse_args "$@"
  validate
  if [[ "$EXECUTE" != "true" ]]; then
    print_plan
    return 0
  fi
  require_cmd docker
  require_cmd curl
  require_cmd aws
  require_cmd cmp
  require_cmd "$PYTHON_BIN"
  docker info >/dev/null
  preflight_isolated_project
  if [[ -d "$OUT_DIR" && -n "$(find "$OUT_DIR" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
    die "evidence directory is not empty: $OUT_DIR"
  fi
  mkdir -p "$OUT_DIR"
  CLUSTER_ID="$("$PYTHON_BIN" -c 'import uuid; print("msgpack-" + uuid.uuid4().hex)')"
  verify_images
  preflight_prometheus
  : >"$OUT_DIR/phases.tsv"
  trap cleanup EXIT
  run_phase mixed-old-peer
  run_phase all-new
  run_phase rollback-old-peer
  write_manifest
  "$PYTHON_BIN" "$PROJECT_ROOT/scripts/check_internode_msgpack_peer_canary.py" \
    --manifest "$OUT_DIR/manifest.json" \
    --out "$OUT_DIR/verification.json"
  log "verified evidence: $OUT_DIR/verification.json"
}

main "$@"
