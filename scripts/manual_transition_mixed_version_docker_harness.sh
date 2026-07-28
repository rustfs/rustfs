#!/usr/bin/env bash
# Copyright 2026 RustFS Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

PROJECT_NAME="${PROJECT_NAME:-rustfs-1508-mixed}"
OLD_IMAGE="${OLD_IMAGE:-rustfs/rustfs:latest}"
NEW_IMAGE="${NEW_IMAGE:-rustfs/rustfs:latest}"
COLD_IMAGE="${COLD_IMAGE:-${NEW_IMAGE}}"
BASE_PORT="${BASE_PORT:-19400}"
OUT_DIR="${OUT_DIR:-${PROJECT_ROOT}/target/manual-transition-1508-docker/$(date +%Y%m%dT%H%M%S)}"
KEEP_UP=false
ROLLBACK_PHASE="${ROLLBACK_PHASE:-after-terminal}"
WAIT_TIMEOUT_SECS="${WAIT_TIMEOUT_SECS:-180}"
POLL_SECONDS="${POLL_SECONDS:-180}"
TRANSITION_WORKERS="${TRANSITION_WORKERS:-2}"
TRANSITION_QUEUE_CAPACITY="${TRANSITION_QUEUE_CAPACITY:-64}"

HOT_ACCESS_KEY="${HOT_ACCESS_KEY:-mvadmin}"
HOT_SECRET_KEY="${HOT_SECRET_KEY:-mvsecret}"
COLD_ACCESS_KEY="${COLD_ACCESS_KEY:-coldadmin}"
COLD_SECRET_KEY="${COLD_SECRET_KEY:-coldsecret}"
TIER_NAME="${TIER_NAME:-COLDMV}"
TIER_BUCKET="${TIER_BUCKET:-manual-transition-cold-tier}"
TIER_PREFIX="${TIER_PREFIX:-tiered}"
JOB_BUCKET="${JOB_BUCKET:-manual-transition-1508}"
JOB_PREFIX="${JOB_PREFIX:-transition/manual-rollout}"
OBJECT_COUNT="${OBJECT_COUNT:-4}"
AWS_SIGV4_SCOPE="${AWS_SIGV4_SCOPE:-aws:amz:us-east-1:s3}"

COMPOSE_FILE=""

usage() {
  cat <<'USAGE'
Usage:
  scripts/manual_transition_mixed_version_docker_harness.sh [options]

Options:
  --project-name <name>        Docker compose project name
  --old-image <image>          Old RustFS image for node1 and rollback node2
  --new-image <image>          New RustFS image for node2-node4
  --cold-image <image>         Cold-tier RustFS image (default: new image)
  --base-port <port>           Host port base; cold uses base, hot nodes use base+1..4
  --out-dir <path>             Artifact output directory
  --object-count <n>           Non-empty probe object count
  --tier <name>                Remote tier name
  --keep-up                    Leave Docker services running
  --rollback-phase <phase>     Rollback node2 timing: after-terminal, in-flight, none
  --no-rollback                Alias for --rollback-phase none
  -h, --help                   Show help

Environment:
  PROJECT_NAME OLD_IMAGE NEW_IMAGE COLD_IMAGE BASE_PORT OUT_DIR KEEP_UP
  HOT_ACCESS_KEY HOT_SECRET_KEY COLD_ACCESS_KEY COLD_SECRET_KEY
  TIER_NAME TIER_BUCKET TIER_PREFIX JOB_BUCKET JOB_PREFIX OBJECT_COUNT
  ROLLBACK_PHASE WAIT_TIMEOUT_SECS POLL_SECONDS TRANSITION_WORKERS
  TRANSITION_QUEUE_CAPACITY AWS_SIGV4_SCOPE

Artifacts:
  compose.yml, image inspect files, health/readiness logs, API responses,
  terminal status, old-node readback, container logs, summary.env.

Result classifications:
  strict_mixed_rollout_pass          Real old/new images, non-empty completed transition, zero failures
  baseline_tiered_storage_pass      Same old/new image completed transition; useful baseline, not #1508 closure
  blocked_manual_api_not_implemented Manual transition API returned 501 before job admission
  blocked_manual_api_unavailable    Manual transition API did not return a usable job_id
  blocked_cluster_readiness_failed  Docker cluster did not reach health/readiness before admission
  blocked_empty_scan_or_lifecycle   Job completed without lifecycle-matching transition work
  blocked_manual_job_preempted_by_lifecycle_queue Lifecycle/immediate transition queued work before the job
  strict_mixed_rollout_fail         Mixed rollout ran but did not satisfy the strict #1508 gate
USAGE
}

log_info() {
  printf '[INFO] %s\n' "$*"
}

log_warn() {
  printf '[WARN] %s\n' "$*"
}

log_error() {
  printf '[ERROR] %s\n' "$*" >&2
}

arg_value() {
  local flag="$1"
  local value="${2:-}"
  if [[ -z "$value" || "$value" == --* ]]; then
    log_error "missing value for ${flag}"
    exit 1
  fi
  printf '%s' "$value"
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    log_error "required command not found: $1"
    exit 1
  fi
}

parse_positive_int() {
  local name="$1"
  local value="$2"
  if ! [[ "$value" =~ ^[0-9]+$ ]] || (( value <= 0 )); then
    log_error "${name} must be a positive integer"
    exit 1
  fi
}

validate_rollback_phase() {
  case "$ROLLBACK_PHASE" in
    after-terminal|in-flight|none)
      ;;
    *)
      log_error "--rollback-phase must be one of: after-terminal, in-flight, none"
      exit 1
      ;;
  esac
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --project-name)
        PROJECT_NAME="$(arg_value "$1" "${2:-}")"
        shift 2
        ;;
      --old-image)
        OLD_IMAGE="$(arg_value "$1" "${2:-}")"
        shift 2
        ;;
      --new-image)
        NEW_IMAGE="$(arg_value "$1" "${2:-}")"
        shift 2
        ;;
      --cold-image)
        COLD_IMAGE="$(arg_value "$1" "${2:-}")"
        shift 2
        ;;
      --base-port)
        BASE_PORT="$(arg_value "$1" "${2:-}")"
        shift 2
        ;;
      --out-dir)
        OUT_DIR="$(arg_value "$1" "${2:-}")"
        shift 2
        ;;
      --object-count)
        OBJECT_COUNT="$(arg_value "$1" "${2:-}")"
        shift 2
        ;;
      --tier)
        TIER_NAME="$(arg_value "$1" "${2:-}")"
        shift 2
        ;;
      --keep-up)
        KEEP_UP=true
        shift
        ;;
      --rollback-phase)
        ROLLBACK_PHASE="$(arg_value "$1" "${2:-}")"
        shift 2
        ;;
      --no-rollback)
        ROLLBACK_PHASE=none
        shift
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        log_error "unknown argument: $1"
        usage
        exit 1
        ;;
    esac
  done
}

compose() {
  docker compose --project-name "$PROJECT_NAME" -f "$COMPOSE_FILE" "$@"
}

network_name() {
  printf '%s_rustfs-1508-net' "$PROJECT_NAME"
}

hot_volumes() {
  printf 'http://node{1...4}:9000/data/rustfs{0...3}'
}

hot_endpoint() {
  local node_index="$1"
  printf 'http://127.0.0.1:%s' "$((BASE_PORT + node_index))"
}

cold_endpoint() {
  printf 'http://127.0.0.1:%s' "$BASE_PORT"
}

cleanup() {
  collect_logs || true
  if [[ "$KEEP_UP" == "true" ]]; then
    log_info "KEEP_UP=true, leaving Docker services running"
    return
  fi
  compose down -v >/dev/null 2>&1 || true
}

write_compose_file() {
  COMPOSE_FILE="${OUT_DIR}/compose.yml"
  cat >"$COMPOSE_FILE" <<EOF
services:
  cold:
    image: ${COLD_IMAGE}
    hostname: cold
    user: "0:0"
    environment:
      - RUSTFS_ADDRESS=:9000
      - RUSTFS_ACCESS_KEY=${COLD_ACCESS_KEY}
      - RUSTFS_SECRET_KEY=${COLD_SECRET_KEY}
      - RUSTFS_VOLUMES=/data/rustfs0 /data/rustfs1 /data/rustfs2 /data/rustfs3
      - RUSTFS_UNSAFE_BYPASS_DISK_CHECK=true
      - RUSTFS_OBS_LOGGER_LEVEL=warn
    volumes:
      - cold_data_0:/data/rustfs0
      - cold_data_1:/data/rustfs1
      - cold_data_2:/data/rustfs2
      - cold_data_3:/data/rustfs3
    ports:
      - "${BASE_PORT}:9000"
    networks:
      - rustfs-1508-net

  node1:
    image: ${OLD_IMAGE}
    hostname: node1
    user: "0:0"
    environment: &hot-env
      - RUSTFS_ADDRESS=:9000
      - RUSTFS_ACCESS_KEY=${HOT_ACCESS_KEY}
      - RUSTFS_SECRET_KEY=${HOT_SECRET_KEY}
      - RUSTFS_VOLUMES=$(hot_volumes)
      - RUSTFS_SCANNER_ENABLED=false
      - RUSTFS_SCANNER_CYCLE=3600
      - RUSTFS_SCANNER_START_DELAY_SECS=3600
      - RUSTFS_MAX_TRANSITION_WORKERS=${TRANSITION_WORKERS}
      - RUSTFS_TRANSITION_QUEUE_CAPACITY=${TRANSITION_QUEUE_CAPACITY}
      - RUSTFS_UNSAFE_BYPASS_DISK_CHECK=true
      - RUSTFS_OBS_LOGGER_LEVEL=warn
    volumes:
      - node1_data_0:/data/rustfs0
      - node1_data_1:/data/rustfs1
      - node1_data_2:/data/rustfs2
      - node1_data_3:/data/rustfs3
    ports:
      - "$((BASE_PORT + 1)):9000"
    networks:
      - rustfs-1508-net

  node2:
    image: ${NEW_IMAGE}
    hostname: node2
    user: "0:0"
    environment: *hot-env
    volumes:
      - node2_data_0:/data/rustfs0
      - node2_data_1:/data/rustfs1
      - node2_data_2:/data/rustfs2
      - node2_data_3:/data/rustfs3
    ports:
      - "$((BASE_PORT + 2)):9000"
    networks:
      - rustfs-1508-net

  node3:
    image: ${NEW_IMAGE}
    hostname: node3
    user: "0:0"
    environment: *hot-env
    volumes:
      - node3_data_0:/data/rustfs0
      - node3_data_1:/data/rustfs1
      - node3_data_2:/data/rustfs2
      - node3_data_3:/data/rustfs3
    ports:
      - "$((BASE_PORT + 3)):9000"
    networks:
      - rustfs-1508-net

  node4:
    image: ${NEW_IMAGE}
    hostname: node4
    user: "0:0"
    environment: *hot-env
    volumes:
      - node4_data_0:/data/rustfs0
      - node4_data_1:/data/rustfs1
      - node4_data_2:/data/rustfs2
      - node4_data_3:/data/rustfs3
    ports:
      - "$((BASE_PORT + 4)):9000"
    networks:
      - rustfs-1508-net

networks:
  rustfs-1508-net:

volumes:
  cold_data_0:
  cold_data_1:
  cold_data_2:
  cold_data_3:
  node1_data_0:
  node1_data_1:
  node1_data_2:
  node1_data_3:
  node2_data_0:
  node2_data_1:
  node2_data_2:
  node2_data_3:
  node3_data_0:
  node3_data_1:
  node3_data_2:
  node3_data_3:
  node4_data_0:
  node4_data_1:
  node4_data_2:
  node4_data_3:
EOF
}

curl_signed() {
  local access_key="$1"
  local secret_key="$2"
  local method="$3"
  local url="$4"
  shift 4
  curl -sS --aws-sigv4 "$AWS_SIGV4_SCOPE" --user "${access_key}:${secret_key}" -X "$method" "$url" "$@"
}

curl_hot() {
  local method="$1"
  local url="$2"
  shift 2
  curl_signed "$HOT_ACCESS_KEY" "$HOT_SECRET_KEY" "$method" "$url" "$@"
}

curl_cold() {
  local method="$1"
  local url="$2"
  shift 2
  curl_signed "$COLD_ACCESS_KEY" "$COLD_SECRET_KEY" "$method" "$url" "$@"
}

url_encode() {
  jq -rn --arg v "$1" '$v|@uri'
}

wait_http_ok() {
  local url="$1"
  local tag="$2"
  local start now code
  start="$(date +%s)"
  while true; do
    code="$(curl -sS -o /dev/null -w '%{http_code}' "$url" || true)"
    printf '%s %s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$url" "$code" >>"${OUT_DIR}/health-${tag}.log"
    if [[ "$code" == "200" ]]; then
      return 0
    fi
    now="$(date +%s)"
    if (( now - start >= WAIT_TIMEOUT_SECS )); then
      log_error "timed out waiting for ${url}"
      return 1
    fi
    sleep 2
  done
}

wait_cluster_ready() {
  local index endpoint
  wait_http_ok "$(cold_endpoint)/health" "cold-live"
  wait_http_ok "$(cold_endpoint)/health/ready" "cold-ready"
  for index in 1 2 3 4; do
    endpoint="$(hot_endpoint "$index")"
    wait_http_ok "${endpoint}/health" "node${index}-live"
    wait_http_ok "${endpoint}/health/ready" "node${index}-ready"
  done
}

create_bucket() {
  local endpoint="$1"
  local bucket="$2"
  local result_file="$3"
  curl_hot PUT "${endpoint%/}/${bucket}" -o "${result_file}.response" -w "%{http_code}\n" >"${result_file}.http_code" || true
}

add_tier() {
  local body="${OUT_DIR}/add-tier.json"
  cat >"$body" <<EOF
{
  "type": "rustfs",
  "rustfs": {
    "name": "${TIER_NAME}",
    "endpoint": "http://cold:9000",
    "accessKey": "${COLD_ACCESS_KEY}",
    "secretKey": "${COLD_SECRET_KEY}",
    "bucket": "${TIER_BUCKET}",
    "prefix": "${TIER_PREFIX}",
    "region": "us-east-1",
    "storageClass": ""
  }
}
EOF
  curl_hot PUT "$(hot_endpoint 2)/rustfs/admin/v3/tier" \
    -H 'Content-Type: application/json' \
    --data-binary "@${body}" \
    -o "${OUT_DIR}/add-tier.response" \
    -w "%{http_code}\n" >"${OUT_DIR}/add-tier.http_code"
}

put_lifecycle() {
  local lifecycle="${OUT_DIR}/lifecycle.xml"
  cat >"$lifecycle" <<EOF
<LifecycleConfiguration>
  <Rule>
    <ID>manual-transition-1508</ID>
    <Filter><Prefix>${JOB_PREFIX}/</Prefix></Filter>
    <Status>Enabled</Status>
    <Transition><Days>0</Days><StorageClass>${TIER_NAME}</StorageClass></Transition>
  </Rule>
</LifecycleConfiguration>
EOF
  curl_hot PUT "$(hot_endpoint 2)/${JOB_BUCKET}?lifecycle" \
    -H 'Content-Type: application/xml' \
    --data-binary "@${lifecycle}" \
    -o "${OUT_DIR}/put-lifecycle.response" \
    -w "%{http_code}\n" >"${OUT_DIR}/put-lifecycle.http_code"
}

seed_objects() {
  local index key
  for index in $(seq 1 "$OBJECT_COUNT"); do
    key="${JOB_PREFIX}/object-$(printf '%02d' "$index").txt"
    printf 'rustfs #1508 mixed-version transition probe %s\n' "$index" >"${OUT_DIR}/object-${index}.txt"
    curl_hot PUT "$(hot_endpoint 2)/${JOB_BUCKET}/${key}" \
      --data-binary "@${OUT_DIR}/object-${index}.txt" \
      -o "${OUT_DIR}/put-object-${index}.response" \
      -w "%{http_code}\n" >"${OUT_DIR}/put-object-${index}.http_code"
    printf '%s\n' "$key" >>"${OUT_DIR}/object-keys.txt"
  done
}

start_transition_job() {
  local query response job_id run_http_code
  query="bucket=$(url_encode "$JOB_BUCKET")"
  query="${query}&prefix=$(url_encode "${JOB_PREFIX}/")"
  query="${query}&tier=$(url_encode "$TIER_NAME")"
  query="${query}&dryRun=false&maxObjects=${OBJECT_COUNT}&mode=async"
  curl_hot POST "$(hot_endpoint 2)/rustfs/admin/v3/ilm/transition/run?${query}" \
    -o "${OUT_DIR}/run-response.json" \
    -w "%{http_code}\n" >"${OUT_DIR}/run-response.http_code" || true
  response="$(cat "${OUT_DIR}/run-response.json" 2>/dev/null || true)"
  run_http_code="$(cat "${OUT_DIR}/run-response.http_code" 2>/dev/null || true)"
  job_id="$(printf '%s' "$response" | jq -r '.job_id // empty')"
  if [[ -z "$job_id" ]]; then
    log_warn "manual transition response omitted job_id, http_code=${run_http_code:-unknown}"
    return 1
  fi
  printf '%s\n' "$job_id" >"${OUT_DIR}/job-id.txt"
}

replace_node2_with_old_image() {
  local network
  network="$(network_name)"
  log_info "Replacing node2 with old image ${OLD_IMAGE} for ${ROLLBACK_PHASE} rollback readback"
  compose stop node2 >/dev/null
  compose rm -f node2 >/dev/null
  docker run -d \
    --name "${PROJECT_NAME}-node2-rollback-old" \
    --network "$network" \
    --network-alias node2 \
    --user 0:0 \
    -p "$((BASE_PORT + 2)):9000" \
    -e RUSTFS_ADDRESS=:9000 \
    -e RUSTFS_ACCESS_KEY="$HOT_ACCESS_KEY" \
    -e RUSTFS_SECRET_KEY="$HOT_SECRET_KEY" \
    -e RUSTFS_VOLUMES="$(hot_volumes)" \
    -e RUSTFS_SCANNER_ENABLED=false \
    -e RUSTFS_SCANNER_CYCLE=3600 \
    -e RUSTFS_SCANNER_START_DELAY_SECS=3600 \
    -e RUSTFS_MAX_TRANSITION_WORKERS="$TRANSITION_WORKERS" \
    -e RUSTFS_TRANSITION_QUEUE_CAPACITY="$TRANSITION_QUEUE_CAPACITY" \
    -e RUSTFS_UNSAFE_BYPASS_DISK_CHECK=true \
    -e RUSTFS_OBS_LOGGER_LEVEL=warn \
    -v "${PROJECT_NAME}_node2_data_0:/data/rustfs0" \
    -v "${PROJECT_NAME}_node2_data_1:/data/rustfs1" \
    -v "${PROJECT_NAME}_node2_data_2:/data/rustfs2" \
    -v "${PROJECT_NAME}_node2_data_3:/data/rustfs3" \
    "$OLD_IMAGE" >/dev/null
}

poll_terminal_status() {
  local job_id="$1"
  local status_url status_json terminal_state
  status_url="$(hot_endpoint 3)/rustfs/admin/v3/ilm/transition/jobs/${job_id}"
  for _ in $(seq 1 "$POLL_SECONDS"); do
    status_json="$(curl_hot GET "$status_url")"
    printf '%s\n' "$status_json" >"${OUT_DIR}/status-terminal.json"
    terminal_state="$(printf '%s' "$status_json" | jq -r '.status // ""')"
    case "$terminal_state" in
      completed|partial|failed|cancelled|unknown)
        printf '%s\n' "$terminal_state" >"${OUT_DIR}/terminal-state.txt"
        return 0
        ;;
    esac
    sleep 1
  done
  log_error "manual transition job did not reach a terminal status within ${POLL_SECONDS}s"
  return 1
}

capture_old_node_readback() {
  local job_id="$1"
  curl_hot GET "$(hot_endpoint 1)/rustfs/admin/v3/ilm/transition/jobs/${job_id}" \
    -o "${OUT_DIR}/old-node-status.response" \
    -w "%{http_code}\n" >"${OUT_DIR}/old-node-status.http_code" || true
  curl_hot GET "$(hot_endpoint 2)/rustfs/admin/v3/ilm/transition/jobs/${job_id}" \
    -o "${OUT_DIR}/rollback-node2-status.response" \
    -w "%{http_code}\n" >"${OUT_DIR}/rollback-node2-status.http_code" || true
}

head_probe() {
  local key
  key="$(head -n 1 "${OUT_DIR}/object-keys.txt")"
  curl_hot HEAD "$(hot_endpoint 3)/${JOB_BUCKET}/${key}" \
    -D "${OUT_DIR}/head-object.headers" \
    -o /dev/null \
    -w "%{http_code}\n" >"${OUT_DIR}/head-object.http_code" || true
}

image_id() {
  local file="$1"
  jq -r '.[0].Id // ""' "$file" 2>/dev/null || true
}

is_compat_readback_code() {
  case "$1" in
    200|501)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

classify_result() {
  local terminal_state="$1"
  local transition_completed="$2"
  local transition_failed="$3"
  local tier_failure="$4"
  local old_code="$5"
  local rollback_code="$6"
  local run_http_code="$7"
  local lifecycle_config_found="$8"
  local scanned="$9"
  local eligible="${10}"
  local skipped_already_transitioned="${11}"
  local skipped_already_in_flight="${12}"
  local old_image_id new_image_id images_are_mixed readback_ok

  if [[ -f "${OUT_DIR}/readiness-failed" ]]; then
    printf 'blocked_cluster_readiness_failed\n'
    return
  fi

  old_image_id="$(image_id "${OUT_DIR}/old-image.inspect.json")"
  new_image_id="$(image_id "${OUT_DIR}/new-image.inspect.json")"
  images_are_mixed=false
  if [[ "$OLD_IMAGE" != "$NEW_IMAGE" && -n "$old_image_id" && -n "$new_image_id" && "$old_image_id" != "$new_image_id" ]]; then
    images_are_mixed=true
  fi

  readback_ok=false
  if is_compat_readback_code "$old_code"; then
    if [[ "$ROLLBACK_PHASE" == "none" ]] || is_compat_readback_code "$rollback_code"; then
      readback_ok=true
    fi
  fi

  if [[ "$run_http_code" == "501" ]]; then
    printf 'blocked_manual_api_not_implemented\n'
    return
  fi
  if [[ -z "$(cat "${OUT_DIR}/job-id.txt" 2>/dev/null || true)" ]]; then
    printf 'blocked_manual_api_unavailable\n'
    return
  fi
  if [[ "$terminal_state" == "completed" && ( "$lifecycle_config_found" != "true" || "$scanned" == "0" || "$eligible" == "0" || "$transition_completed" == "0" ) ]]; then
    printf 'blocked_empty_scan_or_lifecycle\n'
    return
  fi
  if [[ "$transition_completed" == "0" && "$eligible" != "0" && "$transition_failed" == "0" && "$tier_failure" == "0" ]]; then
    if [[ "$skipped_already_transitioned" != "0" || "$skipped_already_in_flight" != "0" ]]; then
      printf 'blocked_manual_job_preempted_by_lifecycle_queue\n'
      return
    fi
  fi
  if [[ "$terminal_state" == "completed" && "$transition_completed" != "0" && "$transition_failed" == "0" && "$tier_failure" == "0" ]]; then
    if [[ "$images_are_mixed" == "true" && "$readback_ok" == "true" ]]; then
      printf 'strict_mixed_rollout_pass\n'
      return
    fi
    printf 'baseline_tiered_storage_pass\n'
    return
  fi
  printf 'strict_mixed_rollout_fail\n'
}

collect_logs() {
  local service
  if [[ -z "$COMPOSE_FILE" || ! -f "$COMPOSE_FILE" ]]; then
    return 0
  fi
  for service in cold node1 node2 node3 node4; do
    compose logs --no-color "$service" >"${OUT_DIR}/${service}.log" 2>/dev/null || true
  done
  docker logs "${PROJECT_NAME}-node2-rollback-old" >"${OUT_DIR}/node2-rollback-old.log" 2>/dev/null || true
}

summarize() {
  local terminal_state transition_completed tier_failure transition_failed old_code rollback_code run_http_code lifecycle_config_found scanned eligible skipped_already_transitioned skipped_already_in_flight queue_queued queue_active result_classification
  terminal_state="$(cat "${OUT_DIR}/terminal-state.txt" 2>/dev/null || true)"
  transition_completed="$(jq -r '.report.transition_completed // 0' "${OUT_DIR}/status-terminal.json" 2>/dev/null || printf '0')"
  tier_failure="$(jq -r '.report.tier_failure // 0' "${OUT_DIR}/status-terminal.json" 2>/dev/null || printf '0')"
  transition_failed="$(jq -r '.report.transition_failed // 0' "${OUT_DIR}/status-terminal.json" 2>/dev/null || printf '0')"
  old_code="$(cat "${OUT_DIR}/old-node-status.http_code" 2>/dev/null || true)"
  rollback_code="$(cat "${OUT_DIR}/rollback-node2-status.http_code" 2>/dev/null || true)"
  run_http_code="$(cat "${OUT_DIR}/run-response.http_code" 2>/dev/null || true)"
  lifecycle_config_found="$(jq -r '.report.lifecycle_config_found // false' "${OUT_DIR}/status-terminal.json" 2>/dev/null || printf 'false')"
  scanned="$(jq -r '.report.scanned // 0' "${OUT_DIR}/status-terminal.json" 2>/dev/null || printf '0')"
  eligible="$(jq -r '.report.eligible // 0' "${OUT_DIR}/status-terminal.json" 2>/dev/null || printf '0')"
  skipped_already_transitioned="$(jq -r '.report.skipped_already_transitioned // 0' "${OUT_DIR}/status-terminal.json" 2>/dev/null || printf '0')"
  skipped_already_in_flight="$(jq -r '.report.skipped_already_in_flight // 0' "${OUT_DIR}/status-terminal.json" 2>/dev/null || printf '0')"
  queue_queued="$(jq -r '.queue_snapshot.queued // 0' "${OUT_DIR}/status-terminal.json" 2>/dev/null || printf '0')"
  queue_active="$(jq -r '.queue_snapshot.active // 0' "${OUT_DIR}/status-terminal.json" 2>/dev/null || printf '0')"
  result_classification="$(classify_result "$terminal_state" "$transition_completed" "$transition_failed" "$tier_failure" "$old_code" "$rollback_code" "$run_http_code" "$lifecycle_config_found" "$scanned" "$eligible" "$skipped_already_transitioned" "$skipped_already_in_flight")"
  cat >"${OUT_DIR}/summary.env" <<EOF
project_name=${PROJECT_NAME}
old_image=${OLD_IMAGE}
new_image=${NEW_IMAGE}
cold_image=${COLD_IMAGE}
old_image_id=$(image_id "${OUT_DIR}/old-image.inspect.json")
new_image_id=$(image_id "${OUT_DIR}/new-image.inspect.json")
base_port=${BASE_PORT}
tier=${TIER_NAME}
job_bucket=${JOB_BUCKET}
job_prefix=${JOB_PREFIX}
object_count=${OBJECT_COUNT}
transition_workers=${TRANSITION_WORKERS}
transition_queue_capacity=${TRANSITION_QUEUE_CAPACITY}
run_http_code=${run_http_code}
terminal_state=${terminal_state}
lifecycle_config_found=${lifecycle_config_found}
scanned=${scanned}
eligible=${eligible}
skipped_already_transitioned=${skipped_already_transitioned}
skipped_already_in_flight=${skipped_already_in_flight}
transition_completed=${transition_completed}
transition_failed=${transition_failed}
tier_failure=${tier_failure}
queue_queued=${queue_queued}
queue_active=${queue_active}
old_node_status_http_code=${old_code}
rollback_node2_status_http_code=${rollback_code}
rollback_phase=${ROLLBACK_PHASE}
rollback_new2_to_old=$([[ "$ROLLBACK_PHASE" == "none" ]] && printf 'false' || printf 'true')
result_classification=${result_classification}
EOF
  cat "${OUT_DIR}/summary.env"
  if [[ "$result_classification" != "strict_mixed_rollout_pass" ]]; then
    log_error "strict #1508 mixed-version transition gate failed; see ${OUT_DIR}"
    return 1
  fi
}

main() {
  parse_args "$@"
  parse_positive_int "--base-port" "$BASE_PORT"
  parse_positive_int "--object-count" "$OBJECT_COUNT"
  validate_rollback_phase
  require_cmd docker
  require_cmd curl
  require_cmd jq

  mkdir -p "$OUT_DIR"
  write_compose_file
  trap cleanup EXIT INT TERM

  log_info "Writing artifacts under ${OUT_DIR}"
  docker image inspect "$OLD_IMAGE" >"${OUT_DIR}/old-image.inspect.json"
  docker image inspect "$NEW_IMAGE" >"${OUT_DIR}/new-image.inspect.json"
  docker image inspect "$COLD_IMAGE" >"${OUT_DIR}/cold-image.inspect.json"

  compose up -d
  if ! wait_cluster_ready; then
    printf 'cluster readiness failed before manual transition admission\n' >"${OUT_DIR}/readiness-failed"
    collect_logs
    summarize
    return 1
  fi

  curl_cold PUT "$(cold_endpoint)/${TIER_BUCKET}" -o "${OUT_DIR}/create-cold-bucket.response" -w "%{http_code}\n" >"${OUT_DIR}/create-cold-bucket.http_code"
  create_bucket "$(hot_endpoint 2)" "$JOB_BUCKET" "${OUT_DIR}/create-hot-bucket"
  add_tier
  put_lifecycle
  seed_objects
  if start_transition_job; then
    if [[ "$ROLLBACK_PHASE" == "in-flight" ]]; then
      replace_node2_with_old_image
    fi
    poll_terminal_status "$(cat "${OUT_DIR}/job-id.txt")" || true
    if [[ "$ROLLBACK_PHASE" == "after-terminal" ]]; then
      replace_node2_with_old_image
      wait_http_ok "$(hot_endpoint 2)/health" "rollback-node2-live" || true
    fi
    capture_old_node_readback "$(cat "${OUT_DIR}/job-id.txt")"
    head_probe
  else
    log_warn "Skipping terminal polling because no manual transition job was admitted"
  fi
  collect_logs
  summarize
}

main "$@"
