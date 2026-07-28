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
ROLLBACK_NEW2_TO_OLD=true
WAIT_TIMEOUT_SECS="${WAIT_TIMEOUT_SECS:-180}"
POLL_SECONDS="${POLL_SECONDS:-180}"

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
  --no-rollback                Do not replace node2 with old image after job admission
  -h, --help                   Show help

Environment:
  PROJECT_NAME OLD_IMAGE NEW_IMAGE COLD_IMAGE BASE_PORT OUT_DIR KEEP_UP
  HOT_ACCESS_KEY HOT_SECRET_KEY COLD_ACCESS_KEY COLD_SECRET_KEY
  TIER_NAME TIER_BUCKET TIER_PREFIX JOB_BUCKET JOB_PREFIX OBJECT_COUNT
  WAIT_TIMEOUT_SECS POLL_SECONDS AWS_SIGV4_SCOPE

Artifacts:
  compose.yml, image inspect files, health/readiness logs, API responses,
  terminal status, old-node readback, container logs, summary.env.
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
      --no-rollback)
        ROLLBACK_NEW2_TO_OLD=false
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
    environment: &hot-env
      - RUSTFS_ADDRESS=:9000
      - RUSTFS_ACCESS_KEY=${HOT_ACCESS_KEY}
      - RUSTFS_SECRET_KEY=${HOT_SECRET_KEY}
      - RUSTFS_VOLUMES=$(hot_volumes)
      - RUSTFS_SCANNER_ENABLED=false
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
  local query response job_id
  query="bucket=$(url_encode "$JOB_BUCKET")"
  query="${query}&prefix=$(url_encode "${JOB_PREFIX}/")"
  query="${query}&tier=$(url_encode "$TIER_NAME")"
  query="${query}&dryRun=false&maxObjects=${OBJECT_COUNT}&mode=async"
  response="$(curl_hot POST "$(hot_endpoint 2)/rustfs/admin/v3/ilm/transition/run?${query}")"
  printf '%s\n' "$response" >"${OUT_DIR}/run-response.json"
  job_id="$(printf '%s' "$response" | jq -r '.job_id // empty')"
  if [[ -z "$job_id" ]]; then
    log_error "manual transition response omitted job_id"
    return 1
  fi
  printf '%s\n' "$job_id" >"${OUT_DIR}/job-id.txt"
}

replace_node2_with_old_image() {
  local network
  network="$(network_name)"
  log_info "Replacing node2 with old image ${OLD_IMAGE} for in-flight rollback readback"
  compose stop node2 >/dev/null
  compose rm -f node2 >/dev/null
  docker run -d \
    --name "${PROJECT_NAME}-node2-rollback-old" \
    --network "$network" \
    --network-alias node2 \
    -p "$((BASE_PORT + 2)):9000" \
    -e RUSTFS_ADDRESS=:9000 \
    -e RUSTFS_ACCESS_KEY="$HOT_ACCESS_KEY" \
    -e RUSTFS_SECRET_KEY="$HOT_SECRET_KEY" \
    -e RUSTFS_VOLUMES="$(hot_volumes)" \
    -e RUSTFS_SCANNER_ENABLED=false \
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
  local terminal_state transition_completed tier_failure transition_failed old_code rollback_code
  terminal_state="$(cat "${OUT_DIR}/terminal-state.txt" 2>/dev/null || true)"
  transition_completed="$(jq -r '.report.transition_completed // 0' "${OUT_DIR}/status-terminal.json" 2>/dev/null || printf '0')"
  tier_failure="$(jq -r '.report.tier_failure // 0' "${OUT_DIR}/status-terminal.json" 2>/dev/null || printf '0')"
  transition_failed="$(jq -r '.report.transition_failed // 0' "${OUT_DIR}/status-terminal.json" 2>/dev/null || printf '0')"
  old_code="$(cat "${OUT_DIR}/old-node-status.http_code" 2>/dev/null || true)"
  rollback_code="$(cat "${OUT_DIR}/rollback-node2-status.http_code" 2>/dev/null || true)"
  cat >"${OUT_DIR}/summary.env" <<EOF
project_name=${PROJECT_NAME}
old_image=${OLD_IMAGE}
new_image=${NEW_IMAGE}
cold_image=${COLD_IMAGE}
base_port=${BASE_PORT}
tier=${TIER_NAME}
job_bucket=${JOB_BUCKET}
job_prefix=${JOB_PREFIX}
object_count=${OBJECT_COUNT}
terminal_state=${terminal_state}
transition_completed=${transition_completed}
transition_failed=${transition_failed}
tier_failure=${tier_failure}
old_node_status_http_code=${old_code}
rollback_node2_status_http_code=${rollback_code}
rollback_new2_to_old=${ROLLBACK_NEW2_TO_OLD}
EOF
  cat "${OUT_DIR}/summary.env"
  if [[ "$terminal_state" != "completed" || "$transition_completed" == "0" || "$transition_failed" != "0" || "$tier_failure" != "0" ]]; then
    log_error "strict #1508 mixed-version transition gate failed; see ${OUT_DIR}"
    return 1
  fi
}

main() {
  parse_args "$@"
  parse_positive_int "--base-port" "$BASE_PORT"
  parse_positive_int "--object-count" "$OBJECT_COUNT"
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
  wait_cluster_ready

  curl_cold PUT "$(cold_endpoint)/${TIER_BUCKET}" -o "${OUT_DIR}/create-cold-bucket.response" -w "%{http_code}\n" >"${OUT_DIR}/create-cold-bucket.http_code"
  create_bucket "$(hot_endpoint 2)" "$JOB_BUCKET" "${OUT_DIR}/create-hot-bucket"
  add_tier
  put_lifecycle
  seed_objects
  start_transition_job
  if [[ "$ROLLBACK_NEW2_TO_OLD" == "true" ]]; then
    replace_node2_with_old_image
  fi
  poll_terminal_status "$(cat "${OUT_DIR}/job-id.txt")"
  capture_old_node_readback "$(cat "${OUT_DIR}/job-id.txt")"
  head_probe
  collect_logs
  summarize
}

main "$@"
