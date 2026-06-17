#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.decommission.yml}"
RUSTFS_SERVICE_NAME="${RUSTFS_SERVICE_NAME:-rustfs-decommission-latest}"
TIER_SERVICE_NAME="${TIER_SERVICE_NAME:-rustfs-decommission-tier}"
MC_ALIAS_NAME="${MC_ALIAS_NAME:-rustfs-decom}"
TIER_ALIAS_NAME="${TIER_ALIAS_NAME:-rustfs-decom-tier}"
S3_ENDPOINT="${S3_ENDPOINT:-http://127.0.0.1:9100}"
TIER_ENDPOINT="${TIER_ENDPOINT:-http://127.0.0.1:9200}"
TIER_ENDPOINT_FROM_RUSTFS="${TIER_ENDPOINT_FROM_RUSTFS:-http://rustfs-decommission-tier:9000}"
ACCESS_KEY="${ACCESS_KEY:-rustfsadmin}"
SECRET_KEY="${SECRET_KEY:-rustfsadmin}"
TIER_ACCESS_KEY="${TIER_ACCESS_KEY:-minioadmin}"
TIER_SECRET_KEY="${TIER_SECRET_KEY:-minioadmin}"
TIER_NAME="${TIER_NAME:-COLDTIER}"
TIER_BUCKET="${TIER_BUCKET:-rustfs-tier}"
TIER_PREFIX="${TIER_PREFIX:-decommission-tier}"
POOL_CMDLINE="${POOL_CMDLINE:-/data/pool1/disk{1...4}}"
STATE_DIR="${DECOM_TIER_STATE_DIR:-.tmp/decommission-tier-lifecycle}"
OBJECT_COUNT="${OBJECT_COUNT:-12}"
TRANSITION_DAYS="${TRANSITION_DAYS:-0}"
EXPIRY_DAYS="${EXPIRY_DAYS:-0}"
WAIT_TIMEOUT_SECONDS="${WAIT_TIMEOUT_SECONDS:-900}"
POLL_INTERVAL_SECONDS="${POLL_INTERVAL_SECONDS:-5}"
GET_WORKERS="${GET_WORKERS:-8}"
GET_LOAD_SECONDS="${GET_LOAD_SECONDS:-60}"
SCANNER_SAMPLES="${SCANNER_SAMPLES:-3}"
SCANNER_INTERVAL_SECONDS="${SCANNER_INTERVAL_SECONDS:-10}"

usage() {
    cat <<'EOF'
Usage:
  decommission_tier_lifecycle_recovery.sh up
  decommission_tier_lifecycle_recovery.sh prepare
  decommission_tier_lifecycle_recovery.sh wait-transition
  decommission_tier_lifecycle_recovery.sh expire-recovery
  decommission_tier_lifecycle_recovery.sh decommission
  decommission_tier_lifecycle_recovery.sh verify
  decommission_tier_lifecycle_recovery.sh run
  decommission_tier_lifecycle_recovery.sh status
  decommission_tier_lifecycle_recovery.sh down
  decommission_tier_lifecycle_recovery.sh reset

Environment:
  COMPOSE_FILE              Compose file. Default: docker-compose.decommission.yml
  MC_ALIAS_NAME             RustFS mc alias. Default: rustfs-decom
  TIER_ALIAS_NAME           tier backend mc alias. Default: rustfs-decom-tier
  S3_ENDPOINT               RustFS endpoint from host. Default: http://127.0.0.1:9100
  TIER_ENDPOINT             MinIO tier endpoint from host. Default: http://127.0.0.1:9200
  TIER_ENDPOINT_FROM_RUSTFS MinIO endpoint reachable by RustFS container.
  TIER_NAME                 RustFS tier name. Must be uppercase. Default: COLDTIER
  OBJECT_COUNT              Objects per lifecycle/decommission prefix. Default: 12
  TRANSITION_DAYS           Lifecycle transition days. Default: 0
  EXPIRY_DAYS               Lifecycle expiry days for expire/ objects. Default: 0
  WAIT_TIMEOUT_SECONDS      Poll timeout for transition/expiry/decommission. Default: 900

The run target starts a local RustFS two-pool deployment plus a disposable
MinIO tier backend, transitions two object prefixes, restarts RustFS to exercise
recovery, expires one prefix while concurrent GET runs, then decommissions the
second prefix's pool and verifies remote tier data was not removed.
EOF
}

require_bin() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "missing required binary: $1" >&2
        exit 1
    fi
}

is_positive_integer() {
    [[ "$1" =~ ^[1-9][0-9]*$ ]]
}

is_nonnegative_integer() {
    [[ "$1" =~ ^[0-9]+$ ]]
}

validate_numeric_env() {
    if ! is_positive_integer "$OBJECT_COUNT"; then
        echo "OBJECT_COUNT must be a positive integer" >&2
        exit 1
    fi
    if ! is_nonnegative_integer "$TRANSITION_DAYS" || ! is_nonnegative_integer "$EXPIRY_DAYS"; then
        echo "TRANSITION_DAYS and EXPIRY_DAYS must be nonnegative integers" >&2
        exit 1
    fi
    if ! is_positive_integer "$WAIT_TIMEOUT_SECONDS" || ! is_positive_integer "$POLL_INTERVAL_SECONDS"; then
        echo "WAIT_TIMEOUT_SECONDS and POLL_INTERVAL_SECONDS must be positive integers" >&2
        exit 1
    fi
}

compose() {
    docker compose -f "$COMPOSE_FILE" "$@"
}

state_file() {
    printf '%s/state.env\n' "$STATE_DIR"
}

work_dir() {
    printf '%s/work\n' "$STATE_DIR"
}

load_state() {
    local file
    file="$(state_file)"
    if [[ ! -f "$file" ]]; then
        echo "state file not found: $file; run prepare first" >&2
        exit 1
    fi
    # shellcheck disable=SC1090
    source "$file"
}

write_state() {
    mkdir -p "$STATE_DIR"
    cat >"$(state_file)" <<EOF
RUN_ID='$RUN_ID'
TEST_BUCKET='$TEST_BUCKET'
EXPIRY_PREFIX='$EXPIRY_PREFIX'
DECOM_PREFIX='$DECOM_PREFIX'
TIER_NAME='$TIER_NAME'
TIER_BUCKET='$TIER_BUCKET'
TIER_PREFIX='$TIER_PREFIX'
REMOTE_AFTER_TRANSITION='${REMOTE_AFTER_TRANSITION:-0}'
REMOTE_BEFORE_DECOMMISSION='${REMOTE_BEFORE_DECOMMISSION:-0}'
EOF
}

update_state_value() {
    local key="$1"
    local value="$2"
    load_state
    case "$key" in
        REMOTE_AFTER_TRANSITION) REMOTE_AFTER_TRANSITION="$value" ;;
        REMOTE_BEFORE_DECOMMISSION) REMOTE_BEFORE_DECOMMISSION="$value" ;;
        *)
            echo "unsupported state key: $key" >&2
            exit 1
            ;;
    esac
    write_state
}

urlencode() {
    local raw="$1"
    if command -v jq >/dev/null 2>&1; then
        jq -rn --arg v "$raw" '$v|@uri'
        return
    fi
    python3 -c 'import sys, urllib.parse; print(urllib.parse.quote(sys.argv[1], safe=""))' "$raw"
}

admin_request() {
    local method="$1"
    local path="$2"
    local body_file="${3:-}"
    local url="${S3_ENDPOINT%/}/rustfs/admin/v3${path}"

    if [[ -n "$body_file" ]]; then
        local body
        body="$(<"$body_file")"
        awscurl \
            --fail-with-body \
            --service s3 \
            --region us-east-1 \
            --access_key "$ACCESS_KEY" \
            --secret_key "$SECRET_KEY" \
            -H "Content-Type: application/json" \
            -X "$method" \
            -d "$body" \
            "$url"
    else
        awscurl \
            --fail-with-body \
            --service s3 \
            --region us-east-1 \
            --access_key "$ACCESS_KEY" \
            --secret_key "$SECRET_KEY" \
            -X "$method" \
            "$url"
    fi
}

wait_container_ready() {
    local service="$1"
    local elapsed=0
    local container_id
    container_id="$(compose ps -q "$service")"
    if [[ -z "$container_id" ]]; then
        echo "container not found for service $service" >&2
        exit 1
    fi

    while (( elapsed < WAIT_TIMEOUT_SECONDS )); do
        local status
        status="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$container_id")"
        if [[ "$status" == "healthy" || "$status" == "running" ]]; then
            echo "$service is $status"
            return
        fi
        sleep "$POLL_INTERVAL_SECONDS"
        elapsed=$((elapsed + POLL_INTERVAL_SECONDS))
    done

    echo "$service did not become ready within ${WAIT_TIMEOUT_SECONDS}s" >&2
    compose logs --tail=200 "$service" || true
    exit 1
}

configure_aliases() {
    require_bin mc
    mc alias set "$MC_ALIAS_NAME" "$S3_ENDPOINT" "$ACCESS_KEY" "$SECRET_KEY" >/dev/null
    mc alias set "$TIER_ALIAS_NAME" "$TIER_ENDPOINT" "$TIER_ACCESS_KEY" "$TIER_SECRET_KEY" >/dev/null
    echo "configured aliases: $MC_ALIAS_NAME, $TIER_ALIAS_NAME"
}

wait_tier_backend_ready() {
    local elapsed=0
    while (( elapsed < WAIT_TIMEOUT_SECONDS )); do
        if mc alias set "$TIER_ALIAS_NAME" "$TIER_ENDPOINT" "$TIER_ACCESS_KEY" "$TIER_SECRET_KEY" >/dev/null 2>&1 \
            && mc ls "$TIER_ALIAS_NAME" >/dev/null 2>&1; then
            echo "tier backend is ready"
            return
        fi
        sleep "$POLL_INTERVAL_SECONDS"
        elapsed=$((elapsed + POLL_INTERVAL_SECONDS))
    done

    echo "tier backend did not become ready within ${WAIT_TIMEOUT_SECONDS}s" >&2
    compose logs --tail=200 "$TIER_SERVICE_NAME" || true
    exit 1
}

up() {
    require_bin docker
    require_bin mc
    mkdir -p deploy/data/decommission deploy/logs/decommission deploy/data/decommission-tier
    compose up -d "$TIER_SERVICE_NAME" "$RUSTFS_SERVICE_NAME"
    wait_container_ready "$TIER_SERVICE_NAME"
    wait_tier_backend_ready
    wait_container_ready "$RUSTFS_SERVICE_NAME"
    configure_aliases
    mc mb --ignore-existing "${TIER_ALIAS_NAME}/${TIER_BUCKET}"
}

down() {
    require_bin docker
    compose down
}

reset() {
    require_bin docker
    compose down -v --remove-orphans || true
    rm -rf deploy/data/decommission deploy/logs/decommission deploy/data/decommission-tier "$STATE_DIR"
    echo "tier lifecycle decommission environment reset"
}

write_tier_config() {
    local file="$1"
    cat >"$file" <<EOF
{
  "type": "minio",
  "minio": {
    "name": "${TIER_NAME}",
    "endpoint": "${TIER_ENDPOINT_FROM_RUSTFS}",
    "accessKey": "${TIER_ACCESS_KEY}",
    "secretKey": "${TIER_SECRET_KEY}",
    "bucket": "${TIER_BUCKET}",
    "prefix": "${TIER_PREFIX}/${RUN_ID}",
    "region": "us-east-1"
  }
}
EOF
}

write_lifecycle_config() {
    local file="$1"
    local include_expiry="$2"
    local expiry_block=""
    if [[ "$include_expiry" == "true" ]]; then
        expiry_block='
      "Expiration": {
        "Days": '"${EXPIRY_DAYS}"'
      },'
    fi

    cat >"$file" <<EOF
{
  "Rules": [
    {
      "ID": "transition-expire-prefix",
      "Status": "Enabled",
      "Filter": {
        "Prefix": "${EXPIRY_PREFIX}"
      },${expiry_block}
      "Transition": {
        "Days": ${TRANSITION_DAYS},
        "StorageClass": "${TIER_NAME}"
      }
    },
    {
      "ID": "transition-decommission-prefix",
      "Status": "Enabled",
      "Filter": {
        "Prefix": "${DECOM_PREFIX}"
      },
      "Transition": {
        "Days": ${TRANSITION_DAYS},
        "StorageClass": "${TIER_NAME}"
      }
    }
  ]
}
EOF
}

prepare() {
    require_bin mc
    require_bin awscurl
    validate_numeric_env

    RUN_ID="$(date -u +%Y%m%d%H%M%S)-$$"
    TEST_BUCKET="decom-tier-${RUN_ID}"
    EXPIRY_PREFIX="expire/"
    DECOM_PREFIX="decom/"
    REMOTE_AFTER_TRANSITION=0
    REMOTE_BEFORE_DECOMMISSION=0

    local dir
    dir="$(work_dir)"
    mkdir -p "$dir"
    write_state

    local tier_config lifecycle_config payload
    tier_config="${dir}/tier-config.json"
    lifecycle_config="${dir}/lifecycle-transition.json"
    payload="${dir}/payload.txt"

    write_tier_config "$tier_config"
    admin_request DELETE "/tier/$(urlencode "$TIER_NAME")?force=true" >/dev/null 2>&1 || true
    admin_request PUT "/tier?force=true" "$tier_config"
    admin_request GET "/tier/$(urlencode "$TIER_NAME")" >/dev/null

    mc mb --ignore-existing "${MC_ALIAS_NAME}/${TEST_BUCKET}"
    write_lifecycle_config "$lifecycle_config" false
    mc ilm import "${MC_ALIAS_NAME}/${TEST_BUCKET}" <"$lifecycle_config"

    for i in $(seq 1 "$OBJECT_COUNT"); do
        printf 'expiry object %s run %s\n' "$i" "$RUN_ID" >"$payload"
        mc cp "$payload" "${MC_ALIAS_NAME}/${TEST_BUCKET}/${EXPIRY_PREFIX}obj-${i}.txt" >/dev/null
        printf 'decommission object %s run %s\n' "$i" "$RUN_ID" >"$payload"
        mc cp "$payload" "${MC_ALIAS_NAME}/${TEST_BUCKET}/${DECOM_PREFIX}obj-${i}.txt" >/dev/null
    done

    echo "prepared bucket: ${TEST_BUCKET}"
}

remote_count() {
    load_state
    mc find "${TIER_ALIAS_NAME}/${TIER_BUCKET}/${TIER_PREFIX}/${RUN_ID}" --type f 2>/dev/null | wc -l | tr -d ' '
}

wait_transition() {
    require_bin mc
    load_state

    local expected elapsed count
    expected=$((OBJECT_COUNT * 2))
    elapsed=0
    while (( elapsed < WAIT_TIMEOUT_SECONDS )); do
        count="$(remote_count)"
        echo "remote tier object count: ${count}/${expected}"
        if (( count >= expected )); then
            update_state_value REMOTE_AFTER_TRANSITION "$count"
            echo "transition completed"
            return
        fi
        sleep "$POLL_INTERVAL_SECONDS"
        elapsed=$((elapsed + POLL_INTERVAL_SECONDS))
    done

    echo "timed out waiting for transitioned objects in tier backend" >&2
    exit 1
}

start_get_load() {
    load_state
    local dir errors pids_file
    dir="$(work_dir)"
    mkdir -p "$dir"
    errors="${dir}/get-errors.log"
    pids_file="${dir}/get-load.pids"
    : >"$errors"
    : >"$pids_file"

    for worker in $(seq 1 "$GET_WORKERS"); do
        (
            end_at=$((SECONDS + GET_LOAD_SECONDS))
            while (( SECONDS < end_at )); do
                idx=$(( (RANDOM % OBJECT_COUNT) + 1 ))
                mc cat "${MC_ALIAS_NAME}/${TEST_BUCKET}/${EXPIRY_PREFIX}obj-${idx}.txt" >/dev/null 2>>"$errors" || true
            done
        ) &
        echo "$!" >>"$pids_file"
    done
}

wait_get_load() {
    local pids_file
    pids_file="$(work_dir)/get-load.pids"
    if [[ ! -f "$pids_file" ]]; then
        return
    fi
    while read -r pid; do
        [[ -z "$pid" ]] && continue
        wait "$pid" || true
    done <"$pids_file"
}

assert_no_bad_get_errors() {
    local errors
    errors="$(work_dir)/get-errors.log"
    if [[ ! -f "$errors" ]]; then
        return
    fi
    if grep -E 'NoSuchVersion|remote.*NoSuch|invalid.*version' "$errors" >/dev/null 2>&1; then
        echo "concurrent GET observed invalid version-state errors:" >&2
        grep -E 'NoSuchVersion|remote.*NoSuch|invalid.*version' "$errors" >&2
        exit 1
    fi
}

restart_rustfs() {
    require_bin docker
    compose restart "$RUSTFS_SERVICE_NAME"
    wait_container_ready "$RUSTFS_SERVICE_NAME"
    configure_aliases
}

expired_count() {
    load_state
    local expired=0
    for i in $(seq 1 "$OBJECT_COUNT"); do
        if ! mc stat "${MC_ALIAS_NAME}/${TEST_BUCKET}/${EXPIRY_PREFIX}obj-${i}.txt" >/dev/null 2>&1; then
            expired=$((expired + 1))
        fi
    done
    printf '%s\n' "$expired"
}

wait_expiry_and_cleanup() {
    load_state
    local dir lifecycle_config before expected_max elapsed expired count
    dir="$(work_dir)"
    lifecycle_config="${dir}/lifecycle-expire.json"
    write_lifecycle_config "$lifecycle_config" true

    before="$(remote_count)"
    mc ilm import "${MC_ALIAS_NAME}/${TEST_BUCKET}" <"$lifecycle_config"
    restart_rustfs
    start_get_load

    expected_max=$((before - OBJECT_COUNT))
    if (( expected_max < 0 )); then
        expected_max=0
    fi

    elapsed=0
    while (( elapsed < WAIT_TIMEOUT_SECONDS )); do
        expired="$(expired_count)"
        count="$(remote_count)"
        echo "expiry progress: expired=${expired}/${OBJECT_COUNT}, remote=${count}, target_remote<=${expected_max}"
        if (( expired >= OBJECT_COUNT && count <= expected_max )); then
            wait_get_load
            assert_no_bad_get_errors
            echo "lifecycle expiry and remote cleanup completed"
            return
        fi
        sleep "$POLL_INTERVAL_SECONDS"
        elapsed=$((elapsed + POLL_INTERVAL_SECONDS))
    done

    wait_get_load
    assert_no_bad_get_errors
    echo "timed out waiting for lifecycle expiry cleanup" >&2
    exit 1
}

run_decommission() {
    require_bin mc
    load_state
    local before
    before="$(remote_count)"
    update_state_value REMOTE_BEFORE_DECOMMISSION "$before"
    ./scripts/test/decommission_validation.sh start "$MC_ALIAS_NAME" "$POOL_CMDLINE"
    ./scripts/test/decommission_validation.sh wait "$MC_ALIAS_NAME" "$POOL_CMDLINE" "$WAIT_TIMEOUT_SECONDS"
}

verify_decommission() {
    load_state
    local after
    for i in $(seq 1 "$OBJECT_COUNT"); do
        mc stat "${MC_ALIAS_NAME}/${TEST_BUCKET}/${DECOM_PREFIX}obj-${i}.txt" >/dev/null
    done

    after="$(remote_count)"
    if (( after < REMOTE_BEFORE_DECOMMISSION )); then
        echo "remote tier object count decreased during decommission: before=${REMOTE_BEFORE_DECOMMISSION}, after=${after}" >&2
        exit 1
    fi

    echo "decommission verification passed; remote tier count before=${REMOTE_BEFORE_DECOMMISSION}, after=${after}"
}

capture_scanner_status() {
    require_bin awscurl
    load_state
    local out_dir
    out_dir="$(work_dir)/scanner-status"
    mkdir -p "$out_dir"

    for i in $(seq 1 "$SCANNER_SAMPLES"); do
        admin_request GET "/scanner/status" >"${out_dir}/scanner-status-${i}.json" || true
        sleep "$SCANNER_INTERVAL_SECONDS"
    done
    echo "scanner status samples: ${out_dir}"
}

capture_decommission_status() {
    load_state
    local out_dir
    out_dir="$(work_dir)/decommission-status"
    mkdir -p "$out_dir"
    ./scripts/test/decommission_validation.sh status "$MC_ALIAS_NAME" "$POOL_CMDLINE" >"${out_dir}/status.txt" 2>&1 || true
    echo "decommission status sample: ${out_dir}/status.txt"
}

status() {
    load_state
    echo "bucket: ${TEST_BUCKET}"
    echo "tier backend objects: $(remote_count)"
    admin_request GET "/tier-stats?tier=$(urlencode "$TIER_NAME")" || true
    echo
    ./scripts/test/decommission_validation.sh status "$MC_ALIAS_NAME" "$POOL_CMDLINE" || true
}

verify() {
    load_state
    assert_no_bad_get_errors
    verify_decommission
    capture_decommission_status
    capture_scanner_status
    cat <<EOF
verification passed:
  - expire/ objects are expired and remote tier cleanup reached the expected count
  - concurrent GET did not emit NoSuchVersion or invalid version-state errors
  - decom/ tiered objects remain readable after decommission
  - remote tier object count did not decrease during decommission

manual evidence:
  - inspect $(work_dir)/scanner-status for missed/recovery counters
  - inspect $(work_dir)/get-errors.log for any unexpected non-version errors
EOF
}

run_all() {
    up
    prepare
    wait_transition
    wait_expiry_and_cleanup
    run_decommission
    verify
}

main() {
    if [[ $# -ne 1 ]]; then
        usage
        exit 1
    fi

    case "$1" in
        up) up ;;
        prepare) prepare ;;
        wait-transition) wait_transition ;;
        expire-recovery) wait_expiry_and_cleanup ;;
        decommission) run_decommission ;;
        verify) verify ;;
        run) run_all ;;
        status) status ;;
        down) down ;;
        reset) reset ;;
        -h|--help) usage ;;
        *)
            usage
            exit 1
            ;;
    esac
}

main "$@"
