#!/usr/bin/env bash
set -euo pipefail

MC_MODE="${MC_MODE:-auto}"
MC_BIN="${MC_BIN:-mc}"
MC_DOCKER_IMAGE="${MC_DOCKER_IMAGE:-minio/mc:latest}"
MC_CONFIG_DIR="${MC_CONFIG_DIR:-.tmp/mc-config}"
MC_DOCKER_NETWORK="${MC_DOCKER_NETWORK:-rustfs-decommission-network}"

usage() {
    cat <<'EOF'
Usage:
  decommission_validation.sh prepare <alias>
  decommission_validation.sh start <alias> <pool-cmdline>
  decommission_validation.sh status <alias> <pool-cmdline>
  decommission_validation.sh wait <alias> <pool-cmdline> [timeout-seconds]
  decommission_validation.sh verify <alias>
  decommission_validation.sh cancel <alias> [pool-cmdline]

Environment:
  DECOM_STATE_DIR   Directory for generated state files. Default: .tmp/decommission-validation
  DECOM_ADMIN_API   Admin API mode: auto, mc, rustfs. Default: auto
  MC_MODE           mc execution mode: auto, binary, docker. Default: auto

Examples:
  ./scripts/test/decommission_validation.sh prepare rustfs
  ./scripts/test/decommission_validation.sh start rustfs 'http://server{5...8}/disk{1...4}'
  ./scripts/test/decommission_validation.sh wait rustfs 'http://server{5...8}/disk{1...4}' 900
  ./scripts/test/decommission_validation.sh verify rustfs
EOF
}

require_bin() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "missing required binary: $1" >&2
        exit 1
    fi
}

mc_mode() {
    case "${MC_MODE}" in
        auto)
            if command -v "${MC_BIN}" >/dev/null 2>&1; then
                printf 'binary\n'
            else
                printf 'docker\n'
            fi
            ;;
        binary|docker)
            printf '%s\n' "${MC_MODE}"
            ;;
        *)
            echo "unsupported MC_MODE: ${MC_MODE}" >&2
            exit 1
            ;;
    esac
}

mc_cmd() {
    case "$(mc_mode)" in
        binary)
            "${MC_BIN}" "$@"
            ;;
        docker)
            require_bin docker
            mkdir -p "${MC_CONFIG_DIR}"
            docker run --rm -i \
                --network "${MC_DOCKER_NETWORK}" \
                -v "${PWD}:/work" \
                -w /work \
                -v "${MC_CONFIG_DIR}:/root/.mc" \
                "${MC_DOCKER_IMAGE}" "$@"
            ;;
    esac
}

state_dir() {
    printf '%s\n' "${DECOM_STATE_DIR:-.tmp/decommission-validation}"
}

state_file() {
    local alias_name="$1"
    printf '%s/%s.env\n' "$(state_dir)" "$alias_name"
}

load_state() {
    local alias_name="$1"
    local file
    file="$(state_file "$alias_name")"
    if [[ ! -f "$file" ]]; then
        echo "state file not found: $file" >&2
        exit 1
    fi
    # shellcheck disable=SC1090
    source "$file"
}

admin_alias() {
    printf '%s\n' "$1"
}

admin_api_mode() {
    printf '%s\n' "${DECOM_ADMIN_API:-auto}"
}

mc_alias_field() {
    local alias_name="$1"
    local field="$2"
    local alias_json

    alias_json="$(mc_cmd alias list --json 2>/dev/null | grep -F "\"alias\":\"${alias_name}\"" | tail -n 1 || true)"
    if [[ -z "$alias_json" ]]; then
        return 1
    fi

    if command -v jq >/dev/null 2>&1; then
        printf '%s\n' "$alias_json" | jq -r ".${field} // empty"
        return
    fi

    printf '%s\n' "$alias_json" | sed -n "s/.*\"${field}\":\"\\([^\"]*\\)\".*/\\1/p"
}

urlencode() {
    local raw="$1"
    if command -v jq >/dev/null 2>&1; then
        jq -rn --arg v "$raw" '$v|@uri'
        return
    fi

    python3 -c 'import sys, urllib.parse; print(urllib.parse.quote(sys.argv[1], safe=""))' "$raw"
}

rustfs_admin_base() {
    local alias_name="$1"
    local url
    url="$(mc_alias_field "$alias_name" "URL")"
    if [[ -z "$url" ]]; then
        echo "unable to resolve mc alias URL for ${alias_name}" >&2
        exit 1
    fi
    printf '%s/rustfs/admin/v3' "${url%/}"
}

rustfs_admin_request() {
    local alias_name="$1"
    local method="$2"
    local path="$3"
    local access_key secret_key

    access_key="$(mc_alias_field "$alias_name" "accessKey")"
    secret_key="$(mc_alias_field "$alias_name" "secretKey")"

    if [[ -z "$access_key" || -z "$secret_key" ]]; then
        echo "unable to resolve mc alias credentials for ${alias_name}" >&2
        exit 1
    fi

    awscurl \
        --fail-with-body \
        --service s3 \
        --region us-east-1 \
        --access_key "$access_key" \
        --secret_key "$secret_key" \
        -X "$method" \
        "$(rustfs_admin_base "$alias_name")${path}"
}

detect_admin_api() {
    local alias_name="$1"
    local requested
    requested="$(admin_api_mode)"

    case "$requested" in
        mc|rustfs)
            printf '%s\n' "$requested"
            return
            ;;
        auto)
            ;;
        *)
            echo "unsupported DECOM_ADMIN_API mode: ${requested}" >&2
            exit 1
            ;;
    esac

    if ! command -v awscurl >/dev/null 2>&1; then
        printf 'mc\n'
        return
    fi

    if rustfs_admin_request "$alias_name" GET "/pools/list" >/dev/null 2>&1; then
        printf 'rustfs\n'
    else
        printf 'mc\n'
    fi
}

write_state() {
    local alias_name="$1"
    local run_id="$2"
    local basic_bucket="$3"
    local lock_bucket="$4"
    local state_path
    state_path="$(state_file "$alias_name")"
    mkdir -p "$(dirname "$state_path")"
    cat >"$state_path" <<EOF
ALIAS_NAME='$alias_name'
RUN_ID='$run_id'
BASIC_BUCKET='$basic_bucket'
LOCK_BUCKET='$lock_bucket'
EOF
    printf 'state file: %s\n' "$state_path"
}

prepare() {
    local alias_name="$1"

    require_bin dd

    local run_id
    run_id="$(date +%Y%m%d%H%M%S)-$$"
    local basic_bucket="decom-basic-${run_id}"
    local lock_bucket="decom-lock-${run_id}"
    mc_cmd mb "${alias_name}/${basic_bucket}"
    mc_cmd version enable "${alias_name}/${basic_bucket}"
    printf 'alpha-v1\n' | mc_cmd pipe "${alias_name}/${basic_bucket}/single.txt"
    printf 'alpha-v2\n' | mc_cmd pipe "${alias_name}/${basic_bucket}/single.txt"
    dd if=/dev/zero bs=1M count=20 status=none | mc_cmd pipe "${alias_name}/${basic_bucket}/multipart.bin"
    printf 'tombstone\n' | mc_cmd pipe "${alias_name}/${basic_bucket}/delete-marker.txt"
    mc_cmd rm "${alias_name}/${basic_bucket}/delete-marker.txt"

    mc_cmd mb --with-lock "${alias_name}/${lock_bucket}"
    mc_cmd retention set --default GOVERNANCE "1d" "${alias_name}/${lock_bucket}"
    printf 'alpha-v2\n' | mc_cmd pipe "${alias_name}/${lock_bucket}/locked.txt"

    write_state "$alias_name" "$run_id" "$basic_bucket" "$lock_bucket"

    cat <<EOF
prepared buckets:
  basic: ${basic_bucket}
  lock : ${lock_bucket}

next steps:
  ./scripts/test/decommission_validation.sh start ${alias_name} '<pool-cmdline>'
  ./scripts/test/decommission_validation.sh wait ${alias_name} '<pool-cmdline>' 900
  ./scripts/test/decommission_validation.sh verify ${alias_name}
EOF
}

start_decommission() {
    local alias_name="$1"
    local pool_cmdline="$2"
    local mode
    mode="$(detect_admin_api "$alias_name")"

    case "$mode" in
        rustfs)
            require_bin awscurl
            rustfs_admin_request "$alias_name" POST "/pools/decommission?pool=${pool_cmdline}"
            ;;
        mc)
            mc_cmd admin decommission start "$(admin_alias "$alias_name")" "$pool_cmdline"
            ;;
    esac
}

status_decommission() {
    local alias_name="$1"
    local pool_cmdline="$2"
    local mode
    mode="$(detect_admin_api "$alias_name")"

    case "$mode" in
        rustfs)
            require_bin awscurl
            rustfs_admin_request "$alias_name" GET "/pools/status?pool=${pool_cmdline}"
            ;;
        mc)
            mc_cmd admin decommission status "$(admin_alias "$alias_name")" "$pool_cmdline"
            ;;
    esac
}

parse_status_field() {
    local json_input="$1"
    local field="$2"
    if command -v jq >/dev/null 2>&1; then
        printf '%s\n' "$json_input" | jq -r ".. | .${field}? // empty" 2>/dev/null | tail -n 1
    else
        printf '%s\n' "$json_input" | grep -Eo "\"${field}\"[[:space:]]*:[[:space:]]*(true|false)" | tail -n 1 | awk -F: '{gsub(/[[:space:]]/, "", $2); print $2}'
    fi
}

wait_decommission() {
    local alias_name="$1"
    local pool_cmdline="$2"
    local timeout_seconds="${3:-900}"
    local elapsed=0
    local interval=5
    local mode

    mode="$(detect_admin_api "$alias_name")"

    while (( elapsed < timeout_seconds )); do
        local json_output
        case "$mode" in
            rustfs)
                json_output="$(rustfs_admin_request "$alias_name" GET "/pools/status?pool=${pool_cmdline}" 2>/dev/null || true)"
                ;;
            mc)
                json_output="$(mc_cmd admin decommission status --json "$(admin_alias "$alias_name")" "$pool_cmdline" 2>/dev/null || true)"
                ;;
        esac

        if [[ -n "$json_output" ]]; then
            printf '%s\n' "$json_output"
        else
            status_decommission "$alias_name" "$pool_cmdline" || true
        fi

        local complete failed canceled
        complete="$(parse_status_field "$json_output" "complete")"
        failed="$(parse_status_field "$json_output" "failed")"
        canceled="$(parse_status_field "$json_output" "canceled")"

        if [[ "$failed" == "true" ]]; then
            echo "decommission finished with failed=true" >&2
            exit 1
        fi
        if [[ "$canceled" == "true" ]]; then
            echo "decommission finished with canceled=true" >&2
            exit 1
        fi
        if [[ "$complete" == "true" ]]; then
            echo "decommission completed successfully"
            return
        fi

        sleep "$interval"
        elapsed=$((elapsed + interval))
    done

    echo "timed out waiting for decommission completion after ${timeout_seconds}s" >&2
    exit 1
}

verify() {
    local alias_name="$1"
    load_state "$alias_name"

    mc_cmd stat "${alias_name}/${BASIC_BUCKET}/single.txt"
    mc_cmd stat "${alias_name}/${BASIC_BUCKET}/multipart.bin"
    mc_cmd stat "${alias_name}/${LOCK_BUCKET}/locked.txt"

    echo "version listing for ${BASIC_BUCKET}:"
    mc_cmd ls --versions "${alias_name}/${BASIC_BUCKET}"

    echo
    echo "object lock bucket listing for ${LOCK_BUCKET}:"
    mc_cmd ls --versions "${alias_name}/${LOCK_BUCKET}"

    cat <<EOF
basic verification passed:
  - latest single object is readable
  - multipart object is readable
  - object-lock bucket object is readable

manual follow-up:
  - confirm delete-marker history still exists in 'mc ls --versions ${alias_name}/${BASIC_BUCKET}'
  - confirm 'decommission_validation.sh status ${alias_name} ${POOL_CMDLINE:-<pool-cmdline>}' shows complete=true and failed=false
EOF
}

cancel_decommission() {
    local alias_name="$1"
    local pool_cmdline="${2:-}"
    local mode
    mode="$(detect_admin_api "$alias_name")"

    case "$mode" in
        rustfs)
            require_bin awscurl
            if [[ -n "$pool_cmdline" ]]; then
                rustfs_admin_request "$alias_name" POST "/pools/cancel?pool=${pool_cmdline}"
            else
                rustfs_admin_request "$alias_name" POST "/pools/cancel"
            fi
            ;;
        mc)
            if [[ -n "$pool_cmdline" ]]; then
                mc_cmd admin decommission cancel "$(admin_alias "$alias_name")" "$pool_cmdline"
            else
                mc_cmd admin decommission cancel "$(admin_alias "$alias_name")"
            fi
            ;;
    esac
}

main() {
    if [[ $# -lt 2 ]]; then
        usage
        exit 1
    fi

    local cmd="$1"
    shift

    case "$cmd" in
        prepare)
            if [[ $# -ne 1 ]]; then
                usage
                exit 1
            fi
            prepare "$1"
            ;;
        start)
            if [[ $# -ne 2 ]]; then
                usage
                exit 1
            fi
            start_decommission "$1" "$2"
            ;;
        status)
            if [[ $# -ne 2 ]]; then
                usage
                exit 1
            fi
            status_decommission "$1" "$2"
            ;;
        wait)
            if [[ $# -lt 2 || $# -gt 3 ]]; then
                usage
                exit 1
            fi
            wait_decommission "$1" "$2" "${3:-900}"
            ;;
        verify)
            if [[ $# -ne 1 ]]; then
                usage
                exit 1
            fi
            verify "$1"
            ;;
        cancel)
            if [[ $# -lt 1 || $# -gt 2 ]]; then
                usage
                exit 1
            fi
            cancel_decommission "$1" "${2:-}"
            ;;
        *)
            usage
            exit 1
            ;;
    esac
}

main "$@"
