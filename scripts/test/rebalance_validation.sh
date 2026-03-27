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
  rebalance_validation.sh start <alias>
  rebalance_validation.sh status <alias>
  rebalance_validation.sh wait <alias> [timeout-seconds]
  rebalance_validation.sh stop <alias>

Environment:
  MC_MODE            mc execution mode: auto, binary, docker. Default: auto
  MC_BIN             mc binary path when MC_MODE=binary. Default: mc
  MC_DOCKER_IMAGE    mc image when MC_MODE=docker. Default: minio/mc:latest
  MC_CONFIG_DIR      Persistent mc config directory. Default: .tmp/mc-config
  MC_DOCKER_NETWORK  Docker network used by mc in docker mode. Default: rustfs-decommission-network

Examples:
  ./scripts/test/rebalance_validation.sh start rustfs-decom
  ./scripts/test/rebalance_validation.sh wait rustfs-decom 1800
  ./scripts/test/rebalance_validation.sh status rustfs-decom
  ./scripts/test/rebalance_validation.sh stop rustfs-decom
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

admin_alias() {
    printf '%s\n' "$1"
}

parse_rebalance_state() {
    python3 - <<'PY'
import json
import sys

doc = json.load(sys.stdin)
pools = doc.get("pools") or []
statuses = [(pool.get("status") or "") for pool in pools]
failed = any(status == "Failed" or pool.get("lastError") for pool, status in zip(pools, statuses))
started = any(status == "Started" for status in statuses)
stopped = any(status == "Stopped" for status in statuses)
completed = bool(pools) and all(status in ("Completed", "None", "") for status in statuses)

print(f"started={'true' if started else 'false'}")
print(f"failed={'true' if failed else 'false'}")
print(f"stopped={'true' if stopped else 'false'}")
print(f"completed={'true' if completed else 'false'}")
PY
}

start_rebalance() {
    local alias_name="$1"
    mc_cmd admin rebalance start --json "$(admin_alias "$alias_name")"
}

status_rebalance() {
    local alias_name="$1"
    mc_cmd admin rebalance status --json "$(admin_alias "$alias_name")"
}

wait_rebalance() {
    local alias_name="$1"
    local timeout_seconds="${2:-1800}"
    local elapsed=0
    local interval=5

    while (( elapsed < timeout_seconds )); do
        local json_output=""
        json_output="$(status_rebalance "$alias_name" 2>/dev/null || true)"

        if [[ -n "${json_output}" ]]; then
            printf '%s\n' "${json_output}"
        else
            echo "rebalance status returned empty output" >&2
        fi

        if [[ -n "${json_output}" ]]; then
            local state
            state="$(printf '%s\n' "${json_output}" | parse_rebalance_state)"
            local started failed stopped completed
            started="$(printf '%s\n' "${state}" | sed -n 's/^started=//p')"
            failed="$(printf '%s\n' "${state}" | sed -n 's/^failed=//p')"
            stopped="$(printf '%s\n' "${state}" | sed -n 's/^stopped=//p')"
            completed="$(printf '%s\n' "${state}" | sed -n 's/^completed=//p')"

            if [[ "${failed}" == "true" ]]; then
                echo "rebalance finished with failed=true" >&2
                exit 1
            fi

            if [[ "${stopped}" == "true" ]]; then
                echo "rebalance stopped before completion" >&2
                exit 1
            fi

            if [[ "${completed}" == "true" ]]; then
                echo "rebalance completed successfully"
                return
            fi

            if [[ "${started}" != "true" ]]; then
                echo "rebalance has not entered started state yet" >&2
            fi
        fi

        sleep "${interval}"
        elapsed=$((elapsed + interval))
    done

    echo "timed out waiting for rebalance completion after ${timeout_seconds}s" >&2
    exit 1
}

stop_rebalance() {
    local alias_name="$1"
    mc_cmd admin rebalance stop --json "$(admin_alias "$alias_name")"
}

main() {
    if [[ $# -lt 2 ]]; then
        usage
        exit 1
    fi

    local cmd="$1"
    shift

    case "${cmd}" in
        start)
            if [[ $# -ne 1 ]]; then
                usage
                exit 1
            fi
            start_rebalance "$1"
            ;;
        status)
            if [[ $# -ne 1 ]]; then
                usage
                exit 1
            fi
            status_rebalance "$1"
            ;;
        wait)
            if [[ $# -lt 1 || $# -gt 2 ]]; then
                usage
                exit 1
            fi
            wait_rebalance "$1" "${2:-1800}"
            ;;
        stop)
            if [[ $# -ne 1 ]]; then
                usage
                exit 1
            fi
            stop_rebalance "$1"
            ;;
        *)
            usage
            exit 1
            ;;
    esac
}

main "$@"
