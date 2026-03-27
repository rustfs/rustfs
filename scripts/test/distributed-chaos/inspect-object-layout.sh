#!/usr/bin/env bash
set -euo pipefail

OUT_FILE="${OUT_FILE:-}"
STRICT_EXPECT_FULL_LAYOUT="${STRICT_EXPECT_FULL_LAYOUT:-0}"
DATA_ROOT="${DATA_ROOT:-/data}"
NODES_CSV="${NODES_CSV:-rustfs-node1,rustfs-node2,rustfs-node3,rustfs-node4}"
DISKS_CSV="${DISKS_CSV:-rustfs0,rustfs1,rustfs2,rustfs3}"

usage() {
    cat <<'EOF'
Usage:
  inspect-object-layout.sh <bucket> <object>

Environment:
  OUT_FILE                  Optional file to save the report
  STRICT_EXPECT_FULL_LAYOUT Exit with status 2 when any node/disk copy is missing
  DATA_ROOT                 Root directory inside RustFS containers
  NODES_CSV                 Comma-separated RustFS container names
  DISKS_CSV                 Comma-separated disk directory names under DATA_ROOT
EOF
}

require_bin() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "missing required binary: $1" >&2
        exit 1
    fi
}

report() {
    local bucket="$1"
    local object="$2"
    local -a nodes
    local -a disks
    local total=0
    local present=0
    local node
    local disk
    local path
    local status

    IFS=',' read -r -a nodes <<< "${NODES_CSV}"
    IFS=',' read -r -a disks <<< "${DISKS_CSV}"

    echo "bucket=${bucket}"
    echo "object=${object}"
    echo "data_root=${DATA_ROOT}"
    echo "nodes=${NODES_CSV}"
    echo "disks=${DISKS_CSV}"
    echo
    printf "%-14s %-8s %-7s %s\n" "container" "disk" "status" "details"

    for node in "${nodes[@]}"; do
        for disk in "${disks[@]}"; do
            total=$((total + 1))
            path="${DATA_ROOT}/${disk}/${bucket}/${object}"
            status="$(docker exec "${node}" sh -c "
                if [ -f '${path}/xl.meta' ]; then
                    size=\$(wc -c < '${path}/xl.meta' | tr -d ' ')
                    files=\$(find '${path}' -maxdepth 2 -type f | wc -l | tr -d ' ')
                    echo present xl_meta_bytes=\${size} files=\${files} path='${path}'
                else
                    echo missing path='${path}'
                fi
            " 2>/dev/null || echo "exec-error path='${path}'")"
            if [[ "${status}" == present* ]]; then
                present=$((present + 1))
                printf "%-14s %-8s %-7s %s\n" "${node}" "${disk}" "present" "${status#present }"
            elif [[ "${status}" == missing* ]]; then
                printf "%-14s %-8s %-7s %s\n" "${node}" "${disk}" "missing" "${status#missing }"
            else
                printf "%-14s %-8s %-7s %s\n" "${node}" "${disk}" "error" "${status}"
            fi
        done
    done

    echo
    echo "summary present=${present} missing=$((total - present)) expected=${total}"
}

main() {
    if [[ $# -ne 2 ]]; then
        usage
        exit 1
    fi

    require_bin docker

    local bucket="$1"
    local object="$2"
    local tmp
    tmp="$(mktemp)"
    trap "rm -f '${tmp}'" EXIT

    report "${bucket}" "${object}" | tee "${tmp}"

    if [[ -n "${OUT_FILE}" ]]; then
        mkdir -p "$(dirname "${OUT_FILE}")"
        cp "${tmp}" "${OUT_FILE}"
    fi

    if [[ "${STRICT_EXPECT_FULL_LAYOUT}" == "1" ]]; then
        local summary
        summary="$(tail -n 1 "${tmp}")"
        if [[ "${summary}" != *"missing=0"* ]]; then
            exit 2
        fi
    fi
}

main "$@"
