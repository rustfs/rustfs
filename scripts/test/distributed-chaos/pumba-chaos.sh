#!/usr/bin/env bash
set -euo pipefail

PUMBA_IMAGE="${PUMBA_IMAGE:-ghcr.io/alexei-led/pumba:latest}"
TARGET_DEFAULT='re2:^rustfs-node[1-4]$'

usage() {
    cat <<'EOF'
Usage:
  pumba-chaos.sh delay [target_regex] [latency_ms] [duration]
  pumba-chaos.sh loss [target_regex] [probability_0_to_1] [duration]
  pumba-chaos.sh kill [target_regex] [sleep_between_runs] [count]
  pumba-chaos.sh stress [target_regex] [duration] [stress_ng_args]

Examples:
  pumba-chaos.sh delay 're2:^rustfs-node1$' 250 5m
  pumba-chaos.sh loss 're2:^rustfs-node[12]$' 0.10 3m
  pumba-chaos.sh kill 're2:^rustfs-node[1-4]$' 45s 3
  pumba-chaos.sh stress 're2:^rustfs-node2$' 120s '--cpu 4 --timeout 120s --metrics-brief'
EOF
}

require_bin() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "missing required binary: $1" >&2
        exit 1
    fi
}

pumba() {
    docker run --rm -i -v /var/run/docker.sock:/var/run/docker.sock "${PUMBA_IMAGE}" "$@"
}

do_delay() {
    local target="${1:-${TARGET_DEFAULT}}"
    local latency_ms="${2:-250}"
    local duration="${3:-5m}"
    pumba netem --duration "${duration}" delay --time "${latency_ms}" "${target}"
}

do_loss() {
    local target="${1:-${TARGET_DEFAULT}}"
    local probability="${2:-0.10}"
    local duration="${3:-3m}"
    pumba iptables --duration "${duration}" loss --probability "${probability}" "${target}"
}

do_kill() {
    local target="${1:-${TARGET_DEFAULT}}"
    local sleep_gap="${2:-45s}"
    local count="${3:-3}"
    local i
    for ((i = 1; i <= count; i++)); do
        pumba kill "${target}"
        if (( i < count )); then
            sleep "${sleep_gap}"
        fi
    done
}

do_stress() {
    local target="${1:-${TARGET_DEFAULT}}"
    local duration="${2:-120s}"
    local stressors="${3:---cpu 4 --timeout 120s --metrics-brief}"
    pumba stress --duration "${duration}" --stressors="${stressors}" "${target}"
}

main() {
    if [[ $# -lt 1 ]]; then
        usage
        exit 1
    fi

    require_bin docker

    case "$1" in
        delay)
            shift
            do_delay "$@"
            ;;
        loss)
            shift
            do_loss "$@"
            ;;
        kill)
            shift
            do_kill "$@"
            ;;
        stress)
            shift
            do_stress "$@"
            ;;
        *)
            usage
            exit 1
            ;;
    esac
}

main "$@"
