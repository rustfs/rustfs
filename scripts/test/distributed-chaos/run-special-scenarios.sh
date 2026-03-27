#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="/Users/overtrue/www/rustfs"
DIST_DIR="${ROOT_DIR}/scripts/test/distributed-chaos"
ARTIFACT_ROOT="${ARTIFACT_ROOT:-${ROOT_DIR}/artifacts/distributed-chaos/special}"
STAMP="$(date +"%Y%m%d-%H%M%S")"

WARP_MODE="${WARP_MODE:-docker}"
WARP_DOCKER_IMAGE="${WARP_DOCKER_IMAGE:-minio/warp:latest}"

DISTRIBUTED_ENDPOINT="${DISTRIBUTED_ENDPOINT:-http://127.0.0.1:9000}"
DISTRIBUTED_BASE_URL="${DISTRIBUTED_BASE_URL:-http://rustfs-lb:9000}"
DISTRIBUTED_WARP_NETWORK="${DISTRIBUTED_WARP_NETWORK:-rustfs-chaos-net}"
DISTRIBUTED_WARP_HOST="${DISTRIBUTED_WARP_HOST:-rustfs-lb:9000}"

MULTI_POOL_ENDPOINT="${MULTI_POOL_ENDPOINT:-http://127.0.0.1:9100}"
MULTI_POOL_DOCKER_ENDPOINT="${MULTI_POOL_DOCKER_ENDPOINT:-http://rustfs-decommission-latest:9000}"
MULTI_POOL_WARP_NETWORK="${MULTI_POOL_WARP_NETWORK:-rustfs-decommission-network}"
MULTI_POOL_WARP_HOST="${MULTI_POOL_WARP_HOST:-rustfs-decommission-latest:9000}"
MULTI_POOL_ALIAS="${MULTI_POOL_ALIAS:-rustfs-decom}"
MULTI_POOL_DECOMMISSION_POOL="${MULTI_POOL_DECOMMISSION_POOL:-/data/pool1/disk{1...4}}"
MULTI_POOL_CONTAINER="${MULTI_POOL_CONTAINER:-rustfs-decommission-latest}"

SPECIAL_WARMUP_SECONDS="${SPECIAL_WARMUP_SECONDS:-15}"

WRITE_CHAOS_WORKLOAD="${WRITE_CHAOS_WORKLOAD:-small-put}"
WRITE_CHAOS_DURATION="${WRITE_CHAOS_DURATION:-6m}"
WRITE_CHAOS_CONCURRENT="${WRITE_CHAOS_CONCURRENT:-192}"
WRITE_CHAOS_OBJ_SIZE="${WRITE_CHAOS_OBJ_SIZE:-1MiB}"
WRITE_CHAOS_DELAY_DURATION="${WRITE_CHAOS_DELAY_DURATION:-90s}"
WRITE_CHAOS_LOSS_DURATION="${WRITE_CHAOS_LOSS_DURATION:-90s}"
WRITE_CHAOS_STRESS_DURATION="${WRITE_CHAOS_STRESS_DURATION:-120s}"

CONTROL_PLANE_WORKLOAD="${CONTROL_PLANE_WORKLOAD:-small-put}"
CONTROL_PLANE_DURATION="${CONTROL_PLANE_DURATION:-8m}"
CONTROL_PLANE_CONCURRENT="${CONTROL_PLANE_CONCURRENT:-128}"
CONTROL_PLANE_OBJ_SIZE="${CONTROL_PLANE_OBJ_SIZE:-1MiB}"
DECOMMISSION_TIMEOUT_SECONDS="${DECOMMISSION_TIMEOUT_SECONDS:-1800}"
REBALANCE_TIMEOUT_SECONDS="${REBALANCE_TIMEOUT_SECONDS:-1800}"

MULTI_POOL_CHAOS_WORKLOAD="${MULTI_POOL_CHAOS_WORKLOAD:-mixed}"
MULTI_POOL_CHAOS_DURATION="${MULTI_POOL_CHAOS_DURATION:-6m}"
MULTI_POOL_CHAOS_CONCURRENT="${MULTI_POOL_CHAOS_CONCURRENT:-64}"
MULTI_POOL_CHAOS_OBJECTS="${MULTI_POOL_CHAOS_OBJECTS:-4000}"
MULTI_POOL_CHAOS_OBJ_SIZE="${MULTI_POOL_CHAOS_OBJ_SIZE:-8MiB}"
MULTI_POOL_PAUSE_SECONDS="${MULTI_POOL_PAUSE_SECONDS:-15}"
MULTI_POOL_RESTART_AFTER_PAUSE="${MULTI_POOL_RESTART_AFTER_PAUSE:-1}"

declare -a BACKGROUND_PIDS=()

usage() {
    cat <<'EOF'
Usage:
  run-special-scenarios.sh write-chaos
  run-special-scenarios.sh decommission-under-load
  run-special-scenarios.sh rebalance-under-load
  run-special-scenarios.sh multi-pool-chaos

Scenarios:
  write-chaos             Run sustained writes on the 4-node distributed cluster while injecting delay, loss, kill, and stress.
  decommission-under-load Start a local two-pool cluster, keep writes running, and execute pool decommission.
  rebalance-under-load    Start a local two-pool cluster, keep writes running, and execute rebalance.
  multi-pool-chaos        Run a violent mixed workload on the local two-pool cluster, then pause and optionally restart the service.

Artifacts:
  Each scenario writes logs and a summary.md file under artifacts/distributed-chaos/special/<scenario>-<timestamp>/
EOF
}

require_bin() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "missing required binary: $1" >&2
        exit 1
    fi
}

record_health_snapshot() {
    local endpoint="$1"
    local prefix="$2"
    mkdir -p "$(dirname "${prefix}")"
    curl -fsS "${endpoint}/health" > "${prefix}-health.json" 2>&1 || true
    curl -fsS "${endpoint}/health/ready" > "${prefix}-ready.json" 2>&1 || true
}

start_background_command() {
    local log_path="$1"
    shift
    local exit_path="${log_path}.exit_code"
    mkdir -p "$(dirname "${log_path}")"
    (
        set +e
        "$@" > "${log_path}" 2>&1
        echo $? > "${exit_path}"
    ) &
    local pid=$!
    BACKGROUND_PIDS+=("${pid}")
    printf '%s\n' "${pid}"
}

wait_background_command() {
    local pid="$1"
    local log_path="$2"
    local exit_path="${log_path}.exit_code"
    wait "${pid}" || true
    if [[ -f "${exit_path}" ]]; then
        cat "${exit_path}"
    else
        printf '1\n'
    fi
}

cleanup_background_jobs() {
    local pid
    for pid in "${BACKGROUND_PIDS[@]:-}"; do
        if kill -0 "${pid}" >/dev/null 2>&1; then
            kill "${pid}" >/dev/null 2>&1 || true
        fi
    done
}

write_summary() {
    local summary_path="$1"
    shift
    cat > "${summary_path}" <<EOF
$*
EOF
}

ensure_distributed_ready() {
    require_bin curl
    curl -fsS "${DISTRIBUTED_ENDPOINT}/health" >/dev/null
    curl -fsS "${DISTRIBUTED_ENDPOINT}/health/ready" >/dev/null
}

ensure_multi_pool_ready() {
    require_bin curl
    curl -fsS "${MULTI_POOL_ENDPOINT}/health" >/dev/null
    curl -fsS "${MULTI_POOL_ENDPOINT}/health/ready" >/dev/null
}

run_write_chaos() {
    local scenario_dir="${ARTIFACT_ROOT}/write-chaos-${STAMP}"
    mkdir -p "${scenario_dir}"

    ensure_distributed_ready
    record_health_snapshot "${DISTRIBUTED_ENDPOINT}" "${scenario_dir}/health-before"

    local warp_log="${scenario_dir}/warp/console.log"
    local warp_pid
    warp_pid="$(
        start_background_command "${warp_log}" \
            env \
            OUT_DIR="${scenario_dir}/warp" \
            WARP_MODE="${WARP_MODE}" \
            WARP_DOCKER_IMAGE="${WARP_DOCKER_IMAGE}" \
            WARP_DOCKER_NETWORK="${DISTRIBUTED_WARP_NETWORK}" \
            WARP_HOST="${DISTRIBUTED_WARP_HOST}" \
            WARP_SMALL_PUT_DURATION="${WRITE_CHAOS_DURATION}" \
            WARP_SMALL_PUT_CONCURRENT="${WRITE_CHAOS_CONCURRENT}" \
            WARP_SMALL_PUT_OBJ_SIZE="${WRITE_CHAOS_OBJ_SIZE}" \
            "${DIST_DIR}/run-warp.sh" "${WRITE_CHAOS_WORKLOAD}"
    )"

    sleep "${SPECIAL_WARMUP_SECONDS}"

    "${DIST_DIR}/pumba-chaos.sh" delay 're2:^rustfs-node[12]$' 200 "${WRITE_CHAOS_DELAY_DURATION}" \
        > "${scenario_dir}/chaos-delay.log" 2>&1
    "${DIST_DIR}/pumba-chaos.sh" loss 're2:^rustfs-node[34]$' 0.05 "${WRITE_CHAOS_LOSS_DURATION}" \
        > "${scenario_dir}/chaos-loss.log" 2>&1
    "${DIST_DIR}/pumba-chaos.sh" kill 're2:^rustfs-node4$' 1s 1 \
        > "${scenario_dir}/chaos-kill.log" 2>&1
    "${DIST_DIR}/pumba-chaos.sh" stress 're2:^rustfs-node2$' "${WRITE_CHAOS_STRESS_DURATION}" '--cpu 4 --timeout 120s --metrics-brief' \
        > "${scenario_dir}/chaos-stress.log" 2>&1

    local warp_status
    warp_status="$(wait_background_command "${warp_pid}" "${warp_log}")"

    record_health_snapshot "${DISTRIBUTED_ENDPOINT}" "${scenario_dir}/health-after"
    SIGNING_MODE=docker_awscurl BASE_URL="${DISTRIBUTED_BASE_URL}" \
        "${DIST_DIR}/collect-admin-metrics.sh" once \
        > "${scenario_dir}/admin-metrics.log" 2>&1 || true

    local verdict="PASS"
    if [[ "${warp_status}" != "0" ]]; then
        verdict="FAIL"
    fi

    write_summary "${scenario_dir}/summary.md" "# Write Chaos Scenario

- Verdict: \`${verdict}\`
- Endpoint: \`${DISTRIBUTED_ENDPOINT}\`
- Workload: \`${WRITE_CHAOS_WORKLOAD}\`
- Warmup: \`${SPECIAL_WARMUP_SECONDS}s\`
- Warp exit code: \`${warp_status}\`
- Artifacts:
  - Warp log: \`${warp_log}\`
  - Delay log: \`${scenario_dir}/chaos-delay.log\`
  - Loss log: \`${scenario_dir}/chaos-loss.log\`
  - Kill log: \`${scenario_dir}/chaos-kill.log\`
  - Stress log: \`${scenario_dir}/chaos-stress.log\`
  - Admin metrics: \`${scenario_dir}/admin-metrics.log\`
  - Health before: \`${scenario_dir}/health-before-health.json\`, \`${scenario_dir}/health-before-ready.json\`
  - Health after: \`${scenario_dir}/health-after-health.json\`, \`${scenario_dir}/health-after-ready.json\`

This scenario keeps PUT traffic running and then injects delay, packet loss, a single-node kill, and CPU stress."
}

run_decommission_under_load() {
    local scenario_dir="${ARTIFACT_ROOT}/decommission-under-load-${STAMP}"
    mkdir -p "${scenario_dir}"

    MC_MODE=docker MC_CONFIG_DIR="${scenario_dir}/mc-config" MC_DOCKER_NETWORK="${MULTI_POOL_WARP_NETWORK}" MC_DOCKER_ENDPOINT="${MULTI_POOL_DOCKER_ENDPOINT}" \
        "${ROOT_DIR}/scripts/test/decommission_docker.sh" up \
        > "${scenario_dir}/cluster-up.log" 2>&1

    ensure_multi_pool_ready
    record_health_snapshot "${MULTI_POOL_ENDPOINT}" "${scenario_dir}/health-before"

    MC_MODE=docker MC_CONFIG_DIR="${scenario_dir}/mc-config" MC_DOCKER_NETWORK="${MULTI_POOL_WARP_NETWORK}" \
        "${ROOT_DIR}/scripts/test/decommission_validation.sh" prepare "${MULTI_POOL_ALIAS}" \
        > "${scenario_dir}/prepare.log" 2>&1

    local warp_log="${scenario_dir}/warp/console.log"
    local warp_pid
    warp_pid="$(
        start_background_command "${warp_log}" \
            env \
            OUT_DIR="${scenario_dir}/warp" \
            WARP_MODE="${WARP_MODE}" \
            WARP_DOCKER_IMAGE="${WARP_DOCKER_IMAGE}" \
            WARP_DOCKER_NETWORK="${MULTI_POOL_WARP_NETWORK}" \
            WARP_HOST="${MULTI_POOL_WARP_HOST}" \
            WARP_SMALL_PUT_DURATION="${CONTROL_PLANE_DURATION}" \
            WARP_SMALL_PUT_CONCURRENT="${CONTROL_PLANE_CONCURRENT}" \
            WARP_SMALL_PUT_OBJ_SIZE="${CONTROL_PLANE_OBJ_SIZE}" \
            "${DIST_DIR}/run-warp.sh" "${CONTROL_PLANE_WORKLOAD}"
    )"

    sleep "${SPECIAL_WARMUP_SECONDS}"

    MC_MODE=docker MC_CONFIG_DIR="${scenario_dir}/mc-config" MC_DOCKER_NETWORK="${MULTI_POOL_WARP_NETWORK}" \
        "${ROOT_DIR}/scripts/test/decommission_validation.sh" start "${MULTI_POOL_ALIAS}" "${MULTI_POOL_DECOMMISSION_POOL}" \
        > "${scenario_dir}/decommission-start.log" 2>&1
    MC_MODE=docker MC_CONFIG_DIR="${scenario_dir}/mc-config" MC_DOCKER_NETWORK="${MULTI_POOL_WARP_NETWORK}" \
        "${ROOT_DIR}/scripts/test/decommission_validation.sh" wait "${MULTI_POOL_ALIAS}" "${MULTI_POOL_DECOMMISSION_POOL}" "${DECOMMISSION_TIMEOUT_SECONDS}" \
        > "${scenario_dir}/decommission-wait.log" 2>&1
    MC_MODE=docker MC_CONFIG_DIR="${scenario_dir}/mc-config" MC_DOCKER_NETWORK="${MULTI_POOL_WARP_NETWORK}" \
        "${ROOT_DIR}/scripts/test/decommission_validation.sh" verify "${MULTI_POOL_ALIAS}" \
        > "${scenario_dir}/decommission-verify.log" 2>&1

    local warp_status
    warp_status="$(wait_background_command "${warp_pid}" "${warp_log}")"

    record_health_snapshot "${MULTI_POOL_ENDPOINT}" "${scenario_dir}/health-after"

    local verdict="PASS"
    if [[ "${warp_status}" != "0" ]]; then
        verdict="FAIL"
    fi

    write_summary "${scenario_dir}/summary.md" "# Decommission Under Load Scenario

- Verdict: \`${verdict}\`
- Endpoint: \`${MULTI_POOL_ENDPOINT}\`
- Alias: \`${MULTI_POOL_ALIAS}\`
- Pool: \`${MULTI_POOL_DECOMMISSION_POOL}\`
- Warp exit code: \`${warp_status}\`
- Artifacts:
  - Cluster startup: \`${scenario_dir}/cluster-up.log\`
  - Prepare: \`${scenario_dir}/prepare.log\`
  - Decommission start: \`${scenario_dir}/decommission-start.log\`
  - Decommission wait: \`${scenario_dir}/decommission-wait.log\`
  - Decommission verify: \`${scenario_dir}/decommission-verify.log\`
  - Warp log: \`${warp_log}\`
  - Health before: \`${scenario_dir}/health-before-health.json\`, \`${scenario_dir}/health-before-ready.json\`
  - Health after: \`${scenario_dir}/health-after-health.json\`, \`${scenario_dir}/health-after-ready.json\`

This scenario keeps writes active while pool decommission is running on the disposable two-pool environment."
}

run_rebalance_under_load() {
    local scenario_dir="${ARTIFACT_ROOT}/rebalance-under-load-${STAMP}"
    mkdir -p "${scenario_dir}"

    MC_MODE=docker MC_CONFIG_DIR="${scenario_dir}/mc-config" MC_DOCKER_NETWORK="${MULTI_POOL_WARP_NETWORK}" MC_DOCKER_ENDPOINT="${MULTI_POOL_DOCKER_ENDPOINT}" \
        "${ROOT_DIR}/scripts/test/decommission_docker.sh" up \
        > "${scenario_dir}/cluster-up.log" 2>&1

    ensure_multi_pool_ready
    record_health_snapshot "${MULTI_POOL_ENDPOINT}" "${scenario_dir}/health-before"

    MC_MODE=docker MC_CONFIG_DIR="${scenario_dir}/mc-config" MC_DOCKER_NETWORK="${MULTI_POOL_WARP_NETWORK}" \
        "${ROOT_DIR}/scripts/test/decommission_validation.sh" prepare "${MULTI_POOL_ALIAS}" \
        > "${scenario_dir}/prepare.log" 2>&1

    local warp_log="${scenario_dir}/warp/console.log"
    local warp_pid
    warp_pid="$(
        start_background_command "${warp_log}" \
            env \
            OUT_DIR="${scenario_dir}/warp" \
            WARP_MODE="${WARP_MODE}" \
            WARP_DOCKER_IMAGE="${WARP_DOCKER_IMAGE}" \
            WARP_DOCKER_NETWORK="${MULTI_POOL_WARP_NETWORK}" \
            WARP_HOST="${MULTI_POOL_WARP_HOST}" \
            WARP_SMALL_PUT_DURATION="${CONTROL_PLANE_DURATION}" \
            WARP_SMALL_PUT_CONCURRENT="${CONTROL_PLANE_CONCURRENT}" \
            WARP_SMALL_PUT_OBJ_SIZE="${CONTROL_PLANE_OBJ_SIZE}" \
            "${DIST_DIR}/run-warp.sh" "${CONTROL_PLANE_WORKLOAD}"
    )"

    sleep "${SPECIAL_WARMUP_SECONDS}"

    MC_MODE=docker MC_CONFIG_DIR="${scenario_dir}/mc-config" MC_DOCKER_NETWORK="${MULTI_POOL_WARP_NETWORK}" \
        "${ROOT_DIR}/scripts/test/rebalance_validation.sh" start "${MULTI_POOL_ALIAS}" \
        > "${scenario_dir}/rebalance-start.log" 2>&1
    MC_MODE=docker MC_CONFIG_DIR="${scenario_dir}/mc-config" MC_DOCKER_NETWORK="${MULTI_POOL_WARP_NETWORK}" \
        "${ROOT_DIR}/scripts/test/rebalance_validation.sh" wait "${MULTI_POOL_ALIAS}" "${REBALANCE_TIMEOUT_SECONDS}" \
        > "${scenario_dir}/rebalance-wait.log" 2>&1
    MC_MODE=docker MC_CONFIG_DIR="${scenario_dir}/mc-config" MC_DOCKER_NETWORK="${MULTI_POOL_WARP_NETWORK}" \
        "${ROOT_DIR}/scripts/test/decommission_validation.sh" verify "${MULTI_POOL_ALIAS}" \
        > "${scenario_dir}/rebalance-verify.log" 2>&1

    local warp_status
    warp_status="$(wait_background_command "${warp_pid}" "${warp_log}")"

    record_health_snapshot "${MULTI_POOL_ENDPOINT}" "${scenario_dir}/health-after"

    local verdict="PASS"
    if [[ "${warp_status}" != "0" ]]; then
        verdict="FAIL"
    fi

    write_summary "${scenario_dir}/summary.md" "# Rebalance Under Load Scenario

- Verdict: \`${verdict}\`
- Endpoint: \`${MULTI_POOL_ENDPOINT}\`
- Alias: \`${MULTI_POOL_ALIAS}\`
- Warp exit code: \`${warp_status}\`
- Artifacts:
  - Cluster startup: \`${scenario_dir}/cluster-up.log\`
  - Prepare: \`${scenario_dir}/prepare.log\`
  - Rebalance start: \`${scenario_dir}/rebalance-start.log\`
  - Rebalance wait: \`${scenario_dir}/rebalance-wait.log\`
  - Verify: \`${scenario_dir}/rebalance-verify.log\`
  - Warp log: \`${warp_log}\`
  - Health before: \`${scenario_dir}/health-before-health.json\`, \`${scenario_dir}/health-before-ready.json\`
  - Health after: \`${scenario_dir}/health-after-health.json\`, \`${scenario_dir}/health-after-ready.json\`

This scenario keeps writes active while the disposable two-pool environment runs rebalance."
}

run_multi_pool_chaos() {
    local scenario_dir="${ARTIFACT_ROOT}/multi-pool-chaos-${STAMP}"
    mkdir -p "${scenario_dir}"

    MC_MODE=docker MC_CONFIG_DIR="${scenario_dir}/mc-config" MC_DOCKER_NETWORK="${MULTI_POOL_WARP_NETWORK}" MC_DOCKER_ENDPOINT="${MULTI_POOL_DOCKER_ENDPOINT}" \
        "${ROOT_DIR}/scripts/test/decommission_docker.sh" up \
        > "${scenario_dir}/cluster-up.log" 2>&1

    ensure_multi_pool_ready
    record_health_snapshot "${MULTI_POOL_ENDPOINT}" "${scenario_dir}/health-before"

    MC_MODE=docker MC_CONFIG_DIR="${scenario_dir}/mc-config" MC_DOCKER_NETWORK="${MULTI_POOL_WARP_NETWORK}" \
        "${ROOT_DIR}/scripts/test/decommission_validation.sh" prepare "${MULTI_POOL_ALIAS}" \
        > "${scenario_dir}/prepare.log" 2>&1

    local warp_log="${scenario_dir}/warp/console.log"
    local warp_pid
    warp_pid="$(
        start_background_command "${warp_log}" \
            env \
            OUT_DIR="${scenario_dir}/warp" \
            WARP_MODE="${WARP_MODE}" \
            WARP_DOCKER_IMAGE="${WARP_DOCKER_IMAGE}" \
            WARP_DOCKER_NETWORK="${MULTI_POOL_WARP_NETWORK}" \
            WARP_HOST="${MULTI_POOL_WARP_HOST}" \
            WARP_MIXED_DURATION="${MULTI_POOL_CHAOS_DURATION}" \
            WARP_MIXED_CONCURRENT="${MULTI_POOL_CHAOS_CONCURRENT}" \
            WARP_MIXED_OBJECTS="${MULTI_POOL_CHAOS_OBJECTS}" \
            WARP_MIXED_OBJ_SIZE="${MULTI_POOL_CHAOS_OBJ_SIZE}" \
            "${DIST_DIR}/run-warp.sh" "${MULTI_POOL_CHAOS_WORKLOAD}"
    )"

    sleep "${SPECIAL_WARMUP_SECONDS}"
    docker pause "${MULTI_POOL_CONTAINER}" > "${scenario_dir}/pause.log" 2>&1 || true
    sleep "${MULTI_POOL_PAUSE_SECONDS}"
    docker unpause "${MULTI_POOL_CONTAINER}" >> "${scenario_dir}/pause.log" 2>&1 || true

    if [[ "${MULTI_POOL_RESTART_AFTER_PAUSE}" == "1" ]]; then
        docker restart "${MULTI_POOL_CONTAINER}" > "${scenario_dir}/restart.log" 2>&1 || true
        ensure_multi_pool_ready
    fi

    local warp_status
    warp_status="$(wait_background_command "${warp_pid}" "${warp_log}")"

    record_health_snapshot "${MULTI_POOL_ENDPOINT}" "${scenario_dir}/health-after"

    local verdict="PASS"
    if [[ "${warp_status}" != "0" ]]; then
        verdict="FAIL"
    fi

    write_summary "${scenario_dir}/summary.md" "# Multi-Pool Chaos Scenario

- Verdict: \`${verdict}\`
- Endpoint: \`${MULTI_POOL_ENDPOINT}\`
- Container: \`${MULTI_POOL_CONTAINER}\`
- Workload: \`${MULTI_POOL_CHAOS_WORKLOAD}\`
- Warp exit code: \`${warp_status}\`
- Artifacts:
  - Cluster startup: \`${scenario_dir}/cluster-up.log\`
  - Prepare: \`${scenario_dir}/prepare.log\`
  - Pause log: \`${scenario_dir}/pause.log\`
  - Restart log: \`${scenario_dir}/restart.log\`
  - Warp log: \`${warp_log}\`
  - Health before: \`${scenario_dir}/health-before-health.json\`, \`${scenario_dir}/health-before-ready.json\`
  - Health after: \`${scenario_dir}/health-after-health.json\`, \`${scenario_dir}/health-after-ready.json\`

This scenario runs a violent mixed workload on the disposable two-pool environment, pauses the service, and optionally restarts it before verifying recovery."
}

main() {
    trap cleanup_background_jobs EXIT

    if [[ $# -ne 1 ]]; then
        usage
        exit 1
    fi

    require_bin docker
    require_bin curl

    case "$1" in
        write-chaos)
            run_write_chaos
            ;;
        decommission-under-load)
            run_decommission_under_load
            ;;
        rebalance-under-load)
            run_rebalance_under_load
            ;;
        multi-pool-chaos)
            run_multi_pool_chaos
            ;;
        *)
            usage
            exit 1
            ;;
    esac
}

main "$@"
