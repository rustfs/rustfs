#!/usr/bin/env bash
set -euo pipefail

# One-click A/B benchmark driver for the internode gRPC optimization stages (P0-P3).
#
# For a given --stage and --phase it:
#   1. computes the RUSTFS_INTERNODE_* *server* env for that stage/phase (per the runbook matrix),
#   2. writes it to <out-dir>/server-env.sh and prints it,
#   3. runs the appropriate underlying bench into a labeled <out-dir>.
#
# IMPORTANT: RUSTFS_INTERNODE_* are SERVER env. The load-driven stages (p0/p1/p2) benchmark an
# ALREADY-RUNNING cluster, so you must (re)start rustfs on every node with the emitted env BEFORE
# running the "after" (and the baseline "before"). This script cannot change a running server.
# The p3 (docker four-node) path exports the env so a compose that forwards it can pick it up.
#
# See docs/operations/internode-grpc-benchmark-runbook.md for metrics and acceptance gates.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
TRANSPORT_BENCH="${PROJECT_ROOT}/scripts/run_internode_transport_baseline.sh"
FAILOVER_BENCH="${PROJECT_ROOT}/scripts/run_four_node_cluster_failover_bench.sh"

STAGE=""
PHASE=""
OUT_ROOT="${PROJECT_ROOT}/target/bench/internode-transport"
DRY_RUN=false
LIFECYCLE_FEATURE="both"
TRANSPORT_FEATURE="all"
PASSTHROUGH=()

usage() {
  cat <<'USAGE'
Usage:
  scripts/run_internode_grpc_ab_bench.sh --stage <p0|p1|p2|p3> --phase <before|after|request-only|canary|rollback> [options] [-- <bench args>]

Required:
  --stage <p0|p1|p2|p3>     Which optimization stage to A/B.
  --phase <phase>           baseline or enabled phase. p0/p1/p3 accept before|after; p2 also accepts request-only|canary|rollback.

Options:
  --out-root <dir>          Artifact root. Default: target/bench/internode-transport
  --dry-run                 Print the env + the command, do not run the bench.
  --lifecycle-feature <prewarm|offline|both>
                            P3 feature to enable. Default: both (legacy combined experiment).
  --transport-feature <nodelay|stream-window|connection-window|max-message|all>
                            P0 parameter to compare. Default: all (legacy combined experiment).
  -h, --help                Show this help.

Everything after `--` is passed through to the underlying bench:
  p0/p1/p2 -> run_internode_transport_baseline.sh  (e.g. --access-key AK --secret-key SK --metrics-url URL)
  p3       -> run_four_node_cluster_failover_bench.sh

Examples:
  # P1 baseline then enabled (restart the cluster with the emitted env between the two):
  scripts/run_internode_grpc_ab_bench.sh --stage p1 --phase before -- --access-key AK --secret-key SK
  scripts/run_internode_grpc_ab_bench.sh --stage p1 --phase after  -- --access-key AK --secret-key SK

  # P3 failover A/B (docker four-node):
  scripts/run_internode_grpc_ab_bench.sh --stage p3 --phase after
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --stage) STAGE="$2"; shift 2 ;;
    --phase) PHASE="$2"; shift 2 ;;
    --out-root) OUT_ROOT="$2"; shift 2 ;;
    --dry-run) DRY_RUN=true; shift ;;
    --lifecycle-feature) LIFECYCLE_FEATURE="$2"; shift 2 ;;
    --transport-feature)
      [[ $# -ge 2 ]] || { echo "ERROR: --transport-feature requires a value" >&2; exit 2; }
      TRANSPORT_FEATURE="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    --) shift; PASSTHROUGH=("$@"); break ;;
    *) echo "Unknown argument: $1" >&2; usage; exit 2 ;;
  esac
done

[[ -n "${STAGE}" && -n "${PHASE}" ]] || { echo "ERROR: --stage and --phase are required" >&2; usage; exit 2; }
case "${LIFECYCLE_FEATURE}" in prewarm|offline|both) ;; *) echo "ERROR: invalid --lifecycle-feature" >&2; exit 2 ;; esac
case "${TRANSPORT_FEATURE}" in nodelay|stream-window|connection-window|max-message|all) ;; *) echo "ERROR: invalid --transport-feature" >&2; exit 2 ;; esac
if [[ "${STAGE}" != "p0" && "${TRANSPORT_FEATURE}" != "all" ]]; then
  echo "ERROR: --transport-feature is only valid for --stage p0" >&2
  exit 2
fi
if [[ "${STAGE}" != "p3" && "${LIFECYCLE_FEATURE}" != "both" ]]; then
  echo "ERROR: --lifecycle-feature is only valid for --stage p3" >&2
  exit 2
fi
case "${PHASE}" in before|after|request-only|canary|rollback) ;; *) echo "ERROR: --phase must be before, after, request-only, canary, or rollback" >&2; exit 2 ;; esac
if [[ "${STAGE}" != "p2" ]]; then
  case "${PHASE}" in
    before|after) ;;
    *) echo "ERROR: --phase ${PHASE} is only valid for --stage p2" >&2; exit 2 ;;
  esac
fi

# Emit the RUSTFS_INTERNODE_* env for the stage/phase as `KEY=VALUE` lines. Empty output means
# "cluster defaults" (nothing to set). "before" = feature disabled, "after" = feature enabled.
stage_env() {
  local stage="$1" phase="$2"
  case "${stage}" in
    p0)
      if [[ "${phase}" == "before" ]]; then
        if [[ "${TRANSPORT_FEATURE}" == "all" || "${TRANSPORT_FEATURE}" == "nodelay" ]]; then
          printf '%s\n' 'RUSTFS_INTERNODE_RPC_TCP_NODELAY=false'
        fi
        if [[ "${TRANSPORT_FEATURE}" == "all" || "${TRANSPORT_FEATURE}" == "stream-window" ]]; then
          printf '%s\n' 'RUSTFS_INTERNODE_RPC_HTTP2_STREAM_WINDOW_SIZE=0'
        fi
        if [[ "${TRANSPORT_FEATURE}" == "all" || "${TRANSPORT_FEATURE}" == "connection-window" ]]; then
          printf '%s\n' 'RUSTFS_INTERNODE_RPC_HTTP2_CONN_WINDOW_SIZE=0'
        fi
        if [[ "${TRANSPORT_FEATURE}" == "all" || "${TRANSPORT_FEATURE}" == "max-message" ]]; then
          printf '%s\n' 'RUSTFS_INTERNODE_RPC_MAX_MESSAGE_SIZE=4194304'
        fi
      fi
      # The reset list below clears the baseline overrides before restoring defaults.
      ;;
    p1)
      if [[ "${phase}" == "after" ]]; then
        printf '%s\n' \
          'RUSTFS_INTERNODE_CHANNEL_ISOLATION=true' \
          'RUSTFS_INTERNODE_BULK_CHANNELS=2'
      else
        printf '%s\n' 'RUSTFS_INTERNODE_CHANNEL_ISOLATION=false'
      fi
      ;;
    p2)
      if [[ "${phase}" == "after" || "${phase}" == "canary" ]]; then
        # Only enable after the msgpack json-fallback counter has read zero across a window and
        # the fleet confirmation gate has passed.
        printf '%s\n' \
          'RUSTFS_INTERNODE_RPC_MSGPACK_ONLY=true' \
          'RUSTFS_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED=true'
      elif [[ "${phase}" == "request-only" ]]; then
        # Requesting msgpack-only without fleet confirmation must keep the compatibility JSON fields.
        printf '%s\n' \
          'RUSTFS_INTERNODE_RPC_MSGPACK_ONLY=true' \
          'RUSTFS_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED=false'
      else
        printf '%s\n' \
          'RUSTFS_INTERNODE_RPC_MSGPACK_ONLY=false' \
          'RUSTFS_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED=false'
      fi
      ;;
    p3)
      if [[ "${phase}" == "after" ]]; then
        printf '%s\n' \
          "RUSTFS_INTERNODE_PREWARM=$([[ "${LIFECYCLE_FEATURE}" != "offline" ]] && echo true || echo false)" \
          "RUSTFS_INTERNODE_OFFLINE_BYPASS=$([[ "${LIFECYCLE_FEATURE}" != "prewarm" ]] && echo true || echo false)" \
          'RUSTFS_INTERNODE_OFFLINE_REPROBE_SECS=5' \
          'RUSTFS_INTERNODE_OFFLINE_FAILURE_THRESHOLD=3'
      else
        printf '%s\n' \
          'RUSTFS_INTERNODE_PREWARM=false' \
          'RUSTFS_INTERNODE_OFFLINE_BYPASS=false'
      fi
      ;;
    *) echo "ERROR: unknown --stage '${stage}' (expected p0|p1|p2|p3)" >&2; exit 2 ;;
  esac
}

ENV_LINES="$(stage_env "${STAGE}" "${PHASE}")"
# A phase owns only these settings. Clear them both in the emitted file and this process so
# sourcing before/after in one shell cannot silently carry baseline overrides into a candidate.
case "${STAGE}" in
  p0) RESET_VARS=(RUSTFS_INTERNODE_RPC_TCP_NODELAY RUSTFS_INTERNODE_RPC_HTTP2_STREAM_WINDOW_SIZE RUSTFS_INTERNODE_RPC_HTTP2_CONN_WINDOW_SIZE RUSTFS_INTERNODE_RPC_MAX_MESSAGE_SIZE) ;;
  p1) RESET_VARS=(RUSTFS_INTERNODE_CHANNEL_ISOLATION RUSTFS_INTERNODE_BULK_CHANNELS RUSTFS_INTERNODE_BULK_HTTP2_STREAM_WINDOW_SIZE RUSTFS_INTERNODE_BULK_HTTP2_CONN_WINDOW_SIZE RUSTFS_INTERNODE_BULK_CONCURRENCY_LIMIT RUSTFS_INTERNODE_BULK_BUFFER_SIZE) ;;
  p2) RESET_VARS=(RUSTFS_INTERNODE_RPC_MSGPACK_ONLY RUSTFS_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED) ;;
  p3) RESET_VARS=(RUSTFS_INTERNODE_PREWARM RUSTFS_INTERNODE_OFFLINE_BYPASS RUSTFS_INTERNODE_OFFLINE_REPROBE_SECS RUSTFS_INTERNODE_OFFLINE_FAILURE_THRESHOLD) ;;
esac
ARTIFACT_STAGE="${STAGE}"
if [[ "${STAGE}" == "p0" && "${TRANSPORT_FEATURE}" != "all" ]]; then
  ARTIFACT_STAGE="${STAGE}-${TRANSPORT_FEATURE}"
fi
OUT_DIR="${OUT_ROOT}/${ARTIFACT_STAGE}-${PHASE}"
mkdir -p "${OUT_DIR}"
ENV_FILE="${OUT_DIR}/server-env.sh"

{
  echo "# RustFS server env for internode stage ${STAGE} / phase ${PHASE}."
  if [[ "${STAGE}" == "p0" ]]; then echo "# Transport feature: ${TRANSPORT_FEATURE}"; fi
  if [[ "${STAGE}" == "p2" && "${PHASE}" == "canary" ]]; then
    echo "# Source this only on the selected canary RustFS node before starting that node; keep the rest on p2/before or p2/request-only env."
  else
    echo "# Source this on EVERY rustfs node before starting the server, then run the bench."
  fi
  for variable in "${RESET_VARS[@]}"; do echo "unset ${variable}"; done
  if [[ -n "${ENV_LINES}" ]]; then
    while IFS= read -r line; do echo "export ${line}"; done <<<"${ENV_LINES}"
  else
    echo "# (cluster defaults — no overrides needed for this stage/phase)"
  fi
} >"${ENV_FILE}"

echo "== internode gRPC A/B: stage=${STAGE} phase=${PHASE} =="
echo "-- server env (also written to ${ENV_FILE}):"
sed 's/^/   /' "${ENV_FILE}"
echo "-- artifacts: ${OUT_DIR}"

# Export for the p3 docker path (best-effort: only takes effect if the compose forwards the env).
for variable in "${RESET_VARS[@]}"; do unset "${variable}"; done
if [[ -n "${ENV_LINES}" ]]; then
  while IFS= read -r line; do export "${line?}"; done <<<"${ENV_LINES}"
fi

run() {
  if [[ "${DRY_RUN}" == "true" ]]; then
    local redact_next=false argument
    echo "-- dry-run, would execute:"
    for argument in "$@"; do
      if [[ "${redact_next}" == "true" ]]; then
        printf '   %q ' REDACTED
        redact_next=false
        continue
      fi
      case "${argument}" in
        --access-key|--secret-key|--session-token)
          printf '   %q ' "${argument}"
          redact_next=true
          ;;
        --access-key=*|--secret-key=*|--session-token=*|-accessKey=*|-secretKey=*)
          printf '   %q ' "${argument%%=*}=REDACTED"
          ;;
        *) printf '   %q ' "${argument}" ;;
      esac
    done
    echo
    return 0
  fi
  "$@"
}

case "${STAGE}" in
  p0|p1|p2)
    echo "-- NOTE: (re)start rustfs on all nodes with the env above before this measurement is valid."
    [[ -x "${TRANSPORT_BENCH}" ]] || { echo "ERROR: ${TRANSPORT_BENCH} not found/executable" >&2; exit 1; }
    if ((${#PASSTHROUGH[@]} > 0)); then
      run "${TRANSPORT_BENCH}" --out-dir "${OUT_DIR}" "${PASSTHROUGH[@]}"
    else
      run "${TRANSPORT_BENCH}" --out-dir "${OUT_DIR}"
    fi
    ;;
  p3)
    [[ -x "${FAILOVER_BENCH}" ]] || { echo "ERROR: ${FAILOVER_BENCH} not found/executable" >&2; exit 1; }
    if ((${#PASSTHROUGH[@]} > 0)); then
      OUT_DIR="${OUT_DIR}" run "${FAILOVER_BENCH}" "${PASSTHROUGH[@]}"
    else
      OUT_DIR="${OUT_DIR}" run "${FAILOVER_BENCH}"
    fi
    ;;
esac
