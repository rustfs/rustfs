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
PASSTHROUGH=()

usage() {
  cat <<'USAGE'
Usage:
  scripts/run_internode_grpc_ab_bench.sh --stage <p0|p1|p2|p3> --phase <before|after> [options] [-- <bench args>]

Required:
  --stage <p0|p1|p2|p3>     Which optimization stage to A/B.
  --phase <before|after>    baseline (feature off) or enabled (feature on).

Options:
  --out-root <dir>          Artifact root. Default: target/bench/internode-transport
  --dry-run                 Print the env + the command, do not run the bench.
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
    -h|--help) usage; exit 0 ;;
    --) shift; PASSTHROUGH=("$@"); break ;;
    *) echo "Unknown argument: $1" >&2; usage; exit 2 ;;
  esac
done

[[ -n "${STAGE}" && -n "${PHASE}" ]] || { echo "ERROR: --stage and --phase are required" >&2; usage; exit 2; }
case "${PHASE}" in before|after) ;; *) echo "ERROR: --phase must be before|after" >&2; exit 2 ;; esac

# Emit the RUSTFS_INTERNODE_* env for the stage/phase as `KEY=VALUE` lines. Empty output means
# "cluster defaults" (nothing to set). "before" = feature disabled, "after" = feature enabled.
stage_env() {
  local stage="$1" phase="$2"
  case "${stage}" in
    p0)
      if [[ "${phase}" == "before" ]]; then
        printf '%s\n' \
          'RUSTFS_INTERNODE_RPC_TCP_NODELAY=false' \
          'RUSTFS_INTERNODE_RPC_HTTP2_STREAM_WINDOW_SIZE=0' \
          'RUSTFS_INTERNODE_RPC_HTTP2_CONN_WINDOW_SIZE=0' \
          'RUSTFS_INTERNODE_RPC_MAX_MESSAGE_SIZE=4194304'
      fi
      # after: P0 defaults are the enabled state -> no overrides needed.
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
      if [[ "${phase}" == "after" ]]; then
        # Only enable after the msgpack json-fallback counter has read zero across a window.
        printf '%s\n' 'RUSTFS_INTERNODE_RPC_MSGPACK_ONLY=true'
      else
        printf '%s\n' 'RUSTFS_INTERNODE_RPC_MSGPACK_ONLY=false'
      fi
      ;;
    p3)
      if [[ "${phase}" == "after" ]]; then
        printf '%s\n' \
          'RUSTFS_INTERNODE_PREWARM=true' \
          'RUSTFS_INTERNODE_OFFLINE_BYPASS=true' \
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
OUT_DIR="${OUT_ROOT}/${STAGE}-${PHASE}"
mkdir -p "${OUT_DIR}"
ENV_FILE="${OUT_DIR}/server-env.sh"

{
  echo "# RustFS server env for internode stage ${STAGE} / phase ${PHASE}."
  echo "# Source this on EVERY rustfs node before starting the server, then run the bench."
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
if [[ -n "${ENV_LINES}" ]]; then
  while IFS= read -r line; do export "${line?}"; done <<<"${ENV_LINES}"
fi

run() {
  if [[ "${DRY_RUN}" == "true" ]]; then
    echo "-- dry-run, would execute:"; printf '   %q ' "$@"; echo
    return 0
  fi
  "$@"
}

case "${STAGE}" in
  p0|p1|p2)
    echo "-- NOTE: (re)start rustfs on all nodes with the env above before this measurement is valid."
    [[ -x "${TRANSPORT_BENCH}" ]] || { echo "ERROR: ${TRANSPORT_BENCH} not found/executable" >&2; exit 1; }
    run "${TRANSPORT_BENCH}" --out-dir "${OUT_DIR}" "${PASSTHROUGH[@]}"
    ;;
  p3)
    [[ -x "${FAILOVER_BENCH}" ]] || { echo "ERROR: ${FAILOVER_BENCH} not found/executable" >&2; exit 1; }
    OUT_DIR="${OUT_DIR}" run "${FAILOVER_BENCH}" "${PASSTHROUGH[@]}"
    ;;
esac
