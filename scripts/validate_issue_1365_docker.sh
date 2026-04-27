#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose-simple.yml}"
WAIT_TIMEOUT_SECS="${WAIT_TIMEOUT_SECS:-120}"
KEEP_UP="${KEEP_UP:-false}"
RUN_S3_TESTS="${RUN_S3_TESTS:-true}"
S3_HOST="${S3_HOST:-127.0.0.1}"
S3_PORT="${S3_PORT:-9000}"

usage() {
  cat <<'USAGE'
Usage:
  scripts/validate_issue_1365_docker.sh [options]

Options:
  --compose-file <path>     docker compose file (default: docker-compose-simple.yml)
  --wait-timeout <secs>     health wait timeout (default: 120)
  --keep-up                 keep compose services up after the script exits
  --skip-s3-tests           skip scripts/s3-tests/run.sh
  -h, --help                show help

Environment:
  COMPOSE_FILE
  WAIT_TIMEOUT_SECS
  KEEP_UP
  RUN_S3_TESTS
  S3_HOST
  S3_PORT
USAGE
}

log_info() {
  printf '[INFO] %s\n' "$*"
}

log_error() {
  printf '[ERROR] %s\n' "$*" >&2
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    log_error "command not found: $1"
    exit 1
  fi
}

compose() {
  docker compose -f "${PROJECT_ROOT}/${COMPOSE_FILE}" "$@"
}

cleanup() {
  if [[ "${KEEP_UP}" == "true" ]]; then
    log_info "KEEP_UP=true, leaving compose services running"
    return
  fi

  log_info "Stopping docker compose services"
  compose down -v >/dev/null 2>&1 || true
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --compose-file)
        COMPOSE_FILE="$2"
        shift 2
        ;;
      --wait-timeout)
        WAIT_TIMEOUT_SECS="$2"
        shift 2
        ;;
      --keep-up)
        KEEP_UP=true
        shift
        ;;
      --skip-s3-tests)
        RUN_S3_TESTS=false
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

wait_for_endpoint() {
  local url="$1"
  local start now
  start="$(date +%s)"

  while true; do
    if curl -fsS --connect-timeout 2 --max-time 3 "${url}" >/dev/null 2>&1; then
      return 0
    fi

    now="$(date +%s)"
    if (( now - start >= WAIT_TIMEOUT_SECS )); then
      log_error "timed out waiting for ${url}"
      compose ps || true
      compose logs rustfs --tail 200 || true
      return 1
    fi

    sleep 2
  done
}

main() {
  parse_args "$@"
  require_cmd docker
  require_cmd curl

  trap cleanup EXIT INT TERM

  log_info "Starting docker compose from ${COMPOSE_FILE}"
  compose up -d --build

  log_info "Waiting for RustFS health endpoint"
  wait_for_endpoint "http://${S3_HOST}:${S3_PORT}/health"

  log_info "Waiting for RustFS readiness endpoint"
  wait_for_endpoint "http://${S3_HOST}:${S3_PORT}/health/ready"

  log_info "Docker health checks passed"

  if [[ "${RUN_S3_TESTS}" == "true" ]]; then
    log_info "Running S3 compatibility tests against the running dockerized service"
    (
      cd "${PROJECT_ROOT}"
      DEPLOY_MODE=existing S3_HOST="${S3_HOST}" S3_PORT="${S3_PORT}" ./scripts/s3-tests/run.sh
    )
  else
    log_info "Skipping S3 compatibility tests"
  fi

  log_info "Issue 1365 docker validation completed successfully"
}

main "$@"
