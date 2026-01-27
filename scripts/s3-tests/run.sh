#!/bin/bash
# Copyright 2024 RustFS Team
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

# Disable proxy for localhost requests to avoid interference
export NO_PROXY="127.0.0.1,localhost,::1"
export no_proxy="${NO_PROXY}"
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY

# Configuration
S3_ACCESS_KEY="${S3_ACCESS_KEY:-rustfsadmin}"
S3_SECRET_KEY="${S3_SECRET_KEY:-rustfsadmin}"
S3_ALT_ACCESS_KEY="${S3_ALT_ACCESS_KEY:-rustfsalt}"
S3_ALT_SECRET_KEY="${S3_ALT_SECRET_KEY:-rustfsalt}"
S3_REGION="${S3_REGION:-us-east-1}"
S3_HOST="${S3_HOST:-127.0.0.1}"
S3_PORT="${S3_PORT:-9000}"

# Test parameters
TEST_MODE="${TEST_MODE:-single}"
MAXFAIL="${MAXFAIL:-1}"
XDIST="${XDIST:-0}"

# Directories (define early for use in test list loading)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $*"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $*"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $*"
}

# =============================================================================
# Test Classification Files
# =============================================================================
# Tests are classified into three categories stored in text files:
#   - non_standard_tests.txt:  Ceph/RGW specific tests (permanently excluded)
#   - unimplemented_tests.txt: Standard S3 features not yet implemented
#   - implemented_tests.txt:   Tests that should pass on RustFS
#
# By default, only tests listed in implemented_tests.txt are run.
# Use TESTEXPR env var to override and run custom test selection.
# =============================================================================

# Test list files location
TEST_LISTS_DIR="${SCRIPT_DIR}"
IMPLEMENTED_TESTS_FILE="${TEST_LISTS_DIR}/implemented_tests.txt"
NON_STANDARD_TESTS_FILE="${TEST_LISTS_DIR}/non_standard_tests.txt"
UNIMPLEMENTED_TESTS_FILE="${TEST_LISTS_DIR}/unimplemented_tests.txt"

# =============================================================================
# build_testexpr_from_file: Read test names from file and build pytest -k expr
# =============================================================================
# Reads test names from a file (one per line, ignoring comments and empty lines)
# and builds a pytest -k expression to include only those tests.
# =============================================================================
build_testexpr_from_file() {
    local file="$1"
    local expr=""

    if [[ ! -f "${file}" ]]; then
        log_error "Test list file not found: ${file}"
        return 1
    fi

    while IFS= read -r line || [[ -n "$line" ]]; do
        # Skip empty lines and comments
        [[ -z "$line" || "$line" =~ ^[[:space:]]*# ]] && continue
        # Trim whitespace
        line=$(echo "$line" | xargs)
        [[ -z "$line" ]] && continue

        if [[ -n "${expr}" ]]; then
            expr+=" or "
        fi
        expr+="${line}"
    done < "${file}"

    echo "${expr}"
}

# =============================================================================
# MARKEXPR: pytest marker expression (safety net for marker-based filtering)
# =============================================================================
# Even though we use file-based test selection, we keep marker exclusions
# as a safety net to ensure no non-standard tests slip through.
# =============================================================================
if [[ -z "${MARKEXPR:-}" ]]; then
    # Minimal marker exclusions as safety net (file-based filtering is primary)
    MARKEXPR="not fails_on_aws and not fails_on_rgw and not fails_on_dbstore"
fi

# =============================================================================
# TESTEXPR: pytest -k expression to select specific tests
# =============================================================================
# By default, builds an inclusion expression from implemented_tests.txt.
# Use TESTEXPR env var to override with custom selection.
#
# The file-based approach provides:
#   1. Clear visibility of which tests are run
#   2. Easy maintenance - edit txt files to add/remove tests
#   3. Separation of concerns - test classification vs test execution
# =============================================================================
if [[ -z "${TESTEXPR:-}" ]]; then
    if [[ -f "${IMPLEMENTED_TESTS_FILE}" ]]; then
        log_info "Loading test list from: ${IMPLEMENTED_TESTS_FILE}"
        TESTEXPR=$(build_testexpr_from_file "${IMPLEMENTED_TESTS_FILE}")
        if [[ -z "${TESTEXPR}" ]]; then
            log_error "No tests found in ${IMPLEMENTED_TESTS_FILE}"
            exit 1
        fi
        # Count tests for logging
        TEST_COUNT=$(grep -v '^#' "${IMPLEMENTED_TESTS_FILE}" | grep -v '^[[:space:]]*$' | wc -l | xargs)
        log_info "Loaded ${TEST_COUNT} tests from implemented_tests.txt"
    else
        log_warn "Test list file not found: ${IMPLEMENTED_TESTS_FILE}"
        log_warn "Falling back to exclusion-based filtering"
        # Fallback to exclusion-based filtering if file doesn't exist
        EXCLUDED_TESTS=(
            "test_post_object"
            "test_bucket_list_objects_anonymous"
            "test_bucket_listv2_objects_anonymous"
            "test_bucket_concurrent_set_canned_acl"
            "test_bucket_acl"
            "test_object_acl"
            "test_access_bucket"
            "test_100_continue"
            "test_cors"
            "test_object_raw"
            "test_versioning"
            "test_versioned"
        )
        TESTEXPR=""
        for pattern in "${EXCLUDED_TESTS[@]}"; do
            if [[ -n "${TESTEXPR}" ]]; then
                TESTEXPR+=" and "
            fi
            TESTEXPR+="not ${pattern}"
        done
    fi
fi

# Configuration file paths
S3TESTS_CONF_TEMPLATE="${S3TESTS_CONF_TEMPLATE:-.github/s3tests/s3tests.conf}"
S3TESTS_CONF="${S3TESTS_CONF:-s3tests.conf}"

# Service deployment mode: "build", "binary", "docker", or "existing"
# - "build": Compile with cargo build --release and run (default)
# - "binary": Use pre-compiled binary (RUSTFS_BINARY path or default)
# - "docker": Build Docker image and run in container
# - "existing": Use already running service (skip start, use S3_HOST and S3_PORT)
DEPLOY_MODE="${DEPLOY_MODE:-build}"
RUSTFS_BINARY="${RUSTFS_BINARY:-}"
NO_CACHE="${NO_CACHE:-false}"

# Additional directories (SCRIPT_DIR and PROJECT_ROOT defined earlier)
ARTIFACTS_DIR="${PROJECT_ROOT}/artifacts/s3tests-${TEST_MODE}"
CONTAINER_NAME="rustfs-${TEST_MODE}"
NETWORK_NAME="rustfs-net"
DATA_ROOT="${DATA_ROOT:-target}"
DATA_DIR="${PROJECT_ROOT}/${DATA_ROOT}/test-data/${CONTAINER_NAME}"
RUSTFS_PID=""

show_usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Options:
  -h, --help     Show this help message
  --no-cache     Force rebuild even if binary exists and is recent (for build mode)

Deployment Modes (via DEPLOY_MODE environment variable):
  1. build      - Compile with cargo build --release and run (default)
  2. binary     - Use pre-compiled binary (use RUSTFS_BINARY or default: ./target/release/rustfs)
  3. docker     - Build Docker image and run in container
  4. existing   - Use already running service (specify S3_HOST and S3_PORT)

Environment Variables:
  DEPLOY_MODE            - Deployment mode: "build", "binary", "docker", or "existing" (default: "build")
  RUSTFS_BINARY          - Path to RustFS binary (for binary mode, default: ./target/release/rustfs)
  S3_HOST                - S3 service host (default: 127.0.0.1)
  S3_PORT                - S3 service port (default: 9000)
  S3_ACCESS_KEY          - Main user access key (default: rustfsadmin)
  S3_SECRET_KEY          - Main user secret key (default: rustfsadmin)
  S3_ALT_ACCESS_KEY      - Alt user access key (default: rustfsalt)
  S3_ALT_SECRET_KEY      - Alt user secret key (default: rustfsalt)
  MAXFAIL                - Stop after N failures (default: 1)
  XDIST                  - Enable parallel execution with N workers (default: 0)
  MARKEXPR               - pytest marker expression (default: safety net exclusions)
  TESTEXPR               - pytest -k expression (default: from implemented_tests.txt)
  S3TESTS_CONF_TEMPLATE  - Path to s3tests config template (default: .github/s3tests/s3tests.conf)
  S3TESTS_CONF           - Path to generated s3tests config (default: s3tests.conf)
  DATA_ROOT              - Root directory for test data storage (default: target)
                            Final path: \${DATA_ROOT}/test-data/\${CONTAINER_NAME}

Test Classification Files (in scripts/s3-tests/):
  implemented_tests.txt    - Tests that should pass (run by default)
  unimplemented_tests.txt  - Standard S3 features not yet implemented
  non_standard_tests.txt   - Ceph/RGW specific tests (permanently excluded)

Notes:
  - Tests are loaded from implemented_tests.txt by default
  - Set TESTEXPR to override with custom test selection
  - In build mode, if the binary exists and was compiled less than 30 minutes ago,
    compilation will be skipped unless --no-cache is specified.

Examples:
  # Use Docker (default)
  $0

  # Use pre-compiled binary
  DEPLOY_MODE=binary RUSTFS_BINARY=./target/release/rustfs $0

  # Use Docker
  DEPLOY_MODE=docker $0

  # Use existing service
  DEPLOY_MODE=existing S3_HOST=192.168.1.100 S3_PORT=9000 $0

  # Force rebuild in build mode
  $0 --no-cache

EOF
}

cleanup() {
    log_info "Cleaning up..."

    if [ "${DEPLOY_MODE}" = "docker" ]; then
        docker rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true
        docker network rm "${NETWORK_NAME}" >/dev/null 2>&1 || true
    elif [ "${DEPLOY_MODE}" = "build" ] || [ "${DEPLOY_MODE}" = "binary" ]; then
        if [ -n "${RUSTFS_PID}" ]; then
            log_info "Stopping RustFS process (PID: ${RUSTFS_PID})..."
            kill "${RUSTFS_PID}" 2>/dev/null || true
            wait "${RUSTFS_PID}" 2>/dev/null || true
        fi
    fi
}

trap cleanup EXIT

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            show_usage
            exit 0
            ;;
        --no-cache)
            NO_CACHE="true"
            ;;
        *)
            log_error "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
    shift
done

cd "${PROJECT_ROOT}"

# Check port availability (except for existing mode)
if [ "${DEPLOY_MODE}" != "existing" ]; then
    if nc -z "${S3_HOST}" "${S3_PORT}" 2>/dev/null || timeout 1 bash -c "cat < /dev/null > /dev/tcp/${S3_HOST}/${S3_PORT}" 2>/dev/null; then
        log_error "Port ${S3_PORT} is already in use on ${S3_HOST}"
        log_error "Please stop the service using this port or use DEPLOY_MODE=existing to connect to an existing service"
        exit 1
    fi
fi

# Start RustFS based on deployment mode
if [ "${DEPLOY_MODE}" = "existing" ]; then
    log_info "Using existing RustFS service at ${S3_HOST}:${S3_PORT}"
    log_info "Skipping service startup..."
elif [ "${DEPLOY_MODE}" = "binary" ]; then
    # Determine binary path
    if [ -z "${RUSTFS_BINARY}" ]; then
        RUSTFS_BINARY="${PROJECT_ROOT}/target/release/rustfs"
    fi

    if [ ! -f "${RUSTFS_BINARY}" ]; then
        log_error "RustFS binary not found at: ${RUSTFS_BINARY}"
        log_info "Please compile the binary first:"
        log_info "  cargo build --release"
        log_info "Or specify the path: RUSTFS_BINARY=/path/to/rustfs $0"
        exit 1
    fi

    log_info "Using pre-compiled binary: ${RUSTFS_BINARY}"

    # Prepare data and artifacts directories
    mkdir -p "${DATA_DIR}"
    mkdir -p "${ARTIFACTS_DIR}/rustfs-${TEST_MODE}"
    for i in {0..3}; do
        mkdir -p "${DATA_DIR}/rustfs${i}"
    done

    # Start RustFS binary
    log_info "Starting RustFS binary..."
    RUST_LOG="${RUST_LOG:-info}" "${RUSTFS_BINARY}" \
        --address "${S3_HOST}:${S3_PORT}" \
        --access-key "${S3_ACCESS_KEY}" \
        --secret-key "${S3_SECRET_KEY}" \
        "${DATA_DIR}/rustfs0" "${DATA_DIR}/rustfs1" "${DATA_DIR}/rustfs2" "${DATA_DIR}/rustfs3" \
        > "${ARTIFACTS_DIR}/rustfs-${TEST_MODE}/rustfs.log" 2>&1 &

    RUSTFS_PID=$!
    log_info "RustFS started with PID: ${RUSTFS_PID}"

elif [ "${DEPLOY_MODE}" = "build" ]; then
    RUSTFS_BINARY="${PROJECT_ROOT}/target/release/rustfs"

    # Check if we should skip compilation
    SHOULD_BUILD=true
    if [ -f "${RUSTFS_BINARY}" ]; then
        # Get file modification time in seconds since epoch
        # Try Linux format first, fallback to macOS format
        FILE_MTIME=$(stat -c %Y "${RUSTFS_BINARY}" 2>/dev/null || stat -f %m "${RUSTFS_BINARY}" 2>/dev/null)

        if [ -n "${FILE_MTIME}" ]; then
            CURRENT_TIME=$(date +%s)
            AGE_SECONDS=$((CURRENT_TIME - FILE_MTIME))
            AGE_MINUTES=$((AGE_SECONDS / 60))

            if [ "${AGE_MINUTES}" -lt 30 ] && [ "${NO_CACHE}" != "true" ]; then
                log_info "Binary exists and is recent (${AGE_MINUTES} minutes old), skipping compilation."
                log_info "Use --no-cache to force rebuild."
                SHOULD_BUILD=false
            fi
        fi
    fi

    if [ "${SHOULD_BUILD}" = "true" ]; then
        if [ "${NO_CACHE}" = "true" ]; then
            log_info "Building RustFS with cargo build --release (--no-cache forced)..."
        else
            log_info "Building RustFS with cargo build --release..."
        fi
        cargo build --release || {
            log_error "Failed to build RustFS"
            exit 1
        }

        if [ ! -f "${RUSTFS_BINARY}" ]; then
            log_error "RustFS binary not found at: ${RUSTFS_BINARY}"
            log_error "Build completed but binary not found"
            exit 1
        fi

        log_info "Build successful, using binary: ${RUSTFS_BINARY}"
    else
        log_info "Using existing binary: ${RUSTFS_BINARY}"
    fi

    # Prepare data and artifacts directories
    mkdir -p "${DATA_DIR}"
    mkdir -p "${ARTIFACTS_DIR}/rustfs-${TEST_MODE}"
    for i in {0..3}; do
        mkdir -p "${DATA_DIR}/rustfs${i}"
    done

    # Start RustFS binary
    log_info "Starting RustFS binary..."
    RUST_LOG="${RUST_LOG:-info}" "${RUSTFS_BINARY}" \
        --address "${S3_HOST}:${S3_PORT}" \
        --access-key "${S3_ACCESS_KEY}" \
        --secret-key "${S3_SECRET_KEY}" \
        "${DATA_DIR}/rustfs0" "${DATA_DIR}/rustfs1" "${DATA_DIR}/rustfs2" "${DATA_DIR}/rustfs3" \
        > "${ARTIFACTS_DIR}/rustfs-${TEST_MODE}/rustfs.log" 2>&1 &

    RUSTFS_PID=$!
    log_info "RustFS started with PID: ${RUSTFS_PID}"

elif [ "${DEPLOY_MODE}" = "docker" ]; then
    # Build Docker image and run in container
    log_info "Building RustFS Docker image..."
    DOCKER_BUILDKIT=1 docker build \
        --platform linux/amd64 \
        -t rustfs-ci \
        -f Dockerfile.source . || {
        log_error "Failed to build Docker image"
        exit 1
    }

    # Create network
    log_info "Creating Docker network..."
    docker network inspect "${NETWORK_NAME}" >/dev/null 2>&1 || docker network create "${NETWORK_NAME}"

    # Remove existing container
    log_info "Removing existing container (if any)..."
    docker rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true

    # Start RustFS container
    log_info "Starting RustFS container..."
    docker run -d --name "${CONTAINER_NAME}" \
        --network "${NETWORK_NAME}" \
        -p "${S3_PORT}:9000" \
        -e RUSTFS_ADDRESS=0.0.0.0:9000 \
        -e RUSTFS_ACCESS_KEY="${S3_ACCESS_KEY}" \
        -e RUSTFS_SECRET_KEY="${S3_SECRET_KEY}" \
        -e RUSTFS_VOLUMES="/data/rustfs0 /data/rustfs1 /data/rustfs2 /data/rustfs3" \
        -v "/tmp/${CONTAINER_NAME}:/data" \
        rustfs-ci || {
        log_error "Failed to start container"
        docker logs "${CONTAINER_NAME}" || true
        exit 1
    }
else
    log_error "Invalid DEPLOY_MODE: ${DEPLOY_MODE}"
    log_error "Must be one of: build, binary, docker, existing"
    show_usage
    exit 1
fi

# Step 5: Wait for RustFS ready (improved health check)
log_info "Waiting for RustFS to be ready..."

if [ "${DEPLOY_MODE}" = "docker" ]; then
    log_info "Step 1: Waiting for container to start..."
    for i in {1..30}; do
        if [ "$(docker inspect -f '{{.State.Running}}' "${CONTAINER_NAME}" 2>/dev/null)" == "true" ]; then
            break
        fi
        sleep 1
    done

    if [ "$(docker inspect -f '{{.State.Running}}' "${CONTAINER_NAME}" 2>/dev/null)" != "true" ]; then
        log_error "Container failed to start"
        docker logs "${CONTAINER_NAME}" || true
        exit 1
    fi
elif [ "${DEPLOY_MODE}" = "build" ] || [ "${DEPLOY_MODE}" = "binary" ]; then
    log_info "Step 1: Waiting for process to start..."
    for i in {1..10}; do
        if kill -0 "${RUSTFS_PID}" 2>/dev/null; then
            break
        fi
        sleep 1
    done

    if ! kill -0 "${RUSTFS_PID}" 2>/dev/null; then
        log_error "RustFS process failed to start"
        if [ -f "${ARTIFACTS_DIR}/rustfs-${TEST_MODE}/rustfs.log" ]; then
            tail -50 "${ARTIFACTS_DIR}/rustfs-${TEST_MODE}/rustfs.log"
        fi
        exit 1
    fi
elif [ "${DEPLOY_MODE}" = "existing" ]; then
    log_info "Step 1: Checking existing service..."
    # Skip container/process checks for existing service
fi

log_info "Step 2: Waiting for port ${S3_PORT} to be listening..."
for i in {1..30}; do
    if nc -z "${S3_HOST}" "${S3_PORT}" 2>/dev/null || timeout 1 bash -c "cat < /dev/null > /dev/tcp/${S3_HOST}/${S3_PORT}" 2>/dev/null; then
        log_info "Port ${S3_PORT} is listening"
        break
    fi
    sleep 1
done

log_info "Step 3: Waiting for service to fully initialize..."

# Check if log file indicates server is started (most reliable method)
check_server_ready_from_log() {
    if [ -f "${ARTIFACTS_DIR}/rustfs-${TEST_MODE}/rustfs.log" ]; then
        if grep -q "server started successfully" "${ARTIFACTS_DIR}/rustfs-${TEST_MODE}/rustfs.log" 2>/dev/null; then
            return 0
        fi
    fi
    return 1
}

# Test S3 API readiness
test_s3_api_ready() {
    # Step 1: Check if server is responding using /health endpoint
    # /health is a probe path that bypasses readiness gate, so it can be used
    # to check if the server is up and running, even if readiness gate is not ready yet
    HEALTH_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
        -X GET \
        "http://${S3_HOST}:${S3_PORT}/health" \
        --max-time 5 2>/dev/null || echo "000")

    if [ "${HEALTH_CODE}" = "000" ]; then
        # Connection failed - server might not be running or not listening yet
        return 1
    elif [ "${HEALTH_CODE}" != "200" ]; then
        # Health endpoint returned non-200 status, server might have issues
        return 1
    fi

    # Step 2: Test S3 API with signed request (awscurl) if available
    # This tests if S3 API is actually ready and can process requests
    if command -v awscurl >/dev/null 2>&1; then
        export PATH="$HOME/.local/bin:$PATH"
        RESPONSE=$(awscurl --service s3 --region "${S3_REGION}" \
            --access_key "${S3_ACCESS_KEY}" \
            --secret_key "${S3_SECRET_KEY}" \
            -X GET "http://${S3_HOST}:${S3_PORT}/" 2>&1)

        if echo "${RESPONSE}" | grep -q "<ListAllMyBucketsResult"; then
            # S3 API is ready and responding correctly
            return 0
        fi
        # If awscurl failed, check if it's a 503 (Service Unavailable) which means not ready
        if echo "${RESPONSE}" | grep -q "503\|Service not ready"; then
            return 1  # Not ready yet (readiness gate is blocking S3 API)
        fi
        # Other errors from awscurl - might be auth issues or other problems
        # But server is up, so we'll consider it ready (S3 API might have other issues)
        return 0
    fi

    # Step 3: Fallback - if /health returns 200, server is up and readiness gate is ready
    # Since /health is a probe path and returns 200, and we don't have awscurl to test S3 API,
    # we can assume the server is ready. The readiness gate would have blocked /health if not ready.
    # Note: Root path "/" with HEAD method returns 501 Not Implemented (S3 doesn't support HEAD on root),
    # so we can't use it as a reliable test. Since /health already confirmed readiness, we return success.
    return 0
}

# First, wait for server to log "server started successfully"
log_info "Waiting for server startup completion..."
for i in {1..30}; do
    if check_server_ready_from_log; then
        log_info "Server startup complete detected in log"
        # Give it a moment for FullReady to be set (happens just before the log message)
        sleep 2
        break
    fi
    if [ $i -eq 30 ]; then
        log_warn "Server startup message not found in log after 30 attempts, continuing with API check..."
    fi
    sleep 1
done

# Now verify S3 API is actually responding
log_info "Verifying S3 API readiness..."
for i in {1..20}; do
    if test_s3_api_ready; then
        log_info "RustFS is fully ready (S3 API responding)"
        break
    fi

    if [ $i -eq 20 ]; then
        log_error "RustFS S3 API readiness check timed out"
        log_error "Checking service status..."

        # Check if server is still running
        if [ "${DEPLOY_MODE}" = "build" ] || [ "${DEPLOY_MODE}" = "binary" ]; then
            if [ -n "${RUSTFS_PID}" ] && ! kill -0 "${RUSTFS_PID}" 2>/dev/null; then
                log_error "RustFS process is not running (PID: ${RUSTFS_PID})"
                if [ -f "${ARTIFACTS_DIR}/rustfs-${TEST_MODE}/rustfs.log" ]; then
                    log_error "Last 50 lines of RustFS log:"
                    tail -50 "${ARTIFACTS_DIR}/rustfs-${TEST_MODE}/rustfs.log"
                fi
                exit 1
            fi
        fi

        # Show last test attempt with detailed diagnostics
        log_error "Last S3 API readiness test diagnostics:"

        # Test /health endpoint (probe path, bypasses readiness gate)
        log_error "Step 1: Testing /health endpoint (probe path):"
        HEALTH_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
            -X GET \
            "http://${S3_HOST}:${S3_PORT}/health" \
            --max-time 5 2>&1 || echo "000")
        log_error "  /health HTTP status code: ${HEALTH_CODE}"
        if [ "${HEALTH_CODE}" != "200" ]; then
            log_error "  /health endpoint response:"
            curl -s "http://${S3_HOST}:${S3_PORT}/health" 2>&1 | head -5 || true
        fi

        # Test S3 API with signed request if awscurl is available
        if command -v awscurl >/dev/null 2>&1; then
            export PATH="$HOME/.local/bin:$PATH"
            log_error "Step 2: Testing S3 API with awscurl (signed request):"
            awscurl --service s3 --region "${S3_REGION}" \
                --access_key "${S3_ACCESS_KEY}" \
                --secret_key "${S3_SECRET_KEY}" \
                -X GET "http://${S3_HOST}:${S3_PORT}/" 2>&1 | head -20
        else
            log_error "Step 2: Testing S3 API root path (unsigned HEAD request):"
            HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
                -X HEAD \
                "http://${S3_HOST}:${S3_PORT}/" \
                --max-time 5 2>&1 || echo "000")
            log_error "  Root path HTTP status code: ${HTTP_CODE}"
            if [ "${HTTP_CODE}" = "503" ]; then
                log_error "  Note: 503 indicates readiness gate is blocking (service not ready)"
            elif [ "${HTTP_CODE}" = "000" ]; then
                log_error "  Note: 000 indicates connection failure"
            fi
        fi

        # Output logs based on deployment mode
        if [ "${DEPLOY_MODE}" = "docker" ]; then
            docker logs "${CONTAINER_NAME}" 2>&1 | tail -50
        elif [ "${DEPLOY_MODE}" = "build" ] || [ "${DEPLOY_MODE}" = "binary" ]; then
            if [ -f "${ARTIFACTS_DIR}/rustfs-${TEST_MODE}/rustfs.log" ]; then
                log_error "Last 50 lines of RustFS log:"
                tail -50 "${ARTIFACTS_DIR}/rustfs-${TEST_MODE}/rustfs.log"
            fi
        fi
        exit 1
    fi

    sleep 1
done

# Step 6: Generate s3tests config
log_info "Generating s3tests config..."
mkdir -p "${ARTIFACTS_DIR}"

# Resolve template and output paths (relative to PROJECT_ROOT)
TEMPLATE_PATH="${PROJECT_ROOT}/${S3TESTS_CONF_TEMPLATE}"
CONF_OUTPUT_PATH="${PROJECT_ROOT}/${S3TESTS_CONF}"

# Check if template exists
if [ ! -f "${TEMPLATE_PATH}" ]; then
    log_error "S3tests config template not found: ${TEMPLATE_PATH}"
    log_error "Please specify S3TESTS_CONF_TEMPLATE environment variable"
    exit 1
fi

# Install gettext for envsubst if not available
if ! command -v envsubst >/dev/null 2>&1; then
    log_info "Installing gettext-base for envsubst..."
    if command -v apt-get >/dev/null 2>&1; then
        sudo apt-get update && sudo apt-get install -y gettext-base || {
            log_warn "Failed to install gettext-base, trying alternative method"
        }
    elif command -v brew >/dev/null 2>&1; then
        brew install gettext || {
            log_warn "Failed to install gettext via brew"
        }
    fi
fi

log_info "Using template: ${TEMPLATE_PATH}"
log_info "Generating config: ${CONF_OUTPUT_PATH}"

# Export all required variables for envsubst
export S3_HOST S3_ACCESS_KEY S3_SECRET_KEY S3_ALT_ACCESS_KEY S3_ALT_SECRET_KEY
envsubst < "${TEMPLATE_PATH}" > "${CONF_OUTPUT_PATH}" || {
    log_error "Failed to generate s3tests config"
    exit 1
}

# Step 7: Provision s3-tests alt user
# Note: Main user (rustfsadmin) is a system user and doesn't need to be created via API
log_info "Provisioning s3-tests alt user..."

# Helper function to install Python packages with fallback for externally-managed environments
install_python_package() {
    local package=$1
    local error_output

    # Try --user first (works on most Linux systems)
    error_output=$(python3 -m pip install --user --upgrade pip "${package}" 2>&1)
    if [ $? -eq 0 ]; then
        return 0
    fi

    # If that fails with externally-managed-environment error, try with --break-system-packages
    if echo "${error_output}" | grep -q "externally-managed-environment"; then
        log_warn "Detected externally-managed Python environment, using --break-system-packages flag"
        python3 -m pip install --user --break-system-packages --upgrade pip "${package}" || {
            log_error "Failed to install ${package} even with --break-system-packages"
            return 1
        }
        return 0
    fi

    # Other errors - show the error output
    log_error "Failed to install ${package}: ${error_output}"
    return 1
}

if ! command -v awscurl >/dev/null 2>&1; then
    install_python_package awscurl || {
        log_error "Failed to install awscurl"
        exit 1
    }
    # Add common Python user bin directories to PATH
    # macOS: ~/Library/Python/X.Y/bin
    # Linux: ~/.local/bin
    PYTHON_VERSION=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')" 2>/dev/null || echo "3.14")
    export PATH="$HOME/Library/Python/${PYTHON_VERSION}/bin:$HOME/.local/bin:$PATH"
    # Verify awscurl is now available
    if ! command -v awscurl >/dev/null 2>&1; then
        log_error "awscurl installed but not found in PATH. Tried:"
        log_error "  - $HOME/Library/Python/${PYTHON_VERSION}/bin"
        log_error "  - $HOME/.local/bin"
        log_error "Please ensure awscurl is in your PATH"
        exit 1
    fi
fi

# Provision alt user (required by suite)
log_info "Provisioning alt user (${S3_ALT_ACCESS_KEY})..."
awscurl \
    --service s3 \
    --region "${S3_REGION}" \
    --access_key "${S3_ACCESS_KEY}" \
    --secret_key "${S3_SECRET_KEY}" \
    -X PUT \
    -H 'Content-Type: application/json' \
    -d "{\"secretKey\":\"${S3_ALT_SECRET_KEY}\",\"status\":\"enabled\",\"policy\":\"readwrite\"}" \
    "http://${S3_HOST}:${S3_PORT}/rustfs/admin/v3/add-user?accessKey=${S3_ALT_ACCESS_KEY}" || {
    log_error "Failed to add alt user"
    exit 1
}

# Explicitly attach built-in policy via policy mapping
awscurl \
    --service s3 \
    --region "${S3_REGION}" \
    --access_key "${S3_ACCESS_KEY}" \
    --secret_key "${S3_SECRET_KEY}" \
    -X PUT \
    "http://${S3_HOST}:${S3_PORT}/rustfs/admin/v3/set-user-or-group-policy?policyName=readwrite&userOrGroup=${S3_ALT_ACCESS_KEY}&isGroup=false" || {
    log_error "Failed to set user policy"
    exit 1
}

# Sanity check: alt user can list buckets
awscurl \
    --service s3 \
    --region "${S3_REGION}" \
    --access_key "${S3_ALT_ACCESS_KEY}" \
    --secret_key "${S3_ALT_SECRET_KEY}" \
    -X GET \
    "http://${S3_HOST}:${S3_PORT}/" >/dev/null || {
    log_error "Alt user cannot list buckets"
    exit 1
}

log_info "Alt user provisioned successfully"

# Step 8: Prepare s3-tests
log_info "Preparing s3-tests..."
if [ ! -d "${PROJECT_ROOT}/s3-tests" ]; then
    git clone --depth 1 https://github.com/ceph/s3-tests.git "${PROJECT_ROOT}/s3-tests" || {
        log_error "Failed to clone s3-tests"
        exit 1
    }
fi

cd "${PROJECT_ROOT}/s3-tests"

# Install tox if not available
if ! command -v tox >/dev/null 2>&1; then
    install_python_package tox || {
        log_error "Failed to install tox"
        exit 1
    }
    # Add common Python user bin directories to PATH (same as awscurl)
    PYTHON_VERSION=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')" 2>/dev/null || echo "3.14")
    export PATH="$HOME/Library/Python/${PYTHON_VERSION}/bin:$HOME/.local/bin:$PATH"
fi

# Step 9: Run ceph s3-tests
log_info "Running ceph s3-tests..."
mkdir -p "${ARTIFACTS_DIR}"

XDIST_ARGS=""
if [ "${XDIST}" != "0" ]; then
    # Add pytest-xdist to requirements.txt so tox installs it inside its virtualenv
    echo "pytest-xdist" >> requirements.txt
    XDIST_ARGS="-n ${XDIST} --dist=loadgroup"
fi

# Resolve config path (absolute path for tox)
CONF_OUTPUT_PATH="${PROJECT_ROOT}/${S3TESTS_CONF}"

# Run tests from s3tests/functional
S3TEST_CONF="${CONF_OUTPUT_PATH}" \
    tox -- \
    -vv -ra --showlocals --tb=long \
    --maxfail="${MAXFAIL}" \
    --junitxml="${ARTIFACTS_DIR}/junit.xml" \
    ${XDIST_ARGS} \
    s3tests/functional/test_s3.py \
    -m "${MARKEXPR}" \
    -k "${TESTEXPR}" \
    2>&1 | tee "${ARTIFACTS_DIR}/pytest.log"

TEST_EXIT_CODE=${PIPESTATUS[0]}

# Step 10: Collect RustFS logs
log_info "Collecting RustFS logs..."
mkdir -p "${ARTIFACTS_DIR}/rustfs-${TEST_MODE}"

if [ "${DEPLOY_MODE}" = "docker" ]; then
    docker logs "${CONTAINER_NAME}" > "${ARTIFACTS_DIR}/rustfs-${TEST_MODE}/rustfs.log" 2>&1 || true
    docker inspect "${CONTAINER_NAME}" > "${ARTIFACTS_DIR}/rustfs-${TEST_MODE}/inspect.json" || true
elif [ "${DEPLOY_MODE}" = "build" ] || [ "${DEPLOY_MODE}" = "binary" ]; then
    # Logs are already being written to file, just copy metadata
    echo "{\"pid\": ${RUSTFS_PID}, \"binary\": \"${RUSTFS_BINARY}\", \"mode\": \"binary\"}" > "${ARTIFACTS_DIR}/rustfs-${TEST_MODE}/inspect.json" || true
elif [ "${DEPLOY_MODE}" = "existing" ]; then
    log_info "Skipping log collection for existing service"
    echo "{\"host\": \"${S3_HOST}\", \"port\": ${S3_PORT}, \"mode\": \"existing\"}" > "${ARTIFACTS_DIR}/rustfs-${TEST_MODE}/inspect.json" || true
fi

# Summary
if [ ${TEST_EXIT_CODE} -eq 0 ]; then
    log_info "Tests completed successfully!"
    log_info "Results: ${ARTIFACTS_DIR}/junit.xml"
    log_info "Logs: ${ARTIFACTS_DIR}/pytest.log"
else
    log_error "Tests failed with exit code ${TEST_EXIT_CODE}"
    log_info "Check results: ${ARTIFACTS_DIR}/junit.xml"
    log_info "Check logs: ${ARTIFACTS_DIR}/pytest.log"
    log_info "Check RustFS logs: ${ARTIFACTS_DIR}/rustfs-${TEST_MODE}/rustfs.log"
fi

exit ${TEST_EXIT_CODE}
