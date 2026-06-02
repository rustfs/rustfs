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

# Cleanup script for the local s3-tests data directory.
# It reports port usage but does not kill unrelated processes.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
DATA_DIR="${PROJECT_ROOT}/target/test-data/rustfs-single"
PORT="${S3_PORT:-9000}"

log_info() {
    echo "[INFO] $*"
}

log_warn() {
    echo "[WARN] $*"
}

check_port_usage() {
    log_info "Checking for processes using port ${PORT}..."

    PID=""

    # Try lsof first (works on macOS and Linux)
    if command -v lsof >/dev/null 2>&1; then
        PID=$(lsof -ti:${PORT} 2>/dev/null || true)
    fi

    # Fallback: try using netstat or ss
    if [ -z "$PID" ]; then
        if command -v netstat >/dev/null 2>&1; then
            PID=$(netstat -tuln 2>/dev/null | grep ":${PORT} " | awk '{print $7}' | cut -d'/' -f1 | head -1 || true)
        elif command -v ss >/dev/null 2>&1; then
            PID=$(ss -tuln 2>/dev/null | grep ":${PORT} " | awk '{print $6}' | cut -d',' -f2 | cut -d'=' -f2 | head -1 || true)
        fi
    fi

    if [ -n "$PID" ] && [ "$PID" != "-" ]; then
        log_warn "Port ${PORT} is in use by PID(s): ${PID}"
        log_warn "Not killing unrelated processes; stop the owner manually or use another S3_PORT."
    else
        log_info "No process found using port ${PORT}"
    fi
}

cleanup_data_directory() {
    log_info "Cleaning test data directory: ${DATA_DIR}"
    
    if [ -d "${DATA_DIR}" ]; then
        # Remove all contents but keep the directory structure
        find "${DATA_DIR}" -mindepth 1 -delete 2>/dev/null || true
        
        # Recreate subdirectories if they were deleted
        for i in {0..3}; do
            mkdir -p "${DATA_DIR}/rustfs${i}"
        done
        
        log_info "Test data directory cleaned"
    else
        log_info "Test data directory does not exist, creating it..."
        mkdir -p "${DATA_DIR}"
        for i in {0..3}; do
            mkdir -p "${DATA_DIR}/rustfs${i}"
        done
    fi
}

main() {
    log_info "Starting cleanup..."
    check_port_usage
    cleanup_data_directory
    log_info "Cleanup completed"
}

main "$@"
