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

# Cleanup script for port 9000 and test data directory
# This script kills any process using port 9000 and cleans the test data directory

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
DATA_DIR="${PROJECT_ROOT}/target/test-data/rustfs-single"
PORT="${S3_PORT:-9000}"
HOST="${S3_HOST:-127.0.0.1}"

log_info() {
    echo "[INFO] $*"
}

log_warn() {
    echo "[WARN] $*"
}

cleanup_port_9000() {
    log_info "Checking for processes using port ${PORT}..."
    
    # Try to find process using port 9000
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
    
    # If we found a PID, kill it
    if [ -n "$PID" ] && [ "$PID" != "-" ]; then
        log_info "Found process ${PID} using port ${PORT}, killing it..."
        kill -9 "$PID" 2>/dev/null || true
        sleep 2
        
        # Verify the process is gone
        if kill -0 "$PID" 2>/dev/null; then
            log_warn "Process ${PID} is still running, trying force kill..."
            kill -9 "$PID" 2>/dev/null || true
            sleep 1
        fi
    else
        log_info "No process found using port ${PORT}"
    fi
    
    # Also try to kill any rustfs processes (more aggressive cleanup)
    if pgrep -f "rustfs.*${PORT}" >/dev/null 2>&1; then
        log_info "Killing any remaining rustfs processes using port ${PORT}..."
        pkill -f "rustfs.*${PORT}" 2>/dev/null || true
        sleep 1
    fi
    
    # Verify port is released
    log_info "Verifying port ${PORT} is released..."
    for i in {1..10}; do
        if command -v nc >/dev/null 2>&1; then
            if ! nc -z "${HOST}" "${PORT}" 2>/dev/null; then
                log_info "Port ${PORT} is now available"
                break
            fi
        elif timeout 1 bash -c "cat < /dev/null > /dev/tcp/${HOST}/${PORT}" 2>/dev/null; then
            # Port is still in use
            if [ $i -eq 10 ]; then
                log_warn "Port ${PORT} may still be in use after cleanup attempts"
            fi
        else
            log_info "Port ${PORT} is now available"
            break
        fi
        sleep 1
    done
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
    cleanup_port_9000
    cleanup_data_directory
    log_info "Cleanup completed"
}

main "$@"
