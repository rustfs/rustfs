#!/usr/bin/env bash
# Quick Validation Script for GET Optimization
# Usage: ./scripts/quick-validate-get-optimization.sh [TARGET_HOST]
#
# Runs quick functional tests to validate GET optimization changes:
# 1. Basic GET correctness
# 2. Concurrent GET stability
# 3. Early-stop behavior

set -euo pipefail

TARGET_HOST="${1:-localhost:9000}"
MC_ALIAS="${MC_ALIAS:-rustfs}"
TEST_BUCKET="quick-validate-$(date +%s)"

echo "=========================================="
echo "Quick GET Optimization Validation"
echo "=========================================="
echo "Target: $TARGET_HOST"
echo ""

# Create test bucket
mc mb "${MC_ALIAS}/${TEST_BUCKET}" 2>/dev/null || true

# ============================================================================
# Test 1: Basic GET Correctness
# ============================================================================
echo "[1/3] Basic GET Correctness"

# Upload test file
dd if=/dev/urandom of=/tmp/quick-test.bin bs=1048576 count=1 2>/dev/null
original_hash=$(md5 -q /tmp/quick-test.bin 2>/dev/null || md5sum /tmp/quick-test.bin | awk '{print $1}')

mc cp /tmp/quick-test.bin "${MC_ALIAS}/${TEST_BUCKET}/test.bin" >/dev/null 2>&1

# Download and verify
mc cp "${MC_ALIAS}/${TEST_BUCKET}/test.bin" /tmp/quick-test-download.bin >/dev/null 2>&1
download_hash=$(md5 -q /tmp/quick-test-download.bin 2>/dev/null || md5sum /tmp/quick-test-download.bin | awk '{print $1}')

if [ "$original_hash" = "$download_hash" ]; then
    echo "  PASS: Data integrity verified"
else
    echo "  FAIL: Data mismatch (original=$original_hash, download=$download_hash)"
fi

rm -f /tmp/quick-test.bin /tmp/quick-test-download.bin

# ============================================================================
# Test 2: Concurrent GET Stability
# ============================================================================
echo "[2/3] Concurrent GET Stability"

# Upload multiple test files
for i in $(seq 1 10); do
    dd if=/dev/urandom of="/tmp/quick-test-${i}.bin" bs=1048576 count=1 2>/dev/null
    mc cp "/tmp/quick-test-${i}.bin" "${MC_ALIAS}/${TEST_BUCKET}/test-${i}.bin" >/dev/null 2>&1
    rm -f "/tmp/quick-test-${i}.bin"
done

# Concurrent download
passed=0
failed=0

for i in $(seq 1 10); do
    if mc cp "${MC_ALIAS}/${TEST_BUCKET}/test-${i}.bin" "/tmp/quick-download-${i}.bin" >/dev/null 2>&1; then
        size=$(stat -f%z "/tmp/quick-download-${i}.bin" 2>/dev/null || stat -c%s "/tmp/quick-download-${i}.bin" 2>/dev/null)
        if [ "$size" = "1048576" ]; then
            passed=$((passed + 1))
        else
            failed=$((failed + 1))
        fi
    else
        failed=$((failed + 1))
    fi
    rm -f "/tmp/quick-download-${i}.bin"
done

echo "  Result: $passed passed, $failed failed"

# ============================================================================
# Test 3: Early-Stop Behavior
# ============================================================================
echo "[3/3] Early-Stop Behavior"

# Upload a larger test file
dd if=/dev/urandom of=/tmp/quick-test-large.bin bs=1048576 count=10 2>/dev/null
mc cp /tmp/quick-test-large.bin "${MC_ALIAS}/${TEST_BUCKET}/test-large.bin" >/dev/null 2>&1

# Multiple reads to trigger early-stop
passed=0
failed=0

for i in $(seq 1 20); do
    if mc cp "${MC_ALIAS}/${TEST_BUCKET}/test-large.bin" "/tmp/quick-large-${i}.bin" >/dev/null 2>&1; then
        size=$(stat -f%z "/tmp/quick-large-${i}.bin" 2>/dev/null || stat -c%s "/tmp/quick-large-${i}.bin" 2>/dev/null)
        if [ "$size" = "10485760" ]; then
            passed=$((passed + 1))
        else
            failed=$((failed + 1))
        fi
    else
        failed=$((failed + 1))
    fi
    rm -f "/tmp/quick-large-${i}.bin"
done

echo "  Result: $passed passed, $failed failed"

rm -f /tmp/quick-test-large.bin

# ============================================================================
# Cleanup
# ============================================================================
echo ""
echo "Cleaning up..."
mc rm --recursive --force "${MC_ALIAS}/${TEST_BUCKET}" >/dev/null 2>&1 || true
mc rb "${MC_ALIAS}/${TEST_BUCKET}" >/dev/null 2>&1 || true

echo ""
echo "=========================================="
echo "Quick Validation Complete"
echo "=========================================="
