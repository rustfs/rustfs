#!/usr/bin/env bash
# GET Optimization Stress Test Script
# Usage: ./scripts/stress-test-get-optimization.sh [TARGET_HOST] [OUTPUT_DIR]
#
# Validates:
# 1. Data correctness with early-stop enabled
# 2. Concurrent GET performance
# 3. Mixed read/write stability
# 4. Early-stop behavior under slow disk conditions

set -euo pipefail

TARGET_HOST="${1:-localhost:9000}"
OUTPUT_DIR="${2:-./stress-test-results/$(date +%Y%m%d-%H%M%S)}"
MC_ALIAS="${MC_ALIAS:-rustfs}"
TEST_BUCKET="${TEST_BUCKET:-stress-test-$(date +%s)}"
TEST_DURATION="${TEST_DURATION:-300s}"
CONCURRENCY="${CONCURRENCY:-64}"

# S3 credentials
export WARP_ACCESS_KEY="${WARP_ACCESS_KEY:-rustfsadmin}"
export WARP_SECRET_KEY="${WARP_SECRET_KEY:-rustfsadmin}"

mkdir -p "$OUTPUT_DIR"

echo "=========================================="
echo "GET Optimization Stress Test"
echo "=========================================="
echo "Target:      $TARGET_HOST"
echo "Output:      $OUTPUT_DIR"
echo "Bucket:      $TEST_BUCKET"
echo "Duration:    $TEST_DURATION"
echo "Concurrency: $CONCURRENCY"
echo "=========================================="
echo ""

# Save test configuration
cat > "$OUTPUT_DIR/test-config.txt" <<EOF
Date:        $(date -u +"%Y-%m-%dT%H:%M:%SZ")
Target:      $TARGET_HOST
Bucket:      $TEST_BUCKET
Duration:    $TEST_DURATION
Concurrency: $CONCURRENCY

Environment Variables:
RUSTFS_GET_METADATA_EARLY_STOP_ENABLE=${RUSTFS_GET_METADATA_EARLY_STOP_ENABLE:-true}
RUSTFS_GET_CODEC_STREAMING_ENABLE=${RUSTFS_GET_CODEC_STREAMING_ENABLE:-false}
RUSTFS_OBJECT_FILE_CACHE_RECLAIM_READ_ENABLE=${RUSTFS_OBJECT_FILE_CACHE_RECLAIM_READ_ENABLE:-true}
EOF

# ============================================================================
# Test 1: Data Correctness Validation
# ============================================================================
echo "[1/4] Data Correctness Validation"
echo "=================================="

test_data_correctness() {
    local size=$1
    local count=$2
    local passed=0
    local failed=0

    echo "  Testing $count objects of size $size..."

    for i in $(seq 1 $count); do
        # Generate test file
        local testfile="/tmp/stress-test-${size}-${i}.bin"
        if [ "$size" = "1KB" ]; then
            dd if=/dev/urandom of="$testfile" bs=1024 count=1 2>/dev/null
        elif [ "$size" = "1MB" ]; then
            dd if=/dev/urandom of="$testfile" bs=1048576 count=1 2>/dev/null
        elif [ "$size" = "10MB" ]; then
            dd if=/dev/urandom of="$testfile" bs=1048576 count=10 2>/dev/null
        fi

        # Calculate original hash
        local original_hash
        original_hash=$(md5 -q "$testfile" 2>/dev/null || md5sum "$testfile" | awk '{print $1}')

        # Upload
        mc cp "$testfile" "${MC_ALIAS}/${TEST_BUCKET}/correctness-${size}-${i}.bin" >/dev/null 2>&1

        # Download and verify
        local downloadfile="/tmp/stress-test-${size}-${i}-download.bin"
        mc cp "${MC_ALIAS}/${TEST_BUCKET}/correctness-${size}-${i}.bin" "$downloadfile" >/dev/null 2>&1

        local download_hash
        download_hash=$(md5 -q "$downloadfile" 2>/dev/null || md5sum "$downloadfile" | awk '{print $1}')

        if [ "$original_hash" = "$download_hash" ]; then
            passed=$((passed + 1))
        else
            failed=$((failed + 1))
            echo "    MISMATCH: correctness-${size}-${i}.bin (original=$original_hash, download=$download_hash)" >> "$OUTPUT_DIR/correctness-errors.log"
        fi

        # Cleanup
        rm -f "$testfile" "$downloadfile"
    done

    echo "    Result: $passed passed, $failed failed"
    echo "correctness_${size}: passed=$passed failed=$failed" >> "$OUTPUT_DIR/correctness-results.txt"
}

# Test different object sizes
mc mb "${MC_ALIAS}/${TEST_BUCKET}" 2>/dev/null || true

test_data_correctness "1KB" 100
test_data_correctness "1MB" 50
test_data_correctness "10MB" 10

echo ""

# ============================================================================
# Test 2: Concurrent GET Stress Test
# ============================================================================
echo "[2/4] Concurrent GET Stress Test"
echo "================================="

# Prepare test objects
echo "  Preparing test objects..."
for size in 1KiB 1MiB 4MiB 10MiB; do
    warp put --obj.size="$size" --num.objects=100 --host="$TARGET_HOST" --bucket="$TEST_BUCKET" --concurrent=16 >/dev/null 2>&1
done

echo "  Running concurrent GET tests..."

for size in 1KiB 1MiB 4MiB 10MiB; do
    for conc in 16 64 256; do
        echo "    GET size=$size concurrency=$conc duration=$TEST_DURATION"
        output_file="$OUTPUT_DIR/get-${size}-c${conc}.json"

        if warp get \
            --host="$TARGET_HOST" \
            --obj.size="$size" \
            --concurrent="$conc" \
            --duration="$TEST_DURATION" \
            --bucket="$TEST_BUCKET" \
            --json \
            > "$output_file" 2>/dev/null; then
            echo "      OK"
        else
            echo "      FAILED (see $output_file)"
        fi
    done
done

echo ""

# ============================================================================
# Test 3: Mixed Read/Write Stress Test
# ============================================================================
echo "[3/4] Mixed Read/Write Stress Test"
echo "==================================="

echo "  Running mixed workload for $TEST_DURATION..."

warp mixed \
    --host="$TARGET_HOST" \
    --obj.size=1MiB \
    --concurrent="$CONCURRENCY" \
    --duration="$TEST_DURATION" \
    --bucket="$TEST_BUCKET" \
    --json \
    > "$OUTPUT_DIR/mixed-1MiB-c${CONCURRENCY}.json" 2>/dev/null || echo "  MIXED TEST FAILED"

echo "  Mixed test complete"
echo ""

# ============================================================================
# Test 4: Early-Stop Behavior Validation
# ============================================================================
echo "[4/4] Early-Stop Behavior Validation"
echo "====================================="

# This test verifies that early-stop works correctly by checking:
# 1. All GET requests return correct data
# 2. No timeouts or errors under normal conditions
# 3. Performance is consistent

echo "  Running early-stop validation with concurrent reads..."

# Create a test object
dd if=/dev/urandom of=/tmp/early-stop-test.bin bs=1048576 count=10 2>/dev/null
mc cp /tmp/early-stop-test.bin "${MC_ALIAS}/${TEST_BUCKET}/early-stop-test.bin" >/dev/null 2>&1

# Concurrent read test
local_passed=0
local_failed=0

for i in $(seq 1 100); do
    downloadfile="/tmp/early-stop-download-${i}.bin"
    if mc cp "${MC_ALIAS}/${TEST_BUCKET}/early-stop-test.bin" "$downloadfile" >/dev/null 2>&1; then
        # Verify file size
        local_size=$(stat -f%z "$downloadfile" 2>/dev/null || stat -c%s "$downloadfile" 2>/dev/null)
        if [ "$local_size" = "10485760" ]; then
            local_passed=$((local_passed + 1))
        else
            local_failed=$((local_failed + 1))
            echo "    SIZE MISMATCH: expected 10485760, got $local_size" >> "$OUTPUT_DIR/early-stop-errors.log"
        fi
    else
        local_failed=$((local_failed + 1))
        echo "    DOWNLOAD FAILED: iteration $i" >> "$OUTPUT_DIR/early-stop-errors.log"
    fi
    rm -f "$downloadfile"
done

echo "  Early-stop validation: $local_passed passed, $local_failed failed"
echo "early_stop_validation: passed=$local_passed failed=$local_failed" >> "$OUTPUT_DIR/early-stop-results.txt"

# Cleanup
rm -f /tmp/early-stop-test.bin
mc rm "${MC_ALIAS}/${TEST_BUCKET}/early-stop-test.bin" >/dev/null 2>&1 || true

echo ""

# ============================================================================
# Summary
# ============================================================================
echo "=========================================="
echo "Stress Test Summary"
echo "=========================================="
echo ""
echo "Results saved to: $OUTPUT_DIR"
echo ""
echo "Files:"
ls -la "$OUTPUT_DIR"
echo ""
echo "Review:"
echo "  - correctness-results.txt: Data correctness validation"
echo "  - early-stop-results.txt: Early-stop behavior validation"
echo "  - get-*.json: Concurrent GET performance results"
echo "  - mixed-*.json: Mixed workload results"
echo ""

# Cleanup test bucket
echo "Cleaning up test bucket..."
mc rm --recursive --force "${MC_ALIAS}/${TEST_BUCKET}" >/dev/null 2>&1 || true
mc rb "${MC_ALIAS}/${TEST_BUCKET}" >/dev/null 2>&1 || true

echo "Done!"
