#!/usr/bin/env bash
# Benchmark script for GET small-file optimization (SF01-SF07)
# Matches baseline parameters from issue714-local-single-machine-multidisk-get-2026-06-26.md
set -euo pipefail

WARP_HOST="${WARP_HOST:-127.0.0.1:19031}"
export WARP_ACCESS_KEY="${WARP_ACCESS_KEY:-rustfsadmin}"
export WARP_SECRET_KEY="${WARP_SECRET_KEY:-rustfsadmin}"

SIZES="1KiB 4KiB 10KiB 100KiB 1MiB"
CONCURRENCY=32
DURATION=10s
ROUNDS=3
COOLDOWN=10
OBJECTS=8
OUT_DIR="target/bench/sf-optimization-$(date +%Y%m%d-%H%M%S)"

mkdir -p "$OUT_DIR"

echo "=========================================="
echo "GET Small-File Optimization Benchmark"
echo "=========================================="
echo "Host: $WARP_HOST"
echo "Output: $OUT_DIR"
echo ""

# Save environment info
cat > "$OUT_DIR/meta.env" <<EOF
DATE=$(date -u +%Y-%m-%dT%H:%M:%SZ)
HOST=$WARP_HOST
CONCURRENCY=$CONCURRENCY
DURATION=$DURATION
ROUNDS=$ROUNDS
OBJECTS=$OBJECTS
BRANCH=$(git rev-parse --abbrev-ref HEAD)
COMMIT=$(git rev-parse --short HEAD)
RUST_VERSION=$(rustc --version)
EOF

# CSV header for summary
echo "size,round,throughput_mib_s,requests_per_s,p50_ms,total_bytes,errors" > "$OUT_DIR/summary.csv"

for size in $SIZES; do
    echo ""
    echo "=========================================="
    echo "Testing: $size"
    echo "=========================================="

    for round in $(seq 1 $ROUNDS); do
        echo "  Round $round/$ROUNDS..."

        json_file="$OUT_DIR/get-${size}-round${round}.json"

        warp get \
            --host="$WARP_HOST" \
            --obj.size="$size" \
            --concurrent=$CONCURRENCY \
            --duration=$DURATION \
            --objects=$OBJECTS \
            --noclear \
            --lookup=path \
            --analyze.out="$json_file" \
            2>/dev/null

        # Extract key metrics from JSON
        if [ -f "$json_file" ]; then
            throughput=$(python3 -c "
import json, sys
with open('$json_file') as f:
    data = json.load(f)
for op in data.get('operations', []):
    if op.get('operation') == 'GET':
        mb_s = op.get('mb_per_sec', 0)
        rps = op.get('requests_per_sec', 0)
        p50 = 0
        for t in op.get('throughput', []):
            pass
        # Get p50 from time_series_aggregated
        tsa = op.get('time_series_aggregated', {})
        if tsa:
            p50 = tsa.get('median_ms', 0)
        total = op.get('total_bytes', 0)
        errors = op.get('requests_errors', 0) or 0
        print(f'{mb_s:.2f},{rps:.2f},{p50:.1f},{total},{errors}')
        sys.exit(0)
print('0,0,0,0,0')
" 2>/dev/null || echo "0,0,0,0,0")

            echo "$size,$round,$throughput" >> "$OUT_DIR/summary.csv"
            echo "    -> $throughput"
        else
            echo "$size,$round,0,0,0,0,0" >> "$OUT_DIR/summary.csv"
            echo "    -> FAILED (no output)"
        fi

        echo "  Cooling down ${COOLDOWN}s..."
        sleep $COOLDOWN
    done

    # Extra cooldown between sizes
    echo "  Extra cooldown ${COOLDOWN}s between sizes..."
    sleep $COOLDOWN
done

echo ""
echo "=========================================="
echo "Benchmark Complete"
echo "=========================================="
echo "Results: $OUT_DIR/summary.csv"
echo ""

# Print summary table
echo "Summary (MiB/s):"
echo "---------------------------------------------------"
printf "%-10s" "Size"
for r in $(seq 1 $ROUNDS); do
    printf "%-12s" "Round $r"
done
printf "%-12s\n" "Median"
echo "---------------------------------------------------"

for size in $SIZES; do
    printf "%-10s" "$size"
    values=()
    for r in $(seq 1 $ROUNDS); do
        val=$(grep "^$size,$r," "$OUT_DIR/summary.csv" | cut -d',' -f3)
        values+=("$val")
        printf "%-12s" "$val"
    done
    # Calculate median
    median=$(printf '%s\n' "${values[@]}" | sort -n | sed -n "$(((${#values[@]}+1)/2))p")
    printf "%-12s\n" "$median"
done

echo ""
echo "Comparison with baseline (MiB/s):"
echo "---------------------------------------------------"
printf "%-10s %-12s %-12s %-12s %-12s\n" "Size" "MinIO" "RustFS main" "This branch" "vs main"
echo "---------------------------------------------------"

# Baseline data from issue714
declare -A MINIO_DATA=(
    ["1KiB"]="21.15" ["4KiB"]="51.19" ["10KiB"]="201.23" ["100KiB"]="1142.89" ["1MiB"]="7264.10"
)
declare -A MAIN_DATA=(
    ["1KiB"]="2.88" ["4KiB"]="11.30" ["10KiB"]="28.56" ["100KiB"]="277.49" ["1MiB"]="2270.27"
)

for size in $SIZES; do
    values=()
    for r in $(seq 1 $ROUNDS); do
        val=$(grep "^$size,$r," "$OUT_DIR/summary.csv" | cut -d',' -f3)
        values+=("$val")
    done
    median=$(printf '%s\n' "${values[@]}" | sort -n | sed -n "$(((${#values[@]}+1)/2))p")
    main_val=${MAIN_DATA[$size]}
    minio_val=${MINIO_DATA[$size]}

    if [ "$main_val" != "0" ] && [ "$main_val" != "" ]; then
        pct_change=$(python3 -c "print(f'{(($median - $main_val) / $main_val * 100):+.1f}%')" 2>/dev/null || echo "N/A")
    else
        pct_change="N/A"
    fi

    printf "%-10s %-12s %-12s %-12s %-12s\n" "$size" "$minio_val" "$main_val" "$median" "$pct_change"
done
