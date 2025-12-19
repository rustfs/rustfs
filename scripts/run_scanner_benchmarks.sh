#!/usr/bin/env bash

# Scanner performance benchmark runner
# Usage: ./scripts/run_scanner_benchmarks.sh [test_type] [quick]

set -e

WORKSPACE_ROOT="/home/dandan/code/rust/rustfs"
cd "$WORKSPACE_ROOT"

# Default parameters
QUICK_MODE=false
TEST_TYPE="all"

# Parse command-line arguments
if [[ "$1" == "quick" ]] || [[ "$2" == "quick" ]]; then
    QUICK_MODE=true
fi

if [[ -n "$1" ]] && [[ "$1" != "quick" ]]; then
    TEST_TYPE="$1"
fi

# Benchmark options for quick mode
if [[ "$QUICK_MODE" == "true" ]]; then
    BENCH_ARGS="--sample-size 10 --warm-up-time 1 --measurement-time 2"
    echo "🚀 Running benchmarks in quick mode..."
else
    BENCH_ARGS=""
    echo "🏃 Running the full benchmark suite..."
fi

echo "📊 Scanner performance benchmarks"
echo "Working directory: $WORKSPACE_ROOT"
echo "Selected benchmark group: $TEST_TYPE"
echo "Quick mode: $QUICK_MODE"
echo "="

# Verify the workspace compiles
echo "🔧 Checking compilation status..."
if ! cargo check --package rustfs-ahm --benches --quiet; then
    echo "❌ Benchmark compilation failed"
    exit 1
fi
echo "✅ Compilation succeeded"

# Helper to run an individual benchmark target
run_benchmark() {
    local bench_name=$1
    local description=$2

    echo ""
    echo "🧪 Running $description"
    echo "Benchmark: $bench_name"
    echo "Arguments: $BENCH_ARGS"

    if timeout 300 cargo bench --package rustfs-ahm --bench "$bench_name" -- $BENCH_ARGS; then
        echo "✅ $description finished"
    else
        echo "⚠️  $description timed out or failed"
        return 1
    fi
}

# Dispatch benchmarks based on the requested test type
case "$TEST_TYPE" in
    "business" | "business_io")
        run_benchmark "business_io_impact" "Business I/O impact"
        ;;
    "scanner" | "performance")
        run_benchmark "scanner_performance" "Scanner performance"
        ;;
    "resource" | "contention")
        run_benchmark "resource_contention" "Resource contention"
        ;;
    "adaptive" | "scheduling")
        run_benchmark "adaptive_scheduling" "Adaptive scheduling"
        ;;
    "list")
        echo "📋 Available benchmarks:"
        cargo bench --package rustfs-ahm -- --list
        ;;
    "all")
        echo "🚀 Running the full benchmark suite..."

        echo ""
        echo "=== 1/4 Business I/O impact ==="
        if ! run_benchmark "business_io_impact" "Business I/O impact"; then
            echo "⚠️  Business I/O impact benchmark failed, continuing..."
        fi

        echo ""
        echo "=== 2/4 Scanner performance ==="
        if ! run_benchmark "scanner_performance" "Scanner performance"; then
            echo "⚠️  Scanner performance benchmark failed, continuing..."
        fi

        echo ""
        echo "=== 3/4 Resource contention ==="
        if ! run_benchmark "resource_contention" "Resource contention"; then
            echo "⚠️  Resource contention benchmark failed, continuing..."
        fi

        echo ""
        echo "=== 4/4 Adaptive scheduling ==="
        if ! run_benchmark "adaptive_scheduling" "Adaptive scheduling"; then
            echo "⚠️  Adaptive scheduling benchmark failed"
        fi
        ;;
    *)
        echo "❌ Unknown test type: $TEST_TYPE"
        echo ""
        echo "Usage: $0 [test_type] [quick]"
        echo ""
        echo "Available test types:"
        echo "  all                 - run the entire benchmark suite (default)"
        echo "  business|business_io - business I/O impact benchmark"
        echo "  scanner|performance - scanner performance benchmark"
        echo "  resource|contention - resource contention benchmark"
        echo "  adaptive|scheduling - adaptive scheduling benchmark"
        echo "  list               - list all benchmarks"
        echo ""
        echo "Options:"
        echo "  quick              - quick mode (smaller sample size and duration)"
        echo ""
        echo "Examples:"
        echo "  $0 business quick  - run the business I/O benchmark in quick mode"
        echo "  $0 all            - run every benchmark"
        echo "  $0 list           - list available benchmarks"
        exit 1
        ;;
esac

echo ""
echo "🎉 Benchmark script finished!"
echo "📊 Detailed HTML reports are available under target/criterion/"