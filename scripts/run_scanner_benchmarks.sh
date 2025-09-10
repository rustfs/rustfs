#!/bin/bash

# Scanner性能优化基准测试运行脚本
# 使用方法: ./scripts/run_scanner_benchmarks.sh [test_type] [quick]

set -e

WORKSPACE_ROOT="/home/dandan/code/rust/rustfs"
cd "$WORKSPACE_ROOT"

# 基本参数
QUICK_MODE=false
TEST_TYPE="all"

# 解析命令行参数
if [[ "$1" == "quick" ]] || [[ "$2" == "quick" ]]; then
    QUICK_MODE=true
fi

if [[ -n "$1" ]] && [[ "$1" != "quick" ]]; then
    TEST_TYPE="$1"
fi

# 快速模式的基准测试参数
if [[ "$QUICK_MODE" == "true" ]]; then
    BENCH_ARGS="--sample-size 10 --warm-up-time 1 --measurement-time 2"
    echo "🚀 运行快速基准测试模式..."
else
    BENCH_ARGS=""
    echo "🏃 运行完整基准测试模式..."
fi

echo "📊 Scanner性能优化基准测试"
echo "工作目录: $WORKSPACE_ROOT"
echo "测试类型: $TEST_TYPE"
echo "快速模式: $QUICK_MODE"
echo "="

# 检查编译状态
echo "🔧 检查编译状态..."
if ! cargo check --package rustfs-ahm --benches --quiet; then
    echo "❌ 基准测试编译失败"
    exit 1
fi
echo "✅ 编译检查通过"

# 基准测试函数
run_benchmark() {
    local bench_name=$1
    local description=$2
    
    echo ""
    echo "🧪 运行 $description"
    echo "基准测试: $bench_name"
    echo "参数: $BENCH_ARGS"
    
    if timeout 300 cargo bench --package rustfs-ahm --bench "$bench_name" -- $BENCH_ARGS; then
        echo "✅ $description 完成"
    else
        echo "⚠️  $description 运行超时或失败"
        return 1
    fi
}

# 运行指定的基准测试
case "$TEST_TYPE" in
    "business" | "business_io")
        run_benchmark "business_io_impact" "业务IO影响测试"
        ;;
    "scanner" | "performance")
        run_benchmark "scanner_performance" "Scanner性能测试"
        ;;
    "resource" | "contention")
        run_benchmark "resource_contention" "资源竞争测试"
        ;;
    "adaptive" | "scheduling")
        run_benchmark "adaptive_scheduling" "智能调度测试"
        ;;
    "list")
        echo "📋 列出所有可用的基准测试:"
        cargo bench --package rustfs-ahm -- --list
        ;;
    "all")
        echo "🚀 运行所有基准测试..."
        
        echo ""
        echo "=== 1/4 业务IO影响测试 ==="
        if ! run_benchmark "business_io_impact" "业务IO影响测试"; then
            echo "⚠️  业务IO影响测试失败，继续运行其他测试..."
        fi
        
        echo ""
        echo "=== 2/4 Scanner性能测试 ==="
        if ! run_benchmark "scanner_performance" "Scanner性能测试"; then
            echo "⚠️  Scanner性能测试失败，继续运行其他测试..."
        fi
        
        echo ""
        echo "=== 3/4 资源竞争测试 ==="
        if ! run_benchmark "resource_contention" "资源竞争测试"; then
            echo "⚠️  资源竞争测试失败，继续运行其他测试..."
        fi
        
        echo ""
        echo "=== 4/4 智能调度测试 ==="
        if ! run_benchmark "adaptive_scheduling" "智能调度测试"; then
            echo "⚠️  智能调度测试失败"
        fi
        ;;
    *)
        echo "❌ 未知的测试类型: $TEST_TYPE"
        echo ""
        echo "用法: $0 [test_type] [quick]"
        echo ""
        echo "测试类型:"
        echo "  all                 - 运行所有基准测试 (默认)"
        echo "  business|business_io - 业务IO影响测试"
        echo "  scanner|performance - Scanner性能测试"
        echo "  resource|contention - 资源竞争测试"
        echo "  adaptive|scheduling - 智能调度测试"
        echo "  list               - 列出所有可用测试"
        echo ""
        echo "选项:"
        echo "  quick              - 快速模式 (减少样本数和测试时间)"
        echo ""
        echo "示例:"
        echo "  $0 business quick  - 快速运行业务IO测试"
        echo "  $0 all            - 运行所有完整测试"
        echo "  $0 list           - 列出所有测试"
        exit 1
        ;;
esac

echo ""
echo "🎉 基准测试脚本执行完成!"
echo "📊 查看结果: target/criterion/ 目录下有详细的HTML报告"