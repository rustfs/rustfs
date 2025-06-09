#!/bin/bash

# Reed-Solomon 实现性能比较脚本
# 
# 这个脚本将运行不同的基准测试来比较混合模式和纯Erasure模式的性能
# 
# 使用方法:
#   ./run_benchmarks.sh [quick|full|comparison]
#
#   quick      - 快速测试主要场景
#   full       - 完整基准测试套件
#   comparison - 专门对比两种实现模式

set -e

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 输出带颜色的信息
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 检查是否安装了必要工具
check_requirements() {
    print_info "检查系统要求..."
    
    if ! command -v cargo &> /dev/null; then
        print_error "cargo 未安装，请先安装 Rust 工具链"
        exit 1
    fi
    
    # 检查是否安装了 criterion
    if ! grep -q "criterion" Cargo.toml; then
        print_error "Cargo.toml 中未找到 criterion 依赖"
        exit 1
    fi
    
    print_success "系统要求检查通过"
}

# 清理之前的测试结果
cleanup() {
    print_info "清理之前的测试结果..."
    rm -rf target/criterion
    print_success "清理完成"
}

# 运行纯 Erasure 模式基准测试
run_erasure_benchmark() {
    print_info "🏛️ 开始运行纯 Erasure 模式基准测试..."
    echo "================================================"
    
    cargo bench --bench comparison_benchmark \
        --features reed-solomon-erasure \
        -- --save-baseline erasure_baseline
    
    print_success "纯 Erasure 模式基准测试完成"
}

# 运行混合模式基准测试（默认）
run_hybrid_benchmark() {
    print_info "🎯 开始运行混合模式基准测试（默认）..."
    echo "================================================"
    
    cargo bench --bench comparison_benchmark \
        -- --save-baseline hybrid_baseline
    
    print_success "混合模式基准测试完成"
}

# 运行完整的基准测试套件
run_full_benchmark() {
    print_info "🚀 开始运行完整基准测试套件..."
    echo "================================================"
    
    # 运行详细的基准测试（使用默认混合模式）
    cargo bench --bench erasure_benchmark
    
    print_success "完整基准测试套件完成"
}

# 运行性能对比测试
run_comparison_benchmark() {
    print_info "📊 开始运行性能对比测试..."
    echo "================================================"
    
    print_info "步骤 1: 测试纯 Erasure 模式..."
    cargo bench --bench comparison_benchmark \
        --features reed-solomon-erasure \
        -- --save-baseline erasure_baseline
    
    print_info "步骤 2: 测试混合模式并与 Erasure 模式对比..."
    cargo bench --bench comparison_benchmark \
        -- --baseline erasure_baseline
    
    print_success "性能对比测试完成"
}

# 生成比较报告
generate_comparison_report() {
    print_info "📊 生成性能比较报告..."
    
    if [ -d "target/criterion" ]; then
        print_info "基准测试结果已保存到 target/criterion/ 目录"
        print_info "你可以打开 target/criterion/report/index.html 查看详细报告"
        
        # 如果有 python 环境，可以启动简单的 HTTP 服务器查看报告
        if command -v python3 &> /dev/null; then
            print_info "你可以运行以下命令启动本地服务器查看报告:"
            echo "  cd target/criterion && python3 -m http.server 8080"
            echo "  然后在浏览器中访问 http://localhost:8080/report/index.html"
        fi
    else
        print_warning "未找到基准测试结果目录"
    fi
}

# 快速测试模式
run_quick_test() {
    print_info "🏃 运行快速性能测试..."
    
    print_info "测试纯 Erasure 模式..."
    cargo bench --bench comparison_benchmark \
        --features reed-solomon-erasure \
        -- encode_comparison --quick
    
    print_info "测试混合模式（默认）..."
    cargo bench --bench comparison_benchmark \
        -- encode_comparison --quick
    
    print_success "快速测试完成"
}

# 显示帮助信息
show_help() {
    echo "Reed-Solomon 性能基准测试脚本"
    echo ""
    echo "实现模式："
    echo "  🎯 混合模式（默认）    - SIMD + Erasure 智能回退，推荐使用"
    echo "  🏛️ 纯 Erasure 模式    - 稳定兼容的 reed-solomon-erasure 实现"
    echo ""
    echo "使用方法:"
    echo "  $0 [command]"
    echo ""
    echo "命令:"
    echo "  quick        运行快速性能测试"
    echo "  full         运行完整基准测试套件（混合模式）"
    echo "  comparison   运行详细的实现模式对比测试"
    echo "  erasure      只测试纯 Erasure 模式"
    echo "  hybrid       只测试混合模式（默认行为）"
    echo "  clean        清理测试结果"
    echo "  help         显示此帮助信息"
    echo ""
    echo "示例:"
    echo "  $0 quick              # 快速测试两种模式"
    echo "  $0 comparison         # 详细对比测试"
    echo "  $0 full              # 完整测试套件（混合模式）"
    echo "  $0 hybrid            # 只测试混合模式"
    echo "  $0 erasure           # 只测试纯 Erasure 模式"
    echo ""
    echo "模式说明:"
    echo "  混合模式: 大分片(≥512B)使用SIMD优化，小分片自动回退到Erasure"
    echo "  Erasure模式: 所有情况都使用reed-solomon-erasure实现"
}

# 显示测试配置信息
show_test_info() {
    print_info "📋 测试配置信息:"
    echo "  - 当前目录: $(pwd)"
    echo "  - Rust 版本: $(rustc --version)"
    echo "  - Cargo 版本: $(cargo --version)"
    echo "  - CPU 架构: $(uname -m)"
    echo "  - 操作系统: $(uname -s)"
    
    # 检查 CPU 特性
    if [ -f "/proc/cpuinfo" ]; then
        echo "  - CPU 型号: $(grep 'model name' /proc/cpuinfo | head -1 | cut -d: -f2 | xargs)"
        if grep -q "avx2" /proc/cpuinfo; then
            echo "  - SIMD 支持: AVX2 ✅ (混合模式将利用SIMD优化)"
        elif grep -q "sse4" /proc/cpuinfo; then
            echo "  - SIMD 支持: SSE4 ✅ (混合模式将利用SIMD优化)"
        else
            echo "  - SIMD 支持: 未检测到高级 SIMD 特性 (混合模式将主要使用Erasure)"
        fi
    fi
    
    echo "  - 默认模式: 混合模式 (SIMD + Erasure 智能回退)"
    echo "  - 回退阈值: 512字节分片大小"
    echo ""
}

# 主函数
main() {
    print_info "🧪 Reed-Solomon 实现性能基准测试"
    echo "================================================"
    
    check_requirements
    show_test_info
    
    case "${1:-help}" in
        "quick")
            run_quick_test
            generate_comparison_report
            ;;
        "full")
            cleanup
            run_full_benchmark
            generate_comparison_report
            ;;
        "comparison")
            cleanup
            run_comparison_benchmark
            generate_comparison_report
            ;;
        "erasure")
            cleanup
            run_erasure_benchmark
            generate_comparison_report
            ;;
        "hybrid")
            cleanup
            run_hybrid_benchmark
            generate_comparison_report
            ;;
        "clean")
            cleanup
            ;;
        "help"|"--help"|"-h")
            show_help
            ;;
        *)
            print_error "未知命令: $1"
            echo ""
            show_help
            exit 1
            ;;
    esac
    
    print_success "✨ 基准测试执行完成!"
    print_info "💡 提示: 推荐使用混合模式（默认），它能自动在SIMD和Erasure之间智能选择"
}

# 如果直接运行此脚本，调用主函数
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi 