#!/bin/bash

# Reed-Solomon SIMD 性能基准测试脚本
# 使用高性能 SIMD 实现进行纠删码性能测试

set -e

# ANSI 颜色码
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
NC='\033[0m' # No Color

# 打印带颜色的消息
print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# 检查系统要求
check_requirements() {
    print_info "检查系统要求..."
    
    # 检查 Rust
    if ! command -v cargo &> /dev/null; then
        print_error "Cargo 未找到，请确保已安装 Rust"
        exit 1
    fi
    
    # 检查 criterion
    if ! cargo --list | grep -q "bench"; then
        print_error "未找到基准测试支持，请确保使用的是支持基准测试的 Rust 版本"
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

# 运行 SIMD 模式基准测试
run_simd_benchmark() {
    print_info "🎯 开始运行 SIMD 模式基准测试..."
    echo "================================================"
    
    cargo bench --bench comparison_benchmark \
        -- --save-baseline simd_baseline
    
    print_success "SIMD 模式基准测试完成"
}

# 运行完整的基准测试套件
run_full_benchmark() {
    print_info "🚀 开始运行完整基准测试套件..."
    echo "================================================"
    
    # 运行详细的基准测试
    cargo bench --bench erasure_benchmark
    
    print_success "完整基准测试套件完成"
}

# 运行性能测试
run_performance_test() {
    print_info "📊 开始运行性能测试..."
    echo "================================================"
    
    print_info "步骤 1: 运行编码基准测试..."
    cargo bench --bench comparison_benchmark \
        -- encode --save-baseline encode_baseline
    
    print_info "步骤 2: 运行解码基准测试..."
    cargo bench --bench comparison_benchmark \
        -- decode --save-baseline decode_baseline
    
    print_success "性能测试完成"
}

# 运行大数据集测试
run_large_data_test() {
    print_info "🗂️ 开始运行大数据集测试..."
    echo "================================================"
    
    cargo bench --bench erasure_benchmark \
        -- large_data --save-baseline large_data_baseline
    
    print_success "大数据集测试完成"
}

# 生成比较报告
generate_comparison_report() {
    print_info "📊 生成性能报告..."
    
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
    
    print_info "测试 SIMD 编码性能..."
    cargo bench --bench comparison_benchmark \
        -- encode --quick
    
    print_info "测试 SIMD 解码性能..."
    cargo bench --bench comparison_benchmark \
        -- decode --quick
    
    print_success "快速测试完成"
}

# 显示帮助信息
show_help() {
    echo "Reed-Solomon SIMD 性能基准测试脚本"
    echo ""
    echo "实现模式："
    echo "  🎯 SIMD 模式 - 高性能 SIMD 优化的 reed-solomon-simd 实现"
    echo ""
    echo "使用方法:"
    echo "  $0 [command]"
    echo ""
    echo "命令:"
    echo "  quick        运行快速性能测试"
    echo "  full         运行完整基准测试套件"
    echo "  performance  运行详细的性能测试"
    echo "  simd         运行 SIMD 模式测试"
    echo "  large        运行大数据集测试"
    echo "  clean        清理测试结果"
    echo "  help         显示此帮助信息"
    echo ""
    echo "示例:"
    echo "  $0 quick              # 快速性能测试"
    echo "  $0 performance        # 详细性能测试"
    echo "  $0 full              # 完整测试套件"
    echo "  $0 simd              # SIMD 模式测试"
    echo "  $0 large             # 大数据集测试"
    echo ""
    echo "实现特性:"
    echo "  - 使用 reed-solomon-simd 高性能 SIMD 实现"
    echo "  - 支持编码器/解码器实例缓存"
    echo "  - 优化的内存管理和线程安全"
    echo "  - 跨平台 SIMD 指令支持"
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
            echo "  - SIMD 支持: AVX2 ✅ (将使用高级 SIMD 优化)"
        elif grep -q "sse4" /proc/cpuinfo; then
            echo "  - SIMD 支持: SSE4 ✅ (将使用 SIMD 优化)"
        else
            echo "  - SIMD 支持: 基础 SIMD 特性"
        fi
    fi
    
    echo "  - 实现: reed-solomon-simd (高性能 SIMD 优化)"
    echo "  - 特性: 实例缓存、线程安全、跨平台 SIMD"
    echo ""
}

# 主函数
main() {
    print_info "🧪 Reed-Solomon SIMD 实现性能基准测试"
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
        "performance")
            cleanup
            run_performance_test
            generate_comparison_report
            ;;
        "simd")
            cleanup
            run_simd_benchmark
            generate_comparison_report
            ;;
        "large")
            cleanup
            run_large_data_test
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
}

# 启动脚本
main "$@" 