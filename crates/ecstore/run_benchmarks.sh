#!/usr/bin/env bash
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


# Reed-Solomon SIMD performance benchmark script
# Run erasure-coding benchmarks using the high-performance SIMD implementation

set -e

# ANSI color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
NC='\033[0m' # No Color

# Print colored messages
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

# Validate system requirements
check_requirements() {
    print_info "Checking system requirements..."
    
    # Check for Rust
    if ! command -v cargo &> /dev/null; then
        print_error "Cargo not found; install Rust first"
        exit 1
    fi
    
    # Check criterion support
    if ! cargo --list | grep -q "bench"; then
        print_error "Benchmark support missing; use a Rust toolchain with criterion support"
        exit 1
    fi
    
    print_success "System requirements satisfied"
}

# Remove previous benchmark artifacts
cleanup() {
    print_info "Cleaning previous benchmark artifacts..."
    rm -rf target/criterion
    print_success "Cleanup complete"
}

# Run SIMD-only benchmarks
run_simd_benchmark() {
    print_info "🎯 Starting SIMD-only benchmark run..."
    echo "================================================"
    
    cargo bench --bench comparison_benchmark \
        -- --save-baseline simd_baseline
    
    print_success "SIMD-only benchmarks completed"
}

# Run the full benchmark suite
run_full_benchmark() {
    print_info "🚀 Starting full benchmark suite..."
    echo "================================================"
    
    # Execute detailed benchmarks
    cargo bench --bench erasure_benchmark
    
    print_success "Full benchmark suite finished"
}

# Run performance tests
run_performance_test() {
    print_info "📊 Starting performance tests..."
    echo "================================================"
    
    print_info "Step 1: running encoding benchmarks..."
    cargo bench --bench comparison_benchmark \
        -- encode --save-baseline encode_baseline
    
    print_info "Step 2: running decoding benchmarks..."
    cargo bench --bench comparison_benchmark \
        -- decode --save-baseline decode_baseline
    
    print_success "Performance tests completed"
}

# Run large dataset tests
run_large_data_test() {
    print_info "🗂️ Starting large-dataset tests..."
    echo "================================================"
    
    cargo bench --bench erasure_benchmark \
        -- large_data --save-baseline large_data_baseline
    
    print_success "Large-dataset tests completed"
}

# Generate comparison report
generate_comparison_report() {
    print_info "📊 Generating performance report..."
    
    if [ -d "target/criterion" ]; then
        print_info "Benchmark results saved under target/criterion/"
        print_info "Open target/criterion/report/index.html for the HTML report"
        
        # If Python is available, start a simple HTTP server to browse the report
        if command -v python3 &> /dev/null; then
            print_info "Run the following command to serve the report locally:"
            echo "  cd target/criterion && python3 -m http.server 8080"
            echo "  Then open http://localhost:8080/report/index.html"
        fi
    else
        print_warning "Benchmark result directory not found"
    fi
}

# Quick test mode
run_quick_test() {
    print_info "🏃 Running quick performance test..."
    
    print_info "Testing SIMD encoding performance..."
    cargo bench --bench comparison_benchmark \
        -- encode --quick
    
    print_info "Testing SIMD decoding performance..."
    cargo bench --bench comparison_benchmark \
        -- decode --quick
    
    print_success "Quick test complete"
}

# Display help
show_help() {
    echo "Reed-Solomon SIMD performance benchmark script"
    echo ""
    echo "Modes:"
    echo "  🎯 simd         High-performance reed-solomon-simd implementation"
    echo ""
    echo "Usage:"
    echo "  $0 [command]"
    echo ""
    echo "Commands:"
    echo "  quick        Run the quick performance test"
    echo "  full         Run the full benchmark suite"
    echo "  performance  Run detailed performance tests"
    echo "  simd         Run the SIMD-only tests"
    echo "  large        Run large-dataset tests"
    echo "  clean        Remove previous results"
    echo "  help         Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 quick              # Quick performance test"
    echo "  $0 performance        # Detailed performance test"
    echo "  $0 full              # Full benchmark suite"
    echo "  $0 simd              # SIMD-only benchmark"
    echo "  $0 large             # Large-dataset benchmark"
    echo ""
    echo "Features:"
    echo "  - Uses the high-performance reed-solomon-simd implementation"
    echo "  - Caches encoder/decoder instances"
    echo "  - Optimized memory management and thread safety"
    echo "  - Cross-platform SIMD instruction support"
}

# Show benchmark configuration
show_test_info() {
    print_info "📋 Benchmark configuration:"
    echo "  - Working directory: $(pwd)"
    echo "  - Rust version: $(rustc --version)"
    echo "  - Cargo version: $(cargo --version)"
    echo "  - CPU architecture: $(uname -m)"
    echo "  - Operating system: $(uname -s)"
    
    # Inspect CPU capabilities
    if [ -f "/proc/cpuinfo" ]; then
        echo "  - CPU model: $(grep 'model name' /proc/cpuinfo | head -1 | cut -d: -f2 | xargs)"
        if grep -q "avx2" /proc/cpuinfo; then
            echo "  - SIMD support: AVX2 ✅ (using advanced SIMD optimizations)"
        elif grep -q "sse4" /proc/cpuinfo; then
            echo "  - SIMD support: SSE4 ✅ (using SIMD optimizations)"
        else
            echo "  - SIMD support: baseline features"
        fi
    fi
    
    echo "  - Implementation: reed-solomon-simd (SIMD-optimized)"
    echo "  - Highlights: instance caching, thread safety, cross-platform SIMD"
    echo ""
}

# Main entry point
main() {
    print_info "🧪 Reed-Solomon SIMD benchmark suite"
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
            print_error "Unknown command: $1"
            echo ""
            show_help
            exit 1
            ;;
    esac
    
    print_success "✨ Benchmark run completed!"
}

# Launch script
main "$@" 