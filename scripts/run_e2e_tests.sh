#!/usr/bin/env bash

# E2E Test Runner Script
# Automatically starts RustFS instance, runs tests, and cleans up

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_DIR="$PROJECT_ROOT/target/debug"
RUSTFS_BINARY="$TARGET_DIR/rustfs"
DATA_DIR="$TARGET_DIR/rustfs_test_data"
RUSTFS_PID=""
TEST_FILTER=""
TEST_TYPE="all"

# Function to print colored output
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

# Function to show usage
show_usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Options:
    -h, --help              Show this help message
    -t, --test <pattern>    Run specific test(s) matching pattern
    -f, --file <file>       Run all tests in specific file (e.g., sql, basic)
    -a, --all               Run all e2e tests (default)
    
Examples:
    $0                                          # Run all e2e tests
    $0 -t test_select_object_content_csv_basic  # Run specific test
    $0 -f sql                                   # Run all SQL tests
    $0 -f reliant::sql                          # Run all tests in sql.rs file

EOF
}

# Function to cleanup on exit
cleanup() {
    print_info "Cleaning up..."
    
    # Stop RustFS if running
    if [ ! -z "$RUSTFS_PID" ] && kill -0 "$RUSTFS_PID" 2>/dev/null; then
        print_info "Stopping RustFS (PID: $RUSTFS_PID)..."
        kill "$RUSTFS_PID" 2>/dev/null || true
        sleep 2
        
        # Force kill if still running
        if kill -0 "$RUSTFS_PID" 2>/dev/null; then
            print_warning "Force killing RustFS..."
            kill -9 "$RUSTFS_PID" 2>/dev/null || true
        fi
    fi
    
    # Clean up data directory
    if [ -d "$DATA_DIR" ]; then
        print_info "Removing test data directory: $DATA_DIR"
        rm -rf "$DATA_DIR"
    fi
    
    print_success "Cleanup completed"
}

# Set trap to cleanup on exit
trap cleanup EXIT INT TERM

# Function to build RustFS
build_rustfs() {
    print_info "Building RustFS..."
    cd "$PROJECT_ROOT"
    
    if ! cargo build --bin rustfs; then
        print_error "Failed to build RustFS"
        exit 1
    fi
    
    if [ ! -f "$RUSTFS_BINARY" ]; then
        print_error "RustFS binary not found at: $RUSTFS_BINARY"
        exit 1
    fi
    
    print_success "RustFS built successfully"
}

# Function to check if required tools are available
check_dependencies() {
    local missing_tools=()
    
    if ! command -v curl >/dev/null 2>&1; then
        missing_tools+=("curl")
    fi
    
    if ! command -v cargo >/dev/null 2>&1; then
        missing_tools+=("cargo")
    fi
    
    if [ ${#missing_tools[@]} -gt 0 ]; then
        print_error "Missing required tools: ${missing_tools[*]}"
        print_error "Please install the missing tools and try again"
        exit 1
    fi
}

# Function to start RustFS
start_rustfs() {
    print_info "Starting RustFS instance..."
    
    # Create data directory and logs directory
    mkdir -p "$DATA_DIR"
    mkdir -p "$TARGET_DIR/logs"
    
    # Start RustFS in background with environment variables
    cd "$TARGET_DIR"
    RUSTFS_ACCESS_KEY=rustfsadmin RUSTFS_SECRET_KEY=rustfsadmin \
    RUSTFS_OBS_LOG_DIRECTORY="$TARGET_DIR/logs" \
    ./rustfs --address :9000 "$DATA_DIR" > rustfs.log 2>&1 &
    RUSTFS_PID=$!
    
    print_info "RustFS started with PID: $RUSTFS_PID"
    print_info "Data directory: $DATA_DIR"
    print_info "Log file: $TARGET_DIR/rustfs.log"
    print_info "RustFS logs directory: $TARGET_DIR/logs"
    
    # Wait for RustFS to be ready
    print_info "Waiting for RustFS to be ready..."
    local max_attempts=15  # Reduced from 30 to 15 seconds
    local attempt=0
    
    while [ $attempt -lt $max_attempts ]; do
        # Check if process is still running first (faster check)
        if ! kill -0 "$RUSTFS_PID" 2>/dev/null; then
            print_error "RustFS process died unexpectedly"
            print_error "Log output:"
            cat "$TARGET_DIR/rustfs.log" || true
            exit 1
        fi
        
        # Try simple HTTP connection first (most reliable)
        if curl -s --noproxy localhost --connect-timeout 2 --max-time 3 "http://localhost:9000/" >/dev/null 2>&1; then
            print_success "RustFS is ready!"
            return 0
        fi
        
        # Try health endpoint if available
        if curl -s --noproxy localhost --connect-timeout 2 --max-time 3 "http://localhost:9000/health" >/dev/null 2>&1; then
            print_success "RustFS is ready!"
            return 0
        fi
        
        # Try port connectivity check (faster than HTTP)
        if nc -z localhost 9000 2>/dev/null; then
            print_info "Port 9000 is open, verifying HTTP response..."
            if curl -s --noproxy localhost --connect-timeout 1 --max-time 2 "http://localhost:9000/" >/dev/null 2>&1; then
                print_success "RustFS is ready!"
                return 0
            fi
        fi
        
        sleep 1
        attempt=$((attempt + 1))
        echo -n "."
    done
    
    echo
    print_warning "RustFS health check failed within $max_attempts seconds"
    print_info "Checking if RustFS process is still running..."
    if kill -0 "$RUSTFS_PID" 2>/dev/null; then
        print_info "RustFS process is still running (PID: $RUSTFS_PID)"
        print_info "Trying final connection attempts..."
        
        # Quick final attempts with shorter timeouts
        for i in 1 2 3; do
            if curl -s --noproxy localhost --connect-timeout 1 --max-time 2 "http://localhost:9000/" >/dev/null 2>&1; then
                print_success "RustFS is now ready!"
                return 0
            fi
            if nc -z localhost 9000 2>/dev/null; then
                print_info "Port 9000 is accessible, continuing with tests..."
                return 0
            fi
            sleep 1
        done
        
        print_warning "RustFS may be slow to respond, but process is running"
        print_info "Continuing with tests anyway..."
        return 0
    else
        print_error "RustFS process has died"
        print_error "Log output:"
        cat "$TARGET_DIR/rustfs.log" || true
        return 1
    fi
}

# Function to run tests
run_tests() {
    print_info "Running e2e tests..."
    cd "$PROJECT_ROOT"
    
    local test_cmd="cargo test --package e2e_test --lib"
    
    case "$TEST_TYPE" in
        "specific")
            test_cmd="$test_cmd -- $TEST_FILTER --exact --show-output --ignored"
            print_info "Running specific test: $TEST_FILTER"
            ;;
        "file")
            test_cmd="$test_cmd -- $TEST_FILTER --show-output --ignored"
            print_info "Running tests in file/module: $TEST_FILTER"
            ;;
        "all")
            test_cmd="$test_cmd -- --show-output --ignored"
            print_info "Running all e2e tests"
            ;;
    esac
    
    print_info "Test command: $test_cmd"
    
    if eval "$test_cmd"; then
        print_success "All tests passed!"
        return 0
    else
        print_error "Some tests failed!"
        return 1
    fi
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            show_usage
            exit 0
            ;;
        -t|--test)
            TEST_FILTER="$2"
            TEST_TYPE="specific"
            shift 2
            ;;
        -f|--file)
            TEST_FILTER="$2"
            TEST_TYPE="file"
            shift 2
            ;;
        -a|--all)
            TEST_TYPE="all"
            shift
            ;;
        *)
            print_error "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# Main execution
main() {
    print_info "Starting E2E Test Runner"
    print_info "Project root: $PROJECT_ROOT"
    print_info "Target directory: $TARGET_DIR"
    
    # Check dependencies
    check_dependencies
    
    # Build RustFS
    build_rustfs
    
    # Start RustFS
    if ! start_rustfs; then
        print_error "Failed to start RustFS properly"
        print_info "Checking if we can still run tests..."
        if [ ! -z "$RUSTFS_PID" ] && kill -0 "$RUSTFS_PID" 2>/dev/null; then
            print_info "RustFS process is still running, attempting to continue..."
        else
            print_error "RustFS is not running, cannot proceed with tests"
            exit 1
        fi
    fi
    
    # Run tests
    local test_result=0
    run_tests || test_result=$?
    
    # Cleanup will be handled by trap
    
    if [ $test_result -eq 0 ]; then
        print_success "E2E tests completed successfully!"
        exit 0
    else
        print_error "E2E tests failed!"
        exit 1
    fi
}

# Run main function
main "$@"