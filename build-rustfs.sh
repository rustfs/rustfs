#!/usr/bin/env bash

# RustFS Binary Build Script
# This script compiles RustFS binaries for different platforms and architectures

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Auto-detect current platform
detect_platform() {
    local arch=$(uname -m)
    local os=$(uname -s | tr '[:upper:]' '[:lower:]')

    case "$os" in
        "linux")
            case "$arch" in
                "x86_64")
                    # Default to GNU for better compatibility
                    echo "x86_64-unknown-linux-gnu"
                    ;;
                "aarch64"|"arm64")
                    echo "aarch64-unknown-linux-gnu"
                    ;;
                "armv7l")
                    echo "armv7-unknown-linux-gnueabihf"
                    ;;
                "loongarch64")
                    echo "loongarch64-unknown-linux-gnu"
                    ;;
                *)
                    echo "unknown-platform"
                    ;;
            esac
            ;;
        "darwin")
            case "$arch" in
                "x86_64")
                    echo "x86_64-apple-darwin"
                    ;;
                "arm64"|"aarch64")
                    echo "aarch64-apple-darwin"
                    ;;
                *)
                    echo "unknown-platform"
                    ;;
            esac
            ;;
        *)
            echo "unknown-platform"
            ;;
    esac
}

# Cross-platform SHA256 checksum generation
generate_sha256() {
    local file="$1"
    local output_file="$2"
    local os=$(uname -s | tr '[:upper:]' '[:lower:]')

    case "$os" in
        "linux")
            if command -v sha256sum &> /dev/null; then
                sha256sum "$file" > "$output_file"
            elif command -v shasum &> /dev/null; then
                shasum -a 256 "$file" > "$output_file"
            else
                print_message $RED "❌ No SHA256 command found (sha256sum or shasum)"
                return 1
            fi
            ;;
        "darwin")
            if command -v shasum &> /dev/null; then
                shasum -a 256 "$file" > "$output_file"
            elif command -v sha256sum &> /dev/null; then
                sha256sum "$file" > "$output_file"
            else
                print_message $RED "❌ No SHA256 command found (shasum or sha256sum)"
                return 1
            fi
            ;;
        *)
            # Try common commands in order
            if command -v sha256sum &> /dev/null; then
                sha256sum "$file" > "$output_file"
            elif command -v shasum &> /dev/null; then
                shasum -a 256 "$file" > "$output_file"
            else
                print_message $RED "❌ No SHA256 command found"
                return 1
            fi
            ;;
    esac
}

# Default values
OUTPUT_DIR="target/release"
PLATFORM=$(detect_platform)  # Auto-detect current platform
BINARY_NAME="rustfs"
BUILD_TYPE="release"
SIGN=false
WITH_CONSOLE=true
FORCE_CONSOLE_UPDATE=false
CONSOLE_VERSION="latest"
SKIP_VERIFICATION=false
CUSTOM_PLATFORM=""

# Print usage
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Description:"
    echo "  Build RustFS binary for the current platform. Designed for CI/CD pipelines"
    echo "  where different runners build platform-specific binaries natively."
    echo "  Includes automatic verification to ensure the built binary is functional."
    echo ""
    echo "Options:"
    echo "  -o, --output-dir DIR       Output directory (default: target/release)"
    echo "  -b, --binary-name NAME     Binary name (default: rustfs)"
    echo "  -p, --platform TARGET      Target platform (default: auto-detect)"
    echo "                              Supported platforms:"
    echo "                                x86_64-unknown-linux-gnu"
    echo "                                aarch64-unknown-linux-gnu"
    echo "                                loongarch64-unknown-linux-gnu"
    echo "                                armv7-unknown-linux-gnueabihf"
    echo "                                x86_64-unknown-linux-musl"
    echo "                                aarch64-unknown-linux-musl"
    echo "                                armv7-unknown-linux-musleabihf"
    echo "                                x86_64-apple-darwin"
    echo "                                aarch64-apple-darwin"
    echo "                                x86_64-pc-windows-msvc"
    echo "                                aarch64-pc-windows-msvc"
    echo "  --dev                      Build in dev mode"
    echo "  --sign                     Sign binaries after build"
    echo "  --with-console             Download console static assets (default)"
    echo "  --no-console               Skip console static assets"
    echo "  --force-console-update     Force update console assets even if they exist"
    echo "  --console-version VERSION  Console version to download (default: latest)"
    echo "  --skip-verification        Skip binary verification after build"
    echo "  -h, --help                 Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                         # Build for current platform (includes console assets)"
    echo "  $0 --dev                   # Development build"
    echo "  $0 --sign                  # Build and sign binary (release CI)"
    echo "  $0 --no-console            # Build without console static assets"
    echo "  $0 --force-console-update  # Force update console assets"
    echo "  $0 --platform x86_64-unknown-linux-musl  # Build for specific platform"
    echo "  $0 --skip-verification     # Skip binary verification (for cross-compilation)"
    echo ""
    echo "Detected platform: $(detect_platform)"
    echo "CI Usage: Run this script on each platform's runner to build native binaries"
}

# Print colored message
print_message() {
    local color=$1
    local message=$2
    echo -e "${color}${message}${NC}"
}

# Prevent zig/ld from hitting macOS file descriptor defaults during linking
ensure_file_descriptor_limit() {
    local required_limit=4096
    local current_limit
    current_limit=$(ulimit -Sn 2>/dev/null || echo "")

    if [ -z "$current_limit" ] || [ "$current_limit" = "unlimited" ]; then
        return
    fi

    if (( current_limit >= required_limit )); then
        return
    fi

    local hard_limit target_limit
    hard_limit=$(ulimit -Hn 2>/dev/null || echo "")
    target_limit=$required_limit

    if [ -n "$hard_limit" ] && [ "$hard_limit" != "unlimited" ] && (( hard_limit < required_limit )); then
        target_limit=$hard_limit
    fi

    if ulimit -Sn "$target_limit" 2>/dev/null; then
        print_message $YELLOW "🔧 Increased open file limit from $current_limit to $target_limit to avoid ProcessFdQuotaExceeded"
    else
        print_message $YELLOW "⚠️ Unable to raise ulimit -n automatically (current: $current_limit, needed: $required_limit). Please run 'ulimit -n $required_limit' manually before building."
    fi
}

# Get version from git
get_version() {
    if git describe --abbrev=0 --tags >/dev/null 2>&1; then
        git describe --abbrev=0 --tags
    else
        git rev-parse --short HEAD
    fi
}

# Setup rust environment
setup_rust_environment() {
    print_message $BLUE "🔧 Setting up Rust environment..."

    # Install required target for current platform
    print_message $YELLOW "Installing target: $PLATFORM"
    rustup target add "$PLATFORM"

    # Set up environment variables for musl targets
    if [[ "$PLATFORM" == *"musl"* ]]; then
        print_message $YELLOW "Setting up environment for musl target..."
        export RUSTFLAGS="-C target-feature=-crt-static"

        # For cargo-zigbuild, set up additional environment variables
        if command -v cargo-zigbuild &> /dev/null; then
            print_message $YELLOW "Configuring cargo-zigbuild for musl target..."

            # Set environment variables for better musl support
            export CC_x86_64_unknown_linux_musl="zig cc -target x86_64-linux-musl"
            export CXX_x86_64_unknown_linux_musl="zig c++ -target x86_64-linux-musl"
            export AR_x86_64_unknown_linux_musl="zig ar"
            export CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER="zig cc -target x86_64-linux-musl"

            export CC_aarch64_unknown_linux_musl="zig cc -target aarch64-linux-musl"
            export CXX_aarch64_unknown_linux_musl="zig c++ -target aarch64-linux-musl"
            export AR_aarch64_unknown_linux_musl="zig ar"
            export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER="zig cc -target aarch64-linux-musl"

            # Set environment variables for zstd-sys to avoid target parsing issues
            export ZSTD_SYS_USE_PKG_CONFIG=1
            export PKG_CONFIG_ALLOW_CROSS=1
        fi
    fi

    # Install required tools
    if [ "$SIGN" = true ]; then
        if ! command -v minisign &> /dev/null; then
            print_message $YELLOW "Installing minisign for binary signing..."
            cargo install minisign
        fi
    fi
}

# Download console static assets
download_console_assets() {
    local static_dir="rustfs/static"
    local console_exists=false

    # Check if console assets already exist
    if [ -d "$static_dir" ] && [ -f "$static_dir/index.html" ]; then
        console_exists=true
        local static_size=$(du -sh "$static_dir" 2>/dev/null | cut -f1 || echo "unknown")
        print_message $YELLOW "Console static assets already exist ($static_size)"
    fi

    # Determine if we need to download
    local should_download=false
    if [ "$WITH_CONSOLE" = true ]; then
        if [ "$console_exists" = false ]; then
            print_message $BLUE "🎨 Console assets not found, downloading..."
            should_download=true
        elif [ "$FORCE_CONSOLE_UPDATE" = true ]; then
            print_message $BLUE "🎨 Force updating console assets..."
            should_download=true
        else
            print_message $GREEN "✅ Console assets already available, skipping download"
        fi
    else
        if [ "$console_exists" = true ]; then
            print_message $GREEN "✅ Using existing console assets"
        else
            print_message $YELLOW "⚠️  Console assets not found. Use --download-console to download them."
        fi
    fi

    if [ "$should_download" = true ]; then
        print_message $BLUE "📥 Downloading console static assets..."

        # Create static directory
        mkdir -p "$static_dir"

        # Download from GitHub Releases (consistent with Docker build)
        local download_url
        if [ "$CONSOLE_VERSION" = "latest" ]; then
            print_message $YELLOW "Getting latest console release info..."
            # For now, use dl.rustfs.com as fallback until GitHub Releases includes console assets
            download_url="https://dl.rustfs.com/artifacts/console/rustfs-console-latest.zip"
        else
            download_url="https://dl.rustfs.com/artifacts/console/rustfs-console-${CONSOLE_VERSION}.zip"
        fi

        print_message $YELLOW "Downloading from: $download_url"

        # Download with retries
        local temp_file="console-assets-temp.zip"
        local download_success=false

        for i in {1..3}; do
            if curl -L "$download_url" -o "$temp_file" --retry 3 --retry-delay 5 --max-time 300; then
                download_success=true
                break
            else
                print_message $YELLOW "Download attempt $i failed, retrying..."
                sleep 2
            fi
        done

        if [ "$download_success" = true ]; then
            # Verify the downloaded file
            if [ -f "$temp_file" ] && [ -s "$temp_file" ]; then
                print_message $BLUE "📦 Extracting console assets..."

                # Extract to static directory
                if unzip -o "$temp_file" -d "$static_dir"; then
                    rm "$temp_file"
                    local final_size=$(du -sh "$static_dir" 2>/dev/null | cut -f1 || echo "unknown")
                    print_message $GREEN "✅ Console assets downloaded successfully ($final_size)"
                else
                    print_message $RED "❌ Failed to extract console assets"
                    rm -f "$temp_file"
                    return 1
                fi
            else
                print_message $RED "❌ Downloaded file is empty or invalid"
                rm -f "$temp_file"
                return 1
            fi
        else
            print_message $RED "❌ Failed to download console assets after 3 attempts"
            print_message $YELLOW "💡 Console assets are optional. Build will continue without them."
            rm -f "$temp_file"
        fi
    fi
}

# Verify binary functionality
verify_binary() {
    local binary_path="$1"

    # Check if binary exists
    if [ ! -f "$binary_path" ]; then
        print_message $RED "❌ Binary file not found: $binary_path"
        return 1
    fi

    # Check if binary is executable
    if [ ! -x "$binary_path" ]; then
        print_message $RED "❌ Binary is not executable: $binary_path"
        return 1
    fi

    # Check basic functionality - try to run help command
    print_message $YELLOW "   Testing --help command..."
    if ! "$binary_path" --help >/dev/null 2>&1; then
        print_message $RED "❌ Binary failed to run --help command"
        return 1
    fi

    # Check version command
    print_message $YELLOW "   Testing --version command..."
    if ! "$binary_path" --version >/dev/null 2>&1; then
        print_message $YELLOW "⚠️  Binary does not support --version command (this is optional)"
    fi

    # Try to get some basic info about the binary
    local file_info=$(file "$binary_path" 2>/dev/null || echo "unknown")
    print_message $YELLOW "   Binary info: $file_info"

    # Check if it's a valid ELF/Mach-O binary
    if command -v readelf >/dev/null 2>&1; then
        if readelf -h "$binary_path" >/dev/null 2>&1; then
            print_message $YELLOW "   ELF binary structure: valid"
        fi
    elif command -v otool >/dev/null 2>&1; then
        if otool -h "$binary_path" >/dev/null 2>&1; then
            print_message $YELLOW "   Mach-O binary structure: valid"
        fi
    fi

    return 0
}

# Build binary for current platform
build_binary() {
    local version=$(get_version)
    local output_file="${OUTPUT_DIR}/${PLATFORM}/${BINARY_NAME}"

    print_message $BLUE "🏗️  Building for platform: $PLATFORM"
    print_message $YELLOW "   Version: $version"
    print_message $YELLOW "   Output: $output_file"

    # Create output directory
    mkdir -p "${OUTPUT_DIR}/${PLATFORM}"

    # Simple build logic matching the working version (4fb4b353)
    # Force rebuild by touching build.rs
    touch rustfs/build.rs

    # Determine build command based on platform and cross-compilation needs
    local build_cmd=""
    local current_platform=$(detect_platform)

    print_message $BLUE "📦 Using working version build logic..."

    # Check if we need cross-compilation
    if [ "$PLATFORM" != "$current_platform" ]; then
        # Cross-compilation needed
        if [[ "$PLATFORM" == *"apple-darwin"* ]]; then
            print_message $RED "❌ macOS cross-compilation not supported"
            print_message $YELLOW "💡 macOS targets must be built natively on macOS runners"
            return 1
        elif [[ "$PLATFORM" == *"windows"* ]]; then
            # Use cross for Windows ARM64
            if ! command -v cross &> /dev/null; then
                print_message $YELLOW "📦 Installing cross tool..."
                cargo install cross --git https://github.com/cross-rs/cross
            fi
            build_cmd="cross build"
        else
            # Use zigbuild for Linux ARM64 (matches working version)
            if ! command -v cargo-zigbuild &> /dev/null; then
                print_message $RED "❌ cargo-zigbuild not found. Please install it first."
                return 1
            fi
            build_cmd="cargo zigbuild"
        fi
    else
        # Native compilation
        build_cmd="RUSTFLAGS=-Clink-arg=-lm cargo build"
    fi

    if [ "$BUILD_TYPE" = "release" ]; then
        build_cmd+=" --release"
    fi

    build_cmd+=" --target $PLATFORM"
    build_cmd+=" -p rustfs --bins"

    print_message $BLUE "📦 Executing: $build_cmd"

    # Execute build (this matches exactly what the working version does)
    if eval $build_cmd; then
        print_message $GREEN "✅ Successfully built for $PLATFORM"

        # Copy binary to output directory
        cp "target/${PLATFORM}/${BUILD_TYPE}/${BINARY_NAME}" "$output_file"

        # Generate checksums
        print_message $BLUE "🔐 Generating checksums..."
        (cd "${OUTPUT_DIR}/${PLATFORM}" && generate_sha256 "${BINARY_NAME}" "${BINARY_NAME}.sha256sum")

        # Verify binary functionality (if not skipped)
        if [ "$SKIP_VERIFICATION" = false ]; then
            print_message $BLUE "🔍 Verifying binary functionality..."
            if verify_binary "$output_file"; then
                print_message $GREEN "✅ Binary verification passed"
            else
                print_message $RED "❌ Binary verification failed"
                return 1
            fi
        else
            print_message $YELLOW "⚠️  Binary verification skipped by user request"
        fi

        # Sign binary if requested
        if [ "$SIGN" = true ]; then
            print_message $BLUE "✍️  Signing binary..."
            (cd "${OUTPUT_DIR}/${PLATFORM}" && minisign -S -m "${BINARY_NAME}" -s ~/.minisign/minisign.key)
        fi

        print_message $GREEN "✅ Build completed successfully"
    else
        print_message $RED "❌ Failed to build for $PLATFORM"
        return 1
    fi
}



# Main build function
build_rustfs() {
    local version=$(get_version)

    print_message $BLUE "🚀 Starting RustFS binary build process..."
    print_message $YELLOW "   Version: $version"
    print_message $YELLOW "   Platform: $PLATFORM"
    print_message $YELLOW "   Output Directory: $OUTPUT_DIR"
    print_message $YELLOW "   Build Type: $BUILD_TYPE"
    print_message $YELLOW "   Sign: $SIGN"
    print_message $YELLOW "   With Console: $WITH_CONSOLE"
    if [ "$WITH_CONSOLE" = true ]; then
        print_message $YELLOW "   Console Version: $CONSOLE_VERSION"
        print_message $YELLOW "   Force Console Update: $FORCE_CONSOLE_UPDATE"
    fi
    print_message $YELLOW "   Skip Verification: $SKIP_VERIFICATION"
    echo ""

    # Setup environment
    setup_rust_environment
    echo ""

    # Download console assets if requested
    download_console_assets
    echo ""

    # Build binary
    build_binary
    echo ""

    print_message $GREEN "🎉 Build process completed successfully!"

    # Show built binary
    local binary_file="${OUTPUT_DIR}/${PLATFORM}/${BINARY_NAME}"
    if [ -f "$binary_file" ]; then
        local size=$(ls -lh "$binary_file" | awk '{print $5}')
        print_message $BLUE "📋 Built binary: $binary_file ($size)"
    fi
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -o|--output-dir)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        -b|--binary-name)
            BINARY_NAME="$2"
            shift 2
            ;;
        -p|--platform)
            CUSTOM_PLATFORM="$2"
            shift 2
            ;;
        --dev)
            BUILD_TYPE="debug"
            shift
            ;;
        --sign)
            SIGN=true
            shift
            ;;
        --with-console)
            WITH_CONSOLE=true
            shift
            ;;
        --no-console)
            WITH_CONSOLE=false
            shift
            ;;
        --force-console-update)
            FORCE_CONSOLE_UPDATE=true
            WITH_CONSOLE=true  # Auto-enable download when forcing update
            shift
            ;;
        --console-version)
            CONSOLE_VERSION="$2"
            shift 2
            ;;
        --skip-verification)
            SKIP_VERIFICATION=true
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            print_message $RED "❌ Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# Main execution
main() {
    print_message $BLUE "🦀 RustFS Binary Build Script"
    echo ""

    # Check if we're in a Rust project
    if [ ! -f "Cargo.toml" ]; then
        print_message $RED "❌ No Cargo.toml found. Are you in a Rust project directory?"
        exit 1
    fi

    # Override platform if specified
    if [ -n "$CUSTOM_PLATFORM" ]; then
        PLATFORM="$CUSTOM_PLATFORM"
        print_message $YELLOW "🎯 Using specified platform: $PLATFORM"

        # Auto-enable skip verification for cross-compilation
        if [ "$PLATFORM" != "$(detect_platform)" ]; then
            SKIP_VERIFICATION=true
            print_message $YELLOW "⚠️  Cross-compilation detected, enabling --skip-verification"
        fi
    fi

    ensure_file_descriptor_limit

    # Start build process
    build_rustfs
}

# Run main function
main
