#!/bin/bash

# RustFS Binary Build Script
# This script compiles RustFS binaries for different platforms and architectures

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
OUTPUT_DIR="target/release"
PLATFORMS=("x86_64-unknown-linux-musl" "aarch64-unknown-linux-musl")
BINARY_NAME="rustfs"
BUILD_TYPE="release"
CROSS_COMPILE=false
UPLOAD=false
SIGN=false

# Print usage
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -o, --output-dir DIR       Output directory (default: target/release)"
    echo "  -p, --platform PLATFORM   Target platform (default: x86_64-unknown-linux-musl,aarch64-unknown-linux-musl)"
    echo "  -b, --binary-name NAME     Binary name (default: rustfs)"
    echo "  --dev                      Build in dev mode"
    echo "  --cross                    Use cross compilation"
    echo "  --upload                   Upload binaries after build"
    echo "  --sign                     Sign binaries after build"
    echo "  -h, --help                 Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                         # Build for default platforms"
    echo "  $0 --cross                 # Build using cross compilation"
    echo "  $0 --upload --sign         # Build, sign and upload binaries"
    echo "  $0 -p x86_64-unknown-linux-musl  # Build for specific platform"
}

# Print colored message
print_message() {
    local color=$1
    local message=$2
    echo -e "${color}${message}${NC}"
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

    # Install required targets
    for platform in "${PLATFORMS[@]}"; do
        print_message $YELLOW "Installing target: $platform"
        rustup target add "$platform"
    done

    # Install cross if needed
    if [ "$CROSS_COMPILE" = true ]; then
        if ! command -v cross &> /dev/null; then
            print_message $YELLOW "Installing cross compilation tool..."
            cargo install cross
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

# Build for specific platform
build_for_platform() {
    local platform=$1
    local version=$(get_version)
    local output_file="${OUTPUT_DIR}/${platform}/${BINARY_NAME}"

    print_message $BLUE "🏗️  Building for platform: $platform"
    print_message $YELLOW "   Version: $version"
    print_message $YELLOW "   Output: $output_file"

    # Create output directory
    mkdir -p "${OUTPUT_DIR}/${platform}"

    # Build command
    local build_cmd=""
    if [ "$CROSS_COMPILE" = true ]; then
        build_cmd="cross build"
    else
        build_cmd="cargo build"
    fi

    if [ "$BUILD_TYPE" = "release" ]; then
        build_cmd+=" --release"
    fi

    build_cmd+=" --target $platform"
    build_cmd+=" --bin $BINARY_NAME"

    print_message $BLUE "📦 Executing: $build_cmd"

    # Execute build
    if eval $build_cmd; then
        print_message $GREEN "✅ Successfully built for $platform"

        # Copy binary to output directory
        cp "target/${platform}/${BUILD_TYPE}/${BINARY_NAME}" "$output_file"

        # Generate checksums
        print_message $BLUE "🔐 Generating checksums..."
        (cd "${OUTPUT_DIR}/${platform}" && sha256sum "${BINARY_NAME}" > "${BINARY_NAME}.sha256sum")

        # Sign binary if requested
        if [ "$SIGN" = true ]; then
            print_message $BLUE "✍️  Signing binary..."
            (cd "${OUTPUT_DIR}/${platform}" && minisign -S -m "${BINARY_NAME}" -s ~/.minisign/minisign.key)
        fi

        print_message $GREEN "✅ Platform $platform completed successfully"
    else
        print_message $RED "❌ Failed to build for $platform"
        return 1
    fi
}

# Upload binaries
upload_binaries() {
    local version=$(get_version)

    print_message $BLUE "📤 Uploading binaries..."

    for platform in "${PLATFORMS[@]}"; do
        local binary_dir="${OUTPUT_DIR}/${platform}"

        if [ -f "${binary_dir}/${BINARY_NAME}" ]; then
            print_message $YELLOW "Uploading $platform binaries..."

            # Example upload command - customize based on your storage
            # aws s3 cp "${binary_dir}/${BINARY_NAME}" "s3://releases.rustfs.com/server/rustfs/release/${platform}/archive/rustfs.${version}"
            # aws s3 cp "${binary_dir}/${BINARY_NAME}.sha256sum" "s3://releases.rustfs.com/server/rustfs/release/${platform}/archive/rustfs.${version}.sha256sum"

            # For now, just show what would be uploaded
            print_message $BLUE "Would upload: ${binary_dir}/${BINARY_NAME} -> releases.rustfs.com/server/rustfs/release/${platform}/archive/rustfs.${version}"
            print_message $BLUE "Would upload: ${binary_dir}/${BINARY_NAME}.sha256sum -> releases.rustfs.com/server/rustfs/release/${platform}/archive/rustfs.${version}.sha256sum"

            if [ "$SIGN" = true ] && [ -f "${binary_dir}/${BINARY_NAME}.minisig" ]; then
                print_message $BLUE "Would upload: ${binary_dir}/${BINARY_NAME}.minisig -> releases.rustfs.com/server/rustfs/release/${platform}/archive/rustfs.${version}.minisig"
            fi
        fi
    done
}

# Main build function
build_binaries() {
    local version=$(get_version)

    print_message $BLUE "🚀 Starting RustFS binary build process..."
    print_message $YELLOW "   Version: $version"
    print_message $YELLOW "   Platforms: ${PLATFORMS[*]}"
    print_message $YELLOW "   Output Directory: $OUTPUT_DIR"
    print_message $YELLOW "   Build Type: $BUILD_TYPE"
    print_message $YELLOW "   Cross Compile: $CROSS_COMPILE"
    print_message $YELLOW "   Sign: $SIGN"
    print_message $YELLOW "   Upload: $UPLOAD"
    echo ""

    # Setup environment
    setup_rust_environment
    echo ""

    # Build for each platform
    for platform in "${PLATFORMS[@]}"; do
        build_for_platform "$platform"
        echo ""
    done

    # Upload if requested
    if [ "$UPLOAD" = true ]; then
        upload_binaries
    fi

    print_message $GREEN "🎉 Build process completed successfully!"

    # Show built binaries
    print_message $BLUE "📋 Built binaries:"
    for platform in "${PLATFORMS[@]}"; do
        local binary_file="${OUTPUT_DIR}/${platform}/${BINARY_NAME}"
        if [ -f "$binary_file" ]; then
            local size=$(ls -lh "$binary_file" | awk '{print $5}')
            print_message $YELLOW "   $platform: $binary_file ($size)"
        fi
    done
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -o|--output-dir)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        -p|--platform)
            # Parse comma-separated platforms
            IFS=',' read -ra PLATFORMS <<< "$2"
            shift 2
            ;;
        -b|--binary-name)
            BINARY_NAME="$2"
            shift 2
            ;;
        --dev)
            BUILD_TYPE="dev"
            shift
            ;;
        --cross)
            CROSS_COMPILE=true
            shift
            ;;
        --upload)
            UPLOAD=true
            shift
            ;;
        --sign)
            SIGN=true
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

    # Start build process
    build_binaries
}

# Run main function
main
