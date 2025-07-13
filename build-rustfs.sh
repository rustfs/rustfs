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

# Auto-detect current platform
detect_platform() {
    local arch=$(uname -m)
    local os=$(uname -s | tr '[:upper:]' '[:lower:]')

    case "$os" in
        "linux")
            case "$arch" in
                "x86_64")
                    echo "x86_64-unknown-linux-musl"
                    ;;
                "aarch64"|"arm64")
                    echo "aarch64-unknown-linux-musl"
                    ;;
                "armv7l")
                    echo "armv7-unknown-linux-musleabihf"
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

# Default values
OUTPUT_DIR="target/release"
PLATFORM=$(detect_platform)  # Auto-detect current platform
BINARY_NAME="rustfs"
BUILD_TYPE="release"
UPLOAD=false
SIGN=false
DOWNLOAD_CONSOLE=false
FORCE_CONSOLE_UPDATE=false
CONSOLE_VERSION="latest"

# Print usage
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Description:"
    echo "  Build RustFS binary for the current platform. Designed for CI/CD pipelines"
    echo "  where different runners build platform-specific binaries natively."
    echo ""
    echo "Options:"
    echo "  -o, --output-dir DIR       Output directory (default: target/release)"
    echo "  -b, --binary-name NAME     Binary name (default: rustfs)"
    echo "  --dev                      Build in dev mode"
    echo "  --upload                   Upload binaries after build"
    echo "  --sign                     Sign binaries after build"
    echo "  --download-console         Download console static assets"
    echo "  --force-console-update     Force update console assets even if they exist"
    echo "  --console-version VERSION  Console version to download (default: latest)"
    echo "  -h, --help                 Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                         # Build for current platform (typical CI usage)"
    echo "  $0 --dev                   # Development build"
    echo "  $0 --upload --sign         # Build, sign and upload (release CI)"
    echo "  $0 --download-console      # Build with console static assets"
    echo "  $0 --force-console-update  # Force update console assets"
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
    if [ "$DOWNLOAD_CONSOLE" = true ]; then
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

# Build binary for current platform
build_binary() {
    local version=$(get_version)
    local output_file="${OUTPUT_DIR}/${PLATFORM}/${BINARY_NAME}"

    print_message $BLUE "🏗️  Building for platform: $PLATFORM"
    print_message $YELLOW "   Version: $version"
    print_message $YELLOW "   Output: $output_file"

    # Create output directory
    mkdir -p "${OUTPUT_DIR}/${PLATFORM}"

    # Build command - always use native cargo for current platform
    local build_cmd="cargo build"

    if [ "$BUILD_TYPE" = "release" ]; then
        build_cmd+=" --release"
    fi

    build_cmd+=" --target $PLATFORM"
    build_cmd+=" --bin $BINARY_NAME"

    print_message $BLUE "📦 Executing: $build_cmd"

    # Execute build
    if eval $build_cmd; then
        print_message $GREEN "✅ Successfully built for $PLATFORM"

        # Copy binary to output directory
        cp "target/${PLATFORM}/${BUILD_TYPE}/${BINARY_NAME}" "$output_file"

        # Generate checksums
        print_message $BLUE "🔐 Generating checksums..."
        (cd "${OUTPUT_DIR}/${PLATFORM}" && sha256sum "${BINARY_NAME}" > "${BINARY_NAME}.sha256sum")

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

# Upload binary
upload_binary() {
    local version=$(get_version)
    local binary_dir="${OUTPUT_DIR}/${PLATFORM}"

    print_message $BLUE "📤 Uploading binary for $PLATFORM..."

    if [ -f "${binary_dir}/${BINARY_NAME}" ]; then
        print_message $YELLOW "Uploading $PLATFORM binary..."

        # Example upload command - customize based on your storage
        # aws s3 cp "${binary_dir}/${BINARY_NAME}" "s3://dl.rustfs.com/release/rustfs.${version}"
        # aws s3 cp "${binary_dir}/${BINARY_NAME}.sha256sum" "s3://dl.rustfs.com/release/rustfs.${version}.sha256sum"

        # For now, just show what would be uploaded
        print_message $BLUE "Would upload: ${binary_dir}/${BINARY_NAME} -> dl.rustfs.com/artifacts/rustfs/release/rustfs.${version}"
        print_message $BLUE "Would upload: ${binary_dir}/${BINARY_NAME}.sha256sum -> dl.rustfs.com/artifacts/rustfs/release/rustfs.${version}.sha256sum"

        if [ "$SIGN" = true ] && [ -f "${binary_dir}/${BINARY_NAME}.minisig" ]; then
            print_message $BLUE "Would upload: ${binary_dir}/${BINARY_NAME}.minisig -> dl.rustfs.com/artifacts/rustfs/release/rustfs.${version}.minisig"
        fi
    else
        print_message $RED "❌ Binary not found: ${binary_dir}/${BINARY_NAME}"
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
    print_message $YELLOW "   Upload: $UPLOAD"
    print_message $YELLOW "   Download Console: $DOWNLOAD_CONSOLE"
    if [ "$DOWNLOAD_CONSOLE" = true ]; then
        print_message $YELLOW "   Console Version: $CONSOLE_VERSION"
        print_message $YELLOW "   Force Console Update: $FORCE_CONSOLE_UPDATE"
    fi
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

    # Upload if requested
    if [ "$UPLOAD" = true ]; then
        upload_binary
    fi

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
        --dev)
            BUILD_TYPE="dev"
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
        --download-console)
            DOWNLOAD_CONSOLE=true
            shift
            ;;
        --force-console-update)
            FORCE_CONSOLE_UPDATE=true
            DOWNLOAD_CONSOLE=true  # Auto-enable download when forcing update
            shift
            ;;
        --console-version)
            CONSOLE_VERSION="$2"
            shift 2
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
    build_rustfs
}

# Run main function
main
