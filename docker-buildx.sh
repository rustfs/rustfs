#!/usr/bin/env bash

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
REGISTRY="ghcr.io"
NAMESPACE="rustfs"
PLATFORMS="linux/amd64,linux/arm64"
PUSH=false
NO_CACHE=false
RELEASE=""
CHANNEL="release"

# Print usage
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -r, --registry REGISTRY    Docker registry (default: ghcr.io)"
    echo "  -n, --namespace NAMESPACE  Image namespace (default: rustfs)"
    echo "  -p, --platforms PLATFORMS  Target platforms (default: linux/amd64,linux/arm64)"
    echo "  --push                     Push images to registry"
    echo "  --no-cache                 Disable build cache"
    echo "  --release VERSION          Specify release version (default: auto-detect from git)"
    echo "  --channel CHANNEL          Download channel: release or dev (default: release)"
    echo "  -h, --help                 Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                         # Build all variants locally"
    echo "  $0 --push                  # Build and push all variants"
    echo "  $0 --push --no-cache       # Build and push with no cache"
    echo "  $0 --release v1.0.0        # Build specific release version"
    echo "  $0 --channel dev           # Build with dev channel binaries"
    echo "  $0 --release latest --channel dev  # Build latest dev build"
}

# Print colored message
print_message() {
    local color=$1
    local message=$2
    echo -e "${color}${message}${NC}"
}

# Check if Docker buildx is available
check_buildx() {
    if ! docker buildx version >/dev/null 2>&1; then
        print_message $RED "❌ Docker buildx is not available. Please install Docker with buildx support."
        exit 1
    fi
}

# Setup buildx builder
setup_builder() {
    local builder_name="rustfs-builder"

    print_message $BLUE "🔧 Setting up Docker buildx builder..."

    # Check if builder exists
    if docker buildx ls | grep -q "$builder_name"; then
        print_message $YELLOW "⚠️  Builder '$builder_name' already exists, using existing one"
        docker buildx use "$builder_name"
    else
        # Create new builder
        docker buildx create --name "$builder_name" --driver docker-container --bootstrap
        docker buildx use "$builder_name"
        print_message $GREEN "✅ Created and activated builder '$builder_name'"
    fi

    # Inspect builder
    docker buildx inspect --bootstrap
}

# Get version from git
get_version() {
    if [ -n "$RELEASE" ]; then
        echo "$RELEASE"
        return
    fi

    # Try to get version from git tag
    if git describe --abbrev=0 --tags >/dev/null 2>&1; then
        git describe --abbrev=0 --tags
    else
        # Fallback to commit hash
        git rev-parse --short HEAD
    fi
}

# Build and push images
build_and_push() {
    local version=$(get_version)
    local image_base="${REGISTRY}/${NAMESPACE}/rustfs"
    local build_date=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    local vcs_ref=$(git rev-parse --short HEAD)

    print_message $BLUE "🚀 Building RustFS Docker images..."
    print_message $YELLOW "   Version: $version"
    print_message $YELLOW "   Registry: $REGISTRY"
    print_message $YELLOW "   Namespace: $NAMESPACE"
    print_message $YELLOW "   Platforms: $PLATFORMS"
    print_message $YELLOW "   Channel: $CHANNEL"
    print_message $YELLOW "   Build Date: $build_date"
    print_message $YELLOW "   VCS Ref: $vcs_ref"
    print_message $YELLOW "   Push: $PUSH"
    print_message $YELLOW "   No Cache: $NO_CACHE"
    echo ""

    # Build command base
    local build_cmd="docker buildx build"
    build_cmd+=" --platform $PLATFORMS"
    build_cmd+=" --build-arg RELEASE=$version"
    build_cmd+=" --build-arg CHANNEL=$CHANNEL"
    build_cmd+=" --build-arg BUILD_DATE=$build_date"
    build_cmd+=" --build-arg VCS_REF=$vcs_ref"

    if [ "$NO_CACHE" = true ]; then
        build_cmd+=" --no-cache"
    fi

    if [ "$PUSH" = true ]; then
        build_cmd+=" --push"
    else
        build_cmd+=" --load"
    fi

    # Build latest variant
    print_message $BLUE "🏗️  Building latest variant..."
    local latest_cmd="$build_cmd"

    # Add channel-specific tags
    if [ "$CHANNEL" = "dev" ]; then
        latest_cmd+=" -t ${image_base}:dev-latest"
    else
        latest_cmd+=" -t ${image_base}:latest"
    fi

    latest_cmd+=" --build-arg RELEASE=latest"
    latest_cmd+=" -f Dockerfile ."

    print_message $BLUE "📦 Executing: $latest_cmd"
    if eval $latest_cmd; then
        print_message $GREEN "✅ Successfully built latest variant"
    else
        print_message $RED "❌ Failed to build latest variant"
        print_message $YELLOW "💡 Note: Make sure rustfs binaries are available at:"
        print_message $YELLOW "   https://github.com/rustfs/rustfs/releases"
        exit 1
    fi

    # Prune build cache
    docker buildx prune -f

    # Build release variant (only if not latest)
    if [ "$RELEASE" != "latest" ]; then
        print_message $BLUE "🏗️  Building release variant..."
        local release_cmd="$build_cmd"
        release_cmd+=" -t ${image_base}:${version}"

        # Add channel-specific tags
        if [ "$CHANNEL" = "dev" ]; then
            release_cmd+=" -t ${image_base}:dev-${version}"
        else
            release_cmd+=" -t ${image_base}:release"
        fi

        release_cmd+=" --build-arg RELEASE=${version}"
        release_cmd+=" -f Dockerfile ."

        print_message $BLUE "📦 Executing: $release_cmd"
        if eval $release_cmd; then
            print_message $GREEN "✅ Successfully built release variant"
        else
            print_message $RED "❌ Failed to build release variant"
            print_message $YELLOW "💡 Note: Make sure rustfs binaries are available at:"
            print_message $YELLOW "   https://github.com/rustfs/rustfs/releases"
            exit 1
        fi
    else
        print_message $BLUE "⏭️  Skipping release variant (already built as latest)"
    fi

    # Final cleanup
    docker buildx prune -f
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -r|--registry)
            REGISTRY="$2"
            shift 2
            ;;
        -n|--namespace)
            NAMESPACE="$2"
            shift 2
            ;;
        -p|--platforms)
            PLATFORMS="$2"
            shift 2
            ;;
        --push)
            PUSH=true
            shift
            ;;
        --no-cache)
            NO_CACHE=true
            shift
            ;;
        --release)
            RELEASE="$2"
            shift 2
            ;;
        --channel)
            CHANNEL="$2"
            if [ "$CHANNEL" != "release" ] && [ "$CHANNEL" != "dev" ]; then
                print_message $RED "❌ Invalid channel: $CHANNEL. Must be 'release' or 'dev'"
                exit 1
            fi
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
    print_message $BLUE "🐳 RustFS Docker Buildx Build Script"
    print_message $YELLOW "📋 Build Strategy: Uses pre-built binaries from GitHub Releases"
    print_message $YELLOW "🚀 Production images only - optimized for distribution"
    echo ""

    # Check prerequisites
    check_buildx

    # Setup builder
    setup_builder
    echo ""

    # Start build process
    build_and_push

    print_message $GREEN "🎉 Build process completed successfully!"

    # Show built images if not pushing
    if [ "$PUSH" = false ]; then
        print_message $BLUE "📋 Built images:"
        docker images | grep "${NAMESPACE}/rustfs" | head -10
    fi
}

# Run main function
main
