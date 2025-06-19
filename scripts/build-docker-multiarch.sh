#!/bin/bash
set -euo pipefail

# 多架构 Docker 构建脚本
# 支持构建并推送 x86_64 和 ARM64 架构的镜像

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

# 默认配置
REGISTRY_IMAGE_DOCKERHUB="rustfs/rustfs"
REGISTRY_IMAGE_GHCR="ghcr.io/rustfs/s3-rustfs"
VERSION="${VERSION:-latest}"
PUSH="${PUSH:-false}"
IMAGE_TYPE="${IMAGE_TYPE:-production}"

# 帮助信息
show_help() {
    cat << EOF
用法: $0 [选项]

选项:
    -h, --help          显示此帮助信息
    -v, --version TAG   设置镜像版本标签 (默认: latest)
    -p, --push          推送镜像到仓库
    -t, --type TYPE     镜像类型 (production|ubuntu|rockylinux|devenv, 默认: production)

环境变量:
    DOCKERHUB_USERNAME  Docker Hub 用户名
    DOCKERHUB_TOKEN     Docker Hub 访问令牌
    GITHUB_TOKEN        GitHub 访问令牌

示例:
    # 仅构建不推送
    $0 --version v1.0.0

    # 构建并推送到仓库
    $0 --version v1.0.0 --push

    # 构建 Ubuntu 版本
    $0 --type ubuntu --version v1.0.0
EOF
}

# 解析命令行参数
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            show_help
            exit 0
            ;;
        -v|--version)
            VERSION="$2"
            shift 2
            ;;
        -p|--push)
            PUSH=true
            shift
            ;;
        -t|--type)
            IMAGE_TYPE="$2"
            shift 2
            ;;
        *)
            echo "未知参数: $1"
            show_help
            exit 1
            ;;
    esac
done

# 设置 Dockerfile 和后缀
case "$IMAGE_TYPE" in
    production)
        DOCKERFILE="Dockerfile"
        SUFFIX=""
        ;;
    ubuntu)
        DOCKERFILE=".docker/Dockerfile.ubuntu22.04"
        SUFFIX="-ubuntu22.04"
        ;;
    rockylinux)
        DOCKERFILE=".docker/Dockerfile.rockylinux9.3"
        SUFFIX="-rockylinux9.3"
        ;;
    devenv)
        DOCKERFILE=".docker/Dockerfile.devenv"
        SUFFIX="-devenv"
        ;;
    *)
        echo "错误: 不支持的镜像类型: $IMAGE_TYPE"
        echo "支持的类型: production, ubuntu, rockylinux, devenv"
        exit 1
        ;;
esac

echo "🚀 开始多架构 Docker 构建"
echo "📋 构建信息:"
echo "   - 镜像类型: $IMAGE_TYPE"
echo "   - Dockerfile: $DOCKERFILE"
echo "   - 版本标签: $VERSION$SUFFIX"
echo "   - 推送: $PUSH"
echo "   - 架构: linux/amd64, linux/arm64"

# 检查必要的工具
if ! command -v docker &> /dev/null; then
    echo "❌ 错误: 未找到 docker 命令"
    exit 1
fi

# 检查 Docker Buildx
if ! docker buildx version &> /dev/null; then
    echo "❌ 错误: Docker Buildx 不可用"
    echo "请运行: docker buildx install"
    exit 1
fi

# 创建并使用 buildx 构建器
BUILDER_NAME="rustfs-multiarch-builder"
if ! docker buildx inspect "$BUILDER_NAME" &> /dev/null; then
    echo "🔨 创建多架构构建器..."
    docker buildx create --name "$BUILDER_NAME" --use --bootstrap
else
    echo "🔨 使用现有构建器..."
    docker buildx use "$BUILDER_NAME"
fi

# 构建多架构二进制文件
echo "🔧 构建多架构二进制文件..."

# 检查是否存在预构建的二进制文件
if [[ ! -f "target/x86_64-unknown-linux-musl/release/rustfs" ]] || [[ ! -f "target/aarch64-unknown-linux-gnu/release/rustfs" ]]; then
    echo "⚠️  未找到预构建的二进制文件，正在构建..."

    # 安装构建依赖
    if ! command -v cross &> /dev/null; then
        echo "📦 安装 cross 工具..."
        cargo install cross
    fi

    # 生成 protobuf 代码
    echo "📝 生成 protobuf 代码..."
    cargo run --bin gproto || true

    # 构建 x86_64
    echo "🔨 构建 x86_64 二进制文件..."
    cargo build --release --target x86_64-unknown-linux-musl --bin rustfs

    # 构建 ARM64
    echo "🔨 构建 ARM64 二进制文件..."
    cross build --release --target aarch64-unknown-linux-gnu --bin rustfs
fi

# 准备构建参数
BUILD_ARGS=""
TAGS=""

# Docker Hub 标签
if [[ -n "${DOCKERHUB_USERNAME:-}" ]]; then
    TAGS="$TAGS -t $REGISTRY_IMAGE_DOCKERHUB:$VERSION$SUFFIX"
fi

# GitHub Container Registry 标签
if [[ -n "${GITHUB_TOKEN:-}" ]]; then
    TAGS="$TAGS -t $REGISTRY_IMAGE_GHCR:$VERSION$SUFFIX"
fi

# 如果没有设置标签，使用本地标签
if [[ -z "$TAGS" ]]; then
    TAGS="-t rustfs:$VERSION$SUFFIX"
fi

# 构建镜像
echo "🏗️ 构建多架构 Docker 镜像..."
BUILD_CMD="docker buildx build \
    --platform linux/amd64,linux/arm64 \
    --file $DOCKERFILE \
    $TAGS \
    --build-arg BUILDTIME=$(date -u +%Y-%m-%dT%H:%M:%SZ) \
    --build-arg VERSION=$VERSION \
    --build-arg REVISION=$(git rev-parse --short HEAD 2>/dev/null || echo 'unknown')"

if [[ "$PUSH" == "true" ]]; then
    # 登录到仓库
    if [[ -n "${DOCKERHUB_USERNAME:-}" ]] && [[ -n "${DOCKERHUB_TOKEN:-}" ]]; then
        echo "🔐 登录到 Docker Hub..."
        echo "$DOCKERHUB_TOKEN" | docker login --username "$DOCKERHUB_USERNAME" --password-stdin
    fi

    if [[ -n "${GITHUB_TOKEN:-}" ]]; then
        echo "🔐 登录到 GitHub Container Registry..."
        echo "$GITHUB_TOKEN" | docker login ghcr.io --username "$(whoami)" --password-stdin
    fi

    BUILD_CMD="$BUILD_CMD --push"
else
    BUILD_CMD="$BUILD_CMD --load"
fi

BUILD_CMD="$BUILD_CMD ."

echo "📋 执行构建命令:"
echo "$BUILD_CMD"
echo ""

# 执行构建
eval "$BUILD_CMD"

echo ""
echo "✅ 多架构 Docker 镜像构建完成!"

if [[ "$PUSH" == "true" ]]; then
    echo "🚀 镜像已推送到仓库"

    # 显示推送的镜像信息
    echo ""
    echo "📦 推送的镜像:"
    if [[ -n "${DOCKERHUB_USERNAME:-}" ]]; then
        echo "   - $REGISTRY_IMAGE_DOCKERHUB:$VERSION$SUFFIX"
    fi
    if [[ -n "${GITHUB_TOKEN:-}" ]]; then
        echo "   - $REGISTRY_IMAGE_GHCR:$VERSION$SUFFIX"
    fi

    echo ""
    echo "🔍 验证多架构支持:"
    if [[ -n "${DOCKERHUB_USERNAME:-}" ]]; then
        echo "   docker buildx imagetools inspect $REGISTRY_IMAGE_DOCKERHUB:$VERSION$SUFFIX"
    fi
    if [[ -n "${GITHUB_TOKEN:-}" ]]; then
        echo "   docker buildx imagetools inspect $REGISTRY_IMAGE_GHCR:$VERSION$SUFFIX"
    fi
else
    echo "💾 镜像已构建到本地"
    echo ""
    echo "🔍 查看镜像:"
    echo "   docker images rustfs:$VERSION$SUFFIX"
fi

echo ""
echo "🎉 构建任务完成!"
