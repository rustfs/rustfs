###########
# 远程开发，需要 VSCode 安装 Dev Containers, Remote SSH, Remote Explorer
# https://code.visualstudio.com/docs/remote/containers
###########
DOCKER_CLI ?= docker
IMAGE_NAME ?= rustfs:v1.0.0
CONTAINER_NAME ?= rustfs-dev
# Docker build configurations
DOCKERFILE_PRODUCTION = Dockerfile
DOCKERFILE_SOURCE = Dockerfile.source

# Code quality and formatting targets
.PHONY: fmt
fmt:
	@echo "🔧 Formatting code..."
	cargo fmt --all

.PHONY: fmt-check
fmt-check:
	@echo "📝 Checking code formatting..."
	cargo fmt --all --check

.PHONY: clippy
clippy:
	@echo "🔍 Running clippy checks..."
	cargo clippy --fix --allow-dirty 
	cargo clippy --all-targets --all-features -- -D warnings

.PHONY: check
check:
	@echo "🔨 Running compilation check..."
	cargo check --all-targets

.PHONY: test
test:
	@echo "🧪 Running tests..."
	cargo nextest run --all --exclude e2e_test
	cargo test --all --doc

.PHONY: pre-commit
pre-commit: fmt clippy check test
	@echo "✅ All pre-commit checks passed!"

.PHONY: setup-hooks
setup-hooks:
	@echo "🔧 Setting up git hooks..."
	chmod +x .git/hooks/pre-commit
	@echo "✅ Git hooks setup complete!"

.PHONY: e2e-server
e2e-server:
	sh $(shell pwd)/scripts/run.sh

.PHONY: probe-e2e
probe-e2e:
	sh $(shell pwd)/scripts/probe.sh

# Native build using build-rustfs.sh script
.PHONY: build
build:
	@echo "🔨 Building RustFS using build-rustfs.sh script..."
	./build-rustfs.sh

.PHONY: build-dev
build-dev:
	@echo "🔨 Building RustFS in development mode..."
	./build-rustfs.sh --dev

# Docker-based build (alternative approach)
# Usage: make BUILD_OS=ubuntu22.04 build-docker
# Output: target/ubuntu22.04/release/rustfs
BUILD_OS ?= rockylinux9.3
.PHONY: build-docker
build-docker: SOURCE_BUILD_IMAGE_NAME = rustfs-$(BUILD_OS):v1
build-docker: SOURCE_BUILD_CONTAINER_NAME = rustfs-$(BUILD_OS)-build
build-docker: BUILD_CMD = /root/.cargo/bin/cargo build --release --bin rustfs --target-dir /root/s3-rustfs/target/$(BUILD_OS)
build-docker:
	@echo "🐳 Building RustFS using Docker ($(BUILD_OS))..."
	$(DOCKER_CLI) build -t $(SOURCE_BUILD_IMAGE_NAME) -f $(DOCKERFILE_SOURCE) .
	$(DOCKER_CLI) run --rm --name $(SOURCE_BUILD_CONTAINER_NAME) -v $(shell pwd):/root/s3-rustfs -it $(SOURCE_BUILD_IMAGE_NAME) $(BUILD_CMD)

.PHONY: build-musl
build-musl:
	@echo "🔨 Building rustfs for x86_64-unknown-linux-musl..."
	@echo "💡 On macOS/Windows, use 'make build-docker' or 'make docker-dev' instead"
	./build-rustfs.sh --platform x86_64-unknown-linux-musl

.PHONY: build-gnu
build-gnu:
	@echo "🔨 Building rustfs for x86_64-unknown-linux-gnu..."
	@echo "💡 On macOS/Windows, use 'make build-docker' or 'make docker-dev' instead"
	./build-rustfs.sh --platform x86_64-unknown-linux-gnu

.PHONY: build-musl-arm64
build-musl-arm64:
	@echo "🔨 Building rustfs for aarch64-unknown-linux-musl..."
	@echo "💡 On macOS/Windows, use 'make build-docker' or 'make docker-dev' instead"
	./build-rustfs.sh --platform aarch64-unknown-linux-musl

.PHONY: build-gnu-arm64
build-gnu-arm64:
	@echo "🔨 Building rustfs for aarch64-unknown-linux-gnu..."
	@echo "💡 On macOS/Windows, use 'make build-docker' or 'make docker-dev' instead"
	./build-rustfs.sh --platform aarch64-unknown-linux-gnu

.PHONY: deploy-dev
deploy-dev: build-musl
	@echo "🚀 Deploying to dev server: $${IP}"
	./scripts/dev_deploy.sh $${IP}

# ========================================================================================
# Docker Multi-Architecture Builds (Primary Methods)
# ========================================================================================

# Production builds using docker-buildx.sh (for CI/CD and production)
.PHONY: docker-buildx
docker-buildx:
	@echo "🏗️ Building multi-architecture production Docker images with buildx..."
	./docker-buildx.sh

.PHONY: docker-buildx-push
docker-buildx-push:
	@echo "🚀 Building and pushing multi-architecture production Docker images with buildx..."
	./docker-buildx.sh --push

.PHONY: docker-buildx-version
docker-buildx-version:
	@if [ -z "$(VERSION)" ]; then \
		echo "❌ 错误: 请指定版本, 例如: make docker-buildx-version VERSION=v1.0.0"; \
		exit 1; \
	fi
	@echo "🏗️ Building multi-architecture production Docker images (version: $(VERSION))..."
	./docker-buildx.sh --release $(VERSION)

.PHONY: docker-buildx-push-version
docker-buildx-push-version:
	@if [ -z "$(VERSION)" ]; then \
		echo "❌ 错误: 请指定版本, 例如: make docker-buildx-push-version VERSION=v1.0.0"; \
		exit 1; \
	fi
	@echo "🚀 Building and pushing multi-architecture production Docker images (version: $(VERSION))..."
	./docker-buildx.sh --release $(VERSION) --push

# Development/Source builds using direct buildx commands
.PHONY: docker-dev
docker-dev:
	@echo "🏗️ Building multi-architecture development Docker images with buildx..."
	@echo "💡 This builds from source code and is intended for local development and testing"
	@echo "⚠️  Multi-arch images cannot be loaded locally, use docker-dev-push to push to registry"
	$(DOCKER_CLI) buildx build \
		--platform linux/amd64,linux/arm64 \
		--file $(DOCKERFILE_SOURCE) \
		--tag rustfs:source-latest \
		--tag rustfs:dev-latest \
		.

.PHONY: docker-dev-local
docker-dev-local:
	@echo "🏗️ Building single-architecture development Docker image for local use..."
	@echo "💡 This builds from source code for the current platform and loads locally"
	$(DOCKER_CLI) buildx build \
		--file $(DOCKERFILE_SOURCE) \
		--tag rustfs:source-latest \
		--tag rustfs:dev-latest \
		--load \
		.

.PHONY: docker-dev-push
docker-dev-push:
	@if [ -z "$(REGISTRY)" ]; then \
		echo "❌ 错误: 请指定镜像仓库, 例如: make docker-dev-push REGISTRY=ghcr.io/username"; \
		exit 1; \
	fi
	@echo "🚀 Building and pushing multi-architecture development Docker images..."
	@echo "💡 推送到仓库: $(REGISTRY)"
	$(DOCKER_CLI) buildx build \
		--platform linux/amd64,linux/arm64 \
		--file $(DOCKERFILE_SOURCE) \
		--tag $(REGISTRY)/rustfs:source-latest \
		--tag $(REGISTRY)/rustfs:dev-latest \
		--push \
		.



# Local production builds using direct buildx (alternative to docker-buildx.sh)
.PHONY: docker-buildx-production-local
docker-buildx-production-local:
	@echo "🏗️ Building single-architecture production Docker image locally..."
	@echo "💡 Alternative to docker-buildx.sh for local testing"
	$(DOCKER_CLI) buildx build \
		--file $(DOCKERFILE_PRODUCTION) \
		--tag rustfs:production-latest \
		--tag rustfs:latest \
		--load \
		--build-arg RELEASE=latest \
		.

# ========================================================================================
# Single Architecture Docker Builds (Traditional)
# ========================================================================================

.PHONY: docker-build-production
docker-build-production:
	@echo "🏗️ Building single-architecture production Docker image..."
	@echo "💡 Consider using 'make docker-buildx-production-local' for multi-arch support"
	$(DOCKER_CLI) build -f $(DOCKERFILE_PRODUCTION) -t rustfs:latest .

.PHONY: docker-build-source
docker-build-source:
	@echo "🏗️ Building single-architecture source Docker image..."
	@echo "💡 Consider using 'make docker-dev-local' for multi-arch support"
	$(DOCKER_CLI) build -f $(DOCKERFILE_SOURCE) -t rustfs:source .

# ========================================================================================
# Development Environment
# ========================================================================================

.PHONY: dev-env-start
dev-env-start:
	@echo "🚀 Starting development environment..."
	$(DOCKER_CLI) buildx build \
		--file $(DOCKERFILE_SOURCE) \
		--tag rustfs:dev \
		--load \
		.
	$(DOCKER_CLI) stop $(CONTAINER_NAME) 2>/dev/null || true
	$(DOCKER_CLI) rm $(CONTAINER_NAME) 2>/dev/null || true
	$(DOCKER_CLI) run -d --name $(CONTAINER_NAME) \
		-p 9010:9010 -p 9000:9000 \
		-v $(shell pwd):/workspace \
		-it rustfs:dev

.PHONY: dev-env-stop
dev-env-stop:
	@echo "🛑 Stopping development environment..."
	$(DOCKER_CLI) stop $(CONTAINER_NAME) 2>/dev/null || true
	$(DOCKER_CLI) rm $(CONTAINER_NAME) 2>/dev/null || true

.PHONY: dev-env-restart
dev-env-restart: dev-env-stop dev-env-start



# ========================================================================================
# Build Utilities
# ========================================================================================

.PHONY: docker-inspect-multiarch
docker-inspect-multiarch:
	@if [ -z "$(IMAGE)" ]; then \
		echo "❌ 错误: 请指定镜像, 例如: make docker-inspect-multiarch IMAGE=rustfs/rustfs:latest"; \
		exit 1; \
	fi
	@echo "🔍 Inspecting multi-architecture image: $(IMAGE)"
	docker buildx imagetools inspect $(IMAGE)

.PHONY: build-cross-all
build-cross-all:
	@echo "🔧 Building all target architectures..."
	@echo "💡 On macOS/Windows, use 'make docker-dev' for reliable multi-arch builds"
	@echo "🔨 Generating protobuf code..."
	cargo run --bin gproto || true
	@echo "🔨 Building x86_64-unknown-linux-gnu..."
	./build-rustfs.sh --platform x86_64-unknown-linux-gnu
	@echo "🔨 Building aarch64-unknown-linux-gnu..."
	./build-rustfs.sh --platform aarch64-unknown-linux-gnu
	@echo "🔨 Building x86_64-unknown-linux-musl..."
	./build-rustfs.sh --platform x86_64-unknown-linux-musl
	@echo "🔨 Building aarch64-unknown-linux-musl..."
	./build-rustfs.sh --platform aarch64-unknown-linux-musl
	@echo "✅ All architectures built successfully!"

# ========================================================================================
# Help and Documentation
# ========================================================================================

.PHONY: help-build
help-build:
	@echo "🔨 RustFS 构建帮助："
	@echo ""
	@echo "🚀 本地构建 (推荐使用):"
	@echo "  make build                               # 构建 RustFS 二进制文件 (默认包含 console)"
	@echo "  make build-dev                           # 开发模式构建"
	@echo "  make build-musl                          # 构建 x86_64 musl 版本"
	@echo "  make build-gnu                           # 构建 x86_64 GNU 版本"
	@echo "  make build-musl-arm64                    # 构建 aarch64 musl 版本"
	@echo "  make build-gnu-arm64                     # 构建 aarch64 GNU 版本"
	@echo ""
	@echo "🐳 Docker 构建:"
	@echo "  make build-docker                        # 使用 Docker 容器构建"
	@echo "  make build-docker BUILD_OS=ubuntu22.04   # 指定构建系统"
	@echo ""
	@echo "🏗️ 跨架构构建:"
	@echo "  make build-cross-all                     # 构建所有架构的二进制文件"
	@echo ""
	@echo "🔧 直接使用 build-rustfs.sh 脚本:"
	@echo "  ./build-rustfs.sh --help                 # 查看脚本帮助"
	@echo "  ./build-rustfs.sh --no-console           # 构建时跳过 console 资源"
	@echo "  ./build-rustfs.sh --force-console-update # 强制更新 console 资源"
	@echo "  ./build-rustfs.sh --dev                  # 开发模式构建"
	@echo "  ./build-rustfs.sh --sign                 # 签名二进制文件"
	@echo "  ./build-rustfs.sh --platform x86_64-unknown-linux-gnu   # 指定目标平台"
	@echo "  ./build-rustfs.sh --skip-verification    # 跳过二进制验证"
	@echo ""
	@echo "💡 build-rustfs.sh 脚本提供了更多选项、智能检测和二进制验证功能"

.PHONY: help-docker
help-docker:
	@echo "🐳 Docker 多架构构建帮助："
	@echo ""
	@echo "🚀 生产镜像构建 (推荐使用 docker-buildx.sh):"
	@echo "  make docker-buildx                       # 构建生产多架构镜像（不推送）"
	@echo "  make docker-buildx-push                  # 构建并推送生产多架构镜像"
	@echo "  make docker-buildx-version VERSION=v1.0.0        # 构建指定版本"
	@echo "  make docker-buildx-push-version VERSION=v1.0.0   # 构建并推送指定版本"
	@echo ""
	@echo "🔧 开发/源码镜像构建 (本地开发测试):"
	@echo "  make docker-dev                          # 构建开发多架构镜像（无法本地加载）"
	@echo "  make docker-dev-local                    # 构建开发单架构镜像（本地加载）"
	@echo "  make docker-dev-push REGISTRY=xxx       # 构建并推送开发镜像"
	@echo ""
	@echo "🏗️ 本地生产镜像构建 (替代方案):"
	@echo "  make docker-buildx-production-local      # 本地构建生产单架构镜像"
	@echo ""
	@echo "📦 单架构构建 (传统方式):"
	@echo "  make docker-build-production             # 构建单架构生产镜像"
	@echo "  make docker-build-source                 # 构建单架构源码镜像"
	@echo ""
	@echo "🚀 开发环境管理:"
	@echo "  make dev-env-start                       # 启动开发容器环境"
	@echo "  make dev-env-stop                        # 停止开发容器环境"
	@echo "  make dev-env-restart                     # 重启开发容器环境"
	@echo ""
	@echo "🔧 辅助工具:"
	@echo "  make build-cross-all                     # 构建所有架构的二进制文件"
	@echo "  make docker-inspect-multiarch IMAGE=xxx  # 检查镜像的架构支持"
	@echo ""
	@echo "📋 环境变量:"
	@echo "  REGISTRY          镜像仓库地址 (推送时需要)"
	@echo "  DOCKERHUB_USERNAME    Docker Hub 用户名"
	@echo "  DOCKERHUB_TOKEN       Docker Hub 访问令牌"
	@echo "  GITHUB_TOKEN          GitHub 访问令牌"
	@echo ""
	@echo "💡 建议："
	@echo "  - 生产用途: 使用 docker-buildx* 命令 (基于预编译二进制)"
	@echo "  - 本地开发: 使用 docker-dev* 命令 (从源码构建)"
	@echo "  - 开发环境: 使用 dev-env-* 命令管理开发容器"

.PHONY: help
help:
	@echo "🦀 RustFS Makefile 帮助："
	@echo ""
	@echo "📋 主要命令分类："
	@echo "  make help-build                          # 显示构建相关帮助"
	@echo "  make help-docker                         # 显示 Docker 相关帮助"
	@echo ""
	@echo "🔧 代码质量："
	@echo "  make fmt                                 # 格式化代码"
	@echo "  make clippy                              # 运行 clippy 检查"
	@echo "  make test                                # 运行测试"
	@echo "  make pre-commit                          # 运行所有预提交检查"
	@echo ""
	@echo "🚀 快速开始："
	@echo "  make build                               # 构建 RustFS 二进制"
	@echo "  make docker-dev-local                    # 构建开发 Docker 镜像（本地）"
	@echo "  make dev-env-start                       # 启动开发环境"
	@echo ""
	@echo "💡 更多帮助请使用 'make help-build' 或 'make help-docker'"
