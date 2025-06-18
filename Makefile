###########
# 远程开发，需要 VSCode 安装 Dev Containers, Remote SSH, Remote Explorer
# https://code.visualstudio.com/docs/remote/containers
###########
DOCKER_CLI ?= docker
IMAGE_NAME ?= rustfs:v1.0.0
CONTAINER_NAME ?= rustfs-dev
DOCKERFILE_PATH = $(shell pwd)/.docker

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
	cargo clippy --all-targets --all-features -- -D warnings

.PHONY: check
check:
	@echo "🔨 Running compilation check..."
	cargo check --all-targets

.PHONY: test
test:
	@echo "🧪 Running tests..."
	cargo test --all --exclude e2e_test

.PHONY: pre-commit
pre-commit: fmt clippy check test
	@echo "✅ All pre-commit checks passed!"

.PHONY: setup-hooks
setup-hooks:
	@echo "🔧 Setting up git hooks..."
	chmod +x .git/hooks/pre-commit
	@echo "✅ Git hooks setup complete!"

.PHONY: init-devenv
init-devenv:
	$(DOCKER_CLI) build -t $(IMAGE_NAME) -f $(DOCKERFILE_PATH)/Dockerfile.devenv .
	$(DOCKER_CLI) stop $(CONTAINER_NAME)
	$(DOCKER_CLI) rm $(CONTAINER_NAME)
	$(DOCKER_CLI) run -d --name $(CONTAINER_NAME) -p 9010:9010 -p 9000:9000 -v $(shell pwd):/root/s3-rustfs -it $(IMAGE_NAME)

.PHONY: start
start:
	$(DOCKER_CLI) start $(CONTAINER_NAME)

.PHONY: stop
stop:
	$(DOCKER_CLI) stop $(CONTAINER_NAME)

.PHONY: e2e-server
e2e-server:
	sh $(shell pwd)/scripts/run.sh

.PHONY: probe-e2e
probe-e2e:
	sh $(shell pwd)/scripts/probe.sh

# make BUILD_OS=ubuntu22.04 build
# in target/ubuntu22.04/release/rustfs

# make BUILD_OS=rockylinux9.3 build
# in target/rockylinux9.3/release/rustfs
BUILD_OS ?= rockylinux9.3
.PHONY: build
build: ROCKYLINUX_BUILD_IMAGE_NAME = rustfs-$(BUILD_OS):v1
build: ROCKYLINUX_BUILD_CONTAINER_NAME = rustfs-$(BUILD_OS)-build
build: BUILD_CMD = /root/.cargo/bin/cargo build --release --bin rustfs --target-dir /root/s3-rustfs/target/$(BUILD_OS)
build:
	$(DOCKER_CLI) build -t $(ROCKYLINUX_BUILD_IMAGE_NAME) -f $(DOCKERFILE_PATH)/Dockerfile.$(BUILD_OS) .
	$(DOCKER_CLI) run --rm --name $(ROCKYLINUX_BUILD_CONTAINER_NAME) -v $(shell pwd):/root/s3-rustfs -it $(ROCKYLINUX_BUILD_IMAGE_NAME) $(BUILD_CMD)

.PHONY: build-musl
build-musl:
	@echo "🔨 Building rustfs for x86_64-unknown-linux-musl..."
	cargo build --target x86_64-unknown-linux-musl --bin rustfs -r

.PHONY: build-gnu
build-gnu:
	@echo "🔨 Building rustfs for x86_64-unknown-linux-gnu..."
	cargo build --target x86_64-unknown-linux-gnu --bin rustfs -r

.PHONY: deploy-dev
deploy-dev: build-musl
	@echo "🚀 Deploying to dev server: $${IP}"
	./scripts/dev_deploy.sh $${IP}

# Multi-architecture Docker build targets
.PHONY: docker-build-multiarch
docker-build-multiarch:
	@echo "🏗️ Building multi-architecture Docker images..."
	./scripts/build-docker-multiarch.sh

.PHONY: docker-build-multiarch-push
docker-build-multiarch-push:
	@echo "🚀 Building and pushing multi-architecture Docker images..."
	./scripts/build-docker-multiarch.sh --push

.PHONY: docker-build-multiarch-version
docker-build-multiarch-version:
	@if [ -z "$(VERSION)" ]; then \
		echo "❌ 错误: 请指定版本, 例如: make docker-build-multiarch-version VERSION=v1.0.0"; \
		exit 1; \
	fi
	@echo "🏗️ Building multi-architecture Docker images (version: $(VERSION))..."
	./scripts/build-docker-multiarch.sh --version $(VERSION)

.PHONY: docker-push-multiarch-version
docker-push-multiarch-version:
	@if [ -z "$(VERSION)" ]; then \
		echo "❌ 错误: 请指定版本, 例如: make docker-push-multiarch-version VERSION=v1.0.0"; \
		exit 1; \
	fi
	@echo "🚀 Building and pushing multi-architecture Docker images (version: $(VERSION))..."
	./scripts/build-docker-multiarch.sh --version $(VERSION) --push

.PHONY: docker-build-ubuntu
docker-build-ubuntu:
	@echo "🏗️ Building multi-architecture Ubuntu Docker images..."
	./scripts/build-docker-multiarch.sh --type ubuntu

.PHONY: docker-build-rockylinux
docker-build-rockylinux:
	@echo "🏗️ Building multi-architecture RockyLinux Docker images..."
	./scripts/build-docker-multiarch.sh --type rockylinux

.PHONY: docker-build-devenv
docker-build-devenv:
	@echo "🏗️ Building multi-architecture development environment Docker images..."
	./scripts/build-docker-multiarch.sh --type devenv

.PHONY: docker-build-all-types
docker-build-all-types:
	@echo "🏗️ Building all multi-architecture Docker image types..."
	./scripts/build-docker-multiarch.sh --type production
	./scripts/build-docker-multiarch.sh --type ubuntu
	./scripts/build-docker-multiarch.sh --type rockylinux
	./scripts/build-docker-multiarch.sh --type devenv

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
	@if ! command -v cross &> /dev/null; then \
		echo "📦 Installing cross..."; \
		cargo install cross; \
	fi
	@echo "🔨 Generating protobuf code..."
	cargo run --bin gproto || true
	@echo "🔨 Building x86_64-unknown-linux-musl..."
	cargo build --release --target x86_64-unknown-linux-musl --bin rustfs
	@echo "🔨 Building aarch64-unknown-linux-gnu..."
	cross build --release --target aarch64-unknown-linux-gnu --bin rustfs
	@echo "✅ All architectures built successfully!"

.PHONY: help-docker
help-docker:
	@echo "🐳 Docker 多架构构建帮助："
	@echo ""
	@echo "基本构建:"
	@echo "  make docker-build-multiarch              # 构建多架构镜像（不推送）"
	@echo "  make docker-build-multiarch-push         # 构建并推送多架构镜像"
	@echo ""
	@echo "版本构建:"
	@echo "  make docker-build-multiarch-version VERSION=v1.0.0   # 构建指定版本"
	@echo "  make docker-push-multiarch-version VERSION=v1.0.0    # 构建并推送指定版本"
	@echo ""
	@echo "镜像类型:"
	@echo "  make docker-build-ubuntu                 # 构建 Ubuntu 镜像"
	@echo "  make docker-build-rockylinux             # 构建 RockyLinux 镜像"
	@echo "  make docker-build-devenv                 # 构建开发环境镜像"
	@echo "  make docker-build-all-types              # 构建所有类型镜像"
	@echo ""
	@echo "辅助工具:"
	@echo "  make build-cross-all                     # 构建所有架构的二进制文件"
	@echo "  make docker-inspect-multiarch IMAGE=xxx  # 检查镜像的架构支持"
	@echo ""
	@echo "环境变量 (在推送时需要设置):"
	@echo "  DOCKERHUB_USERNAME    Docker Hub 用户名"
	@echo "  DOCKERHUB_TOKEN       Docker Hub 访问令牌"
	@echo "  GITHUB_TOKEN          GitHub 访问令牌"
