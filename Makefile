###########
# 远程开发，需要 VSCode 安装 Dev Containers, Remote SSH, Remote Explorer
# https://code.visualstudio.com/docs/remote/containers
###########
DOCKER_CLI ?= docker
IMAGE_NAME ?= rustfs:v1.0.0
CONTAINER_NAME ?= rustfs-dev
DOCKERFILE_PATH = $(shell pwd)/.docker

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
