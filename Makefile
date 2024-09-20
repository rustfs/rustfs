###########
# 远程开发，需要 VSCode 安装 Dev Containers, Remote SSH, Remote Explorer
# https://code.visualstudio.com/docs/remote/containers
###########
DOCKER_CLI ?= docker
IMAGE_NAME ?= rustfs:v1.0.0
CONTAINER_NAME ?= rustfs-dev

.PHONY: init
init:
	$(DOCKER_CLI) build -t $(IMAGE_NAME) .
	$(DOCKER_CLI) stop $(CONTAINER_NAME)
	$(DOCKER_CLI) rm $(CONTAINER_NAME)
	$(DOCKER_CLI) run -d --name $(CONTAINER_NAME) -p 9010:9010 -v $(shell pwd):/root/s3-rustfs -it $(IMAGE_NAME)

.PHONY: start
start:
	$(DOCKER_CLI) start $(CONTAINER_NAME)
