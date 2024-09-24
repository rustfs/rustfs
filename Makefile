###########
# 远程开发，需要 VSCode 安装 Dev Containers, Remote SSH, Remote Explorer
# https://code.visualstudio.com/docs/remote/containers
###########
DOCKER_CLI ?= docker
IMAGE_NAME ?= rustfs:v1.0.0
CONTAINER_NAME ?= rustfs-dev
DOCKERFILE ?= $(shell pwd)/.docker/Dockerfile.devenv

.PHONY: init-devenv
init-devenv:
	$(DOCKER_CLI) build -t $(IMAGE_NAME) -f $(DOCKERFILE) .
	$(DOCKER_CLI) stop $(CONTAINER_NAME)
	$(DOCKER_CLI) rm $(CONTAINER_NAME)
	$(DOCKER_CLI) run -d --name $(CONTAINER_NAME) -p 9010:9010 -p 9000:9000 -v $(shell pwd):/root/s3-rustfs -it $(IMAGE_NAME)

.PHONY: start
start:
	$(DOCKER_CLI) start $(CONTAINER_NAME)

.PHONY: stop
stop:
	$(DOCKER_CLI) stop $(CONTAINER_NAME)	

.PHONY: e2e
e2e-server:
	sh $(shell pwd)/scripts/run.sh 
