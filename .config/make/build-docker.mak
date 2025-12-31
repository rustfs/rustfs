## —— Docker-based build (alternative approach) ----------------------------------------------------

# Usage: make BUILD_OS=ubuntu22.04 build-docker
# Output: target/ubuntu22.04/release/rustfs

.PHONY: build-docker
build-docker: SOURCE_BUILD_IMAGE_NAME = rustfs-$(BUILD_OS):v1
build-docker: SOURCE_BUILD_CONTAINER_NAME = rustfs-$(BUILD_OS)-build
build-docker: BUILD_CMD = /root/.cargo/bin/cargo build --release --bin rustfs --target-dir /root/s3-rustfs/target/$(BUILD_OS)
build-docker: ## Build using Docker container # e.g (make build-docker BUILD_OS=ubuntu22.04)
	@echo "🐳 Building RustFS using Docker ($(BUILD_OS))..."
	$(DOCKER_CLI) buildx build -t $(SOURCE_BUILD_IMAGE_NAME) -f $(DOCKERFILE_SOURCE) .
	$(DOCKER_CLI) run --rm --name $(SOURCE_BUILD_CONTAINER_NAME) -v $(shell pwd):/root/s3-rustfs -it $(SOURCE_BUILD_IMAGE_NAME) $(BUILD_CMD)

.PHONY: docker-inspect-multiarch
docker-inspect-multiarch: ## Check image architecture support
	@if [ -z "$(IMAGE)" ]; then \
		echo "❌ Error: Please specify image, example: make docker-inspect-multiarch IMAGE=rustfs/rustfs:latest"; \
		exit 1; \
	fi
	@echo "🔍 Inspecting multi-architecture image: $(IMAGE)"
	docker buildx imagetools inspect $(IMAGE)