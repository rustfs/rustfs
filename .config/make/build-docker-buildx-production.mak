## —— Production builds using docker buildx (for CI/CD and production) -----------------------------

.PHONY: docker-buildx
docker-buildx: ## Build multi-architecture production Docker images
	@echo "🏗️ Building multi-architecture production Docker images with buildx..."
	./docker-buildx.sh

.PHONY: docker-buildx-push
docker-buildx-push: ## Build and push multi-architecture production Docker images
	@echo "🚀 Building and pushing multi-architecture production Docker images with buildx..."
	./docker-buildx.sh --push

.PHONY: docker-buildx-version
docker-buildx-version: ## Build and version multi-architecture production Docker images
	@if [ -z "$(VERSION)" ]; then \
		echo "❌ Error: Please specify version, example: make docker-buildx-version VERSION=v1.0.0"; \
		exit 1; \
	fi
	@echo "🏗️ Building multi-architecture production Docker images (version: $(VERSION))..."
	./docker-buildx.sh --release $(VERSION)

.PHONY: docker-buildx-push-version
docker-buildx-push-version: ## Build and version and push multi-architecture production Docker images
	@if [ -z "$(VERSION)" ]; then \
		echo "❌ Error: Please specify version, example: make docker-buildx-push-version VERSION=v1.0.0"; \
		exit 1; \
	fi
	@echo "🚀 Building and pushing multi-architecture production Docker images (version: $(VERSION))..."
	./docker-buildx.sh --release $(VERSION) --push

.PHONY: docker-buildx-production-local
docker-buildx-production-local: ## Build single-architecture production Docker image for local use
	@echo "🏗️ Building single-architecture production Docker image locally..."
	@echo "💡 Alternative to docker-buildx.sh for local testing"
	$(DOCKER_CLI) buildx build \
		--file $(DOCKERFILE_PRODUCTION) \
		--tag rustfs:production-latest \
		--tag rustfs:latest \
		--load \
		--build-arg RELEASE=latest \
		.
