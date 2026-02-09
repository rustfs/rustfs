## —— Development/Source builds using direct buildx commands ---------------------------------------

.PHONY: docker-dev
docker-dev: ## Build dev multi-arch image (cannot load locally)
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
docker-dev-local: ## Build dev single-arch image (local load)
	@echo "🏗️ Building single-architecture development Docker image for local use..."
	@echo "💡 This builds from source code for the current platform and loads locally"
	$(DOCKER_CLI) buildx build \
		--file $(DOCKERFILE_SOURCE) \
		--tag rustfs:source-latest \
		--tag rustfs:dev-latest \
		--load \
		.

.PHONY: docker-dev-push
docker-dev-push: ## Build and push multi-arch development image # e.g (make docker-dev-push REGISTRY=xxx)
	@if [ -z "$(REGISTRY)" ]; then \
		echo "❌ Error: Please specify registry, example: make docker-dev-push REGISTRY=ghcr.io/username"; \
		exit 1; \
	fi
	@echo "🚀 Building and pushing multi-architecture development Docker images..."
	@echo "💡 Pushing to registry: $(REGISTRY)"
	$(DOCKER_CLI) buildx build \
		--platform linux/amd64,linux/arm64 \
		--file $(DOCKERFILE_SOURCE) \
		--tag $(REGISTRY)/rustfs:source-latest \
		--tag $(REGISTRY)/rustfs:dev-latest \
		--push \
		.

.PHONY: dev-env-start
dev-env-start: ## Start development container environment
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
dev-env-stop: ## Stop development container environment
	@echo "🛑 Stopping development environment..."
	$(DOCKER_CLI) stop $(CONTAINER_NAME) 2>/dev/null || true
	$(DOCKER_CLI) rm $(CONTAINER_NAME) 2>/dev/null || true

.PHONY: dev-env-restart
dev-env-restart: dev-env-stop dev-env-start ## Restart development container environment
