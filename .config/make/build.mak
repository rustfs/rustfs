## —— Native build using build-rustfs.sh script ----------------------------------------------------

.PHONY: build
build: ## Build RustFS using build-rustfs.sh script
	@echo "🔨 Building RustFS using build-rustfs.sh script..."
	./build-rustfs.sh

.PHONY: build-dev
build-dev: ## Build RustFS in development mode
	@echo "🔨 Building RustFS in development mode..."
	./build-rustfs.sh --dev

.PHONY: build-musl
build-musl: ## Build rustfs for x86_64-unknown-linux-musl
	@echo "🔨 Building rustfs for x86_64-unknown-linux-musl..."
	@echo "💡 On macOS/Windows, use 'make build-docker' or 'make docker-dev' instead"
	./build-rustfs.sh --platform x86_64-unknown-linux-musl

.PHONY: build-gnu
build-gnu: ## Build rustfs for x86_64-unknown-linux-gnu
	@echo "🔨 Building rustfs for x86_64-unknown-linux-gnu..."
	@echo "💡 On macOS/Windows, use 'make build-docker' or 'make docker-dev' instead"
	./build-rustfs.sh --platform x86_64-unknown-linux-gnu

.PHONY: build-musl-arm64
build-musl-arm64: ## Build rustfs for aarch64-unknown-linux-musl
	@echo "🔨 Building rustfs for aarch64-unknown-linux-musl..."
	@echo "💡 On macOS/Windows, use 'make build-docker' or 'make docker-dev' instead"
	./build-rustfs.sh --platform aarch64-unknown-linux-musl

.PHONY: build-gnu-arm64
build-gnu-arm64: ## Build rustfs for aarch64-unknown-linux-gnu
	@echo "🔨 Building rustfs for aarch64-unknown-linux-gnu..."
	@echo "💡 On macOS/Windows, use 'make build-docker' or 'make docker-dev' instead"
	./build-rustfs.sh --platform aarch64-unknown-linux-gnu


.PHONY: build-cross-all
build-cross-all: ## Build all target architectures
	@echo "🔧 Building all target architectures..."
	@echo "💡 On macOS/Windows, use 'make docker-dev' for reliable multi-arch builds"
	@echo "🔨 Generating protobuf code..."
	cargo run --bin gproto || true
	@echo "🔨 Building x86_64-unknown-linux-gnu..."
	./build-rustfs.sh

.PHONY: build-all-architectures
build-all-architectures: core-deps ## Build All architectures
	@echo "🔨 Running compilatio--platform x86_64-unknown-linux-gnu
	@echo "🔨 Building aarch64-unknown-linux-gnu..."
	./build-rustfs.sh --platform aarch64-unknown-linux-gnu
	@echo "🔨 Building x86_64-unknown-linux-musl..."
	./build-rustfs.sh --platform x86_64-unknown-linux-musl
	@echo "🔨 Building aarch64-unknown-linux-musl..."
	./build-rustfs.sh --platform aarch64-unknown-linux-musl
	@echo "✅ All architectures built successfully!"