## —— Local Native Build using build-rustfs.sh script (Recommended) --------------------------------

.PHONY: build
build: ## Build RustFS binary (includes console by default)
	@echo "🔨 Building RustFS using build-rustfs.sh script..."
	./build-rustfs.sh

.PHONY: build-dev
build-dev: ## Build RustFS in Development mode
	@echo "🔨 Building RustFS in development mode..."
	./build-rustfs.sh --dev

.PHONY: build-musl
build-musl: ## Build x86_64 musl version
	@echo "🔨 Building rustfs for x86_64-unknown-linux-musl..."
	@echo "💡 On macOS/Windows, use 'make build-docker' or 'make docker-dev' instead"
	./build-rustfs.sh --platform x86_64-unknown-linux-musl

.PHONY: build-gnu
build-gnu: ## Build x86_64 GNU version
	@echo "🔨 Building rustfs for x86_64-unknown-linux-gnu..."
	@echo "💡 On macOS/Windows, use 'make build-docker' or 'make docker-dev' instead"
	./build-rustfs.sh --platform x86_64-unknown-linux-gnu

.PHONY: build-musl-arm64
build-musl-arm64: ## Build aarch64 musl version
	@echo "🔨 Building rustfs for aarch64-unknown-linux-musl..."
	@echo "💡 On macOS/Windows, use 'make build-docker' or 'make docker-dev' instead"
	./build-rustfs.sh --platform aarch64-unknown-linux-musl

.PHONY: build-gnu-arm64
build-gnu-arm64: ## Build aarch64 GNU version
	@echo "🔨 Building rustfs for aarch64-unknown-linux-gnu..."
	@echo "💡 On macOS/Windows, use 'make build-docker' or 'make docker-dev' instead"
	./build-rustfs.sh --platform aarch64-unknown-linux-gnu


.PHONY: build-cross-all
build-cross-all: core-deps ## Build binaries for all architectures
	@echo "🔧 Building all target architectures..."
	@echo "💡 On macOS/Windows, use 'make docker-dev' for reliable multi-arch builds"
	@echo "🔨 Generating protobuf code..."
	cargo run --bin gproto || true

	@echo "🔨 Building rustfs for x86_64-unknown-linux-musl..."
	./build-rustfs.sh --platform x86_64-unknown-linux-musl

	@echo "🔨 Building rustfs for x86_64-unknown-linux-gnu..."
	./build-rustfs.sh --platform x86_64-unknown-linux-gnu

	@echo "🔨 Building rustfs for aarch64-unknown-linux-musl..."
	./build-rustfs.sh --platform aarch64-unknown-linux-musl

	@echo "🔨 Building rustfs for aarch64-unknown-linux-gnu..."
	./build-rustfs.sh --platform aarch64-unknown-linux-gnu
