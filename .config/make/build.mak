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


## —— Profiling build (dial9 Tokio runtime telemetry) ------------------------------------------

# dial9 hooks Tokio's unstable runtime instrumentation, so it needs
# `--cfg tokio_unstable`. That flag is deliberately absent from
# .cargo/config.toml: it is not free, and release binaries do not carry it.
# Setting RUSTFLAGS here replaces (never appends to) the config-file value, and
# crates/obs/build.rs fails the build if the feature and the flag disagree.
#
# DIAL9_FEATURES can add `dial9-s3` (upload sealed segments) or
# `dial9-taskdump` (async backtraces of stalled tasks; Linux x86_64/aarch64,
# and also needs --cfg tokio_taskdump).
DIAL9_FEATURES ?= dial9
DIAL9_RUSTFLAGS ?= --cfg tokio_unstable

.PHONY: build-profiling
build-profiling: ## Build RustFS with dial9 Tokio runtime telemetry (diagnostic builds only)
	@echo "🔬 Building RustFS with dial9 telemetry (features: $(DIAL9_FEATURES))..."
	@echo "⚠️  Diagnostic build: telemetry writes trace segments to disk continuously."
	RUSTFLAGS="$(DIAL9_RUSTFLAGS)" cargo build --release --bin rustfs --features $(DIAL9_FEATURES)

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
