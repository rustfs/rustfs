## —— Code quality and Formatting ------------------------------------------------------------------

.PHONY: fmt
fmt: core-deps fmt-deps ## Format code
	@echo "🔧 Formatting code..."
	cargo fmt --all

.PHONY: fmt-check
fmt-check: core-deps fmt-deps ## Check code formatting
	@echo "📝 Checking code formatting..."
	cargo fmt --all --check

.PHONY: clippy-check
clippy-check: core-deps ## Run clippy checks
	@echo "🔍 Running clippy checks..."
	cargo clippy --fix --allow-dirty
	cargo clippy --all-targets --all-features -- -D warnings

.PHONY: unsafe-code-check
unsafe-code-check: ## Check unsafe_code allowances have SAFETY comments
	@echo "🔒 Checking unsafe_code allowances..."
	./scripts/check_unsafe_code_allowances.sh

.PHONY: compilation-check
compilation-check: core-deps ## Run compilation check
	@echo "🔨 Running compilation check..."
	cargo check --all-targets
