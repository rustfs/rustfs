## —— Tests and e2e test ---------------------------------------------------------------------------

TEST_THREADS ?= 1

.PHONY: script-tests
script-tests: ## Run shell script tests
	@echo "Running script tests..."
	./scripts/test_build_rustfs_options.sh

.PHONY: test
test: core-deps test-deps script-tests ## Run all tests
	@echo "🧪 Running tests..."
	@if command -v cargo-nextest >/dev/null 2>&1; then \
		cargo nextest run --all --exclude e2e_test; \
	else \
		echo "ℹ️ cargo-nextest not found; falling back to 'cargo test'"; \
		cargo test --workspace --exclude e2e_test -- --nocapture --test-threads="$(TEST_THREADS)"; \
	fi
	cargo test --all --doc

.PHONY: e2e-server
e2e-server: ## Run e2e-server tests
	sh $(shell pwd)/scripts/run.sh

.PHONY: probe-e2e
probe-e2e: ## Probe e2e tests
	sh $(shell pwd)/scripts/probe.sh
