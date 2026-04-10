## —— Tests and e2e test ---------------------------------------------------------------------------

TEST_THREADS ?= 1

.PHONY: test
test: core-deps test-deps ## Run all tests
	@echo "🧪 Running tests..."
	@if command -v cargo-nextest >/dev/null 2>&1; then \
		cargo nextest run --all --exclude e2e_test; \
	else \
		echo "ℹ️ cargo-nextest not found; falling back to 'cargo test'"; \
		cargo test --workspace --exclude e2e_test -- --nocapture --test-threads="$(TEST_THREADS)"; \
	fi
	cargo test --all --doc
	cargo test -p rustfs get_object_chunk_fast_path
	cargo test -p rustfs materialize_chunk_stream_before_commit
	touch rustfs/build.rs
	cargo build -p rustfs --bins --jobs 2
	cargo test -p e2e_test archive_multipart_roundtrip_preserves_bytes
	cargo test -p e2e_test presigned_get_and_reverse_proxy_preserve_multipart_bytes_with_fast_path

.PHONY: e2e-server
e2e-server: ## Run e2e-server tests
	sh $(shell pwd)/scripts/run.sh

.PHONY: probe-e2e
probe-e2e: ## Probe e2e tests
	sh $(shell pwd)/scripts/probe.sh
