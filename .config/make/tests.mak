## —— Tests and e2e test ---------------------------------------------------------------------------

TEST_THREADS ?= 1

# cargo-nextest is a HARD dependency of `make test`.
#
# nextest changes test semantics vs plain `cargo test`: it runs every test in
# its own process (so serial_test's #[serial] mutex does not serialize across
# tests) and it is the only runner that honours .config/nextest.toml
# [test-groups] (e.g. the ecstore-serial-flaky serialization guard). CI runs
# nextest (.github/actions/setup/action.yml installs it), so a silent fallback
# to `cargo test` would run with different serialization behaviour than CI and
# mask (or invent) flakes.
#
# Installing cargo-nextest:
#   cargo install cargo-nextest --locked          # from source
#   # or a prebuilt binary (faster) via the taiki-e installer / get.nexte.st:
#   #   https://nexte.st/docs/installation/
#
# Escape hatch: set RUSTFS_ALLOW_CARGO_TEST_FALLBACK=1 to run the plain
# `cargo test` fallback anyway. Results are NOT authoritative — semantics
# differ from CI and .config/nextest.toml test-groups will NOT apply.

.PHONY: script-tests
script-tests: ## Run shell script tests
	@echo "Running script tests..."
	./scripts/test_build_rustfs_options.sh
	./scripts/test_entrypoint_credentials.sh

.PHONY: test
test: core-deps script-tests ## Run all tests (needs cargo-nextest; RUSTFS_ALLOW_CARGO_TEST_FALLBACK=1 to override)
	@echo "🧪 Running tests..."
	@if command -v cargo-nextest >/dev/null 2>&1; then \
		cargo nextest run --all --exclude e2e_test; \
	elif [ "$${RUSTFS_ALLOW_CARGO_TEST_FALLBACK:-0}" = "1" ]; then \
		echo >&2 "⚠️  ============================================================================"; \
		echo >&2 "⚠️  cargo-nextest NOT found — running the 'cargo test' fallback (opt-in)."; \
		echo >&2 "⚠️  TEST SEMANTICS DIFFER FROM CI; results are NOT authoritative:"; \
		echo >&2 "⚠️    * nextest runs each test in its own process; 'cargo test' does not,"; \
		echo >&2 "⚠️      so serial_test #[serial] serialization behaves differently."; \
		echo >&2 "⚠️    * .config/nextest.toml [test-groups] will NOT apply (e.g. the"; \
		echo >&2 "⚠️      ecstore-serial-flaky group), so load-sensitive tests may flake here"; \
		echo >&2 "⚠️      but pass on CI (or vice versa)."; \
		echo >&2 "⚠️  Install cargo-nextest and re-run before trusting these results."; \
		echo >&2 "⚠️  ============================================================================"; \
		cargo test --workspace --exclude e2e_test -- --nocapture --test-threads="$(TEST_THREADS)"; \
	else \
		echo >&2 "❌ cargo-nextest is required for 'make test' but was not found."; \
		echo >&2 ""; \
		echo >&2 "   RustFS tests run under cargo-nextest (process-per-test isolation)."; \
		echo >&2 "   CI runs nextest and .config/nextest.toml [test-groups] only take effect"; \
		echo >&2 "   under nextest. Plain 'cargo test' has different serialization semantics"; \
		echo >&2 "   and is NOT a faithful substitute."; \
		echo >&2 ""; \
		echo >&2 "   Install it with either:"; \
		echo >&2 "     cargo install cargo-nextest --locked"; \
		echo >&2 "   or a prebuilt binary (faster) — see https://nexte.st/docs/installation/"; \
		echo >&2 ""; \
		echo >&2 "   To run the plain 'cargo test' fallback anyway (results NOT authoritative;"; \
		echo >&2 "   serialization semantics differ from CI), re-run with:"; \
		echo >&2 "     RUSTFS_ALLOW_CARGO_TEST_FALLBACK=1 make test"; \
		exit 1; \
	fi
	cargo test --all --doc

.PHONY: e2e-server
e2e-server: ## Run e2e-server tests
	sh $(shell pwd)/scripts/run.sh

.PHONY: probe-e2e
probe-e2e: ## Probe e2e tests
	sh $(shell pwd)/scripts/probe.sh
