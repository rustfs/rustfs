## —— Coverage --------------------------------------------------------------------------------------

# Local equivalent of the weekly coverage workflow (.github/workflows/coverage.yml,
# backlog#1153 infra-5): same measurement scope (--workspace --exclude e2e_test,
# nextest `ci` profile) and the same per-crate table. Slow — the instrumented
# build cannot reuse your normal target cache and then runs the whole suite.
# Doctests are not measured (needs nightly). Outputs land in target/llvm-cov/.
.PHONY: coverage
coverage: core-deps ## Workspace line coverage (cargo-llvm-cov + nextest; slow, writes target/llvm-cov/)
	@if ! command -v cargo-llvm-cov >/dev/null 2>&1; then \
		echo >&2 "❌ cargo-llvm-cov is required for 'make coverage' but was not found."; \
		echo >&2 "   Install it with:"; \
		echo >&2 "     cargo install cargo-llvm-cov --locked"; \
		echo >&2 "     rustup component add llvm-tools-preview"; \
		exit 1; \
	fi
	@if ! command -v cargo-nextest >/dev/null 2>&1; then \
		echo >&2 "❌ cargo-nextest is required for 'make coverage' (see 'make test')."; \
		echo >&2 "   Install it with: cargo install cargo-nextest --locked"; \
		exit 1; \
	fi
	NEXTEST_PROFILE=ci cargo llvm-cov nextest --workspace --exclude e2e_test --no-report
	@mkdir -p target/llvm-cov
	cargo llvm-cov report --lcov --output-path target/llvm-cov/lcov.info
	cargo llvm-cov report --json --output-path target/llvm-cov/coverage.json
	python3 scripts/coverage_per_crate.py target/llvm-cov/coverage.json
