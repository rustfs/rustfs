## —— Check and Inform Dependencies ----------------------------------------------------------------

# Fatal check
# Checks all required dependencies and exits with error if not found
# (e.g., cargo, rustfmt)
check-%:
	@command -v $* >/dev/null 2>&1 || { \
		echo >&2 "❌ '$*' is not installed."; \
		exit 1; \
	}

# Warning-only check
# Checks for optional dependencies and issues a warning if not found
warn-%:
	@command -v $* >/dev/null 2>&1 || { \
		echo >&2 "⚠️ '$*' is not installed."; \
	}

# For checking dependencies use check-<dep-name> or warn-<dep-name>
#
# NOTE: cargo-nextest is a HARD dependency of `make test`, gated inside the
# test recipe itself (with a RUSTFS_ALLOW_CARGO_TEST_FALLBACK=1 escape hatch)
# rather than a warn-only prerequisite here — see .config/make/tests.mak.
.PHONY: core-deps fmt-deps
core-deps: check-cargo ## Check core dependencies
fmt-deps: check-rustfmt ## Check lint and formatting dependencies
