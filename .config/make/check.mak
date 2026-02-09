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
# (e.g., cargo-nextest for enhanced testing)
warn-%:
	@command -v $* >/dev/null 2>&1 || { \
		echo >&2 "⚠️ '$*' is not installed."; \
	}

# For checking dependencies use check-<dep-name> or warn-<dep-name>
.PHONY: core-deps fmt-deps test-deps
core-deps: check-cargo ## Check core dependencies
fmt-deps: check-rustfmt ## Check lint and formatting dependencies
test-deps: warn-cargo-nextest ## Check tests dependencies
