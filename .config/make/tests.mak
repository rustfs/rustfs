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
.PHONY: release-package-tests
release-package-tests: ## Run release package integration on Linux with fpm, dpkg and rpm installed
	$(RUSTFS_PYTHON_BIN) ./scripts/test_release_packages.py

.PHONY: release-image-tests
release-image-tests: ## Run OCI integration with Docker, Go and skopeo; set RELEASE_IMAGE_VARIANT and RELEASE_IMAGE_OUTPUT
	$(RUSTFS_PYTHON_BIN) ./scripts/test_release_images.py

script-tests: ## Run shell script tests
	@echo "Running script tests..."
	trap 'status=$$?; printf "ERROR: script-tests failed (exit %s): %s\n" "$$status" "$$BASH_COMMAND" >&2; exit "$$status"' ERR
	./scripts/test_build_rustfs_options.sh
	./scripts/test_docker_runtime_timezone.sh
	./scripts/test_entrypoint_credentials.sh
	./scripts/test_entrypoint_directories.sh
	./scripts/test_internode_grpc_ab_bench.sh
	$(RUSTFS_PYTHON_BIN) ./scripts/test_internode_grpc_window.py
	./scripts/test_object_batch_bench_enhanced.sh
	./scripts/test_package_service_scripts.sh
	./scripts/test_hotpath_warp_ab_gate.sh
	./scripts/test_hotpath_warp_abba.sh
	./scripts/test_scanner_validation_harness.sh
	./scripts/test_scanner_heal_checkpoint_crash_evidence.sh
	./scripts/test_scanner_heal_authority_evidence.sh
	./scripts/test_scanner_heal_scoped_ack_evidence.sh
	./scripts/test_scanner_heal_legacy_rollback_evidence.sh
	./scripts/test_scanner_heal_g14_multiset_evidence.sh
	./scripts/test_scanner_heal_scheduler_pressure_evidence.sh
	./scripts/test_scanner_heal_status_outcome_evidence.sh
	./scripts/test_scanner_heal_maintenance_evidence.sh
	./scripts/test_scanner_heal_w13_mrf_evidence.sh
	./scripts/test_scanner_heal_w16_recovery_evidence.sh
	./scripts/test_exact_1mib_handoff_abba.sh
	./scripts/test_pinned_paired_abba_bench.sh
	./scripts/test_manual_transition_runbooks.sh
	./scripts/test_fuzz_runner.sh
	./scripts/test_python_bin.sh
	$(RUSTFS_PYTHON_BIN) ./scripts/test_check_doc_paths.py
	./scripts/check_embedded_secrets.sh --self-test
	./scripts/check_s3s_footprint.sh --self-test
	./scripts/test_check_test_count_ratchet.sh
	./scripts/test_guard_source_set.sh
	./scripts/test_split_guard_rules.sh
	$(RUSTFS_PYTHON_BIN) ./scripts/check_test_wiring.py --self-test
	$(RUSTFS_PYTHON_BIN) ./scripts/codemods/s3_error_to_s3_types.py --self-test
	$(RUSTFS_PYTHON_BIN) ./scripts/codemods/split_bucket_usecase.py --self-test
	$(RUSTFS_PYTHON_BIN) ./scripts/codemods/split_ecfs_legacy_edge.py --self-test
	$(RUSTFS_PYTHON_BIN) ./scripts/codemods/split_lifecycle_transition_api_test.py --self-test
	$(RUSTFS_PYTHON_BIN) ./scripts/check_test_seams.py --self-test
	$(RUSTFS_PYTHON_BIN) ./scripts/check_test_retirements.py --self-test
	$(RUSTFS_PYTHON_BIN) ./scripts/test_e2e_binary.py
	$(RUSTFS_PYTHON_BIN) ./scripts/test_download_release_asset.py
	$(RUSTFS_PYTHON_BIN) ./scripts/test_reference_binary.py
	$(RUSTFS_PYTHON_BIN) ./scripts/test_inspect_btf_prototypes.py
	$(RUSTFS_PYTHON_BIN) ./scripts/test_release_build_workflow.py
	$(RUSTFS_PYTHON_BIN) -m unittest discover -s scripts/release -p "test_*.py"
	$(RUSTFS_PYTHON_BIN) ./scripts/test_release_glibc.py
	$(RUSTFS_PYTHON_BIN) ./scripts/test_upgrade_workflow.py
	$(RUSTFS_PYTHON_BIN) ./scripts/test_migration_gate_evidence.py
	$(RUSTFS_PYTHON_BIN) ./scripts/ci_gate.py --self-test
	$(RUSTFS_PYTHON_BIN) ./scripts/check_connect_release_boundary.py --self-test
	$(RUSTFS_PYTHON_BIN) ./scripts/test_ci_test_artifact.py
	$(RUSTFS_PYTHON_BIN) ./scripts/test_ci_merge_group_proof.py
	$(RUSTFS_PYTHON_BIN) ./scripts/test_ci_merge_queue.py
	$(RUSTFS_PYTHON_BIN) -m unittest discover -s scripts/ci -p 'test_*.py'
	$(RUSTFS_PYTHON_BIN) ./scripts/check_security_coverage.py --self-test
	$(RUSTFS_PYTHON_BIN) ./scripts/check_scheduled_validation_freshness.py --self-test
	$(RUSTFS_PYTHON_BIN) ./scripts/test_security_workflow.py
	$(RUSTFS_PYTHON_BIN) ./scripts/test_nightly_candidate.py
	$(RUSTFS_PYTHON_BIN) ./scripts/test_functional_chain.py
	$(RUSTFS_PYTHON_BIN) ./scripts/test_functional_chain_health.py
	$(RUSTFS_PYTHON_BIN) ./scripts/test_repro_rejoin_readiness_docker.py
	$(RUSTFS_PYTHON_BIN) ./scripts/test_validation_registry.py
	$(RUSTFS_PYTHON_BIN) ./scripts/test_inspect_validation_runner.py
	$(RUSTFS_PYTHON_BIN) ./scripts/test_retain_validation_evidence.py
	$(RUSTFS_PYTHON_BIN) ./scripts/test_ci_timing_report.py
	$(RUSTFS_PYTHON_BIN) ./scripts/test_junit_diff.py
	$(RUSTFS_PYTHON_BIN) ./scripts/test_dual_stack.py
	$(RUSTFS_PYTHON_BIN) ./scripts/test_mint_stack_diff.py
	$(RUSTFS_PYTHON_BIN) ./scripts/check_conformance_rulings.py --self-test
	$(RUSTFS_PYTHON_BIN) ./scripts/compat_matrix_report.py --self-test
	$(RUSTFS_PYTHON_BIN) ./scripts/s3-tests/test_report_compat.py
	$(RUSTFS_PYTHON_BIN) ./scripts/s3-tests/test_runner_tools.py
	$(RUSTFS_PYTHON_BIN) ./scripts/s3-tests/test_api_coverage.py
	$(RUSTFS_PYTHON_BIN) ./scripts/s3-tests/test_reference_parity.py
	bash -n ./scripts/validate_object_data_cache_cold_stampede.sh
	$(RUSTFS_PYTHON_BIN) ./scripts/check_object_data_cache_follower_samples.py --self-test
	./scripts/validate_object_data_cache_cold_stampede.sh --self-test
	./scripts/run_scanner_heal_evidence_case.sh --self-test

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
