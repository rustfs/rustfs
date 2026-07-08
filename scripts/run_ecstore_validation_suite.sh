#!/usr/bin/env bash
set -euo pipefail

PROFILE="quick"
OUT_DIR=""
DRY_RUN=false
SKIP_E2E=false
SKIP_S3_TESTS=false
SKIP_COVERAGE=false
REQUIRE_FIXTURES=false
UNIT_COVERAGE_MIN="95"
UNIT_COVERAGE_TARGET="100"
UNIT_COVERAGE_SCOPE="ec-critical"

usage() {
  cat <<'USAGE'
Usage:
  scripts/run_ecstore_validation_suite.sh [options]

Profiles:
  quick        Core EC read/write/recovery checks for PR smoke (default)
  full         quick + s3-tests subset + fixture tests + coverage
  destructive full + cluster/concurrency/destructive recovery checks
  fuzz         bounded fuzz smoke for malformed storage inputs

Options:
  --profile <quick|full|destructive|fuzz>
  --out-dir <path>       Default: target/ecstore-validation/<timestamp>
  --skip-e2e             Skip e2e_test commands
  --skip-s3-tests        Skip scripts/s3-tests/run.sh in full/destructive
  --skip-coverage        Explicitly skip the mandatory unit coverage gate
  --unit-coverage-min <percent>
                         Minimum rustfs-ecstore unit line coverage (default: 95)
  --unit-coverage-scope <ec-critical|crate>
                         Coverage scope for the hard gate (default: ec-critical)
  --require-fixtures     Fail when optional EC fixture tests cannot run
  --dry-run              Print commands and write metadata without executing
  -h, --help

Artifacts:
  <out-dir>/run-metadata.env
  <out-dir>/summary.tsv
  <out-dir>/blackbox-matrix.tsv
  <out-dir>/logs/*.log
USAGE
}

arg_value() {
  local flag="$1"
  local value="${2:-}"
  if [[ -z "$value" || "$value" == --* ]]; then
    echo "ERROR: missing value for $flag" >&2
    exit 1
  fi
  printf '%s\n' "$value"
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --profile) PROFILE="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --out-dir) OUT_DIR="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --skip-e2e) SKIP_E2E=true; shift ;;
      --skip-s3-tests) SKIP_S3_TESTS=true; shift ;;
      --skip-coverage) SKIP_COVERAGE=true; shift ;;
      --unit-coverage-min) UNIT_COVERAGE_MIN="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --unit-coverage-scope) UNIT_COVERAGE_SCOPE="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --require-fixtures) REQUIRE_FIXTURES=true; shift ;;
      --dry-run) DRY_RUN=true; shift ;;
      -h|--help) usage; exit 0 ;;
      *)
        echo "ERROR: unknown arg: $1" >&2
        usage
        exit 1
        ;;
    esac
  done
}

validate_args() {
  case "$PROFILE" in
    quick|full|destructive|fuzz) ;;
    *)
      echo "ERROR: --profile must be quick, full, destructive, or fuzz" >&2
      exit 1
      ;;
  esac

  if ! [[ "$UNIT_COVERAGE_MIN" =~ ^([0-9]|[1-9][0-9]|100)(\.[0-9]+)?$ ]]; then
    echo "ERROR: --unit-coverage-min must be between 0 and 100" >&2
    exit 1
  fi
  case "$UNIT_COVERAGE_SCOPE" in
    ec-critical|crate) ;;
    *)
      echo "ERROR: --unit-coverage-scope must be ec-critical or crate" >&2
      exit 1
      ;;
  esac
}

setup_workspace() {
  local root
  root="$(git rev-parse --show-toplevel)"
  cd "$root"

  if [[ -z "$OUT_DIR" ]]; then
    OUT_DIR="target/ecstore-validation/$(date -u +%Y%m%dT%H%M%SZ)"
  fi
	  mkdir -p "$OUT_DIR/logs"

	  SUMMARY="$OUT_DIR/summary.tsv"
	  BLACKBOX_MATRIX="$OUT_DIR/blackbox-matrix.tsv"
	  printf 'status\tlabel\tlog\n' >"$SUMMARY"
}

write_metadata() {
  {
    printf 'profile=%s\n' "$PROFILE"
    printf 'started_at=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf 'dry_run=%s\n' "$DRY_RUN"
    printf 'skip_e2e=%s\n' "$SKIP_E2E"
    printf 'skip_s3_tests=%s\n' "$SKIP_S3_TESTS"
    printf 'skip_coverage=%s\n' "$SKIP_COVERAGE"
    printf 'unit_coverage_min=%s\n' "$UNIT_COVERAGE_MIN"
    printf 'unit_coverage_target=%s\n' "$UNIT_COVERAGE_TARGET"
    printf 'unit_coverage_scope=%s\n' "$UNIT_COVERAGE_SCOPE"
    printf 'require_fixtures=%s\n' "$REQUIRE_FIXTURES"
    printf 'git_commit=%s\n' "$(git rev-parse HEAD 2>/dev/null || true)"
    printf 'git_branch=%s\n' "$(git branch --show-current 2>/dev/null || true)"
    printf 'rustc=%s\n' "$(rustc --version 2>/dev/null || true)"
    printf 'cargo=%s\n' "$(cargo --version 2>/dev/null || true)"
  } >"$OUT_DIR/run-metadata.env"
}

safe_label() {
  printf '%s\n' "$1" | tr -c 'A-Za-z0-9_.-' '_'
}

quote_command() {
  local quoted=()
  local arg
  for arg in "$@"; do
    quoted+=("$(printf '%q' "$arg")")
  done
  printf '%s ' "${quoted[@]}"
}

record_skip() {
  local label="$1"
  local reason="$2"
  local log="$OUT_DIR/logs/$(safe_label "$label").log"
  printf 'SKIP: %s\n' "$reason" | tee "$log"
  printf 'SKIP\t%s\t%s\n' "$label" "$log" >>"$SUMMARY"
}

run_step() {
  local label="$1"
  shift
  local log="$OUT_DIR/logs/$(safe_label "$label").log"

  {
    printf 'label=%s\n' "$label"
    printf 'command='
    quote_command "$@"
    printf '\n'
  } >"$log"

  if [[ "$DRY_RUN" == "true" ]]; then
    printf 'DRY-RUN: '
    quote_command "$@"
    printf '\n'
    printf 'DRY_RUN\t%s\t%s\n' "$label" "$log" >>"$SUMMARY"
    return 0
  fi

  printf 'RUN: %s\n' "$label"
  if "$@" 2>&1 | tee -a "$log"; then
    printf 'PASS\t%s\t%s\n' "$label" "$log" >>"$SUMMARY"
  else
    local status=$?
    printf 'FAIL\t%s\t%s\n' "$label" "$log" >>"$SUMMARY"
    return "$status"
  fi
}

run_core_unit_steps() {
	  run_step "filemeta-lib" cargo test -p rustfs-filemeta --lib
	  run_step "ecstore-erasure-lib" cargo test -p rustfs-ecstore --lib erasure
	  run_step "ecstore-set-disk-read-lib" cargo test -p rustfs-ecstore --lib set_disk::read
	  run_step "ecstore-io-primitives-lib" cargo test -p rustfs-ecstore --lib set_disk::core::io_primitives
	  run_step "ecstore-rename-rollback" \
	    cargo test -p rustfs-ecstore --lib set_disk::tests::test_rename_data_quorum_failure_rolls_back_destination_object
	  run_step "ecstore-disk-local-lib" cargo test -p rustfs-ecstore --lib disk::local
  run_step "ecstore-lib-all" cargo test -p rustfs-ecstore --lib -- --test-threads=1
}

run_quick_e2e_steps() {
  if [[ "$SKIP_E2E" == "true" ]]; then
    record_skip "e2e-quick" "--skip-e2e was set"
    return
  fi

  run_step "e2e-reliability-disk-fault" cargo test --package e2e_test reliability_disk_fault_test -- --nocapture
  run_step "e2e-heal-erasure-disk-rebuild" cargo test --package e2e_test heal_erasure_disk_rebuild_test -- --nocapture
  run_step "e2e-namespace-lock-quorum" cargo test --package e2e_test namespace_lock_quorum_test -- --nocapture
}

run_quick_profile() {
  run_core_unit_steps
  run_quick_e2e_steps
}

fixture_available() {
	  [[ -n "${RUSTFS_MINIO_FIXTURE_ROOT:-}" && -d "${RUSTFS_MINIO_FIXTURE_ROOT}" && -n "${RUSTFS_MINIO_STATIC_KMS_KEY_B64:-}" ]]
}

legacy_fixture_available() {
  local root="${RUSTFS_LEGACY_TEST_ROOT:-$(pwd)}"
  local disk="${RUSTFS_LEGACY_TEST_DISK:-}"
  if [[ -z "$disk" ]]; then
    if [[ -f "$root/test/.minio.sys/format.json" ]]; then
      disk="test"
    else
      disk="test1"
    fi
  fi

  if [[ ! -f "$root/$disk/.rustfs.sys/format.json" && ! -f "$root/$disk/.minio.sys/format.json" ]]; then
    return 1
  fi

	  [[ -f "$root/$disk/vvvv/ktvzip.tar.gz/xl.meta" || -f "$root/$disk/vvvv/path_traversal.md/xl.meta" ]]
}

minio_fixture_missing_reason() {
	  if [[ -z "${RUSTFS_MINIO_FIXTURE_ROOT:-}" ]]; then
	    printf 'RUSTFS_MINIO_FIXTURE_ROOT is not set\n'
	  elif [[ ! -d "${RUSTFS_MINIO_FIXTURE_ROOT}" ]]; then
	    printf 'RUSTFS_MINIO_FIXTURE_ROOT does not exist: %s\n' "$RUSTFS_MINIO_FIXTURE_ROOT"
	  elif [[ -z "${RUSTFS_MINIO_STATIC_KMS_KEY_B64:-}" ]]; then
	    printf 'RUSTFS_MINIO_STATIC_KMS_KEY_B64 is not set\n'
	  else
	    printf 'unknown MinIO fixture gate failure\n'
	  fi
}

write_blackbox_matrix() {
	  local e2e_status="enabled"
	  local s3_status="enabled"
	  local destructive_status="enabled"
	  local legacy_status="missing-optional"
	  local minio_status="missing-optional"
	  if [[ "$SKIP_E2E" == "true" ]]; then
	    e2e_status="disabled"
	    destructive_status="disabled"
	  fi
	  if [[ "$SKIP_S3_TESTS" == "true" ]]; then
	    s3_status="disabled"
	  fi
	  if legacy_fixture_available; then
	    legacy_status="enabled"
	  elif [[ "$REQUIRE_FIXTURES" == "true" ]]; then
	    legacy_status="missing-required"
	  fi
	  if fixture_available; then
	    minio_status="enabled"
	  elif [[ "$REQUIRE_FIXTURES" == "true" ]]; then
	    minio_status="missing-required"
	  fi

	  {
	    printf 'profile\tscenario\tgate\tcommand\tfixture_env\tstatus\n'
	    printf 'quick\tsingle-node disk fault read/write\tblack-box\tcargo test --package e2e_test reliability_disk_fault_test -- --nocapture\tnone\t%s\n' "$e2e_status"
	    printf 'quick\theal degraded erasure disk rebuild\tblack-box\tcargo test --package e2e_test heal_erasure_disk_rebuild_test -- --nocapture\tnone\t%s\n' "$e2e_status"
	    printf 'quick\tnamespace lock quorum under EC ops\tblack-box\tcargo test --package e2e_test namespace_lock_quorum_test -- --nocapture\tnone\t%s\n' "$e2e_status"
	    printf 'full\tlegacy bitrot read fixture restore\tfixture\tcargo test -p rustfs-ecstore --test legacy_bitrot_read_test -- --nocapture\tRUSTFS_LEGACY_TEST_ROOT,RUSTFS_LEGACY_TEST_DISK\t%s\n' "$legacy_status"
	    printf 'full\tMinIO generated encrypted read and negative restore fixture\tfixture\tcargo test -p rustfs-ecstore --features rio-v2 --test minio_generated_read_test -- --ignored --nocapture\tRUSTFS_MINIO_FIXTURE_ROOT,RUSTFS_MINIO_STATIC_KMS_KEY_B64\t%s\n' "$minio_status"
	    printf 'full\tS3 multipart range versioning delete subset\tblack-box\tenv TESTEXPR=\"multipart or range or versioning or delete\" DEPLOY_MODE=build MAXFAIL=0 ./scripts/s3-tests/run.sh\tnone\t%s\n' "$s3_status"
	    printf 'destructive\tdistributed cluster concurrency\tblack-box\tcargo test --package e2e_test cluster_concurrency_test -- --nocapture\tnone\t%s\n' "$destructive_status"
	    printf 'destructive\tstale multipart cleanup cluster\tblack-box\tcargo test --package e2e_test stale_multipart_cleanup_cluster_test -- --nocapture\tnone\t%s\n' "$destructive_status"
	    printf 'destructive\tdelete marker migration semantics\tblack-box\tcargo test --package e2e_test delete_marker_migration_semantics_test -- --nocapture\tnone\t%s\n' "$destructive_status"
	  } >"$BLACKBOX_MATRIX"
}

run_fixture_gate() {
	  local label="ecstore-fixture-gate"
	  local log="$OUT_DIR/logs/$(safe_label "$label").log"
	  local legacy_ok=false
	  local minio_ok=false
	  if legacy_fixture_available; then
	    legacy_ok=true
	  fi
	  if fixture_available; then
	    minio_ok=true
	  fi

	  {
	    printf 'label=%s\n' "$label"
	    printf 'legacy_fixture_available=%s\n' "$legacy_ok"
	    printf 'minio_fixture_available=%s\n' "$minio_ok"
	    printf 'require_fixtures=%s\n' "$REQUIRE_FIXTURES"
	    if [[ "$minio_ok" != "true" ]]; then
	      printf 'minio_fixture_reason=%s\n' "$(minio_fixture_missing_reason)"
	    fi
	  } | tee "$log"

	  if [[ "$DRY_RUN" == "true" ]]; then
	    printf 'DRY_RUN\t%s\t%s\n' "$label" "$log" >>"$SUMMARY"
	    return 0
	  fi

	  if [[ "$legacy_ok" == "true" && "$minio_ok" == "true" ]]; then
	    printf 'PASS\t%s\t%s\n' "$label" "$log" >>"$SUMMARY"
	    return 0
	  fi

	  if [[ "$REQUIRE_FIXTURES" == "true" ]]; then
	    printf 'FAIL\t%s\t%s\n' "$label" "$log" >>"$SUMMARY"
	    return 1
	  fi

	  printf 'SKIP\t%s\t%s\n' "$label" "$log" >>"$SUMMARY"
}

run_fixture_steps() {
	  if legacy_fixture_available; then
	    run_step "ecstore-legacy-bitrot-read-fixture" cargo test -p rustfs-ecstore --test legacy_bitrot_read_test -- --nocapture
	  elif [[ "$REQUIRE_FIXTURES" == "true" ]]; then
    echo "ERROR: RUSTFS_LEGACY_TEST_ROOT/RUSTFS_LEGACY_TEST_DISK is required for legacy bitrot fixture tests" >&2
    exit 1
  else
    record_skip "ecstore-legacy-bitrot-read-fixture" "legacy bitrot fixture is not present"
  fi

	  if fixture_available; then
	    run_step "ecstore-minio-generated-read-fixture" \
	      cargo test -p rustfs-ecstore --features rio-v2 --test minio_generated_read_test -- --ignored --nocapture
	  elif [[ "$REQUIRE_FIXTURES" == "true" ]]; then
	    echo "ERROR: $(minio_fixture_missing_reason)" >&2
	    exit 1
	  else
	    record_skip "ecstore-minio-generated-read-fixture" "$(minio_fixture_missing_reason)"
	  fi
}

run_s3_subset() {
  if [[ "$SKIP_S3_TESTS" == "true" ]]; then
    record_skip "s3-tests-ec-subset" "--skip-s3-tests was set"
    return
  fi

  run_step "s3-tests-ec-subset" \
    env TESTEXPR="multipart or range or versioning or delete" DEPLOY_MODE=build MAXFAIL=0 ./scripts/s3-tests/run.sh
}

write_coverage_reports() {
  local lcov_path="$1"
  local coverage_summary="$2"
  local coverage_files="$3"
  local root="$4"

  awk -v root="$root" -v scope="$UNIT_COVERAGE_SCOPE" -v summary="$coverage_summary" -v files="$coverage_files" '
    function is_crate_file(f) {
      return index(f, root "/crates/ecstore/src/") == 1
    }
    function is_ec_critical_file(f) {
      return index(f, root "/crates/ecstore/src/erasure/") == 1 ||
        f == root "/crates/ecstore/src/set_disk/read.rs" ||
        f == root "/crates/ecstore/src/set_disk/shard_source.rs" ||
        f == root "/crates/ecstore/src/set_disk/metadata.rs" ||
        f == root "/crates/ecstore/src/set_disk/ops/object.rs" ||
        f == root "/crates/ecstore/src/set_disk/core/io_primitives.rs" ||
        f == root "/crates/ecstore/src/disk/local.rs"
    }
    function is_gate_file(f) {
      return scope == "crate" ? is_crate_file(f) : is_ec_critical_file(f)
    }
    function pct(hit, found) {
      return found == 0 ? 0 : (hit / found) * 100
    }
    function flush() {
      if (file == "") {
        return
      }
      if (is_crate_file(file)) {
        crate_hit += hit
        crate_found += found
      }
      if (is_ec_critical_file(file)) {
        ec_hit += hit
        ec_found += found
      }
      if (is_gate_file(file)) {
        gate_hit += hit
        gate_found += found
      }
      if (is_crate_file(file)) {
        printf "%s\t%.2f\t%d\t%d\n", file, pct(hit, found), hit, found >> files
      }
    }
    BEGIN {
      print "file\tline_coverage\thit\tfound" > files
    }
    /^SF:/ {
      flush()
      file = substr($0, 4)
      hit = 0
      found = 0
      next
    }
    /^LH:/ { hit += substr($0, 4); next }
    /^LF:/ { found += substr($0, 4); next }
    END {
      flush()
      if (gate_found == 0) {
        exit 2
      }
      print "metric\tactual\tmin\ttarget\tscope" > summary
      printf "gate_line_coverage\t%.2f\t%s\t%s\t%s\n", pct(gate_hit, gate_found), min, target, scope >> summary
      printf "ec_critical_line_coverage\t%.2f\t%s\t%s\tec-critical\n", pct(ec_hit, ec_found), min, target >> summary
      printf "crate_line_coverage\t%.2f\t%s\t%s\tcrate\n", pct(crate_hit, crate_found), min, target >> summary
    }
  ' min="$UNIT_COVERAGE_MIN" target="$UNIT_COVERAGE_TARGET" "$lcov_path"
}

run_coverage() {
  if [[ "$SKIP_COVERAGE" == "true" ]]; then
    record_skip "ecstore-coverage" "--skip-coverage was set"
    return
  fi

  local label="ecstore-unit-coverage"
  local log="$OUT_DIR/logs/$(safe_label "$label").log"
  local coverage_dir="$OUT_DIR/coverage/ecstore"
  local lcov_path="$coverage_dir/lcov.info"
  local coverage_summary="$coverage_dir/summary.tsv"
  local coverage_files="$coverage_dir/files.tsv"
  local cmd=(cargo llvm-cov -p rustfs-ecstore --lib --lcov --output-path "$lcov_path" -- --test-threads=1)
  local root
  root="$(pwd)"
  mkdir -p "$coverage_dir"

  {
    printf 'label=%s\n' "$label"
    printf 'unit_coverage_min=%s\n' "$UNIT_COVERAGE_MIN"
    printf 'unit_coverage_target=%s\n' "$UNIT_COVERAGE_TARGET"
    printf 'unit_coverage_scope=%s\n' "$UNIT_COVERAGE_SCOPE"
    printf 'command='
    quote_command "${cmd[@]}"
    printf '\n'
  } >"$log"

  if [[ "$DRY_RUN" == "true" ]]; then
    printf 'DRY-RUN: '
    quote_command "${cmd[@]}"
    printf '\n'
    printf 'DRY_RUN\t%s\t%s\n' "$label" "$log" >>"$SUMMARY"
    return
  fi

  if ! command -v cargo-llvm-cov >/dev/null 2>&1; then
    printf 'ERROR: cargo-llvm-cov is required for the ECStore unit coverage gate\n' | tee -a "$log"
    printf 'FAIL\t%s\t%s\n' "$label" "$log" >>"$SUMMARY"
    return 127
  fi

  printf 'RUN: %s\n' "$label"
  if "${cmd[@]}" 2>&1 | tee -a "$log"; then
    :
  else
    local status=$?
    printf 'FAIL\t%s\t%s\n' "$label" "$log" >>"$SUMMARY"
    return "$status"
  fi

  if ! write_coverage_reports "$lcov_path" "$coverage_summary" "$coverage_files" "$root"; then
    printf 'ERROR: unable to compute scoped coverage from %s\n' "$lcov_path" | tee -a "$log"
    printf 'FAIL\t%s\t%s\n' "$label" "$log" >>"$SUMMARY"
    return 1
  fi

  local line_coverage
  line_coverage="$(awk -F '\t' '$1 == "gate_line_coverage" { print $2 }' "$coverage_summary")"
  printf 'gate_line_coverage=%s%% scope=%s min=%s%% target=%s%%\n' \
    "$line_coverage" "$UNIT_COVERAGE_SCOPE" "$UNIT_COVERAGE_MIN" "$UNIT_COVERAGE_TARGET" | tee -a "$log"
  printf 'coverage_summary=%s\ncoverage_files=%s\n' "$coverage_summary" "$coverage_files" | tee -a "$log"

  if awk -v actual="$line_coverage" -v min="$UNIT_COVERAGE_MIN" 'BEGIN { exit !(actual + 0 >= min + 0) }'; then
    printf 'PASS\t%s\t%s\n' "$label" "$log" >>"$SUMMARY"
  else
    printf 'FAIL: %s coverage %s%% is below %s%%\n' "$UNIT_COVERAGE_SCOPE" "$line_coverage" "$UNIT_COVERAGE_MIN" | tee -a "$log"
    printf 'FAIL\t%s\t%s\n' "$label" "$log" >>"$SUMMARY"
    return 1
  fi
}

run_full_profile() {
	  run_quick_profile
	  run_fixture_gate
	  run_fixture_steps
	  run_s3_subset
	  run_coverage
}

run_destructive_profile() {
  run_full_profile
  if [[ "$SKIP_E2E" == "true" ]]; then
    record_skip "e2e-destructive" "--skip-e2e was set"
    return
  fi

  run_step "e2e-cluster-concurrency" cargo test --package e2e_test cluster_concurrency_test -- --nocapture
  run_step "e2e-stale-multipart-cleanup-cluster" cargo test --package e2e_test stale_multipart_cleanup_cluster_test -- --nocapture
  run_step "e2e-delete-marker-migration-semantics" cargo test --package e2e_test delete_marker_migration_semantics_test -- --nocapture
}

run_fuzz_profile() {
  local max_total_time="${MAX_TOTAL_TIME:-60}"
  run_step "fuzz-storage-inputs" env MAX_TOTAL_TIME="$max_total_time" ./scripts/fuzz/run.sh
}

main() {
  parse_args "$@"
	  validate_args
	  setup_workspace
	  write_metadata
	  write_blackbox_matrix

	  case "$PROFILE" in
    quick) run_quick_profile ;;
    full) run_full_profile ;;
    destructive) run_destructive_profile ;;
    fuzz) run_fuzz_profile ;;
  esac

  printf 'summary=%s\n' "$SUMMARY"
  printf 'artifacts=%s\n' "$OUT_DIR"
}

main "$@"
