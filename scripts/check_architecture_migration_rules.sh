#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ARCH_DOC_DIR="${ROOT_DIR}/docs/architecture"
BOUNDARY_DOC="${ARCH_DOC_DIR}/crate-boundaries.md"
REGISTER_DOC="${ARCH_DOC_DIR}/compat-cleanup-register.md"

FAILURES=0

report_failure() {
  printf 'Architecture migration rule failed: %s\n' "$1" >&2
  FAILURES=$((FAILURES + 1))
}

contains_line() {
  local needle="$1"
  local file="$2"

  grep -qxF "$needle" "$file"
}

require_source_line() {
  local file="$1"
  local expected="$2"
  local description="$3"

  if ! contains_line "$expected" "${ROOT_DIR}/${file}"; then
    report_failure "${description} missing exact source line in ${file}: ${expected}"
  fi
}

require_source_contains() {
  local file="$1"
  local expected="$2"
  local description="$3"

  if ! grep -qF "$expected" "${ROOT_DIR}/${file}"; then
    report_failure "${description} missing source text in ${file}: ${expected}"
  fi
}

if grep -Eq '^[[:space:]]*#!\[allow\([^]]*dead_code' "${ROOT_DIR}/crates/ecstore/src/lib.rs"; then
  report_failure "ecstore crate root must not use allow(dead_code); scope temporary allowances to owning modules"
fi

require_source_contains "docs/architecture/overview.md" "## Baseline" "architecture overview baseline section"
require_source_contains "docs/architecture/overview.md" "## Core Principle" "architecture overview core principle section"
require_source_contains "docs/architecture/overview.md" "## Phase Order" "architecture overview phase order section"
require_source_contains "docs/architecture/runtime-lifecycle.md" "## Startup And Readiness" "runtime lifecycle startup readiness section"
require_source_contains "docs/architecture/runtime-lifecycle.md" "## Shutdown Lifecycle Boundary" "runtime lifecycle shutdown boundary section"
require_source_contains "docs/architecture/runtime-lifecycle.md" "## AppContext Foundation" "runtime lifecycle AppContext section"
require_source_contains "docs/architecture/storage-control-data-plane.md" "## Storage API Contracts" "storage control/data plane contracts section"
require_source_contains "docs/architecture/storage-control-data-plane.md" "## Cluster Control Plane" "storage control/data plane cluster section"
require_source_contains "docs/architecture/storage-control-data-plane.md" "## Background Controllers" "storage control/data plane background controllers section"
require_source_contains "docs/architecture/crate-boundaries.md" "## Dependency Direction" "crate boundaries dependency direction section"
require_source_contains "docs/architecture/crate-boundaries.md" "storage-api -> ecstore" "crate boundaries storage dependency rule"
require_source_contains "docs/architecture/crate-boundaries.md" "extension-schema -> rustfs" "crate boundaries extension dependency rule"
require_source_contains "docs/architecture/readiness-matrix.md" "## Request Behavior Matrix" "readiness matrix request behavior section"
require_source_contains "docs/architecture/readiness-matrix.md" "## Runtime Dependency Matrix" "readiness matrix runtime dependency section"
require_source_contains "docs/architecture/readiness-matrix.md" "## Probe Semantics" "readiness matrix probe semantics section"
require_source_contains "docs/architecture/readiness-matrix.md" "S3 data plane" "readiness matrix S3 data plane behavior"
require_source_contains "docs/architecture/readiness-matrix.md" "Internode RPC and gRPC" "readiness matrix internode RPC behavior"
require_source_contains "docs/architecture/readiness-matrix.md" "StorageReady" "readiness matrix storage dependency"
require_source_contains "docs/architecture/readiness-matrix.md" "IamReady" "readiness matrix IAM dependency"
require_source_contains "docs/architecture/readiness-matrix.md" "FullReady" "readiness matrix full readiness dependency"
require_source_contains "docs/architecture/readiness-matrix.md" "RUSTFS_HEALTH_PEER_READY_CHECK_ENABLE" "readiness matrix peer health gate"
require_source_contains "docs/architecture/global-state-crate-split-plan.md" "## Remaining Global Owners" "global state plan owner inventory"
require_source_contains "docs/architecture/global-state-crate-split-plan.md" "## Runtime Source Boundaries" "global state plan runtime source section"
require_source_contains "docs/architecture/global-state-crate-split-plan.md" "## Fallback Removal Plan" "global state plan fallback section"
require_source_contains "docs/architecture/global-state-crate-split-plan.md" "## Crate Split Evaluation" "global state plan crate split section"
require_source_contains "docs/architecture/global-state-crate-split-plan.md" "rustfs_ecstore::api::global" "global state plan facade boundary"
require_source_contains "docs/architecture/global-state-crate-split-plan.md" "global-state-inventory.md" "global state inventory plan link"
require_source_contains "docs/architecture/global-state-inventory.md" "## Global State Classification" "global state inventory classification section"
require_source_contains "docs/architecture/global-state-inventory.md" "## Runtime Migration Inventory" "global state inventory migration section"
require_source_contains "docs/architecture/global-state-inventory.md" "GLOBAL_EXPIRY_STATE" "global state inventory first candidate"
require_source_contains "docs/architecture/global-state-inventory.md" "## RustFS Owner-Local Static Inventory" "global state inventory RustFS owner-local static section"
require_source_contains "docs/architecture/global-state-inventory.md" "KEYSTONE_AUTH" "global state inventory RustFS auth static inventory"
require_source_contains "docs/architecture/global-state-inventory.md" "DEADLOCK_DETECTOR" "global state inventory RustFS storage static inventory"
require_source_contains "docs/architecture/global-state-inventory.md" "CONCURRENCY_MANAGER" "global state inventory RustFS concurrency static inventory"
require_source_contains "docs/architecture/obs-ecstore-dependency-inventory.md" "## Dependency Inventory" "observability ECStore dependency inventory section"
require_source_contains "docs/architecture/obs-ecstore-dependency-inventory.md" "## Extraction Plan" "observability ECStore extraction plan section"
require_source_contains "docs/architecture/obs-ecstore-dependency-inventory.md" "crates/obs/src/metrics/storage_api.rs" "observability ECStore storage_api boundary"
require_source_contains "docs/architecture/overview.md" "ecstore-api-facade-inventory.md" "architecture overview ECStore facade inventory link"
require_source_contains "docs/architecture/ecstore-module-split-plan.md" "ecstore-api-facade-inventory.md" "ECStore split plan facade inventory link"
require_source_contains "docs/architecture/ecstore-api-facade-inventory.md" "## Facade Group Inventory" "ECStore facade inventory group section"
require_source_contains "docs/architecture/ecstore-api-facade-inventory.md" "## External Consumer Boundaries" "ECStore facade inventory consumer boundary section"
require_source_contains "docs/architecture/ecstore-api-facade-inventory.md" "## Split Dependency Inventory" "ECStore split dependency inventory section"
require_source_contains "docs/architecture/ecstore-api-facade-inventory.md" "## Shrink Rules" "ECStore facade shrink rules section"
require_source_contains "docs/architecture/ecstore-api-facade-inventory.md" "rustfs/src/storage/storage_api.rs" "RustFS storage ECStore facade boundary inventory"
require_source_contains "docs/architecture/ecstore-api-facade-inventory.md" "crates/scanner/src/storage_api.rs" "scanner ECStore facade boundary inventory"
require_source_contains "docs/architecture/ecstore-api-facade-inventory.md" "crates/obs/src/metrics/storage_api.rs" "OBS ECStore facade boundary inventory"
require_source_contains "docs/architecture/ecstore-api-facade-inventory.md" "crates/iam/src/storage_api.rs" "IAM ECStore facade boundary inventory"
require_source_contains "docs/architecture/ecstore-api-facade-inventory.md" "crates/heal/src/heal/storage_api.rs" "heal ECStore facade boundary inventory"
require_source_contains "docs/architecture/ecstore-api-facade-inventory.md" "crates/notify/src/storage_api.rs" "notify ECStore facade boundary inventory"
require_source_contains "docs/architecture/ecstore-api-facade-inventory.md" "crates/protocols/src/swift/storage_api.rs" "Swift ECStore facade boundary inventory"
require_source_contains "docs/architecture/ecstore-api-facade-inventory.md" "crates/s3select-api/src/storage_api.rs" "S3 Select ECStore facade boundary inventory"
require_source_contains "docs/architecture/ecstore-api-facade-inventory.md" "Do not add direct \`rustfs_ecstore::api\` imports outside the boundary files" "ECStore facade new-import shrink rule"

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

PR_TYPES_FILE="${TMP_DIR}/pr_types.txt"
PR_TYPE_HITS_FILE="${TMP_DIR}/pr_type_hits.txt"
SOURCE_MARKERS_FILE="${TMP_DIR}/source_markers.txt"
SOURCE_IDS_FILE="${TMP_DIR}/source_ids.txt"
REGISTER_IDS_FILE="${TMP_DIR}/register_ids.txt"
LEGACY_STORAGE_API_HITS_FILE="${TMP_DIR}/legacy_storage_api_hits.txt"
STORE_API_BUCKET_DTO_REEXPORTS_FILE="${TMP_DIR}/store_api_bucket_dto_reexports.txt"
STORE_API_BUCKET_OPERATION_HITS_FILE="${TMP_DIR}/store_api_bucket_operation_hits.txt"
STORE_API_MULTIPART_DTO_REEXPORTS_FILE="${TMP_DIR}/store_api_multipart_dto_reexports.txt"
STORE_API_OBJECT_HELPER_REEXPORTS_FILE="${TMP_DIR}/store_api_object_helper_reexports.txt"
STORE_API_RANGE_HELPER_REEXPORTS_FILE="${TMP_DIR}/store_api_range_helper_reexports.txt"
STORE_API_LIST_HELPER_REEXPORTS_FILE="${TMP_DIR}/store_api_list_helper_reexports.txt"
STORE_API_LIST_RESPONSE_REEXPORTS_FILE="${TMP_DIR}/store_api_list_response_reexports.txt"
ECSTORE_OBJECT_API_LIST_ALIAS_INTERNAL_HITS_FILE="${TMP_DIR}/ecstore_object_api_list_alias_internal_hits.txt"
ECSTORE_OBJECT_API_STORAGE_ALIAS_HITS_FILE="${TMP_DIR}/ecstore_object_api_storage_alias_hits.txt"
ECSTORE_OBJECT_API_EXTERNAL_ALIAS_EXPECTED_FILE="${TMP_DIR}/ecstore_object_api_external_alias_expected.txt"
ECSTORE_OBJECT_API_EXTERNAL_ALIAS_ACTUAL_FILE="${TMP_DIR}/ecstore_object_api_external_alias_actual.txt"
ECSTORE_OBJECT_API_EXTERNAL_ALIAS_DIFF_FILE="${TMP_DIR}/ecstore_object_api_external_alias_diff.txt"
ECSTORE_OBJECT_API_UNAPPROVED_NAME_HITS_FILE="${TMP_DIR}/ecstore_object_api_unapproved_name_hits.txt"
STARTUP_PUBLIC_SHIM_HITS_FILE="${TMP_DIR}/startup_public_shim_hits.txt"
ECSTORE_STORAGE_API_ROOT_REEXPORT_HITS_FILE="${TMP_DIR}/ecstore_storage_api_root_reexport_hits.txt"
ECSTORE_STORAGE_API_ROOT_CONSUMER_HITS_FILE="${TMP_DIR}/ecstore_storage_api_root_consumer_hits.txt"
ECSTORE_TEST_DIRECT_API_HITS_FILE="${TMP_DIR}/ecstore_test_direct_api_hits.txt"
ECSTORE_BENCH_DIRECT_API_HITS_FILE="${TMP_DIR}/ecstore_bench_direct_api_hits.txt"
LOCAL_STORAGE_API_RAW_CONTRACT_PATH_HITS_FILE="${TMP_DIR}/local_storage_api_raw_contract_path_hits.txt"
STORE_API_DELETE_DTO_REEXPORTS_FILE="${TMP_DIR}/store_api_delete_dto_reexports.txt"
STORE_API_DELETE_DTO_INTERNAL_HITS_FILE="${TMP_DIR}/store_api_delete_dto_internal_hits.txt"
STORE_API_LIFECYCLE_HELPER_DEFINITION_HITS_FILE="${TMP_DIR}/store_api_lifecycle_helper_definition_hits.txt"
STORE_API_LIFECYCLE_HELPER_OLD_CONSUMER_HITS_FILE="${TMP_DIR}/store_api_lifecycle_helper_old_consumer_hits.txt"
LIFECYCLE_AUDIT_SINK_BYPASS_HITS_FILE="${TMP_DIR}/lifecycle_audit_sink_bypass_hits.txt"
LIFECYCLE_CONFIG_BOUNDARY_BYPASS_HITS_FILE="${TMP_DIR}/lifecycle_config_boundary_bypass_hits.txt"
LIFECYCLE_METADATA_BOUNDARY_BYPASS_HITS_FILE="${TMP_DIR}/lifecycle_metadata_boundary_bypass_hits.txt"
LIFECYCLE_TAGGING_BOUNDARY_BYPASS_HITS_FILE="${TMP_DIR}/lifecycle_tagging_boundary_bypass_hits.txt"
LIFECYCLE_OBJECT_LOCK_BOUNDARY_BYPASS_HITS_FILE="${TMP_DIR}/lifecycle_object_lock_boundary_bypass_hits.txt"
LIFECYCLE_REPLICATION_SINK_BYPASS_HITS_FILE="${TMP_DIR}/lifecycle_replication_sink_bypass_hits.txt"
STORE_API_EXTERNAL_LIST_CONSUMER_HITS_FILE="${TMP_DIR}/store_api_external_list_consumer_hits.txt"
STORE_API_EXTERNAL_OPERATION_CONSUMER_HITS_FILE="${TMP_DIR}/store_api_external_operation_consumer_hits.txt"
STORE_API_OBJECT_OPERATION_LOCAL_METHOD_HITS_FILE="${TMP_DIR}/store_api_object_operation_local_method_hits.txt"
ECSTORE_DIRECT_STORAGE_API_SOURCE_HITS_FILE="${TMP_DIR}/ecstore_direct_storage_api_source_hits.txt"
DIRECT_ECSTORE_IMPORT_HITS_FILE="${TMP_DIR}/direct_ecstore_import_hits.txt"
ECSTORE_API_FACADE_REQUIRED_HITS_FILE="${TMP_DIR}/ecstore_api_facade_required_hits.txt"
ECSTORE_PUBLIC_FACADE_BYPASS_HITS_FILE="${TMP_DIR}/ecstore_public_facade_bypass_hits.txt"
TEST_HARNESS_NESTED_STORAGE_COMPAT_HITS_FILE="${TMP_DIR}/test_harness_nested_storage_compat_hits.txt"
RUSTFS_NESTED_STORAGE_COMPAT_HITS_FILE="${TMP_DIR}/rustfs_nested_storage_compat_hits.txt"
RUSTFS_RUNTIME_SCALAR_STORAGE_COMPAT_HITS_FILE="${TMP_DIR}/rustfs_runtime_scalar_storage_compat_hits.txt"
RUSTFS_RUNTIME_SECONDARY_STORAGE_COMPAT_HITS_FILE="${TMP_DIR}/rustfs_runtime_secondary_storage_compat_hits.txt"
RUSTFS_ROOT_STORAGE_COMPAT_REEXPORT_HITS_FILE="${TMP_DIR}/rustfs_root_storage_compat_reexport_hits.txt"
RUSTFS_ROOT_BUCKET_STORAGE_COMPAT_MODULE_HITS_FILE="${TMP_DIR}/rustfs_root_bucket_storage_compat_module_hits.txt"
RUSTFS_ROOT_RUNTIME_STORAGE_COMPAT_MODULE_HITS_FILE="${TMP_DIR}/rustfs_root_runtime_storage_compat_module_hits.txt"
RUSTFS_ROOT_CONSUMER_COMPAT_HITS_FILE="${TMP_DIR}/rustfs_root_consumer_compat_hits.txt"
RUSTFS_OWNER_COMPAT_CONSUMER_HITS_FILE="${TMP_DIR}/rustfs_owner_compat_consumer_hits.txt"
RUSTFS_LOCAL_COMPAT_GLOB_EXPORT_HITS_FILE="${TMP_DIR}/rustfs_local_compat_glob_export_hits.txt"
RUSTFS_ADMIN_CONFIG_STORAGE_COMPAT_MODULE_HITS_FILE="${TMP_DIR}/rustfs_admin_config_storage_compat_module_hits.txt"
RUSTFS_STORAGE_BUCKET_STORAGE_COMPAT_MODULE_HITS_FILE="${TMP_DIR}/rustfs_storage_bucket_storage_compat_module_hits.txt"
RUSTFS_STORAGE_OWNER_COMPAT_REEXPORT_HITS_FILE="${TMP_DIR}/rustfs_storage_owner_compat_reexport_hits.txt"
RUSTFS_STORAGE_OWNER_DIRECT_STORAGE_SOURCE_HITS_FILE="${TMP_DIR}/rustfs_storage_owner_direct_storage_source_hits.txt"
RUSTFS_STORAGE_OWNER_CONTRACT_ROOT_CONSUMER_HITS_FILE="${TMP_DIR}/rustfs_storage_owner_contract_root_consumer_hits.txt"
RUSTFS_ADMIN_CONTRACT_ROOT_CONSUMER_HITS_FILE="${TMP_DIR}/rustfs_admin_contract_root_consumer_hits.txt"
RUSTFS_ADMIN_BUCKET_STORAGE_COMPAT_MODULE_HITS_FILE="${TMP_DIR}/rustfs_admin_bucket_storage_compat_module_hits.txt"
RUSTFS_APP_BUCKET_STORAGE_COMPAT_MODULE_HITS_FILE="${TMP_DIR}/rustfs_app_bucket_storage_compat_module_hits.txt"
RUSTFS_OUTER_COMPAT_FACADE_ALIAS_HITS_FILE="${TMP_DIR}/rustfs_outer_compat_facade_alias_hits.txt"
RUSTFS_OUTER_COMPAT_SIGNATURE_ALIAS_HITS_FILE="${TMP_DIR}/rustfs_outer_compat_signature_alias_hits.txt"
RUSTFS_STORAGE_COMPAT_RAW_FACADE_PATH_HITS_FILE="${TMP_DIR}/rustfs_storage_compat_raw_facade_path_hits.txt"
RUSTFS_APP_ADMIN_COMPAT_RAW_FACADE_PATH_HITS_FILE="${TMP_DIR}/rustfs_app_admin_compat_raw_facade_path_hits.txt"
OUTER_CONSUMER_COMPAT_RAW_FACADE_PATH_HITS_FILE="${TMP_DIR}/outer_consumer_compat_raw_facade_path_hits.txt"
RUSTFS_ROOT_E2E_COMPAT_RAW_FACADE_PATH_HITS_FILE="${TMP_DIR}/rustfs_root_e2e_compat_raw_facade_path_hits.txt"
ALL_STORAGE_COMPAT_RAW_FACADE_PATH_HITS_FILE="${TMP_DIR}/all_storage_compat_raw_facade_path_hits.txt"
ALL_STORAGE_COMPAT_GROUPED_FACADE_IMPORT_HITS_FILE="${TMP_DIR}/all_storage_compat_grouped_facade_import_hits.txt"
ALL_ECSTORE_API_GROUPED_FACADE_IMPORT_HITS_FILE="${TMP_DIR}/all_ecstore_api_grouped_facade_import_hits.txt"
ALL_ECSTORE_API_RAW_SUBPATH_HITS_FILE="${TMP_DIR}/all_ecstore_api_raw_subpath_hits.txt"
EXTERNAL_PRODUCTION_ECSTORE_IMPORT_HITS_FILE="${TMP_DIR}/external_production_ecstore_import_hits.txt"
COMPLETED_EXTERNAL_OWNER_MODULE_ALIAS_HITS_FILE="${TMP_DIR}/completed_external_owner_module_alias_hits.txt"
COMPLETED_OWNER_BARE_FACADE_IMPORT_HITS_FILE="${TMP_DIR}/completed_owner_bare_facade_import_hits.txt"
COMPLETED_OWNER_SCATTERED_RAW_FACADE_PATH_HITS_FILE="${TMP_DIR}/completed_owner_scattered_raw_facade_path_hits.txt"
RUSTFS_STARTUP_OWNER_MODULE_CONSUMER_HITS_FILE="${TMP_DIR}/rustfs_startup_owner_module_consumer_hits.txt"
RUSTFS_RUNTIME_OWNER_MODULE_CONSUMER_HITS_FILE="${TMP_DIR}/rustfs_runtime_owner_module_consumer_hits.txt"
RUSTFS_ROOT_SERVER_OWNER_MODULE_CONSUMER_HITS_FILE="${TMP_DIR}/rustfs_root_server_owner_module_consumer_hits.txt"
RUSTFS_TABLE_S3_OWNER_MODULE_CONSUMER_HITS_FILE="${TMP_DIR}/rustfs_table_s3_owner_module_consumer_hits.txt"
RUSTFS_APP_SHARED_OWNER_MODULE_CONSUMER_HITS_FILE="${TMP_DIR}/rustfs_app_shared_owner_module_consumer_hits.txt"
RUSTFS_APP_BUCKET_OWNER_SOURCE_HITS_FILE="${TMP_DIR}/rustfs_app_bucket_owner_source_hits.txt"
RUSTFS_APP_ECSTORE_SOURCE_HITS_FILE="${TMP_DIR}/rustfs_app_ecstore_source_hits.txt"
RUSTFS_ADMIN_ECSTORE_SOURCE_HITS_FILE="${TMP_DIR}/rustfs_admin_ecstore_source_hits.txt"
EXTERNAL_RUNTIME_ECSTORE_COMPAT_BYPASS_HITS_FILE="${TMP_DIR}/external_runtime_ecstore_compat_bypass_hits.txt"
EXTERNAL_RUNTIME_STORAGE_API_BYPASS_HITS_FILE="${TMP_DIR}/external_runtime_storage_api_bypass_hits.txt"
GLOBAL_FACADE_BOUNDARY_EXPECTED_FILE="${TMP_DIR}/global_facade_boundary_expected.txt"
GLOBAL_FACADE_BOUNDARY_ACTUAL_FILE="${TMP_DIR}/global_facade_boundary_actual.txt"
GLOBAL_FACADE_BOUNDARY_DIFF_FILE="${TMP_DIR}/global_facade_boundary_diff.txt"
ECSTORE_GLOBAL_READ_ONLY_EXPORT_HITS_FILE="${TMP_DIR}/ecstore_global_read_only_export_hits.txt"
RUSTFS_STORAGE_GLOBAL_READ_ONLY_IMPORT_HITS_FILE="${TMP_DIR}/rustfs_storage_global_read_only_import_hits.txt"
EXTERNAL_STORAGE_API_DOMAIN_BYPASS_HITS_FILE="${TMP_DIR}/external_storage_api_domain_bypass_hits.txt"
EXTERNAL_STORAGE_API_ROOT_REEXPORT_HITS_FILE="${TMP_DIR}/external_storage_api_root_reexport_hits.txt"
RUSTFS_STORAGE_API_ROOT_REEXPORT_HITS_FILE="${TMP_DIR}/rustfs_storage_api_root_reexport_hits.txt"
RUSTFS_APP_ADMIN_STORAGE_HELPER_ROOT_REEXPORT_HITS_FILE="${TMP_DIR}/rustfs_app_admin_storage_helper_root_reexport_hits.txt"
EXTERNAL_TEST_ECSTORE_COMPAT_BYPASS_HITS_FILE="${TMP_DIR}/external_test_ecstore_compat_bypass_hits.txt"
FUZZ_ECSTORE_COMPAT_BYPASS_HITS_FILE="${TMP_DIR}/fuzz_ecstore_compat_bypass_hits.txt"
EXTERNAL_ECSTORE_API_BOUNDARY_HITS_FILE="${TMP_DIR}/external_ecstore_api_boundary_hits.txt"
REPLICATION_FACADE_BYPASS_HITS_FILE="${TMP_DIR}/replication_facade_bypass_hits.txt"
REPLICATION_FACADE_WILDCARD_EXPORT_HITS_FILE="${TMP_DIR}/replication_facade_wildcard_export_hits.txt"
REPLICATION_RESYNC_CONTRACT_BACKSLIDE_HITS_FILE="${TMP_DIR}/replication_resync_contract_backslide_hits.txt"
STORAGE_REPLICATION_HANDLE_BOUNDARY_BYPASS_HITS_FILE="${TMP_DIR}/storage_replication_handle_boundary_bypass_hits.txt"
ADMIN_REPLICATION_DTO_BOUNDARY_BYPASS_HITS_FILE="${TMP_DIR}/admin_replication_dto_boundary_bypass_hits.txt"
APP_REPLICATION_DTO_BOUNDARY_BYPASS_HITS_FILE="${TMP_DIR}/app_replication_dto_boundary_bypass_hits.txt"
SCANNER_REPLICATION_DTO_BOUNDARY_BYPASS_HITS_FILE="${TMP_DIR}/scanner_replication_dto_boundary_bypass_hits.txt"
OBS_REPLICATION_STATS_BOUNDARY_BYPASS_HITS_FILE="${TMP_DIR}/obs_replication_stats_boundary_bypass_hits.txt"
REPLICATION_BANDWIDTH_BOUNDARY_BYPASS_HITS_FILE="${TMP_DIR}/replication_bandwidth_boundary_bypass_hits.txt"
REPLICATION_CONFIG_STORE_BYPASS_HITS_FILE="${TMP_DIR}/replication_config_store_bypass_hits.txt"
REPLICATION_ERROR_BOUNDARY_BYPASS_HITS_FILE="${TMP_DIR}/replication_error_boundary_bypass_hits.txt"
REPLICATION_EVENT_SINK_BYPASS_HITS_FILE="${TMP_DIR}/replication_event_sink_bypass_hits.txt"
REPLICATION_EVENT_HOST_BYPASS_HITS_FILE="${TMP_DIR}/replication_event_host_bypass_hits.txt"
REPLICATION_FILEMETA_BOUNDARY_BYPASS_HITS_FILE="${TMP_DIR}/replication_filemeta_boundary_bypass_hits.txt"
REPLICATION_LOCAL_PATH_BYPASS_HITS_FILE="${TMP_DIR}/replication_local_path_bypass_hits.txt"
REPLICATION_LOCK_BOUNDARY_BYPASS_HITS_FILE="${TMP_DIR}/replication_lock_boundary_bypass_hits.txt"
REPLICATION_METADATA_BOUNDARY_BYPASS_HITS_FILE="${TMP_DIR}/replication_metadata_boundary_bypass_hits.txt"
REPLICATION_MSGP_BOUNDARY_BYPASS_HITS_FILE="${TMP_DIR}/replication_msgp_boundary_bypass_hits.txt"
REPLICATION_RUNTIME_TYPE_BYPASS_HITS_FILE="${TMP_DIR}/replication_runtime_type_bypass_hits.txt"
REPLICATION_ECSTORE_OWNER_BRIDGE_BYPASS_HITS_FILE="${TMP_DIR}/replication_ecstore_owner_bridge_bypass_hits.txt"
REPLICATION_OBJECT_BRIDGE_BYPASS_HITS_FILE="${TMP_DIR}/replication_object_bridge_bypass_hits.txt"
REPLICATION_SCANNER_BRIDGE_BYPASS_HITS_FILE="${TMP_DIR}/replication_scanner_bridge_bypass_hits.txt"
REPLICATION_STORAGE_BOUNDARY_BYPASS_HITS_FILE="${TMP_DIR}/replication_storage_boundary_bypass_hits.txt"
REPLICATION_TARGET_BOUNDARY_BYPASS_HITS_FILE="${TMP_DIR}/replication_target_boundary_bypass_hits.txt"
REPLICATION_TAGGING_BOUNDARY_BYPASS_HITS_FILE="${TMP_DIR}/replication_tagging_boundary_bypass_hits.txt"
REPLICATION_VERSIONING_BOUNDARY_BYPASS_HITS_FILE="${TMP_DIR}/replication_versioning_boundary_bypass_hits.txt"
REPLICATION_RUNTIME_SOURCE_BYPASS_HITS_FILE="${TMP_DIR}/replication_runtime_source_bypass_hits.txt"
GLOBAL_REPLICATION_STATE_BYPASS_HITS_FILE="${TMP_DIR}/global_replication_state_bypass_hits.txt"
GLOBAL_BUCKET_METADATA_SYS_BYPASS_HITS_FILE="${TMP_DIR}/global_bucket_metadata_sys_bypass_hits.txt"
GLOBAL_BUCKET_TARGET_SYS_BYPASS_HITS_FILE="${TMP_DIR}/global_bucket_target_sys_bypass_hits.txt"
EVENT_DISPATCH_HOOK_BYPASS_HITS_FILE="${TMP_DIR}/event_dispatch_hook_bypass_hits.txt"
DATA_USAGE_MEMORY_GLOBAL_BYPASS_HITS_FILE="${TMP_DIR}/data_usage_memory_global_bypass_hits.txt"
WORKLOAD_ADMISSION_PROVIDER_BYPASS_HITS_FILE="${TMP_DIR}/workload_admission_provider_bypass_hits.txt"
ECSTORE_READ_REPAIR_CACHE_BYPASS_HITS_FILE="${TMP_DIR}/ecstore_read_repair_cache_bypass_hits.txt"
ECSTORE_DISK_COMPRESSION_CACHE_BYPASS_HITS_FILE="${TMP_DIR}/ecstore_disk_compression_cache_bypass_hits.txt"
ECSTORE_ERASURE_CODING_CACHE_BYPASS_HITS_FILE="${TMP_DIR}/ecstore_erasure_coding_cache_bypass_hits.txt"
ECSTORE_SET_DISK_CACHE_BYPASS_HITS_FILE="${TMP_DIR}/ecstore_set_disk_cache_bypass_hits.txt"
ECSTORE_DRIVE_TIMEOUT_CACHE_BYPASS_HITS_FILE="${TMP_DIR}/ecstore_drive_timeout_cache_bypass_hits.txt"
ECSTORE_LIFECYCLE_RECOVERY_GUARD_BYPASS_HITS_FILE="${TMP_DIR}/ecstore_lifecycle_recovery_guard_bypass_hits.txt"
ECSTORE_REMOTE_TIER_DELETE_STATE_BYPASS_HITS_FILE="${TMP_DIR}/ecstore_remote_tier_delete_state_bypass_hits.txt"
RUSTFS_OWNER_LOCAL_STATIC_PUBLIC_HITS_FILE="${TMP_DIR}/rustfs_owner_local_static_public_hits.txt"
RUSTFS_OWNER_LOCAL_STATIC_REEXPORT_HITS_FILE="${TMP_DIR}/rustfs_owner_local_static_reexport_hits.txt"
RUSTFS_ADMIN_TARGET_SPEC_STATIC_PUBLIC_HITS_FILE="${TMP_DIR}/rustfs_admin_target_spec_static_public_hits.txt"
RUSTFS_ADMIN_TARGET_SPEC_STATIC_REEXPORT_HITS_FILE="${TMP_DIR}/rustfs_admin_target_spec_static_reexport_hits.txt"
RUSTFS_STORAGE_CONCURRENCY_STATIC_BYPASS_HITS_FILE="${TMP_DIR}/rustfs_storage_concurrency_static_bypass_hits.txt"
GLOBAL_BUCKET_MONITOR_BYPASS_HITS_FILE="${TMP_DIR}/global_bucket_monitor_bypass_hits.txt"
GLOBAL_ENDPOINTS_BYPASS_HITS_FILE="${TMP_DIR}/global_endpoints_bypass_hits.txt"
GLOBAL_IS_ERASURE_BYPASS_HITS_FILE="${TMP_DIR}/global_is_erasure_bypass_hits.txt"
GLOBAL_IS_DIST_ERASURE_BYPASS_HITS_FILE="${TMP_DIR}/global_is_dist_erasure_bypass_hits.txt"
GLOBAL_LOCAL_DISK_MAP_BYPASS_HITS_FILE="${TMP_DIR}/global_local_disk_map_bypass_hits.txt"
GLOBAL_LOCAL_DISK_ID_MAP_BYPASS_HITS_FILE="${TMP_DIR}/global_local_disk_id_map_bypass_hits.txt"
GLOBAL_LOCAL_DISK_SET_DRIVES_BYPASS_HITS_FILE="${TMP_DIR}/global_local_disk_set_drives_bypass_hits.txt"
GLOBAL_ERASURE_SD_BYPASS_HITS_FILE="${TMP_DIR}/global_erasure_sd_bypass_hits.txt"
GLOBAL_ROOT_DISK_THRESHOLD_BYPASS_HITS_FILE="${TMP_DIR}/global_root_disk_threshold_bypass_hits.txt"
GLOBAL_LIFECYCLE_STATE_BYPASS_HITS_FILE="${TMP_DIR}/global_lifecycle_state_bypass_hits.txt"
GLOBAL_LIFECYCLE_SYS_BYPASS_HITS_FILE="${TMP_DIR}/global_lifecycle_sys_bypass_hits.txt"
GLOBAL_EVENT_NOTIFIER_BYPASS_HITS_FILE="${TMP_DIR}/global_event_notifier_bypass_hits.txt"
GLOBAL_NOTIFICATION_SYS_BYPASS_HITS_FILE="${TMP_DIR}/global_notification_sys_bypass_hits.txt"
GLOBAL_BOOT_TIME_BYPASS_HITS_FILE="${TMP_DIR}/global_boot_time_bypass_hits.txt"
GLOBAL_ECSTORE_LOCAL_NODE_NAME_BYPASS_HITS_FILE="${TMP_DIR}/global_ecstore_local_node_name_bypass_hits.txt"
GLOBAL_RUNTIME_SCALAR_BYPASS_HITS_FILE="${TMP_DIR}/global_runtime_scalar_bypass_hits.txt"
GLOBAL_BACKGROUND_CANCEL_BYPASS_HITS_FILE="${TMP_DIR}/global_background_cancel_bypass_hits.txt"
AUDIT_SYSTEM_BYPASS_HITS_FILE="${TMP_DIR}/audit_system_bypass_hits.txt"
HEAL_OWNER_GLOBAL_BYPASS_HITS_FILE="${TMP_DIR}/heal_owner_global_bypass_hits.txt"
GLOBAL_LOCK_CLIENTS_BYPASS_HITS_FILE="${TMP_DIR}/global_lock_clients_bypass_hits.txt"
GLOBAL_BATCH_PROCESSORS_BYPASS_HITS_FILE="${TMP_DIR}/global_batch_processors_bypass_hits.txt"
INTERNODE_DATA_TRANSPORT_BYPASS_HITS_FILE="${TMP_DIR}/internode_data_transport_bypass_hits.txt"
GLOBAL_CAPACITY_MANAGER_BYPASS_HITS_FILE="${TMP_DIR}/global_capacity_manager_bypass_hits.txt"
GLOBAL_CONN_MAP_BYPASS_HITS_FILE="${TMP_DIR}/global_conn_map_bypass_hits.txt"
GLOBAL_LOCAL_NODE_NAME_BYPASS_HITS_FILE="${TMP_DIR}/global_local_node_name_bypass_hits.txt"
GLOBAL_RUSTFS_ADDR_BYPASS_HITS_FILE="${TMP_DIR}/global_rustfs_addr_bypass_hits.txt"
GLOBAL_OUTBOUND_TLS_BYPASS_HITS_FILE="${TMP_DIR}/global_outbound_tls_bypass_hits.txt"
GLOBAL_RPC_SECRET_BYPASS_HITS_FILE="${TMP_DIR}/global_rpc_secret_bypass_hits.txt"
GLOBAL_KMS_SERVICE_MANAGER_BYPASS_HITS_FILE="${TMP_DIR}/global_kms_service_manager_bypass_hits.txt"
GLOBAL_TIER_CONFIG_MGR_BYPASS_HITS_FILE="${TMP_DIR}/global_tier_config_mgr_bypass_hits.txt"
LEGACY_ECSTORE_CONFIG_MODEL_HITS_FILE="${TMP_DIR}/legacy_ecstore_config_model_hits.txt"
ECSTORE_ROOT_STORE_SET_DISK_MODULE_HITS_FILE="${TMP_DIR}/ecstore_root_store_set_disk_module_hits.txt"
ECSTORE_ROOT_STORE_SUPPORT_MODULE_HITS_FILE="${TMP_DIR}/ecstore_root_store_support_module_hits.txt"
ECSTORE_ROOT_LAYOUT_CONTRACT_SUPPORT_MODULE_HITS_FILE="${TMP_DIR}/ecstore_root_layout_contract_support_module_hits.txt"
ECSTORE_ROOT_SERVICE_RUNTIME_MODULE_HITS_FILE="${TMP_DIR}/ecstore_root_service_runtime_module_hits.txt"
ECSTORE_ROOT_DATA_MOVEMENT_MODULE_HITS_FILE="${TMP_DIR}/ecstore_root_data_movement_module_hits.txt"
ECSTORE_ROOT_USAGE_DIAGNOSTICS_MODULE_HITS_FILE="${TMP_DIR}/ecstore_root_usage_diagnostics_module_hits.txt"
ECSTORE_ROOT_RUNTIME_GLOBAL_MODULE_HITS_FILE="${TMP_DIR}/ecstore_root_runtime_global_module_hits.txt"
ECSTORE_ROOT_IO_SUPPORT_MODULE_HITS_FILE="${TMP_DIR}/ecstore_root_io_support_module_hits.txt"
ECSTORE_ROOT_ERASURE_MODULE_HITS_FILE="${TMP_DIR}/ecstore_root_erasure_module_hits.txt"
ECSTORE_ROOT_ERROR_REBALANCE_MODULE_HITS_FILE="${TMP_DIR}/ecstore_root_error_rebalance_module_hits.txt"
ECSTORE_ROOT_SERVICE_DOMAIN_MODULE_HITS_FILE="${TMP_DIR}/ecstore_root_service_domain_module_hits.txt"
ECSTORE_ROOT_SERVICE_DOMAIN_IMPL_HITS_FILE="${TMP_DIR}/ecstore_root_service_domain_impl_hits.txt"
ECSTORE_ROOT_CORE_RUNTIME_MODULE_HITS_FILE="${TMP_DIR}/ecstore_root_core_runtime_module_hits.txt"
ECSTORE_CLUSTER_ROOT_IMPL_HITS_FILE="${TMP_DIR}/ecstore_cluster_root_impl_hits.txt"
ECSTORE_ROOT_RPC_SUPPORT_MODULE_HITS_FILE="${TMP_DIR}/ecstore_root_rpc_support_module_hits.txt"
ECSTORE_ROOT_RPC_IMPL_HITS_FILE="${TMP_DIR}/ecstore_root_rpc_impl_hits.txt"
ECSTORE_OLD_METADATA_OWNER_PATH_HITS_FILE="${TMP_DIR}/ecstore_old_metadata_owner_path_hits.txt"
ECSTORE_ROOT_OWNER_PATH_SHIM_HITS_FILE="${TMP_DIR}/ecstore_root_owner_path_shim_hits.txt"
ALL_STORAGE_COMPAT_SELF_FACADE_PATH_HITS_FILE="${TMP_DIR}/all_storage_compat_self_facade_path_hits.txt"
RUSTFS_LOCAL_COMPAT_OWNER_SELF_PATH_HITS_FILE="${TMP_DIR}/rustfs_local_compat_owner_self_path_hits.txt"
RUSTFS_ROOT_COMPAT_RELATIVE_CONSUMER_HITS_FILE="${TMP_DIR}/rustfs_root_compat_relative_consumer_hits.txt"
RUSTFS_STORAGE_CORE_COMPAT_RELATIVE_CONSUMER_HITS_FILE="${TMP_DIR}/rustfs_storage_core_compat_relative_consumer_hits.txt"
RUSTFS_LOCAL_COMPAT_RELATIVE_CONSUMER_HITS_FILE="${TMP_DIR}/rustfs_local_compat_relative_consumer_hits.txt"
RUSTFS_APP_ADMIN_SECONDARY_COMPAT_BRIDGE_HITS_FILE="${TMP_DIR}/rustfs_app_admin_secondary_compat_bridge_hits.txt"
RUSTFS_STORAGE_SECONDARY_COMPAT_BRIDGE_HITS_FILE="${TMP_DIR}/rustfs_storage_secondary_compat_bridge_hits.txt"
RUSTFS_NESTED_SECONDARY_COMPAT_BRIDGE_HITS_FILE="${TMP_DIR}/rustfs_nested_secondary_compat_bridge_hits.txt"
RUSTFS_STORAGE_LOCAL_COMPAT_RELATIVE_CONSUMER_HITS_FILE="${TMP_DIR}/rustfs_storage_local_compat_relative_consumer_hits.txt"
RUSTFS_RUNTIME_LOCAL_COMPAT_BRIDGE_HITS_FILE="${TMP_DIR}/rustfs_runtime_local_compat_bridge_hits.txt"
RUSTFS_ROOT_ONE_OFF_COMPAT_BRIDGE_HITS_FILE="${TMP_DIR}/rustfs_root_one_off_compat_bridge_hits.txt"
RUSTFS_STARTUP_COMPAT_BRIDGE_HITS_FILE="${TMP_DIR}/rustfs_startup_compat_bridge_hits.txt"
RUSTFS_ADMIN_LOCAL_COMPAT_RELATIVE_CONSUMER_HITS_FILE="${TMP_DIR}/rustfs_admin_local_compat_relative_consumer_hits.txt"
RUSTFS_APP_SERVER_LOCAL_COMPAT_RELATIVE_CONSUMER_HITS_FILE="${TMP_DIR}/rustfs_app_server_local_compat_relative_consumer_hits.txt"
RUSTFS_HEAL_TEST_LOCAL_COMPAT_RELATIVE_CONSUMER_HITS_FILE="${TMP_DIR}/rustfs_heal_test_local_compat_relative_consumer_hits.txt"
STANDALONE_CRATE_LOCAL_COMPAT_RELATIVE_CONSUMER_HITS_FILE="${TMP_DIR}/standalone_crate_local_compat_relative_consumer_hits.txt"
SCANNER_BUCKET_STORAGE_COMPAT_MODULE_HITS_FILE="${TMP_DIR}/scanner_bucket_storage_compat_module_hits.txt"
SCANNER_STORAGE_API_SOURCE_BYPASS_HITS_FILE="${TMP_DIR}/scanner_storage_api_source_bypass_hits.txt"
SCANNER_STORAGE_API_TEST_BYPASS_HITS_FILE="${TMP_DIR}/scanner_storage_api_test_bypass_hits.txt"
SCANNER_EXPIRY_STATE_BYPASS_HITS_FILE="${TMP_DIR}/scanner_expiry_state_bypass_hits.txt"
HEAL_STORAGE_API_SOURCE_BYPASS_HITS_FILE="${TMP_DIR}/heal_storage_api_source_bypass_hits.txt"
HEAL_STORAGE_API_TEST_BYPASS_HITS_FILE="${TMP_DIR}/heal_storage_api_test_bypass_hits.txt"
OBS_STORAGE_API_SOURCE_BYPASS_HITS_FILE="${TMP_DIR}/obs_storage_api_source_bypass_hits.txt"
NOTIFY_STORAGE_COMPAT_MODULE_HITS_FILE="${TMP_DIR}/notify_storage_compat_module_hits.txt"
OBS_STORAGE_COMPAT_PASSTHROUGH_HITS_FILE="${TMP_DIR}/obs_storage_compat_passthrough_hits.txt"
E2E_STORAGE_COMPAT_RPC_PASSTHROUGH_HITS_FILE="${TMP_DIR}/e2e_storage_compat_rpc_passthrough_hits.txt"
TEST_STORAGE_COMPAT_PASSTHROUGH_HITS_FILE="${TMP_DIR}/test_storage_compat_passthrough_hits.txt"
TEST_FUZZ_COMPAT_BRIDGE_HITS_FILE="${TMP_DIR}/test_fuzz_compat_bridge_hits.txt"
STANDALONE_THIN_COMPAT_BRIDGE_HITS_FILE="${TMP_DIR}/standalone_thin_compat_bridge_hits.txt"
APP_NOTIFY_THIN_COMPAT_BRIDGE_HITS_FILE="${TMP_DIR}/app_notify_thin_compat_bridge_hits.txt"
EXTERNAL_OWNER_COMPAT_BRIDGE_HITS_FILE="${TMP_DIR}/external_owner_compat_bridge_hits.txt"
PRODUCTION_UNUSED_COMPAT_ALLOW_HITS_FILE="${TMP_DIR}/production_unused_compat_allow_hits.txt"
BROAD_STORE_API_COMPAT_REEXPORT_HITS_FILE="${TMP_DIR}/broad_store_api_compat_reexport_hits.txt"
NESTED_STORE_API_COMPAT_MODULE_HITS_FILE="${TMP_DIR}/nested_store_api_compat_module_hits.txt"
UNAPPROVED_STORE_API_COMPAT_ALIAS_HITS_FILE="${TMP_DIR}/unapproved_store_api_compat_alias_hits.txt"
PUBLIC_STORE_API_MODULE_HITS_FILE="${TMP_DIR}/public_store_api_module_hits.txt"
ECSTORE_PUBLIC_ROOT_MODULE_HITS_FILE="${TMP_DIR}/ecstore_public_root_module_hits.txt"
ECSTORE_PUBLIC_GLOBAL_REEXPORT_HITS_FILE="${TMP_DIR}/ecstore_public_global_reexport_hits.txt"
STORE_API_MODULE_PATH_HITS_FILE="${TMP_DIR}/store_api_module_path_hits.txt"
ECSTORE_COMPAT_PASSTHROUGH_EXPECTED_FILE="${TMP_DIR}/ecstore_compat_passthrough_expected.txt"
ECSTORE_COMPAT_PASSTHROUGH_ACTUAL_FILE="${TMP_DIR}/ecstore_compat_passthrough_actual.txt"
ECSTORE_COMPAT_PASSTHROUGH_DIFF_FILE="${TMP_DIR}/ecstore_compat_passthrough_diff.txt"
RUSTFS_WORKLOAD_DIRECT_FOREGROUND_MAPPING_HITS_FILE="${TMP_DIR}/rustfs_workload_direct_foreground_mapping_hits.txt"
IAM_RUNTIME_SOURCE_BYPASS_HITS_FILE="${TMP_DIR}/iam_runtime_source_bypass_hits.txt"
RUSTFS_APP_CONTEXT_RUNTIME_SOURCE_BYPASS_HITS_FILE="${TMP_DIR}/rustfs_app_context_runtime_source_bypass_hits.txt"
RUSTFS_APP_RUNTIME_STORAGE_API_BYPASS_HITS_FILE="${TMP_DIR}/rustfs_app_runtime_storage_api_bypass_hits.txt"
RUSTFS_STARTUP_RUNTIME_SOURCE_BYPASS_HITS_FILE="${TMP_DIR}/rustfs_startup_runtime_source_bypass_hits.txt"
RUSTFS_STARTUP_APP_CONTEXT_ENTRYPOINT_HITS_FILE="${TMP_DIR}/rustfs_startup_app_context_entrypoint_hits.txt"
RUSTFS_SERVER_RUNTIME_SOURCE_BYPASS_HITS_FILE="${TMP_DIR}/rustfs_server_runtime_source_bypass_hits.txt"
RUSTFS_SERVER_APP_CONTEXT_ENTRYPOINT_HITS_FILE="${TMP_DIR}/rustfs_server_app_context_entrypoint_hits.txt"
RUSTFS_OWNER_RUNTIME_APP_CONTEXT_ENTRYPOINT_HITS_FILE="${TMP_DIR}/rustfs_owner_runtime_app_context_entrypoint_hits.txt"
RUSTFS_STORAGE_RUNTIME_SOURCE_BYPASS_HITS_FILE="${TMP_DIR}/rustfs_storage_runtime_source_bypass_hits.txt"
RUSTFS_STORAGE_ECFS_USECASE_BYPASS_HITS_FILE="${TMP_DIR}/rustfs_storage_ecfs_usecase_bypass_hits.txt"
RUSTFS_STORAGE_GLOBAL_APP_CONTEXT_REEXPORT_HITS_FILE="${TMP_DIR}/rustfs_storage_global_app_context_reexport_hits.txt"
RUSTFS_ADMIN_RUNTIME_SOURCE_BYPASS_HITS_FILE="${TMP_DIR}/rustfs_admin_runtime_source_bypass_hits.txt"
RUSTFS_APP_CONTEXT_DIRECT_BYPASS_HITS_FILE="${TMP_DIR}/rustfs_app_context_direct_bypass_hits.txt"
RUSTFS_APP_USECASE_RUNTIME_SOURCE_BYPASS_HITS_FILE="${TMP_DIR}/rustfs_app_usecase_runtime_source_bypass_hits.txt"
RUSTFS_APP_USECASE_STORAGE_WILDCARD_HITS_FILE="${TMP_DIR}/rustfs_app_usecase_storage_wildcard_hits.txt"
RUSTFS_APP_USECASE_CONTRACT_ROOT_CONSUMER_HITS_FILE="${TMP_DIR}/rustfs_app_usecase_contract_root_consumer_hits.txt"
RUSTFS_APP_WILDCARD_IMPORT_HITS_FILE="${TMP_DIR}/rustfs_app_wildcard_import_hits.txt"
RUSTFS_APP_USECASE_S3_API_BYPASS_HITS_FILE="${TMP_DIR}/rustfs_app_usecase_s3_api_bypass_hits.txt"
RUSTFS_APP_USECASE_STORAGE_API_BYPASS_HITS_FILE="${TMP_DIR}/rustfs_app_usecase_storage_api_bypass_hits.txt"
RUSTFS_APP_ADMIN_STORAGE_API_BYPASS_HITS_FILE="${TMP_DIR}/rustfs_app_admin_storage_api_bypass_hits.txt"
RUSTFS_ADMIN_STORAGE_API_ROOT_FACADE_HITS_FILE="${TMP_DIR}/rustfs_admin_storage_api_root_facade_hits.txt"
RUSTFS_ROOT_STORAGE_API_BYPASS_HITS_FILE="${TMP_DIR}/rustfs_root_storage_api_bypass_hits.txt"
RUSTFS_ROOT_STORAGE_API_CONTRACT_BYPASS_HITS_FILE="${TMP_DIR}/rustfs_root_storage_api_contract_bypass_hits.txt"
RUSTFS_ROOT_STORAGE_API_DOMAIN_BYPASS_HITS_FILE="${TMP_DIR}/rustfs_root_storage_api_domain_bypass_hits.txt"
RUSTFS_ROOT_STORAGE_CONTRACT_ROOT_CONSUMER_HITS_FILE="${TMP_DIR}/rustfs_root_storage_contract_root_consumer_hits.txt"
RUSTFS_ROOT_RUNTIME_FACADE_ROOT_CONSUMER_HITS_FILE="${TMP_DIR}/rustfs_root_runtime_facade_root_consumer_hits.txt"
RUSTFS_STORAGE_OWNER_ROOT_FACADE_CONSUMER_HITS_FILE="${TMP_DIR}/rustfs_storage_owner_root_facade_consumer_hits.txt"
RUSTFS_STORAGE_OWNER_RUNTIME_ROOT_CONSUMER_HITS_FILE="${TMP_DIR}/rustfs_storage_owner_runtime_root_consumer_hits.txt"
RUSTFS_STORAGE_OWNER_HELPER_ROOT_CONSUMER_HITS_FILE="${TMP_DIR}/rustfs_storage_owner_helper_root_consumer_hits.txt"
RUSTFS_STORAGE_OWNER_RPC_ROOT_CONSUMER_HITS_FILE="${TMP_DIR}/rustfs_storage_owner_rpc_root_consumer_hits.txt"
RUSTFS_STORAGE_OWNER_WILDCARD_IMPORT_HITS_FILE="${TMP_DIR}/rustfs_storage_owner_wildcard_import_hits.txt"
RUSTFS_STORAGE_OWNER_ROOT_EXPORT_GLOB_HITS_FILE="${TMP_DIR}/rustfs_storage_owner_root_export_glob_hits.txt"
RUSTFS_STORAGE_OWNER_RUNTIME_ROOT_EXPORT_HITS_FILE="${TMP_DIR}/rustfs_storage_owner_runtime_root_export_hits.txt"
RUSTFS_OBJECT_LAYER_FALLBACK_BYPASS_HITS_FILE="${TMP_DIR}/rustfs_object_layer_fallback_bypass_hits.txt"
RUSTFS_STORAGE_FACADE_DIRECT_OWNER_BYPASS_HITS_FILE="${TMP_DIR}/rustfs_storage_facade_direct_owner_bypass_hits.txt"
RUSTFS_STORAGE_OWNER_SSE_ROOT_EXPORT_HITS_FILE="${TMP_DIR}/rustfs_storage_owner_sse_root_export_hits.txt"
RUSTFS_STORAGE_OWNER_TEST_ROOT_CONSUMER_HITS_FILE="${TMP_DIR}/rustfs_storage_owner_test_root_consumer_hits.txt"
RUSTFS_ADMIN_STORAGE_API_DOMAIN_BYPASS_HITS_FILE="${TMP_DIR}/rustfs_admin_storage_api_domain_bypass_hits.txt"
RUSTFS_APP_STORAGE_API_DOMAIN_BYPASS_HITS_FILE="${TMP_DIR}/rustfs_app_storage_api_domain_bypass_hits.txt"
RUSTFS_APP_STORAGE_API_CONTRACT_BYPASS_HITS_FILE="${TMP_DIR}/rustfs_app_storage_api_contract_bypass_hits.txt"
RUSTFS_ADMIN_STORAGE_API_CONTRACT_BYPASS_HITS_FILE="${TMP_DIR}/rustfs_admin_storage_api_contract_bypass_hits.txt"
RUSTFS_STORAGE_DIRECT_APP_CONTEXT_BYPASS_HITS_FILE="${TMP_DIR}/rustfs_storage_direct_app_context_bypass_hits.txt"

awk '
  /^## PR Types$/ {
    in_section = 1
    next
  }
  in_section && /^## / {
    exit
  }
  in_section && /^- `/ {
    line = $0
    sub(/^- `/, "", line)
    sub(/`.*/, "", line)
    print line
  }
' "$BOUNDARY_DOC" | sort >"$PR_TYPES_FILE"

if [[ ! -s "$PR_TYPES_FILE" ]]; then
  report_failure "no PR types found in docs/architecture/crate-boundaries.md"
fi

while IFS= read -r pr_type; do
  if [[ ! "$pr_type" =~ ^[a-z][a-z0-9]*(-[a-z0-9]+)*$ ]]; then
    report_failure "invalid PR type spelling in crate-boundaries.md: ${pr_type}"
  fi
done <"$PR_TYPES_FILE"

if duplicates="$(uniq -d "$PR_TYPES_FILE")" && [[ -n "$duplicates" ]]; then
  report_failure "duplicate PR types in crate-boundaries.md: ${duplicates//$'\n'/, }"
fi

(
  cd "$ROOT_DIR"
  {
    printf '%s\0' ARCHITECTURE.md
    find docs/architecture -type f -name '*.md' -print0
  } | xargs -0 perl -ne '
    if (!defined $current_file || $ARGV ne $current_file) {
      $current_file = $ARGV;
      $line = 0;
      $in_next_prs = 0;
    }
    $line++;

    if (/^##\s+Next PRs\b/) {
      $in_next_prs = 1;
    } elsif (/^##\s+/) {
      $in_next_prs = 0;
    }

    if ($in_next_prs || /\bPR(?:s| type| Types)?\b/) {
      while (/`([a-z][a-z0-9]*(?:-[a-z0-9]+)*)`/g) {
        print "$ARGV:$line:$1\n";
      }
    }
  '
) >"$PR_TYPE_HITS_FILE"

while IFS=: read -r file line token; do
  [[ -z "${token:-}" ]] && continue
  if ! contains_line "$token" "$PR_TYPES_FILE"; then
    report_failure "${file}:${line} references unknown PR type '${token}'"
  fi
done <"$PR_TYPE_HITS_FILE"

(
  cd "$ROOT_DIR"
  rg -n --no-heading \
    'RUSTFS_COMPAT_TODO\([A-Za-z0-9][A-Za-z0-9_-]*\)' \
    --glob '!docs/**' \
    --glob '!target/**' \
    --glob '!scripts/check_architecture_migration_rules.sh' \
    . || true
) >"$SOURCE_MARKERS_FILE"

if [[ -s "$SOURCE_MARKERS_FILE" ]]; then
  while IFS= read -r hit; do
    if [[ "$hit" != *"Remove after "* ]]; then
      report_failure "compat marker must state a removal condition: ${hit}"
    fi
  done <"$SOURCE_MARKERS_FILE"
fi

sed -E 's/.*RUSTFS_COMPAT_TODO\(([A-Za-z0-9][A-Za-z0-9_-]*)\).*/\1/' "$SOURCE_MARKERS_FILE" |
  sort -u >"$SOURCE_IDS_FILE"

awk '
  /^## Open Items$/ {
    in_section = 1
    next
  }
  in_section && /^## / {
    exit
  }
  in_section {
    print
  }
' "$REGISTER_DOC" |
  perl -ne '
    while (/RUSTFS_COMPAT_TODO\(([A-Za-z0-9][A-Za-z0-9_-]*)\)|`([A-Za-z0-9][A-Za-z0-9_-]*)`/g) {
      my $id = defined $1 ? $1 : $2;
      print "$id\n" unless $id eq "task-id";
    }
  ' |
  sort -u >"$REGISTER_IDS_FILE"

if [[ -s "$SOURCE_IDS_FILE" ]] && awk '/^## Open Items$/ { in_section = 1; next } in_section && /^## / { exit } in_section && /No compatibility code/ { found = 1 } END { exit found ? 0 : 1 }' "$REGISTER_DOC"; then
  report_failure "compat cleanup register still says no compatibility code while source markers exist"
fi

while IFS= read -r source_id; do
  [[ -z "$source_id" ]] && continue
  if ! contains_line "$source_id" "$REGISTER_IDS_FILE"; then
    report_failure "source marker RUSTFS_COMPAT_TODO(${source_id}) has no cleanup-register entry"
  fi
done <"$SOURCE_IDS_FILE"

while IFS= read -r register_id; do
  [[ -z "$register_id" ]] && continue
  if ! contains_line "$register_id" "$SOURCE_IDS_FILE"; then
    report_failure "cleanup-register entry ${register_id} has no source marker"
  fi
done <"$REGISTER_IDS_FILE"

require_source_line \
  "crates/storage-api/src/lib.rs" \
  "pub use admin::{DiskSetSelector, StorageAdminApi};" \
  "storage-api public admin contract re-export"
require_source_line \
  "rustfs/src/lib.rs" \
  "pub mod startup_entrypoint;" \
  "startup process entrypoint public module"
require_source_line \
  "rustfs/src/lib.rs" \
  "pub(crate) mod startup_iam;" \
  "startup IAM shim crate-private module"
require_source_line \
  "crates/ecstore/src/lib.rs" \
  "mod core;" \
  "ECStore core owner module crate-private visibility"
require_source_line \
  "crates/ecstore/src/lib.rs" \
  "mod data_movement;" \
  "ECStore data movement owner module crate-private visibility"
require_source_line \
  "crates/ecstore/src/lib.rs" \
  "mod erasure;" \
  "ECStore erasure owner module crate-private visibility"
require_source_line \
  "crates/ecstore/src/lib.rs" \
  "mod cluster;" \
  "ECStore cluster control-plane owner module crate-private visibility"
require_source_line \
  "crates/ecstore/src/lib.rs" \
  "pub(crate) mod layout;" \
  "ECStore layout owner module crate-private visibility"
require_source_line \
  "crates/ecstore/src/api/mod.rs" \
  "pub mod cluster {" \
  "ECStore cluster control-plane public facade"
for ecstore_private_module in \
  bucket \
  cache_value \
  client \
  config \
  data_usage \
  diagnostics \
  disk \
  error \
  io_support \
  runtime \
  services \
  set_disk \
  store; do
  require_source_line \
    "crates/ecstore/src/lib.rs" \
    "mod ${ecstore_private_module};" \
    "ECStore legacy ${ecstore_private_module} root module crate-private visibility"
done
require_source_line \
  "crates/storage-api/src/lib.rs" \
  "pub use bucket::{BucketInfo, BucketOperations, BucketOptions, DeleteBucketOptions, MakeBucketOptions, SRBucketDeleteOp};" \
  "storage-api public bucket contract re-export"
require_source_line \
  "crates/storage-api/src/lib.rs" \
  "pub use capability::{CapabilitySnapshotError, CapabilityState, CapabilityStatus};" \
  "storage-api public capability contract re-export"
require_source_contains \
  "crates/concurrency/src/lib.rs" \
  "pub use workload::{" \
  "concurrency public workload admission contract re-export"
require_source_contains \
  "crates/concurrency/src/lib.rs" \
  "AdmissionState, WorkloadAdmissionRegistrySnapshot, WorkloadAdmissionSnapshot, WorkloadAdmissionSnapshotProvider," \
  "concurrency public workload admission contract status re-exports"
require_source_contains \
  "crates/concurrency/src/lib.rs" \
  "WorkloadClass," \
  "concurrency public workload class re-export"
require_source_contains \
  "rustfs/src/storage/concurrency/manager.rs" \
  "impl WorkloadAdmissionSnapshotProvider for ConcurrencyManager" \
  "storage concurrency workload admission snapshot provider"
require_source_contains \
  "rustfs/src/storage/concurrency/manager.rs" \
  "pub fn workload_admission_registry_snapshot(&self) -> WorkloadAdmissionRegistrySnapshot" \
  "storage concurrency workload admission registry snapshot"
require_source_contains \
  "rustfs/src/storage/concurrency/manager.rs" \
  "WorkloadClass::ForegroundRead => self.get_object_admission_snapshot()" \
  "storage concurrency foreground read admission snapshot"
require_source_line \
  "crates/storage-api/src/lib.rs" \
  "pub use multipart::{CompletePart, ListMultipartsInfo, ListPartsInfo, MultipartInfo, MultipartUploadResult, PartInfo};" \
  "storage-api public multipart DTO re-export"
require_source_line \
  "crates/storage-api/src/lib.rs" \
  "pub use observability::{" \
  "storage-api public observability contract re-export"
require_source_line \
  "crates/storage-api/src/lib.rs" \
  "pub use object::{HTTPPreconditions, HTTPRangeError, HTTPRangeSpec, ObjectLockRetentionOptions};" \
  "storage-api public object helper contract re-export"
require_source_line \
  "crates/storage-api/src/lib.rs" \
  "pub use object::{DeletedObject, ObjectToDelete};" \
  "storage-api public delete object contract re-export"
require_source_line \
  "crates/storage-api/src/lib.rs" \
  "pub use object::{ExpirationOptions, TransitionedObject};" \
  "storage-api public lifecycle helper contract re-export"
require_source_line \
  "crates/storage-api/src/lib.rs" \
  "pub use object::{ListObjectVersionsInfo, ListObjectsInfo, ListObjectsV2Info, ListOperations, ObjectInfoOrErr};" \
  "storage-api public list response contract re-export"
require_source_line \
  "crates/storage-api/src/lib.rs" \
  "pub use object::{HealOperations, MultipartOperations, NamespaceLocking, ObjectIO, ObjectOperations};" \
  "storage-api public object operation contract re-export"
require_source_line \
  "crates/storage-api/src/lib.rs" \
  "pub use object::{ObjectPreconditionError, ObjectPreconditionPart, ObjectPreconditionState};" \
  "storage-api public object precondition contract re-export"
require_source_line \
  "crates/storage-api/src/lib.rs" \
  "pub use object::{VersionMarker, WalkOptions, WalkVersionsSortOrder};" \
  "storage-api public list helper contract re-export"
require_source_line \
  "crates/storage-api/src/lib.rs" \
  "pub use error::{StorageErrorCode, StorageResult};" \
  "storage-api public error contract re-export"
require_source_line \
  "crates/storage-api/src/lib.rs" \
  "pub use topology::{" \
  "storage-api public topology contract re-export"
require_source_line \
  "rustfs/src/lib.rs" \
  "pub mod runtime_capabilities;" \
  "RustFS runtime capability provider module"
require_source_line \
  "rustfs/src/lib.rs" \
  "pub mod workload_admission;" \
  "RustFS workload admission provider module"
require_source_contains \
  "rustfs/src/runtime_capabilities.rs" \
  "impl ObservabilitySnapshotProvider for RustFsObservabilitySnapshotProvider" \
  "RustFS observability snapshot provider implementation"
require_source_contains \
  "rustfs/src/runtime_capabilities.rs" \
  "impl TopologySnapshotProvider for EndpointTopologySnapshotProvider" \
  "RustFS endpoint topology snapshot provider implementation"
require_source_contains \
  "rustfs/src/runtime_capabilities.rs" \
  "pub fn topology_snapshot_from_endpoint_pools(endpoint_pools: &EndpointServerPools) -> TopologySnapshot" \
  "RustFS endpoint topology snapshot helper"
require_source_contains \
  "rustfs/src/workload_admission.rs" \
  "impl WorkloadAdmissionSnapshotProvider for RustFsWorkloadAdmissionSnapshotProvider" \
  "RustFS workload admission snapshot provider implementation"
require_source_contains \
  "rustfs/src/workload_admission.rs" \
  "storage_concurrency_workload_admission_snapshot().overlay(runtime_owner_workload_admission_registry_snapshot())" \
  "RustFS workload admission provider composition"
require_source_contains \
  "rustfs/src/workload_admission.rs" \
  "let storage_provider: &dyn WorkloadAdmissionSnapshotProvider = get_concurrency_manager();" \
  "RustFS workload admission storage provider boundary"
require_source_contains \
  "rustfs/src/workload_admission.rs" \
  "pub fn foreground_read_workload_admission_snapshot() -> WorkloadAdmissionSnapshot" \
  "RustFS foreground-read workload admission snapshot helper"
require_source_contains \
  "rustfs/src/workload_admission.rs" \
  "pub fn metadata_workload_admission_snapshot() -> WorkloadAdmissionSnapshot" \
  "RustFS metadata workload admission snapshot helper"
require_source_contains \
  "rustfs/src/workload_admission.rs" \
  "metadata_workload_admission_snapshot()," \
  "RustFS metadata workload admission class mapping"
require_source_contains \
  "rustfs/src/workload_admission.rs" \
  "pub fn scanner_workload_admission_snapshot() -> WorkloadAdmissionSnapshot" \
  "RustFS scanner workload admission snapshot helper"
require_source_contains \
  "rustfs/src/workload_admission.rs" \
  "scanner_workload_admission_snapshot()," \
  "RustFS scanner workload admission class mapping"
require_source_contains \
  "rustfs/src/workload_admission.rs" \
  "pub fn repair_workload_admission_snapshot() -> WorkloadAdmissionSnapshot" \
  "RustFS repair workload admission snapshot helper"
require_source_contains \
  "rustfs/src/workload_admission.rs" \
  "repair_workload_admission_snapshot()," \
  "RustFS repair workload admission class mapping"
require_source_contains \
  "rustfs/src/workload_admission.rs" \
  "pub fn replication_workload_admission_snapshot() -> WorkloadAdmissionSnapshot" \
  "RustFS replication workload admission snapshot helper"
require_source_contains \
  "rustfs/src/workload_admission.rs" \
  "replication_workload_admission_snapshot()," \
  "RustFS replication workload admission class mapping"

(
  cd "$ROOT_DIR"
  rg -n --no-heading 'WorkloadClass::ForegroundRead => foreground_read_workload_admission_snapshot\(\)' \
    rustfs/src/workload_admission.rs || true
) >"$RUSTFS_WORKLOAD_DIRECT_FOREGROUND_MAPPING_HITS_FILE"

if [[ -s "$RUSTFS_WORKLOAD_DIRECT_FOREGROUND_MAPPING_HITS_FILE" ]]; then
  report_failure "RustFS workload admission foreground reads must be composed through the storage provider registry: $(paste -sd '; ' "$RUSTFS_WORKLOAD_DIRECT_FOREGROUND_MAPPING_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --no-heading '\bStorageAPI\b' crates/ecstore/src rustfs/src || true
) >"$LEGACY_STORAGE_API_HITS_FILE"

if [[ -s "$LEGACY_STORAGE_API_HITS_FILE" ]]; then
  report_failure "old StorageAPI facade identifier reintroduced in production source: $(paste -sd '; ' "$LEGACY_STORAGE_API_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --no-heading 'pub(?:\(crate\))? use rustfs_storage_api::\{[^}]*\b(?:BucketInfo|BucketOptions|DeleteBucketOptions|MakeBucketOptions|SRBucketDeleteOp)\b' \
    crates/ecstore/src/store_api.rs 2>/dev/null || true
) >"$STORE_API_BUCKET_DTO_REEXPORTS_FILE"

if [[ -s "$STORE_API_BUCKET_DTO_REEXPORTS_FILE" ]]; then
  report_failure "old ecstore store_api bucket DTO re-export reintroduced: $(paste -sd '; ' "$STORE_API_BUCKET_DTO_REEXPORTS_FILE")"
fi

(
  cd "$ROOT_DIR"
  store_api_bucket_operation_scan_targets=()
  if [[ -f crates/ecstore/src/store_api.rs ]]; then
    store_api_bucket_operation_scan_targets+=(crates/ecstore/src/store_api.rs)
  fi
  if [[ -f crates/ecstore/src/store_api/traits.rs ]]; then
    store_api_bucket_operation_scan_targets+=(crates/ecstore/src/store_api/traits.rs)
  fi
  if (( ${#store_api_bucket_operation_scan_targets[@]} > 0 )); then
    rg -n --no-heading 'pub(?:\(crate\))? use rustfs_storage_api::\{[^}]*\bBucketOperations\b|pub trait BucketOperations\b' \
      "${store_api_bucket_operation_scan_targets[@]}" || true
  fi
) >"$STORE_API_BUCKET_OPERATION_HITS_FILE"

if [[ -s "$STORE_API_BUCKET_OPERATION_HITS_FILE" ]]; then
  report_failure "old ecstore store_api BucketOperations path reintroduced: $(paste -sd '; ' "$STORE_API_BUCKET_OPERATION_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --no-heading 'pub(?:\(crate\))? use rustfs_storage_api::\{[^}]*\b(?:CompletePart|ListMultipartsInfo|ListPartsInfo|MultipartInfo|MultipartUploadResult|PartInfo)\b|pub struct (?:CompletePart|ListMultipartsInfo|ListPartsInfo|MultipartInfo|MultipartUploadResult|PartInfo)\b' \
    crates/ecstore/src/store_api.rs crates/ecstore/src/store_api/types.rs 2>/dev/null || true
) >"$STORE_API_MULTIPART_DTO_REEXPORTS_FILE"

if [[ -s "$STORE_API_MULTIPART_DTO_REEXPORTS_FILE" ]]; then
  report_failure "old ecstore store_api multipart DTO path reintroduced: $(paste -sd '; ' "$STORE_API_MULTIPART_DTO_REEXPORTS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --no-heading 'pub(?:\(crate\))? use rustfs_storage_api(?:::\{[^}]*\b(?:HTTPPreconditions|ObjectLockRetentionOptions|ObjectPreconditionError|ObjectPreconditionPart|ObjectPreconditionState)\b|::(?:HTTPPreconditions|ObjectLockRetentionOptions|ObjectPreconditionError|ObjectPreconditionPart|ObjectPreconditionState)\b)|pub (?:enum ObjectPreconditionError|struct (?:HTTPPreconditions|ObjectLockRetentionOptions|ObjectPreconditionPart|ObjectPreconditionState))\b' \
    crates/ecstore/src/store_api.rs crates/ecstore/src/store_api/types.rs 2>/dev/null || true
) >"$STORE_API_OBJECT_HELPER_REEXPORTS_FILE"

if [[ -s "$STORE_API_OBJECT_HELPER_REEXPORTS_FILE" ]]; then
  report_failure "old ecstore store_api object helper path reintroduced: $(paste -sd '; ' "$STORE_API_OBJECT_HELPER_REEXPORTS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --no-heading 'pub(?:\(crate\))? use rustfs_storage_api(?:::\{[^}]*\b(?:HTTPRangeError|HTTPRangeSpec)\b|::(?:HTTPRangeError|HTTPRangeSpec)\b)|pub (?:enum HTTPRangeError|struct HTTPRangeSpec)\b' \
    crates/ecstore/src/store_api.rs crates/ecstore/src/store_api/types.rs crates/ecstore/src/store_api/readers.rs 2>/dev/null || true
) >"$STORE_API_RANGE_HELPER_REEXPORTS_FILE"

if [[ -s "$STORE_API_RANGE_HELPER_REEXPORTS_FILE" ]]; then
  report_failure "old ecstore store_api range helper path reintroduced: $(paste -sd '; ' "$STORE_API_RANGE_HELPER_REEXPORTS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --no-heading 'pub(?:\(crate\))? use rustfs_storage_api(?:::\{[^}]*\b(?:VersionMarker|WalkOptions|WalkVersionsSortOrder)\b|::(?:VersionMarker|WalkOptions|WalkVersionsSortOrder)\b)|pub (?:enum (?:VersionMarker|WalkVersionsSortOrder)|struct WalkOptions)\b' \
    crates/ecstore/src/store_api.rs crates/ecstore/src/store_api/types.rs 2>/dev/null || true
) >"$STORE_API_LIST_HELPER_REEXPORTS_FILE"

if [[ -s "$STORE_API_LIST_HELPER_REEXPORTS_FILE" ]]; then
  report_failure "old ecstore store_api list helper path reintroduced: $(paste -sd '; ' "$STORE_API_LIST_HELPER_REEXPORTS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --no-heading 'pub(?:\(crate\))? use rustfs_storage_api(?:::\{[^}]*\b(?:ListObjectVersionsInfo|ListObjectsInfo|ListObjectsV2Info|ListOperations|ObjectInfoOrErr)\b|::(?:ListObjectVersionsInfo|ListObjectsInfo|ListObjectsV2Info|ListOperations|ObjectInfoOrErr)\b)|pub struct (?:ListObjectVersionsInfo|ListObjectsInfo|ListObjectsV2Info|ObjectInfoOrErr)\b' \
    crates/ecstore/src/store_api.rs crates/ecstore/src/store_api/types.rs 2>/dev/null || true
) >"$STORE_API_LIST_RESPONSE_REEXPORTS_FILE"

if [[ -s "$STORE_API_LIST_RESPONSE_REEXPORTS_FILE" ]]; then
  report_failure "old ecstore store_api list response path reintroduced: $(paste -sd '; ' "$STORE_API_LIST_RESPONSE_REEXPORTS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --no-heading 'pub(?:\(crate\))? use rustfs_storage_api(?:::\{[^}]*\b(?:DeletedObject|ObjectToDelete)\b|::(?:DeletedObject|ObjectToDelete)\b)|pub struct (?:DeletedObject|ObjectToDelete)\b' \
    crates/ecstore/src/store_api.rs crates/ecstore/src/store_api/types.rs 2>/dev/null || true
) >"$STORE_API_DELETE_DTO_REEXPORTS_FILE"

if [[ -s "$STORE_API_DELETE_DTO_REEXPORTS_FILE" ]]; then
  report_failure "old ecstore store_api delete DTO path reintroduced: $(paste -sd '; ' "$STORE_API_DELETE_DTO_REEXPORTS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --no-heading 'crate::store_api::(?:DeletedObject|ObjectToDelete)|store_api::\{[^}]*\b(?:DeletedObject|ObjectToDelete)\b' \
    crates/ecstore/src --glob '*.rs' --glob '!store_api/types.rs' || true
) >"$STORE_API_DELETE_DTO_INTERNAL_HITS_FILE"

if [[ -s "$STORE_API_DELETE_DTO_INTERNAL_HITS_FILE" ]]; then
  report_failure "ecstore internal delete DTO old store_api path reintroduced: $(paste -sd '; ' "$STORE_API_DELETE_DTO_INTERNAL_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --no-heading 'pub struct (?:ExpirationOptions|TransitionedObject)\b' \
    crates/ecstore/src/bucket/lifecycle/core.rs \
    crates/ecstore/src/bucket/lifecycle/bucket_lifecycle_ops.rs || true
) >"$STORE_API_LIFECYCLE_HELPER_DEFINITION_HITS_FILE"

if [[ -s "$STORE_API_LIFECYCLE_HELPER_DEFINITION_HITS_FILE" ]]; then
  report_failure "ECStore lifecycle helper DTO definitions must stay in rustfs-storage-api: $(paste -sd '; ' "$STORE_API_LIFECYCLE_HELPER_DEFINITION_HITS_FILE")"
fi

require_source_line \
  "crates/ecstore/src/bucket/lifecycle/core.rs" \
  "pub use crate::storage_api_contracts::lifecycle::ExpirationOptions;" \
  "ECStore ExpirationOptions compatibility re-export"
require_source_line \
  "crates/ecstore/src/bucket/lifecycle/bucket_lifecycle_ops.rs" \
  "pub use crate::storage_api_contracts::lifecycle::TransitionedObject;" \
  "ECStore TransitionedObject compatibility re-export"

(
  cd "$ROOT_DIR"
  rg -n --no-heading 'crate::bucket::lifecycle::(?:bucket_lifecycle_ops::TransitionedObject|core::ExpirationOptions|lifecycle::ExpirationOptions)|use crate::bucket::lifecycle::(?:bucket_lifecycle_ops::TransitionedObject|core::ExpirationOptions|lifecycle::ExpirationOptions)|rustfs_ecstore::bucket::lifecycle::bucket_lifecycle_ops::TransitionedObject' \
    crates/ecstore/src rustfs/src crates/scanner/src crates/scanner/tests crates/notify/src --glob '*.rs' || true
) >"$STORE_API_LIFECYCLE_HELPER_OLD_CONSUMER_HITS_FILE"

if [[ -s "$STORE_API_LIFECYCLE_HELPER_OLD_CONSUMER_HITS_FILE" ]]; then
  report_failure "lifecycle helper DTO consumers must import rustfs-storage-api directly: $(paste -sd '; ' "$STORE_API_LIFECYCLE_HELPER_OLD_CONSUMER_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '\b(?:send_event|EventArgs)\b' \
    crates/ecstore/src/bucket/lifecycle \
    --glob '*.rs' |
    rg -v '^crates/ecstore/src/bucket/lifecycle/bucket_lifecycle_audit\.rs:' || true
) >"$LIFECYCLE_AUDIT_SINK_BYPASS_HITS_FILE"

if [[ -s "$LIFECYCLE_AUDIT_SINK_BYPASS_HITS_FILE" ]]; then
  report_failure "lifecycle audit/event emission must stay behind bucket_lifecycle_audit sink: $(paste -sd '; ' "$LIFECYCLE_AUDIT_SINK_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'crate::config::com::|use crate::config::com' \
    crates/ecstore/src/bucket/lifecycle \
    --glob '*.rs' |
    rg -v '^crates/ecstore/src/bucket/lifecycle/config_boundary\.rs:' || true
) >"$LIFECYCLE_CONFIG_BOUNDARY_BYPASS_HITS_FILE"

if [[ -s "$LIFECYCLE_CONFIG_BOUNDARY_BYPASS_HITS_FILE" ]]; then
  report_failure "lifecycle config persistence access must stay behind lifecycle config_boundary: $(paste -sd '; ' "$LIFECYCLE_CONFIG_BOUNDARY_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'metadata_sys::get_(lifecycle|object_lock|replication)_config|use crate::bucket::metadata_sys::get_(lifecycle|object_lock|replication)_config' \
    crates/ecstore/src/bucket/lifecycle \
    --glob '*.rs' |
    rg -v '^crates/ecstore/src/bucket/lifecycle/metadata_boundary\.rs:' || true
) >"$LIFECYCLE_METADATA_BOUNDARY_BYPASS_HITS_FILE"

if [[ -s "$LIFECYCLE_METADATA_BOUNDARY_BYPASS_HITS_FILE" ]]; then
  report_failure "lifecycle metadata config reads must stay behind lifecycle metadata_boundary: $(paste -sd '; ' "$LIFECYCLE_METADATA_BOUNDARY_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'crate::bucket::tagging::decode_tags_to_map|use crate::bucket::tagging' \
    crates/ecstore/src/bucket/lifecycle \
    --glob '*.rs' |
    rg -v '^crates/ecstore/src/bucket/lifecycle/tagging_boundary\.rs:' || true
) >"$LIFECYCLE_TAGGING_BOUNDARY_BYPASS_HITS_FILE"

if [[ -s "$LIFECYCLE_TAGGING_BOUNDARY_BYPASS_HITS_FILE" ]]; then
  report_failure "lifecycle tag decoding must stay behind lifecycle tagging_boundary: $(paste -sd '; ' "$LIFECYCLE_TAGGING_BOUNDARY_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'crate::bucket::object_lock::objectlock_sys::(check_object_lock_for_deletion|is_object_locked_by_metadata)|use crate::bucket::object_lock::objectlock_sys' \
    crates/ecstore/src/bucket/lifecycle \
    --glob '*.rs' |
    rg -v '^crates/ecstore/src/bucket/lifecycle/object_lock_boundary\.rs:' || true
) >"$LIFECYCLE_OBJECT_LOCK_BOUNDARY_BYPASS_HITS_FILE"

if [[ -s "$LIFECYCLE_OBJECT_LOCK_BOUNDARY_BYPASS_HITS_FILE" ]]; then
  report_failure "lifecycle object-lock checks must stay behind lifecycle object_lock_boundary: $(paste -sd '; ' "$LIFECYCLE_OBJECT_LOCK_BOUNDARY_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'crate::bucket::replication' \
    crates/ecstore/src/bucket/lifecycle \
    --glob '*.rs' |
    rg -v '^crates/ecstore/src/bucket/lifecycle/replication_sink\.rs:' || true
) >"$LIFECYCLE_REPLICATION_SINK_BYPASS_HITS_FILE"

if [[ -s "$LIFECYCLE_REPLICATION_SINK_BYPASS_HITS_FILE" ]]; then
  report_failure "lifecycle replication scheduling must stay behind lifecycle replication_sink: $(paste -sd '; ' "$LIFECYCLE_REPLICATION_SINK_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --no-heading 'rustfs_ecstore::store_api(?:::\{[^}]*\b(?:ListObjectVersionsInfo|ListObjectsV2Info|ObjectInfoOrErr)\b|::(?:ListObjectVersionsInfo|ListObjectsV2Info|ObjectInfoOrErr)\b)' \
    rustfs/src crates/iam/src || true
) >"$STORE_API_EXTERNAL_LIST_CONSUMER_HITS_FILE"

if [[ -s "$STORE_API_EXTERNAL_LIST_CONSUMER_HITS_FILE" ]]; then
  report_failure "external list response consumers must use rustfs-storage-api contracts: $(paste -sd '; ' "$STORE_API_EXTERNAL_LIST_CONSUMER_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '^use rustfs_storage_api|^pub use rustfs_storage_api|rustfs_storage_api::' \
    crates/ecstore/src \
    --glob '*.rs' \
    --glob '!crates/ecstore/src/storage_api_contracts/mod.rs' \
    --glob '!crates/ecstore/src/storage_api_contracts/**' \
    --glob '!storage_api_contracts.rs' \
    --glob '!storage_api_contracts/mod.rs' \
    --glob '!storage_api_contracts/**' || true
) >"$ECSTORE_DIRECT_STORAGE_API_SOURCE_HITS_FILE"

if [[ -s "$ECSTORE_DIRECT_STORAGE_API_SOURCE_HITS_FILE" ]]; then
  report_failure "ECStore modules must route storage-api symbols through crates/ecstore/src/storage_api_contracts: $(paste -sd '; ' "$ECSTORE_DIRECT_STORAGE_API_SOURCE_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '^pub(?:\(crate\))? use rustfs_storage_api' \
    crates/ecstore/src/storage_api_contracts/mod.rs || true
) >"$ECSTORE_STORAGE_API_ROOT_REEXPORT_HITS_FILE"

if [[ -s "$ECSTORE_STORAGE_API_ROOT_REEXPORT_HITS_FILE" ]]; then
  report_failure "ECStore storage_api_contracts must expose rustfs-storage-api contracts from domain modules, not root re-exports: $(paste -sd '; ' "$ECSTORE_STORAGE_API_ROOT_REEXPORT_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  {
    rg -n -U --with-filename 'storage_api_contracts::\{\s*[A-Z]' \
      crates/ecstore/src \
      --glob '*.rs' \
      --glob '!crates/ecstore/src/storage_api_contracts/mod.rs' \
      --glob '!crates/ecstore/src/storage_api_contracts/**' \
      --glob '!storage_api_contracts.rs' \
      --glob '!storage_api_contracts/mod.rs' \
      --glob '!storage_api_contracts/**' || true
    rg -n --with-filename 'storage_api_contracts::[A-Z]' \
      crates/ecstore/src \
      --glob '*.rs' \
      --glob '!crates/ecstore/src/storage_api_contracts/mod.rs' \
      --glob '!crates/ecstore/src/storage_api_contracts/**' \
      --glob '!storage_api_contracts.rs' \
      --glob '!storage_api_contracts/mod.rs' \
      --glob '!storage_api_contracts/**' || true
  }
) >"$ECSTORE_STORAGE_API_ROOT_CONSUMER_HITS_FILE"

if [[ -s "$ECSTORE_STORAGE_API_ROOT_CONSUMER_HITS_FILE" ]]; then
  report_failure "ECStore modules must import storage_api_contracts from domain modules, not the root facade: $(paste -sd '; ' "$ECSTORE_STORAGE_API_ROOT_CONSUMER_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'rustfs_ecstore::api::|rustfs_storage_api' \
    crates/ecstore/tests \
    --glob '*.rs' |
    rg -v '^crates/ecstore/tests/storage_api\.rs:' || true
) >"$ECSTORE_TEST_DIRECT_API_HITS_FILE"

if [[ -s "$ECSTORE_TEST_DIRECT_API_HITS_FILE" ]]; then
  report_failure "ECStore integration tests must route ECStore and storage-api symbols through crates/ecstore/tests/storage_api.rs: $(paste -sd '; ' "$ECSTORE_TEST_DIRECT_API_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'rustfs_ecstore::api::|rustfs_storage_api' \
    crates/ecstore/benches \
    --glob '*.rs' |
    rg -v '^crates/ecstore/benches/storage_api/mod\.rs:' || true
) >"$ECSTORE_BENCH_DIRECT_API_HITS_FILE"

if [[ -s "$ECSTORE_BENCH_DIRECT_API_HITS_FILE" ]]; then
  report_failure "ECStore benches must route ECStore and storage-api symbols through crates/ecstore/benches/storage_api/mod.rs: $(paste -sd '; ' "$ECSTORE_BENCH_DIRECT_API_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  find crates/ecstore/src -type f -name '*.rs' -print0 |
    xargs -0 perl -0ne '
      while (/crate::object_api(?:::\{[^}]*\b(?:ListObjectsInfo|ListObjectsV2Info|ListObjectVersionsInfo|ObjectInfoOrErr|WalkOptions)\b|::(?:ListObjectsInfo|ListObjectsV2Info|ListObjectVersionsInfo|ObjectInfoOrErr|WalkOptions)\b)/sg) {
        my $prefix = substr($_, 0, $-[0]);
        my $line = ($prefix =~ tr/\n//) + 1;
        my $match = $&;
        $match =~ s/\s+/ /g;
        print "$ARGV:$line:$match\n";
      }
    ' || true
) >"$ECSTORE_OBJECT_API_LIST_ALIAS_INTERNAL_HITS_FILE"

if [[ -s "$ECSTORE_OBJECT_API_LIST_ALIAS_INTERNAL_HITS_FILE" ]]; then
  report_failure "ECStore internal list/walk consumers must bind rustfs-storage-api generic contracts directly: $(paste -sd '; ' "$ECSTORE_OBJECT_API_LIST_ALIAS_INTERNAL_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --no-heading 'pub type (?:ListObjectsInfo|ListObjectsV2Info|ListObjectVersionsInfo|ObjectInfoOrErr|WalkOptions|ObjectToDelete|DeletedObject)\b' \
    crates/ecstore/src/object_api/types.rs || true
) >"$ECSTORE_OBJECT_API_STORAGE_ALIAS_HITS_FILE"

if [[ -s "$ECSTORE_OBJECT_API_STORAGE_ALIAS_HITS_FILE" ]]; then
  report_failure "ECStore object_api must not re-export storage-api passthrough aliases: $(paste -sd '; ' "$ECSTORE_OBJECT_API_STORAGE_ALIAS_HITS_FILE")"
fi

cat >"$ECSTORE_OBJECT_API_EXTERNAL_ALIAS_EXPECTED_FILE" <<'EOF'
EOF

(
  cd "$ROOT_DIR"
  find rustfs/src crates -type f -name 'storage_compat.rs' -print0 |
    xargs -0 perl -ne '
      if (/^\s*(?:(?:pub(?:\([^)]*\))?)\s+)?type\s+([A-Za-z0-9_]+)\s*=\s*rustfs_ecstore::object_api::(GetObjectReader|ObjectInfo|ObjectOptions|PutObjReader)\s*;/) {
        print "$ARGV:$1=$2\n";
      }
    ' | sort
) >"$ECSTORE_OBJECT_API_EXTERNAL_ALIAS_ACTUAL_FILE"

if ! diff -u "$ECSTORE_OBJECT_API_EXTERNAL_ALIAS_EXPECTED_FILE" "$ECSTORE_OBJECT_API_EXTERNAL_ALIAS_ACTUAL_FILE" >"$ECSTORE_OBJECT_API_EXTERNAL_ALIAS_DIFF_FILE"; then
  report_failure "external ECStore object_api compatibility aliases changed without migration-plan approval: $(paste -sd '; ' "$ECSTORE_OBJECT_API_EXTERNAL_ALIAS_DIFF_FILE")"
fi

(
  cd "$ROOT_DIR"
  find rustfs/src crates -type f -name 'storage_compat.rs' -print0 |
    xargs -0 perl -ne '
      while (/rustfs_ecstore::object_api::([A-Za-z0-9_]+)/g) {
        my $name = $1;
        next if $name =~ /^(?:GetObjectReader|ObjectInfo|ObjectOptions|PutObjReader)$/;
        print "$ARGV:$.:rustfs_ecstore::object_api::$name\n";
      }
    ' | sort
) >"$ECSTORE_OBJECT_API_UNAPPROVED_NAME_HITS_FILE"

if [[ -s "$ECSTORE_OBJECT_API_UNAPPROVED_NAME_HITS_FILE" ]]; then
  report_failure "external storage compatibility boundaries must not expose new ECStore object_api names: $(paste -sd '; ' "$ECSTORE_OBJECT_API_UNAPPROVED_NAME_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  {
    perl -ne '
      if (/^\s*pub\s+mod\s+(startup_[A-Za-z0-9_]+);/ && $1 ne "startup_entrypoint") {
        print "$ARGV:$.:$_";
      }
    ' rustfs/src/lib.rs
    find rustfs/src -maxdepth 1 -type f -name 'startup_*.rs' ! -name 'startup_entrypoint.rs' -print0 |
      xargs -0 perl -ne '
        if (/^\s*pub\s+(?!\()/) {
          print "$ARGV:$.:$_";
        }
      '
  } || true
) >"$STARTUP_PUBLIC_SHIM_HITS_FILE"

if [[ -s "$STARTUP_PUBLIC_SHIM_HITS_FILE" ]]; then
  report_failure "startup owner boundaries must stay crate-private except startup_entrypoint: $(paste -sd '; ' "$STARTUP_PUBLIC_SHIM_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --no-heading 'rustfs_ecstore::store_api(?:::\{[^}]*\b(?:ObjectIO|ObjectOperations|ListOperations|MultipartOperations|HealOperations|NamespaceLocking)\b|::(?:ObjectIO|ObjectOperations|ListOperations|MultipartOperations|HealOperations|NamespaceLocking)\b)' \
    rustfs/src crates --glob '!crates/ecstore/**' || true
) >"$STORE_API_EXTERNAL_OPERATION_CONSUMER_HITS_FILE"

if [[ -s "$STORE_API_EXTERNAL_OPERATION_CONSUMER_HITS_FILE" ]]; then
  report_failure "external operation consumers must use rustfs-storage-api traits: $(paste -sd '; ' "$STORE_API_EXTERNAL_OPERATION_CONSUMER_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --no-heading '\brustfs_ecstore::|\b(?:use|pub(?:\([^)]*\))? use) rustfs_ecstore\b' \
    rustfs/src crates fuzz/fuzz_targets \
    --glob '!crates/ecstore/**' \
    --glob '!**/storage_compat.rs' \
    --glob '!**/*storage_compat.rs' \
    --glob '!**/ecstore_compat.rs' \
    --glob '!**/ecstore_test_compat.rs' \
    --glob '!**/ecstore_test_compat/**' \
    --glob '!**/ecstore_fuzz_compat.rs' \
    --glob '!target/**' \
    | rg -v '^(rustfs/src/(admin/mod|app/mod)\.rs|rustfs/src/storage/storage_api\.rs|crates/e2e_test/src/storage_api\.rs|crates/heal/src/heal/storage_api\.rs|crates/heal/tests/(endpoint_index_test|heal_bug_fixes_test|heal_integration_test)/storage_api\.rs|crates/iam/src/storage_api\.rs|crates/notify/src/storage_api\.rs|crates/obs/src/metrics/storage_api\.rs|crates/protocols/src/swift/storage_api\.rs|crates/s3select-api/src/storage_api\.rs|crates/scanner/src/storage_api\.rs|crates/scanner/tests/storage_api/mod\.rs|fuzz/fuzz_targets/(bucket_validation_storage_api|path_containment_storage_api)\.rs):' || true
) |
  cat >"$DIRECT_ECSTORE_IMPORT_HITS_FILE"

if [[ -s "$DIRECT_ECSTORE_IMPORT_HITS_FILE" ]]; then
  report_failure "direct rustfs_ecstore imports outside compatibility boundaries are forbidden: $(paste -sd '; ' "$DIRECT_ECSTORE_IMPORT_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  find rustfs/src crates fuzz/fuzz_targets -type f -name 'storage_compat.rs' \
    ! -path '*/crates/ecstore/*' -print0 |
    xargs -0 perl -ne '
      if (/rustfs_ecstore::(?!api::)/) {
        chomp;
        print "$ARGV:$.:$_\n";
      }
    ' || true
) >"$ECSTORE_API_FACADE_REQUIRED_HITS_FILE"

if [[ -s "$ECSTORE_API_FACADE_REQUIRED_HITS_FILE" ]]; then
  report_failure "outer storage compatibility boundaries must route ECStore imports through rustfs_ecstore::api: $(paste -sd '; ' "$ECSTORE_API_FACADE_REQUIRED_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --no-heading 'rustfs_ecstore::(?:admin_server_info|bucket|cache_value|client|compress|config|data_usage|disk|disks_layout|endpoints|error|event_notification|global|metrics_realtime|notification_sys|pools|rebalance|rio|rpc|set_disk|store|store_utils|tier)::' \
    rustfs/src crates/*/src crates/*/tests fuzz/fuzz_targets \
    --glob '*storage_compat.rs' \
    --glob '!crates/ecstore/**' || true
) >"$ECSTORE_PUBLIC_FACADE_BYPASS_HITS_FILE"

if [[ -s "$ECSTORE_PUBLIC_FACADE_BYPASS_HITS_FILE" ]]; then
  report_failure "outer storage compatibility boundaries must use rustfs_ecstore::api facade for ECStore public surfaces: $(paste -sd '; ' "$ECSTORE_PUBLIC_FACADE_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --no-heading '\bstorage_compat::ecstore\b|\bmod\s+ecstore\b' \
    crates/e2e_test/src crates/heal/tests crates/scanner/tests fuzz/fuzz_targets \
    --glob '*.rs' \
    --glob '!target/**' || true
) >"$TEST_HARNESS_NESTED_STORAGE_COMPAT_HITS_FILE"

if [[ -s "$TEST_HARNESS_NESTED_STORAGE_COMPAT_HITS_FILE" ]]; then
  report_failure "test and fuzz storage compatibility harnesses must use direct aliases instead of nested ecstore modules: $(paste -sd '; ' "$TEST_HARNESS_NESTED_STORAGE_COMPAT_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --no-heading '\bstorage_compat::ecstore\b|\bmod\s+ecstore\b' \
    rustfs/src \
    --glob '*.rs' \
    --glob '!target/**' || true
) >"$RUSTFS_NESTED_STORAGE_COMPAT_HITS_FILE"

if [[ -s "$RUSTFS_NESTED_STORAGE_COMPAT_HITS_FILE" ]]; then
  report_failure "RustFS runtime storage compatibility paths must use direct aliases instead of nested ecstore modules: $(paste -sd '; ' "$RUSTFS_NESTED_STORAGE_COMPAT_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --no-heading '\bstorage_compat::(?:store|error|global|endpoints|set_disk|rpc|metrics_realtime|notification_sys|admin_server_info|store_utils|data_usage|disks_layout|event_notification)::|\bstorage_compat::disk::(?:RUSTFS_META_BUCKET|DiskAPI|endpoint::Endpoint)\b|\bstorage_compat::\{[^}\n]*(?:store|error|global|endpoints|set_disk|rpc|metrics_realtime|notification_sys|admin_server_info|store_utils|data_usage|disks_layout|event_notification|disk)::' \
    rustfs/src \
    --glob '*.rs' \
    --glob '!target/**' || true
) >"$RUSTFS_RUNTIME_SCALAR_STORAGE_COMPAT_HITS_FILE"

if [[ -s "$RUSTFS_RUNTIME_SCALAR_STORAGE_COMPAT_HITS_FILE" ]]; then
  report_failure "RustFS runtime scalar storage compatibility paths must use direct aliases instead of secondary modules: $(paste -sd '; ' "$RUSTFS_RUNTIME_SCALAR_STORAGE_COMPAT_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  {
    rg -n --no-heading '\bstorage_compat::(?:bucket|config|rio|client|tier|compress|disk|rebalance)::' \
      rustfs/src \
      --glob '*.rs' \
      --glob '!target/**' || true
    rg -n --no-heading '\bpub\(crate\)\s+mod\s+(?:bucket|config|rio|client|tier|compress|disk|rebalance)\b' \
      rustfs/src \
      --glob '*storage_compat.rs' \
      --glob '!target/**' || true
    find rustfs/src -type f -name '*.rs' -print0 |
      xargs -0 perl -0ne '
        while (/use\s+crate::(?:app::|admin::|storage::)?storage_compat::\{[^;]*?\b(bucket|config|rio|client|tier|compress|disk|rebalance)::/sg) {
          print "$ARGV:grouped storage_compat::$1 import\n";
        }
      '
  }
) >"$RUSTFS_RUNTIME_SECONDARY_STORAGE_COMPAT_HITS_FILE"

if [[ -s "$RUSTFS_RUNTIME_SECONDARY_STORAGE_COMPAT_HITS_FILE" ]]; then
  report_failure "RustFS runtime secondary storage compatibility paths must use direct aliases instead of bucket/config/rio/client/tier/compress/disk/rebalance modules: $(paste -sd '; ' "$RUSTFS_RUNTIME_SECONDARY_STORAGE_COMPAT_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --no-heading 'pub\(crate\)\s+use\s+rustfs_ecstore::api::' \
    rustfs/src/storage_compat.rs 2>/dev/null || true
) >"$RUSTFS_ROOT_STORAGE_COMPAT_REEXPORT_HITS_FILE"

if [[ -s "$RUSTFS_ROOT_STORAGE_COMPAT_REEXPORT_HITS_FILE" ]]; then
  report_failure "RustFS root storage compatibility must use local aliases or wrappers instead of re-exporting ECStore API symbols: $(paste -sd '; ' "$RUSTFS_ROOT_STORAGE_COMPAT_REEXPORT_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --no-heading 'pub\(crate\)\s+use rustfs_ecstore::api::bucket::\{[^}]*\b(?:metadata|metadata_sys|quota)\b[^}]*\}\s*;|pub\(crate\)\s+use rustfs_ecstore::api::bucket::(?:metadata|metadata_sys|quota)\s*;' \
    rustfs/src/storage_compat.rs 2>/dev/null || true
) >"$RUSTFS_ROOT_BUCKET_STORAGE_COMPAT_MODULE_HITS_FILE"

if [[ -s "$RUSTFS_ROOT_BUCKET_STORAGE_COMPAT_MODULE_HITS_FILE" ]]; then
  report_failure "RustFS root storage compatibility must expose bucket metadata/quota contracts as explicit aliases: $(paste -sd '; ' "$RUSTFS_ROOT_BUCKET_STORAGE_COMPAT_MODULE_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --no-heading 'pub\(crate\)\s+use rustfs_ecstore::api::config::\{[^}]*\bcom\b[^}]*\}\s*;|pub\(crate\)\s+use rustfs_ecstore::api::config::\{[^}]*\binit\s*(?:,|})[^}]*\}\s*;|pub\(crate\)\s+use rustfs_ecstore::api::disk::\{[^}]*\bendpoint::Endpoint\b[^}]*\}\s*;' \
    rustfs/src/storage_compat.rs 2>/dev/null || true
) >"$RUSTFS_ROOT_RUNTIME_STORAGE_COMPAT_MODULE_HITS_FILE"

if [[ -s "$RUSTFS_ROOT_RUNTIME_STORAGE_COMPAT_MODULE_HITS_FILE" ]]; then
  report_failure "RustFS root storage compatibility must expose config init and disk endpoint contracts as explicit aliases: $(paste -sd '; ' "$RUSTFS_ROOT_RUNTIME_STORAGE_COMPAT_MODULE_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --no-heading 'pub\(crate\)\s+(?:async\s+)?fn\s+(?:all_local_disk|read_ecstore_config|save_ecstore_config|get_global_endpoints_opt|get_global_lock_clients|is_dist_erasure|resolve_object_store_handle|register_event_dispatch_hook|verify_rpc_signature|get_notification_config|init_bucket_metadata_sys|try_migrate_bucket_metadata|try_migrate_iam_config|init_background_replication|init_ecstore_config|init_global_config_sys|try_migrate_server_config|get_global_region|set_global_endpoints|set_global_region|set_global_rustfs_port|shutdown_background_services|update_erasure_type|new_global_notification_sys|init_local_disks|init_lock_clients|prewarm_local_disk_id_map)\b|pub\(crate\)\s+(?:const|type)\s+(?:TONIC_RPC_PREFIX|EcstoreEventArgs|ECStore|EcstoreResult)\b|pub\(crate\)\s+trait\s+DiskAPI\b' \
    rustfs/src/storage_compat.rs 2>/dev/null || true
) >"$RUSTFS_ROOT_CONSUMER_COMPAT_HITS_FILE"

if [[ -s "$RUSTFS_ROOT_CONSUMER_COMPAT_HITS_FILE" ]]; then
  report_failure "RustFS root storage compatibility must not own capacity/server/startup consumer wrappers: $(paste -sd '; ' "$RUSTFS_ROOT_CONSUMER_COMPAT_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  {
    for file in \
      rustfs/src/admin/storage_compat.rs \
      rustfs/src/app/storage_compat.rs \
      rustfs/src/storage/storage_compat.rs; do
      [[ -e "$file" ]] && printf '%s:1:RustFS owner bridge file exists\n' "$file"
    done
    rg -n --with-filename 'storage_compat' \
      rustfs/src/admin \
      rustfs/src/app \
      rustfs/src/storage \
      -g '*.rs' || true
  }
) >"$RUSTFS_OWNER_COMPAT_CONSUMER_HITS_FILE"

if [[ -s "$RUSTFS_OWNER_COMPAT_CONSUMER_HITS_FILE" ]]; then
  report_failure "RustFS app/admin/storage consumers must import owner APIs directly instead of local storage compatibility bridges: $(paste -sd '; ' "$RUSTFS_OWNER_COMPAT_CONSUMER_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --no-heading 'pub\(crate\)\s+use\s+crate::(?:admin|app|storage)::storage_compat::\*;' \
    rustfs/src/admin/handlers/storage_compat.rs 2>/dev/null || true
) >"$RUSTFS_LOCAL_COMPAT_GLOB_EXPORT_HITS_FILE"

if [[ -s "$RUSTFS_LOCAL_COMPAT_GLOB_EXPORT_HITS_FILE" ]]; then
  report_failure "Narrowed local compatibility boundaries must use explicit re-exports: $(paste -sd '; ' "$RUSTFS_LOCAL_COMPAT_GLOB_EXPORT_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --no-heading 'pub\(crate\)\s+use rustfs_ecstore::api::config::\{[^}]*\bcom\b[^}]*\}\s*;|pub\(crate\)\s+use rustfs_ecstore::api::config::\{[^}]*\binit\s*(?:,|})[^}]*\}\s*;' \
    rustfs/src/admin/storage_compat.rs 2>/dev/null || true
) >"$RUSTFS_ADMIN_CONFIG_STORAGE_COMPAT_MODULE_HITS_FILE"

if [[ -s "$RUSTFS_ADMIN_CONFIG_STORAGE_COMPAT_MODULE_HITS_FILE" ]]; then
  report_failure "RustFS admin storage compatibility must expose config IO and default initialization as explicit aliases: $(paste -sd '; ' "$RUSTFS_ADMIN_CONFIG_STORAGE_COMPAT_MODULE_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --no-heading 'pub\(crate\)\s+use rustfs_ecstore::api::bucket::\{[^}]*\b(?:metadata|metadata_sys|object_lock|policy_sys|replication|tagging|utils|versioning|versioning_sys)\b[^}]*\}\s*;|pub\(crate\)\s+use rustfs_ecstore::api::bucket::(?:metadata|metadata_sys|object_lock|policy_sys|replication|tagging|utils|versioning|versioning_sys)\s*;|pub\(crate\)\s+use rustfs_ecstore::api::client::object_api_utils\s*;|pub\(crate\)\s+use rustfs_ecstore::api::config::com\s*;' \
    rustfs/src/storage/storage_compat.rs 2>/dev/null || true
) >"$RUSTFS_STORAGE_BUCKET_STORAGE_COMPAT_MODULE_HITS_FILE"

if [[ -s "$RUSTFS_STORAGE_BUCKET_STORAGE_COMPAT_MODULE_HITS_FILE" ]]; then
  report_failure "RustFS storage compatibility must expose bucket/object-api/config contracts as explicit aliases: $(paste -sd '; ' "$RUSTFS_STORAGE_BUCKET_STORAGE_COMPAT_MODULE_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  perl -0ne '
    while (/pub\(crate\)\s+use\s+rustfs_ecstore::api::([^;]+);/g) {
      my $path = $1;
      $path =~ s/\s+/ /g;
      $path =~ s/^\s+|\s+$//g;
      next if $path eq "bucket::replication::ReplicationConfigurationExt";
      next if $path eq "bucket::versioning::VersioningApi";
      next if $path eq "disk::DiskAPI";
      next if $path eq "rpc::PeerS3Client";
      print "$ARGV:pub(crate) use rustfs_ecstore::api::$path;\n";
    }
  ' rustfs/src/storage/storage_compat.rs 2>/dev/null || true
) >"$RUSTFS_STORAGE_OWNER_COMPAT_REEXPORT_HITS_FILE"

if [[ -s "$RUSTFS_STORAGE_OWNER_COMPAT_REEXPORT_HITS_FILE" ]]; then
  report_failure "RustFS storage owner compatibility must use local aliases or wrappers instead of re-exporting ECStore API symbols except temporary traits: $(paste -sd '; ' "$RUSTFS_STORAGE_OWNER_COMPAT_REEXPORT_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'rustfs_ecstore::|^use rustfs_storage_api|rustfs_storage_api::' \
    rustfs/src/storage \
    -g '!storage_api.rs' || true
) >"$RUSTFS_STORAGE_OWNER_DIRECT_STORAGE_SOURCE_HITS_FILE"

if [[ -s "$RUSTFS_STORAGE_OWNER_DIRECT_STORAGE_SOURCE_HITS_FILE" ]]; then
  report_failure "RustFS storage owner modules must route ECStore and storage-api symbols through rustfs/src/storage/storage_api.rs: $(paste -sd '; ' "$RUSTFS_STORAGE_OWNER_DIRECT_STORAGE_SOURCE_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  {
    rg -n -U --with-filename 'crate::storage::contract::\{\s*[A-Z]' \
      rustfs/src/storage \
      --glob '*.rs' \
      --glob '!storage_api.rs' || true
    rg -n --with-filename 'crate::storage::contract::[A-Z]' \
      rustfs/src/storage \
      --glob '*.rs' \
      --glob '!storage_api.rs' || true
  }
) >"$RUSTFS_STORAGE_OWNER_CONTRACT_ROOT_CONSUMER_HITS_FILE"

if [[ -s "$RUSTFS_STORAGE_OWNER_CONTRACT_ROOT_CONSUMER_HITS_FILE" ]]; then
  report_failure "RustFS storage owner modules must import storage contracts from domain modules, not the root contract facade: $(paste -sd '; ' "$RUSTFS_STORAGE_OWNER_CONTRACT_ROOT_CONSUMER_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'rustfs_storage_api::[A-Za-z_]' \
    rustfs/src/storage_api.rs \
    rustfs/src/app/storage_api.rs \
    rustfs/src/admin/storage_api.rs \
    rustfs/src/storage/storage_api.rs \
    crates/iam/src/storage_api.rs \
    crates/protocols/src/swift/storage_api.rs \
    crates/s3select-api/src/storage_api.rs \
    crates/scanner/src/storage_api.rs \
    crates/obs/src/metrics/storage_api.rs \
    crates/heal/src/heal/storage_api.rs || true
) >"$LOCAL_STORAGE_API_RAW_CONTRACT_PATH_HITS_FILE"

if [[ -s "$LOCAL_STORAGE_API_RAW_CONTRACT_PATH_HITS_FILE" ]]; then
  report_failure "local storage_api boundaries must import storage-api contracts once and use local aliases internally: $(paste -sd '; ' "$LOCAL_STORAGE_API_RAW_CONTRACT_PATH_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --no-heading 'pub\(crate\)\s+use rustfs_ecstore::api::bucket::\{[^}]*\b(?:bandwidth|bucket_target_sys|lifecycle|metadata|metadata_sys|quota|replication|target|utils|versioning|versioning_sys)\b[^}]*\}\s*;|pub\(crate\)\s+use rustfs_ecstore::api::bucket::(?:bandwidth|bucket_target_sys|lifecycle|metadata|metadata_sys|quota|replication|target|utils|versioning|versioning_sys)\s*;|pub\(crate\)\s+use rustfs_ecstore::api::config::\{[^}]*\bstorageclass\b[^}]*\}\s*;|pub\(crate\)\s+use rustfs_ecstore::api::config::storageclass\s*;' \
    rustfs/src/admin/storage_compat.rs 2>/dev/null || true
) >"$RUSTFS_ADMIN_BUCKET_STORAGE_COMPAT_MODULE_HITS_FILE"

if [[ -s "$RUSTFS_ADMIN_BUCKET_STORAGE_COMPAT_MODULE_HITS_FILE" ]]; then
  report_failure "RustFS admin storage compatibility must expose bucket/storageclass contracts as explicit aliases: $(paste -sd '; ' "$RUSTFS_ADMIN_BUCKET_STORAGE_COMPAT_MODULE_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --no-heading 'pub\(crate\)\s+use rustfs_ecstore::api::bucket::\{[^}]*\b(?:bucket_target_sys|lifecycle|metadata|metadata_sys|object_lock|policy_sys|quota|replication|tagging|target|utils|versioning|versioning_sys)\b[^}]*\}\s*;|pub\(crate\)\s+use rustfs_ecstore::api::bucket::(?:bucket_target_sys|lifecycle|metadata|metadata_sys|object_lock|policy_sys|quota|replication|tagging|target|utils|versioning|versioning_sys)\s*;|pub\(crate\)\s+use rustfs_ecstore::api::client::(?:object_api_utils|transition_api)\s*;|pub\(crate\)\s+use rustfs_ecstore::api::config::storageclass\s*;' \
    rustfs/src/app/storage_compat.rs 2>/dev/null || true
) >"$RUSTFS_APP_BUCKET_STORAGE_COMPAT_MODULE_HITS_FILE"

if [[ -s "$RUSTFS_APP_BUCKET_STORAGE_COMPAT_MODULE_HITS_FILE" ]]; then
  report_failure "RustFS app storage compatibility must expose bucket/client/storageclass contracts as explicit aliases: $(paste -sd '; ' "$RUSTFS_APP_BUCKET_STORAGE_COMPAT_MODULE_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --no-heading 'rustfs_ecstore::api::(object::(ObjectInfo|ObjectOptions)|error::(Error|Result))' \
    rustfs/src/app/storage_compat.rs \
    rustfs/src/admin/storage_compat.rs \
    rustfs/src/storage/storage_compat.rs 2>/dev/null || true
) >"$RUSTFS_OUTER_COMPAT_FACADE_ALIAS_HITS_FILE"

if [[ -s "$RUSTFS_OUTER_COMPAT_FACADE_ALIAS_HITS_FILE" ]]; then
  report_failure "RustFS outer storage compatibility must use storage-api object aliases and local StorageError aliases instead of raw ECStore object/error facade aliases: $(paste -sd '; ' "$RUSTFS_OUTER_COMPAT_FACADE_ALIAS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --no-heading '(fn .*rustfs_ecstore::api::(bucket::metadata_sys::BucketMetadataSys|bucket::object_lock::objectlock_sys::ObjectLockBlockReason|bucket::lifecycle::tier_sweeper::Jentry|bucket::bandwidth::monitor::Monitor|notification::NotificationSys)|-> .*rustfs_ecstore::api::(bucket::metadata_sys::BucketMetadataSys|bucket::object_lock::objectlock_sys::ObjectLockBlockReason|bucket::lifecycle::tier_sweeper::Jentry|bucket::bandwidth::monitor::Monitor|notification::NotificationSys)|: &?rustfs_ecstore::api::(bucket::metadata_sys::BucketMetadataSys|bucket::object_lock::objectlock_sys::ObjectLockBlockReason|bucket::lifecycle::tier_sweeper::Jentry|bucket::bandwidth::monitor::Monitor|notification::NotificationSys))' \
    rustfs/src/app/storage_compat.rs \
    rustfs/src/admin/storage_compat.rs \
    rustfs/src/storage/storage_compat.rs 2>/dev/null || true
) >"$RUSTFS_OUTER_COMPAT_SIGNATURE_ALIAS_HITS_FILE"

if [[ -s "$RUSTFS_OUTER_COMPAT_SIGNATURE_ALIAS_HITS_FILE" ]]; then
  report_failure "RustFS outer storage compatibility signatures must use local aliases for metadata, object-lock, lifecycle, monitor, and notification facade types: $(paste -sd '; ' "$RUSTFS_OUTER_COMPAT_SIGNATURE_ALIAS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --no-heading 'rustfs_ecstore::api::' rustfs/src/storage/storage_compat.rs \
    2>/dev/null | rg -v '^[0-9]+:use rustfs_ecstore::api::[a-z_]+ as ecstore_[a-z_]+;' || true
) >"$RUSTFS_STORAGE_COMPAT_RAW_FACADE_PATH_HITS_FILE"

if [[ -s "$RUSTFS_STORAGE_COMPAT_RAW_FACADE_PATH_HITS_FILE" ]]; then
  report_failure "RustFS storage owner compatibility must use local ecstore_* module aliases instead of scattered raw ECStore facade paths: $(paste -sd '; ' "$RUSTFS_STORAGE_COMPAT_RAW_FACADE_PATH_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --no-heading 'rustfs_ecstore::api::' rustfs/src/app/storage_compat.rs rustfs/src/admin/storage_compat.rs \
    2>/dev/null | rg -v '^[^:]+:[0-9]+:use rustfs_ecstore::api::[a-z_]+ as ecstore_[a-z_]+;' || true
) >"$RUSTFS_APP_ADMIN_COMPAT_RAW_FACADE_PATH_HITS_FILE"

if [[ -s "$RUSTFS_APP_ADMIN_COMPAT_RAW_FACADE_PATH_HITS_FILE" ]]; then
  report_failure "RustFS app/admin storage compatibility must use local ecstore_* module aliases instead of scattered raw ECStore facade paths: $(paste -sd '; ' "$RUSTFS_APP_ADMIN_COMPAT_RAW_FACADE_PATH_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  {
    for file in \
      crates/iam/src/storage_compat.rs \
      crates/heal/src/heal/storage_compat.rs \
      crates/obs/src/storage_compat.rs \
      crates/notify/src/storage_compat.rs \
      crates/protocols/src/swift/storage_compat.rs \
      crates/s3select-api/src/storage_compat.rs \
      crates/scanner/src/storage_compat.rs; do
      [[ -f "$file" ]] && rg -n --with-filename --no-heading 'rustfs_ecstore::api::' "$file"
    done
  } \
    | rg -v '^[^:]+:[0-9]+:use rustfs_ecstore::api::[a-z_]+ as ecstore_[a-z_]+;' || true
) >"$OUTER_CONSUMER_COMPAT_RAW_FACADE_PATH_HITS_FILE"

if [[ -s "$OUTER_CONSUMER_COMPAT_RAW_FACADE_PATH_HITS_FILE" ]]; then
  report_failure "outer consumer storage compatibility must use local ecstore_* module aliases instead of scattered raw ECStore facade paths: $(paste -sd '; ' "$OUTER_CONSUMER_COMPAT_RAW_FACADE_PATH_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  if [[ -f crates/e2e_test/src/storage_compat.rs ]]; then
    rg -n --with-filename --no-heading 'rustfs_ecstore::api::' crates/e2e_test/src/storage_compat.rs \
      | rg -v '^[^:]+:[0-9]+:use rustfs_ecstore::api::[a-z_]+ as ecstore_[a-z_]+;' || true
  fi
) >"$RUSTFS_ROOT_E2E_COMPAT_RAW_FACADE_PATH_HITS_FILE"

if [[ -s "$RUSTFS_ROOT_E2E_COMPAT_RAW_FACADE_PATH_HITS_FILE" ]]; then
  report_failure "RustFS root/e2e storage compatibility must use local ecstore_* module aliases instead of scattered raw ECStore facade paths: $(paste -sd '; ' "$RUSTFS_ROOT_E2E_COMPAT_RAW_FACADE_PATH_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'rustfs_ecstore::api::' rustfs/src crates fuzz \
    --glob '*storage_compat.rs' \
    --glob '*_compat.rs' \
    --glob '!**/ecstore_compat.rs' \
    --glob '!**/ecstore_test_compat.rs' \
    --glob '!**/ecstore_test_compat/**' \
    | rg -v '^[^:]+:[0-9]+:use rustfs_ecstore::api::[a-z_]+ as ecstore_[a-z_]+;' || true
) >"$ALL_STORAGE_COMPAT_RAW_FACADE_PATH_HITS_FILE"

if [[ -s "$ALL_STORAGE_COMPAT_RAW_FACADE_PATH_HITS_FILE" ]]; then
  report_failure "storage compatibility boundaries must keep raw ECStore facade paths behind local ecstore_* module aliases: $(paste -sd '; ' "$ALL_STORAGE_COMPAT_RAW_FACADE_PATH_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '^use rustfs_ecstore::api::\{' rustfs/src crates fuzz \
    --glob '*storage_compat.rs' \
    --glob '*_compat.rs' || true
) >"$ALL_STORAGE_COMPAT_GROUPED_FACADE_IMPORT_HITS_FILE"

if [[ -s "$ALL_STORAGE_COMPAT_GROUPED_FACADE_IMPORT_HITS_FILE" ]]; then
  report_failure "storage compatibility boundaries must import ECStore facade modules through explicit per-module ecstore_* aliases: $(paste -sd '; ' "$ALL_STORAGE_COMPAT_GROUPED_FACADE_IMPORT_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '^use rustfs_ecstore::api::\{' rustfs/src crates fuzz \
    --glob '*.rs' \
    --glob '!crates/ecstore/**' || true
) >"$ALL_ECSTORE_API_GROUPED_FACADE_IMPORT_HITS_FILE"

if [[ -s "$ALL_ECSTORE_API_GROUPED_FACADE_IMPORT_HITS_FILE" ]]; then
  report_failure "non-ECStore sources must import ECStore facade modules through explicit per-module ecstore_* aliases: $(paste -sd '; ' "$ALL_ECSTORE_API_GROUPED_FACADE_IMPORT_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'rustfs_ecstore::api::[a-z_]+::' rustfs/src crates fuzz \
    --glob '*.rs' \
    --glob '!crates/ecstore/**' \
    --glob '!**/ecstore_compat.rs' \
    --glob '!**/ecstore_test_compat.rs' \
    --glob '!**/ecstore_test_compat/**' |
    rg -v '^(fuzz/fuzz_targets/(bucket_validation_storage_api|path_containment_storage_api)\.rs|crates/e2e_test/src/storage_api\.rs|crates/heal/src/heal/storage_api\.rs|crates/heal/tests/(endpoint_index_test|heal_bug_fixes_test|heal_integration_test)/storage_api\.rs|crates/iam/src/storage_api\.rs|crates/notify/src/storage_api\.rs|crates/obs/src/metrics/storage_api\.rs|crates/protocols/src/swift/storage_api\.rs|crates/s3select-api/src/storage_api\.rs|crates/scanner/src/storage_api\.rs|crates/scanner/tests/storage_api/mod\.rs|rustfs/src/admin/mod\.rs|rustfs/src/app/mod\.rs|rustfs/src/storage/storage_api\.rs):' || true
) >"$ALL_ECSTORE_API_RAW_SUBPATH_HITS_FILE"

if [[ -s "$ALL_ECSTORE_API_RAW_SUBPATH_HITS_FILE" ]]; then
  report_failure "non-ECStore sources must keep raw ECStore facade subpaths behind local ecstore_* module aliases: $(paste -sd '; ' "$ALL_ECSTORE_API_RAW_SUBPATH_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '^(?:pub\(crate\) )?use rustfs_ecstore::api::[a-z_]+ as ecstore_[a-z_]+;' \
    crates/heal/src/heal/storage_api.rs \
    crates/heal/tests/endpoint_index_test/storage_api.rs \
    crates/heal/tests/heal_bug_fixes_test/storage_api.rs \
    crates/heal/tests/heal_integration_test/storage_api.rs \
    crates/iam/src/lib.rs \
    crates/notify/src/lib.rs \
    crates/obs/src/metrics/mod.rs \
    crates/protocols/src/swift/mod.rs \
    crates/s3select-api/src/lib.rs \
    crates/scanner/src/storage_api.rs \
    crates/scanner/tests/storage_api/mod.rs \
    crates/e2e_test/src/reliant/grpc_lock_client.rs \
    crates/e2e_test/src/reliant/node_interact_test.rs \
    crates/e2e_test/src/replication_extension_test.rs \
    rustfs/src/admin/mod.rs \
    rustfs/src/app/mod.rs \
    rustfs/src/storage/storage_api.rs \
    fuzz/fuzz_targets/bucket_validation.rs \
    fuzz/fuzz_targets/path_containment.rs || true
) >"$COMPLETED_EXTERNAL_OWNER_MODULE_ALIAS_HITS_FILE"

if [[ -s "$COMPLETED_EXTERNAL_OWNER_MODULE_ALIAS_HITS_FILE" ]]; then
  report_failure "completed external owner and test/fuzz boundaries must expose explicit ECStore symbols instead of ecstore_* module aliases: $(paste -sd '; ' "$COMPLETED_EXTERNAL_OWNER_MODULE_ALIAS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '^(?:pub\(crate\)\s+)?use\s+rustfs_ecstore::api::[a-z_]+\s*;|^(?:pub\(crate\)\s+)?use\s+rustfs_ecstore::api::[a-z_]+::\*\s*;' \
    crates/heal/src/heal/storage_api.rs \
    crates/heal/tests/endpoint_index_test/storage_api.rs \
    crates/heal/tests/heal_bug_fixes_test/storage_api.rs \
    crates/heal/tests/heal_integration_test/storage_api.rs \
    crates/iam/src/lib.rs \
    crates/notify/src/lib.rs \
    crates/obs/src/metrics/mod.rs \
    crates/protocols/src/swift/mod.rs \
    crates/s3select-api/src/lib.rs \
    crates/scanner/src/storage_api.rs \
    crates/scanner/tests/storage_api/mod.rs \
    crates/e2e_test/src/reliant/grpc_lock_client.rs \
    crates/e2e_test/src/reliant/node_interact_test.rs \
    crates/e2e_test/src/replication_extension_test.rs \
    rustfs/src/admin/mod.rs \
    rustfs/src/app/mod.rs \
    rustfs/src/storage/storage_api.rs \
    fuzz/fuzz_targets/bucket_validation.rs \
    fuzz/fuzz_targets/path_containment.rs || true
) >"$COMPLETED_OWNER_BARE_FACADE_IMPORT_HITS_FILE"

if [[ -s "$COMPLETED_OWNER_BARE_FACADE_IMPORT_HITS_FILE" ]]; then
  report_failure "completed owner and test/fuzz boundaries must not import bare or glob ECStore facade modules: $(paste -sd '; ' "$COMPLETED_OWNER_BARE_FACADE_IMPORT_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'rustfs_ecstore::api::[a-z_]+::' \
    crates/heal/src/heal/storage_api.rs \
    crates/heal/tests/endpoint_index_test/storage_api.rs \
    crates/heal/tests/heal_bug_fixes_test/storage_api.rs \
    crates/heal/tests/heal_integration_test/storage_api.rs \
    crates/iam/src/lib.rs \
    crates/notify/src/lib.rs \
    crates/obs/src/metrics/mod.rs \
    crates/protocols/src/swift/mod.rs \
    crates/s3select-api/src/lib.rs \
    crates/scanner/src/storage_api.rs \
    crates/scanner/tests/storage_api/mod.rs \
    crates/e2e_test/src/reliant/grpc_lock_client.rs \
    crates/e2e_test/src/reliant/node_interact_test.rs \
    crates/e2e_test/src/replication_extension_test.rs \
    rustfs/src/admin/mod.rs \
    rustfs/src/app/mod.rs \
    rustfs/src/storage/storage_api.rs \
    fuzz/fuzz_targets/bucket_validation.rs \
    fuzz/fuzz_targets/path_containment.rs |
    rg -v '^[^:]+:[0-9]+:\s*(?:pub\(crate\)\s+)?use\s+rustfs_ecstore::api::[a-z_]+::' || true
) >"$COMPLETED_OWNER_SCATTERED_RAW_FACADE_PATH_HITS_FILE"

if [[ -s "$COMPLETED_OWNER_SCATTERED_RAW_FACADE_PATH_HITS_FILE" ]]; then
  report_failure "completed owner and test/fuzz boundaries must keep raw ECStore facade subpaths in explicit import declarations: $(paste -sd '; ' "$COMPLETED_OWNER_SCATTERED_RAW_FACADE_PATH_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'crate::storage::.*ecstore_|^\s*ecstore_[a-z_]+(?:::|,|\})' \
    rustfs/src/startup_notification.rs \
    rustfs/src/startup_fs_guard.rs \
    rustfs/src/startup_services.rs \
    rustfs/src/startup_server.rs \
    rustfs/src/startup_storage.rs || true
) >"$RUSTFS_STARTUP_OWNER_MODULE_CONSUMER_HITS_FILE"

if [[ -s "$RUSTFS_STARTUP_OWNER_MODULE_CONSUMER_HITS_FILE" ]]; then
  report_failure "RustFS startup runtime consumers must use storage owner symbols instead of ecstore_* modules: $(paste -sd '; ' "$RUSTFS_STARTUP_OWNER_MODULE_CONSUMER_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'crate::storage::.*ecstore_|^\s*ecstore_[a-z_]+(?:::|,|\})' \
    rustfs/src/server/http.rs \
    rustfs/src/capacity/service.rs \
    rustfs/src/workload_admission.rs || true
) >"$RUSTFS_RUNTIME_OWNER_MODULE_CONSUMER_HITS_FILE"

if [[ -s "$RUSTFS_RUNTIME_OWNER_MODULE_CONSUMER_HITS_FILE" ]]; then
  report_failure "RustFS runtime server/capacity/workload consumers must use storage owner symbols instead of ecstore_* modules: $(paste -sd '; ' "$RUSTFS_RUNTIME_OWNER_MODULE_CONSUMER_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'crate::storage::.*ecstore_|^\s*ecstore_[a-z_]+(?:::|,|\})' \
    rustfs/src/init.rs \
    rustfs/src/runtime_capabilities.rs \
    rustfs/src/server/readiness.rs \
    rustfs/src/server/event.rs \
    rustfs/src/server/module_switch.rs \
    rustfs/src/error.rs || true
) >"$RUSTFS_ROOT_SERVER_OWNER_MODULE_CONSUMER_HITS_FILE"

if [[ -s "$RUSTFS_ROOT_SERVER_OWNER_MODULE_CONSUMER_HITS_FILE" ]]; then
  report_failure "RustFS root/server runtime consumers must use storage owner symbols instead of ecstore_* modules: $(paste -sd '; ' "$RUSTFS_ROOT_SERVER_OWNER_MODULE_CONSUMER_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'crate::storage::.*ecstore_|^\s*ecstore_[a-z_]+(?:::|,|\})' \
    rustfs/src/startup_bucket_metadata.rs \
    rustfs/src/startup_shutdown.rs \
    rustfs/src/table_catalog.rs \
    rustfs/src/storage/s3_api/bucket.rs \
    rustfs/src/storage/s3_api/multipart.rs \
    rustfs/src/config/config_test.rs || true
) >"$RUSTFS_TABLE_S3_OWNER_MODULE_CONSUMER_HITS_FILE"

if [[ -s "$RUSTFS_TABLE_S3_OWNER_MODULE_CONSUMER_HITS_FILE" ]]; then
  report_failure "RustFS table/S3/startup consumers must use storage owner symbols instead of ecstore_* modules: $(paste -sd '; ' "$RUSTFS_TABLE_S3_OWNER_MODULE_CONSUMER_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'ecstore_(?:disk::(?:error::DiskError|endpoint::Endpoint|error_reduce::is_all_buckets_not_found)|error::(?:StorageError|is_err_bucket_not_found|is_err_object_not_found|is_err_version_not_found)|global::(?:get_global_endpoints_opt|get_global_region|get_global_tier_config_mgr|new_object_layer_fn|set_object_store_resolver)|layout::(?:EndpointServerPools|Endpoints|PoolEndpoints)|notification::(?:NotificationSys|get_global_notification_sys)|rio::(?:DynReader|HashReader|WriteEncryption|WritePlan|DecryptReader|EncryptReader|HardLimitReader|boxed_reader|compression_metadata_value|wrap_reader)|set_disk::(?:get_lock_acquire_timeout|is_valid_storage_class)|storage::(?:ECStore|init_local_disks))' \
    rustfs/src/app/mod.rs || true
) >"$RUSTFS_APP_SHARED_OWNER_MODULE_CONSUMER_HITS_FILE"

if [[ -s "$RUSTFS_APP_SHARED_OWNER_MODULE_CONSUMER_HITS_FILE" ]]; then
  report_failure "RustFS app shared runtime facade must use storage owner symbols instead of duplicate ecstore_* calls: $(paste -sd '; ' "$RUSTFS_APP_SHARED_OWNER_MODULE_CONSUMER_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'rustfs_ecstore::api::(?:bucket|client|config)::' \
    rustfs/src/app/mod.rs || true
) >"$RUSTFS_APP_BUCKET_OWNER_SOURCE_HITS_FILE"

if [[ -s "$RUSTFS_APP_BUCKET_OWNER_SOURCE_HITS_FILE" ]]; then
  report_failure "RustFS app bucket facade must source completed bucket/client/config symbols through storage owner re-exports: $(paste -sd '; ' "$RUSTFS_APP_BUCKET_OWNER_SOURCE_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'rustfs_ecstore::api::' \
    rustfs/src/app/mod.rs || true
) >"$RUSTFS_APP_ECSTORE_SOURCE_HITS_FILE"

if [[ -s "$RUSTFS_APP_ECSTORE_SOURCE_HITS_FILE" ]]; then
  report_failure "RustFS app facade must source ECStore API symbols through storage owner re-exports: $(paste -sd '; ' "$RUSTFS_APP_ECSTORE_SOURCE_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'rustfs_ecstore::api::' \
    rustfs/src/admin/mod.rs || true
) >"$RUSTFS_ADMIN_ECSTORE_SOURCE_HITS_FILE"

if [[ -s "$RUSTFS_ADMIN_ECSTORE_SOURCE_HITS_FILE" ]]; then
  report_failure "RustFS admin facade must source ECStore API symbols through storage owner re-exports: $(paste -sd '; ' "$RUSTFS_ADMIN_ECSTORE_SOURCE_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'rustfs_ecstore::api::' \
    crates/heal/src/heal \
    crates/iam/src \
    crates/notify/src \
    crates/obs/src/metrics \
    crates/protocols/src/swift \
    crates/s3select-api/src \
    crates/scanner/src \
    --glob '*.rs' \
    --glob '!**/ecstore_compat.rs' |
    rg -v '^(crates/heal/src/heal/storage_api.rs|crates/iam/src/storage_api.rs|crates/notify/src/storage_api.rs|crates/obs/src/metrics/storage_api.rs|crates/protocols/src/swift/storage_api.rs|crates/s3select-api/src/storage_api.rs|crates/scanner/src/storage_api.rs):' || true
) >"$EXTERNAL_RUNTIME_ECSTORE_COMPAT_BYPASS_HITS_FILE"

if [[ -s "$EXTERNAL_RUNTIME_ECSTORE_COMPAT_BYPASS_HITS_FILE" ]]; then
  report_failure "external runtime crates must source ECStore API symbols through their local storage_api boundary: $(paste -sd '; ' "$EXTERNAL_RUNTIME_ECSTORE_COMPAT_BYPASS_HITS_FILE")"
fi

cat >"$GLOBAL_FACADE_BOUNDARY_EXPECTED_FILE" <<'EOF'
rustfs/src/storage/storage_api.rs
EOF

(
  cd "$ROOT_DIR"
  rg -l 'rustfs_ecstore::api::global' crates rustfs/src --glob '*.rs' | LC_ALL=C sort
) >"$GLOBAL_FACADE_BOUNDARY_ACTUAL_FILE"

if ! diff -u "$GLOBAL_FACADE_BOUNDARY_EXPECTED_FILE" "$GLOBAL_FACADE_BOUNDARY_ACTUAL_FILE" >"$GLOBAL_FACADE_BOUNDARY_DIFF_FILE"; then
  report_failure "ECStore global facade access must stay inside reviewed local storage_api boundaries: $(tr '\n' '; ' <"$GLOBAL_FACADE_BOUNDARY_DIFF_FILE")"
fi

READ_ONLY_ECSTORE_GLOBAL_SYMBOL_PATTERN='get_global_bucket_monitor|get_global_deployment_id|get_global_endpoints|get_global_endpoints_opt|get_global_lock_client|get_global_lock_clients|get_global_region|get_global_tier_config_mgr|global_rustfs_port|is_dist_erasure|is_erasure|is_erasure_sd|is_first_cluster_node_local|new_object_layer_fn|resolve_object_store_handle'

(
  cd "$ROOT_DIR"
  awk '/pub mod global \{/,/^}/ {print FILENAME ":" FNR ":" $0}' crates/ecstore/src/api/mod.rs |
    rg "\b(${READ_ONLY_ECSTORE_GLOBAL_SYMBOL_PATTERN})\b" || true
) >"$ECSTORE_GLOBAL_READ_ONLY_EXPORT_HITS_FILE"

if [[ -s "$ECSTORE_GLOBAL_READ_ONLY_EXPORT_HITS_FILE" ]]; then
  report_failure "read-only ECStore runtime getters must be exported through rustfs_ecstore::api::runtime, not api::global: $(paste -sd '; ' "$ECSTORE_GLOBAL_READ_ONLY_EXPORT_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  awk '/pub\(crate\) mod ecstore_global \{/,/^}/ {print FILENAME ":" FNR ":" $0}' rustfs/src/storage/storage_api.rs |
    rg "\b(${READ_ONLY_ECSTORE_GLOBAL_SYMBOL_PATTERN})\b" || true
) >"$RUSTFS_STORAGE_GLOBAL_READ_ONLY_IMPORT_HITS_FILE"

if [[ -s "$RUSTFS_STORAGE_GLOBAL_READ_ONLY_IMPORT_HITS_FILE" ]]; then
  report_failure "RustFS storage facade must source read-only ECStore runtime getters through api::runtime: $(paste -sd '; ' "$RUSTFS_STORAGE_GLOBAL_READ_ONLY_IMPORT_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '^use rustfs_storage_api|rustfs_storage_api::' \
    crates/heal/src/heal \
    crates/iam/src \
    crates/obs/src/metrics \
    crates/protocols/src/swift \
    crates/s3select-api/src \
    crates/scanner/src \
    --glob '*.rs' |
    rg -v '^(crates/heal/src/heal/storage_api.rs|crates/iam/src/storage_api.rs|crates/obs/src/metrics/storage_api.rs|crates/protocols/src/swift/storage_api.rs|crates/s3select-api/src/storage_api.rs|crates/scanner/src/storage_api.rs):' || true
) >"$EXTERNAL_RUNTIME_STORAGE_API_BYPASS_HITS_FILE"

if [[ -s "$EXTERNAL_RUNTIME_STORAGE_API_BYPASS_HITS_FILE" ]]; then
  report_failure "external runtime crates must source storage-api contracts through their local storage_api boundary: $(paste -sd '; ' "$EXTERNAL_RUNTIME_STORAGE_API_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'use (crate::|super::)?storage_api::\{|use storage_api::\{|\b(crate::|super::)?storage_api::(BucketInfo|BucketOperations|BucketOptions|DeleteBucketOptions|DiskSetSelector|ECStore|Erasure|HTTPRangeSpec|ListOperations|MakeBucketOptions|NamespaceLocking|ObjectIO|ObjectOperations|StorageAdminApi|TonicInterceptor|BucketTargetSys)\b' \
    crates/ecstore/benches \
    crates/ecstore/tests \
    crates/heal/src/heal \
    crates/heal/tests \
    crates/iam/src \
    crates/notify/src \
    crates/obs/src/metrics \
    crates/protocols/src/swift \
    crates/s3select-api/src \
    crates/scanner/src \
    crates/scanner/tests \
    crates/e2e_test/src \
    rustfs/src/admin/service/config.rs \
    rustfs/src/storage/rpc/node_service.rs \
    --glob '*.rs' \
    --glob '!**/storage_api.rs' \
    --glob '!**/storage_api/mod.rs' || true
) >"$EXTERNAL_STORAGE_API_DOMAIN_BYPASS_HITS_FILE"

if [[ -s "$EXTERNAL_STORAGE_API_DOMAIN_BYPASS_HITS_FILE" ]]; then
  report_failure "external local storage_api consumers must use domain modules instead of flat imports: $(paste -sd '; ' "$EXTERNAL_STORAGE_API_DOMAIN_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '^pub(?:\(crate\))? use rustfs_storage_api' \
    crates/ecstore/benches \
    crates/ecstore/tests \
    crates/heal/src/heal \
    crates/heal/tests \
    crates/iam/src \
    crates/obs/src/metrics \
    crates/protocols/src/swift \
    crates/s3select-api/src \
    crates/scanner/src \
    crates/scanner/tests \
    crates/e2e_test/src \
    --glob '**/storage_api.rs' \
    --glob '**/storage_api/mod.rs' || true
) >"$EXTERNAL_STORAGE_API_ROOT_REEXPORT_HITS_FILE"

if [[ -s "$EXTERNAL_STORAGE_API_ROOT_REEXPORT_HITS_FILE" ]]; then
  report_failure "external local storage_api boundaries must expose storage-api contracts from consumer-domain modules, not root re-exports: $(paste -sd '; ' "$EXTERNAL_STORAGE_API_ROOT_REEXPORT_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '^pub(?:\(crate\))? use rustfs_storage_api' \
    rustfs/src/storage_api.rs \
    rustfs/src/storage/storage_api.rs \
    rustfs/src/admin/storage_api.rs \
    rustfs/src/app/storage_api.rs || true
) >"$RUSTFS_STORAGE_API_ROOT_REEXPORT_HITS_FILE"

if [[ -s "$RUSTFS_STORAGE_API_ROOT_REEXPORT_HITS_FILE" ]]; then
  report_failure "RustFS local storage_api boundaries must expose storage-api contracts from domain modules, not root re-exports: $(paste -sd '; ' "$RUSTFS_STORAGE_API_ROOT_REEXPORT_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '^pub(?:\(crate\))? use crate::storage::' \
    rustfs/src/admin/storage_api.rs \
    rustfs/src/app/storage_api.rs || true
) >"$RUSTFS_APP_ADMIN_STORAGE_HELPER_ROOT_REEXPORT_HITS_FILE"

if [[ -s "$RUSTFS_APP_ADMIN_STORAGE_HELPER_ROOT_REEXPORT_HITS_FILE" ]]; then
  report_failure "RustFS app/admin storage_api boundaries must expose storage helpers from domain modules, not root re-exports: $(paste -sd '; ' "$RUSTFS_APP_ADMIN_STORAGE_HELPER_ROOT_REEXPORT_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'get_global_(?:action_cred|server_config|notification_sys)' \
    crates/iam/src \
    --glob '*.rs' |
    rg -v '^crates/iam/src/(runtime_sources|storage_api)\.rs:' || true
) >"$IAM_RUNTIME_SOURCE_BYPASS_HITS_FILE"

if [[ -s "$IAM_RUNTIME_SOURCE_BYPASS_HITS_FILE" ]]; then
  report_failure "IAM runtime-source globals must stay behind IAM runtime or storage boundaries: $(paste -sd '; ' "$IAM_RUNTIME_SOURCE_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'get_global_(?:kms_service_manager|encryption_service|iam_sys|action_cred|server_config|notification_sys|bucket_metadata_sys|bucket_monitor|replication_pool|replication_stats|boot_time|endpoints_opt|deployment_id|lock_client|lock_clients|region|tier_config_mgr|expiry_state|local_node_name|buffer_config|db)|init_global_kms_service_manager|set_global_(?:server_config|storage_class|outbound_tls_generation)|load_global_outbound_tls_(?:generation|state)|global_(?:rustfs_port|internode_metrics)|get_daily_all_tier_stats|collect_scanner_metrics_report|new_object_layer_fn|notifier_global::' \
    rustfs/src/app/context.rs \
    rustfs/src/app/context \
    --glob '*.rs' |
    rg -v '^rustfs/src/app/context/runtime_sources\.rs:' || true
) >"$RUSTFS_APP_CONTEXT_RUNTIME_SOURCE_BYPASS_HITS_FILE"

if [[ -s "$RUSTFS_APP_CONTEXT_RUNTIME_SOURCE_BYPASS_HITS_FILE" ]]; then
  report_failure "RustFS AppContext fallback runtime globals must stay behind rustfs/src/app/context/runtime_sources.rs: $(paste -sd '; ' "$RUSTFS_APP_CONTEXT_RUNTIME_SOURCE_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '\b(?:init_global_action_credentials|set_global_region|set_global_rustfs_port|set_global_addr|set_global_init_time_now|init_global_kms_service_manager|init_global_buffer_config|set_buffer_profile_enabled)\b|rustfs_obs::(?:\{[^}]*\b(?:init_obs|set_global_guard)\b|(?:init_obs|set_global_guard|observability_metric_enabled|init_metrics_runtime)\b)|rustfs_io_metrics::set_put_stage_metrics_enabled\b|\b(?:publish_global_outbound_tls_state|record_tls_generation|record_tls_reload_result|record_tls_reload_skipped|init_tls_metrics)\b' \
    rustfs/src/init.rs \
    rustfs/src/startup_*.rs \
    rustfs/src/server/tls_material.rs \
    --glob '*.rs' |
    rg -v 'startup_runtime_sources::' |
    rg -v '^rustfs/src/startup_runtime_sources\.rs:' || true
) >"$RUSTFS_STARTUP_RUNTIME_SOURCE_BYPASS_HITS_FILE"

if [[ -s "$RUSTFS_STARTUP_RUNTIME_SOURCE_BYPASS_HITS_FILE" ]]; then
  report_failure "RustFS startup runtime globals must stay behind rustfs/src/startup_runtime_sources.rs: $(paste -sd '; ' "$RUSTFS_STARTUP_RUNTIME_SOURCE_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'crate::app::context::|use crate::app::context|app::context::' \
    rustfs/src/startup_runtime_sources.rs \
    --glob '*.rs' || true
) >"$RUSTFS_STARTUP_APP_CONTEXT_ENTRYPOINT_HITS_FILE"

if [[ -s "$RUSTFS_STARTUP_APP_CONTEXT_ENTRYPOINT_HITS_FILE" ]]; then
  report_failure "RustFS startup AppContext reads must use rustfs/src/runtime_sources.rs entrypoints: $(paste -sd '; ' "$RUSTFS_STARTUP_APP_CONTEXT_ENTRYPOINT_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'crate::app::context::(?:\{[^}]*\b(?:resolve_server_config|resolve_notify_interface|resolve_object_store_handle|resolve_kms_runtime_service_manager|resolve_iam_ready|resolve_endpoints_handle|resolve_lock_clients_handle)\b|(?:resolve_server_config|resolve_notify_interface|resolve_object_store_handle|resolve_kms_runtime_service_manager|resolve_iam_ready|resolve_endpoints_handle|resolve_lock_clients_handle)\b)' \
    rustfs/src/server/audit.rs \
    rustfs/src/server/event.rs \
    rustfs/src/server/layer.rs \
    rustfs/src/server/module_switch.rs \
    rustfs/src/server/readiness.rs \
    --glob '*.rs' || true
) >"$RUSTFS_SERVER_RUNTIME_SOURCE_BYPASS_HITS_FILE"

if [[ -s "$RUSTFS_SERVER_RUNTIME_SOURCE_BYPASS_HITS_FILE" ]]; then
  report_failure "RustFS server runtime source reads must stay behind rustfs/src/server/runtime_sources.rs: $(paste -sd '; ' "$RUSTFS_SERVER_RUNTIME_SOURCE_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'crate::app::context::|use crate::app::context|app::context::' \
    rustfs/src/server/runtime_sources.rs \
    --glob '*.rs' || true
) >"$RUSTFS_SERVER_APP_CONTEXT_ENTRYPOINT_HITS_FILE"

if [[ -s "$RUSTFS_SERVER_APP_CONTEXT_ENTRYPOINT_HITS_FILE" ]]; then
  report_failure "RustFS server AppContext reads must use rustfs/src/runtime_sources.rs entrypoints: $(paste -sd '; ' "$RUSTFS_SERVER_APP_CONTEXT_ENTRYPOINT_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'crate::app::context::|use crate::app::context|app::context::' \
    rustfs/src/admin/runtime_sources.rs \
    rustfs/src/app/runtime_sources.rs \
    rustfs/src/storage/runtime_sources.rs \
    --glob '*.rs' || true
) >"$RUSTFS_OWNER_RUNTIME_APP_CONTEXT_ENTRYPOINT_HITS_FILE"

if [[ -s "$RUSTFS_OWNER_RUNTIME_APP_CONTEXT_ENTRYPOINT_HITS_FILE" ]]; then
  report_failure "RustFS owner runtime AppContext reads must use rustfs/src/runtime_sources.rs entrypoints: $(paste -sd '; ' "$RUSTFS_OWNER_RUNTIME_APP_CONTEXT_ENTRYPOINT_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'crate::app::context::(?:\{[^}]*\b(?:resolve_object_store_handle|resolve_buffer_config|resolve_internode_metrics|resolve_local_node_name|resolve_action_credentials|resolve_notify_interface|resolve_performance_metrics|resolve_encryption_service|resolve_region|resolve_ready_iam_handle)\b|(?:resolve_object_store_handle|resolve_buffer_config|resolve_internode_metrics|resolve_local_node_name|resolve_action_credentials|resolve_notify_interface|resolve_performance_metrics|resolve_encryption_service|resolve_region|resolve_ready_iam_handle)\b)' \
    rustfs/src/storage/access.rs \
    rustfs/src/storage/ecfs.rs \
    rustfs/src/storage/ecfs_extend.rs \
    rustfs/src/storage/helper.rs \
    rustfs/src/storage/concurrency/manager.rs \
    rustfs/src/storage/sse.rs \
    rustfs/src/storage/rpc/disk.rs \
    rustfs/src/storage/rpc/health.rs \
    rustfs/src/storage/rpc/http_service.rs \
    --glob '*.rs' || true
) >"$RUSTFS_STORAGE_RUNTIME_SOURCE_BYPASS_HITS_FILE"

if [[ -s "$RUSTFS_STORAGE_RUNTIME_SOURCE_BYPASS_HITS_FILE" ]]; then
  report_failure "RustFS storage runtime source reads must stay behind rustfs/src/storage/runtime_sources.rs: $(paste -sd '; ' "$RUSTFS_STORAGE_RUNTIME_SOURCE_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'crate::app::(?:bucket_usecase|multipart_usecase|object_usecase)|Default(?:Bucket|Multipart|Object)Usecase::from_global\(\)' \
    rustfs/src/storage/ecfs.rs \
    --glob '*.rs' || true
) >"$RUSTFS_STORAGE_ECFS_USECASE_BYPASS_HITS_FILE"

if [[ -s "$RUSTFS_STORAGE_ECFS_USECASE_BYPASS_HITS_FILE" ]]; then
  report_failure "RustFS storage ECFS S3 routes must construct app usecases through rustfs/src/storage/s3_api/mod.rs: $(paste -sd '; ' "$RUSTFS_STORAGE_ECFS_USECASE_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'pub\(crate\) use .*get_global_app_context|get_global_app_context.*pub\(crate\) use' \
    rustfs/src/storage/runtime_sources.rs \
    --glob '*.rs' || true
) >"$RUSTFS_STORAGE_GLOBAL_APP_CONTEXT_REEXPORT_HITS_FILE"

if [[ -s "$RUSTFS_STORAGE_GLOBAL_APP_CONTEXT_REEXPORT_HITS_FILE" ]]; then
  report_failure "RustFS storage AppContext lookup must stay wrapped by rustfs/src/storage/runtime_sources.rs: $(paste -sd '; ' "$RUSTFS_STORAGE_GLOBAL_APP_CONTEXT_REEXPORT_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'crate::app::context::|use crate::app::context|app::context::' rustfs/src/admin \
    | rg -v '^rustfs/src/admin/runtime_sources\.rs:' || true
) >"$RUSTFS_ADMIN_RUNTIME_SOURCE_BYPASS_HITS_FILE"

if [[ -s "$RUSTFS_ADMIN_RUNTIME_SOURCE_BYPASS_HITS_FILE" ]]; then
  report_failure "RustFS admin runtime source reads must stay behind rustfs/src/admin/runtime_sources.rs: $(paste -sd '; ' "$RUSTFS_ADMIN_RUNTIME_SOURCE_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'crate::app::context::|use crate::app::context|app::context::' rustfs/src --glob '*.rs' |
    rg -v '^rustfs/src/app/context' |
    rg -v '(^rustfs/src/[^/]*runtime_sources\.rs:|/runtime_sources\.rs:)' || true
) >"$RUSTFS_APP_CONTEXT_DIRECT_BYPASS_HITS_FILE"

if [[ -s "$RUSTFS_APP_CONTEXT_DIRECT_BYPASS_HITS_FILE" ]]; then
  report_failure "RustFS AppContext resolver reads must stay behind context or runtime_sources modules: $(paste -sd '; ' "$RUSTFS_APP_CONTEXT_DIRECT_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'crate::app::context::|use crate::app::context|app::context::' rustfs/src/app --glob '*.rs' |
    rg -v '^rustfs/src/app/context' |
    rg -v '^rustfs/src/app/runtime_sources\.rs:' || true
) >"$RUSTFS_APP_USECASE_RUNTIME_SOURCE_BYPASS_HITS_FILE"

if [[ -s "$RUSTFS_APP_USECASE_RUNTIME_SOURCE_BYPASS_HITS_FILE" ]]; then
  report_failure "RustFS app usecase runtime source reads must stay behind rustfs/src/app/runtime_sources.rs: $(paste -sd '; ' "$RUSTFS_APP_USECASE_RUNTIME_SOURCE_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'use crate::storage::\*;' rustfs/src/app --glob '*_usecase.rs' || true
) >"$RUSTFS_APP_USECASE_STORAGE_WILDCARD_HITS_FILE"

if [[ -s "$RUSTFS_APP_USECASE_STORAGE_WILDCARD_HITS_FILE" ]]; then
  report_failure "RustFS app usecase consumers must import storage owner APIs explicitly instead of crate::storage::*: $(paste -sd '; ' "$RUSTFS_APP_USECASE_STORAGE_WILDCARD_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'use (s3s::dto|crate::storage::ecfs)::\*;' rustfs/src/app --glob '*.rs' || true
) >"$RUSTFS_APP_WILDCARD_IMPORT_HITS_FILE"

if [[ -s "$RUSTFS_APP_WILDCARD_IMPORT_HITS_FILE" ]]; then
  report_failure "RustFS app consumers must import S3 DTO and ECFS owner APIs explicitly instead of wildcard imports: $(paste -sd '; ' "$RUSTFS_APP_WILDCARD_IMPORT_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'crate::storage::s3_api::|use crate::storage::s3_api|super::s3_api::|use super::s3_api' rustfs/src/app --glob '*_usecase.rs' || true
) >"$RUSTFS_APP_USECASE_S3_API_BYPASS_HITS_FILE"

if [[ -s "$RUSTFS_APP_USECASE_S3_API_BYPASS_HITS_FILE" ]]; then
  report_failure "RustFS app usecases must consume S3 API helpers through rustfs/src/app/storage_api.rs: $(paste -sd '; ' "$RUSTFS_APP_USECASE_S3_API_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  {
    rg -n --with-filename \
      '(use crate::storage::(access|helper|options|request_context|sse|timeout_wrapper|head_prefix|concurrency|ecfs)|crate::storage::sse::EncryptionKeyKind|use crate::storage::\{|use crate::storage::[A-Z])' \
      rustfs/src/app/select_object.rs rustfs/src/app/*_usecase.rs || true
    rg -n --with-filename \
      'use super::(?:\{[^}]*\b(?:DynReader|HashReader|WriteEncryption|WritePlan|DecryptReader|EncryptReader|HardLimitReader|boxed_reader|wrap_reader|compression_metadata_value|is_disk_compressible|MIN_DISK_COMPRESSIBLE_SIZE|get_lock_acquire_timeout|is_valid_storage_class|StorageError|DiskError|is_all_buckets_not_found|is_err_bucket_not_found|is_err_object_not_found|is_err_version_not_found)\b|(?:object_api_utils::to_s3s_etag|storageclass|StorageError|DiskError|DynReader|HashReader|WriteEncryption|WritePlan|DecryptReader|EncryptReader|HardLimitReader|boxed_reader|wrap_reader|compression_metadata_value|is_disk_compressible|MIN_DISK_COMPRESSIBLE_SIZE|get_lock_acquire_timeout|is_valid_storage_class|is_all_buckets_not_found|is_err_bucket_not_found|is_err_object_not_found|is_err_version_not_found)\b)' \
      rustfs/src/app/bucket_usecase.rs rustfs/src/app/object_usecase.rs rustfs/src/app/multipart_usecase.rs rustfs/src/app/lifecycle_transition_api_test.rs || true
    rg -n --with-filename \
      'use super::(?:\{[^}]*\b(?:AppObjectLockConfigExt|AppReplicationConfigExt|AppVersioningConfigExt|predict_lifecycle_expiration|validate_restore_request|bucket_target_sys|lifecycle|metadata|metadata_sys|object_lock|policy_sys|quota|replication|tagging|target|utils|versioning_sys|transition_api|ObjectInfo|ObjectOptions)\b|(?:AppObjectLockConfigExt|AppReplicationConfigExt|AppVersioningConfigExt|predict_lifecycle_expiration|validate_restore_request|bucket_target_sys|lifecycle|metadata|metadata_sys|object_lock|policy_sys|quota|replication|tagging|target|utils|versioning_sys|transition_api|ObjectInfo|ObjectOptions)\b)|super::(?:lifecycle|metadata_sys|object_lock|quota|replication|tagging|target|utils|versioning_sys|transition_api)::|super::super::(?:metadata_sys|lifecycle|target)::' \
      rustfs/src/app/bucket_usecase.rs rustfs/src/app/object_usecase.rs rustfs/src/app/multipart_usecase.rs rustfs/src/app/lifecycle_transition_api_test.rs rustfs/src/app/capacity_dirty_scope_test.rs rustfs/src/app/context.rs rustfs/src/app/context/handles.rs rustfs/src/app/context/interfaces.rs rustfs/src/app/context/runtime_sources.rs || true
  }
) >"$RUSTFS_APP_USECASE_STORAGE_API_BYPASS_HITS_FILE"

if [[ -s "$RUSTFS_APP_USECASE_STORAGE_API_BYPASS_HITS_FILE" ]]; then
  report_failure "RustFS app usecases must consume storage helper APIs through rustfs/src/app/storage_api.rs: $(paste -sd '; ' "$RUSTFS_APP_USECASE_STORAGE_API_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename \
    '(use crate::storage::(access|request_context|ecfs)|use crate::storage::\{[^}]*Storage(ObjectInfo|ObjectOptions|PutObjReader)|crate::storage::StorageObjectOptions)' \
    rustfs/src/app/*_test.rs \
    rustfs/src/admin/router.rs \
    rustfs/src/admin/console.rs \
    rustfs/src/admin/handlers/heal.rs \
    rustfs/src/admin/handlers/metrics.rs \
    rustfs/src/admin/handlers/object_zip_download.rs || true
) >"$RUSTFS_APP_ADMIN_STORAGE_API_BYPASS_HITS_FILE"

if [[ -s "$RUSTFS_APP_ADMIN_STORAGE_API_BYPASS_HITS_FILE" ]]; then
  report_failure "RustFS app/admin completed storage helper consumers must use local storage_api boundaries: $(paste -sd '; ' "$RUSTFS_APP_ADMIN_STORAGE_API_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  {
    rg -n --with-filename \
      'use super::(?:\{[^}]*\b(?:ECStore|EndpointServerPools|Endpoint|Endpoints|PoolEndpoints|StorageClassConfig|TierConfigMgr|DailyAllTierStats|ScannerMetricsReport|ReplicationStats|DynReplicationPool|NotificationSys|BucketBandwidthMonitor|ExpiryState|AppWarmBackend|WarmBackendGetOpts|TierConfig|TierType|PoolStatus|PoolDecommissionInfo|RebalStatus)\b|(?:ECStore|EndpointServerPools|Endpoint|Endpoints|PoolEndpoints|StorageClassConfig|TierConfigMgr|DailyAllTierStats|ScannerMetricsReport|ReplicationStats|DynReplicationPool|NotificationSys|BucketBandwidthMonitor|ExpiryState|AppWarmBackend|WarmBackendGetOpts|TierConfig|TierType|PoolStatus|PoolDecommissionInfo|RebalStatus)\b)' \
      rustfs/src/app \
      --glob '*.rs' \
      --glob '!rustfs/src/app/storage_api.rs' || true
    rg -n --with-filename \
      'super::(?:get_server_info|get_total_usable_capacity|get_total_usable_capacity_free|apply_bucket_usage_memory_overlay|load_data_usage_from_backend|record_bucket_object_delete_memory|record_bucket_object_write_memory|remove_bucket_usage_from_backend|get_global_|global_rustfs_port|new_object_layer_fn|set_object_store_resolver|set_global_storage_class|collect_scanner_metrics_report|init_local_disks)|super::super::(?:get_server_info|get_total_usable_capacity|get_total_usable_capacity_free|apply_bucket_usage_memory_overlay|load_data_usage_from_backend|record_bucket_object_delete_memory|record_bucket_object_write_memory|remove_bucket_usage_from_backend|get_global_|global_rustfs_port|new_object_layer_fn|set_object_store_resolver|set_global_storage_class|collect_scanner_metrics_report|init_local_disks)' \
      rustfs/src/app \
      --glob '*.rs' \
      --glob '!rustfs/src/app/storage_api.rs' || true
    rg -n --with-filename \
      'crate::app::(?:ECStore|EndpointServerPools|Endpoint|Endpoints|PoolEndpoints|StorageClassConfig|TierConfigMgr|DailyAllTierStats|ScannerMetricsReport|ReplicationStats|DynReplicationPool|NotificationSys|BucketBandwidthMonitor|ExpiryState|get_global_|set_object_store_resolver)' \
      rustfs/src --glob '*.rs' || true
    rg -n --with-filename \
      'ecstore_data_usage|pub\(crate\) async fn load_data_usage_from_backend|super::super::load_data_usage_from_backend|crate::admin::load_data_usage_from_backend' \
      rustfs/src/admin/mod.rs rustfs/src/admin/handlers/*.rs || true
  }
) >"$RUSTFS_APP_RUNTIME_STORAGE_API_BYPASS_HITS_FILE"

if [[ -s "$RUSTFS_APP_RUNTIME_STORAGE_API_BYPASS_HITS_FILE" ]]; then
  report_failure "RustFS app/admin runtime and data-usage facades must stay behind local storage_api boundaries: $(paste -sd '; ' "$RUSTFS_APP_RUNTIME_STORAGE_API_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  {
    rg -n --with-filename \
      '\b(?:ecstore_(?:bucket|capacity|client|cluster|config|disk|error|layout|metrics|notification|rebalance|rpc|storage|tier)|AdminReplicationConfigExt|AdminVersioningConfigExt|RUSTFS_META_BUCKET|STORAGE_CLASS_SUB_SYS|read_admin_config|read_admin_config_without_migrate|save_admin_config|save_admin_server_config|delete_admin_config|init_admin_config_defaults|StorageError|ECStore|Endpoint|Endpoints|EndpointServerPools|PoolEndpoints|PeerRestClient|RebalanceStats|RebalanceMeta|RebalanceStopPropagationRecord)\b' \
      rustfs/src/admin/mod.rs || true
    rg -n --with-filename \
      '(?:crate::admin|super(?::super)+)::(?:bandwidth|bucket_target_sys|lifecycle|metadata|metadata_sys|quota|replication|target|versioning|versioning_sys|storageclass|tier|AdminReplicationConfigExt|AdminVersioningConfigExt|RUSTFS_META_BUCKET|STORAGE_CLASS_SUB_SYS|read_admin_config|read_admin_config_without_migrate|save_admin_config|save_admin_server_config|delete_admin_config|init_admin_config_defaults|StorageError|Error|ECStore|Endpoint|Endpoints|EndpointServerPools|PoolEndpoints|PeerRestClient|RebalanceStats|RebalanceMeta|RebalanceStopPropagationRecord|collect_local_metrics|is_reserved_or_invalid_bucket)\b' \
      rustfs/src/admin \
      -g '*.rs' \
      -g '!storage_api.rs' || true
  }
) >"$RUSTFS_ADMIN_STORAGE_API_ROOT_FACADE_HITS_FILE"

if [[ -s "$RUSTFS_ADMIN_STORAGE_API_ROOT_FACADE_HITS_FILE" ]]; then
  report_failure "RustFS admin storage facades must stay behind rustfs/src/admin/storage_api.rs: $(paste -sd '; ' "$RUSTFS_ADMIN_STORAGE_API_ROOT_FACADE_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'use crate::storage(?:;|::|\s*\{)|crate::storage::' \
    rustfs/src \
    --glob '*.rs' \
    --glob '!rustfs/src/admin/**' \
    --glob '!rustfs/src/app/**' \
    --glob '!rustfs/src/storage/**' \
    --glob '!rustfs/src/storage_api.rs' || true
) >"$RUSTFS_ROOT_STORAGE_API_BYPASS_HITS_FILE"

if [[ -s "$RUSTFS_ROOT_STORAGE_API_BYPASS_HITS_FILE" ]]; then
  report_failure "RustFS root/server/startup storage facades must stay behind rustfs/src/storage_api.rs: $(paste -sd '; ' "$RUSTFS_ROOT_STORAGE_API_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '^use rustfs_storage_api|rustfs_storage_api::' \
    rustfs/src \
    --glob '*.rs' \
    --glob '!rustfs/src/admin/**' \
    --glob '!rustfs/src/app/**' \
    --glob '!rustfs/src/storage/**' \
    --glob '!rustfs/src/storage_api.rs' || true
) >"$RUSTFS_ROOT_STORAGE_API_CONTRACT_BYPASS_HITS_FILE"

if [[ -s "$RUSTFS_ROOT_STORAGE_API_CONTRACT_BYPASS_HITS_FILE" ]]; then
  report_failure "RustFS root/server/startup storage contracts must stay behind rustfs/src/storage_api.rs: $(paste -sd '; ' "$RUSTFS_ROOT_STORAGE_API_CONTRACT_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename \
    'use crate::storage_api(?:\s*(?:;|as\b)|::\{)|crate::storage_api::(?:access|concurrency|deadlock_detector|ecfs|ecstore_cluster|request_context|rpc|tonic_service)\b' \
    rustfs/src \
    --glob '*.rs' \
    --glob '!rustfs/src/storage_api.rs' || true
) >"$RUSTFS_ROOT_STORAGE_API_DOMAIN_BYPASS_HITS_FILE"

if [[ -s "$RUSTFS_ROOT_STORAGE_API_DOMAIN_BYPASS_HITS_FILE" ]]; then
  report_failure "RustFS root storage-api consumers must use domain modules from rustfs/src/storage_api.rs: $(paste -sd '; ' "$RUSTFS_ROOT_STORAGE_API_DOMAIN_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n -U --with-filename \
    'crate::storage_api::(?:capacity::(?:\{|all_local_disk|disk_drive_path|disk_endpoint)|protocols::(?:access|ecfs|request_context)\b|server::(?:\{|contract\b|ecfs\b|request_context\b|rpc\b|tonic_service\b|apply_cors_headers|ECStore|Endpoint(?:s|ServerPools)?|Error|EventArgs|StorageObjectInfo|TONIC_RPC_PREFIX|is_dist_erasure|read_config|register_event_dispatch_hook|save_config|verify_rpc_signature)|startup::(?:\{|contract\b|concurrency\b|deadlock_detector\b|ecfs\b|DynReplicationPool|ECStore|EndpointServerPools|Error\b|Result\b|get_bucket_notification_config|init_background_replication|init_bucket_metadata_sys|init_ecstore_config|init_global_config_sys|init_local_disks|init_lock_clients|new_global_notification_sys|prewarm_local_disk_id_map|process_lambda_configurations|process_queue_configurations|process_topic_configurations|set_global_endpoints|set_global_region|set_global_rustfs_port|set_workload_admission_snapshot_provider|shutdown_background_services|try_migrate_bucket_metadata|try_migrate_iam_config|try_migrate_server_config|update_erasure_type)|workload::(?:\{|bucket_metadata_runtime_initialized|replication_queue_current_count))' \
    rustfs/src \
    --glob '*.rs' \
    --glob '!rustfs/src/storage_api.rs' || true
) >"$RUSTFS_ROOT_RUNTIME_FACADE_ROOT_CONSUMER_HITS_FILE"

if [[ -s "$RUSTFS_ROOT_RUNTIME_FACADE_ROOT_CONSUMER_HITS_FILE" ]]; then
  report_failure "RustFS root runtime facade consumers must use consumer-domain modules from rustfs/src/storage_api.rs: $(paste -sd '; ' "$RUSTFS_ROOT_RUNTIME_FACADE_ROOT_CONSUMER_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n -U --with-filename \
    'crate::storage::(?:contract::|StorageObjectInfo\b|StorageObjectOptions\b|to_s3s_etag\b)' \
    rustfs/src/storage \
    --glob '*.rs' \
    --glob '!rustfs/src/storage/storage_api.rs' || true
) >"$RUSTFS_STORAGE_OWNER_ROOT_FACADE_CONSUMER_HITS_FILE"

if [[ -s "$RUSTFS_STORAGE_OWNER_ROOT_FACADE_CONSUMER_HITS_FILE" ]]; then
  report_failure "RustFS storage-owner consumers must use storage_api consumer-domain modules instead of root storage facades: $(paste -sd '; ' "$RUSTFS_STORAGE_OWNER_ROOT_FACADE_CONSUMER_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n -U --with-filename \
    'crate::storage::(?:runtime_sources\b|ECStore\b)' \
    rustfs/src/storage \
    --glob '*.rs' \
    --glob '!rustfs/src/storage/storage_api.rs' || true
) >"$RUSTFS_STORAGE_OWNER_RUNTIME_ROOT_CONSUMER_HITS_FILE"

if [[ -s "$RUSTFS_STORAGE_OWNER_RUNTIME_ROOT_CONSUMER_HITS_FILE" ]]; then
  report_failure "RustFS storage-owner runtime consumers must use storage_api consumer-domain modules instead of root storage runtime facades: $(paste -sd '; ' "$RUSTFS_STORAGE_OWNER_RUNTIME_ROOT_CONSUMER_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n -U --with-filename \
    'crate::storage::(?:parse_object_lock_legal_hold\b|parse_object_lock_retention\b|validate_bucket_object_lock_enabled\b)' \
    rustfs/src/storage \
    --glob '*.rs' \
    --glob '!rustfs/src/storage/storage_api.rs' || true
) >"$RUSTFS_STORAGE_OWNER_HELPER_ROOT_CONSUMER_HITS_FILE"

if [[ -s "$RUSTFS_STORAGE_OWNER_HELPER_ROOT_CONSUMER_HITS_FILE" ]]; then
  report_failure "RustFS storage-owner helper consumers must use storage_api consumer-domain modules instead of root storage helpers: $(paste -sd '; ' "$RUSTFS_STORAGE_OWNER_HELPER_ROOT_CONSUMER_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n -U --with-filename \
    '(?:use\s+super::super::(?:StorageDiskRpcExt|WalkDirOptions|find_local_disk_by_ref|verify_rpc_signature)|super::super::Result<|super::super::super::(?:Error|STORAGE_CLASS_SUB_SYS))' \
    rustfs/src/storage/rpc/http_service.rs \
    rustfs/src/storage/rpc/node_service.rs || true
) >"$RUSTFS_STORAGE_OWNER_RPC_ROOT_CONSUMER_HITS_FILE"

if [[ -s "$RUSTFS_STORAGE_OWNER_RPC_ROOT_CONSUMER_HITS_FILE" ]]; then
  report_failure "RustFS storage-owner RPC consumers must use storage_api consumer-domain modules instead of relative root storage facades: $(paste -sd '; ' "$RUSTFS_STORAGE_OWNER_RPC_ROOT_CONSUMER_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename \
    '^[[:space:]]*use super::\*;' \
    rustfs/src/storage \
    --glob '*.rs' || true
) >"$RUSTFS_STORAGE_OWNER_WILDCARD_IMPORT_HITS_FILE"

if [[ -s "$RUSTFS_STORAGE_OWNER_WILDCARD_IMPORT_HITS_FILE" ]]; then
  report_failure "RustFS storage-owner modules must use explicit imports instead of parent wildcard imports: $(paste -sd '; ' "$RUSTFS_STORAGE_OWNER_WILDCARD_IMPORT_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename \
    '^pub\(crate\) use storage_api::\*;' \
    rustfs/src/storage/mod.rs || true
) >"$RUSTFS_STORAGE_OWNER_ROOT_EXPORT_GLOB_HITS_FILE"

if [[ -s "$RUSTFS_STORAGE_OWNER_ROOT_EXPORT_GLOB_HITS_FILE" ]]; then
  report_failure "RustFS storage owner root must explicitly list storage_api re-exports instead of using a wildcard: $(paste -sd '; ' "$RUSTFS_STORAGE_OWNER_ROOT_EXPORT_GLOB_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  perl -0ne '
    my $source = $_;
    $source =~ s{//[^\n]*}{ }g;
    $source =~ s{/\*.*?\*/}{
      my $comment = $&;
      $comment =~ s/[^\n]/ /g;
      $comment;
    }egs;
    while ($source =~ /^[[:space:]]*pub(?:\(crate\))?\s+use\s+storage_api::([^;]+);/mg) {
      my $start = $-[0];
      my $body = $1;
      next unless $body =~ /\b(ObjectStoreResolver|set_object_store_resolver|ecstore_global|get_global_[A-Za-z0-9_]+|set_global_[A-Za-z0-9_]+|global_rustfs_port|is_dist_erasure|shutdown_background_services|update_erasure_type|init_global_config_sys|new_global_notification_sys|set_workload_admission_snapshot_provider|register_event_dispatch_hook|reload_transition_tier_config|replication_queue_current_count|bucket_metadata_runtime_initialized)\b/;
      my $prefix = substr($source, 0, $start);
      my $line = ($prefix =~ tr/\n//) + 1;
      print "$ARGV:$line:storage_api root re-export includes runtime/global facade symbols\n";
    }
  ' rustfs/src/storage/mod.rs || true
) >"$RUSTFS_STORAGE_OWNER_RUNTIME_ROOT_EXPORT_HITS_FILE"

if [[ -s "$RUSTFS_STORAGE_OWNER_RUNTIME_ROOT_EXPORT_HITS_FILE" ]]; then
  report_failure "RustFS storage owner root must not re-export ECStore runtime/global facades: $(paste -sd '; ' "$RUSTFS_STORAGE_OWNER_RUNTIME_ROOT_EXPORT_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '\bnew_object_layer_fn\b' \
    rustfs/src \
    --glob '*.rs' || true
) >"$RUSTFS_OBJECT_LAYER_FALLBACK_BYPASS_HITS_FILE"

if [[ -s "$RUSTFS_OBJECT_LAYER_FALLBACK_BYPASS_HITS_FILE" ]]; then
  report_failure "RustFS AppContext/storage facades must not use the legacy object-layer fallback helper: $(paste -sd '; ' "$RUSTFS_OBJECT_LAYER_FALLBACK_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg --pcre2 -n --with-filename \
    '(use crate::storage::\{|crate::storage::(?!storage_api\b))' \
    rustfs/src/storage_api.rs \
    rustfs/src/app/storage_api.rs \
    rustfs/src/admin/storage_api.rs || true
) >"$RUSTFS_STORAGE_FACADE_DIRECT_OWNER_BYPASS_HITS_FILE"

if [[ -s "$RUSTFS_STORAGE_FACADE_DIRECT_OWNER_BYPASS_HITS_FILE" ]]; then
  report_failure "RustFS root/app/admin storage facades must consume owner APIs through crate::storage::storage_api: $(paste -sd '; ' "$RUSTFS_STORAGE_FACADE_DIRECT_OWNER_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename \
    '^pub\(crate\) use sse::' \
    rustfs/src/storage/mod.rs || true
) >"$RUSTFS_STORAGE_OWNER_SSE_ROOT_EXPORT_HITS_FILE"

if [[ -s "$RUSTFS_STORAGE_OWNER_SSE_ROOT_EXPORT_HITS_FILE" ]]; then
  report_failure "RustFS storage owner root must expose SSE helpers through storage_api instead of root sse re-exports: $(paste -sd '; ' "$RUSTFS_STORAGE_OWNER_SSE_ROOT_EXPORT_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n -U --with-filename \
    'use\s+(?:super::super|crate::storage)::\{[^}]*?(?:BucketMetadata|DEFAULT_READ_BUFFER_SIZE|bucket_metadata_sys_initialized|get_global_bucket_metadata_sys|set_bucket_metadata|apply_cors_headers|apply_default_lock_retention_metadata|check_preconditions|decode_tags_to_map|get_adaptive_buffer_size_with_profile|get_buffer_size_opt_in|is_etag_equal|matches_origin_pattern|parse_etag|parse_object_lock_legal_hold|parse_object_lock_retention|process_lambda_configurations|process_queue_configurations|process_topic_configurations|remove_object_lock_metadata_for_copy|remove_object_lock_retention_metadata|validate_bucket_object_lock_enabled|validate_list_object_unordered_with_delimiter)' \
    rustfs/src/storage/ecfs_test.rs || true
) >"$RUSTFS_STORAGE_OWNER_TEST_ROOT_CONSUMER_HITS_FILE"

if [[ -s "$RUSTFS_STORAGE_OWNER_TEST_ROOT_CONSUMER_HITS_FILE" ]]; then
  report_failure "RustFS storage-owner tests must use storage_api test consumer modules instead of root storage helper facades: $(paste -sd '; ' "$RUSTFS_STORAGE_OWNER_TEST_ROOT_CONSUMER_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  {
    rg -n -U --with-filename \
      'storage_api::cluster::\{[^}]*?(CapabilitySnapshotError|CapabilityState|CapabilityStatus|DiskCapabilities|MemorySamplingState|ObservabilitySnapshot|ObservabilitySnapshotProvider|PlatformSupport|TopologyCapabilities|TopologySnapshot|TopologySnapshotProvider|UserspaceProfilingCapability)' \
      rustfs/src \
      --glob '*.rs' \
      --glob '!rustfs/src/storage_api.rs' \
      --glob '!rustfs/src/admin/**' \
      --glob '!rustfs/src/app/**' \
      --glob '!rustfs/src/storage/**' || true
    rg -n --with-filename \
      'storage_api::cluster::(?:CapabilitySnapshotError|CapabilityState|CapabilityStatus|DiskCapabilities|MemorySamplingState|ObservabilitySnapshot|ObservabilitySnapshotProvider|PlatformSupport|TopologyCapabilities|TopologySnapshot|TopologySnapshotProvider|UserspaceProfilingCapability)\b|storage_api::server::(?:StorageAdminApi|TransitionedObject)\b|storage_api::startup::(?:BucketOperations|BucketOptions)\b|storage_api::table::(?:HTTPPreconditions|HTTPRangeSpec|ListObjectVersionsInfo|ListObjectsV2Info|ListOperations|NamespaceLocking|ObjectIO|ObjectInfoOrErr|ObjectOperations|WalkOptions)\b|storage_api::error::HTTPRangeError\b' \
      rustfs/src \
      --glob '*.rs' \
      --glob '!rustfs/src/storage_api.rs' \
      --glob '!rustfs/src/admin/**' \
      --glob '!rustfs/src/app/**' \
      --glob '!rustfs/src/storage/**' || true
    rg -n -U --with-filename \
      'storage_api::(?:server::\{[^}]*?(StorageAdminApi|TransitionedObject)|startup::\{[^}]*?(BucketOperations|BucketOptions)|table::\{[^}]*?(HTTPPreconditions|HTTPRangeSpec|ListObjectVersionsInfo|ListObjectsV2Info|ListOperations|NamespaceLocking|ObjectIO|ObjectInfoOrErr|ObjectOperations|WalkOptions)|error::\{[^}]*?HTTPRangeError)' \
      rustfs/src \
      --glob '*.rs' \
      --glob '!rustfs/src/storage_api.rs' \
      --glob '!rustfs/src/admin/**' \
      --glob '!rustfs/src/app/**' \
      --glob '!rustfs/src/storage/**' || true
  }
) >"$RUSTFS_ROOT_STORAGE_CONTRACT_ROOT_CONSUMER_HITS_FILE"

if [[ -s "$RUSTFS_ROOT_STORAGE_CONTRACT_ROOT_CONSUMER_HITS_FILE" ]]; then
  report_failure "RustFS root storage consumers must import storage contracts from domain modules, not root storage_api facades: $(paste -sd '; ' "$RUSTFS_ROOT_STORAGE_CONTRACT_ROOT_CONSUMER_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename \
    '(?:crate::admin|super)::storage_api::(?:ecstore_cluster|ecstore_utils|Error|StorageError|ECStore|Endpoint|EndpointServerPools|Endpoints|PoolEndpoints|PeerRestClient|RequestContext|ReqInfo|authorize_request|spawn_traced|CollectMetricsOpts|MetricType|collect_local_metrics|BucketOperations|BucketOptions|DeleteBucketOptions|HealOperations|ListOperations|MakeBucketOptions|ObjectIO|ObjectOperations|StorageAdminApi|CapabilitySnapshotError|CapabilityState|CapabilityStatus|ObservabilitySnapshot|ObservabilitySnapshotProvider|TopologySnapshot|TopologySnapshotProvider|read_admin_config|read_admin_config_without_migrate|save_admin_config|save_admin_server_config|delete_admin_config|init_admin_config_defaults|STORAGE_CLASS_SUB_SYS|RebalanceStats|RebalanceMeta|RebalanceStopPropagationRecord|StorageObjectOptions|is_reserved_or_invalid_bucket)\b|use (?:crate::admin|super)::storage_api::\{' \
    rustfs/src/admin \
    --glob '*.rs' \
    --glob '!rustfs/src/admin/storage_api.rs' || true
) >"$RUSTFS_ADMIN_STORAGE_API_DOMAIN_BYPASS_HITS_FILE"

if [[ -s "$RUSTFS_ADMIN_STORAGE_API_DOMAIN_BYPASS_HITS_FILE" ]]; then
  report_failure "RustFS admin storage-api consumers must use admin domain modules from rustfs/src/admin/storage_api.rs: $(paste -sd '; ' "$RUSTFS_ADMIN_STORAGE_API_DOMAIN_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '^use rustfs_storage_api|rustfs_storage_api::' \
    rustfs/src/app \
    --glob '*.rs' \
    --glob '!rustfs/src/app/storage_api.rs' || true
) >"$RUSTFS_APP_STORAGE_API_CONTRACT_BYPASS_HITS_FILE"

if [[ -s "$RUSTFS_APP_STORAGE_API_CONTRACT_BYPASS_HITS_FILE" ]]; then
  report_failure "RustFS app storage contracts must stay behind rustfs/src/app/storage_api.rs: $(paste -sd '; ' "$RUSTFS_APP_STORAGE_API_CONTRACT_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  {
    rg -n --with-filename \
      'storage_api::(?:admin_usecase|bucket_usecase|object_usecase|multipart_usecase|select_object)::(?:StorageAdminApi|BucketOperations|BucketOptions|DeleteBucketOptions|MakeBucketOptions|ListObjectVersionsInfo|ListObjectsV2Info|ListOperations|HTTPPreconditions|HTTPRangeSpec|NamespaceLocking|ObjectIO|ObjectOperations|CompletePart|MultipartOperations|MultipartUploadResult)\b' \
      rustfs/src/app \
      --glob '*.rs' \
      --glob '!storage_api.rs' || true
    rg -n -U --with-filename \
      'storage_api::test::\{[^}]*?(BucketOperations|BucketOptions|HealOperations|ListOperations|MakeBucketOptions|MultipartOperations|ObjectIO|ObjectOperations)' \
      rustfs/src/app \
      --glob '*.rs' \
      --glob '!storage_api.rs' || true
    rg -n --with-filename \
      'storage_api::test::(?:BucketOperations|BucketOptions|HealOperations|ListOperations|MakeBucketOptions|MultipartOperations|ObjectIO|ObjectOperations)\b' \
      rustfs/src/app \
      --glob '*.rs' \
      --glob '!storage_api.rs' || true
  }
) >"$RUSTFS_APP_USECASE_CONTRACT_ROOT_CONSUMER_HITS_FILE"

if [[ -s "$RUSTFS_APP_USECASE_CONTRACT_ROOT_CONSUMER_HITS_FILE" ]]; then
  report_failure "RustFS app usecase modules must import storage contracts from domain modules, not usecase root facades: $(paste -sd '; ' "$RUSTFS_APP_USECASE_CONTRACT_ROOT_CONSUMER_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename \
    '(?:crate::app|super)::storage_api::(?:ECStore|Endpoint|EndpointServerPools|Endpoints|PoolEndpoints|HTTPPreconditions|HealOperations|BucketOperations|BucketOptions|CompletePart|DeleteBucketOptions|HTTPRangeSpec|ListObjectVersionsInfo|ListObjectsV2Info|ListOperations|MakeBucketOptions|MultipartOperations|MultipartUploadResult|NamespaceLocking|ObjectIO|ObjectOperations|StorageAdminApi|StorageObjectInfo|StorageObjectOptions|StoragePutObjReader|access|admin|bucket|capacity|compression|concurrency|data_usage|deadlock_detector|ecfs|error|head_prefix|helper|io|object_utils|options|request_context|runtime|s3_api|set_disk|sse|storage_class|timeout_wrapper)\b|use super::storage_api::\{' \
    rustfs/src/app \
    --glob '*.rs' \
    --glob '!rustfs/src/app/storage_api.rs' || true
) >"$RUSTFS_APP_STORAGE_API_DOMAIN_BYPASS_HITS_FILE"

if [[ -s "$RUSTFS_APP_STORAGE_API_DOMAIN_BYPASS_HITS_FILE" ]]; then
  report_failure "RustFS app storage-api consumers must use app domain modules from rustfs/src/app/storage_api.rs: $(paste -sd '; ' "$RUSTFS_APP_STORAGE_API_DOMAIN_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '^use rustfs_storage_api|rustfs_storage_api::' \
    rustfs/src/admin \
    --glob '*.rs' \
    --glob '!rustfs/src/admin/storage_api.rs' || true
) >"$RUSTFS_ADMIN_STORAGE_API_CONTRACT_BYPASS_HITS_FILE"

if [[ -s "$RUSTFS_ADMIN_STORAGE_API_CONTRACT_BYPASS_HITS_FILE" ]]; then
  report_failure "RustFS admin storage contracts must stay behind rustfs/src/admin/storage_api.rs: $(paste -sd '; ' "$RUSTFS_ADMIN_STORAGE_API_CONTRACT_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  {
    rg -n -U --with-filename 'crate::admin::storage_api::contract::\{\s*[A-Z]' \
      rustfs/src/admin \
      --glob '*.rs' \
      --glob '!storage_api.rs' || true
    rg -n --with-filename 'crate::admin::storage_api::contract::[A-Z]' \
      rustfs/src/admin \
      --glob '*.rs' \
      --glob '!storage_api.rs' || true
    rg -n -U --with-filename 'super::storage_api::contract::\{\s*[A-Z]' \
      rustfs/src/admin \
      --glob '*.rs' \
      --glob '!storage_api.rs' || true
    rg -n --with-filename 'super::storage_api::contract::[A-Z]' \
      rustfs/src/admin \
      --glob '*.rs' \
      --glob '!storage_api.rs' || true
  }
) >"$RUSTFS_ADMIN_CONTRACT_ROOT_CONSUMER_HITS_FILE"

if [[ -s "$RUSTFS_ADMIN_CONTRACT_ROOT_CONSUMER_HITS_FILE" ]]; then
  report_failure "RustFS admin modules must import storage contracts from domain modules, not the root contract facade: $(paste -sd '; ' "$RUSTFS_ADMIN_CONTRACT_ROOT_CONSUMER_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'crate::app::context::|use crate::app::context|app::context::' rustfs/src/storage --glob '*.rs' |
    rg -v '^rustfs/src/storage/runtime_sources\.rs:' || true
) >"$RUSTFS_STORAGE_DIRECT_APP_CONTEXT_BYPASS_HITS_FILE"

if [[ -s "$RUSTFS_STORAGE_DIRECT_APP_CONTEXT_BYPASS_HITS_FILE" ]]; then
  report_failure "RustFS storage runtime source reads must stay behind rustfs/src/storage/runtime_sources.rs: $(paste -sd '; ' "$RUSTFS_STORAGE_DIRECT_APP_CONTEXT_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'rustfs_ecstore::api::' \
    crates/heal/tests \
    crates/scanner/tests \
    crates/e2e_test/src \
    --glob '*.rs' \
    | rg -v '^(crates/e2e_test/src/storage_api\.rs|crates/heal/tests/(endpoint_index_test|heal_bug_fixes_test|heal_integration_test)/storage_api\.rs|crates/scanner/tests/storage_api/mod\.rs):' || true
) >"$EXTERNAL_TEST_ECSTORE_COMPAT_BYPASS_HITS_FILE"

if [[ -s "$EXTERNAL_TEST_ECSTORE_COMPAT_BYPASS_HITS_FILE" ]]; then
  report_failure "external test ECStore API imports must stay in local test storage_api boundaries: $(paste -sd '; ' "$EXTERNAL_TEST_ECSTORE_COMPAT_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'rustfs_ecstore::api::' \
    fuzz/fuzz_targets \
    --glob '*.rs' |
    rg -v '^fuzz/fuzz_targets/(bucket_validation_storage_api|path_containment_storage_api)\.rs:' || true
) >"$FUZZ_ECSTORE_COMPAT_BYPASS_HITS_FILE"

if [[ -s "$FUZZ_ECSTORE_COMPAT_BYPASS_HITS_FILE" ]]; then
  report_failure "fuzz ECStore API imports must stay in fuzz storage_api boundary: $(paste -sd '; ' "$FUZZ_ECSTORE_COMPAT_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'rustfs_ecstore::api::' \
    crates/e2e_test/src \
    crates/heal/src \
    crates/heal/tests \
    crates/iam/src \
    crates/notify/src \
    crates/obs/src \
    crates/protocols/src/swift \
    crates/s3select-api/src \
    crates/scanner/src \
    crates/scanner/tests \
    fuzz/fuzz_targets \
    --glob '*.rs' |
    rg -v '^(crates/e2e_test/src/storage_api\.rs|crates/heal/src/heal/storage_api\.rs|crates/heal/tests/(endpoint_index_test|heal_bug_fixes_test|heal_integration_test)/storage_api\.rs|crates/iam/src/storage_api\.rs|crates/notify/src/storage_api\.rs|crates/obs/src/metrics/storage_api\.rs|crates/protocols/src/swift/storage_api\.rs|crates/s3select-api/src/storage_api\.rs|crates/scanner/src/storage_api\.rs|crates/scanner/tests/storage_api/mod\.rs|fuzz/fuzz_targets/(bucket_validation_storage_api|path_containment_storage_api)\.rs):' || true
) >"$EXTERNAL_ECSTORE_API_BOUNDARY_HITS_FILE"

if [[ -s "$EXTERNAL_ECSTORE_API_BOUNDARY_HITS_FILE" ]]; then
  report_failure "external ECStore API facade imports must stay in local storage_api boundary files: $(paste -sd '; ' "$EXTERNAL_ECSTORE_API_BOUNDARY_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'rustfs_ecstore::api::bucket::replication' \
    rustfs/src crates/*/src crates/*/tests crates/ecstore/tests fuzz/fuzz_targets \
    --glob '*.rs' |
    rg -v '^(rustfs/src/(admin/storage_api|app/storage_api|storage/storage_api)\.rs|crates/ecstore/tests/storage_api\.rs|crates/obs/src/metrics/storage_api\.rs|crates/scanner/src/storage_api\.rs|crates/scanner/tests/storage_api/mod\.rs|fuzz/fuzz_targets/(bucket_validation_storage_api|path_containment_storage_api)\.rs):' || true
) >"$REPLICATION_FACADE_BYPASS_HITS_FILE"

if [[ -s "$REPLICATION_FACADE_BYPASS_HITS_FILE" ]]; then
  report_failure "replication facade imports must stay in local storage_api boundaries: $(paste -sd '; ' "$REPLICATION_FACADE_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  {
    rg -n --with-filename '\b(ObsReplicationStats|obs_get_global_replication_stats|replication_stats_handle|get_sr_metrics_for_node|get_proxy_stats|mrf_stats)\b' \
      crates/obs/src/metrics \
      --glob '*.rs' \
      --glob '!crates/obs/src/metrics/storage_api.rs' || true
    rg -n --with-filename 'rustfs_ecstore::api::bucket::replication' \
      crates/obs/src/metrics \
      --glob '*.rs' \
      --glob '!crates/obs/src/metrics/storage_api.rs' || true
  }
) >"$OBS_REPLICATION_STATS_BOUNDARY_BYPASS_HITS_FILE"

if [[ -s "$OBS_REPLICATION_STATS_BOUNDARY_BYPASS_HITS_FILE" ]]; then
  report_failure "obs replication stats access must stay behind crates/obs/src/metrics/storage_api.rs snapshot helpers: $(paste -sd '; ' "$OBS_REPLICATION_STATS_BOUNDARY_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'pub(?:\(crate\))?\s+use\s+(?:config|datatypes|replication_pool|replication_resyncer|replication_state|rule)::\*;' \
    crates/ecstore/src/bucket/replication/mod.rs || true
) >"$REPLICATION_FACADE_WILDCARD_EXPORT_HITS_FILE"

if [[ -s "$REPLICATION_FACADE_WILDCARD_EXPORT_HITS_FILE" ]]; then
  report_failure "replication facade must use explicit compatibility exports instead of wildcard re-exports: $(paste -sd '; ' "$REPLICATION_FACADE_WILDCARD_EXPORT_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'pub\s+(struct|enum)\s+(ResyncOpts|TargetReplicationResyncStatus|BucketReplicationResyncStatus|ResyncStatusType)\b' \
    crates/ecstore/src/bucket/replication \
    --glob '*.rs' || true
) >"$REPLICATION_RESYNC_CONTRACT_BACKSLIDE_HITS_FILE"

if [[ -s "$REPLICATION_RESYNC_CONTRACT_BACKSLIDE_HITS_FILE" ]]; then
  report_failure "resync DTO contracts must stay in crates/replication: $(paste -sd '; ' "$REPLICATION_RESYNC_CONTRACT_BACKSLIDE_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  {
    rg -n --with-filename 'ecstore_bucket::replication::(DynReplicationPool|ReplicationStats|get_global_replication_pool|get_global_replication_stats|init_background_replication)' \
      rustfs/src \
      --glob '*.rs' \
      --glob '!rustfs/src/storage/storage_api.rs' || true
    rg -n --with-filename '\b(get_sr_metrics_for_node|q_cache|inc_proxy)\b' \
      rustfs/src \
      --glob '*.rs' \
      --glob '!rustfs/src/storage/storage_api.rs' || true
  }
) >"$STORAGE_REPLICATION_HANDLE_BOUNDARY_BYPASS_HITS_FILE"

if [[ -s "$STORAGE_REPLICATION_HANDLE_BOUNDARY_BYPASS_HITS_FILE" ]]; then
  report_failure "RustFS replication pool/stat handles must stay behind rustfs/src/storage/storage_api.rs: $(paste -sd '; ' "$STORAGE_REPLICATION_HANDLE_BOUNDARY_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  {
    rg -n --with-filename '\b(ObjectOpts|ResyncOpts)\s*\{' \
      rustfs/src/admin \
      --glob '*.rs' \
      --glob '!rustfs/src/admin/storage_api.rs' || true
    rg -n -U --with-filename 'use\s+(?:crate::admin::storage_api::bucket::replication|super::storage_api::bucket::replication)::\{[^}]*\b(ObjectOpts|ResyncOpts)\b' \
      rustfs/src/admin \
      --glob '*.rs' \
      --glob '!rustfs/src/admin/storage_api.rs' || true
    rg -n --with-filename '(?:crate::admin::storage_api::bucket::replication|super::storage_api::bucket::replication|replication)::(?:ObjectOpts|ResyncOpts)\b' \
      rustfs/src/admin \
      --glob '*.rs' \
      --glob '!rustfs/src/admin/storage_api.rs' || true
  }
) >"$ADMIN_REPLICATION_DTO_BOUNDARY_BYPASS_HITS_FILE"

if [[ -s "$ADMIN_REPLICATION_DTO_BOUNDARY_BYPASS_HITS_FILE" ]]; then
  report_failure "admin replication ObjectOpts/ResyncOpts construction must stay behind rustfs/src/admin/storage_api.rs: $(paste -sd '; ' "$ADMIN_REPLICATION_DTO_BOUNDARY_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  {
    rg -n --with-filename 'ecstore_bucket::replication::(check_replicate_delete|get_must_replicate_options|must_replicate|schedule_replication|schedule_replication_delete)|crate::bucket::replication::(check_replicate_delete|get_must_replicate_options|must_replicate|schedule_replication|schedule_replication_delete)' \
      rustfs/src crates/ecstore/src \
      --glob '*.rs'
    rg -n --with-filename '\b(check_replicate_delete|get_must_replicate_options|must_replicate|schedule_replication|schedule_replication_delete)\b' \
      crates/ecstore/src/bucket/replication/mod.rs
  } || true
) >"$REPLICATION_OBJECT_BRIDGE_BYPASS_HITS_FILE"

if [[ -s "$REPLICATION_OBJECT_BRIDGE_BYPASS_HITS_FILE" ]]; then
  report_failure "object replication work entry points must stay behind ReplicationObjectBridge: $(paste -sd '; ' "$REPLICATION_OBJECT_BRIDGE_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  {
    rg -n --with-filename '\b(get_must_replicate_options|must_replicate|schedule_replication|MustReplicateOptions|ReplicationObjectBridge|ReplicationObjectOpts)\b|ObjectOpts as ReplicationObjectOpts|ReplicationType::Object' \
      rustfs/src/app \
      --glob '*.rs' \
      --glob '!rustfs/src/app/storage_api.rs' || true
    rg -n --with-filename '(?:ecstore_bucket::replication|crate::storage::storage_api::ecstore_bucket::replication)::(?:ObjectOpts|MustReplicateOptions|ReplicationObjectBridge|ReplicationConfigurationExt)' \
      rustfs/src/app \
      --glob '*.rs' \
      --glob '!rustfs/src/app/storage_api.rs' || true
  }
) >"$APP_REPLICATION_DTO_BOUNDARY_BYPASS_HITS_FILE"

if [[ -s "$APP_REPLICATION_DTO_BOUNDARY_BYPASS_HITS_FILE" ]]; then
  report_failure "app replication ObjectOpts/MustReplicateOptions/bridge access must stay behind rustfs/src/app/storage_api.rs: $(paste -sd '; ' "$APP_REPLICATION_DTO_BOUNDARY_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  {
    rg -n --with-filename 'rustfs_ecstore::api::bucket::replication' \
      crates/scanner/src crates/scanner/tests \
      --glob '*.rs' \
      --glob '!crates/scanner/src/storage_api.rs' || true
    rg -n --with-filename '\b(ReplicateObjectInfo|ReplicationType|ResyncDecision|ResyncTargetDecision|EcstoreReplicationConfig|EcstoreReplicationHealQueueResult|EcstoreReplicationQueueAdmission)\b|ReplicationConfig\s*\{' \
      crates/scanner/src crates/scanner/tests \
      --glob '*.rs' \
      --glob '!crates/scanner/src/storage_api.rs' || true
  }
) >"$SCANNER_REPLICATION_DTO_BOUNDARY_BYPASS_HITS_FILE"

if [[ -s "$SCANNER_REPLICATION_DTO_BOUNDARY_BYPASS_HITS_FILE" ]]; then
  report_failure "scanner replication queue DTO access must stay behind crates/scanner/src/storage_api.rs: $(paste -sd '; ' "$SCANNER_REPLICATION_DTO_BOUNDARY_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  {
    rg -n --with-filename 'crate::bucket::replication::(decode_resync_file|encode_resync_file|ObjectOpts|ReplicationConfigurationExt)' \
      crates/ecstore/src \
      --glob '*.rs'
    rg -n -U --with-filename 'use\s+crate::bucket::replication::\{[^}]*\b(decode_resync_file|encode_resync_file|ObjectOpts|ReplicationConfigurationExt)\b' \
      crates/ecstore/src \
      --glob '*.rs'
    rg -n -U --with-filename 'crate::\{[^;]*bucket::replication::\{[^}]*\b(decode_resync_file|encode_resync_file|ObjectOpts|ReplicationConfigurationExt)\b' \
      crates/ecstore/src \
      --glob '*.rs'
  } |
    rg -v '^crates/ecstore/src/bucket/replication/' || true
) >"$REPLICATION_ECSTORE_OWNER_BRIDGE_BYPASS_HITS_FILE"

if [[ -s "$REPLICATION_ECSTORE_OWNER_BRIDGE_BYPASS_HITS_FILE" ]]; then
  report_failure "ECStore owner modules must use replication bridge contracts instead of internal replication helpers: $(paste -sd '; ' "$REPLICATION_ECSTORE_OWNER_BRIDGE_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  find crates/ecstore/src/bucket/replication -type f -name '*.rs' -print0 |
    xargs -0 perl -0ne '
      sub blank {
        my ($text) = @_;
        $text =~ s/[^\n]/ /g;
        return $text;
      }

      sub blank_block_comments {
        my ($text) = @_;
        my $out = $text;
        my $len = length($text);
        my $i = 0;

        while ($i < $len) {
          if ($i + 1 < $len && substr($text, $i, 2) eq "/*") {
            my $start = $i;
            my $depth = 0;

            while ($i < $len) {
              if ($i + 1 < $len && substr($text, $i, 2) eq "/*") {
                $depth++;
                $i += 2;
                next;
              }
              if ($i + 1 < $len && substr($text, $i, 2) eq "*/") {
                $depth--;
                $i += 2;
                last if $depth == 0;
                next;
              }
              $i++;
            }

            substr($out, $start, $i - $start) = blank(substr($text, $start, $i - $start));
            next;
          }

          $i++;
        }

        return $out;
      }

      my $source = $_;
      my $hash = chr(35);
      $source =~ s{b?r(${hash}*)".*?"\1}{blank($&)}egs;
      $source =~ s{b?"(?:\\.|[^"\\])*"}{blank($&)}egs;
      $source =~ s{'\''(?:\\.|[^'\''\\])+'\''}{blank($&)}egs;
      $source = blank_block_comments($source);
      $source =~ s{//[^\n]*}{blank($&)}eg;

      while ($source =~ /(?:crate::bucket::replication\b|crate::bucket::\{[^;]*\breplication\b|crate::\{[^;]*\bbucket::replication\b|crate::\{[^;]*\bbucket::\{[^;]*\breplication\b)/sg) {
        my $prefix = substr($source, 0, $-[0]);
        my $line = ($prefix =~ tr/\n//) + 1;
        my $match = $&;
        $match =~ s/\s+/ /g;
        print "$ARGV:$line:$match\n";
      }
    ' || true
) >"$REPLICATION_LOCAL_PATH_BYPASS_HITS_FILE"

if [[ -s "$REPLICATION_LOCAL_PATH_BYPASS_HITS_FILE" ]]; then
  report_failure "replication modules must use local relative paths instead of crate-qualified replication self paths: $(paste -sd '; ' "$REPLICATION_LOCAL_PATH_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  find crates/ecstore/src/bucket/replication -type f -name '*.rs' -print0 |
    xargs -0 perl -0ne '
      next if $ARGV =~ m{replication_(storage|bandwidth)_boundary\.rs$};

      sub blank {
        my ($text) = @_;
        $text =~ s/[^\n]/ /g;
        return $text;
      }

      sub blank_block_comments {
        my ($text) = @_;
        my $out = $text;
        my $len = length($text);
        my $i = 0;

        while ($i < $len) {
          if ($i + 1 < $len && substr($text, $i, 2) eq "/*") {
            my $start = $i;
            my $depth = 0;

            while ($i < $len) {
              if ($i + 1 < $len && substr($text, $i, 2) eq "/*") {
                $depth++;
                $i += 2;
                next;
              }
              if ($i + 1 < $len && substr($text, $i, 2) eq "*/") {
                $depth--;
                $i += 2;
                last if $depth == 0;
                next;
              }
              $i++;
            }

            substr($out, $start, $i - $start) = blank(substr($text, $start, $i - $start));
            next;
          }

          $i++;
        }

        return $out;
      }

      my $source = $_;
      my $hash = chr(35);
      $source =~ s{b?r(${hash}*)".*?"\1}{blank($&)}egs;
      $source =~ s{b?"(?:\\.|[^"\\])*"}{blank($&)}egs;
      $source =~ s{'\''(?:\\.|[^'\''\\])+'\''}{blank($&)}egs;
      $source = blank_block_comments($source);
      $source =~ s{//[^\n]*}{blank($&)}eg;

      while ($source =~ /([^;]+)(?:;|$)/g) {
        my $statement_start = $-[1];
        my $statement = $1;
        my $normalized = $statement;
        $normalized =~ s/\s+//g;

        my $hits_ecstore =
          $normalized =~ /\bECStore(?:\b|as)/
          && $normalized =~ /crate::(?:store::\{?|\{.*store::\{?)/s;
        my $hits_monitor =
          $normalized =~ /\bMonitor(?:\b|as)/
          && $normalized =~ /crate::(?:bucket::\{?|\{.*bucket::\{?)/s
          && $normalized =~ /bucket::\{?.*bandwidth::\{?.*monitor::\{?/s;

        if ($hits_ecstore || $hits_monitor) {
          my $leading = $statement;
          $leading =~ s/^(\s*).*$/$1/s;
          my $prefix = substr($source, 0, $statement_start);
          my $line = ($prefix =~ tr/\n//) + ($leading =~ tr/\n//) + 1;
          $normalized =~ s/\s+/ /g;
          print "$ARGV:$line:$normalized\n";
        }
      }
    ' || true
) >"$REPLICATION_RUNTIME_TYPE_BYPASS_HITS_FILE"

if [[ -s "$REPLICATION_RUNTIME_TYPE_BYPASS_HITS_FILE" ]]; then
  report_failure "replication runtime concrete types must stay behind local storage or bandwidth boundaries: $(paste -sd '; ' "$REPLICATION_RUNTIME_TYPE_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'crate::runtime::sources(\s+as\s+runtime_sources|::)' \
    crates/ecstore/src/bucket/replication \
    --glob '*.rs' |
    rg -v '^crates/ecstore/src/bucket/replication/runtime_boundary\.rs:' || true
) >"$REPLICATION_RUNTIME_SOURCE_BYPASS_HITS_FILE"

if [[ -s "$REPLICATION_RUNTIME_SOURCE_BYPASS_HITS_FILE" ]]; then
  report_failure "replication runtime-source access must stay behind replication runtime boundary: $(paste -sd '; ' "$REPLICATION_RUNTIME_SOURCE_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'queue_replication_heal_internal' \
    crates/scanner/src crates/scanner/tests \
    --glob '*.rs' || true
) >"$REPLICATION_SCANNER_BRIDGE_BYPASS_HITS_FILE"

if [[ -s "$REPLICATION_SCANNER_BRIDGE_BYPASS_HITS_FILE" ]]; then
  report_failure "scanner replication heal queueing must stay behind ReplicationScannerBridge: $(paste -sd '; ' "$REPLICATION_SCANNER_BRIDGE_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'crate::services::event_notification' \
    crates/ecstore/src/bucket/replication \
    --glob '*.rs' |
    rg -v '^crates/ecstore/src/bucket/replication/replication_event_sink\.rs:' || true
) >"$REPLICATION_EVENT_SINK_BYPASS_HITS_FILE"

if [[ -s "$REPLICATION_EVENT_SINK_BYPASS_HITS_FILE" ]]; then
  report_failure "replication event notification access must stay behind replication event sink: $(paste -sd '; ' "$REPLICATION_EVENT_SINK_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '\b(runtime_sources|runtime_boundary)::default_local_node_name\(\)' \
    crates/ecstore/src/bucket/replication \
    --glob '*.rs' |
    rg -v '^crates/ecstore/src/bucket/replication/replication_event_sink\.rs:' || true
) >"$REPLICATION_EVENT_HOST_BYPASS_HITS_FILE"

if [[ -s "$REPLICATION_EVENT_HOST_BYPASS_HITS_FILE" ]]; then
  report_failure "replication event host selection must stay behind replication event sink: $(paste -sd '; ' "$REPLICATION_EVENT_HOST_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  find crates/ecstore/src/bucket/replication -type f -name '*.rs' -print0 |
    xargs -0 perl -0ne '
      sub emit_match {
        my ($pos, $match) = @_;
        my $prefix = substr($_, 0, $pos);
        my $line = ($prefix =~ tr/\n//) + 1;
        $match =~ s/\s+/ /g;
        print "$ARGV:$line:$match\n";
      }

      while (/(?:crate::config::|use\s+crate::config\b)/sg) {
        emit_match($-[0], $&);
      }

      while (/use\s+crate::\{([^;]*)/sg) {
        my ($pos, $end, $body) = ($-[0], $+[0], $1);
        my ($depth, $token) = (0, "");
        my $matched = 0;

        for my $ch (split //, $body) {
          if ($ch eq "{") {
            $depth++;
            next;
          }
          if ($ch eq "}") {
            $depth-- if $depth > 0;
            next;
          }
          if ($depth == 0 && $ch eq ",") {
            if ($token =~ /^\s*config\b/s) {
              emit_match($pos, substr($_, $pos, $end - $pos));
              $matched = 1;
              last;
            }
            $token = "";
            next;
          }
          $token .= $ch if $depth == 0;
        }

        if (!$matched && $token =~ /^\s*config\b/s) {
          emit_match($pos, substr($_, $pos, $end - $pos));
        }
      }
    ' |
    rg -v '^crates/ecstore/src/bucket/replication/replication_config_store\.rs:' || true
) >"$REPLICATION_CONFIG_STORE_BYPASS_HITS_FILE"

if [[ -s "$REPLICATION_CONFIG_STORE_BYPASS_HITS_FILE" ]]; then
  report_failure "replication config access must stay behind replication config store: $(paste -sd '; ' "$REPLICATION_CONFIG_STORE_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  find crates/ecstore/src/bucket/replication -type f -name '*.rs' -print0 |
    xargs -0 perl -0ne '
      while (/(?:crate::error::|use\s+crate::error\b|use\s+crate::\{[^;]*\berror\b)/sg) {
        my $prefix = substr($_, 0, $-[0]);
        my $line = ($prefix =~ tr/\n//) + 1;
        my $match = $&;
        $match =~ s/\s+/ /g;
        print "$ARGV:$line:$match\n";
      }
    ' |
    rg -v '^crates/ecstore/src/bucket/replication/replication_error_boundary\.rs:' || true
) >"$REPLICATION_ERROR_BOUNDARY_BYPASS_HITS_FILE"

if [[ -s "$REPLICATION_ERROR_BOUNDARY_BYPASS_HITS_FILE" ]]; then
  report_failure "replication error contracts must stay behind replication error boundary: $(paste -sd '; ' "$REPLICATION_ERROR_BOUNDARY_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  find crates/ecstore/src/bucket/replication -type f -name '*.rs' -print0 |
    xargs -0 perl -0ne '
      while (/(?:crate::(?:client::api_get_options|object_api|storage_api_contracts)\b|crate::\{[^;]*\b(?:api_get_options|object_api|storage_api_contracts)\b)/sg) {
        my $prefix = substr($_, 0, $-[0]);
        my $line = ($prefix =~ tr/\n//) + 1;
        my $match = $&;
        $match =~ s/\s+/ /g;
        print "$ARGV:$line:$match\n";
      }
    ' |
    rg -v '^crates/ecstore/src/bucket/replication/replication_storage_boundary\.rs:' || true
) >"$REPLICATION_STORAGE_BOUNDARY_BYPASS_HITS_FILE"

if [[ -s "$REPLICATION_STORAGE_BOUNDARY_BYPASS_HITS_FILE" ]]; then
  report_failure "replication storage contracts must stay behind replication storage boundary: $(paste -sd '; ' "$REPLICATION_STORAGE_BOUNDARY_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  find crates/ecstore/src/bucket/replication -type f -name '*.rs' -print0 |
    xargs -0 perl -0ne '
      while (/crate::bucket::bandwidth(?:::\{[^;]*\breader\b|::reader\b)/sg) {
        my $prefix = substr($_, 0, $-[0]);
        my $line = ($prefix =~ tr/\n//) + 1;
        my $match = $&;
        $match =~ s/\s+/ /g;
        print "$ARGV:$line:$match\n";
      }
    ' |
    rg -v '^crates/ecstore/src/bucket/replication/replication_bandwidth_boundary\.rs:' || true
) >"$REPLICATION_BANDWIDTH_BOUNDARY_BYPASS_HITS_FILE"

if [[ -s "$REPLICATION_BANDWIDTH_BOUNDARY_BYPASS_HITS_FILE" ]]; then
  report_failure "replication bandwidth reader access must stay behind replication bandwidth boundary: $(paste -sd '; ' "$REPLICATION_BANDWIDTH_BOUNDARY_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'crate::set_disk::get_lock_acquire_timeout|\bget_lock_acquire_timeout\(' \
    crates/ecstore/src/bucket/replication \
    --glob '*.rs' |
    rg -v '^crates/ecstore/src/bucket/replication/replication_lock_boundary\.rs:' || true
) >"$REPLICATION_LOCK_BOUNDARY_BYPASS_HITS_FILE"

if [[ -s "$REPLICATION_LOCK_BOUNDARY_BYPASS_HITS_FILE" ]]; then
  report_failure "replication lock timeout access must stay behind replication lock boundary: $(paste -sd '; ' "$REPLICATION_LOCK_BOUNDARY_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'crate::bucket::msgp_decode' \
    crates/ecstore/src/bucket/replication \
    --glob '*.rs' |
    rg -v '^crates/ecstore/src/bucket/replication/replication_msgp_boundary\.rs:' || true
) >"$REPLICATION_MSGP_BOUNDARY_BYPASS_HITS_FILE"

if [[ -s "$REPLICATION_MSGP_BOUNDARY_BYPASS_HITS_FILE" ]]; then
  report_failure "replication msgp helpers must stay behind replication msgp boundary: $(paste -sd '; ' "$REPLICATION_MSGP_BOUNDARY_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'crate::bucket::tagging::decode_tags_to_map|use crate::bucket::tagging' \
    crates/ecstore/src/bucket/replication \
    --glob '*.rs' |
    rg -v '^crates/ecstore/src/bucket/replication/replication_tagging_boundary\.rs:' || true
) >"$REPLICATION_TAGGING_BOUNDARY_BYPASS_HITS_FILE"

if [[ -s "$REPLICATION_TAGGING_BOUNDARY_BYPASS_HITS_FILE" ]]; then
  report_failure "replication tag decoding must stay behind replication tagging boundary: $(paste -sd '; ' "$REPLICATION_TAGGING_BOUNDARY_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'rustfs_filemeta::' crates/ecstore/src/bucket/replication --glob '*.rs' |
    rg -v '^crates/ecstore/src/bucket/replication/replication_filemeta_boundary\.rs:' |
    rg -v '^crates/ecstore/src/bucket/replication/replication_storage_boundary\.rs:' || true
) >"$REPLICATION_FILEMETA_BOUNDARY_BYPASS_HITS_FILE"

if [[ -s "$REPLICATION_FILEMETA_BOUNDARY_BYPASS_HITS_FILE" ]]; then
  report_failure "replication filemeta contracts must stay behind replication filemeta boundary: $(paste -sd '; ' "$REPLICATION_FILEMETA_BOUNDARY_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  find crates/ecstore/src/bucket/replication -type f -name '*.rs' -print0 |
    xargs -0 perl -0ne '
      while (/(?:crate::bucket::(?:bucket_target_sys|target)\b|super(?:::super)+::(?:bucket_target_sys|target)\b|BucketTargetSys::get\(\))/sg) {
        my $prefix = substr($_, 0, $-[0]);
        my $line = ($prefix =~ tr/\n//) + 1;
        my $match = $&;
        $match =~ s/\s+/ /g;
        print "$ARGV:$line:$match\n";
      }
      while (/crate::bucket::\{/g) {
        my $start = $-[0];
        my $body_start = pos($_);
        my $depth = 1;
        my $i = $body_start;
        while ($i < length($_) && $depth > 0) {
          my $ch = substr($_, $i, 1);
          $depth++ if $ch eq "{";
          $depth-- if $ch eq "}";
          $i++;
        }
        next if $depth != 0;
        my $body = substr($_, $body_start, $i - $body_start - 1);
        my $part_start = 0;
        my $inner_depth = 0;
        for (my $j = 0; $j <= length($body); $j++) {
          my $ch = $j < length($body) ? substr($body, $j, 1) : ",";
          $inner_depth++ if $ch eq "{";
          $inner_depth-- if $ch eq "}";
          if ($ch eq "," && $inner_depth == 0) {
            my $part = substr($body, $part_start, $j - $part_start);
            if ($part =~ /^\s*(?:bucket_target_sys|target)\b/s) {
              my $prefix = substr($_, 0, $start);
              my $line = ($prefix =~ tr/\n//) + 1;
              $part =~ s/\s+/ /g;
              print "$ARGV:$line:crate::bucket::{$part}\n";
            }
            $part_start = $j + 1;
          }
        }
      }
      while (/super(?:::super)+::\{/g) {
        my $start = $-[0];
        my $body_start = pos($_);
        my $depth = 1;
        my $i = $body_start;
        while ($i < length($_) && $depth > 0) {
          my $ch = substr($_, $i, 1);
          $depth++ if $ch eq "{";
          $depth-- if $ch eq "}";
          $i++;
        }
        next if $depth != 0;
        my $body = substr($_, $body_start, $i - $body_start - 1);
        my $part_start = 0;
        my $inner_depth = 0;
        for (my $j = 0; $j <= length($body); $j++) {
          my $ch = $j < length($body) ? substr($body, $j, 1) : ",";
          $inner_depth++ if $ch eq "{";
          $inner_depth-- if $ch eq "}";
          if ($ch eq "," && $inner_depth == 0) {
            my $part = substr($body, $part_start, $j - $part_start);
            if ($part =~ /^\s*(?:bucket_target_sys|target)\b/s) {
              my $prefix = substr($_, 0, $start);
              my $line = ($prefix =~ tr/\n//) + 1;
              $part =~ s/\s+/ /g;
              print "$ARGV:$line:super::super::{$part}\n";
            }
            $part_start = $j + 1;
          }
        }
      }
      while (/crate::\{/g) {
        my $start = $-[0];
        my $body_start = pos($_);
        my $depth = 1;
        my $i = $body_start;
        while ($i < length($_) && $depth > 0) {
          my $ch = substr($_, $i, 1);
          $depth++ if $ch eq "{";
          $depth-- if $ch eq "}";
          $i++;
        }
        next if $depth != 0;
        my $body = substr($_, $body_start, $i - $body_start - 1);
        my $part_start = 0;
        my $inner_depth = 0;
        for (my $j = 0; $j <= length($body); $j++) {
          my $ch = $j < length($body) ? substr($body, $j, 1) : ",";
          $inner_depth++ if $ch eq "{";
          $inner_depth-- if $ch eq "}";
          if ($ch eq "," && $inner_depth == 0) {
            my $part = substr($body, $part_start, $j - $part_start);
            my $hit = $part =~ /^\s*bucket\s*::\s*(?:bucket_target_sys|target)\b/s;
            if (!$hit && $part =~ /^\s*bucket\s*::\s*\{(.*)\}\s*$/s) {
              my $bucket_body = $1;
              my $bucket_part_start = 0;
              my $bucket_depth = 0;
              for (my $k = 0; $k <= length($bucket_body); $k++) {
                my $bucket_ch = $k < length($bucket_body) ? substr($bucket_body, $k, 1) : ",";
                $bucket_depth++ if $bucket_ch eq "{";
                $bucket_depth-- if $bucket_ch eq "}";
                if ($bucket_ch eq "," && $bucket_depth == 0) {
                  my $bucket_part = substr($bucket_body, $bucket_part_start, $k - $bucket_part_start);
                  if ($bucket_part =~ /^\s*(?:bucket_target_sys|target)\b/s) {
                    $hit = 1;
                    last;
                  }
                  $bucket_part_start = $k + 1;
                }
              }
            }
            if ($hit) {
              my $prefix = substr($_, 0, $start);
              my $line = ($prefix =~ tr/\n//) + 1;
              $part =~ s/\s+/ /g;
              print "$ARGV:$line:crate::{$part}\n";
            }
            $part_start = $j + 1;
          }
        }
      }
    ' |
    rg -v '^crates/ecstore/src/bucket/replication/replication_target_boundary\.rs:' || true
) >"$REPLICATION_TARGET_BOUNDARY_BYPASS_HITS_FILE"

if [[ -s "$REPLICATION_TARGET_BOUNDARY_BYPASS_HITS_FILE" ]]; then
  report_failure "replication bucket-target access and types must stay behind replication target boundary: $(paste -sd '; ' "$REPLICATION_TARGET_BOUNDARY_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  find crates/ecstore/src/bucket/replication -type f -name '*.rs' -print0 |
    xargs -0 perl -0ne '
      while (/(?:crate::bucket::metadata_sys|super(?:::super)+::metadata_sys\b|metadata_sys::get_replication_config|crate::disk::(?:BUCKET_META_PREFIX|RUSTFS_META_BUCKET)\b)/sg) {
        my $prefix = substr($_, 0, $-[0]);
        my $line = ($prefix =~ tr/\n//) + 1;
        my $match = $&;
        $match =~ s/\s+/ /g;
        print "$ARGV:$line:$match\n";
      }
      while (/super(?:::super)+::\{/g) {
        my $start = $-[0];
        my $body_start = pos($_);
        my $depth = 1;
        my $i = $body_start;
        while ($i < length($_) && $depth > 0) {
          my $ch = substr($_, $i, 1);
          $depth++ if $ch eq "{";
          $depth-- if $ch eq "}";
          $i++;
        }
        next if $depth != 0;
        my $body = substr($_, $body_start, $i - $body_start - 1);
        my $part_start = 0;
        my $inner_depth = 0;
        for (my $j = 0; $j <= length($body); $j++) {
          my $ch = $j < length($body) ? substr($body, $j, 1) : ",";
          $inner_depth++ if $ch eq "{";
          $inner_depth-- if $ch eq "}";
          if ($ch eq "," && $inner_depth == 0) {
            my $part = substr($body, $part_start, $j - $part_start);
            if ($part =~ /^\s*metadata_sys\b/s) {
              my $prefix = substr($_, 0, $start);
              my $line = ($prefix =~ tr/\n//) + 1;
              $part =~ s/\s+/ /g;
              print "$ARGV:$line:super::super::{$part}\n";
            }
            $part_start = $j + 1;
          }
        }
      }
      while (/crate::disk::\{/g) {
        my $start = $-[0];
        my $body_start = pos($_);
        my $depth = 1;
        my $i = $body_start;
        while ($i < length($_) && $depth > 0) {
          my $ch = substr($_, $i, 1);
          $depth++ if $ch eq "{";
          $depth-- if $ch eq "}";
          $i++;
        }
        next if $depth != 0;
        my $body = substr($_, $body_start, $i - $body_start - 1);
        my $part_start = 0;
        my $inner_depth = 0;
        for (my $j = 0; $j <= length($body); $j++) {
          my $ch = $j < length($body) ? substr($body, $j, 1) : ",";
          $inner_depth++ if $ch eq "{";
          $inner_depth-- if $ch eq "}";
          if ($ch eq "," && $inner_depth == 0) {
            my $part = substr($body, $part_start, $j - $part_start);
            if ($part =~ /^\s*(?:BUCKET_META_PREFIX|RUSTFS_META_BUCKET)\b/s) {
              my $prefix = substr($_, 0, $start);
              my $line = ($prefix =~ tr/\n//) + 1;
              $part =~ s/\s+/ /g;
              print "$ARGV:$line:crate::disk::{$part}\n";
            }
            $part_start = $j + 1;
          }
        }
      }
      while (/crate::\{/g) {
        my $start = $-[0];
        my $body_start = pos($_);
        my $depth = 1;
        my $i = $body_start;
        while ($i < length($_) && $depth > 0) {
          my $ch = substr($_, $i, 1);
          $depth++ if $ch eq "{";
          $depth-- if $ch eq "}";
          $i++;
        }
        next if $depth != 0;
        my $body = substr($_, $body_start, $i - $body_start - 1);
        my $part_start = 0;
        my $inner_depth = 0;
        for (my $j = 0; $j <= length($body); $j++) {
          my $ch = $j < length($body) ? substr($body, $j, 1) : ",";
          $inner_depth++ if $ch eq "{";
          $inner_depth-- if $ch eq "}";
          if ($ch eq "," && $inner_depth == 0) {
            my $part = substr($body, $part_start, $j - $part_start);
            my $hit = $part =~ /^\s*disk\s*::\s*(?:BUCKET_META_PREFIX|RUSTFS_META_BUCKET)\b/s;
            if (!$hit && $part =~ /^\s*disk\s*::\s*\{(.*)\}\s*$/s) {
              my $disk_body = $1;
              my $disk_part_start = 0;
              my $disk_depth = 0;
              for (my $k = 0; $k <= length($disk_body); $k++) {
                my $disk_ch = $k < length($disk_body) ? substr($disk_body, $k, 1) : ",";
                $disk_depth++ if $disk_ch eq "{";
                $disk_depth-- if $disk_ch eq "}";
                if ($disk_ch eq "," && $disk_depth == 0) {
                  my $disk_part = substr($disk_body, $disk_part_start, $k - $disk_part_start);
                  if ($disk_part =~ /^\s*(?:BUCKET_META_PREFIX|RUSTFS_META_BUCKET)\b/s) {
                    $hit = 1;
                    last;
                  }
                  $disk_part_start = $k + 1;
                }
              }
            }
            if ($hit) {
              my $prefix = substr($_, 0, $start);
              my $line = ($prefix =~ tr/\n//) + 1;
              $part =~ s/\s+/ /g;
              print "$ARGV:$line:crate::{$part}\n";
            }
            $part_start = $j + 1;
          }
        }
      }
    ' |
    rg -v '^crates/ecstore/src/bucket/replication/replication_metadata_boundary\.rs:' || true
) >"$REPLICATION_METADATA_BOUNDARY_BYPASS_HITS_FILE"

if [[ -s "$REPLICATION_METADATA_BOUNDARY_BYPASS_HITS_FILE" ]]; then
  report_failure "replication metadata access must stay behind replication metadata boundary: $(paste -sd '; ' "$REPLICATION_METADATA_BOUNDARY_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'crate::bucket::versioning_sys|BucketVersioningSys::prefix_(enabled|suspended)' \
    crates/ecstore/src/bucket/replication \
    --glob '*.rs' |
    rg -v '^crates/ecstore/src/bucket/replication/replication_versioning_boundary\.rs:' || true
) >"$REPLICATION_VERSIONING_BOUNDARY_BYPASS_HITS_FILE"

if [[ -s "$REPLICATION_VERSIONING_BOUNDARY_BYPASS_HITS_FILE" ]]; then
  report_failure "replication versioning access must stay behind replication versioning boundary: $(paste -sd '; ' "$REPLICATION_VERSIONING_BOUNDARY_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '\bGLOBAL_REPLICATION_(POOL|STATS)\b' \
    crates rustfs fuzz \
    --glob '*.rs' |
    rg -v '^crates/ecstore/src/(bucket/replication/replication_pool|runtime/sources)\.rs:' || true
) >"$GLOBAL_REPLICATION_STATE_BYPASS_HITS_FILE"

if [[ -s "$GLOBAL_REPLICATION_STATE_BYPASS_HITS_FILE" ]]; then
  report_failure "GLOBAL_REPLICATION_POOL/STATS access must stay behind replication owner or ECStore runtime-source helpers: $(paste -sd '; ' "$GLOBAL_REPLICATION_STATE_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '\bGLOBAL_(BucketMetadataSys|BUCKET_METADATA_SYS)\b' \
    crates rustfs fuzz \
    --glob '*.rs' |
    rg -v '^crates/ecstore/src/metadata/bucket_sys\.rs:' || true
) >"$GLOBAL_BUCKET_METADATA_SYS_BYPASS_HITS_FILE"

if [[ -s "$GLOBAL_BUCKET_METADATA_SYS_BYPASS_HITS_FILE" ]]; then
  report_failure "GLOBAL_BUCKET_METADATA_SYS access must stay behind ECStore bucket metadata owner helpers: $(paste -sd '; ' "$GLOBAL_BUCKET_METADATA_SYS_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '\bGLOBAL_BUCKET_TARGET_SYS\b' \
    crates rustfs fuzz \
    --glob '*.rs' |
    rg -v '^crates/ecstore/src/bucket/bucket_target_sys\.rs:' || true
) >"$GLOBAL_BUCKET_TARGET_SYS_BYPASS_HITS_FILE"

if [[ -s "$GLOBAL_BUCKET_TARGET_SYS_BYPASS_HITS_FILE" ]]; then
  report_failure "GLOBAL_BUCKET_TARGET_SYS access must stay behind ECStore bucket target owner helpers: $(paste -sd '; ' "$GLOBAL_BUCKET_TARGET_SYS_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '\bEVENT_DISPATCH_HOOK\b' \
    crates rustfs fuzz \
    --glob '*.rs' |
    rg -v '^crates/ecstore/src/services/event_notification\.rs:' || true
) >"$EVENT_DISPATCH_HOOK_BYPASS_HITS_FILE"

if [[ -s "$EVENT_DISPATCH_HOOK_BYPASS_HITS_FILE" ]]; then
  report_failure "EVENT_DISPATCH_HOOK access must stay behind ECStore event-notification owner helpers: $(paste -sd '; ' "$EVENT_DISPATCH_HOOK_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '\bUSAGE_(MEMORY_CACHE|CACHE_UPDATING)\b' \
    crates rustfs fuzz \
    --glob '*.rs' |
    rg -v '^crates/ecstore/src/data_usage/mod\.rs:' || true
) >"$DATA_USAGE_MEMORY_GLOBAL_BYPASS_HITS_FILE"

if [[ -s "$DATA_USAGE_MEMORY_GLOBAL_BYPASS_HITS_FILE" ]]; then
  report_failure "data-usage memory globals must stay behind ECStore data-usage owner helpers: $(paste -sd '; ' "$DATA_USAGE_MEMORY_GLOBAL_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '\bWORKLOAD_ADMISSION_SNAPSHOT_PROVIDER\b' \
    crates rustfs fuzz \
    --glob '*.rs' |
    rg -v '^crates/ecstore/src/runtime/sources\.rs:' || true
) >"$WORKLOAD_ADMISSION_PROVIDER_BYPASS_HITS_FILE"

if [[ -s "$WORKLOAD_ADMISSION_PROVIDER_BYPASS_HITS_FILE" ]]; then
  report_failure "WORKLOAD_ADMISSION_SNAPSHOT_PROVIDER access must stay behind ECStore runtime-source helpers: $(paste -sd '; ' "$WORKLOAD_ADMISSION_PROVIDER_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '\bREAD_REPAIR_HEAL_CACHE\b' \
    crates rustfs fuzz \
    --glob '*.rs' |
    rg -v '^crates/ecstore/src/set_disk/read\.rs:' || true
) >"$ECSTORE_READ_REPAIR_CACHE_BYPASS_HITS_FILE"

if [[ -s "$ECSTORE_READ_REPAIR_CACHE_BYPASS_HITS_FILE" ]]; then
  report_failure "READ_REPAIR_HEAL_CACHE access must stay behind ECStore set-disk read owner helpers: $(paste -sd '; ' "$ECSTORE_READ_REPAIR_CACHE_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '\bDISK_COMPRESSION_CONFIG\b' \
    crates rustfs fuzz \
    --glob '*.rs' |
    rg -v '^crates/ecstore/src/io_support/compress\.rs:' || true
) >"$ECSTORE_DISK_COMPRESSION_CACHE_BYPASS_HITS_FILE"

if [[ -s "$ECSTORE_DISK_COMPRESSION_CACHE_BYPASS_HITS_FILE" ]]; then
  report_failure "DISK_COMPRESSION_CONFIG access must stay behind ECStore IO compression owner helpers: $(paste -sd '; ' "$ECSTORE_DISK_COMPRESSION_CACHE_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '\bCACHED_(MAX_INFLIGHT_BYTES|BATCH_BLOCKS|BYTESMUT_INGEST)\b' \
    crates rustfs fuzz \
    --glob '*.rs' |
    rg -v '^crates/ecstore/src/erasure/coding/encode\.rs:' || true
) >"$ECSTORE_ERASURE_CODING_CACHE_BYPASS_HITS_FILE"

if [[ -s "$ECSTORE_ERASURE_CODING_CACHE_BYPASS_HITS_FILE" ]]; then
  report_failure "erasure coding cache access must stay behind ECStore erasure coding owner helpers: $(paste -sd '; ' "$ECSTORE_ERASURE_CODING_CACHE_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '\b(CACHED_(PUT_LARGE_BATCH_MIN_SIZE_BYTES|MULTIPART_PUT_LARGE_BATCH_MIN_SIZE_BYTES)|OBJECT_LOCK_DIAG_ENABLED)\b' \
    crates rustfs fuzz \
    --glob '*.rs' |
    rg -v '^crates/ecstore/src/set_disk/mod\.rs:' || true
) >"$ECSTORE_SET_DISK_CACHE_BYPASS_HITS_FILE"

if [[ -s "$ECSTORE_SET_DISK_CACHE_BYPASS_HITS_FILE" ]]; then
  report_failure "set-disk cache access must stay behind ECStore set-disk owner helpers: $(paste -sd '; ' "$ECSTORE_SET_DISK_CACHE_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '\bDRIVE_TIMEOUT_(PROFILE_CACHE|HEALTH_POLICY_CACHE)\b' \
    crates rustfs fuzz \
    --glob '*.rs' |
    rg -v '^crates/ecstore/src/disk/disk_store\.rs:' || true
) >"$ECSTORE_DRIVE_TIMEOUT_CACHE_BYPASS_HITS_FILE"

if [[ -s "$ECSTORE_DRIVE_TIMEOUT_CACHE_BYPASS_HITS_FILE" ]]; then
  report_failure "drive timeout cache access must stay behind ECStore disk-store owner helpers: $(paste -sd '; ' "$ECSTORE_DRIVE_TIMEOUT_CACHE_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '\bTIER_(FREE_VERSION_RECOVERY_STARTED|DELETE_JOURNAL_RECOVERY_STARTED)\b' \
    crates rustfs fuzz \
    --glob '*.rs' |
    rg -v '^crates/ecstore/src/bucket/lifecycle/bucket_lifecycle_ops\.rs:' || true
) >"$ECSTORE_LIFECYCLE_RECOVERY_GUARD_BYPASS_HITS_FILE"

if [[ -s "$ECSTORE_LIFECYCLE_RECOVERY_GUARD_BYPASS_HITS_FILE" ]]; then
  report_failure "lifecycle recovery guard access must stay behind ECStore lifecycle operation owner helpers: $(paste -sd '; ' "$ECSTORE_LIFECYCLE_RECOVERY_GUARD_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '\b(REMOTE_DELETE_(INFLIGHT|LIMITER|BREAKER)|REMOTE_TIER_DELETE_TEST_HOOK)\b' \
    crates rustfs fuzz \
    --glob '*.rs' |
    rg -v '^crates/ecstore/src/bucket/lifecycle/tier_sweeper\.rs:' || true
) >"$ECSTORE_REMOTE_TIER_DELETE_STATE_BYPASS_HITS_FILE"

if [[ -s "$ECSTORE_REMOTE_TIER_DELETE_STATE_BYPASS_HITS_FILE" ]]; then
  report_failure "remote tier delete state access must stay behind ECStore tier sweeper owner helpers: $(paste -sd '; ' "$ECSTORE_REMOTE_TIER_DELETE_STATE_BYPASS_HITS_FILE")"
fi

RUSTFS_OWNER_LOCAL_STATIC_NAMES='(KEYSTONE_AUTH|KEYSTONE_MAPPER|KEYSTONE_CONFIG|LICENSE_STATE|LICENSE_VERIFIER|CPU_CONT_GUARD|PROFILING_CANCEL_TOKEN|MEMORY_SYSTEM|DIAL9_TELEMETRY_GUARD|DISPLAY_CONFIG_SNAPSHOT|GLOBAL_CONFIG_SNAPSHOT|BUFFER_CONFIG_SINGLETON|BUFFER_PROFILE_ENABLED|LEGACY_CREDENTIAL_WARNED_KEYS|CONSOLE_CONFIG|ACTIVE_HTTP_REQUESTS|USE_STARSHARD_CACHE|BUCKET_CACHE_SMALL|BUCKET_CACHE_LARGE|GLOBAL_SSE_DEK_PROVIDER|SSE_TEST_LOCK|AUTH_FS|LOCK_STATS|DEADLOCK_DETECTOR|GET_OBJECT_BUFFER_THRESHOLD_WARNED|GET_READER_STREAM_BUFFER_SIZE_OVERRIDE|OBJECT_SEEK_SUPPORT_THRESHOLD|OBJECT_SEEK_SUPPORT_CONCURRENCY_THRESHOLDS|SUPPORTED_HEADERS|SITE_REPLICATION_PEER_CLIENT|SITE_REPLICATION_STATE_LOCK|AUDIT_MODULE_ENABLED|NOTIFY_MODULE_ENABLED|PERSISTED_NOTIFY_MODULE_ENABLED|PERSISTED_AUDIT_MODULE_ENABLED|PERSISTED_MODULE_SWITCH_CONFIGURED|DELETE_TAIL_TOTAL|DELETE_CLEANUP_TOTAL|DELETE_REPLICATION_TOTAL|DELETE_NOTIFY_TOTAL|EMBEDDED_SERVER_STARTED|TEST_OUTBOUND_TLS_GENERATION|TEST_REMAINING_FAILURES|CAPACITY_DIRTY_SCOPE_ENV|CAPACITY_DIRTY_SCOPE_INIT|GLOBAL_ENV)'

(
  cd "$ROOT_DIR"
  rg -n --with-filename \
    "^[[:space:]]*pub[[:space:]]*(?:\\([^)]*\\)[[:space:]]*)?static(?:[[:space:]]+mut)?[[:space:]]+${RUSTFS_OWNER_LOCAL_STATIC_NAMES}[[:space:]]*:" \
    rustfs/src \
    --glob '*.rs' || true
) >"$RUSTFS_OWNER_LOCAL_STATIC_PUBLIC_HITS_FILE"

if [[ -s "$RUSTFS_OWNER_LOCAL_STATIC_PUBLIC_HITS_FILE" ]]; then
  report_failure "RustFS owner-local statics must remain private to their owner modules: $(paste -sd '; ' "$RUSTFS_OWNER_LOCAL_STATIC_PUBLIC_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n -U --with-filename \
    "^[[:space:]]*pub[[:space:]]*(?:\\([^)]*\\)[[:space:]]*)?use[[:space:]][^;]*\\b${RUSTFS_OWNER_LOCAL_STATIC_NAMES}\\b" \
    rustfs/src \
    --glob '*.rs' || true
) >"$RUSTFS_OWNER_LOCAL_STATIC_REEXPORT_HITS_FILE"

if [[ -s "$RUSTFS_OWNER_LOCAL_STATIC_REEXPORT_HITS_FILE" ]]; then
  report_failure "RustFS owner-local statics must not be re-exported across owner boundaries: $(paste -sd '; ' "$RUSTFS_OWNER_LOCAL_STATIC_REEXPORT_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename \
    '^[[:space:]]*pub[[:space:]]*(?:\([^)]*\)[[:space:]]*)?static(?:[[:space:]]+mut)?[[:space:]]+(AUDIT_TARGET_SPECS|NOTIFICATION_TARGET_SPECS)[[:space:]]*:' \
    rustfs/src/admin/handlers/audit.rs \
    rustfs/src/admin/handlers/event.rs \
    rustfs/src/admin/handlers/plugins_instances.rs || true
) >"$RUSTFS_ADMIN_TARGET_SPEC_STATIC_PUBLIC_HITS_FILE"

if [[ -s "$RUSTFS_ADMIN_TARGET_SPEC_STATIC_PUBLIC_HITS_FILE" ]]; then
  report_failure "RustFS admin target spec statics must remain private to their handler owners: $(paste -sd '; ' "$RUSTFS_ADMIN_TARGET_SPEC_STATIC_PUBLIC_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n -U --with-filename \
    '^[[:space:]]*pub[[:space:]]*(?:\([^)]*\)[[:space:]]*)?use[[:space:]][^;]*\b(AUDIT_TARGET_SPECS|NOTIFICATION_TARGET_SPECS)\b' \
    rustfs/src \
    --glob '*.rs' || true
) >"$RUSTFS_ADMIN_TARGET_SPEC_STATIC_REEXPORT_HITS_FILE"

if [[ -s "$RUSTFS_ADMIN_TARGET_SPEC_STATIC_REEXPORT_HITS_FILE" ]]; then
  report_failure "RustFS admin target spec statics must not be re-exported across handler boundaries: $(paste -sd '; ' "$RUSTFS_ADMIN_TARGET_SPEC_STATIC_REEXPORT_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename \
    '\b(CONCURRENCY_MANAGER|ACTIVE_GET_REQUESTS|ACTIVE_PUT_REQUESTS|IO_PRIORITY_METRICS)\b' \
    rustfs/src \
    --glob '*.rs' |
    rg -v '^rustfs/src/storage/concurrency/' || true
) >"$RUSTFS_STORAGE_CONCURRENCY_STATIC_BYPASS_HITS_FILE"

if [[ -s "$RUSTFS_STORAGE_CONCURRENCY_STATIC_BYPASS_HITS_FILE" ]]; then
  report_failure "RustFS storage concurrency statics must stay inside the storage concurrency owner boundary: $(paste -sd '; ' "$RUSTFS_STORAGE_CONCURRENCY_STATIC_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '\bGLOBAL_BUCKET_MONITOR\b' \
    crates rustfs fuzz \
    --glob '*.rs' |
    rg -v '^crates/ecstore/src/runtime/global\.rs:' || true
) >"$GLOBAL_BUCKET_MONITOR_BYPASS_HITS_FILE"

if [[ -s "$GLOBAL_BUCKET_MONITOR_BYPASS_HITS_FILE" ]]; then
  report_failure "GLOBAL_BUCKET_MONITOR access must stay behind ECStore runtime helpers: $(paste -sd '; ' "$GLOBAL_BUCKET_MONITOR_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '\bGLOBAL_(Endpoints|ENDPOINTS)\b' \
    crates rustfs fuzz \
    --glob '*.rs' |
    rg -v '^crates/ecstore/src/runtime/(global|sources)\.rs:' || true
) >"$GLOBAL_ENDPOINTS_BYPASS_HITS_FILE"

if [[ -s "$GLOBAL_ENDPOINTS_BYPASS_HITS_FILE" ]]; then
  report_failure "GLOBAL_ENDPOINTS access must stay behind ECStore runtime helpers: $(paste -sd '; ' "$GLOBAL_ENDPOINTS_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '\bGLOBAL_(IsErasure|IS_ERASURE)\b' \
    crates rustfs fuzz \
    --glob '*.rs' |
    rg -v '^crates/ecstore/src/runtime/(global|sources)\.rs:' || true
) >"$GLOBAL_IS_ERASURE_BYPASS_HITS_FILE"

if [[ -s "$GLOBAL_IS_ERASURE_BYPASS_HITS_FILE" ]]; then
  report_failure "GLOBAL_IS_ERASURE access must stay behind ECStore runtime helpers: $(paste -sd '; ' "$GLOBAL_IS_ERASURE_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '\bGLOBAL_(IsDistErasure|IS_DIST_ERASURE)\b' \
    crates rustfs fuzz \
    --glob '*.rs' |
    rg -v '^crates/ecstore/src/runtime/(global|sources)\.rs:' || true
) >"$GLOBAL_IS_DIST_ERASURE_BYPASS_HITS_FILE"

if [[ -s "$GLOBAL_IS_DIST_ERASURE_BYPASS_HITS_FILE" ]]; then
  report_failure "GLOBAL_IS_DIST_ERASURE access must stay behind ECStore runtime helpers: $(paste -sd '; ' "$GLOBAL_IS_DIST_ERASURE_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '\bGLOBAL_LOCAL_DISK_MAP\b' \
    crates rustfs fuzz \
    --glob '*.rs' |
    rg -v '^crates/ecstore/src/runtime/(global|sources)\.rs:' || true
) >"$GLOBAL_LOCAL_DISK_MAP_BYPASS_HITS_FILE"

if [[ -s "$GLOBAL_LOCAL_DISK_MAP_BYPASS_HITS_FILE" ]]; then
  report_failure "GLOBAL_LOCAL_DISK_MAP access must stay behind ECStore runtime-source helpers: $(paste -sd '; ' "$GLOBAL_LOCAL_DISK_MAP_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '\bGLOBAL_LOCAL_DISK_ID_MAP\b' \
    crates rustfs fuzz \
    --glob '*.rs' |
    rg -v '^crates/ecstore/src/runtime/(global|sources)\.rs:' || true
) >"$GLOBAL_LOCAL_DISK_ID_MAP_BYPASS_HITS_FILE"

if [[ -s "$GLOBAL_LOCAL_DISK_ID_MAP_BYPASS_HITS_FILE" ]]; then
  report_failure "GLOBAL_LOCAL_DISK_ID_MAP access must stay behind ECStore runtime-source helpers: $(paste -sd '; ' "$GLOBAL_LOCAL_DISK_ID_MAP_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '\bGLOBAL_LOCAL_DISK_SET_DRIVES\b' \
    crates rustfs fuzz \
    --glob '*.rs' |
    rg -v '^crates/ecstore/src/runtime/(global|sources)\.rs:' || true
) >"$GLOBAL_LOCAL_DISK_SET_DRIVES_BYPASS_HITS_FILE"

if [[ -s "$GLOBAL_LOCAL_DISK_SET_DRIVES_BYPASS_HITS_FILE" ]]; then
  report_failure "GLOBAL_LOCAL_DISK_SET_DRIVES access must stay behind ECStore runtime-source helpers: $(paste -sd '; ' "$GLOBAL_LOCAL_DISK_SET_DRIVES_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '\bGLOBAL_(IsErasureSD|IS_ERASURE_SD)\b' \
    crates rustfs fuzz \
    --glob '*.rs' |
    rg -v '^crates/ecstore/src/runtime/(global|sources)\.rs:' || true
) >"$GLOBAL_ERASURE_SD_BYPASS_HITS_FILE"

if [[ -s "$GLOBAL_ERASURE_SD_BYPASS_HITS_FILE" ]]; then
  report_failure "GLOBAL_IS_ERASURE_SD access must stay behind ECStore runtime-source helpers: $(paste -sd '; ' "$GLOBAL_ERASURE_SD_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '\bGLOBAL_(RootDiskThreshold|ROOT_DISK_THRESHOLD)\b' \
    crates rustfs fuzz \
    --glob '*.rs' |
    rg -v '^crates/ecstore/src/runtime/(global|sources)\.rs:' || true
) >"$GLOBAL_ROOT_DISK_THRESHOLD_BYPASS_HITS_FILE"

if [[ -s "$GLOBAL_ROOT_DISK_THRESHOLD_BYPASS_HITS_FILE" ]]; then
  report_failure "GLOBAL_ROOT_DISK_THRESHOLD access must stay behind ECStore runtime-source helpers: $(paste -sd '; ' "$GLOBAL_ROOT_DISK_THRESHOLD_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '\bGLOBAL_(ExpiryState|EXPIRY_STATE|TransitionState|TRANSITION_STATE)\b' \
    crates rustfs fuzz \
    --glob '*.rs' |
    rg -v '^crates/ecstore/src/(bucket/lifecycle/bucket_lifecycle_ops|runtime/sources)\.rs:' || true
) >"$GLOBAL_LIFECYCLE_STATE_BYPASS_HITS_FILE"

if [[ -s "$GLOBAL_LIFECYCLE_STATE_BYPASS_HITS_FILE" ]]; then
  report_failure "lifecycle state globals must stay behind ECStore lifecycle owner and runtime-source helpers: $(paste -sd '; ' "$GLOBAL_LIFECYCLE_STATE_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '\bGLOBAL_(LifecycleSys|LIFECYCLE_SYS)\b' \
    crates rustfs fuzz \
    --glob '*.rs' |
    rg -v '^crates/ecstore/src/runtime/(global|sources)\.rs:' || true
) >"$GLOBAL_LIFECYCLE_SYS_BYPASS_HITS_FILE"

if [[ -s "$GLOBAL_LIFECYCLE_SYS_BYPASS_HITS_FILE" ]]; then
  report_failure "GLOBAL_LIFECYCLE_SYS access must stay behind ECStore runtime-source helpers: $(paste -sd '; ' "$GLOBAL_LIFECYCLE_SYS_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '\bGLOBAL_(EventNotifier|EVENT_NOTIFIER)\b' \
    crates rustfs fuzz \
    --glob '*.rs' |
    rg -v '^crates/ecstore/src/runtime/(global|sources)\.rs:' || true
) >"$GLOBAL_EVENT_NOTIFIER_BYPASS_HITS_FILE"

if [[ -s "$GLOBAL_EVENT_NOTIFIER_BYPASS_HITS_FILE" ]]; then
  report_failure "GLOBAL_EVENT_NOTIFIER access must stay behind ECStore runtime-source helpers: $(paste -sd '; ' "$GLOBAL_EVENT_NOTIFIER_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '\bGLOBAL_(NotificationSys|NOTIFICATION_SYS)\b' \
    crates rustfs fuzz \
    --glob '*.rs' |
    rg -v '^crates/ecstore/src/services/notification_sys\.rs:' || true
) >"$GLOBAL_NOTIFICATION_SYS_BYPASS_HITS_FILE"

if [[ -s "$GLOBAL_NOTIFICATION_SYS_BYPASS_HITS_FILE" ]]; then
  report_failure "GLOBAL_NOTIFICATION_SYS access must stay behind ECStore notification owner helpers: $(paste -sd '; ' "$GLOBAL_NOTIFICATION_SYS_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '\bGLOBAL_BOOT_TIME\b' \
    crates rustfs fuzz \
    --glob '*.rs' |
    rg -v '^crates/ecstore/src/runtime/(global|sources)\.rs:' || true
) >"$GLOBAL_BOOT_TIME_BYPASS_HITS_FILE"

if [[ -s "$GLOBAL_BOOT_TIME_BYPASS_HITS_FILE" ]]; then
  report_failure "GLOBAL_BOOT_TIME access must stay behind ECStore runtime-source helpers: $(paste -sd '; ' "$GLOBAL_BOOT_TIME_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '\bGLOBAL_(LocalNodeName(Hex)?|LOCAL_NODE_NAME(_HEX)?_FALLBACK)\b' \
    crates rustfs fuzz \
    --glob '*.rs' |
    rg -v '^crates/ecstore/src/runtime/(global|sources)\.rs:' || true
) >"$GLOBAL_ECSTORE_LOCAL_NODE_NAME_BYPASS_HITS_FILE"

if [[ -s "$GLOBAL_ECSTORE_LOCAL_NODE_NAME_BYPASS_HITS_FILE" ]]; then
  report_failure "ECStore local node fallback globals must stay behind ECStore runtime-source helpers: $(paste -sd '; ' "$GLOBAL_ECSTORE_LOCAL_NODE_NAME_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '\bGLOBAL_(DEPLOYMENT_ID|REGION|RUSTFS_PORT)\b|\bglobalDeploymentIDPtr\b|\bruntime::global::global_rustfs_port\b' \
    crates rustfs fuzz \
    --glob '*.rs' |
    rg -v '^(crates/common/src/globals|crates/ecstore/src/runtime/(global|sources))\.rs:' || true
) >"$GLOBAL_RUNTIME_SCALAR_BYPASS_HITS_FILE"

if [[ -s "$GLOBAL_RUNTIME_SCALAR_BYPASS_HITS_FILE" ]]; then
  report_failure "runtime scalar globals must stay behind owner helpers: $(paste -sd '; ' "$GLOBAL_RUNTIME_SCALAR_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '\bGLOBAL_BACKGROUND_SERVICES_CANCEL_TOKEN\b|\bget_background_services_cancel_token\b' \
    crates rustfs fuzz \
    --glob '*.rs' |
    rg -v '^crates/ecstore/src/runtime/(global|sources)\.rs:' || true
) >"$GLOBAL_BACKGROUND_CANCEL_BYPASS_HITS_FILE"

if [[ -s "$GLOBAL_BACKGROUND_CANCEL_BYPASS_HITS_FILE" ]]; then
  report_failure "background service cancellation must stay behind ECStore runtime-source helpers: $(paste -sd '; ' "$GLOBAL_BACKGROUND_CANCEL_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '\bAUDIT_SYSTEM\b' \
    crates rustfs fuzz \
    --glob '*.rs' |
    rg -v '^crates/audit/src/global\.rs:' || true
) >"$AUDIT_SYSTEM_BYPASS_HITS_FILE"

if [[ -s "$AUDIT_SYSTEM_BYPASS_HITS_FILE" ]]; then
  report_failure "AUDIT_SYSTEM access must stay behind audit owner helpers: $(paste -sd '; ' "$AUDIT_SYSTEM_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '\bGLOBAL_(HEAL_MANAGER|HEAL_CHANNEL_PROCESSOR|AHM_SERVICES_CANCEL_TOKEN)\b' \
    crates rustfs fuzz \
    --glob '*.rs' |
    rg -v '^crates/heal/src/lib\.rs:' || true
) >"$HEAL_OWNER_GLOBAL_BYPASS_HITS_FILE"

if [[ -s "$HEAL_OWNER_GLOBAL_BYPASS_HITS_FILE" ]]; then
  report_failure "heal owner globals must stay behind heal owner helpers: $(paste -sd '; ' "$HEAL_OWNER_GLOBAL_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '\bGLOBAL_(LOCAL_LOCK_CLIENT|LOCK_CLIENTS)\b' \
    crates rustfs fuzz \
    --glob '*.rs' |
    rg -v '^crates/ecstore/src/runtime/global\.rs:' || true
) >"$GLOBAL_LOCK_CLIENTS_BYPASS_HITS_FILE"

if [[ -s "$GLOBAL_LOCK_CLIENTS_BYPASS_HITS_FILE" ]]; then
  report_failure "ECStore lock client globals must stay behind ECStore runtime helpers: $(paste -sd '; ' "$GLOBAL_LOCK_CLIENTS_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '\bGLOBAL_PROCESSORS\b' \
    crates rustfs fuzz \
    --glob '*.rs' |
    rg -v '^crates/ecstore/src/services/batch_processor\.rs:' || true
) >"$GLOBAL_BATCH_PROCESSORS_BYPASS_HITS_FILE"

if [[ -s "$GLOBAL_BATCH_PROCESSORS_BYPASS_HITS_FILE" ]]; then
  report_failure "GLOBAL_PROCESSORS access must stay behind ECStore batch processor owner helpers: $(paste -sd '; ' "$GLOBAL_BATCH_PROCESSORS_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '\bINTERNODE_DATA_TRANSPORT\b' \
    crates rustfs fuzz \
    --glob '*.rs' |
    rg -v '^crates/ecstore/src/cluster/rpc/internode_data_transport\.rs:' || true
) >"$INTERNODE_DATA_TRANSPORT_BYPASS_HITS_FILE"

if [[ -s "$INTERNODE_DATA_TRANSPORT_BYPASS_HITS_FILE" ]]; then
  report_failure "internode data transport static must stay behind ECStore internode transport helpers: $(paste -sd '; ' "$INTERNODE_DATA_TRANSPORT_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '\bGLOBAL_CAPACITY_MANAGER\b' \
    crates rustfs fuzz \
    --glob '*.rs' |
    rg -v '^crates/object-capacity/src/capacity_manager\.rs:' || true
) >"$GLOBAL_CAPACITY_MANAGER_BYPASS_HITS_FILE"

if [[ -s "$GLOBAL_CAPACITY_MANAGER_BYPASS_HITS_FILE" ]]; then
  report_failure "GLOBAL_CAPACITY_MANAGER access must stay behind rustfs_object_capacity helpers: $(paste -sd '; ' "$GLOBAL_CAPACITY_MANAGER_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '\bGLOBAL_CONN_MAP\b' \
    crates rustfs fuzz \
    --glob '*.rs' |
    rg -v '^crates/common/src/globals\.rs:' || true
) >"$GLOBAL_CONN_MAP_BYPASS_HITS_FILE"

if [[ -s "$GLOBAL_CONN_MAP_BYPASS_HITS_FILE" ]]; then
  report_failure "GLOBAL_CONN_MAP access must stay behind rustfs_common connection cache helpers: $(paste -sd '; ' "$GLOBAL_CONN_MAP_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '\bGLOBAL_LOCAL_NODE_NAME\b' \
    crates rustfs fuzz \
    --glob '*.rs' |
    rg -v '^crates/common/src/globals\.rs:' || true
) >"$GLOBAL_LOCAL_NODE_NAME_BYPASS_HITS_FILE"

if [[ -s "$GLOBAL_LOCAL_NODE_NAME_BYPASS_HITS_FILE" ]]; then
  report_failure "GLOBAL_LOCAL_NODE_NAME access must stay behind rustfs_common runtime helpers: $(paste -sd '; ' "$GLOBAL_LOCAL_NODE_NAME_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '\bGLOBAL_RUSTFS_(HOST|ADDR)\b' \
    crates rustfs fuzz \
    --glob '*.rs' |
    rg -v '^crates/common/src/globals\.rs:' || true
) >"$GLOBAL_RUSTFS_ADDR_BYPASS_HITS_FILE"

if [[ -s "$GLOBAL_RUSTFS_ADDR_BYPASS_HITS_FILE" ]]; then
  report_failure "GLOBAL_RUSTFS_HOST/ADDR access must stay behind rustfs_common runtime helpers: $(paste -sd '; ' "$GLOBAL_RUSTFS_ADDR_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '\bGLOBAL_(ROOT_CERT|MTLS_IDENTITY|OUTBOUND_TLS_GENERATION)\b' \
    crates rustfs fuzz \
    --glob '*.rs' |
    rg -v '^crates/common/src/globals\.rs:' || true
) >"$GLOBAL_OUTBOUND_TLS_BYPASS_HITS_FILE"

if [[ -s "$GLOBAL_OUTBOUND_TLS_BYPASS_HITS_FILE" ]]; then
  report_failure "outbound TLS globals must stay behind rustfs_common runtime helpers: $(paste -sd '; ' "$GLOBAL_OUTBOUND_TLS_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '\bGLOBAL_RUSTFS_RPC_SECRET\b' \
    crates rustfs fuzz \
    --glob '*.rs' |
    rg -v '^crates/credentials/src/credentials\.rs:' || true
) >"$GLOBAL_RPC_SECRET_BYPASS_HITS_FILE"

if [[ -s "$GLOBAL_RPC_SECRET_BYPASS_HITS_FILE" ]]; then
  report_failure "GLOBAL_RUSTFS_RPC_SECRET access must stay behind rustfs_credentials helpers: $(paste -sd '; ' "$GLOBAL_RPC_SECRET_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '\bGLOBAL_KMS_SERVICE_MANAGER\b' \
    crates rustfs fuzz \
    --glob '*.rs' |
    rg -v '^crates/kms/src/service_manager\.rs:' || true
) >"$GLOBAL_KMS_SERVICE_MANAGER_BYPASS_HITS_FILE"

if [[ -s "$GLOBAL_KMS_SERVICE_MANAGER_BYPASS_HITS_FILE" ]]; then
  report_failure "GLOBAL_KMS_SERVICE_MANAGER access must stay behind rustfs_kms service manager helpers: $(paste -sd '; ' "$GLOBAL_KMS_SERVICE_MANAGER_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '\bGLOBAL_(TierConfigMgr|TIER_CONFIG_MGR)\b' \
    crates rustfs fuzz \
    --glob '*.rs' |
    rg -v '^crates/ecstore/src/runtime/(global|sources)\.rs:' || true
) >"$GLOBAL_TIER_CONFIG_MGR_BYPASS_HITS_FILE"

if [[ -s "$GLOBAL_TIER_CONFIG_MGR_BYPASS_HITS_FILE" ]]; then
  report_failure "GLOBAL_TIER_CONFIG_MGR access must stay behind ECStore runtime-source helpers: $(paste -sd '; ' "$GLOBAL_TIER_CONFIG_MGR_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  {
    rg -n --with-filename 'rustfs_ecstore::config::(?:\{[^}]*\b(?:Config|KV|KVS|register_default_kvs|get_global_server_config|set_global_server_config)\b|(?:Config|KV|KVS|register_default_kvs|get_global_server_config|set_global_server_config)\b)' \
      crates rustfs fuzz \
      --glob '*.rs' || true
    rg -n --with-filename 'pub(?:\([^)]*\))?\s+use\s+(?:crate::config|rustfs_config::server_config)::(?:\{[^}]*\b(?:Config|KV|KVS|register_default_kvs|get_global_server_config|set_global_server_config)\b|(?:Config|KV|KVS|register_default_kvs|get_global_server_config|set_global_server_config)\b)' \
      crates/ecstore/src crates/ecstore/tests \
      --glob '*.rs' || true
  }
) >"$LEGACY_ECSTORE_CONFIG_MODEL_HITS_FILE"

if [[ -s "$LEGACY_ECSTORE_CONFIG_MODEL_HITS_FILE" ]]; then
  report_failure "server-config model and global accessors must stay owned by rustfs_config::server_config, not ECStore compatibility paths: $(paste -sd '; ' "$LEGACY_ECSTORE_CONFIG_MODEL_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '^(?:pub\(crate\) )?use rustfs_ecstore::api::[a-z_]+ as ecstore_[a-z_]+;' \
    crates/heal/src crates/iam/src crates/notify/src crates/obs/src crates/protocols/src crates/s3select-api/src crates/scanner/src \
    --glob '*.rs' |
    rg -v '^(crates/heal/src/heal/storage_api.rs|crates/iam/src/lib.rs|crates/notify/src/lib.rs|crates/obs/src/metrics/mod.rs|crates/protocols/src/swift/mod.rs|crates/s3select-api/src/lib.rs|crates/scanner/src/storage_api.rs):' || true
) >"$EXTERNAL_PRODUCTION_ECSTORE_IMPORT_HITS_FILE"

if [[ -s "$EXTERNAL_PRODUCTION_ECSTORE_IMPORT_HITS_FILE" ]]; then
  report_failure "external production ECStore facade imports must stay at crate or module owner roots: $(paste -sd '; ' "$EXTERNAL_PRODUCTION_ECSTORE_IMPORT_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'crate::(?:admin|app|storage)::storage_compat::ecstore_|crate::storage_compat::ecstore_' rustfs/src crates fuzz \
    --glob '*storage_compat.rs' \
    --glob '*_compat.rs' || true
) >"$ALL_STORAGE_COMPAT_SELF_FACADE_PATH_HITS_FILE"

if [[ -s "$ALL_STORAGE_COMPAT_SELF_FACADE_PATH_HITS_FILE" ]]; then
  report_failure "storage compatibility boundaries must reference local ECStore facade aliases directly or through super::, not crate-qualified storage_compat self paths: $(paste -sd '; ' "$ALL_STORAGE_COMPAT_SELF_FACADE_PATH_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'crate::(?:admin|app|storage)::storage_compat' \
    rustfs/src/admin rustfs/src/app rustfs/src/storage \
    --glob '*storage_compat.rs' \
    --glob '*_storage_compat.rs' || true
) >"$RUSTFS_LOCAL_COMPAT_OWNER_SELF_PATH_HITS_FILE"

if [[ -s "$RUSTFS_LOCAL_COMPAT_OWNER_SELF_PATH_HITS_FILE" ]]; then
  report_failure "RustFS local compatibility bridge modules must use relative owner storage_compat paths instead of crate-qualified owner self paths: $(paste -sd '; ' "$RUSTFS_LOCAL_COMPAT_OWNER_SELF_PATH_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'crate::startup_storage_compat' \
    rustfs/src/startup_*.rs \
    rustfs/src/init.rs \
    rustfs/src/runtime_capabilities.rs \
    rustfs/src/workload_admission.rs \
    rustfs/src/table_catalog.rs \
    rustfs/src/error.rs || true
) >"$RUSTFS_ROOT_COMPAT_RELATIVE_CONSUMER_HITS_FILE"

if [[ -s "$RUSTFS_ROOT_COMPAT_RELATIVE_CONSUMER_HITS_FILE" ]]; then
  report_failure "RustFS root compatibility consumers must use relative owner paths instead of crate-qualified root compatibility paths: $(paste -sd '; ' "$RUSTFS_ROOT_COMPAT_RELATIVE_CONSUMER_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'crate::storage::core_storage_compat' \
    rustfs/src/storage/mod.rs \
    rustfs/src/storage/ecfs.rs \
    rustfs/src/storage/ecfs_extend.rs \
    rustfs/src/storage/access.rs \
    rustfs/src/storage/head_prefix.rs \
    rustfs/src/storage/options.rs \
    rustfs/src/storage/sse.rs || true
) >"$RUSTFS_STORAGE_CORE_COMPAT_RELATIVE_CONSUMER_HITS_FILE"

if [[ -s "$RUSTFS_STORAGE_CORE_COMPAT_RELATIVE_CONSUMER_HITS_FILE" ]]; then
  report_failure "RustFS storage owner consumers must use relative core_storage_compat paths instead of crate-qualified storage owner paths: $(paste -sd '; ' "$RUSTFS_STORAGE_CORE_COMPAT_RELATIVE_CONSUMER_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  {
    rg -n --with-filename 'crate::app::usecase_storage_compat' \
      rustfs/src/app/*.rs || true
    rg -n --with-filename 'crate::admin::router_storage_compat' \
      rustfs/src/admin/router.rs || true
    rg -n --with-filename 'crate::storage::core_storage_compat' \
      rustfs/src/storage/ecfs_test.rs || true
  }
) >"$RUSTFS_LOCAL_COMPAT_RELATIVE_CONSUMER_HITS_FILE"

if [[ -s "$RUSTFS_LOCAL_COMPAT_RELATIVE_CONSUMER_HITS_FILE" ]]; then
  report_failure "RustFS local compatibility consumers must use relative owner paths instead of crate-qualified local compatibility paths: $(paste -sd '; ' "$RUSTFS_LOCAL_COMPAT_RELATIVE_CONSUMER_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'router_storage_compat|usecase_storage_compat' \
    rustfs/src/admin \
    rustfs/src/app \
    --glob '*.rs' || true
) >"$RUSTFS_APP_ADMIN_SECONDARY_COMPAT_BRIDGE_HITS_FILE"

if [[ -s "$RUSTFS_APP_ADMIN_SECONDARY_COMPAT_BRIDGE_HITS_FILE" ]]; then
  report_failure "RustFS app/admin consumers must route directly through their owner storage_compat boundary instead of secondary compatibility bridges: $(paste -sd '; ' "$RUSTFS_APP_ADMIN_SECONDARY_COMPAT_BRIDGE_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'core_storage_compat' \
    rustfs/src/storage \
    --glob '*.rs' || true
) >"$RUSTFS_STORAGE_SECONDARY_COMPAT_BRIDGE_HITS_FILE"

if [[ -s "$RUSTFS_STORAGE_SECONDARY_COMPAT_BRIDGE_HITS_FILE" ]]; then
  report_failure "RustFS storage owner consumers must route directly through storage_compat instead of the secondary core_storage_compat bridge: $(paste -sd '; ' "$RUSTFS_STORAGE_SECONDARY_COMPAT_BRIDGE_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  {
    for file in \
      rustfs/src/admin/handlers/storage_compat.rs \
      rustfs/src/admin/service/storage_compat.rs \
      rustfs/src/app/context/storage_compat.rs \
      rustfs/src/storage/rpc/storage_compat.rs; do
      [[ -e "$file" ]] && printf '%s:1:secondary bridge file exists\n' "$file"
    done
    rg -n --with-filename 'mod storage_compat' \
      rustfs/src/admin/handlers/mod.rs \
      rustfs/src/admin/service/mod.rs \
      rustfs/src/app/context.rs \
      rustfs/src/storage/rpc/mod.rs || true
    if rg -n --with-filename --pcre2 '(?<!super::)super::storage_compat' \
      rustfs/src/admin/handlers \
      -g '*.rs' \
      -g '!storage_compat.rs' >/dev/null 2>&1; then
      rg -n --with-filename --pcre2 '(?<!super::)super::storage_compat' \
        rustfs/src/admin/handlers \
        -g '*.rs' \
        -g '!storage_compat.rs' || true
    else
      find rustfs/src/admin/handlers -type f -name '*.rs' ! -name 'storage_compat.rs' -print0 |
        xargs -0 perl -ne 'print "$ARGV:$.:$_" if /super::storage_compat/ && !/super::super::storage_compat/' || true
    fi
  }
) >"$RUSTFS_NESTED_SECONDARY_COMPAT_BRIDGE_HITS_FILE"

if [[ -s "$RUSTFS_NESTED_SECONDARY_COMPAT_BRIDGE_HITS_FILE" ]]; then
  report_failure "RustFS nested and handler consumers must route directly through owner storage_compat instead of secondary compatibility bridges: $(paste -sd '; ' "$RUSTFS_NESTED_SECONDARY_COMPAT_BRIDGE_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  {
    rg -n --with-filename 'crate::storage::rpc::storage_compat' \
      rustfs/src/storage/rpc/http_service.rs \
      rustfs/src/storage/rpc/node_service.rs || true
    rg -n --with-filename 'crate::storage::s3_api::storage_compat' \
      rustfs/src/storage/s3_api/bucket.rs \
      rustfs/src/storage/s3_api/multipart.rs || true
  }
) >"$RUSTFS_STORAGE_LOCAL_COMPAT_RELATIVE_CONSUMER_HITS_FILE"

if [[ -s "$RUSTFS_STORAGE_LOCAL_COMPAT_RELATIVE_CONSUMER_HITS_FILE" ]]; then
  report_failure "RustFS storage RPC/S3 API compatibility consumers must use relative owner paths instead of crate-qualified local compatibility paths: $(paste -sd '; ' "$RUSTFS_STORAGE_LOCAL_COMPAT_RELATIVE_CONSUMER_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  {
    for file in \
      rustfs/src/capacity/storage_compat.rs \
      rustfs/src/server/storage_compat.rs \
      rustfs/src/storage/s3_api/storage_compat.rs; do
      [[ -e "$file" ]] && printf '%s:1:runtime local bridge file exists\n' "$file"
    done
    rg -n --with-filename 'mod storage_compat|pub\(crate\) mod storage_compat' \
      rustfs/src/capacity/mod.rs \
      rustfs/src/server/mod.rs \
      rustfs/src/storage/s3_api/mod.rs || true
    rg -n --with-filename 'super::storage_compat' \
      rustfs/src/capacity \
      rustfs/src/server \
      rustfs/src/storage/s3_api \
      -g '*.rs' || true
  }
) >"$RUSTFS_RUNTIME_LOCAL_COMPAT_BRIDGE_HITS_FILE"

if [[ -s "$RUSTFS_RUNTIME_LOCAL_COMPAT_BRIDGE_HITS_FILE" ]]; then
  report_failure "RustFS capacity/server/S3 API runtime consumers must use direct owner APIs instead of local compatibility bridge modules: $(paste -sd '; ' "$RUSTFS_RUNTIME_LOCAL_COMPAT_BRIDGE_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  {
    for file in \
      rustfs/src/config_storage_compat.rs \
      rustfs/src/error_storage_compat.rs \
      rustfs/src/runtime_capabilities_storage_compat.rs \
      rustfs/src/table_catalog_storage_compat.rs \
      rustfs/src/workload_admission_storage_compat.rs; do
      [[ -e "$file" ]] && printf '%s:1:root one-off bridge file exists\n' "$file"
    done
    rg -n --with-filename '\bmod\s+(?:config_storage_compat|error_storage_compat|runtime_capabilities_storage_compat|table_catalog_storage_compat|workload_admission_storage_compat)|(?:crate::|self::|super::)(?:config_storage_compat|error_storage_compat|runtime_capabilities_storage_compat|table_catalog_storage_compat|workload_admission_storage_compat)|(?:config_storage_compat|error_storage_compat|runtime_capabilities_storage_compat|table_catalog_storage_compat|workload_admission_storage_compat)::' \
      rustfs/src \
      -g '*.rs' || true
  }
) >"$RUSTFS_ROOT_ONE_OFF_COMPAT_BRIDGE_HITS_FILE"

if [[ -s "$RUSTFS_ROOT_ONE_OFF_COMPAT_BRIDGE_HITS_FILE" ]]; then
  report_failure "RustFS root one-off consumers must use direct ECStore owner APIs instead of compatibility bridge modules: $(paste -sd '; ' "$RUSTFS_ROOT_ONE_OFF_COMPAT_BRIDGE_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  {
    [[ -e rustfs/src/startup_storage_compat.rs ]] && printf '%s:1:startup bridge file exists\n' "rustfs/src/startup_storage_compat.rs"
    rg -n --with-filename '\bmod\s+startup_storage_compat|(?:crate::|self::|super::)startup_storage_compat|startup_storage_compat::' \
      rustfs/src \
      -g '*.rs' || true
  }
) >"$RUSTFS_STARTUP_COMPAT_BRIDGE_HITS_FILE"

if [[ -s "$RUSTFS_STARTUP_COMPAT_BRIDGE_HITS_FILE" ]]; then
  report_failure "RustFS startup consumers must use direct ECStore owner APIs instead of startup compatibility bridge modules: $(paste -sd '; ' "$RUSTFS_STARTUP_COMPAT_BRIDGE_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  {
    rg -n --with-filename 'crate::admin::handlers::storage_compat' \
      rustfs/src/admin/handlers \
      -g '*.rs' \
      -g '!storage_compat.rs' || true
    rg -n --with-filename 'crate::admin::service::storage_compat' \
      rustfs/src/admin/service \
      -g '*.rs' \
      -g '!storage_compat.rs' || true
  }
) >"$RUSTFS_ADMIN_LOCAL_COMPAT_RELATIVE_CONSUMER_HITS_FILE"

if [[ -s "$RUSTFS_ADMIN_LOCAL_COMPAT_RELATIVE_CONSUMER_HITS_FILE" ]]; then
  report_failure "RustFS admin compatibility consumers must use relative owner paths instead of crate-qualified local compatibility paths: $(paste -sd '; ' "$RUSTFS_ADMIN_LOCAL_COMPAT_RELATIVE_CONSUMER_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  {
    rg -n --with-filename 'crate::app::context::storage_compat' \
      rustfs/src/app/context \
      -g '*.rs' \
      -g '!storage_compat.rs' || true
    rg -n --with-filename 'crate::server::storage_compat' \
      rustfs/src/server/readiness.rs || true
  }
) >"$RUSTFS_APP_SERVER_LOCAL_COMPAT_RELATIVE_CONSUMER_HITS_FILE"

if [[ -s "$RUSTFS_APP_SERVER_LOCAL_COMPAT_RELATIVE_CONSUMER_HITS_FILE" ]]; then
  report_failure "RustFS app context/server compatibility consumers must use relative owner paths instead of crate-qualified local compatibility paths: $(paste -sd '; ' "$RUSTFS_APP_SERVER_LOCAL_COMPAT_RELATIVE_CONSUMER_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  {
    rg -n --with-filename 'crate::config_storage_compat' \
      rustfs/src/config/config_test.rs || true
    rg -n --with-filename 'crate::heal::storage_compat' \
      crates/heal/src/error.rs \
      crates/heal/src/heal/channel.rs || true
    rg -n --with-filename 'crate::common::storage_compat' \
      crates/heal/tests \
      crates/scanner/tests \
      -g '*.rs' || true
  }
) >"$RUSTFS_HEAL_TEST_LOCAL_COMPAT_RELATIVE_CONSUMER_HITS_FILE"

if [[ -s "$RUSTFS_HEAL_TEST_LOCAL_COMPAT_RELATIVE_CONSUMER_HITS_FILE" ]]; then
  report_failure "RustFS config/heal/scanner compatibility consumers must use relative owner paths instead of crate-qualified local compatibility paths: $(paste -sd '; ' "$RUSTFS_HEAL_TEST_LOCAL_COMPAT_RELATIVE_CONSUMER_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  {
    for file in \
      crates/heal/tests/common/storage_compat.rs \
      crates/scanner/tests/common/storage_compat.rs \
      crates/e2e_test/src/ecstore_test_compat.rs \
      crates/heal/tests/ecstore_test_compat/mod.rs \
      crates/scanner/tests/ecstore_test_compat/mod.rs \
      fuzz/fuzz_targets/ecstore_fuzz_compat.rs \
      fuzz/fuzz_targets/bucket_validation/storage_compat.rs \
      fuzz/fuzz_targets/path_containment/storage_compat.rs; do
      [[ -e "$file" ]] && printf '%s:1:test/fuzz bridge file exists\n' "$file"
    done
    rg -n --with-filename 'common::storage_compat|storage_compat::|\bmod\s+storage_compat|#\[path\s*=\s*"[^"]*storage_compat\.rs"\]|ecstore_test_compat|ecstore_fuzz_compat' \
      crates/e2e_test/src \
      crates/heal/tests \
      crates/scanner/tests \
      fuzz/fuzz_targets/bucket_validation.rs \
      fuzz/fuzz_targets/path_containment.rs \
      -g '*.rs' || true
  }
) >"$TEST_FUZZ_COMPAT_BRIDGE_HITS_FILE"

if [[ -s "$TEST_FUZZ_COMPAT_BRIDGE_HITS_FILE" ]]; then
  report_failure "test and fuzz targets must import ECStore owner APIs directly instead of local compatibility bridges: $(paste -sd '; ' "$TEST_FUZZ_COMPAT_BRIDGE_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  {
    for file in \
      crates/e2e_test/src/storage_compat.rs \
      rustfs/src/storage/ecstore_compat.rs \
      crates/heal/src/heal/ecstore_compat.rs \
      crates/iam/src/ecstore_compat.rs \
      crates/iam/src/store/storage_compat.rs \
      crates/notify/src/ecstore_compat.rs \
      crates/notify/src/storage_compat.rs \
      crates/obs/src/metrics/ecstore_compat.rs \
      crates/obs/src/storage_compat.rs \
      crates/protocols/src/swift/ecstore_compat.rs \
      crates/protocols/src/swift/storage_compat.rs \
      crates/scanner/src/ecstore_compat.rs \
      crates/s3select-api/src/ecstore_compat.rs \
      crates/s3select-api/src/storage_compat.rs; do
      [[ -e "$file" ]] && printf '%s:1:standalone thin bridge file exists\n' "$file"
    done
    rg -n --with-filename 'storage_compat' \
      crates/e2e_test/src \
      crates/notify/src \
      crates/obs/src \
      crates/protocols/src/swift \
      crates/s3select-api/src \
      -g '*.rs' || true
    rg -n --with-filename 'ecstore_compat' \
      rustfs/src/storage \
      crates/heal/src/heal \
      crates/iam/src \
      crates/notify/src \
      crates/obs/src/metrics \
      crates/protocols/src/swift \
      crates/scanner/src \
      crates/s3select-api/src \
      -g '*.rs' || true
    rg -n --with-filename '^\s*use\s+super::storage_compat|store::storage_compat|\bmod\s+storage_compat' \
      crates/iam/src/store.rs \
      crates/iam/src/store/object.rs || true
  }
) >"$STANDALONE_THIN_COMPAT_BRIDGE_HITS_FILE"

if [[ -s "$STANDALONE_THIN_COMPAT_BRIDGE_HITS_FILE" ]]; then
  report_failure "storage owner and standalone e2e/IAM/heal/scanner/notify/obs/swift/s3select consumers must import owner APIs directly instead of local thin compatibility bridges: $(paste -sd '; ' "$STANDALONE_THIN_COMPAT_BRIDGE_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  {
    [[ -e rustfs/src/app/context/compat.rs ]] && printf '%s:1:app context compatibility bridge file exists\n' "rustfs/src/app/context/compat.rs"
    [[ -e crates/notify/src/event_bridge.rs ]] && printf '%s:1:notify event bridge re-export file exists\n' "crates/notify/src/event_bridge.rs"
    rg -n --with-filename '\bmod\s+compat;|pub\s+use\s+compat::\*|\bmod\s+event_bridge;|pub\s+use\s+event_bridge::' \
      rustfs/src/app/context.rs \
      crates/notify/src/lib.rs || true
  }
) >"$APP_NOTIFY_THIN_COMPAT_BRIDGE_HITS_FILE"

if [[ -s "$APP_NOTIFY_THIN_COMPAT_BRIDGE_HITS_FILE" ]]; then
  report_failure "app context and notify event bridge thin compatibility modules must stay collapsed into owner roots: $(paste -sd '; ' "$APP_NOTIFY_THIN_COMPAT_BRIDGE_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  {
    for file in \
      crates/iam/src/storage_compat.rs \
      crates/heal/src/heal/storage_compat.rs \
      crates/scanner/src/storage_compat.rs; do
      [[ -e "$file" ]] && printf '%s:1:external owner bridge file exists\n' "$file"
    done
    rg -n --with-filename 'storage_compat' \
      crates/iam/src \
      crates/heal/src \
      crates/scanner/src \
      -g '*.rs' || true
  }
) >"$EXTERNAL_OWNER_COMPAT_BRIDGE_HITS_FILE"

if [[ -s "$EXTERNAL_OWNER_COMPAT_BRIDGE_HITS_FILE" ]]; then
  report_failure "IAM/heal/scanner consumers must import owner APIs directly instead of local storage compatibility bridges: $(paste -sd '; ' "$EXTERNAL_OWNER_COMPAT_BRIDGE_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'crate::storage_compat' \
    crates/scanner/src \
    crates/iam/src \
    crates/obs/src \
    crates/s3select-api/src \
    crates/e2e_test/src \
    fuzz/fuzz_targets \
    -g '*.rs' || true
) >"$STANDALONE_CRATE_LOCAL_COMPAT_RELATIVE_CONSUMER_HITS_FILE"

if [[ -s "$STANDALONE_CRATE_LOCAL_COMPAT_RELATIVE_CONSUMER_HITS_FILE" ]]; then
  report_failure "Standalone crate compatibility consumers must use relative owner paths instead of crate-qualified local compatibility paths: $(paste -sd '; ' "$STANDALONE_CRATE_LOCAL_COMPAT_RELATIVE_CONSUMER_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  if [[ -f crates/scanner/src/storage_compat.rs ]]; then
    rg -n --no-heading 'pub\(crate\)\s+use rustfs_ecstore::api::bucket::\{[^}]*\b(?:bucket_target_sys|lifecycle|metadata_sys|replication|versioning|versioning_sys)\b[^}]*\}\s*;' \
      crates/scanner/src/storage_compat.rs || true
  fi
) >"$SCANNER_BUCKET_STORAGE_COMPAT_MODULE_HITS_FILE"

if [[ -s "$SCANNER_BUCKET_STORAGE_COMPAT_MODULE_HITS_FILE" ]]; then
  report_failure "scanner storage compatibility must expose bucket contracts as explicit aliases: $(paste -sd '; ' "$SCANNER_BUCKET_STORAGE_COMPAT_MODULE_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'rustfs_ecstore::api::|rustfs_storage_api' \
    crates/scanner/src \
    --glob '*.rs' |
    rg -v '^crates/scanner/src/storage_api\.rs:' || true
) >"$SCANNER_STORAGE_API_SOURCE_BYPASS_HITS_FILE"

if [[ -s "$SCANNER_STORAGE_API_SOURCE_BYPASS_HITS_FILE" ]]; then
  report_failure "scanner source files must route ECStore and storage-api symbols through scanner::storage_api: $(paste -sd '; ' "$SCANNER_STORAGE_API_SOURCE_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename '\b(?:get_global_expiry_state|ecstore_get_global_expiry_state)\b' \
    crates/scanner/src \
    crates/scanner/tests \
    --glob '*.rs' || true
) >"$SCANNER_EXPIRY_STATE_BYPASS_HITS_FILE"

if [[ -s "$SCANNER_EXPIRY_STATE_BYPASS_HITS_FILE" ]]; then
  report_failure "scanner expiry-state access must use the ECStore runtime expiry_state_handle boundary: $(paste -sd '; ' "$SCANNER_EXPIRY_STATE_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'rustfs_ecstore::api::|rustfs_storage_api' \
    crates/scanner/tests \
    --glob '*.rs' |
    rg -v '^crates/scanner/tests/storage_api/mod\.rs:' || true
) >"$SCANNER_STORAGE_API_TEST_BYPASS_HITS_FILE"

if [[ -s "$SCANNER_STORAGE_API_TEST_BYPASS_HITS_FILE" ]]; then
  report_failure "scanner tests must route ECStore and storage-api symbols through scanner test storage_api: $(paste -sd '; ' "$SCANNER_STORAGE_API_TEST_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'rustfs_ecstore::api::|rustfs_storage_api' \
    crates/heal/src/heal \
    --glob '*.rs' |
    rg -v '^crates/heal/src/heal/storage_api\.rs:' || true
) >"$HEAL_STORAGE_API_SOURCE_BYPASS_HITS_FILE"

if [[ -s "$HEAL_STORAGE_API_SOURCE_BYPASS_HITS_FILE" ]]; then
  report_failure "heal source files must route ECStore and storage-api symbols through heal::storage_api: $(paste -sd '; ' "$HEAL_STORAGE_API_SOURCE_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'rustfs_ecstore::api::|rustfs_storage_api' \
    crates/heal/tests \
    --glob '*.rs' |
    rg -v '^crates/heal/tests/(endpoint_index_test|heal_bug_fixes_test|heal_integration_test)/storage_api\.rs:' || true
) >"$HEAL_STORAGE_API_TEST_BYPASS_HITS_FILE"

if [[ -s "$HEAL_STORAGE_API_TEST_BYPASS_HITS_FILE" ]]; then
  report_failure "heal tests must route ECStore and storage-api symbols through heal test storage_api: $(paste -sd '; ' "$HEAL_STORAGE_API_TEST_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'rustfs_ecstore::|^use rustfs_storage_api|rustfs_storage_api::' \
    crates/obs/src \
    --glob '*.rs' |
    rg -v '^crates/obs/src/metrics/storage_api\.rs:' || true
) >"$OBS_STORAGE_API_SOURCE_BYPASS_HITS_FILE"

if [[ -s "$OBS_STORAGE_API_SOURCE_BYPASS_HITS_FILE" ]]; then
  report_failure "obs source files must route ECStore and storage-api symbols through obs metrics storage_api boundary: $(paste -sd '; ' "$OBS_STORAGE_API_SOURCE_BYPASS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  if [[ -f crates/notify/src/storage_compat.rs ]]; then
    rg -n --no-heading 'use rustfs_ecstore::api::\{[^}]*\b(?:config|global)\b[^}]*\}\s*;' \
      crates/notify/src/storage_compat.rs || true
  fi
) >"$NOTIFY_STORAGE_COMPAT_MODULE_HITS_FILE"

if [[ -s "$NOTIFY_STORAGE_COMPAT_MODULE_HITS_FILE" ]]; then
  report_failure "notify storage compatibility must not import broad config/global modules: $(paste -sd '; ' "$NOTIFY_STORAGE_COMPAT_MODULE_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  if [[ -f crates/obs/src/storage_compat.rs ]]; then
    rg -n --no-heading 'pub\(crate\)\s+use rustfs_ecstore::api::data_usage::load_data_usage_from_backend' \
      crates/obs/src/storage_compat.rs || true
  fi
) >"$OBS_STORAGE_COMPAT_PASSTHROUGH_HITS_FILE"

if [[ -s "$OBS_STORAGE_COMPAT_PASSTHROUGH_HITS_FILE" ]]; then
  report_failure "OBS storage compatibility must wrap data-usage access instead of re-exporting ECStore functions: $(paste -sd '; ' "$OBS_STORAGE_COMPAT_PASSTHROUGH_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  if [[ -f crates/e2e_test/src/storage_compat.rs ]]; then
    rg -n --no-heading 'pub\(crate\)\s+use rustfs_ecstore::api::rpc::\{[^}]*\b(?:gen_tonic_signature_interceptor|node_service_time_out_client|node_service_time_out_client_no_auth)\b[^}]*\}\s*;' \
      crates/e2e_test/src/storage_compat.rs || true
  fi
) >"$E2E_STORAGE_COMPAT_RPC_PASSTHROUGH_HITS_FILE"

if [[ -s "$E2E_STORAGE_COMPAT_RPC_PASSTHROUGH_HITS_FILE" ]]; then
  report_failure "e2e storage compatibility must wrap RPC helpers instead of re-exporting broad ECStore functions: $(paste -sd '; ' "$E2E_STORAGE_COMPAT_RPC_PASSTHROUGH_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --no-heading 'pub\(crate\)\s+use rustfs_ecstore::api::(?:bucket::(?:lifecycle|metadata_sys|utils)|capacity|client|disk|layout|storage|tier)::[^;]*\{[^}]*\}\s*;|pub\(crate\)\s+use rustfs_ecstore::api::bucket::metadata_sys::init_bucket_metadata_sys|pub\(crate\)\s+use rustfs_ecstore::api::storage::init_local_disks' \
    crates/heal/tests crates/scanner/tests fuzz/fuzz_targets \
    --glob '*storage_compat.rs' || true
) >"$TEST_STORAGE_COMPAT_PASSTHROUGH_HITS_FILE"

if [[ -s "$TEST_STORAGE_COMPAT_PASSTHROUGH_HITS_FILE" ]]; then
  report_failure "test and fuzz storage compatibility boundaries must use explicit aliases or wrappers instead of grouped ECStore passthroughs: $(paste -sd '; ' "$TEST_STORAGE_COMPAT_PASSTHROUGH_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --no-heading '#!\[allow\(unused_imports\)\]' \
    rustfs/src crates/*/src fuzz/fuzz_targets \
    --glob '*storage_compat.rs' \
    --glob '!crates/ecstore/**' \
    --glob '!crates/e2e_test/**' || true
) >"$PRODUCTION_UNUSED_COMPAT_ALLOW_HITS_FILE"

if [[ -s "$PRODUCTION_UNUSED_COMPAT_ALLOW_HITS_FILE" ]]; then
  report_failure "production storage compatibility boundaries must not hide unused ECStore re-exports: $(paste -sd '; ' "$PRODUCTION_UNUSED_COMPAT_ALLOW_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --no-heading '(?:use|pub(?:\([^)]*\))? use) rustfs_ecstore::\{[^}]*\bstore_api\b[^}]*\}|(?:use|pub(?:\([^)]*\))? use) rustfs_ecstore::store_api\s*(?:;|as\b)' \
    rustfs/src crates/*/src fuzz/fuzz_targets \
    --glob '*storage_compat.rs' \
    --glob '!crates/ecstore/**' \
    --glob '!crates/e2e_test/**' || true
) >"$BROAD_STORE_API_COMPAT_REEXPORT_HITS_FILE"

if [[ -s "$BROAD_STORE_API_COMPAT_REEXPORT_HITS_FILE" ]]; then
  report_failure "storage compatibility boundaries must expose explicit store_api contracts only: $(paste -sd '; ' "$BROAD_STORE_API_COMPAT_REEXPORT_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --no-heading '\b(?:pub(?:\([^)]*\))?\s+)?mod\s+store_api\b' \
    rustfs/src crates/*/src fuzz/fuzz_targets \
    --glob '*storage_compat.rs' \
    --glob '!crates/ecstore/**' \
    --glob '!crates/e2e_test/**' || true
) >"$NESTED_STORE_API_COMPAT_MODULE_HITS_FILE"

if [[ -s "$NESTED_STORE_API_COMPAT_MODULE_HITS_FILE" ]]; then
  report_failure "storage compatibility boundaries must use direct type aliases instead of nested store_api modules: $(paste -sd '; ' "$NESTED_STORE_API_COMPAT_MODULE_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --no-heading 'rustfs_ecstore::store_api::' \
    rustfs/src crates/*/src fuzz/fuzz_targets \
    --glob '*storage_compat.rs' \
    --glob '!crates/ecstore/**' \
    --glob '!crates/e2e_test/**' || true
) >"$UNAPPROVED_STORE_API_COMPAT_ALIAS_HITS_FILE"

if [[ -s "$UNAPPROVED_STORE_API_COMPAT_ALIAS_HITS_FILE" ]]; then
  report_failure "storage compatibility boundaries must use rustfs_ecstore::object_api for ECStore-owned object DTO and reader aliases: $(paste -sd '; ' "$UNAPPROVED_STORE_API_COMPAT_ALIAS_HITS_FILE")"
fi

if rg -n --no-heading '^\s*pub\s+mod\s+store_api\s*;' "$ROOT_DIR/crates/ecstore/src/lib.rs" >"$PUBLIC_STORE_API_MODULE_HITS_FILE"; then
  report_failure "ECStore store_api must remain private; expose ECStore-owned object DTO and reader aliases through rustfs_ecstore::object_api"
fi

if rg -n --no-heading '^\s*pub\s+mod\s+(batch_processor|bitrot|cluster|erasure_coding|event|object_api|store_list_objects)\s*;' "$ROOT_DIR/crates/ecstore/src/lib.rs" >"$ECSTORE_PUBLIC_ROOT_MODULE_HITS_FILE"; then
  report_failure "facade-covered ECStore root modules must remain private; expose compatibility through rustfs_ecstore::api: $(paste -sd '; ' "$ECSTORE_PUBLIC_ROOT_MODULE_HITS_FILE")"
fi

if rg -n --no-heading '^\s*pub\s+use\s+global::' "$ROOT_DIR/crates/ecstore/src/lib.rs" >"$ECSTORE_PUBLIC_GLOBAL_REEXPORT_HITS_FILE"; then
  report_failure "ECStore global root re-exports must remain private; expose compatibility through rustfs_ecstore::api::global: $(paste -sd '; ' "$ECSTORE_PUBLIC_GLOBAL_REEXPORT_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  {
    [[ -e crates/ecstore/src/store_api.rs ]] && printf '%s\n' 'crates/ecstore/src/store_api.rs'
    [[ -d crates/ecstore/src/store_api ]] && printf '%s\n' 'crates/ecstore/src/store_api'
    true
  }
) >"$STORE_API_MODULE_PATH_HITS_FILE"

if [[ -s "$STORE_API_MODULE_PATH_HITS_FILE" ]]; then
  report_failure "legacy ECStore store_api module files must not be restored: $(paste -sd '; ' "$STORE_API_MODULE_PATH_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  {
    [[ -e crates/ecstore/src/store.rs ]] && printf '%s\n' 'crates/ecstore/src/store.rs'
    [[ -e crates/ecstore/src/set_disk.rs ]] && printf '%s\n' 'crates/ecstore/src/set_disk.rs'
    true
  }
) >"$ECSTORE_ROOT_STORE_SET_DISK_MODULE_HITS_FILE"

if [[ -s "$ECSTORE_ROOT_STORE_SET_DISK_MODULE_HITS_FILE" ]]; then
  report_failure "ECStore store and set_disk owners must stay in directory modules: $(paste -sd '; ' "$ECSTORE_ROOT_STORE_SET_DISK_MODULE_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  {
    [[ -e crates/ecstore/src/store_list_objects.rs ]] && printf '%s\n' 'crates/ecstore/src/store_list_objects.rs'
    [[ -e crates/ecstore/src/store_utils.rs ]] && printf '%s\n' 'crates/ecstore/src/store_utils.rs'
    [[ -e crates/ecstore/src/store_init.rs ]] && printf '%s\n' 'crates/ecstore/src/store_init.rs'
    true
  }
) >"$ECSTORE_ROOT_STORE_SUPPORT_MODULE_HITS_FILE"

if [[ -s "$ECSTORE_ROOT_STORE_SUPPORT_MODULE_HITS_FILE" ]]; then
  report_failure "ECStore store support modules must stay under the store owner directory: $(paste -sd '; ' "$ECSTORE_ROOT_STORE_SUPPORT_MODULE_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  {
    [[ -e crates/ecstore/src/disks_layout.rs ]] && printf '%s\n' 'crates/ecstore/src/disks_layout.rs'
    [[ -e crates/ecstore/src/endpoints.rs ]] && printf '%s\n' 'crates/ecstore/src/endpoints.rs'
    [[ -e crates/ecstore/src/storage_api_contracts.rs ]] && printf '%s\n' 'crates/ecstore/src/storage_api_contracts.rs'
    true
  }
) >"$ECSTORE_ROOT_LAYOUT_CONTRACT_SUPPORT_MODULE_HITS_FILE"

if [[ -s "$ECSTORE_ROOT_LAYOUT_CONTRACT_SUPPORT_MODULE_HITS_FILE" ]]; then
  report_failure "ECStore layout facade and storage API contract modules must stay under owner directories: $(paste -sd '; ' "$ECSTORE_ROOT_LAYOUT_CONTRACT_SUPPORT_MODULE_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  {
    [[ -e crates/ecstore/src/batch_processor.rs ]] && printf '%s\n' 'crates/ecstore/src/batch_processor.rs'
    [[ -e crates/ecstore/src/event_notification.rs ]] && printf '%s\n' 'crates/ecstore/src/event_notification.rs'
    [[ -e crates/ecstore/src/metrics_realtime.rs ]] && printf '%s\n' 'crates/ecstore/src/metrics_realtime.rs'
    [[ -e crates/ecstore/src/notification_sys.rs ]] && printf '%s\n' 'crates/ecstore/src/notification_sys.rs'
    true
  }
) >"$ECSTORE_ROOT_SERVICE_RUNTIME_MODULE_HITS_FILE"

if [[ -s "$ECSTORE_ROOT_SERVICE_RUNTIME_MODULE_HITS_FILE" ]]; then
  report_failure "ECStore service runtime modules must stay under the services owner directory: $(paste -sd '; ' "$ECSTORE_ROOT_SERVICE_RUNTIME_MODULE_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  {
    [[ -e crates/ecstore/src/data_movement.rs ]] && printf '%s\n' 'crates/ecstore/src/data_movement.rs'
    [[ -e crates/ecstore/src/data_movement_backpressure.rs ]] && printf '%s\n' 'crates/ecstore/src/data_movement_backpressure.rs'
    true
  }
) >"$ECSTORE_ROOT_DATA_MOVEMENT_MODULE_HITS_FILE"

if [[ -s "$ECSTORE_ROOT_DATA_MOVEMENT_MODULE_HITS_FILE" ]]; then
  report_failure "ECStore data movement modules must stay under the data_movement owner directory: $(paste -sd '; ' "$ECSTORE_ROOT_DATA_MOVEMENT_MODULE_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  {
    [[ -e crates/ecstore/src/admin_server_info.rs ]] && printf '%s\n' 'crates/ecstore/src/admin_server_info.rs'
    [[ -e crates/ecstore/src/data_usage.rs ]] && printf '%s\n' 'crates/ecstore/src/data_usage.rs'
    [[ -e crates/ecstore/src/get_diagnostics.rs ]] && printf '%s\n' 'crates/ecstore/src/get_diagnostics.rs'
    true
  }
) >"$ECSTORE_ROOT_USAGE_DIAGNOSTICS_MODULE_HITS_FILE"

if [[ -s "$ECSTORE_ROOT_USAGE_DIAGNOSTICS_MODULE_HITS_FILE" ]]; then
  report_failure "ECStore usage and diagnostics modules must stay under owner directories: $(paste -sd '; ' "$ECSTORE_ROOT_USAGE_DIAGNOSTICS_MODULE_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  {
    [[ -e crates/ecstore/src/global.rs ]] && printf '%s\n' 'crates/ecstore/src/global.rs'
    [[ -e crates/ecstore/src/runtime_sources.rs ]] && printf '%s\n' 'crates/ecstore/src/runtime_sources.rs'
    true
  }
) >"$ECSTORE_ROOT_RUNTIME_GLOBAL_MODULE_HITS_FILE"

if [[ -s "$ECSTORE_ROOT_RUNTIME_GLOBAL_MODULE_HITS_FILE" ]]; then
  report_failure "ECStore runtime global modules must stay under the runtime owner directory: $(paste -sd '; ' "$ECSTORE_ROOT_RUNTIME_GLOBAL_MODULE_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  {
    [[ -e crates/ecstore/src/bitrot.rs ]] && printf '%s\n' 'crates/ecstore/src/bitrot.rs'
    [[ -e crates/ecstore/src/compress.rs ]] && printf '%s\n' 'crates/ecstore/src/compress.rs'
    [[ -e crates/ecstore/src/rio.rs ]] && printf '%s\n' 'crates/ecstore/src/rio.rs'
    true
  }
) >"$ECSTORE_ROOT_IO_SUPPORT_MODULE_HITS_FILE"

if [[ -s "$ECSTORE_ROOT_IO_SUPPORT_MODULE_HITS_FILE" ]]; then
  report_failure "ECStore I/O support modules must stay under the io_support owner directory: $(paste -sd '; ' "$ECSTORE_ROOT_IO_SUPPORT_MODULE_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  {
    [[ -e crates/ecstore/src/erasure_codec ]] && printf '%s\n' 'crates/ecstore/src/erasure_codec'
    [[ -e crates/ecstore/src/erasure_coding ]] && printf '%s\n' 'crates/ecstore/src/erasure_coding'
    rg -n --with-filename '^mod erasure_(codec|coding);|#\[path = "erasure_(codec|coding)/' crates/ecstore/src/lib.rs || true
    true
  }
) >"$ECSTORE_ROOT_ERASURE_MODULE_HITS_FILE"

if [[ -s "$ECSTORE_ROOT_ERASURE_MODULE_HITS_FILE" ]]; then
  report_failure "ECStore erasure modules must stay under the erasure owner directory: $(paste -sd '; ' "$ECSTORE_ROOT_ERASURE_MODULE_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  {
    [[ -e crates/ecstore/src/error.rs ]] && printf '%s\n' 'crates/ecstore/src/error.rs'
    [[ -e crates/ecstore/src/rebalance.rs ]] && printf '%s\n' 'crates/ecstore/src/rebalance.rs'
    true
  }
) >"$ECSTORE_ROOT_ERROR_REBALANCE_MODULE_HITS_FILE"

if [[ -s "$ECSTORE_ROOT_ERROR_REBALANCE_MODULE_HITS_FILE" ]]; then
  report_failure "ECStore error and rebalance modules must stay under owner directories: $(paste -sd '; ' "$ECSTORE_ROOT_ERROR_REBALANCE_MODULE_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  {
    [[ -e crates/ecstore/src/rebalance ]] && printf '%s\n' 'crates/ecstore/src/rebalance'
    [[ -e crates/ecstore/src/tier ]] && printf '%s\n' 'crates/ecstore/src/tier'
    rg -n --with-filename '^\s*mod\s+(rebalance|tier);|#\[path = "(rebalance|tier)/' crates/ecstore/src/lib.rs || true
    true
  }
) >"$ECSTORE_ROOT_SERVICE_DOMAIN_MODULE_HITS_FILE"

if [[ -s "$ECSTORE_ROOT_SERVICE_DOMAIN_MODULE_HITS_FILE" ]]; then
  report_failure "ECStore rebalance and tier modules must stay under the services owner directory: $(paste -sd '; ' "$ECSTORE_ROOT_SERVICE_DOMAIN_MODULE_HITS_FILE")"
fi

rg -n --with-filename 'crate::(rebalance|tier)\b' \
  "${ROOT_DIR}/crates/ecstore/src" \
  >"$ECSTORE_ROOT_SERVICE_DOMAIN_IMPL_HITS_FILE" || true

if [[ -s "$ECSTORE_ROOT_SERVICE_DOMAIN_IMPL_HITS_FILE" ]]; then
  report_failure "ECStore internal rebalance and tier consumers must use the services owner path: $(paste -sd '; ' "$ECSTORE_ROOT_SERVICE_DOMAIN_IMPL_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  {
    [[ -e crates/ecstore/src/pools.rs ]] && printf '%s\n' 'crates/ecstore/src/pools.rs'
    [[ -e crates/ecstore/src/sets.rs ]] && printf '%s\n' 'crates/ecstore/src/sets.rs'
    [[ -e crates/ecstore/src/pools_test.rs ]] && printf '%s\n' 'crates/ecstore/src/pools_test.rs'
    [[ -e crates/ecstore/src/store_test.rs ]] && printf '%s\n' 'crates/ecstore/src/store_test.rs'
    true
  }
) >"$ECSTORE_ROOT_CORE_RUNTIME_MODULE_HITS_FILE"

if [[ -s "$ECSTORE_ROOT_CORE_RUNTIME_MODULE_HITS_FILE" ]]; then
  report_failure "ECStore core runtime modules must stay under the core owner directory: $(paste -sd '; ' "$ECSTORE_ROOT_CORE_RUNTIME_MODULE_HITS_FILE")"
fi

rg -n --with-filename 'pub (struct|enum|fn) Cluster|pub fn (topology|membership|pool_state|local_node_storage|peer_health)_snapshot' \
  "${ROOT_DIR}/crates/ecstore/src/cluster/mod.rs" \
  >"$ECSTORE_CLUSTER_ROOT_IMPL_HITS_FILE" || true

if [[ -s "$ECSTORE_CLUSTER_ROOT_IMPL_HITS_FILE" ]]; then
  report_failure "ECStore cluster root module must only re-export control-plane owner symbols: $(paste -sd '; ' "$ECSTORE_CLUSTER_ROOT_IMPL_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  {
    [[ -e crates/ecstore/src/rpc/client.rs ]] && printf '%s\n' 'crates/ecstore/src/rpc/client.rs'
    [[ -e crates/ecstore/src/rpc/context_propagation.rs ]] && printf '%s\n' 'crates/ecstore/src/rpc/context_propagation.rs'
    [[ -e crates/ecstore/src/rpc/http_auth.rs ]] && printf '%s\n' 'crates/ecstore/src/rpc/http_auth.rs'
    [[ -e crates/ecstore/src/rpc/internode_data_transport.rs ]] && printf '%s\n' 'crates/ecstore/src/rpc/internode_data_transport.rs'
    [[ -e crates/ecstore/src/rpc/peer_rest_client.rs ]] && printf '%s\n' 'crates/ecstore/src/rpc/peer_rest_client.rs'
    [[ -e crates/ecstore/src/rpc/peer_s3_client.rs ]] && printf '%s\n' 'crates/ecstore/src/rpc/peer_s3_client.rs'
    [[ -e crates/ecstore/src/rpc/remote_disk.rs ]] && printf '%s\n' 'crates/ecstore/src/rpc/remote_disk.rs'
    [[ -e crates/ecstore/src/rpc/remote_locker.rs ]] && printf '%s\n' 'crates/ecstore/src/rpc/remote_locker.rs'
    [[ -e crates/ecstore/src/rpc/runtime_sources.rs ]] && printf '%s\n' 'crates/ecstore/src/rpc/runtime_sources.rs'
    [[ -e crates/ecstore/src/rpc/mod.rs ]] && printf '%s\n' 'crates/ecstore/src/rpc/mod.rs'
    rg -n --with-filename '^\s*mod\s+rpc;' crates/ecstore/src/lib.rs || true
    true
  }
) >"$ECSTORE_ROOT_RPC_SUPPORT_MODULE_HITS_FILE"

if [[ -s "$ECSTORE_ROOT_RPC_SUPPORT_MODULE_HITS_FILE" ]]; then
  report_failure "ECStore RPC modules must stay under the cluster/rpc owner directory: $(paste -sd '; ' "$ECSTORE_ROOT_RPC_SUPPORT_MODULE_HITS_FILE")"
fi

rg -n --with-filename 'crate::rpc\b' \
  "${ROOT_DIR}/crates/ecstore/src" \
  >"$ECSTORE_ROOT_RPC_IMPL_HITS_FILE" || true

if [[ -s "$ECSTORE_ROOT_RPC_IMPL_HITS_FILE" ]]; then
  report_failure "ECStore internal RPC consumers must use the cluster/rpc owner path: $(paste -sd '; ' "$ECSTORE_ROOT_RPC_IMPL_HITS_FILE")"
fi

require_source_contains \
  "crates/ecstore/src/cluster/control_plane.rs" \
  "pub struct ClusterRpcBoundarySnapshot" \
  "ECStore cluster RPC boundary snapshot"
require_source_contains \
  "crates/ecstore/src/cluster/control_plane.rs" \
  "ClusterRpcTransport::Grpc" \
  "ECStore cluster control RPC transport model"
require_source_contains \
  "crates/ecstore/src/cluster/rpc/internode_data_transport.rs" \
  "Internode metadata, lock, health, and administrative calls remain on the" \
  "ECStore internode data transport control-plane separation note"

(
  cd "$ROOT_DIR"
  {
    [[ -e crates/ecstore/src/bucket/metadata.rs ]] && printf '%s\n' 'crates/ecstore/src/bucket/metadata.rs'
    [[ -e crates/ecstore/src/bucket/metadata_sys.rs ]] && printf '%s\n' 'crates/ecstore/src/bucket/metadata_sys.rs'
    [[ -e crates/ecstore/src/bucket/metadata_test.rs ]] && printf '%s\n' 'crates/ecstore/src/bucket/metadata_test.rs'
    [[ -e crates/ecstore/src/set_disk/metadata.rs ]] && printf '%s\n' 'crates/ecstore/src/set_disk/metadata.rs'
    true
  }
) >"$ECSTORE_OLD_METADATA_OWNER_PATH_HITS_FILE"

if [[ -s "$ECSTORE_OLD_METADATA_OWNER_PATH_HITS_FILE" ]]; then
  report_failure "ECStore metadata modules must stay under the metadata owner directory: $(paste -sd '; ' "$ECSTORE_OLD_METADATA_OWNER_PATH_HITS_FILE")"
fi

rg -n --with-filename '#\[path = "(data_movement|layout|core|store|diagnostics|services|io_support|runtime)/' \
  "${ROOT_DIR}/crates/ecstore/src/lib.rs" \
  >"$ECSTORE_ROOT_OWNER_PATH_SHIM_HITS_FILE" || true

if [[ -s "$ECSTORE_ROOT_OWNER_PATH_SHIM_HITS_FILE" ]]; then
  report_failure "ECStore root lib must not restore owner path shims for data_movement/layout/core/store/diagnostics/services/io_support/runtime modules: $(paste -sd '; ' "$ECSTORE_ROOT_OWNER_PATH_SHIM_HITS_FILE")"
fi

cat >"$ECSTORE_COMPAT_PASSTHROUGH_EXPECTED_FILE" <<'EOF'
EOF
sort -o "$ECSTORE_COMPAT_PASSTHROUGH_EXPECTED_FILE" "$ECSTORE_COMPAT_PASSTHROUGH_EXPECTED_FILE"

(
  cd "$ROOT_DIR"
  find rustfs/src crates fuzz/fuzz_targets -type f -name 'storage_compat.rs' -print0 |
    xargs -0 perl -0ne '
      my $file = $ARGV;
      while (/pub(?:\([^)]*\))?\s+use\s+rustfs_ecstore::\{([^}]*)\}\s*;/sg) {
        my $items = $1;
        for my $item (split /,/, $items) {
          $item =~ s/^\s+|\s+$//g;
          next if $item eq "";
          print "$file:$item\n";
        }
      }
      while (/pub(?:\([^)]*\))?\s+use\s+rustfs_ecstore::([A-Za-z_][A-Za-z0-9_]*)\s*;/g) {
        print "$file:$1\n";
      }
    ' | sort
) >"$ECSTORE_COMPAT_PASSTHROUGH_ACTUAL_FILE"

if ! diff -u "$ECSTORE_COMPAT_PASSTHROUGH_EXPECTED_FILE" "$ECSTORE_COMPAT_PASSTHROUGH_ACTUAL_FILE" >"$ECSTORE_COMPAT_PASSTHROUGH_DIFF_FILE"; then
  report_failure "ECStore compatibility passthrough allowlist drifted: $(tr '\n' '; ' <"$ECSTORE_COMPAT_PASSTHROUGH_DIFF_FILE")"
fi

(
  cd "$ROOT_DIR"
  if [[ -f crates/ecstore/src/store_api/traits.rs ]]; then
    rg -n --no-heading 'async fn (get_object_reader|put_object|get_object_info|verify_object_integrity|copy_object|delete_object_version|delete_object|delete_objects|put_object_metadata|get_object_tags|put_object_tags|delete_object_tags|add_partial|transition_object|restore_transitioned_object|list_multipart_uploads|new_multipart_upload|copy_object_part|put_object_part|get_multipart_info|list_object_parts|abort_multipart_upload|complete_multipart_upload|heal_format|heal_bucket|heal_object|get_pool_and_set|check_abandoned_parts|new_ns_lock)\b' \
      crates/ecstore/src/store_api/traits.rs || true
  fi
) >"$STORE_API_OBJECT_OPERATION_LOCAL_METHOD_HITS_FILE"

if [[ -s "$STORE_API_OBJECT_OPERATION_LOCAL_METHOD_HITS_FILE" ]]; then
  report_failure "old ecstore operation method signatures reintroduced: $(paste -sd '; ' "$STORE_API_OBJECT_OPERATION_LOCAL_METHOD_HITS_FILE")"
fi

require_source_contains \
  "crates/ecstore/tests/ecstore_contract_compat_test.rs" \
  "fn ecstore_implements_storage_admin_api_contract()" \
  "ECStore StorageAdminApi compile-time coverage test"
require_source_contains \
  "crates/ecstore/tests/ecstore_contract_compat_test.rs" \
  "fn ecstore_implements_storage_namespace_locking_contract()" \
  "ECStore storage-api NamespaceLocking compile-time coverage test"
require_source_contains \
  "crates/ecstore/tests/ecstore_contract_compat_test.rs" \
  "fn ecstore_implements_storage_object_io_contract()" \
  "ECStore storage-api ObjectIO compile-time coverage test"
require_source_contains \
  "crates/ecstore/tests/ecstore_contract_compat_test.rs" \
  "fn ecstore_implements_storage_object_operations_contract()" \
  "ECStore storage-api ObjectOperations compile-time coverage test"
require_source_contains \
  "crates/ecstore/tests/ecstore_contract_compat_test.rs" \
  "fn ecstore_implements_storage_list_operations_contract()" \
  "ECStore storage-api ListOperations compile-time coverage test"
require_source_contains \
  "crates/ecstore/tests/ecstore_contract_compat_test.rs" \
  "fn ecstore_implements_storage_multipart_operations_contract()" \
  "ECStore storage-api MultipartOperations compile-time coverage test"
require_source_contains \
  "crates/ecstore/tests/ecstore_contract_compat_test.rs" \
  "fn ecstore_implements_storage_heal_operations_contract()" \
  "ECStore storage-api HealOperations compile-time coverage test"
require_source_contains \
  "crates/ecstore/tests/ecstore_contract_compat_test.rs" \
  "fn set_disks_implements_storage_namespace_locking_contract()" \
  "SetDisks storage-api NamespaceLocking compile-time coverage test"
require_source_contains \
  "crates/ecstore/tests/ecstore_contract_compat_test.rs" \
  "fn set_disks_implements_storage_object_io_contract()" \
  "SetDisks storage-api ObjectIO compile-time coverage test"
require_source_contains \
  "crates/ecstore/tests/ecstore_contract_compat_test.rs" \
  "fn set_disks_implements_storage_bucket_operations_contract()" \
  "SetDisks storage-api BucketOperations compile-time coverage test"
require_source_contains \
  "crates/ecstore/tests/ecstore_contract_compat_test.rs" \
  "fn set_disks_implements_storage_object_operations_contract()" \
  "SetDisks storage-api ObjectOperations compile-time coverage test"
require_source_contains \
  "crates/ecstore/tests/ecstore_contract_compat_test.rs" \
  "fn set_disks_implements_storage_list_operations_contract()" \
  "SetDisks storage-api ListOperations compile-time coverage test"
require_source_contains \
  "crates/ecstore/tests/ecstore_contract_compat_test.rs" \
  "fn set_disks_implements_storage_multipart_operations_contract()" \
  "SetDisks storage-api MultipartOperations compile-time coverage test"
require_source_contains \
  "crates/ecstore/tests/ecstore_contract_compat_test.rs" \
  "fn set_disks_implements_storage_heal_operations_contract()" \
  "SetDisks storage-api HealOperations compile-time coverage test"

if (( FAILURES > 0 )); then
  exit 1
fi

echo "Architecture migration rules passed."
