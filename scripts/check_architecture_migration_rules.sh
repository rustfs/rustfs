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
STORE_API_DELETE_DTO_REEXPORTS_FILE="${TMP_DIR}/store_api_delete_dto_reexports.txt"
STORE_API_DELETE_DTO_INTERNAL_HITS_FILE="${TMP_DIR}/store_api_delete_dto_internal_hits.txt"
STORE_API_LIFECYCLE_HELPER_DEFINITION_HITS_FILE="${TMP_DIR}/store_api_lifecycle_helper_definition_hits.txt"
STORE_API_LIFECYCLE_HELPER_OLD_CONSUMER_HITS_FILE="${TMP_DIR}/store_api_lifecycle_helper_old_consumer_hits.txt"
STORE_API_EXTERNAL_LIST_CONSUMER_HITS_FILE="${TMP_DIR}/store_api_external_list_consumer_hits.txt"
STORE_API_EXTERNAL_OPERATION_CONSUMER_HITS_FILE="${TMP_DIR}/store_api_external_operation_consumer_hits.txt"
STORE_API_OBJECT_OPERATION_LOCAL_METHOD_HITS_FILE="${TMP_DIR}/store_api_object_operation_local_method_hits.txt"
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
ALL_STORAGE_COMPAT_SELF_FACADE_PATH_HITS_FILE="${TMP_DIR}/all_storage_compat_self_facade_path_hits.txt"
RUSTFS_LOCAL_COMPAT_OWNER_SELF_PATH_HITS_FILE="${TMP_DIR}/rustfs_local_compat_owner_self_path_hits.txt"
RUSTFS_ROOT_COMPAT_RELATIVE_CONSUMER_HITS_FILE="${TMP_DIR}/rustfs_root_compat_relative_consumer_hits.txt"
RUSTFS_STORAGE_CORE_COMPAT_RELATIVE_CONSUMER_HITS_FILE="${TMP_DIR}/rustfs_storage_core_compat_relative_consumer_hits.txt"
RUSTFS_LOCAL_COMPAT_RELATIVE_CONSUMER_HITS_FILE="${TMP_DIR}/rustfs_local_compat_relative_consumer_hits.txt"
RUSTFS_APP_ADMIN_SECONDARY_COMPAT_BRIDGE_HITS_FILE="${TMP_DIR}/rustfs_app_admin_secondary_compat_bridge_hits.txt"
RUSTFS_STORAGE_SECONDARY_COMPAT_BRIDGE_HITS_FILE="${TMP_DIR}/rustfs_storage_secondary_compat_bridge_hits.txt"
RUSTFS_NESTED_SECONDARY_COMPAT_BRIDGE_HITS_FILE="${TMP_DIR}/rustfs_nested_secondary_compat_bridge_hits.txt"
RUSTFS_STORAGE_LOCAL_COMPAT_RELATIVE_CONSUMER_HITS_FILE="${TMP_DIR}/rustfs_storage_local_compat_relative_consumer_hits.txt"
RUSTFS_ADMIN_LOCAL_COMPAT_RELATIVE_CONSUMER_HITS_FILE="${TMP_DIR}/rustfs_admin_local_compat_relative_consumer_hits.txt"
RUSTFS_APP_SERVER_LOCAL_COMPAT_RELATIVE_CONSUMER_HITS_FILE="${TMP_DIR}/rustfs_app_server_local_compat_relative_consumer_hits.txt"
RUSTFS_HEAL_TEST_LOCAL_COMPAT_RELATIVE_CONSUMER_HITS_FILE="${TMP_DIR}/rustfs_heal_test_local_compat_relative_consumer_hits.txt"
STANDALONE_CRATE_LOCAL_COMPAT_RELATIVE_CONSUMER_HITS_FILE="${TMP_DIR}/standalone_crate_local_compat_relative_consumer_hits.txt"
SCANNER_BUCKET_STORAGE_COMPAT_MODULE_HITS_FILE="${TMP_DIR}/scanner_bucket_storage_compat_module_hits.txt"
NOTIFY_STORAGE_COMPAT_MODULE_HITS_FILE="${TMP_DIR}/notify_storage_compat_module_hits.txt"
OBS_STORAGE_COMPAT_PASSTHROUGH_HITS_FILE="${TMP_DIR}/obs_storage_compat_passthrough_hits.txt"
E2E_STORAGE_COMPAT_RPC_PASSTHROUGH_HITS_FILE="${TMP_DIR}/e2e_storage_compat_rpc_passthrough_hits.txt"
TEST_STORAGE_COMPAT_PASSTHROUGH_HITS_FILE="${TMP_DIR}/test_storage_compat_passthrough_hits.txt"
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
  "mod disks_layout;" \
  "ECStore legacy disks-layout compatibility module crate-private visibility"
require_source_line \
  "crates/ecstore/src/lib.rs" \
  "mod endpoints;" \
  "ECStore legacy endpoint compatibility module crate-private visibility"
require_source_line \
  "crates/ecstore/src/lib.rs" \
  "mod cluster;" \
  "ECStore cluster control-plane owner module crate-private visibility"
require_source_line \
  "crates/ecstore/src/api/mod.rs" \
  "pub mod cluster {" \
  "ECStore cluster control-plane public facade"
for ecstore_private_module in \
  admin_server_info \
  bucket \
  cache_value \
  client \
  compress \
  config \
  data_usage \
  disk \
  error \
  event_notification \
  global \
  metrics_realtime \
  notification_sys \
  pools \
  rebalance \
  rio \
  rpc \
  set_disk \
  store \
  store_utils \
  tier; do
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
  "WorkloadClass::Metadata => Some(metadata_workload_admission_snapshot())" \
  "RustFS metadata workload admission class mapping"
require_source_contains \
  "rustfs/src/workload_admission.rs" \
  "pub fn scanner_workload_admission_snapshot() -> WorkloadAdmissionSnapshot" \
  "RustFS scanner workload admission snapshot helper"
require_source_contains \
  "rustfs/src/workload_admission.rs" \
  "WorkloadClass::Scanner => Some(scanner_workload_admission_snapshot())" \
  "RustFS scanner workload admission class mapping"
require_source_contains \
  "rustfs/src/workload_admission.rs" \
  "pub fn repair_workload_admission_snapshot() -> WorkloadAdmissionSnapshot" \
  "RustFS repair workload admission snapshot helper"
require_source_contains \
  "rustfs/src/workload_admission.rs" \
  "WorkloadClass::Repair => Some(repair_workload_admission_snapshot())" \
  "RustFS repair workload admission class mapping"
require_source_contains \
  "rustfs/src/workload_admission.rs" \
  "pub fn replication_workload_admission_snapshot() -> WorkloadAdmissionSnapshot" \
  "RustFS replication workload admission snapshot helper"
require_source_contains \
  "rustfs/src/workload_admission.rs" \
  "WorkloadClass::Replication => Some(replication_workload_admission_snapshot())" \
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
  "pub use rustfs_storage_api::ExpirationOptions;" \
  "ECStore ExpirationOptions compatibility re-export"
require_source_line \
  "crates/ecstore/src/bucket/lifecycle/bucket_lifecycle_ops.rs" \
  "pub use rustfs_storage_api::TransitionedObject;" \
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
  rg -n --no-heading 'rustfs_ecstore::store_api(?:::\{[^}]*\b(?:ListObjectVersionsInfo|ListObjectsV2Info|ObjectInfoOrErr)\b|::(?:ListObjectVersionsInfo|ListObjectsV2Info|ObjectInfoOrErr)\b)' \
    rustfs/src crates/iam/src || true
) >"$STORE_API_EXTERNAL_LIST_CONSUMER_HITS_FILE"

if [[ -s "$STORE_API_EXTERNAL_LIST_CONSUMER_HITS_FILE" ]]; then
  report_failure "external list response consumers must use rustfs-storage-api contracts: $(paste -sd '; ' "$STORE_API_EXTERNAL_LIST_CONSUMER_HITS_FILE")"
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
    --glob '!target/**' || true
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
    rg -n --no-heading 'crate::admin::storage_compat' \
      rustfs/src/admin/handlers rustfs/src/admin/service rustfs/src/admin/router.rs \
      --glob '!**/*storage_compat.rs' || true
    rg -n --no-heading 'crate::app::storage_compat' \
      rustfs/src/app \
      --glob '!**/*storage_compat.rs' || true
    rg -n --no-heading 'crate::storage::storage_compat' \
      rustfs/src/storage \
      --glob '!**/*storage_compat.rs' || true
  }
) >"$RUSTFS_OWNER_COMPAT_CONSUMER_HITS_FILE"

if [[ -s "$RUSTFS_OWNER_COMPAT_CONSUMER_HITS_FILE" ]]; then
  report_failure "RustFS owner compatibility consumers must route through their local compatibility boundary: $(paste -sd '; ' "$RUSTFS_OWNER_COMPAT_CONSUMER_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --no-heading 'pub\(crate\)\s+use\s+crate::(?:admin|app|storage)::storage_compat::\*;' \
    rustfs/src/admin/handlers/storage_compat.rs || true
) >"$RUSTFS_LOCAL_COMPAT_GLOB_EXPORT_HITS_FILE"

if [[ -s "$RUSTFS_LOCAL_COMPAT_GLOB_EXPORT_HITS_FILE" ]]; then
  report_failure "Narrowed local compatibility boundaries must use explicit re-exports: $(paste -sd '; ' "$RUSTFS_LOCAL_COMPAT_GLOB_EXPORT_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --no-heading 'pub\(crate\)\s+use rustfs_ecstore::api::config::\{[^}]*\bcom\b[^}]*\}\s*;|pub\(crate\)\s+use rustfs_ecstore::api::config::\{[^}]*\binit\s*(?:,|})[^}]*\}\s*;' \
    rustfs/src/admin/storage_compat.rs || true
) >"$RUSTFS_ADMIN_CONFIG_STORAGE_COMPAT_MODULE_HITS_FILE"

if [[ -s "$RUSTFS_ADMIN_CONFIG_STORAGE_COMPAT_MODULE_HITS_FILE" ]]; then
  report_failure "RustFS admin storage compatibility must expose config IO and default initialization as explicit aliases: $(paste -sd '; ' "$RUSTFS_ADMIN_CONFIG_STORAGE_COMPAT_MODULE_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --no-heading 'pub\(crate\)\s+use rustfs_ecstore::api::bucket::\{[^}]*\b(?:metadata|metadata_sys|object_lock|policy_sys|replication|tagging|utils|versioning|versioning_sys)\b[^}]*\}\s*;|pub\(crate\)\s+use rustfs_ecstore::api::bucket::(?:metadata|metadata_sys|object_lock|policy_sys|replication|tagging|utils|versioning|versioning_sys)\s*;|pub\(crate\)\s+use rustfs_ecstore::api::client::object_api_utils\s*;|pub\(crate\)\s+use rustfs_ecstore::api::config::com\s*;' \
    rustfs/src/storage/storage_compat.rs || true
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
  ' rustfs/src/storage/storage_compat.rs || true
) >"$RUSTFS_STORAGE_OWNER_COMPAT_REEXPORT_HITS_FILE"

if [[ -s "$RUSTFS_STORAGE_OWNER_COMPAT_REEXPORT_HITS_FILE" ]]; then
  report_failure "RustFS storage owner compatibility must use local aliases or wrappers instead of re-exporting ECStore API symbols except temporary traits: $(paste -sd '; ' "$RUSTFS_STORAGE_OWNER_COMPAT_REEXPORT_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --no-heading 'pub\(crate\)\s+use rustfs_ecstore::api::bucket::\{[^}]*\b(?:bandwidth|bucket_target_sys|lifecycle|metadata|metadata_sys|quota|replication|target|utils|versioning|versioning_sys)\b[^}]*\}\s*;|pub\(crate\)\s+use rustfs_ecstore::api::bucket::(?:bandwidth|bucket_target_sys|lifecycle|metadata|metadata_sys|quota|replication|target|utils|versioning|versioning_sys)\s*;|pub\(crate\)\s+use rustfs_ecstore::api::config::\{[^}]*\bstorageclass\b[^}]*\}\s*;|pub\(crate\)\s+use rustfs_ecstore::api::config::storageclass\s*;' \
    rustfs/src/admin/storage_compat.rs || true
) >"$RUSTFS_ADMIN_BUCKET_STORAGE_COMPAT_MODULE_HITS_FILE"

if [[ -s "$RUSTFS_ADMIN_BUCKET_STORAGE_COMPAT_MODULE_HITS_FILE" ]]; then
  report_failure "RustFS admin storage compatibility must expose bucket/storageclass contracts as explicit aliases: $(paste -sd '; ' "$RUSTFS_ADMIN_BUCKET_STORAGE_COMPAT_MODULE_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --no-heading 'pub\(crate\)\s+use rustfs_ecstore::api::bucket::\{[^}]*\b(?:bucket_target_sys|lifecycle|metadata|metadata_sys|object_lock|policy_sys|quota|replication|tagging|target|utils|versioning|versioning_sys)\b[^}]*\}\s*;|pub\(crate\)\s+use rustfs_ecstore::api::bucket::(?:bucket_target_sys|lifecycle|metadata|metadata_sys|object_lock|policy_sys|quota|replication|tagging|target|utils|versioning|versioning_sys)\s*;|pub\(crate\)\s+use rustfs_ecstore::api::client::(?:object_api_utils|transition_api)\s*;|pub\(crate\)\s+use rustfs_ecstore::api::config::storageclass\s*;' \
    rustfs/src/app/storage_compat.rs || true
) >"$RUSTFS_APP_BUCKET_STORAGE_COMPAT_MODULE_HITS_FILE"

if [[ -s "$RUSTFS_APP_BUCKET_STORAGE_COMPAT_MODULE_HITS_FILE" ]]; then
  report_failure "RustFS app storage compatibility must expose bucket/client/storageclass contracts as explicit aliases: $(paste -sd '; ' "$RUSTFS_APP_BUCKET_STORAGE_COMPAT_MODULE_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --no-heading 'rustfs_ecstore::api::(object::(ObjectInfo|ObjectOptions)|error::(Error|Result))' \
    rustfs/src/app/storage_compat.rs \
    rustfs/src/admin/storage_compat.rs \
    rustfs/src/storage/storage_compat.rs || true
) >"$RUSTFS_OUTER_COMPAT_FACADE_ALIAS_HITS_FILE"

if [[ -s "$RUSTFS_OUTER_COMPAT_FACADE_ALIAS_HITS_FILE" ]]; then
  report_failure "RustFS outer storage compatibility must use storage-api object aliases and local StorageError aliases instead of raw ECStore object/error facade aliases: $(paste -sd '; ' "$RUSTFS_OUTER_COMPAT_FACADE_ALIAS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --no-heading '(fn .*rustfs_ecstore::api::(bucket::metadata_sys::BucketMetadataSys|bucket::object_lock::objectlock_sys::ObjectLockBlockReason|bucket::lifecycle::tier_sweeper::Jentry|bucket::bandwidth::monitor::Monitor|notification::NotificationSys)|-> .*rustfs_ecstore::api::(bucket::metadata_sys::BucketMetadataSys|bucket::object_lock::objectlock_sys::ObjectLockBlockReason|bucket::lifecycle::tier_sweeper::Jentry|bucket::bandwidth::monitor::Monitor|notification::NotificationSys)|: &?rustfs_ecstore::api::(bucket::metadata_sys::BucketMetadataSys|bucket::object_lock::objectlock_sys::ObjectLockBlockReason|bucket::lifecycle::tier_sweeper::Jentry|bucket::bandwidth::monitor::Monitor|notification::NotificationSys))' \
    rustfs/src/app/storage_compat.rs \
    rustfs/src/admin/storage_compat.rs \
    rustfs/src/storage/storage_compat.rs || true
) >"$RUSTFS_OUTER_COMPAT_SIGNATURE_ALIAS_HITS_FILE"

if [[ -s "$RUSTFS_OUTER_COMPAT_SIGNATURE_ALIAS_HITS_FILE" ]]; then
  report_failure "RustFS outer storage compatibility signatures must use local aliases for metadata, object-lock, lifecycle, monitor, and notification facade types: $(paste -sd '; ' "$RUSTFS_OUTER_COMPAT_SIGNATURE_ALIAS_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --no-heading 'rustfs_ecstore::api::' rustfs/src/storage/storage_compat.rs \
    | rg -v '^[0-9]+:use rustfs_ecstore::api::[a-z_]+ as ecstore_[a-z_]+;' || true
) >"$RUSTFS_STORAGE_COMPAT_RAW_FACADE_PATH_HITS_FILE"

if [[ -s "$RUSTFS_STORAGE_COMPAT_RAW_FACADE_PATH_HITS_FILE" ]]; then
  report_failure "RustFS storage owner compatibility must use local ecstore_* module aliases instead of scattered raw ECStore facade paths: $(paste -sd '; ' "$RUSTFS_STORAGE_COMPAT_RAW_FACADE_PATH_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --no-heading 'rustfs_ecstore::api::' rustfs/src/app/storage_compat.rs rustfs/src/admin/storage_compat.rs \
    | rg -v '^[^:]+:[0-9]+:use rustfs_ecstore::api::[a-z_]+ as ecstore_[a-z_]+;' || true
) >"$RUSTFS_APP_ADMIN_COMPAT_RAW_FACADE_PATH_HITS_FILE"

if [[ -s "$RUSTFS_APP_ADMIN_COMPAT_RAW_FACADE_PATH_HITS_FILE" ]]; then
  report_failure "RustFS app/admin storage compatibility must use local ecstore_* module aliases instead of scattered raw ECStore facade paths: $(paste -sd '; ' "$RUSTFS_APP_ADMIN_COMPAT_RAW_FACADE_PATH_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --no-heading 'rustfs_ecstore::api::' \
    crates/iam/src/storage_compat.rs \
    crates/heal/src/heal/storage_compat.rs \
    crates/obs/src/storage_compat.rs \
    crates/notify/src/storage_compat.rs \
    crates/protocols/src/swift/storage_compat.rs \
    crates/s3select-api/src/storage_compat.rs \
    crates/scanner/src/storage_compat.rs \
    crates/heal/tests/common/storage_compat.rs \
    crates/scanner/tests/common/storage_compat.rs \
    fuzz/fuzz_targets/path_containment/storage_compat.rs \
    fuzz/fuzz_targets/bucket_validation/storage_compat.rs \
    | rg -v '^[^:]+:[0-9]+:use rustfs_ecstore::api::[a-z_]+ as ecstore_[a-z_]+;' || true
) >"$OUTER_CONSUMER_COMPAT_RAW_FACADE_PATH_HITS_FILE"

if [[ -s "$OUTER_CONSUMER_COMPAT_RAW_FACADE_PATH_HITS_FILE" ]]; then
  report_failure "outer consumer storage compatibility must use local ecstore_* module aliases instead of scattered raw ECStore facade paths: $(paste -sd '; ' "$OUTER_CONSUMER_COMPAT_RAW_FACADE_PATH_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename --no-heading 'rustfs_ecstore::api::' crates/e2e_test/src/storage_compat.rs \
    | rg -v '^[^:]+:[0-9]+:use rustfs_ecstore::api::[a-z_]+ as ecstore_[a-z_]+;' || true
) >"$RUSTFS_ROOT_E2E_COMPAT_RAW_FACADE_PATH_HITS_FILE"

if [[ -s "$RUSTFS_ROOT_E2E_COMPAT_RAW_FACADE_PATH_HITS_FILE" ]]; then
  report_failure "RustFS root/e2e storage compatibility must use local ecstore_* module aliases instead of scattered raw ECStore facade paths: $(paste -sd '; ' "$RUSTFS_ROOT_E2E_COMPAT_RAW_FACADE_PATH_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --with-filename 'rustfs_ecstore::api::' rustfs/src crates fuzz \
    --glob '*storage_compat.rs' \
    --glob '*_compat.rs' \
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
  rg -n --with-filename 'crate::(?:startup_storage_compat|runtime_capabilities_storage_compat|workload_admission_storage_compat|table_catalog_storage_compat|error_storage_compat)' \
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
      rustfs/src/admin/service/storage_compat.rs \
      rustfs/src/app/context/storage_compat.rs \
      rustfs/src/storage/rpc/storage_compat.rs; do
      [[ -e "$file" ]] && printf '%s:1:secondary bridge file exists\n' "$file"
    done
    rg -n --with-filename 'mod storage_compat' \
      rustfs/src/admin/service/mod.rs \
      rustfs/src/app/context.rs \
      rustfs/src/storage/rpc/mod.rs || true
  }
) >"$RUSTFS_NESTED_SECONDARY_COMPAT_BRIDGE_HITS_FILE"

if [[ -s "$RUSTFS_NESTED_SECONDARY_COMPAT_BRIDGE_HITS_FILE" ]]; then
  report_failure "RustFS nested service/context/rpc consumers must route directly through owner storage_compat instead of secondary compatibility bridges: $(paste -sd '; ' "$RUSTFS_NESTED_SECONDARY_COMPAT_BRIDGE_HITS_FILE")"
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
  rg -n --no-heading 'pub\(crate\)\s+use rustfs_ecstore::api::bucket::\{[^}]*\b(?:bucket_target_sys|lifecycle|metadata_sys|replication|versioning|versioning_sys)\b[^}]*\}\s*;' \
    crates/scanner/src/storage_compat.rs || true
) >"$SCANNER_BUCKET_STORAGE_COMPAT_MODULE_HITS_FILE"

if [[ -s "$SCANNER_BUCKET_STORAGE_COMPAT_MODULE_HITS_FILE" ]]; then
  report_failure "scanner storage compatibility must expose bucket contracts as explicit aliases: $(paste -sd '; ' "$SCANNER_BUCKET_STORAGE_COMPAT_MODULE_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --no-heading 'use rustfs_ecstore::api::\{[^}]*\b(?:config|global)\b[^}]*\}\s*;' \
    crates/notify/src/storage_compat.rs || true
) >"$NOTIFY_STORAGE_COMPAT_MODULE_HITS_FILE"

if [[ -s "$NOTIFY_STORAGE_COMPAT_MODULE_HITS_FILE" ]]; then
  report_failure "notify storage compatibility must not import broad config/global modules: $(paste -sd '; ' "$NOTIFY_STORAGE_COMPAT_MODULE_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --no-heading 'pub\(crate\)\s+use rustfs_ecstore::api::data_usage::load_data_usage_from_backend' \
    crates/obs/src/storage_compat.rs || true
) >"$OBS_STORAGE_COMPAT_PASSTHROUGH_HITS_FILE"

if [[ -s "$OBS_STORAGE_COMPAT_PASSTHROUGH_HITS_FILE" ]]; then
  report_failure "OBS storage compatibility must wrap data-usage access instead of re-exporting ECStore functions: $(paste -sd '; ' "$OBS_STORAGE_COMPAT_PASSTHROUGH_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --no-heading 'pub\(crate\)\s+use rustfs_ecstore::api::rpc::\{[^}]*\b(?:gen_tonic_signature_interceptor|node_service_time_out_client|node_service_time_out_client_no_auth)\b[^}]*\}\s*;' \
    crates/e2e_test/src/storage_compat.rs || true
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

if (( FAILURES > 0 )); then
  exit 1
fi

echo "Architecture migration rules passed."
