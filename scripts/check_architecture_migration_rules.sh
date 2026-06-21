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
RUSTFS_ROOT_BUCKET_STORAGE_COMPAT_MODULE_HITS_FILE="${TMP_DIR}/rustfs_root_bucket_storage_compat_module_hits.txt"
RUSTFS_ROOT_RUNTIME_STORAGE_COMPAT_MODULE_HITS_FILE="${TMP_DIR}/rustfs_root_runtime_storage_compat_module_hits.txt"
RUSTFS_ADMIN_CONFIG_STORAGE_COMPAT_MODULE_HITS_FILE="${TMP_DIR}/rustfs_admin_config_storage_compat_module_hits.txt"
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
    --glob '!target/**' || true
) >"$DIRECT_ECSTORE_IMPORT_HITS_FILE"

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
  rg -n --no-heading 'pub\(crate\)\s+use rustfs_ecstore::api::bucket::\{[^}]*\b(?:metadata|metadata_sys|quota)\b[^}]*\}\s*;|pub\(crate\)\s+use rustfs_ecstore::api::bucket::(?:metadata|metadata_sys|quota)\s*;' \
    rustfs/src/storage_compat.rs || true
) >"$RUSTFS_ROOT_BUCKET_STORAGE_COMPAT_MODULE_HITS_FILE"

if [[ -s "$RUSTFS_ROOT_BUCKET_STORAGE_COMPAT_MODULE_HITS_FILE" ]]; then
  report_failure "RustFS root storage compatibility must expose bucket metadata/quota contracts as explicit aliases: $(paste -sd '; ' "$RUSTFS_ROOT_BUCKET_STORAGE_COMPAT_MODULE_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --no-heading 'pub\(crate\)\s+use rustfs_ecstore::api::config::\{[^}]*\bcom\b[^}]*\}\s*;|pub\(crate\)\s+use rustfs_ecstore::api::config::\{[^}]*\binit\s*(?:,|})[^}]*\}\s*;|pub\(crate\)\s+use rustfs_ecstore::api::disk::\{[^}]*\bendpoint::Endpoint\b[^}]*\}\s*;' \
    rustfs/src/storage_compat.rs || true
) >"$RUSTFS_ROOT_RUNTIME_STORAGE_COMPAT_MODULE_HITS_FILE"

if [[ -s "$RUSTFS_ROOT_RUNTIME_STORAGE_COMPAT_MODULE_HITS_FILE" ]]; then
  report_failure "RustFS root storage compatibility must expose config init and disk endpoint contracts as explicit aliases: $(paste -sd '; ' "$RUSTFS_ROOT_RUNTIME_STORAGE_COMPAT_MODULE_HITS_FILE")"
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
