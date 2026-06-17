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
  "crates/storage-api/src/lib.rs" \
  "pub use bucket::{BucketInfo, BucketOperations, BucketOptions, DeleteBucketOptions, MakeBucketOptions, SRBucketDeleteOp};" \
  "storage-api public bucket contract re-export"
require_source_line \
  "crates/storage-api/src/lib.rs" \
  "pub use multipart::{CompletePart, ListMultipartsInfo, ListPartsInfo, MultipartInfo, MultipartUploadResult, PartInfo};" \
  "storage-api public multipart DTO re-export"
require_source_line \
  "crates/storage-api/src/lib.rs" \
  "pub use object::{HTTPPreconditions, HTTPRangeError, HTTPRangeSpec, ObjectLockRetentionOptions};" \
  "storage-api public object helper contract re-export"
require_source_line \
  "crates/storage-api/src/lib.rs" \
  "pub use error::{StorageErrorCode, StorageResult};" \
  "storage-api public error contract re-export"

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
    crates/ecstore/src/store_api.rs || true
) >"$STORE_API_BUCKET_DTO_REEXPORTS_FILE"

if [[ -s "$STORE_API_BUCKET_DTO_REEXPORTS_FILE" ]]; then
  report_failure "old ecstore store_api bucket DTO re-export reintroduced: $(paste -sd '; ' "$STORE_API_BUCKET_DTO_REEXPORTS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --no-heading 'pub(?:\(crate\))? use rustfs_storage_api::\{[^}]*\bBucketOperations\b|pub trait BucketOperations\b' \
    crates/ecstore/src/store_api.rs crates/ecstore/src/store_api/traits.rs || true
) >"$STORE_API_BUCKET_OPERATION_HITS_FILE"

if [[ -s "$STORE_API_BUCKET_OPERATION_HITS_FILE" ]]; then
  report_failure "old ecstore store_api BucketOperations path reintroduced: $(paste -sd '; ' "$STORE_API_BUCKET_OPERATION_HITS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --no-heading 'pub(?:\(crate\))? use rustfs_storage_api::\{[^}]*\b(?:CompletePart|ListMultipartsInfo|ListPartsInfo|MultipartInfo|MultipartUploadResult|PartInfo)\b|pub struct (?:CompletePart|ListMultipartsInfo|ListPartsInfo|MultipartInfo|MultipartUploadResult|PartInfo)\b' \
    crates/ecstore/src/store_api.rs crates/ecstore/src/store_api/types.rs || true
) >"$STORE_API_MULTIPART_DTO_REEXPORTS_FILE"

if [[ -s "$STORE_API_MULTIPART_DTO_REEXPORTS_FILE" ]]; then
  report_failure "old ecstore store_api multipart DTO path reintroduced: $(paste -sd '; ' "$STORE_API_MULTIPART_DTO_REEXPORTS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --no-heading 'pub(?:\(crate\))? use rustfs_storage_api(?:::\{[^}]*\b(?:HTTPPreconditions|ObjectLockRetentionOptions)\b|::(?:HTTPPreconditions|ObjectLockRetentionOptions)\b)|pub struct (?:HTTPPreconditions|ObjectLockRetentionOptions)\b' \
    crates/ecstore/src/store_api.rs crates/ecstore/src/store_api/types.rs || true
) >"$STORE_API_OBJECT_HELPER_REEXPORTS_FILE"

if [[ -s "$STORE_API_OBJECT_HELPER_REEXPORTS_FILE" ]]; then
  report_failure "old ecstore store_api object helper path reintroduced: $(paste -sd '; ' "$STORE_API_OBJECT_HELPER_REEXPORTS_FILE")"
fi

(
  cd "$ROOT_DIR"
  rg -n --no-heading 'pub(?:\(crate\))? use rustfs_storage_api(?:::\{[^}]*\b(?:HTTPRangeError|HTTPRangeSpec)\b|::(?:HTTPRangeError|HTTPRangeSpec)\b)|pub (?:enum HTTPRangeError|struct HTTPRangeSpec)\b' \
    crates/ecstore/src/store_api.rs crates/ecstore/src/store_api/types.rs crates/ecstore/src/store_api/readers.rs || true
) >"$STORE_API_RANGE_HELPER_REEXPORTS_FILE"

if [[ -s "$STORE_API_RANGE_HELPER_REEXPORTS_FILE" ]]; then
  report_failure "old ecstore store_api range helper path reintroduced: $(paste -sd '; ' "$STORE_API_RANGE_HELPER_REEXPORTS_FILE")"
fi

require_source_contains \
  "crates/ecstore/src/store_api/traits.rs" \
  "pub trait NamespaceLocking: Send + Sync + Debug + 'static" \
  "separate namespace-locking operation-group trait"
require_source_contains \
  "crates/ecstore/tests/ecstore_contract_compat_test.rs" \
  "fn ecstore_implements_storage_admin_api_contract()" \
  "ECStore StorageAdminApi compile-time coverage test"
require_source_contains \
  "crates/ecstore/tests/ecstore_contract_compat_test.rs" \
  "fn ecstore_implements_namespace_locking_contract()" \
  "ECStore NamespaceLocking compile-time coverage test"

if (( FAILURES > 0 )); then
  exit 1
fi

echo "Architecture migration rules passed."
