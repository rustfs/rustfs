#!/usr/bin/env bash
# End-to-end probe for the guards that must survive the mechanical split of
# ecfs.rs, bucket_usecase.rs, multipart_usecase.rs and
# lifecycle_transition_api_test.rs (rustfs/backlog#2765).
#
# It copies the working tree, simulates the split (moves real code into
# storage/legacy_edge/, app/bucket_usecase/, app/multipart_usecase/ and
# app/lifecycle_transition_api_test/), plants one forbidden line per rule in a
# split file, and runs the real guard scripts there. Each planted line must be
# reported with its split-file path, and nothing in the files that hold only
# moved real code or ordinary depth-relative imports may be reported: that is
# the "moved and broken -> detected" and "moved -> still passes" proof in one
# run per guard. The real workspace is never modified.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON="${RUSTFS_PYTHON_BIN:-python3}"
FX="$(mktemp -d)"
trap 'rm -rf "$FX"' EXIT

failures=0
checked=0

fail() {
  echo "FAIL: $1" >&2
  failures=$((failures + 1))
}

# expect_reported <output-file> <path> <text> <description>
expect_reported() {
  checked=$((checked + 1))
  if ! grep -F -- "$2" "$1" | grep -qF -- "$3"; then
    fail "$4: no report naming $2 with: $3"
  fi
}

# expect_silent <output-file> <path> <description>
expect_silent() {
  checked=$((checked + 1))
  if grep -qF -- "$2" "$1"; then
    fail "$3: $2 was reported: $(grep -F -- "$2" "$1" | head -3)"
  fi
}

# Copy the tracked and untracked-but-not-ignored files of the working tree.
(cd "$ROOT_DIR" && git ls-files -z --cached --others --exclude-standard) |
  "$PYTHON" -c '
import os, shutil, sys
root, dest = sys.argv[1], sys.argv[2]
for rel in sys.stdin.buffer.read().decode().split("\0"):
    if not rel:
        continue
    src = os.path.join(root, rel)
    if not os.path.lexists(src):
        continue
    out = os.path.join(dest, rel)
    os.makedirs(os.path.dirname(out), exist_ok=True)
    shutil.copy2(src, out, follow_symlinks=False)
' "$ROOT_DIR" "$FX"

APP="$FX/rustfs/src/app"
STORAGE="$FX/rustfs/src/storage"
mkdir -p "$APP/bucket_usecase" "$APP/multipart_usecase" "$APP/lifecycle_transition_api_test" \
  "$STORAGE/legacy_edge/tests"

# Real code moves: the TRACE hot span leaves bucket_usecase.rs with its
# attributes, and the real import blocks of ecfs.rs and the lifecycle
# transition tests are copied into split files.
"$PYTHON" - "$FX" <<'PY'
import sys
from pathlib import Path
fx = Path(sys.argv[1])
bu = fx / "rustfs/src/app/bucket_usecase.rs"
text = bu.read_text()
marker = "    pub async fn execute_list_objects_v2("
assert text.count(marker) == 1, "execute_list_objects_v2 must be defined once"
idx = text.index(marker)
start = text.rindex("\n", 0, idx) + 1
while True:
    prev = text.rindex("\n", 0, start - 1) + 1
    if text[prev:start].lstrip().startswith(("#[", "///")):
        start = prev
    else:
        break
end = text.index("\n    }\n", idx) + len("\n    }\n")
(fx / "rustfs/src/app/bucket_usecase/listing.rs").write_text(
    "impl DefaultBucketUsecase {\n" + text[start:end] + "}\n")
bu.write_text(text[:start] + text[end:])

def imports(path):
    return "".join(l + "\n" for l in path.read_text().splitlines() if l.startswith("use crate::"))

(fx / "rustfs/src/storage/legacy_edge/edge_imports.rs").write_text(
    imports(fx / "rustfs/src/storage/ecfs.rs"))
(fx / "rustfs/src/app/lifecycle_transition_api_test/imports.rs").write_text(
    imports(fx / "rustfs/src/app/lifecycle_transition_api_test.rs"))
PY

# Ordinary depth-relative uses of the parent module in split files: none of
# them reaches crate::app or the storage root, so no rule may report them.
cat >"$APP/bucket_usecase/ordinary.rs" <<'EOF'
use super::{DefaultBucketUsecase, ObjectInfo, ObjectOptions};
use super::ECStore;
use super::StorageError;
fn ordinary() {
    super::tagging::helper();
    super::get_server_info();
}
EOF
cp "$APP/bucket_usecase/ordinary.rs" "$APP/multipart_usecase/ordinary.rs"
cp "$APP/bucket_usecase/ordinary.rs" "$APP/lifecycle_transition_api_test/ordinary.rs"
cat >"$STORAGE/legacy_edge/tests/ordinary.rs" <<'EOF'
mod tests {
    use super::{BucketMetadata, check_preconditions};
}
EOF

# One forbidden line per rule, each in a split file.
cat >"$STORAGE/legacy_edge/bucket_configs.rs" <<'EOF'
use crate::app::context::resolve_region;
use crate::app::bucket_usecase::DefaultBucketUsecase;
use crate::storage::core_storage_compat::Probe;
use crate::startup_server::LayerProbe;
EOF
cat >"$APP/bucket_usecase/cors.rs" <<'EOF'
use crate::storage::*;
use crate::storage::access::ReqInfo;
use super::super::StorageError;
use super::super::ECStore;
fn forbidden() {
    super::super::get_server_info();
}
EOF
cat >"$APP/multipart_usecase/complete.rs" <<'EOF'
use crate::storage::s3_api::multipart;
use super::super::{quota, versioning_sys};
use crate::app::usecase_storage_compat::Probe;
EOF
cat >"$APP/lifecycle_transition_api_test/put.rs" <<'EOF'
use crate::storage::request_context::RequestContext;
fn forbidden() {
    super::super::tagging::probe();
}
EOF
cat >"$STORAGE/legacy_edge/tests/object_meta.rs" <<'EOF'
use super::super::super::{BucketMetadata};
mod tests {
    use super::super::super::super::{check_preconditions};
}
use crate::storage::core_storage_compat::TestProbe;
EOF

# --- architecture rules ------------------------------------------------------
ARCH_OUT="$FX/.arch.out"
if (cd "$FX" && ./scripts/check_architecture_migration_rules.sh) >"$ARCH_OUT" 2>&1; then
  fail "architecture rules passed over planted violations"
fi
E=rustfs/src/storage/legacy_edge/bucket_configs.rs
B=rustfs/src/app/bucket_usecase/cors.rs
M=rustfs/src/app/multipart_usecase/complete.rs
L=rustfs/src/app/lifecycle_transition_api_test/put.rs
T=rustfs/src/storage/legacy_edge/tests/object_meta.rs
expect_reported "$ARCH_OUT" "$E:1:" "storage runtime source reads must stay behind" "runtime-source bypass (ECFS set)"
expect_reported "$ARCH_OUT" "$E:2:" "ECFS S3 routes must construct app usecases" "ECFS usecase bypass"
expect_reported "$ARCH_OUT" "$E:3:" "must use relative core_storage_compat paths" "core_storage_compat (ECFS set)"
expect_reported "$ARCH_OUT" "$B:1:" "instead of crate::storage::*" "storage wildcard (bucket split)"
expect_reported "$ARCH_OUT" "$B:2:" "consume storage helper APIs through" "storage helper import (bucket split)"
expect_reported "$ARCH_OUT" "$B:3:" "consume storage helper APIs through" "depth-aware super::super storage helper"
expect_reported "$ARCH_OUT" "$B:4:" "runtime and data-usage facades must stay behind" "depth-aware super::super runtime facade import"
expect_reported "$ARCH_OUT" "$B:6:" "runtime and data-usage facades must stay behind" "depth-aware super::super runtime facade path"
expect_reported "$ARCH_OUT" "$M:1:" "consume S3 API helpers through" "s3_api bypass (multipart split)"
expect_reported "$ARCH_OUT" "$M:2:" "consume storage helper APIs through" "depth-aware super::super owner import"
expect_reported "$ARCH_OUT" "$M:3:" "local compatibility consumers must use relative" "usecase_storage_compat (multipart split)"
expect_reported "$ARCH_OUT" "$L:1:" "completed storage helper consumers" "test storage helper (lifecycle split)"
expect_reported "$ARCH_OUT" "$L:3:" "consume storage helper APIs through" "depth-aware super::super owner path"
expect_reported "$ARCH_OUT" "$T:1:" "storage-owner tests must use storage_api test consumer" "ecfs_test import at file depth"
expect_reported "$ARCH_OUT" "$T:3:" "storage-owner tests must use storage_api test consumer" "ecfs_test import in an inline tests module"
expect_reported "$ARCH_OUT" "$T:5:" "local compatibility consumers must use relative" "ecfs_test core_storage_compat"
for silent in rustfs/src/app/bucket_usecase/ordinary.rs rustfs/src/app/multipart_usecase/ordinary.rs \
  rustfs/src/app/lifecycle_transition_api_test/ordinary.rs rustfs/src/storage/legacy_edge/tests/ordinary.rs \
  rustfs/src/app/bucket_usecase/listing.rs rustfs/src/storage/legacy_edge/edge_imports.rs \
  rustfs/src/app/lifecycle_transition_api_test/imports.rs; do
  expect_silent "$ARCH_OUT" "$silent" "architecture rules over moved or ordinary code"
done

# --- layer dependencies ------------------------------------------------------
LAYER_OUT="$FX/.layer.out"
if (cd "$FX" && ./scripts/check_layer_dependencies.sh) >"$LAYER_OUT" 2>&1; then
  fail "layer guard passed over a planted reverse dependency"
fi
expect_reported "$LAYER_OUT" "dep|$E|interface->composition|" "crate::startup_server::LayerProbe" "legacy_edge keeps the interface layer"
expect_silent "$LAYER_OUT" "dep|rustfs/src/storage/legacy_edge/edge_imports.rs|" "moved ecfs imports stay legal"
expect_silent "$LAYER_OUT" "dep|rustfs/src/app/lifecycle_transition_api_test/" "moved lifecycle transition tests stay excluded"
expect_silent "$LAYER_OUT" "dep|rustfs/src/app/bucket_usecase/listing.rs|" "moved bucket usecase code keeps the app layer"

# --- logging hot span --------------------------------------------------------
LOG_OUT="$FX/.logging.out"
checked=$((checked + 1))
if ! (cd "$FX" && ./scripts/check_logging_guardrails.sh) >"$LOG_OUT" 2>&1; then
  fail "logging guardrails failed after a clean move of the TRACE hot span: $(tail -3 "$LOG_OUT")"
fi
"$PYTHON" - "$APP/bucket_usecase/listing.rs" <<'PY'
import sys
from pathlib import Path
path = Path(sys.argv[1])
text = path.read_text()
marker = '#[instrument(level = "trace"'
assert text.count(marker) == 1, "the moved hot span must carry one TRACE attribute"
path.write_text(text.replace(marker, '#[instrument(level = "debug"'))
PY
checked=$((checked + 1))
if (cd "$FX" && ./scripts/check_logging_guardrails.sh) >"$LOG_OUT" 2>&1; then
  fail "logging guardrails passed after the moved hot span lost TRACE"
elif ! grep -qF "execute_list_objects_v2 must remain TRACE-only" "$LOG_OUT"; then
  fail "logging guardrails failed for another reason: $(tail -3 "$LOG_OUT")"
fi

if [[ $failures -gt 0 ]]; then
  echo "split guard rules probe: $failures of $checked checks failed" >&2
  exit 1
fi
echo "split guard rules probe: $checked checks passed"
