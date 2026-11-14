#!/usr/bin/env bash
# End-to-end probe for the guards that must survive the mechanical split of
# ecfs.rs, bucket_usecase.rs, multipart_usecase.rs and
# lifecycle_transition_api_test.rs (rustfs/backlog#2765).
#
# It copies the working tree and works out, for each of ecfs.rs,
# bucket_usecase.rs and multipart_usecase.rs, whether that source is still
# unsplit or already split:
#
#   unsplit  the split directory holds no production .rs file; the probe
#            simulates the split (moves real code into the directory) and
#            plants its violations in new split files;
#   split    the split directory holds production .rs files and the source's
#            anchor lives there; the probe plants the same violations at the
#            top of real split files.
#
# A source in neither state (parent file missing, anchor missing, duplicated,
# or on the wrong side) fails the probe with a message naming it. Either way
# the real guard scripts run in the copy: each planted line must be reported
# with its split-file path, and the files holding only real or ordinary code
# must not be reported. The real workspace is never modified.
#
# SPLIT_PROBE_SOURCE_ROOT points the copy at another checkout (read-only), to
# exercise a split branch's tree with that branch's guard scripts.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SOURCE_ROOT="${SPLIT_PROBE_SOURCE_ROOT:-$ROOT_DIR}"
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

# Copy the tracked and untracked-but-not-ignored files of the source checkout.
(cd "$SOURCE_ROOT" && git ls-files -z --cached --others --exclude-standard) |
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
' "$SOURCE_ROOT" "$FX"

# Decide each source's state, move real code where the tree is unsplit, and
# plant one forbidden line per rule. Prints shell assignments for the checks.
PLAN="$FX/.split-probe-plan.sh"
"$PYTHON" - "$FX" >"$PLAN" <<'PY'
import shlex
import sys
from pathlib import Path

fx = Path(sys.argv[1])
APP = "rustfs/src/app"
STORAGE = "rustfs/src/storage"


def die(message):
    print(f"split probe: {message}", file=sys.stderr)
    sys.exit(1)


def is_test_only(rel):
    parts = rel.split("/")
    stem = parts[-1][:-3] if parts[-1].endswith(".rs") else parts[-1]
    return ("tests" in parts[:-1] or stem in ("tests", "test_support")
            or stem.endswith("_test") or stem.endswith("_tests"))


def production_files(split_dir):
    root = fx / split_dir
    if not root.is_dir():
        return []
    return sorted(p.relative_to(fx).as_posix() for p in root.rglob("*.rs")
                  if not is_test_only(p.relative_to(fx).as_posix()))


def defining(rel_paths, marker):
    return [rel for rel in rel_paths if marker in (fx / rel).read_text()]


def state(name, parent, split_dir, anchor):
    """'unsplit' or 'split'; anything else stops the probe naming the source."""
    if not (fx / parent).is_file():
        die(f"{name}: {parent} is missing, so the tree is in neither the unsplit nor the split state")
    split_files = production_files(split_dir)
    total = (fx / parent).read_text().count(anchor) + sum(
        (fx / rel).read_text().count(anchor) for rel in split_files)
    if total != 1:
        die(f"{name}: anchor {anchor!r} occurs {total} times across {parent} and {split_dir}/, expected once")
    in_parent = anchor in (fx / parent).read_text()
    if not split_files:
        if not in_parent:
            die(f"{name}: {split_dir}/ has no production file but {anchor!r} is not in {parent}")
        return "unsplit", split_files
    return "split", split_files


def prepend(rel, block):
    path = fx / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    old = path.read_text() if path.exists() else ""
    path.write_text(block + old)


def pick(split_files, avoid, fallback):
    for rel in split_files:
        if rel not in avoid and not rel.endswith("/mod.rs"):
            return rel
    return fallback


def imports(rel):
    return "".join(l + "\n" for l in (fx / rel).read_text().splitlines() if l.startswith("use crate::"))


LISTING_ANCHOR = "    pub async fn execute_list_objects_v2("
ecfs_state, ecfs_files = state("ecfs", f"{STORAGE}/ecfs.rs", f"{STORAGE}/legacy_edge", "impl S3 for FS {")
bu_state, bu_files = state("bucket usecase", f"{APP}/bucket_usecase.rs", f"{APP}/bucket_usecase", LISTING_ANCHOR)
mu_state, mu_files = state("multipart usecase", f"{APP}/multipart_usecase.rs", f"{APP}/multipart_usecase",
                           "pub async fn execute_complete_multipart_upload(")

# Bucket usecase: the TRACE hot span either moves into listing.rs (unsplit) or
# already lives in a real split file.
if bu_state == "unsplit":
    parent = fx / APP / "bucket_usecase.rs"
    text = parent.read_text()
    idx = text.index(LISTING_ANCHOR)
    start = text.rindex("\n", 0, idx) + 1
    while True:
        prev = text.rindex("\n", 0, start - 1) + 1
        if text[prev:start].lstrip().startswith(("#[", "///")):
            start = prev
        else:
            break
    end = text.index("\n    }\n", idx) + len("\n    }\n")
    listing = f"{APP}/bucket_usecase/listing.rs"
    prepend(listing, "impl DefaultBucketUsecase {\n" + text[start:end] + "}\n")
    parent.write_text(text[:start] + text[end:])
else:
    owners = defining(bu_files, LISTING_ANCHOR)
    if len(owners) != 1:
        die(f"bucket usecase: {APP}/bucket_usecase/ is split but {LISTING_ANCHOR.strip()!r} is not in one of its files")
    listing = owners[0]

edge_imports = f"{STORAGE}/legacy_edge/split_probe_edge_imports.rs"
prepend(edge_imports, imports(f"{STORAGE}/ecfs.rs"))
lt_imports = f"{APP}/lifecycle_transition_api_test/split_probe_imports.rs"
prepend(lt_imports, imports(f"{APP}/lifecycle_transition_api_test.rs"))

if ecfs_state == "split":
    e_file = pick(ecfs_files, set(), f"{STORAGE}/legacy_edge/bucket_configs.rs")
    test_files = sorted(p.relative_to(fx).as_posix() for p in (fx / STORAGE / "legacy_edge/tests").rglob("*.rs")) \
        if (fx / STORAGE / "legacy_edge/tests").is_dir() else []
    t_file = test_files[0] if test_files else f"{STORAGE}/legacy_edge/tests/object_meta.rs"
else:
    e_file = f"{STORAGE}/legacy_edge/bucket_configs.rs"
    t_file = f"{STORAGE}/legacy_edge/tests/object_meta.rs"
b_file = pick(bu_files, {listing}, f"{APP}/bucket_usecase/cors.rs") if bu_state == "split" \
    else f"{APP}/bucket_usecase/cors.rs"
m_file = pick(mu_files, set(), f"{APP}/multipart_usecase/complete.rs") if mu_state == "split" \
    else f"{APP}/multipart_usecase/complete.rs"
l_file = f"{APP}/lifecycle_transition_api_test/split_probe.rs"

PLANTS = {
    e_file: ("use crate::app::context::resolve_region;\n"
             "use crate::app::bucket_usecase::DefaultBucketUsecase;\n"
             "use crate::storage::core_storage_compat::Probe;\n"
             "use crate::startup_server::LayerProbe;\n"),
    b_file: ("use crate::storage::*;\n"
             "use crate::storage::access::ReqInfo;\n"
             "use super::super::StorageError;\n"
             "use super::super::ECStore;\n"
             "fn forbidden() {\n"
             "    super::super::get_server_info();\n"
             "}\n"),
    m_file: ("use crate::storage::s3_api::multipart;\n"
             "use super::super::{quota, versioning_sys};\n"
             "use crate::app::usecase_storage_compat::Probe;\n"),
    l_file: ("use crate::storage::request_context::RequestContext;\n"
             "fn forbidden() {\n"
             "    super::super::tagging::probe();\n"
             "}\n"),
    t_file: ("use super::super::super::{BucketMetadata};\n"
             "mod tests {\n"
             "    use super::super::super::super::{check_preconditions};\n"
             "}\n"
             "use crate::storage::core_storage_compat::TestProbe;\n"),
}
if len(PLANTS) != 5:
    die("two planted violation sets would share one file")

ORDINARY = ("use super::{DefaultBucketUsecase, ObjectInfo, ObjectOptions};\n"
            "use super::ECStore;\n"
            "use super::StorageError;\n"
            "fn ordinary() {\n"
            "    super::tagging::helper();\n"
            "    super::get_server_info();\n"
            "}\n")
ordinary = [f"{APP}/bucket_usecase/split_probe_ordinary.rs", f"{APP}/multipart_usecase/split_probe_ordinary.rs",
            f"{APP}/lifecycle_transition_api_test/split_probe_ordinary.rs"]
for rel in ordinary:
    if (fx / rel).exists():
        die(f"{rel} already exists; the probe will not overwrite a real file")
    prepend(rel, ORDINARY)
edge_ordinary = f"{STORAGE}/legacy_edge/tests/split_probe_ordinary.rs"
if (fx / edge_ordinary).exists():
    die(f"{edge_ordinary} already exists; the probe will not overwrite a real file")
prepend(edge_ordinary, "mod tests {\n    use super::{BucketMetadata, check_preconditions};\n}\n")

for rel, block in PLANTS.items():
    prepend(rel, block)
    if not (fx / rel).read_text().startswith(block):
        die(f"planting into {rel} did not take effect")

# Real split files that carry no planted line hold moved production code only:
# no rule may report them.
real = [rel for rel in ecfs_files + bu_files + mu_files if rel not in PLANTS and rel != listing]

print(f"ECFS_STATE={ecfs_state} BU_STATE={bu_state} MU_STATE={mu_state}")
for var, value in [("E", e_file), ("B", b_file), ("M", m_file), ("L", l_file), ("T", t_file),
                   ("LISTING", listing), ("EDGE_IMPORTS", edge_imports), ("LT_IMPORTS", lt_imports)]:
    print(f"{var}={shlex.quote(value)}")
print("SILENT_FILES=(" + " ".join(shlex.quote(p) for p in ordinary + [edge_ordinary, listing, edge_imports, lt_imports]) + ")")
print("REAL_SPLIT_FILES=(" + " ".join(shlex.quote(p) for p in real) + ")")
PY
# shellcheck source=/dev/null
source "$PLAN"
echo "split probe: ecfs $ECFS_STATE, bucket usecase $BU_STATE, multipart usecase $MU_STATE"

# --- architecture rules ------------------------------------------------------
ARCH_OUT="$FX/.arch.out"
if (cd "$FX" && ./scripts/check_architecture_migration_rules.sh) >"$ARCH_OUT" 2>&1; then
  fail "architecture rules passed over planted violations"
fi
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
for silent in "${SILENT_FILES[@]}"; do
  expect_silent "$ARCH_OUT" "$silent:" "architecture rules over moved or ordinary code"
done
for silent in ${REAL_SPLIT_FILES[@]+"${REAL_SPLIT_FILES[@]}"}; do
  expect_silent "$ARCH_OUT" "$silent:" "architecture rules over real split code"
done

# --- layer dependencies ------------------------------------------------------
LAYER_OUT="$FX/.layer.out"
if (cd "$FX" && ./scripts/check_layer_dependencies.sh) >"$LAYER_OUT" 2>&1; then
  fail "layer guard passed over a planted reverse dependency"
fi
expect_reported "$LAYER_OUT" "dep|$E|interface->composition|" "crate::startup_server::LayerProbe" "legacy_edge keeps the interface layer"
expect_silent "$LAYER_OUT" "dep|$EDGE_IMPORTS|" "moved ecfs imports stay legal"
expect_silent "$LAYER_OUT" "dep|rustfs/src/app/lifecycle_transition_api_test/" "moved lifecycle transition tests stay excluded"
expect_silent "$LAYER_OUT" "dep|$LISTING|" "moved bucket usecase code keeps the app layer"

# --- logging hot span --------------------------------------------------------
LOG_OUT="$FX/.logging.out"
checked=$((checked + 1))
if ! (cd "$FX" && ./scripts/check_logging_guardrails.sh) >"$LOG_OUT" 2>&1; then
  fail "logging guardrails failed with the TRACE hot span in $LISTING: $(tail -3 "$LOG_OUT")"
fi
"$PYTHON" - "$FX/$LISTING" <<'PY'
import sys
from pathlib import Path
path = Path(sys.argv[1])
text = path.read_text()
markers = [
    f'#[instrument(level = "trace", skip({arguments}))]\n    pub async fn execute_list_objects_v2('
    for arguments in ("self, req", "self, envelope, input")
]
if sum(text.count(marker) for marker in markers) != 1:
    sys.exit(f"split probe: {path} must carry the TRACE attribute directly on execute_list_objects_v2")
marker = next(marker for marker in markers if marker in text)
path.write_text(text.replace(marker, marker.replace('"trace"', '"debug"')))
PY
checked=$((checked + 1))
if (cd "$FX" && ./scripts/check_logging_guardrails.sh) >"$LOG_OUT" 2>&1; then
  fail "logging guardrails passed after the hot span in $LISTING lost TRACE"
elif ! grep -qF "execute_list_objects_v2 must remain TRACE-only" "$LOG_OUT"; then
  fail "logging guardrails failed for another reason: $(tail -3 "$LOG_OUT")"
fi

if [[ $failures -gt 0 ]]; then
  echo "split guard rules probe: $failures of $checked checks failed" >&2
  exit 1
fi
echo "split guard rules probe: $checked checks passed (ecfs $ECFS_STATE, bucket usecase $BU_STATE, multipart usecase $MU_STATE)"
