#!/usr/bin/env python3
"""Install only the paired benchmark harness in a clean detached baseline."""

import argparse
from pathlib import Path
import re
import subprocess


def replace_once(source, old, new, path):
    if source.count(old) != 1:
        raise ValueError(f"baseline anchor changed in {path}; inspect it before measuring")
    return source.replace(old, new)


def prepare(root, baseline, revision):
    if baseline.resolve() == root.resolve():
        raise ValueError("baseline must be a separate worktree")
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError("supply the full baseline commit SHA")
    def git(*args):
        return subprocess.check_output(["git", "-C", str(baseline), *args], text=True).strip()
    if git("rev-parse", "HEAD") != revision or git("status", "--porcelain"):
        raise ValueError("baseline must be clean at the requested commit")
    if subprocess.run(["git", "-C", str(baseline), "symbolic-ref", "-q", "HEAD"],
                      stdout=subprocess.DEVNULL).returncode == 0:
        raise ValueError("use a detached baseline worktree")
    edits = {}
    def edit(path, old, new):
        previous = edits.get(path, (baseline / path).read_text())
        edits[path] = replace_once(previous, old, new, path)
    edit("rustfs/src/app/object/mod.rs", "mod test_support;",
         "mod test_support;\n#[cfg(test)]\nmod conditional_read_bench;")
    edit("crates/ecstore/src/set_disk/mod.rs",
         'const ENV_RUSTFS_GET_METADATA_SLOWTAIL_FAULT_DELAY_MS: &str = "RUSTFS_GET_METADATA_SLOWTAIL_FAULT_DELAY_MS";',
         '#[cfg(feature = "test-util")]\nmod conditional_read_bench;\n'
         '#[cfg(feature = "test-util")]\npub use conditional_read_bench::ConditionalReadBenchmarkMetadataGuard;\n\n'
         'const ENV_RUSTFS_GET_METADATA_SLOWTAIL_FAULT_DELAY_MS: &str = "RUSTFS_GET_METADATA_SLOWTAIL_FAULT_DELAY_MS";')
    edit("crates/ecstore/src/set_disk/read.rs",
         "fn get_object_metadata_cache_request_bypass_reason(bucket: &str, opts: &ObjectOptions, read_data: bool) -> Option<&'static str> {",
         "fn get_object_metadata_cache_request_bypass_reason(bucket: &str, opts: &ObjectOptions, read_data: bool) -> Option<&'static str> {\n"
         '    #[cfg(feature = "test-util")]\n    if super::conditional_read_bench::cache_bypass_applies(bucket) {\n'
         '        return Some("benchmark");\n    }')
    edit("crates/ecstore/src/api/mod.rs",
         "pub use crate::bucket::quota::reservation::fail_next_quota_ledger_save_for_test;",
         "pub use crate::bucket::quota::reservation::fail_next_quota_ledger_save_for_test;\n"
         "        pub use crate::set_disk::ConditionalReadBenchmarkMetadataGuard;")
    for path in ("rustfs/src/storage/storage_api.rs", "rustfs/src/app/storage_api.rs"):
        # The facades also expose these types elsewhere; target the test-util block.
        source = (baseline / path).read_text()
        marker = ("pub(crate) use rustfs_ecstore::api::set_disk::test_util::{" if "/storage/" in path else
                  "pub(crate) use crate::storage::storage_api::ecstore_set_disk::{")
        replacement = marker + "\n            ConditionalReadBenchmarkMetadataGuard,"
        edits[path] = replace_once(source, marker, replacement, path)
    for path in ("rustfs/src/app/object/conditional_read_bench.rs",
                 "crates/ecstore/src/set_disk/conditional_read_bench.rs"):
        if (baseline / path).exists():
            raise ValueError(f"baseline already contains {path}")
        edits[path] = (root / path).read_text()
    # Resolve every anchor before writing; never apply a partial adapter.
    for path, source in edits.items():
        (baseline / path).write_text(source)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-worktree", type=Path, required=True)
    parser.add_argument("--revision", required=True)
    args = parser.parse_args()
    prepare(Path(__file__).resolve().parent.parent, args.baseline_worktree, args.revision)


if __name__ == "__main__":
    main()
