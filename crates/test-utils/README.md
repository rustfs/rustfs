# rustfs-test-utils

Shared bootstrap helpers for RustFS integration tests (backlog#1153 infra-1).
Owns the "build a real temp-disk `ECStore`" setup that used to be copy-pasted
across the heal/iam/scanner integration tests and had already drifted.

## Usage

```rust
// [dev-dependencies] rustfs-test-utils = { workspace = true }
use rustfs_test_utils::TestECStoreEnv;

let env = TestECStoreEnv::builder()
    .prefix("rustfs_myfeature_test") // /tmp/<prefix>_<uuid>
    .disk_count(4)                   // single pool, single set
    .build()
    .await;

env.make_bucket("my-bucket", /* versioned = */ true).await;
// env.ecstore : Arc<ECStore>       — bootstrapped store
// env.disk_paths : Vec<PathBuf>    — disk1..diskN for fault injection
// env.temp_root : PathBuf          — root dir (leaked by design, see below)
```

Builder knobs:

- `disk_count(n)` — disks in the single erasure set (default 4).
- `prefix(&str)` — temp-dir prefix; a uuid suffix keeps parallel nextest
  processes isolated.
- `base_dir(path)` — use a caller-owned directory (e.g. `tempfile::TempDir`)
  instead; the caller keeps cleanup ownership.
- `init_bucket_metadata(bool)` — run `init_bucket_metadata_sys` after boot
  (default `true`; the IAM bootstrap test opts out).

`init_tracing()` is exposed separately and is called by `build()`.

The environment does **not** delete `temp_root` on drop: a failed test's
on-disk state stays inspectable, and heal tests keep manipulating
`disk_paths` after setup. Own the directory via `base_dir` if you want
automatic cleanup.

## Constraints

- **dev-dependency only.** No crate may list `rustfs-test-utils` under
  `[dependencies]` — test scaffolding must never reach production binaries.
- **Single-process integration scope.** Multi-node / chaos harnesses are out
  of scope (backlog#1100). Scanner's lifecycle tests are absorbed only after
  ilm-1 activates them (they are `#[ignore]`d today).
- All `rustfs_ecstore` imports stay behind `src/ecstore_test_compat.rs` — the
  sanctioned test-compat boundary pattern enforced by
  `scripts/check_architecture_migration_rules.sh`. Extend that module, never
  import the facade directly from other files.
- Tests using this env bind `127.0.0.1:0` (random port) and unique temp dirs,
  so they stay parallel-safe under nextest's process-per-test model — do not
  add fixed ports or shared paths here. See `docs/testing/README.md` for the
  serial/nextest rules.
