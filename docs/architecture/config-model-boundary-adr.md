# Config Model Boundary ADR

Related issue: [`rustfs/backlog#660`](https://github.com/rustfs/backlog/issues/660)

Task: `CFG-002`

## Decision

Use the existing `crates/config` package (`rustfs-config`) as the target owner
for the pure server-config model. Do not create a new config-model crate for
the first extraction.

The next model extraction PR should introduce the model under:

```text
crates/config/src/server_config.rs
```

The exported path should be:

```rust
rustfs_config::server_config::{Config, KV, KVS}
```

The existing path must remain available through a temporary compatibility
re-export:

```rust
rustfs_ecstore::config::{Config, KV, KVS}
```

That re-export must include `RUSTFS_COMPAT_TODO(CFG-004)` and a matching entry in
[`compat-cleanup-register.md`](compat-cleanup-register.md).

## Why `rustfs-config`

`rustfs-config` is already the lowest RustFS crate for configuration constants
and subsystem identifiers used by ECStore, notify, audit, targets, scanner, IAM,
and admin code. The current `ecstore::config::{Config, KV, KVS}` model already
uses `rustfs-config` constants, so moving the pure model upward to
`rustfs-config` cuts the wrong dependency direction without adding another crate.

Creating a new crate now would add a second config namespace before consumers
are migrated. That would increase re-export and compatibility surface while not
removing any storage or runtime dependency by itself.

## Allowed Dependencies

The server-config model module may use only:

- `std::collections::HashMap`
- `std::sync::{LazyLock, OnceLock}` for the default `KVS` registration surface
- `serde` for `KV` and `KVS` serialization compatibility
- `serde_json` for `Config::marshal` and `Config::unmarshal`
- existing `rustfs-config` constants and subsystem modules

If `serde` and `serde_json` are added to `rustfs-config`, they should be attached
only to a model feature such as `server-config-model` unless the implementation
PR proves that making them non-optional is simpler and harmless for downstream
builds.

## Forbidden Dependencies

The model module must not depend on:

- `rustfs-ecstore`
- `rustfs`
- `StorageAPI` or object persistence helpers
- notify, audit, targets, IAM, scanner, KMS, or admin handler crates
- async runtimes, HTTP/router crates, object-store crates, or runtime lifecycle
  state
- global server-config snapshot state such as `GLOBAL_SERVER_CONFIG`
- `ConfigSys`, `read_config_without_migrate`, `save_server_config`, or any
  `com.rs` persistence helper

## Boundary Split

Move in the first extraction:

- `KV`
- `KVS`
- `Config`
- `DEFAULT_KVS`
- `register_default_kvs`
- `Config::new`
- `Config::get_value`
- `Config::set_defaults`
- `Config::marshal`
- `Config::unmarshal`
- `Config::merge`

Keep in `ecstore`:

- `ConfigSys`
- `GLOBAL_SERVER_CONFIG`
- `get_global_server_config`
- `set_global_server_config`
- `init_global_config_sys`
- `try_migrate_server_config`
- `read_config_without_migrate`
- `save_server_config`
- generic `com.rs` config-object helpers
- storage-class runtime global state

Keep default registration wiring in `ecstore::config::init` until a later PR
extracts a dedicated default-registration contract. The values may be registered
through the moved `rustfs_config::server_config::register_default_kvs`, but the
startup order and caller remain unchanged.

## Required Shape Preservation

The extraction PR must preserve:

- `KV { key, value, hidden_if_empty }`
- `#[serde(default, alias = "hiddenIfEmpty")]` on `KV::hidden_if_empty`
- `KVS(pub Vec<KV>)`
- `Config(pub HashMap<String, HashMap<String, KVS>>)`
- `KVS::new`, `get`, `lookup`, `is_empty`, `keys`, `insert`, and `extend`
- `Config::new`, `get_value`, `set_defaults`, `marshal`, `unmarshal`, and
  `merge`
- `Config::new()` default application after `ecstore::config::init()`
- existing persisted server-config JSON shape
- existing target, notify, audit, scanner, OIDC, and admin interpretation of
  `Config` and `KVS`

## Next PR Requirements

`CFG-003` should be a pure model extraction or narrow `api-extraction` PR. It
must not migrate consumers, change persistence helpers, or alter runtime
behavior.

`CFG-004` should keep the old `rustfs_ecstore::config::*` path as a temporary
compatibility shim and register its removal condition.

`CFG-005` should migrate external consumers one group at a time after the model
and compatibility path are stable.

## Verification Gate

Before pushing an extraction PR, run:

- serde roundtrip tests for old and new paths
- tests for `hiddenIfEmpty` alias compatibility
- tests for `KVS` insertion, lookup, extension, and keys behavior
- tests for `Config::new`, `set_defaults`, `marshal`, `unmarshal`, and `merge`
- a compile smoke test that the old `rustfs_ecstore::config::{Config, KV, KVS}`
  path still works
- `cargo tree -p rustfs-config --edges normal`
- `cargo tree -p rustfs-ecstore --edges normal`
- `./scripts/check_layer_dependencies.sh`
- `./scripts/check_architecture_migration_rules.sh`
- `./scripts/check_metrics_migration_refs.sh`
- `cargo fmt --all --check`
- `make pre-commit`

## Non-Goals

- No consumer migration in `CFG-002`.
- No code movement in `CFG-002`.
- No new crate in `CFG-002`.
- No `com.rs` or `StorageAPI` movement in the first model extraction.
- No global server-config state migration until the model path is stable.
