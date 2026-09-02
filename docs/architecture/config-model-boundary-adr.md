# Config Model Boundary ADR

**Use this when:** you touch the server-config model (`Config`, `KV`, `KVS`) or its persistence, or you need to know which crate owns which part of server configuration.
**Source of truth:** `crates/config/src/server_config.rs` (model, default registration, process-global snapshot) and `crates/ecstore/src/config/` (`ConfigSys`, persistence, migration, storage-class runtime state).

## Decision

`rustfs-config` (`crates/config`) owns the pure server-config model and the process-global server-config snapshot. ECStore keeps config persistence, migration, default-registration wiring, startup initialization, and storage-class runtime state. There is no separate config-model crate, and `rustfs_ecstore::config` does not re-export the model or the snapshot accessors.

Import path: `rustfs_config::server_config::{Config, KV, KVS}`. The model sits behind the `server-config-model` feature of `rustfs-config` (`crates/config/Cargo.toml`), which enables `serde` and `serde_json`.

## Why `rustfs-config`

- It is already the lowest RustFS crate for configuration constants and subsystem identifiers used by ECStore, notify, audit, targets, scanner, IAM, and admin code, and the model needs only those constants.
- Moving the model upward removes the wrong-direction dependency (outer crates importing ECStore for a plain data type) without adding another crate or a second config namespace.

## Ownership

| Item | Owner | Notes |
|---|---|---|
| `KV`, `KVS`, `Config` and their methods (`get_value`, `set_defaults`, `marshal`, `unmarshal`, `merge`) | `crates/config/src/server_config.rs` | Pure data model with serde roundtrip |
| `DEFAULT_KVS`, `register_default_kvs` | `crates/config/src/server_config.rs` | Registration surface; ECStore still calls it from `init()` in `crates/ecstore/src/config/mod.rs` |
| `GLOBAL_SERVER_CONFIG`, `get_global_server_config`, `set_global_server_config` | `crates/config/src/server_config.rs` | Process-global snapshot accessors |
| `ConfigSys`, `init()`, `try_migrate_server_config` | `crates/ecstore/src/config/mod.rs` | Startup order and caller unchanged |
| `read_config_without_migrate`, `save_server_config`, other config-object helpers | `crates/ecstore/src/config/com.rs` | Persistence over the object store |
| `GLOBAL_STORAGE_CLASS` and storage-class parsing | `crates/ecstore/src/config/mod.rs`, `crates/ecstore/src/config/storageclass.rs` | Storage behavior stays in ECStore |

## Allowed Dependencies Of The Model Module

- `std::collections::HashMap` and `std::sync::{LazyLock, OnceLock, RwLock}` for `DEFAULT_KVS` and `GLOBAL_SERVER_CONFIG`;
- `serde` for `KV`/`KVS` and `serde_json` for `Config::marshal` / `Config::unmarshal`, gated by `server-config-model`;
- existing `rustfs-config` constants and subsystem modules.

## Forbidden Dependencies Of The Model Module

- `rustfs-ecstore`, `rustfs`, storage-api traits, or object persistence helpers;
- notify, audit, targets, IAM, scanner, KMS, or admin handler crates;
- async runtimes, HTTP/router crates, object-store crates, or runtime lifecycle state;
- `ConfigSys`, `read_config_without_migrate`, `save_server_config`, or any `com.rs` helper.

## Shape Preservation

Persisted server-config JSON must keep decoding unchanged:

- `KV { key, value, hidden_if_empty }` with `#[serde(default, alias = "hiddenIfEmpty")]` on `hidden_if_empty`;
- `KVS(pub Vec<KV>)` and `Config(pub HashMap<String, HashMap<String, KVS>>)`;
- `KVS::{get, lookup, is_empty, keys, insert, extend}` and `Config::{get_value, set_defaults, marshal, unmarshal, merge}` keep their semantics;
- `Config::new()` applies the defaults registered by `ecstore::config::init()`;
- target, notify, audit, scanner, OIDC, and admin code keep interpreting `Config` and `KVS` the same way.
