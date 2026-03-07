# Config Crate Instructions

Applies to `crates/config/`.

## Environment Variable Naming

- Global configuration variables must use flat `RUSTFS_*` names.
- Do not introduce module-segmented names such as `RUSTFS_CONFIG_*`.

Canonical examples:

- `RUSTFS_REGION`
- `RUSTFS_ADDRESS`
- `RUSTFS_VOLUMES`
- `RUSTFS_LICENSE`
- `RUSTFS_SCANNER_ENABLED`
- `RUSTFS_HEAL_ENABLED`

## Compatibility

- Deprecated aliases must keep warning behavior.
- Document aliases in `crates/config/README.md`.
- Any alias change should include tests for both canonical and deprecated forms.

## Source of Truth

- Constants: `crates/config/src/constants/app.rs`
- Naming conventions: `crates/config/README.md#environment-variable-naming-conventions`
