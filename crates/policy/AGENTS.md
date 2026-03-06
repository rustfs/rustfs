# Policy Crate Instructions

Applies to `crates/policy/`.

## Policy Semantics

- Keep action/resource/condition evaluation semantics stable unless compatibility changes are explicitly requested.
- Avoid introducing permissive fallbacks that may expand access unexpectedly.
- Any semantic change must include focused regression tests.

## Integration Awareness

- Validate compatibility with IAM and request-auth call sites:
  - `crates/iam/`
  - `rustfs/src/auth.rs`
  - `rustfs/src/storage/access.rs`

## Error and Observability

- Prefer explicit, typed errors over implicit fallback behavior.
- Log policy evaluation diagnostics without exposing sensitive credential content.

## Suggested Validation

- `cargo test -p rustfs-policy`
- Full gate before commit: `make pre-commit`
