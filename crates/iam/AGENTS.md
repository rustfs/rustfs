# IAM Crate Instructions

Applies to `crates/iam/`.

## Security Boundaries

- Treat IAM changes as security-sensitive by default.
- Never log secrets, tokens, private claims, or credential material.
- Keep deny/allow evaluation behavior consistent with existing tests unless explicitly changing policy semantics.

## Contract Stability

- Preserve compatibility of user/group/policy attachment behavior and token claim handling.
- When changing IAM interfaces, verify impacted call sites in:
  - `rustfs/src/auth.rs`
  - `rustfs/src/admin/`
  - `crates/policy/`

## Error Handling

- Return explicit errors; do not use panic-driven control flow outside tests.
- Keep error messages actionable but avoid leaking sensitive context.

## Suggested Validation

- `cargo test -p rustfs-iam`
- Full gate before commit: `make pre-commit`
