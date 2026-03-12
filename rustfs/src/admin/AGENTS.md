# Admin Module Instructions

Applies to `rustfs/src/admin/`.

## Auth and Authorization

- Do not bypass authentication or authorization checks for admin routes.
- Keep admin permission checks aligned with IAM and policy evaluation behavior.
- Any change to security-sensitive route behavior must include tests.

## API and Routing Stability

- Preserve existing admin endpoint contracts unless a deliberate compatibility change is requested.
- Update route registration and handler tests when adding or changing endpoints.
- Coordinate cross-module changes with:
  - `crates/iam/`
  - `crates/policy/`
  - `crates/kms/`

## Operational Safety

- Avoid exposing secrets or sensitive internals in admin responses and logs.
- Keep health/readiness/admin status responses deterministic and machine-readable.

## Suggested Validation

- Admin handler and routing tests under `rustfs/src/admin/`
- Full gate before commit: `make pre-commit`
