# E2E Test Crate Instructions

Applies to `crates/e2e_test/`.

## Test Reliability

- Keep end-to-end tests deterministic and environment-aware.
- Prefer readiness checks and explicit polling over fixed sleep-based timing assumptions.
- Ensure tests isolate resources and clean up temporary state.

## Environment Safety

- For local KMS-related E2E runs, keep proxy bypass settings:
  - `NO_PROXY=127.0.0.1,localhost`
  - clear `HTTP_PROXY` and `HTTPS_PROXY`
- Do not hardcode machine-specific paths or credentials.

## Scope and Cost

- Place exhaustive integration behavior here; keep unit behavior in source crates.
- Keep new E2E scenarios focused and avoid redundant overlap with existing suites.

## Suggested Validation

- `cargo test --package e2e_test`
- Full gate before commit: `make pre-commit`
