# GitHub Workflow Instructions

Applies to `.github/` and repository pull-request operations.

## Pull Requests

- PR titles and descriptions must be in English.
- Use `.github/pull_request_template.md` for every PR body.
- Keep all template section headings.
- Use `N/A` for non-applicable sections.
- Include verification commands in the PR details.

## CI Alignment

When changing CI-sensitive behavior, keep local validation aligned with `.github/workflows/ci.yml`.

Current `test-and-lint` gate includes:

- `cargo nextest run --all --exclude e2e_test`
- `cargo test --all --doc`
- `cargo fmt --all --check`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `./scripts/check_layer_dependencies.sh`
