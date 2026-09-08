# RustFS Development Guide

## 📋 Code Quality Requirements

This guide covers the local development environment and the checks expected before contributing.

### 🔧 Code Formatting Rules

**MANDATORY**: All code must be properly formatted before committing. This project enforces strict formatting standards to maintain code consistency and readability.

#### Verification Requirements

Before submitting your changes for review, you **MUST**:

1. **Format your code**:

   ```bash
   cargo fmt --all
   ```

2. **Verify formatting**:

   ```bash
   cargo fmt --all --check
   ```

3. **Pass clippy checks**:

   ```bash
   cargo clippy --all-targets --all-features -- -D warnings
   ```

4. **Ensure compilation**:

   ```bash
   cargo check --all-targets
   ```

#### Quick Commands

We provide convenient Makefile targets for common tasks:

```bash
# Format all code
make fmt

# Check if code is properly formatted
make fmt-check

# Run clippy checks (all targets, all features, -D warnings)
make clippy-check

# Fast workspace compilation check (excludes e2e_test)
make quick-check

# Full compilation check (cargo check --all-targets)
make compilation-check

# Run tests (shell script tests + workspace tests + doc tests)
make test

# Fast pre-commit gate — see below for exactly what it runs
make pre-commit

# Optional full gate for broad cross-module changes (pre-commit + clippy + tests)
make pre-pr
```

> `make test` requires [cargo-nextest](https://nexte.st) (CI runs it and only nextest honours `.config/nextest.toml` test-groups). Install it with `cargo install cargo-nextest --locked` or a prebuilt binary (see https://nexte.st/docs/installation/). To run the plain `cargo test` fallback anyway (results not authoritative — serialization semantics differ from CI), set `RUSTFS_ALLOW_CARGO_TEST_FALLBACK=1`.

> Some guard checks are Python (`test-wiring-check` in `make pre-commit`, plus the
> security-coverage and scheduled-validation self-tests in `make test`) and import
> `tomllib`, so they need **Python 3.11+**. Make resolves the interpreter through
> `scripts/python_bin.sh`, which prefers a `python3.11`+ on `PATH` and otherwise falls
> back to `uv run --python 3.12`. macOS ships `/usr/bin/python3` at 3.9, so install a
> newer one (`brew install python@3.12`) or [uv](https://docs.astral.sh/uv/); pin a
> specific interpreter with `RUSTFS_PYTHON=/path/to/python3.12`.

> For the full test-layer taxonomy (unit / ecstore black-box / e2e / s3s-e2e / S3 compatibility / chaos / fuzz / bench), each layer's entry command, the naming conventions the migration gate depends on, and the serial/nextest rules, see [docs/testing/README.md](docs/testing/README.md).

> For the event, timeout, required-status, and local reproduction matrix, see [docs/testing/ci-gates.md](docs/testing/ci-gates.md).

### 🔒 Automated Pre-commit Hooks
#### What `make pre-commit` and `make pre-pr` actually run

`make pre-commit` is the **fast** gate. It runs, in order
(see `.config/make/pre-commit.mak`):

1. `fmt-check` — `cargo fmt --all --check`
2. `unsafe-code-check` — `./scripts/check_unsafe_code_allowances.sh`
3. `architecture-migration-check` — `./scripts/check_architecture_migration_rules.sh`
4. `logging-guardrails-check` — `./scripts/check_logging_guardrails.sh`
5. `tokio-io-uring-check` — `./scripts/check_no_tokio_io_uring.sh`
6. `extension-schema-check` — `./scripts/check_extension_schema_boundaries.sh`
7. `doc-paths-check` — `./scripts/check_doc_paths.sh`
8. `quick-check` — `cargo check --workspace --exclude e2e_test`

**`make pre-commit` does NOT run clippy and does NOT run any tests.**
It does not replace the scoped Clippy and test checks applicable to a change.

`make pre-pr` is the **full** gate: it runs all of the guard checks above,
then `clippy-check` (`cargo clippy --all-targets --all-features -- -D warnings`)
and `test` (shell script tests, workspace tests excluding `e2e_test`, and doc
tests). Complete the applicable multi-role adversarial review described in
`AGENTS.md` first. Do not run `make pre-pr` locally by default before opening or
updating a pull request. Consider it only for a broad change that spans multiple
modules and whose impact cannot be bounded by targeted checks; decide from the
affected boundaries and risks. CI still runs its configured repository gates.

### 🔒 Git Pre-commit Hooks (optional)

The optional hook uses the checked-in `.pre-commit-config.yaml`. Install [pre-commit](https://pre-commit.com/#installation), then run this from the checkout or a linked worktree:

```bash
make setup-hooks
```

The hook runs `cargo fmt --all --check` when staged files include Rust source. It does not compile the workspace or run tests. Fix formatting with `cargo fmt --all`, inspect and stage the result, then commit again.

`pre-commit install` resolves Git's hook directory for linked worktrees and preserves an existing hook in migration mode. If you use `core.hooksPath`, keep that hook manager and integrate `pre-commit run` there; the installer refuses to silently replace that configuration.

A local hook provides early formatting feedback. With or without it, follow the verification tiers in `AGENTS.md`, run relevant behavioral tests, and satisfy the CI merge gates. `make pre-commit` and `make dev-check` remain explicit broader commands.

### 📝 Formatting Configuration

The project uses the following rustfmt configuration (defined in `rustfmt.toml`):

```toml
max_width = 130
fn_call_width = 90
single_line_let_else_max_width = 100
```

### 🔄 Development Workflow

1. **Make your changes**
2. **Format your code**: `make fmt` or `cargo fmt --all`
3. **Select relevant checks** using the validation tier in `AGENTS.md`; use `make pre-commit` when its broader fast gate adds useful coverage
4. **Commit your changes**: `git commit -m "your message"`
5. **Complete the applicable multi-role adversarial review** for non-exempt changes (see `AGENTS.md`)
6. **Run applicable scoped checks before opening/updating a PR**; consider
   `make pre-pr` only for broad cross-module changes whose impact cannot be
   bounded by targeted checks
7. **Push to your branch**: `git push`

### 🛠️ IDE Integration

#### VS Code

Install the `rust-analyzer` extension and add to your `settings.json`:

```json
{
    "rust-analyzer.rustfmt.extraArgs": ["--config-path", "./rustfmt.toml"],
    "editor.formatOnSave": true,
    "[rust]": {
        "editor.defaultFormatter": "rust-lang.rust-analyzer"
    }
}
```

#### Other IDEs

Configure your IDE to:

- Use the project's `rustfmt.toml` configuration
- Format on save
- Run clippy checks

### ❗ Important Notes

- **Never bypass formatting checks** - they are there for a reason
- **All CI/CD pipelines** will also enforce these same checks
- **Pull requests** will be automatically rejected if formatting checks fail
- **Consistent formatting** improves code readability and reduces merge conflicts

### 🆘 Troubleshooting

#### Pre-commit hook not running?

```bash
pre-commit validate-config
pre-commit run --all-files
# Inspect any configured hook manager; do not overwrite it.
git config --get core.hooksPath
# Install if no separate hook manager is configured.
make setup-hooks
```

#### Formatting issues?

```bash
# Format all code
cargo fmt --all

# Check specific issues
cargo fmt --all --check --verbose
```

#### Clippy issues?

```bash
# See detailed clippy output
cargo clippy --all-targets --all-features -- -D warnings

# Fix automatically fixable issues
cargo clippy --fix --all-targets --all-features
```

## 📝 Pull Request Guidelines

### Language Requirements

**All Pull Request titles and descriptions MUST be written in English.**

This ensures:
- Consistency across all contributions
- Accessibility for international contributors
- Better integration with automated tools and CI/CD systems
- Clear communication in a globally understood language

#### PR Description Requirements

When creating a Pull Request, ensure:

1. **Title**: Use English and follow Conventional Commits format (e.g., `fix: improve s3-tests readiness detection`)
2. **Description**: Write in English, following the PR template format
3. **Code Comments**: Must be in English (as per coding standards)
4. **Commit Messages**: Must be in English (as per commit guidelines)

#### PR Template

Always use the PR template (`.github/pull_request_template.md`) and fill in all sections:
- Type of Change
- Related Issues
- Summary of Changes
- Checklist
- Impact
- Additional Notes

**Note**: While you may communicate with reviewers in Chinese during discussions, the PR itself (title, description, and all formal documentation) must be in English.

---

Following these guidelines ensures high code quality and smooth collaboration across the RustFS project! 🚀
