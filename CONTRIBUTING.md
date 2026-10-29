# RustFS Development Guide

## 📋 Code Quality Requirements

This guide covers the local development environment and the checks expected before contributing.

### Shared Code Standards

All contributions, whether written by a person or generated with assistance, follow the same [implementation and Rust style rules](.agents/references/implementation.md), root [quality and verification policy](AGENTS.md), and applicable scoped `AGENTS.md` files. Repository formatter/lint settings and domain contracts take precedence over personal preferences. Keep style-rule changes in those shared sources rather than maintaining a second version here.

For assisted work, use only repository-maintained skills under `.agents/skills/`; external skills are not part of this project's workflow. Follow the root instructions and ordinary tools when no repository skill matches.

For client setup and evidence that root/scoped rules are actually loaded, see [agent setup and instruction validation](docs/testing/agent-instructions.md). Keep project policy in shared sources rather than copying it into client-specific files.

### 🔧 Code Formatting Rules

Use the repository formatter and lint configuration for changed code. Follow `rust-toolchain.toml`; if local and CI results differ, compare compiler, formatter, and Clippy versions as described in the [shared style contract](.agents/references/implementation.md#shared-style-contract).

#### Verification Requirements

Before submitting changes for review, complete the applicable [verification tier](AGENTS.md#verification) for the final diff. Documentation-only changes use documentation checks; they do not require Cargo formatting, compilation, Clippy, or tests.

- For Rust changes, format with the repository configuration and verify with `cargo fmt --all --check`. Inspect formatter edits and keep unrelated work out of the change.
- For Rust behavior changes, run focused behavioral tests and package-scoped Clippy with the affected targets/features and `-- -D warnings`. Compilation alone does not check lint policy, and a test filter matching zero tests does not verify behavior.
- Add compilation checks only for affected targets/features not already compiled by those checks. Use supported feature combinations; do not default every contribution to `--all-features` or full-workspace checks.
- Complete applicable CI gates before merge. Local scope selection does not waive CI requirements or permit weakening a gate.

#### Quick Commands

We provide convenient Makefile targets for common tasks. Select from them using the verification tier; this is not a checklist to run in full for every change.

```bash
# Format all code
make fmt

# Check if code is properly formatted
make fmt-check

# Run workspace clippy checks (all targets, default features, -D warnings)
make clippy-check

# Fast workspace compilation check (excludes e2e_test)
make quick-check

# Full compilation check (cargo check --all-targets)
make compilation-check

# Run tests (shell script tests + workspace tests + doc tests)
make test

# Fast pre-commit gate — see below for exactly what it runs
make pre-commit

# Optional full gate for broad cross-module changes (guards + clippy + tests)
make pre-pr
```

> `make test` requires [cargo-nextest](https://nexte.st) (CI runs it and only nextest honours `.config/nextest.toml` test-groups). Install it with `cargo install cargo-nextest --locked` or a prebuilt binary (see https://nexte.st/docs/installation/). To run the plain `cargo test` fallback anyway (results not authoritative — serialization semantics differ from CI), set `RUSTFS_ALLOW_CARGO_TEST_FALLBACK=1`.

> Some guard checks are Python (`test-wiring-check` in `make pre-commit`, plus the security-coverage and scheduled-validation self-tests in `make test`) and import `tomllib`, so they need **Python 3.11+**. Make resolves the interpreter through `scripts/python_bin.sh`, which prefers a `python3.11`+ on `PATH` and otherwise falls back to `uv run --python 3.12`. macOS ships `/usr/bin/python3` at 3.9, so install a newer one (`brew install python@3.12`) or [uv](https://docs.astral.sh/uv/); pin a specific interpreter with `RUSTFS_PYTHON=/path/to/python3.12`.

> For the full test-layer taxonomy (unit / ecstore black-box / e2e / s3s-e2e / S3 compatibility / chaos / fuzz / bench), each layer's entry command, the naming conventions the migration gate depends on, and the serial/nextest rules, see [docs/testing/README.md](docs/testing/README.md).

> For the event, timeout, required-status, and local reproduction matrix, see [docs/testing/ci-gates.md](docs/testing/ci-gates.md).

### 🔒 Automated Pre-commit Hooks

#### What `make pre-commit` and `make pre-pr` actually run

`make pre-commit` is the **fast** gate: formatting, repository guards, and quick workspace compilation. The exact dependencies of each target are maintained in [`.config/make/pre-commit.mak`](.config/make/pre-commit.mak).

**`make pre-commit` does not replace the scoped Clippy and behavioral test checks applicable to a change.**

`make pre-pr` is the broader local gate: formatting, shared and additional repository guards, `clippy-check`, and `test` (script tests, workspace tests excluding `e2e_test`, and doc tests). Select it only under the root verification policy. It does not reproduce every CI feature/platform/integration lane; CI still runs its configured repository gates.

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
2. **Format changed source** with the applicable formatter; for Rust, use `make fmt` or `cargo fmt --all` and inspect the diff
3. **Run applicable checks** using the verification tier in `AGENTS.md`; use broader gates only when that tier calls for them
4. **Complete the applicable adversarial review** for non-exempt changes (see `AGENTS.md`), resolve findings, and rerun checks affected by fixes
5. **Commit the verified changes** with an English Conventional Commit message; inspect the staged diff first
6. **Push and open/update the PR** following the [Git and PR rules](.agents/references/pull-requests.md), including authorization requirements for assisted work

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
- **CI workflows** define the checks for each event and changed surface; see [CI gates](docs/testing/ci-gates.md)
- **Required checks** must pass before merge; resolve formatting, lint, and test failures instead of bypassing them
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

Replace `<crate>` with the affected package and add its supported feature selection when needed. Inspect automatic fixes before staging them.

```bash
# See detailed clippy output for the affected package
cargo clippy -p <crate> --all-targets -- -D warnings

# Fix automatically fixable issues
cargo clippy --fix -p <crate> --all-targets
```

## Pull Requests

Follow the [Git and PR rules](.agents/references/pull-requests.md) and current [PR template](.github/pull_request_template.md). Use English Conventional Commit titles, describe the changed behavior, and provide actual verification evidence and material limits. Keep template headings in the template itself rather than maintaining a copied list here.
