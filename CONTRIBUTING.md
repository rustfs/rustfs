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

# Full pre-PR gate (pre-commit gates + clippy + tests)
make pre-pr
```

> `make test` requires [cargo-nextest](https://nexte.st) (CI runs it and only nextest honours `.config/nextest.toml` test-groups). Install it with `cargo install cargo-nextest --locked` or a prebuilt binary (see https://nexte.st/docs/installation/). To run the plain `cargo test` fallback anyway (results not authoritative — serialization semantics differ from CI), set `RUSTFS_ALLOW_CARGO_TEST_FALLBACK=1`.

> For the full test-layer taxonomy (unit / ecstore black-box / e2e / s3s-e2e / S3 compatibility / chaos / fuzz / bench), each layer's entry command, the naming conventions the migration gate depends on, and the serial/nextest rules, see [docs/testing/README.md](docs/testing/README.md).

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
A green `make pre-commit` is not enough to open a pull request.

`make pre-pr` is the **full** gate: it runs all of the guard checks above,
then `clippy-check` (`cargo clippy --all-targets --all-features -- -D warnings`)
and `test` (shell script tests, workspace tests excluding `e2e_test`, and doc
tests). Run `make pre-pr` before opening or updating a pull request — this is
what CI enforces.

### 🔒 Git Pre-commit Hooks (optional)

Git hooks are **not** versioned in this repository, so a fresh clone has no
active pre-commit hook. If you add your own `.git/hooks/pre-commit` (a good
choice is a one-liner that runs `make pre-commit`), you can mark it executable
with:

```bash
make setup-hooks
```

Or manually:

```bash
chmod +x .git/hooks/pre-commit
```

With or without a hook, the expectation is the same: run `make pre-commit`
before committing and `make pre-pr` before opening a pull request.

### 📝 Formatting Configuration

The project uses the following rustfmt configuration (defined in `rustfmt.toml`):

```toml
max_width = 130
fn_call_width = 90
single_line_let_else_max_width = 100
```

### 🚫 Commit Prevention

If you set up a pre-commit hook and your code doesn't meet the formatting requirements, the hook will:

1. **Block the commit** and show clear error messages
2. **Provide exact commands** to fix the issues
3. **Guide you through** the resolution process

Example output when formatting fails:

```
❌ Code formatting check failed!
💡 Please run 'cargo fmt --all' to format your code before committing.

🔧 Quick fix:
   cargo fmt --all
   git add .
   git commit
```

### 🔄 Development Workflow

1. **Make your changes**
2. **Format your code**: `make fmt` or `cargo fmt --all`
3. **Run the fast gate**: `make pre-commit` (no clippy, no tests)
4. **Commit your changes**: `git commit -m "your message"`
5. **Run the full gate before opening/updating a PR**: `make pre-pr` (clippy + tests)
6. **Push to your branch**: `git push`

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
# Check if hook is executable
ls -la .git/hooks/pre-commit

# Make it executable if needed
chmod +x .git/hooks/pre-commit
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
