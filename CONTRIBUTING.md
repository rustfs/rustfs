# RustFS Development Guide

## 📋 Code Quality Requirements

For instructions on setting up and running the local development environment, please see [Development Guide](docs/DEVELOPMENT.md).

### 🔧 Code Formatting Rules

**MANDATORY**: All code must be properly formatted before committing. This project enforces strict formatting standards to maintain code consistency and readability.

#### Pre-commit Requirements

Before every commit, you **MUST**:

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

# Run clippy checks
make clippy

# Run compilation check
make check

# Run tests
make test

# Run all pre-commit checks (format + clippy + check + test)
make pre-commit

# Setup git hooks (one-time setup)
make setup-hooks
```

### 🔒 Automated Pre-commit Hooks

This project includes a pre-commit hook that automatically runs before each commit to ensure:

- ✅ Code is properly formatted (`cargo fmt --all --check`)
- ✅ No clippy warnings (`cargo clippy --all-targets --all-features -- -D warnings`)
- ✅ Code compiles successfully (`cargo check --all-targets`)

#### Setting Up Pre-commit Hooks

Run this command once after cloning the repository:

```bash
make setup-hooks
```

Or manually:

```bash
chmod +x .git/hooks/pre-commit
```

### 📝 Formatting Configuration

The project uses the following rustfmt configuration (defined in `rustfmt.toml`):

```toml
max_width = 130
fn_call_width = 90
single_line_let_else_max_width = 100
```

### 🚫 Commit Prevention

If your code doesn't meet the formatting requirements, the pre-commit hook will:

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
3. **Run pre-commit checks**: `make pre-commit`
4. **Commit your changes**: `git commit -m "your message"`
5. **Push to your branch**: `git push`

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

---

Following these guidelines ensures high code quality and smooth collaboration across the RustFS project! 🚀
