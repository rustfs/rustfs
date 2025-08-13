# Claude AI Rules for RustFS Project

## Core Rules Reference

This project follows the comprehensive AI coding rules defined in `.rules.md`. Please refer to that file for the complete set of development guidelines, coding standards, and best practices.

## Claude-Specific Configuration

When using Claude for this project, ensure you:

1. **Review the unified rules**: Always check `.rules.md` for the latest project guidelines
2. **Follow branch protection**: Never attempt to commit directly to main/master branch
3. **Use English**: All code comments, documentation, and variable names must be in English
4. **Clean code practices**: Only make modifications you're confident about
5. **Test thoroughly**: Ensure all changes pass formatting, linting, and testing requirements
6. **Clean up after yourself**: Remove any temporary scripts or test files created during the session

## Quick Reference

### Critical Rules
- 🚫 **NEVER commit directly to main/master branch**
- ✅ **ALWAYS work on feature branches**
- 📝 **ALWAYS use English for code and documentation**
- 🧹 **ALWAYS clean up temporary files after use**
- 🎯 **ONLY make confident, necessary modifications**

### Pre-commit Checklist
```bash
# Before committing, always run:
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo check --all-targets
cargo test
```

### Branch Workflow
```bash
git checkout main
git pull origin main
git checkout -b feat/your-feature-name
# Make your changes
git add .
git commit -m "feat: your feature description"
git push origin feat/your-feature-name
gh pr create
```

## Claude-Specific Best Practices

1. **Task Analysis**: Always thoroughly analyze the task before starting implementation
2. **Minimal Changes**: Make only the necessary changes to accomplish the task
3. **Clear Communication**: Provide clear explanations of changes and their rationale
4. **Error Prevention**: Verify code correctness before suggesting changes
5. **Documentation**: Ensure all code changes are properly documented in English

## Important Notes

- This file serves as an entry point for Claude AI
- All detailed rules and guidelines are maintained in `.rules.md`
- Updates to coding standards should be made in `.rules.md` to ensure consistency across all AI tools
- When in doubt, always refer to `.rules.md` for authoritative guidance
- Claude should prioritize code quality, safety, and maintainability over speed

## See Also

- [.rules.md](./.rules.md) - Complete AI coding rules and guidelines
- [CONTRIBUTING.md](./CONTRIBUTING.md) - Contribution guidelines
- [README.md](./README.md) - Project overview and setup instructions
