## —— Pre Commit Checks ----------------------------------------------------------------------------

.PHONY: setup-hooks
setup-hooks: ## Set up git hooks
	@echo "🔧 Setting up git hooks..."
	chmod +x .git/hooks/pre-commit
	@echo "✅ Git hooks setup complete!"

.PHONY: pre-commit
pre-commit: fmt clippy-check compilation-check test ## Run pre-commit checks
	@echo "✅ All pre-commit checks passed!"