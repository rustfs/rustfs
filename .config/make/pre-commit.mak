## —— Pre Commit Checks ----------------------------------------------------------------------------

.NOTPARALLEL: pre-commit pre-pr dev-check

.PHONY: setup-hooks
setup-hooks: ## Set up git hooks
	@echo "🔧 Setting up git hooks..."
	chmod +x .git/hooks/pre-commit
	@echo "✅ Git hooks setup complete!"

.PHONY: doc-paths-check
doc-paths-check: ## Check that instruction/architecture docs reference existing file paths
	@echo "📄 Checking doc path references..."
	./scripts/check_doc_paths.sh

.PHONY: pre-commit
pre-commit: fmt-check unsafe-code-check architecture-migration-check logging-guardrails-check doc-paths-check quick-check ## Run fast pre-commit checks without clippy/full tests
	@echo "✅ All pre-commit checks passed!"

.PHONY: pre-pr
pre-pr: fmt-check unsafe-code-check architecture-migration-check logging-guardrails-check doc-paths-check clippy-check test ## Run full pre-PR checks with clippy and tests
	@echo "✅ All pre-PR checks passed!"

.PHONY: dev-check
dev-check: fmt-check unsafe-code-check architecture-migration-check logging-guardrails-check doc-paths-check quick-check ## Run fast local development checks
	@echo "✅ Fast development checks passed!"
