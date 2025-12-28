## —— Deploy using dev_deploy.sh script ------------------------------------------------------------

.PHONY: deploy-dev
deploy-dev: build-musl ## Deploy to dev server
	@echo "🚀 Deploying to dev server: $${IP}"
	./scripts/dev_deploy.sh $${IP}