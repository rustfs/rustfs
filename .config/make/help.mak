## —— Help, Help Build and Help Docker -------------------------------------------------------------


.PHONY: help
help: ## Shows This Help Menu
	echo -e "$$HEADER"
	grep -E '(^[a-zA-Z0-9_-]+:.*?## .*$$)|(^## )' $(MAKEFILE_LIST) | sed 's/^[^:]*://g' | awk 'BEGIN {FS = ":.*?## | #"} ; {printf "${cyan}%-30s${reset} ${white}%s${reset} ${green}%s${reset}\n", $$1, $$2, $$3}' | sed -e 's/\[36m##/\n[32m##/'

.PHONY: help-build
help-build: ## Shows RustFS build help
	@echo ""
	@echo "💡 build-rustfs.sh script provides more options, smart detection and binary verification"
	@echo ""
	@echo "🔧 Direct usage of build-rustfs.sh script:"
	@echo ""
	@echo "	./build-rustfs.sh --help                                # View script help"
	@echo "	./build-rustfs.sh --no-console                          # Build without console resources"
	@echo "	./build-rustfs.sh --force-console-update                # Force update console resources"
	@echo "	./build-rustfs.sh --dev                                 # Development mode build"
	@echo "	./build-rustfs.sh --sign                                # Sign binary files"
	@echo "	./build-rustfs.sh --platform x86_64-unknown-linux-gnu   # Specify target platform"
	@echo "	./build-rustfs.sh --skip-verification                   # Skip binary verification"
	@echo ""

.PHONY: help-docker
help-docker: ## Shows docker environment and suggestion help
	@echo ""
	@echo "📋 Environment Variables:"
	@echo "	REGISTRY                 Image registry address (required for push)"
	@echo "	DOCKERHUB_USERNAME       Docker Hub username"
	@echo "	DOCKERHUB_TOKEN          Docker Hub access token"
	@echo "	GITHUB_TOKEN             GitHub access token"
	@echo ""
	@echo "💡 Suggestions:"
	@echo "	Production use:          Use docker-buildx* commands (based on precompiled binaries)"
	@echo "	Local development:       Use docker-dev* commands (build from source)"
	@echo "	Development environment: Use dev-env-* commands to manage dev containers"
	@echo ""
