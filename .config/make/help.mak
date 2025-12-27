## —— Help -----------------------------------------------------------------------------------------

.PHONY: help
help: ## Shows This Help Menu
	echo -e "$$HEADER"
	grep -E '(^[a-zA-Z0-9_-]+:.*?## .*$$)|(^## )' $(MAKEFILE_LIST) | sed 's/^[^:]*://g' | awk 'BEGIN {FS = ":.*?## | #"} ; {printf "${cyan}%-30s${reset} ${white}%s${reset} ${green}%s${reset}\n", $$1, $$2, $$3}' | sed -e 's/\[36m##/\n[32m##/'




# ========================================================================================
# Help and Documentation
# ========================================================================================

# .PHONY: help-build
# help-build:
# 	@echo "🔨 RustFS Build Help:"
# 	@echo ""
# 	@echo "🚀 Local Build (Recommended):"
# 	@echo "  make build                               # Build RustFS binary (includes console by default)"
# 	@echo "  make build-dev                           # Development mode build"
# 	@echo "  make build-musl                          # Build x86_64 musl version"
# 	@echo "  make build-gnu                           # Build x86_64 GNU version"
# 	@echo "  make build-musl-arm64                    # Build aarch64 musl version"
# 	@echo "  make build-gnu-arm64                     # Build aarch64 GNU version"
# 	@echo ""
# 	@echo "🐳 Docker Build:"
# 	@echo "  make build-docker                        # Build using Docker container"
# 	@echo "  make build-docker BUILD_OS=ubuntu22.04   # Specify build system"
# 	@echo ""
# 	@echo "🏗️ Cross-architecture Build:"
# 	@echo "  make build-cross-all                     # Build binaries for all architectures"
# 	@echo ""
# 	@echo "🔧 Direct usage of build-rustfs.sh script:"
# 	@echo "  ./build-rustfs.sh --help                 # View script help"
# 	@echo "  ./build-rustfs.sh --no-console           # Build without console resources"
# 	@echo "  ./build-rustfs.sh --force-console-update # Force update console resources"
# 	@echo "  ./build-rustfs.sh --dev                  # Development mode build"
# 	@echo "  ./build-rustfs.sh --sign                 # Sign binary files"
# 	@echo "  ./build-rustfs.sh --platform x86_64-unknown-linux-gnu   # Specify target platform"
# 	@echo "  ./build-rustfs.sh --skip-verification    # Skip binary verification"
# 	@echo ""
# 	@echo "💡 build-rustfs.sh script provides more options, smart detection and binary verification"

# .PHONY: help-docker
# help-docker:
# 	@echo "🐳 Docker Multi-architecture Build Help:"
# 	@echo ""
# 	@echo "🚀 Production Image Build (Recommended to use docker-buildx.sh):"
# 	@echo "  make docker-buildx                       # Build production multi-arch image (no push)"
# 	@echo "  make docker-buildx-push                  # Build and push production multi-arch image"
# 	@echo "  make docker-buildx-version VERSION=v1.0.0        # Build specific version"
# 	@echo "  make docker-buildx-push-version VERSION=v1.0.0   # Build and push specific version"
# 	@echo ""
# 	@echo "🔧 Development/Source Image Build (Local development testing):"
# 	@echo "  make docker-dev                          # Build dev multi-arch image (cannot load locally)"
# 	@echo "  make docker-dev-local                    # Build dev single-arch image (local load)"
# 	@echo "  make docker-dev-push REGISTRY=xxx       # Build and push dev image"
# 	@echo ""
# 	@echo "🏗️ Local Production Image Build (Alternative):"
# 	@echo "  make docker-buildx-production-local      # Build production single-arch image locally"
# 	@echo ""
# 	@echo "📦 Single-architecture Build (Traditional way):"
# 	@echo "  make docker-build-production             # Build single-arch production image"
# 	@echo "  make docker-build-source                 # Build single-arch source image"
# 	@echo ""
# 	@echo "🚀 Development Environment Management:"
# 	@echo "  make dev-env-start                       # Start development container environment"
# 	@echo "  make dev-env-stop                        # Stop development container environment"
# 	@echo "  make dev-env-restart                     # Restart development container environment"
# 	@echo ""
# 	@echo "🔧 Auxiliary Tools:"
# 	@echo "  make build-cross-all                     # Build binaries for all architectures"
# 	@echo "  make docker-inspect-multiarch IMAGE=xxx  # Check image architecture support"
# 	@echo ""
# 	@echo "📋 Environment Variables:"
# 	@echo "  REGISTRY          Image registry address (required for push)"
# 	@echo "  DOCKERHUB_USERNAME    Docker Hub username"
# 	@echo "  DOCKERHUB_TOKEN       Docker Hub access token"
# 	@echo "  GITHUB_TOKEN          GitHub access token"
# 	@echo ""
# 	@echo "💡 Suggestions:"
# 	@echo "  - Production use: Use docker-buildx* commands (based on precompiled binaries)"
# 	@echo "  - Local development: Use docker-dev* commands (build from source)"
# 	@echo "  - Development environment: Use dev-env-* commands to manage dev containers"

# .PHONY: help
# help:
# 	@echo "🦀 RustFS Makefile Help:"
# 	@echo ""
# 	@echo "📋 Main Command Categories:"
# 	@echo "  make help-build                          # Show build-related help"
# 	@echo "  make help-docker                         # Show Docker-related help"
# 	@echo ""
# 	@echo "🔧 Code Quality:"
# 	@echo "  make fmt                                 # Format code"
# 	@echo "  make clippy                              # Run clippy checks"
# 	@echo "  make test                                # Run tests"
# 	@echo "  make pre-commit                          # Run all pre-commit checks"
# 	@echo ""
# 	@echo "🚀 Quick Start:"
# 	@echo "  make build                               # Build RustFS binary"
# 	@echo "  make docker-dev-local                    # Build development Docker image (local)"
# 	@echo "  make dev-env-start                       # Start development environment"
# 	@echo ""
# 	@echo "💡 For more help use 'make help-build' or 'make help-docker'"
