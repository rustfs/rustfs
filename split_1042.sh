#!/bin/bash
set -e

# Original PR branch
ORIGIN_BRANCH=$(git branch --show-current)
echo "Current branch: $ORIGIN_BRANCH"

# 1. Config Branch
echo "Creating branch: feat/cold-tier-config"
git checkout main
git pull origin main
git checkout -b feat/cold-tier-config

# Checkout config files
git checkout $ORIGIN_BRANCH -- \
  crates/ecstore/src/tier/cold_tier_config.rs \
  crates/ecstore/src/tier/mod.rs

# Commit
git add crates/ecstore/src/tier
git commit -m "feat(tier): Add cold tier configuration module"

# Push
git push origin feat/cold-tier-config
gh pr create --repo rustfs/rustfs --base main --head tennisleng:feat/cold-tier-config --title "feat(tier): Add cold tier configuration module" --body "Splitting #1042: Core configuration structures."

# 2. Integration Branch
echo "Creating branch: feat/cold-tier-integration"
git checkout feat/cold-tier-config
git checkout -b feat/cold-tier-integration

# Checkout integration files
git checkout $ORIGIN_BRANCH -- \
  crates/ecstore/src/bucket/lifecycle/lifecycle.rs \
  crates/ecstore/src/store_api.rs \
  crates/filemeta/src/fileinfo.rs \
  crates/filemeta/src/filemeta.rs

# Commit
git add .
git commit -m "feat(lifecycle): Integrate cold tiering support"

# Push
git push origin feat/cold-tier-integration
gh pr create --repo rustfs/rustfs --base main --head tennisleng:feat/cold-tier-integration --title "feat(lifecycle): Integrate cold tiering support" --body "Splitting #1042: Integration of cold tiering."

echo "Done splitting #1042"
