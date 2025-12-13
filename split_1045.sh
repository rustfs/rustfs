#!/bin/bash
set -e

# Original PR branch
ORIGIN_BRANCH=$(git branch --show-current)
echo "Current branch: $ORIGIN_BRANCH"

# 1. Create Backend Branch
echo "Creating backend branch: feat/point-in-time-core"
git checkout main
git pull origin main
git checkout -b feat/point-in-time-core

# Checkout specific backend files from original PR
git checkout $ORIGIN_BRANCH -- \
  crates/ecstore/src/point_in_time.rs \
  crates/ecstore/src/store_list_objects.rs \
  crates/ecstore/src/lib.rs

# Commit backend changes
git add crates/ecstore
git commit -m "feat(store): Add point-in-time file history core logic"

# Push backend branch
# git push origin feat/point-in-time-core
# gh pr create --title "feat(store): Add point-in-time file history core logic" --body "Splitting #1045: Core backend implementation."

# 2. Create API Branch (based on Backend or independent? Let's make it independent if possible, but it likely depends on backend)
# If it depends, we base it on backend.
echo "Creating API branch: feat/point-in-time-api"
git checkout feat/point-in-time-core
git checkout -b feat/point-in-time-api

# Checkout API files
git checkout $ORIGIN_BRANCH -- \
  rustfs/src/admin/handlers/file_history.rs \
  rustfs/src/admin/handlers.rs \
  rustfs/src/admin/mod.rs

# Commit API changes
git add rustfs/src/admin
git commit -m "feat(admin): Add file history API endpoints"

# Push API branch
# git push origin feat/point-in-time-api
# gh pr create --title "feat(admin): Add file history API endpoints" --body "Splitting #1045: Admin API handlers. Depends on #new-core-pr"

echo "Split branches created locally. Review them before pushing."
