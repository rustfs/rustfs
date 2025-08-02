#!/bin/bash

# 停止当前rebase并重新开始
git rebase --abort

# 处理 PR #315
git checkout feature/add-server-components-tests
git reset --hard origin/feature/add-server-components-tests
git pull origin main --rebase
git push --force origin feature/add-server-components-tests

# 处理 PR #316  
git checkout feature/add-integration-tests
git reset --hard origin/feature/add-integration-tests
git pull origin main --rebase
git push --force origin feature/add-integration-tests

echo "完成!"
rm quick_fix.sh