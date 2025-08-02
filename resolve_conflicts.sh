#!/bin/bash

cd /workspace

echo "🔧 解决PR #315和#316的冲突..."

# 处理 feature/add-server-components-tests (PR #315)
echo "📝 处理PR #315: feature/add-server-components-tests"
git add Cargo.lock
git rebase --continue
if [ $? -eq 0 ]; then
    echo "✅ Rebase完成，推送更改..."
    git push --force origin feature/add-server-components-tests
    echo "✅ PR #315冲突已解决"
else
    echo "❌ Rebase失败"
fi

# 处理 feature/add-integration-tests (PR #316)  
echo "📝 处理PR #316: feature/add-integration-tests"
git checkout feature/add-integration-tests
git fetch origin main
git rebase origin/main
if [ $? -ne 0 ]; then
    echo "🔧 解决Cargo.lock冲突..."
    git checkout --theirs Cargo.lock
    git add Cargo.lock
    git rebase --continue
fi

if [ $? -eq 0 ]; then
    echo "✅ Rebase完成，推送更改..."
    git push --force origin feature/add-integration-tests
    echo "✅ PR #316冲突已解决"
else
    echo "❌ Rebase失败"
fi

echo ""
echo "🎉 冲突解决完成！"
echo "📋 请检查GitHub上的PR状态："
echo "- PR #315: https://github.com/rustfs/rustfs/pull/315"
echo "- PR #316: https://github.com/rustfs/rustfs/pull/316"

# 删除本脚本
rm -f resolve_conflicts.sh