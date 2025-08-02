#!/usr/bin/env python3
import os
import shutil

# 中止rebase
rebase_dir = ".git/rebase-merge"
if os.path.exists(rebase_dir):
    shutil.rmtree(rebase_dir)
    print("✅ 已中止rebase")

# 恢复HEAD到原分支
with open(".git/HEAD", "w") as f:
    f.write("ref: refs/heads/feature/add-server-components-tests\n")

print("🔧 Git状态已重置")