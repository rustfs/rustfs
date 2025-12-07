---
description: create a PR for point-in-time file history
---

1. Configure git user email to tennisleng@gmail.com
2. Define function `get_branch_name` that prints the current branch name
3. Get the current branch name and store it in a variable called `branch_name`
4. Run cargo fmt
5. Add all changes to git
6. Commit changes with message "feat: Add copy-on-write and point-in-time file history

   Implements support for point-in-time object retrieval and file history
   viewing as part of issue #1021.

   Changes:

   - Added `point_in_time` module to ecstore for finding object versions at specific times
   - Added `file_history` admin handlers for history viewing API
   - Exported new modules in respective crates"

7. Push changes to origin $branch_name
8. Create PR using gh command with title "feat: Add copy-on-write and point-in-time file history" and body "Implements support for point-in-time object retrieval and file history viewing (#1021)."
