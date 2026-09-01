---
name: issue-triage
description: Triage a GitHub issue — determine if it is already fixed, needs implementation, or should be closed. Searches related commits and PRs, verifies implementation status, and posts a triage comment or closes the issue. Use when the user provides an issue URL and asks whether it can be closed or needs work.
---

# Issue Triage

Use this skill when the user provides a GitHub issue URL and asks "can this be closed?", "is this already implemented?", "check completion status", or similar triage questions.

## Workflow

### 1. Fetch issue context

```bash
gh issue view <N> --repo <owner/repo> --json title,body,state,comments,labels,updatedAt
```

Read the issue body to understand what was requested. Extract:
- The specific feature/fix/behavior described.
- Any linked PRs or commits mentioned in the body or comments.
- Any checklist items or sub-issues.

### 2. Search for related work

Search git history for commits referencing the issue:
```bash
git log --oneline --all --grep="<N>" | head -30
```

Search for related PRs:
```bash
gh pr list --search "fixes #<N> OR closes #<N> OR #<N>" --state all --json number,title,state,mergedAt
```

If the issue mentions specific PRs, check their status:
```bash
gh pr view <PR_N> --json state,mergedAt,title
```

### 3. Verify implementation

For each linked or related PR that is merged, verify the fix is actually present on the current main branch:
```bash
git log --oneline main | grep -i "<keyword>"
# or
git log --oneline main --grep="<PR_N>"
```

If the issue describes a specific defect, check the relevant code to confirm the fix is in place:
```bash
grep -n "<pattern>" crates/<relevant>/src/<file>.rs
```

For issues with checklists, verify each item individually. If sub-items are tracked as separate issues, check those too:
```bash
gh issue view <SUB_N> --repo <owner/repo> --json state
```

### 4. Determine verdict

- **All items fixed and merged**: Close with a summary comment listing what was fixed and which PRs.
- **Some items fixed, some remaining**: Comment with status of each item. Do not close.
- **Not yet implemented**: Comment with a summary of what remains. Do not close.
- **Superseded or no longer relevant**: Close with explanation.

### 5. Take action

Close with comment:
```bash
gh issue close <N> --repo <owner/repo> --comment "<body>"
```

Comment without closing:
```bash
gh issue comment <N> --repo <owner/repo> --body-file /tmp/triage.md
```

Update issue labels if needed:
```bash
gh issue edit <N> --repo <owner/repo> --add-label "completed" --remove-label "needs-triage"
```

Always use `--body-file` for multiline content, never inline `--body`.

### 6. Handle multi-issue batches

When the user asks to check multiple issues (e.g., "check all issues by user X" or "scan backlog for closable issues"):
1. List the issues: `gh issue list --repo <repo> --author <user> --state open --json number,title,updatedAt`
2. For each issue, run steps 1-5 above.
3. Report a summary table of all triaged issues with verdicts.

## Output format

### Issue Triage: #<N> — <title>

**State**: OPEN / CLOSED
**Linked PRs**: <list with merge status>

#### Assessment
<what was requested vs what is implemented>

#### Verdict
- Close — all items resolved by <PR list>
- Keep open — <remaining items>
- Not started — <what needs to be done>

#### Action taken
- Closed with comment / Commented / No action

## Notes

- The user may ask in Chinese ("是否可以关闭", "检查完成情况"); respond in the same language.
- When closing, always include a summary of what was fixed and which PRs resolved it — this creates a useful audit trail.
- For issues in `rustfs/backlog`, use `--repo rustfs/backlog`.
- For issues in `rustfs/rustfs`, use `--repo rustfs/rustfs`.
- If the issue has sub-issues (GitHub sub-issues API), check each one's state before declaring the parent complete.
