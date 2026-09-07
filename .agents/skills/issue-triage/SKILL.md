---
name: issue-triage
description: Assess whether a GitHub issue is fixed, needs implementation, or can be closed by checking related work and current code. Use for issue completion/triage requests. Status questions are read-only; comment, close, or change labels only when the conversation authorizes that action.
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

Resolve the issue repository and implementation repository separately (for example, `rustfs/backlog` tracks work in `rustfs/rustfs`). Pass the implementation repository explicitly to PR queries; the current checkout may belong to another repository.

### 2. Search for related work

Search git history for commits referencing the issue:
```bash
git log --oneline --all --grep="<N>" | head -30
```

Search for related PRs:
```bash
gh pr list --repo <implementation-repo> --search "<issue-url>" --state all --json number,title,state,mergedAt
```

Also search qualified issue references and subject keywords; for same-repository
issues, include `#<N>`. Follow explicit links even without a text match. A search
page with no match does not prove the work is absent.

If the issue mentions specific PRs, check their status:
```bash
gh pr view <PR_N> --repo <implementation-repo> --json state,mergedAt,title,mergeCommit,baseRefName
```

### 3. Verify implementation

Fetch the implementation repository's current base branch. For each merged candidate, verify its merge commit is present and inspect the current code for the claimed behavior; a commit message match alone is not proof:
```bash
git fetch <implementation-remote> <base-branch>
git merge-base --is-ancestor <merge-commit> <implementation-remote>/<base-branch>
```

If the issue describes a specific defect, inspect the fetched base's code rather than assuming the current checkout contains it:
```bash
git show <implementation-remote>/<base-branch>:crates/<relevant>/src/<file>.rs
```

For issues with checklists, verify each item individually. If sub-items are tracked as separate issues, check those too:
```bash
gh issue view <SUB_N> --repo <owner/repo> --json state
```

### 4. Determine verdict

- **All items fixed and merged**: Recommend closing; name the verified PRs and behavior.
- **Some items fixed, some remaining**: Keep open; report each remaining item.
- **Not yet implemented**: Keep open; report what remains.
- **Superseded or no longer relevant**: Recommend closing with evidence.

### 5. Take action

For a status-only request, return the assessment without GitHub writes. If commenting, closing, or label edits are authorized, perform only those actions; do not ask again for authority already given. Prepare the final assessment before asking for any missing authority. Write `rustfs/backlog` issue content in Chinese.

Close with comment:
```bash
gh issue close <N> --repo <owner/repo> --comment "<body>"
```

Comment without closing:
```bash
gh issue comment <N> --repo <owner/repo> --body-file /tmp/triage.md
```

Update labels only when label changes are authorized, using existing repository labels; never add tool-specific labels:
```bash
gh issue edit <N> --repo <owner/repo> --add-label "<existing-label>"
```

Always use `--body-file` for multiline content, never inline `--body`.

### 6. Handle multi-issue batches

When the user asks to check multiple issues (e.g., "check all issues by user X" or "scan backlog for closable issues"):
1. List the full requested scope with pagination (for example `gh api --paginate 'repos/<repo>/issues?state=open&per_page=100'`, excluding entries with `pull_request`). Add an author filter only when the user requested one; the default page/limit is not evidence that all issues were checked.
2. For each issue, run steps 1-5 above.
3. Report a summary table of all triaged issues with verdicts.

## Report

Identify the issue and current state, verified implementation/PR evidence,
remaining items, verdict, and action actually taken. Use a table for batches;
a single issue does not require a heading for each field. Follow step 4's
verdicts without repeating the assessment in another template.

## Notes

- The user may ask in Chinese ("是否可以关闭", "检查完成情况"); respond in the same language.
- When closing, always include a summary of what was fixed and which PRs resolved it — this creates a useful audit trail.
- For issues in `rustfs/backlog`, use `--repo rustfs/backlog`.
- For issues in `rustfs/rustfs`, use `--repo rustfs/rustfs`.
- If the issue has sub-issues (GitHub sub-issues API), check each one's state before declaring the parent complete.
