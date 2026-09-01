---
name: pr-review
description: Review a GitHub PR end-to-end from a URL or number — fetch metadata, inspect the diff, run multi-role adversarial review, check CI status, and post the review comment. Use when the user provides a PR link and asks to review it.
---

# PR Review

Use this skill when the user provides a GitHub PR URL or number and asks to review it. This covers the full review lifecycle: data gathering, code review, CI verification, and posting the result.

## Prerequisites

- Read `AGENTS.md` for the repository's adversarial validation policy and change-style rules.
- The `adversarial-validation` skill handles the review role playbooks; this skill orchestrates the workflow around it.

## Workflow

### 1. Gather PR context

```bash
gh pr view <N> --json title,author,state,body,additions,deletions,changedFiles,commits,baseRefName,headRefName
gh pr diff <N> --name-only
```

Read the PR body and linked issues to understand the change's purpose. If the PR references an issue, fetch that too:
```bash
gh issue view <ISSUE> --json title,body,state
```

### 2. Fetch the diff and classify the change

```bash
git fetch origin pull/<N>/head:pr-<N>
git diff main...pr-<N> --stat
```

Classify the change by risk tier (per AGENTS.md):
- **Exempt**: docs/comments/instruction-only, formatting, typos.
- **Mechanical**: renames, file moves, test-only or tooling changes.
- **Standard** (default): any behavior change.
- **High risk**: locking, erasure coding, quorum/heal, replication, multipart, RPC, lifecycle/tiering, metadata formats, persistence/fsync, IAM/KMS/auth, on-disk/on-wire formats, S3 API-visible behavior.

### 3. Cluster changed files and delegate review

Group the changed files into logical clusters (by crate or functional area). For each cluster, spawn a subagent with a focused review prompt that includes:
- The cluster's changed files and their diffs.
- The applicable adversarial role probes (from the `adversarial-validation` skill).
- The repository's AGENTS.md rules relevant to that domain.

For standard-tier changes: correctness adversary + simplicity adversary + test-coverage skeptic, plus every role whose domain the diff touches.
For high-risk changes: run all seven roles.

Each subagent must produce findings (concrete failure scenario with file:line) or a null report ("attacked X, Y, Z — no break found").

### 4. Check CI status

```bash
gh pr checks <N>
```

If any checks fail, investigate:
```bash
gh run view --log-failed --job=<JOB_ID>
```

Determine whether failures are pre-existing (on main), flaky, or caused by the PR.

### 5. Synthesize findings

Combine all subagent findings into a structured review:
- **Summary**: one-paragraph overview of the change and overall assessment.
- **Findings**: each finding with severity (critical/major/minor/nit), file:line, concrete failure scenario, and suggested fix.
- **CI status**: pass/fail with notes on any failures.
- **Verdict**: APPROVE, REQUEST_CHANGES, or COMMENT.

### 6. Post the review

Write the review body to a temp file and post via CLI:
```bash
# Request changes
gh pr review <N> --request-changes --body-file /tmp/pr_review.md

# Approve
gh pr review <N> --approve --body-file /tmp/pr_review.md

# Comment only (no verdict)
gh pr review <N> --comment --body-file /tmp/pr_review.md
```

For inline comments on specific lines, use the GitHub API:
```bash
cat > /tmp/pr_review.json <<'EOF'
{
  "body": "review body",
  "event": "REQUEST_CHANGES",
  "comments": [
    {
      "path": "crates/foo/src/bar.rs",
      "line": 42,
      "body": "finding description"
    }
  ]
}
EOF
gh api --method POST /repos/{owner}/{repo}/pulls/<N>/reviews --input /tmp/pr_review.json
```

Always use `--body-file` or `--input`, never inline multiline `--body`.

### 7. Handle follow-up

If the review requests changes:
- Monitor for new commits: `gh pr view <N> --json commits`
- Re-review changed files only: `git diff pr-<N>..origin/pull/<N>/head`
- Update the review when findings are addressed.

If CI was failing due to pre-existing main breakage:
- Comment on the PR noting the failure is pre-existing.
- Suggest updating the branch: `gh pr update-branch <N>`

## Output format

### PR Review: #<N> — <title>

**Author**: <author>
**Risk tier**: exempt | mechanical | standard | high-risk
**Changed files**: <count> across <cluster count> clusters

#### Summary
<one-paragraph overview>

#### Findings
| Severity | Location | Finding |
|----------|----------|---------|
| critical | file:line | concrete failure scenario |

#### CI Status
- All checks pass / Failing: <details>

#### Verdict
APPROVE / REQUEST_CHANGES / COMMENT

## Notes

- The user may ask for review in Chinese; respond in the same language but keep the review body in English per AGENTS.md rules.
- When the user asks for "多角色对抗 review", run the full adversarial validation protocol — this skill's step 3 covers that.
- If the PR is from a fork, check `maintainerCanModify` before attempting to push fixes.
- For very large PRs (>50 files), cluster aggressively and delegate in parallel to keep review time reasonable.
