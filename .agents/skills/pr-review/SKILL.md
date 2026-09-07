---
name: pr-review
description: Review a GitHub PR from a URL or number using its actual base/head and risk-appropriate code review. Use when the user asks for a PR review, not a status lookup or PR wording edit. Publish a review only when authorized; delegation and monitoring follow the requested scope and root AGENTS.md.
---

# PR Review

Use this skill for PR context and review delivery. An ordinary review request is read-only unless the conversation also authorizes posting or fixes. Reuse that authorization without asking again; prepare the review before requesting any missing publication approval.

## Prerequisites

- Follow root `AGENTS.md`; classify risk with the [review policy](../../references/adversarial-validation.md) and consult relevant [change-style and boundary rules](../../references/implementation.md).
- Select `code-change-verification` for ordinary review or `adversarial-validation` for explicitly adversarial, substantial, or high-risk review; do not run both on the same diff.

## Workflow

### 1. Gather PR context

```bash
gh pr view <N> --repo <owner/repo> --json title,author,state,body,additions,deletions,changedFiles,commits,baseRefName,headRefName,baseRefOid,headRefOid
gh pr diff <N> --repo <owner/repo> --name-only
```

Read the PR body and linked issues to understand the change's purpose. If the PR references an issue, fetch that too:
```bash
gh issue view <ISSUE> --repo <issue-owner/repo> --json title,body,state
```

### 2. Fetch the diff and classify the change

```bash
git fetch <repo-remote> <baseRefName> refs/pull/<N>/head
git diff <baseRefOid>...<headRefOid> --stat
```

Resolve `<repo-remote>` to the PR repository; do not assume the current checkout's `origin` or `main` matches. Record the exact base/head used. If either moved during fetching, refresh the snapshot before reviewing. Classify using the repository review policy; instruction changes that affect agent execution are mechanical, not exempt.

### 3. Review the changed behavior

Group files by functional area to trace callers and invariants. Use the root risk tier's review shape and only matching lenses. File count does not authorize delegation. When delegation is explicitly authorized, high-risk/substantial reviews use exactly two independent reviewers with the applicable lenses split between them; otherwise use two fresh sequential passes. Reviewers do not spawn further agents.

Findings need a concrete failure scenario with `file:line`; a null verdict briefly names the relevant probes. Reuse existing evidence and choose local checks from the final diff under the root verification policy.

### 4. Check CI status

```bash
gh pr checks <N> --repo <owner/repo>
```

Investigate a failed check when it bears on a finding or the user requested CI diagnosis/merge readiness:
```bash
gh run view --repo <owner/repo> --log-failed --job=<JOB_ID>
```

Use current evidence to distinguish pre-existing, flaky, and PR-caused failures. Do not classify them by guesswork or turn a code-only review into unrelated CI repair.

### 5. Synthesize findings

Report the PR, reviewed base/head, and risk tier, then summarize the assessment.
Use the selected review's P0–P3 ratings and root finding standard: supported
findings with `file:line`, failure scenario, and fix, or `No findings`.
State the observed check status, including pending or unavailable checks, and
the verdict (`APPROVE`, `REQUEST_CHANGES`, or `COMMENT`). Do not infer a pass
from missing checks or add style nits to populate a clean review.

### 6. Post the review

Only when posting is authorized, write the review body to a temp file and post via CLI. Refresh the PR head first; if it changed, review the delta and update the verdict before posting:
```bash
# Request changes
gh pr review <N> --repo <owner/repo> --request-changes --body-file /tmp/pr_review.md

# Approve
gh pr review <N> --repo <owner/repo> --approve --body-file /tmp/pr_review.md

# Comment only (no verdict)
gh pr review <N> --repo <owner/repo> --comment --body-file /tmp/pr_review.md
```

For authorized inline comments, use [the submission example](references/posting.md).

Always use `--body-file` or `--input`, never inline multiline `--body`.

### 7. Handle follow-up

Follow the [PR lifecycle](../../references/pull-requests.md) and any explicit monitoring request. For follow-up, fetch the new head and compare the recorded reviewed SHA with the new SHA; revisit affected callers and findings. Never use an unfetched `origin/pull/<N>/head` ref as evidence. Update the posted review or resolve addressed threads only within existing authorization.

## Notes

- The user may ask for review in Chinese; respond in the same language but keep the review body in English per AGENTS.md rules.
- When the user asks for "多角色对抗 review", run the full adversarial validation protocol — this skill's step 3 covers that.
- If the PR is from a fork, check `maintainerCanModify` before attempting to push fixes.
- For very large PRs, batch the review by functional area while keeping the same bounded review shape.
