---
name: pr-review
description: Review a GitHub PR using its exact base/head and repository risk policy. Use for requested PR reviews; publish or fix only within conversation authorization.
---
# PR Review

1. Resolve the PR repository and remote; the current checkout may belong elsewhere. Read the PR purpose and linked issues, then fetch the actual base and PR head:

   ```bash
   gh pr view <N> --repo <owner/repo> --json title,author,state,body,baseRefName,headRefName,baseRefOid,headRefOid
   git fetch <repo-remote> <baseRefName> refs/pull/<N>/head
   git diff <baseRefOid>...<headRefOid>
   ```

   Record both SHAs; refresh if either moved during fetching. Follow relevant [implementation boundaries](../../references/implementation.md).
2. Apply the [risk policy](../../references/adversarial-validation.md). Ordinary reviews use the root finding standard directly. For explicit adversarial, substantial, or high-risk reviews, use [the domain probes](../adversarial-validation/SKILL.md). File count does not authorize delegation; use exactly two independent reviewers only when authorized, otherwise two fresh sequential passes for that risk tier. Reviewers do not spawn further agents.
3. Check `gh pr checks <N> --repo <owner/repo>`. Investigate failures when relevant to a finding or a requested CI/readiness diagnosis. Distinguish pending, unavailable, pre-existing, flaky, and PR-caused states using evidence; a code review does not authorize unrelated repairs.
4. Report the reviewed SHAs, risk tier, supported findings or `No findings`, material verification limits, check state, and verdict. P0/P1 findings block approval. Severity: P0 = demonstrated data loss, security breach, remote crash, or deadlock; P1 = correctness, compatibility, or material hot-path regression; P2 = a concrete maintainability defect; P3 = optional style, only when requested.
5. When posting is authorized, follow [Git and PR rules](../../references/pull-requests.md), recheck the remote head, and review any delta before submitting. Use `gh pr review` with `--approve`, `--request-changes`, or `--comment` and `--body-file`; use [the API example](references/posting.md) for authorized inline comments. Reuse existing authorization.
6. Follow the repository PR lifecycle and explicit monitoring scope. Fetch new heads before comparing to the reviewed SHA; never rely on an unfetched `origin/pull/<N>/head`. Push fixes or resolve threads only when authorized; check `maintainerCanModify` before a fork push.
