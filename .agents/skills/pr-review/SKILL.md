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
2. Apply the [risk policy](../../references/adversarial-validation.md), including its two perspectives for high-risk work. Ordinary reviews use the root finding standard directly. File count does not determine risk or authorize delegation; keep any delegated review within the authorized scope.
3. Check `gh pr checks <N> --repo <owner/repo>`. Investigate failures when relevant to a finding or a requested CI/readiness diagnosis. Distinguish pending, unavailable, pre-existing, flaky, and PR-caused states using evidence; a code review does not authorize unrelated repairs.
4. Report the reviewed SHAs, risk tier, supported findings or `No findings`, material verification limits, check state, and verdict. Set severity from evidenced impact, triggering conditions, reach, and urgency: P0 is a critical issue requiring immediate action with broad or unavoidable impact; P1 is a high-impact defect requiring prompt correction; P2 is an ordinary concrete correctness, compatibility, performance, or maintainability defect; P3 is an optional improvement only when requested. A remote trigger, crash, or deadlock does not by itself establish P0. P0/P1 findings block approval; evaluate other findings and verification gaps against the actual acceptance requirements rather than approving automatically below that threshold.
5. When posting is authorized, follow [Git and PR rules](../../references/pull-requests.md), recheck the remote base and head, and review any changed diff or integration assumptions before submitting. Reuse unaffected evidence. Use `gh pr review` with `--approve`, `--request-changes`, or `--comment` and `--body-file`; use [the API example](references/posting.md) for authorized inline comments. Reuse existing authorization.
6. Follow the repository PR lifecycle and explicit monitoring scope. Fetch new heads before comparing to the reviewed SHA; never rely on an unfetched `origin/pull/<N>/head`. Push fixes or resolve threads only when authorized; check `maintainerCanModify` before a fork push.
