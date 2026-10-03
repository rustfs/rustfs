---
name: issue-triage
description: Verify issue completion against current implementation and merged work. Use for issue triage; comment, close, or change labels only when authorized.
---
# Issue Triage

- Resolve the issue repository separately from the implementation repository: `rustfs/backlog` tracks work in `rustfs/rustfs`. Pass `--repo` explicitly to GitHub queries.
- Read the issue body, relevant comments, linked PRs, checklist, and sub-issues with `gh issue view`. Search explicit links, the full issue URL, qualified references, and subject keywords; an empty search result does not prove implementation is absent.
- Fetch the implementation base and inspect its current code. For each merged candidate, verify `git merge-base --is-ancestor <merge-commit> <remote>/<base>`; a title, commit message, or merged state alone is insufficient. A local checkout may be stale or on another branch.
- Verify every checklist/sub-issue before declaring completion. For batches, paginate the entire requested issue scope, exclude PR entries, and filter by author only when requested.
- Recommend closing only when all requested behavior is present or evidence shows the issue is superseded. Otherwise name what remains. A status request is read-only; reuse existing authority for comments/closing/labels, and follow [Git and PR rules](../../references/pull-requests.md) before posting. Use Chinese for `rustfs/backlog`, existing repository labels only, and a closing comment naming the verified behavior and PRs.

Report the issue, current state, implementation evidence, remaining work, verdict, and action actually taken. Use a table for batches. Write multiline comments via `--body-file`; when closing, post the prepared comment before closing without an inline multiline body.
