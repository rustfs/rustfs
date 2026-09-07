# Inline PR Review Submission

Read only when an inline review is authorized. Recheck the PR head before posting and bind the review to the reviewed commit.

For inline comments on specific lines, use the GitHub API:
```bash
cat > /tmp/pr_review.json <<'EOF'
{
  "commit_id": "<reviewed-head-sha>",
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
