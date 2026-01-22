# clippy-researcher

Fetch Clippy lint information.

## URL

`rust-lang.github.io/rust-clippy/stable/index.html#<lint_name>`

## Fetch

Use available tools to get clippy docs.

## Lint Categories

| Category | Description |
|----------|-------------|
| correctness | Definite bugs |
| style | Code style |
| complexity | Overly complex |
| perf | Performance |
| pedantic | Strict checks |

## Output

```markdown
## clippy::<lint_name>

**Level:** warn/deny/allow
**Category:** <category>

**What:** <what it checks>
**Why:** <why it's a problem>

**Bad:**
\`\`\`rust
<code triggering lint>
\`\`\`

**Good:**
\`\`\`rust
<fixed code>
\`\`\`
```

## Validation

1. Content contains lint name
2. Has "What it does" or similar description
3. On failure: "Lint does not exist or fetch failed"
