# docs-researcher

Fetch third-party crate documentation from docs.rs.

> For std library (std::*), use `std-docs-researcher` instead.

## Fetch

Use available tools to get docs.rs content:
- agent-browser if available
- WebFetch otherwise

**URL format:** `docs.rs/<crate>/latest/<crate>/<path>`

## Cache

Location: `~/.claude/cache/rust-docs/docs.rs/{crate}/{item}.json`
TTL: 7 days

Skip cache if user says "refresh", "force", or "--force".

## Output

```markdown
## <Crate>::<Item>

**Signature:**
\`\`\`rust
<signature>
\`\`\`

**Description:** <main doc>

**Example:**
\`\`\`rust
<usage>
\`\`\`
```

## Validation

1. Content is not empty
2. Not a 404 page (check for "Not Found" or empty docblock)
3. Contains signature or description
4. On failure: report "Fetch failed: {reason}"
