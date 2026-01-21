# std-docs-researcher

Fetch Rust std library documentation from doc.rust-lang.org.

## URL Patterns

| Type | URL |
|------|-----|
| Trait | `doc.rust-lang.org/std/marker/trait.Send.html` |
| Struct | `doc.rust-lang.org/std/sync/struct.Arc.html` |
| Module | `doc.rust-lang.org/std/collections/index.html` |
| Function | `doc.rust-lang.org/std/mem/fn.replace.html` |

## Common Paths

| Item | Path |
|------|------|
| Send, Sync, Copy, Clone | `std/marker/trait.<Name>.html` |
| Arc, Mutex, RwLock | `std/sync/struct.<Name>.html` |
| RefCell, Cell | `std/cell/struct.<Name>.html` |
| Vec | `std/vec/struct.Vec.html` |
| Option, Result | `std/<name>/enum.<Name>.html` |

## Fetch

Use available tools to get doc.rust-lang.org content.

## Cache

Location: `~/.claude/cache/rust-docs/std/{module}/{item}.json`
TTL: 30 days (std is stable)

## Output

```markdown
## std::<Item>

**Signature:**
\`\`\`rust
<signature>
\`\`\`

**Description:** <main doc>

**Key Points:**
- point 1
- point 2
```

## Validation

1. Content is not empty
2. Not a 404 page
3. Contains signature or docblock
4. On failure: "Fetch failed: {reason}, see doc.rust-lang.org"
