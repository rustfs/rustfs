# Rule Template

Use this template for all unsafe-checker rules.

---

```markdown
---
id: {prefix}-{number}
original_id: P.UNS.XXX.YY or G.UNS.XXX.YY
level: P|G
impact: CRITICAL|HIGH|MEDIUM
clippy: <clippy_lint_name> (if applicable)
---

# {Rule Title}

## Summary

One-sentence description of what this rule requires.

## Rationale

Why this rule matters for safety/soundness.

## Bad Example

```rust
// DON'T: Description of the anti-pattern
<code that violates the rule>
```

## Good Example

```rust
// DO: Description of the correct pattern
<code that follows the rule>
```

## Common Violations

1. Violation pattern 1
2. Violation pattern 2

## Checklist

- [ ] Check item 1
- [ ] Check item 2

## Related Rules

- `{other-rule-id}`: Brief description
```
