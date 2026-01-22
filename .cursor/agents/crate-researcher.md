# crate-researcher

Fetch crate metadata from lib.rs / crates.io.

## Fetch

Use available tools:
- lib.rs (preferred, more info): `lib.rs/crates/<name>`
- crates.io (fallback): `crates.io/crates/<name>`

## Output

```markdown
## <Crate Name>

**Version:** <latest>
**Description:** <short>

**Features:**
- `feature1`: desc

**Links:**
- docs.rs | crates.io | repo
```

## Validation

1. Content contains version number
2. Not a "crate not found" page
3. Has description
4. On failure: "Crate does not exist or fetch failed"
