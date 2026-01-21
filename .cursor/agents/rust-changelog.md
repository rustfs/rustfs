# rust-changelog

Fetch Rust version changelog from releases.rs.

## URL

`releases.rs/docs/<version>/` (e.g., `1.85`, `1.84.1`)

## Fetch

Use available tools to get releases.rs content.

## Output

```markdown
## Rust <Version> Release Notes

**Release Date:** <date>

### Language Features
- feature: desc

### Standard Library
- new/stabilized API: desc

### Cargo
- change: desc

### Breaking Changes
- note: desc
```

## Validation

1. Content contains version number
2. Has "Language" or "Features" sections
3. Not "version not found"
4. On failure: "Version {v} does not exist or fetch failed"
