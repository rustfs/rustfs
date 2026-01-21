# docs-cache

Documentation cache helper for agents.

## Cache Directory

```
~/.claude/cache/rust-docs/
├── docs.rs/{crate}/{item}.json
├── std/{module}/{item}.json
├── releases.rs/{version}.json
├── lib.rs/{crate}.json
└── clippy/{lint}.json
```

## TTL by Source

| Source | TTL | Reason |
|--------|-----|--------|
| std/ | 30 days | Stable |
| docs.rs/ | 7 days | Crate updates |
| releases.rs/ | 365 days | Historical |
| lib.rs/ | 1 day | Version changes |
| clippy/ | 14 days | Rust version updates |

## Cache Format

```json
{
  "meta": {
    "url": "...",
    "fetched_at": "2025-01-01T00:00:00Z",
    "expires_at": "2025-01-08T00:00:00Z"
  },
  "content": { ... }
}
```

## Skip Cache

Keywords: refresh, force, --force, update docs
