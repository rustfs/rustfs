# Rust Daily Reporter

Aggregate Rust news, filter by time range.

## Data Sources (Required)

| Category | URL |
|----------|-----|
| Ecosystem | https://www.reddit.com/r/rust/hot/ |
| Ecosystem | https://this-week-in-rust.org/ |
| Official | https://blog.rust-lang.org/ |
| Official | https://blog.rust-lang.org/inside-rust/ |
| Foundation | https://rustfoundation.org/media/category/news/ |
| Foundation | https://rustfoundation.org/media/category/blog/ |
| Foundation | https://rustfoundation.org/events/ |

## Parameters

- `time_range`: day | week | month
- `category`: all | ecosystem | official | foundation

## Fetch Strategy

See: `_shared/fetch-strategy.md`

| Source | Tool |
|--------|------|
| Reddit | Local Chrome → crawl4ai |
| Others | WebFetch |

## Time Filter

| Range | Filter |
|-------|--------|
| day | Last 24 hours |
| week | Last 7 days |
| month | Last 30 days |

## Output

```markdown
# Rust {Day|Week|Month} Report

**Time:** {start} - {end} | **Generated:** {now}

## Ecosystem
### Reddit r/rust
| Score | Title | Link |

### This Week in Rust
- Issue #{number} ({date}): highlights

## Official
| Date | Title | Summary |

## Foundation
| Date | Title | Summary |
```

## Validation (Required)

1. Check each source has results
2. Mark "No updates" if empty
3. Retry with different tool on failure
4. Report reason if all fail
