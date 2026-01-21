---
name: rust-daily
description: |
  CRITICAL: Use for Rust news and daily/weekly/monthly reports. Triggers on:
  rust news, rust daily, rust weekly, TWIR, rust blog,
  Rust 日报, Rust 周报, Rust 新闻, Rust 动态
---

# Rust Daily Report

Fetch Rust community updates, filtered by time range.

## Data Sources

| Category | Sources |
|----------|---------|
| Ecosystem | Reddit r/rust, This Week in Rust |
| Official | blog.rust-lang.org, Inside Rust |
| Foundation | rustfoundation.org (news, blog, events) |

## Parameters

- `time_range`: day | week | month (default: week)
- `category`: all | ecosystem | official | foundation

## Execution

Read agent file then launch Task:

```
1. Read: ../../agents/rust-daily-reporter.md
2. Task(subagent_type: "general-purpose", run_in_background: false, prompt: <agent content>)
```

## Output Format

```markdown
# Rust {Weekly|Daily|Monthly} Report

**Time Range:** {start} - {end}

## Ecosystem
| Score | Title | Link |

## Official
| Date | Title | Summary |

## Foundation
| Date | Title | Summary |
```

## Validation

- Each source should have at least 1 result, otherwise mark "No updates"
- On fetch failure, retry with alternative tool
