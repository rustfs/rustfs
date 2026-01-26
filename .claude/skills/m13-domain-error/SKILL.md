---
name: m13-domain-error
description: "Use when designing domain error handling. Keywords: domain error, error categorization, recovery strategy, retry, fallback, domain error hierarchy, user-facing vs internal errors, error code design, circuit breaker, graceful degradation, resilience, error context, backoff, retry with backoff, error recovery, transient vs permanent error, 领域错误, 错误分类, 恢复策略, 重试, 熔断器, 优雅降级"
---

# Domain Error Strategy

> **Layer 2: Design Choices**

## Core Question

**Who needs to handle this error, and how should they recover?**

Before designing error types:
- Is this user-facing or internal?
- Is recovery possible?
- What context is needed for debugging?

---

## Error Categorization

| Error Type | Audience | Recovery | Example |
|------------|----------|----------|---------|
| User-facing | End users | Guide action | `InvalidEmail`, `NotFound` |
| Internal | Developers | Debug info | `DatabaseError`, `ParseError` |
| System | Ops/SRE | Monitor/alert | `ConnectionTimeout`, `RateLimited` |
| Transient | Automation | Retry | `NetworkError`, `ServiceUnavailable` |
| Permanent | Human | Investigate | `ConfigInvalid`, `DataCorrupted` |

---

## Thinking Prompt

Before designing error types:

1. **Who sees this error?**
   - End user → friendly message, actionable
   - Developer → detailed, debuggable
   - Ops → structured, alertable

2. **Can we recover?**
   - Transient → retry with backoff
   - Degradable → fallback value
   - Permanent → fail fast, alert

3. **What context is needed?**
   - Call chain → anyhow::Context
   - Request ID → structured logging
   - Input data → error payload

---

## Trace Up ↑

To domain constraints (Layer 3):

```
"How should I handle payment failures?"
    ↑ Ask: What are the business rules for retries?
    ↑ Check: domain-fintech (transaction requirements)
    ↑ Check: SLA (availability requirements)
```

| Question | Trace To | Ask |
|----------|----------|-----|
| Retry policy | domain-* | What's acceptable latency for retry? |
| User experience | domain-* | What message should users see? |
| Compliance | domain-* | What must be logged for audit? |

---

## Trace Down ↓

To implementation (Layer 1):

```
"Need typed errors"
    ↓ m06-error-handling: thiserror for library
    ↓ m04-zero-cost: Error enum design

"Need error context"
    ↓ m06-error-handling: anyhow::Context
    ↓ Logging: tracing with fields

"Need retry logic"
    ↓ m07-concurrency: async retry patterns
    ↓ Crates: tokio-retry, backoff
```

---

## Quick Reference

| Recovery Pattern | When | Implementation |
|------------------|------|----------------|
| Retry | Transient failures | exponential backoff |
| Fallback | Degraded mode | cached/default value |
| Circuit Breaker | Cascading failures | failsafe-rs |
| Timeout | Slow operations | `tokio::time::timeout` |
| Bulkhead | Isolation | separate thread pools |

## Error Hierarchy

```rust
#[derive(thiserror::Error, Debug)]
pub enum AppError {
    // User-facing
    #[error("Invalid input: {0}")]
    Validation(String),

    // Transient (retryable)
    #[error("Service temporarily unavailable")]
    ServiceUnavailable(#[source] reqwest::Error),

    // Internal (log details, show generic)
    #[error("Internal error")]
    Internal(#[source] anyhow::Error),
}

impl AppError {
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::ServiceUnavailable(_))
    }
}
```

## Retry Pattern

```rust
use tokio_retry::{Retry, strategy::ExponentialBackoff};

async fn with_retry<F, T, E>(f: F) -> Result<T, E>
where
    F: Fn() -> impl Future<Output = Result<T, E>>,
    E: std::fmt::Debug,
{
    let strategy = ExponentialBackoff::from_millis(100)
        .max_delay(Duration::from_secs(10))
        .take(5);

    Retry::spawn(strategy, || f()).await
}
```

---

## Common Mistakes

| Mistake | Why Wrong | Better |
|---------|-----------|--------|
| Same error for all | No actionability | Categorize by audience |
| Retry everything | Wasted resources | Only transient errors |
| Infinite retry | DoS self | Max attempts + backoff |
| Expose internal errors | Security risk | User-friendly messages |
| No context | Hard to debug | .context() everywhere |

---

## Anti-Patterns

| Anti-Pattern | Why Bad | Better |
|--------------|---------|--------|
| String errors | No structure | thiserror types |
| panic! for recoverable | Bad UX | Result with context |
| Ignore errors | Silent failures | Log or propagate |
| Box<dyn Error> everywhere | Lost type info | thiserror |
| Error in happy path | Performance | Early validation |

---

## Related Skills

| When | See |
|------|-----|
| Error handling basics | m06-error-handling |
| Retry implementation | m07-concurrency |
| Domain modeling | m09-domain |
| User-facing APIs | domain-* |
