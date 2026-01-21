---
name: m12-lifecycle
description: "Use when designing resource lifecycles. Keywords: RAII, Drop, resource lifecycle, connection pool, lazy initialization, connection pool design, resource cleanup patterns, cleanup, scope, OnceCell, Lazy, once_cell, OnceLock, transaction, session management, when is Drop called, cleanup on error, guard pattern, scope guard, 资源生命周期, 连接池, 惰性初始化, 资源清理, RAII 模式"
---

# Resource Lifecycle

> **Layer 2: Design Choices**

## Core Question

**When should this resource be created, used, and cleaned up?**

Before implementing lifecycle:
- What's the resource's scope?
- Who owns the cleanup responsibility?
- What happens on error?

---

## Lifecycle Pattern → Implementation

| Pattern | When | Implementation |
|---------|------|----------------|
| RAII | Auto cleanup | `Drop` trait |
| Lazy init | Deferred creation | `OnceLock`, `LazyLock` |
| Pool | Reuse expensive resources | `r2d2`, `deadpool` |
| Guard | Scoped access | `MutexGuard` pattern |
| Scope | Transaction boundary | Custom struct + Drop |

---

## Thinking Prompt

Before designing lifecycle:

1. **What's the resource cost?**
   - Cheap → create per use
   - Expensive → pool or cache
   - Global → lazy singleton

2. **What's the scope?**
   - Function-local → stack allocation
   - Request-scoped → passed or extracted
   - Application-wide → static or Arc

3. **What about errors?**
   - Cleanup must happen → Drop
   - Cleanup is optional → explicit close
   - Cleanup can fail → Result from close

---

## Trace Up ↑

To domain constraints (Layer 3):

```
"How should I manage database connections?"
    ↑ Ask: What's the connection cost?
    ↑ Check: domain-* (latency requirements)
    ↑ Check: Infrastructure (connection limits)
```

| Question | Trace To | Ask |
|----------|----------|-----|
| Connection pooling | domain-* | What's acceptable latency? |
| Resource limits | domain-* | What are infra constraints? |
| Transaction scope | domain-* | What must be atomic? |

---

## Trace Down ↓

To implementation (Layer 1):

```
"Need automatic cleanup"
    ↓ m02-resource: Implement Drop
    ↓ m01-ownership: Clear owner for cleanup

"Need lazy initialization"
    ↓ m03-mutability: OnceLock for thread-safe
    ↓ m07-concurrency: LazyLock for sync

"Need connection pool"
    ↓ m07-concurrency: Thread-safe pool
    ↓ m02-resource: Arc for sharing
```

---

## Quick Reference

| Pattern | Type | Use Case |
|---------|------|----------|
| RAII | `Drop` trait | Auto cleanup on scope exit |
| Lazy Init | `OnceLock`, `LazyLock` | Deferred initialization |
| Pool | `r2d2`, `deadpool` | Connection reuse |
| Guard | `MutexGuard` | Scoped lock release |
| Scope | Custom struct | Transaction boundaries |

## Lifecycle Events

| Event | Rust Mechanism |
|-------|----------------|
| Creation | `new()`, `Default` |
| Lazy Init | `OnceLock::get_or_init` |
| Usage | `&self`, `&mut self` |
| Cleanup | `Drop::drop()` |

## Pattern Templates

### RAII Guard

```rust
struct FileGuard {
    path: PathBuf,
    _handle: File,
}

impl Drop for FileGuard {
    fn drop(&mut self) {
        // Cleanup: remove temp file
        let _ = std::fs::remove_file(&self.path);
    }
}
```

### Lazy Singleton

```rust
use std::sync::OnceLock;

static CONFIG: OnceLock<Config> = OnceLock::new();

fn get_config() -> &'static Config {
    CONFIG.get_or_init(|| {
        Config::load().expect("config required")
    })
}
```

---

## Common Errors

| Error | Cause | Fix |
|-------|-------|-----|
| Resource leak | Forgot Drop | Implement Drop or RAII wrapper |
| Double free | Manual memory | Let Rust handle |
| Use after drop | Dangling reference | Check lifetimes |
| E0509 move out of Drop | Moving owned field | `Option::take()` |
| Pool exhaustion | Not returned | Ensure Drop returns |

---

## Anti-Patterns

| Anti-Pattern | Why Bad | Better |
|--------------|---------|--------|
| Manual cleanup | Easy to forget | RAII/Drop |
| `lazy_static!` | External dep | `std::sync::OnceLock` |
| Global mutable state | Thread unsafety | `OnceLock` or proper sync |
| Forget to close | Resource leak | Drop impl |

---

## Related Skills

| When | See |
|------|-----|
| Smart pointers | m02-resource |
| Thread-safe init | m07-concurrency |
| Domain scopes | m09-domain |
| Error in cleanup | m06-error-handling |
