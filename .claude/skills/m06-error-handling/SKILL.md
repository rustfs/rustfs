---
name: m06-error-handling
description: "CRITICAL: Use for error handling. Triggers: Result, Option, Error, ?, unwrap, expect, panic, anyhow, thiserror, when to panic vs return Result, custom error, error propagation, 错误处理, Result 用法, 什么时候用 panic"
---

# Error Handling

> **Layer 1: Language Mechanics**

## Core Question

**Is this failure expected or a bug?**

Before choosing error handling strategy:
- Can this fail in normal operation?
- Who should handle this failure?
- What context does the caller need?

---

## Error → Design Question

| Pattern | Don't Just Say | Ask Instead |
|---------|----------------|-------------|
| unwrap panics | "Use ?" | Is None/Err actually possible here? |
| Type mismatch on ? | "Use anyhow" | Are error types designed correctly? |
| Lost error context | "Add .context()" | What does the caller need to know? |
| Too many error variants | "Use Box<dyn Error>" | Is error granularity right? |

---

## Thinking Prompt

Before handling an error:

1. **What kind of failure is this?**
   - Expected → Result<T, E>
   - Absence normal → Option<T>
   - Bug/invariant → panic!
   - Unrecoverable → panic!

2. **Who handles this?**
   - Caller → propagate with ?
   - Current function → match/if-let
   - User → friendly error message
   - Programmer → panic with message

3. **What context is needed?**
   - Type of error → thiserror variants
   - Call chain → anyhow::Context
   - Debug info → anyhow or tracing

---

## Trace Up ↑

When error strategy is unclear:

```
"Should I return Result or Option?"
    ↑ Ask: Is absence/failure normal or exceptional?
    ↑ Check: m09-domain (what does domain say?)
    ↑ Check: domain-* (error handling requirements)
```

| Situation | Trace To | Question |
|-----------|----------|----------|
| Too many unwraps | m09-domain | Is the data model right? |
| Error context design | m13-domain-error | What recovery is needed? |
| Library vs app errors | m11-ecosystem | Who are the consumers? |

---

## Trace Down ↓

From design to implementation:

```
"Expected failure, library code"
    ↓ Use: thiserror for typed errors

"Expected failure, application code"
    ↓ Use: anyhow for ergonomic errors

"Absence is normal (find, get, lookup)"
    ↓ Use: Option<T>

"Bug or invariant violation"
    ↓ Use: panic!, assert!, unreachable!

"Need to propagate with context"
    ↓ Use: .context("what was happening")
```

---

## Quick Reference

| Pattern | When | Example |
|---------|------|---------|
| `Result<T, E>` | Recoverable error | `fn read() -> Result<String, io::Error>` |
| `Option<T>` | Absence is normal | `fn find() -> Option<&Item>` |
| `?` | Propagate error | `let data = file.read()?;` |
| `unwrap()` | Dev/test only | `config.get("key").unwrap()` |
| `expect()` | Invariant holds | `env.get("HOME").expect("HOME set")` |
| `panic!` | Unrecoverable | `panic!("critical failure")` |

## Library vs Application

| Context | Error Crate | Why |
|---------|-------------|-----|
| Library | `thiserror` | Typed errors for consumers |
| Application | `anyhow` | Ergonomic error handling |
| Mixed | Both | thiserror at boundaries, anyhow internally |

## Decision Flowchart

```
Is failure expected?
├─ Yes → Is absence the only "failure"?
│        ├─ Yes → Option<T>
│        └─ No → Result<T, E>
│                 ├─ Library → thiserror
│                 └─ Application → anyhow
└─ No → Is it a bug?
        ├─ Yes → panic!, assert!
        └─ No → Consider if really unrecoverable

Use ? → Need context?
├─ Yes → .context("message")
└─ No → Plain ?
```

---

## Common Errors

| Error | Cause | Fix |
|-------|-------|-----|
| `unwrap()` panic | Unhandled None/Err | Use `?` or match |
| Type mismatch | Different error types | Use `anyhow` or `From` |
| Lost context | `?` without context | Add `.context()` |
| `cannot use ?` | Missing Result return | Return `Result<(), E>` |

---

## Anti-Patterns

| Anti-Pattern | Why Bad | Better |
|--------------|---------|--------|
| `.unwrap()` everywhere | Panics in production | `.expect("reason")` or `?` |
| Ignore errors silently | Bugs hidden | Handle or propagate |
| `panic!` for expected errors | Bad UX, no recovery | Result |
| Box<dyn Error> everywhere | Lost type info | thiserror |

---

## Related Skills

| When | See |
|------|-----|
| Domain error strategy | m13-domain-error |
| Crate boundaries | m11-ecosystem |
| Type-safe errors | m05-type-driven |
| Mental models | m14-mental-model |
