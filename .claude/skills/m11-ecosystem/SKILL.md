---
name: m11-ecosystem
description: "Use when integrating crates or ecosystem questions. Keywords: E0425, E0433, E0603, crate, cargo, dependency, feature flag, workspace, which crate to use, using external C libraries, creating Python extensions, PyO3, wasm, WebAssembly, bindgen, cbindgen, napi-rs, cannot find, private, crate recommendation, best crate for, Cargo.toml, features, crate 推荐, 依赖管理, 特性标志, 工作空间, Python 绑定"
---

# Ecosystem Integration

> **Layer 2: Design Choices**

## Core Question

**What's the right crate for this job, and how should it integrate?**

Before adding dependencies:
- Is there a standard solution?
- What's the maintenance status?
- What's the API stability?

---

## Integration Decision → Implementation

| Need | Choice | Crates |
|------|--------|--------|
| Serialization | Derive-based | serde, serde_json |
| Async runtime | tokio or async-std | tokio (most popular) |
| HTTP client | Ergonomic | reqwest |
| HTTP server | Modern | axum, actix-web |
| Database | SQL or ORM | sqlx, diesel |
| CLI parsing | Derive-based | clap |
| Error handling | App vs lib | anyhow, thiserror |
| Logging | Facade | tracing, log |

---

## Thinking Prompt

Before adding a dependency:

1. **Is it well-maintained?**
   - Recent commits?
   - Active issue response?
   - Breaking changes frequency?

2. **What's the scope?**
   - Do you need the full crate or just a feature?
   - Can feature flags reduce bloat?

3. **How does it integrate?**
   - Trait-based or concrete types?
   - Sync or async?
   - What bounds does it require?

---

## Trace Up ↑

To domain constraints (Layer 3):

```
"Which HTTP framework should I use?"
    ↑ Ask: What are the performance requirements?
    ↑ Check: domain-web (latency, throughput needs)
    ↑ Check: Team expertise (familiarity with framework)
```

| Question | Trace To | Ask |
|----------|----------|-----|
| Framework choice | domain-* | What constraints matter? |
| Library vs build | domain-* | What's the deployment model? |
| API design | domain-* | Who are the consumers? |

---

## Trace Down ↓

To implementation (Layer 1):

```
"Integrate external crate"
    ↓ m04-zero-cost: Trait bounds and generics
    ↓ m06-error-handling: Error type compatibility

"FFI integration"
    ↓ unsafe-checker: Safety requirements
    ↓ m12-lifecycle: Resource cleanup
```

---

## Quick Reference

### Language Interop

| Integration | Crate/Tool | Use Case |
|-------------|------------|----------|
| C/C++ → Rust | `bindgen` | Auto-generate bindings |
| Rust → C | `cbindgen` | Export C headers |
| Python ↔ Rust | `pyo3` | Python extensions |
| Node.js ↔ Rust | `napi-rs` | Node addons |
| WebAssembly | `wasm-bindgen` | Browser/WASI |

### Cargo Features

| Feature | Purpose |
|---------|---------|
| `[features]` | Optional functionality |
| `default = [...]` | Default features |
| `feature = "serde"` | Conditional deps |
| `[workspace]` | Multi-crate projects |

## Error Code Reference

| Error | Cause | Fix |
|-------|-------|-----|
| E0433 | Can't find crate | Add to Cargo.toml |
| E0603 | Private item | Check crate docs |
| Feature not enabled | Optional feature | Enable in `features` |
| Version conflict | Incompatible deps | `cargo update` or pin |
| Duplicate types | Different crate versions | Unify in workspace |

---

## Crate Selection Criteria

| Criterion | Good Sign | Warning Sign |
|-----------|-----------|--------------|
| Maintenance | Recent commits | Years inactive |
| Community | Active issues/PRs | No response |
| Documentation | Examples, API docs | Minimal docs |
| Stability | Semantic versioning | Frequent breaking |
| Dependencies | Minimal, well-known | Heavy, obscure |

---

## Anti-Patterns

| Anti-Pattern | Why Bad | Better |
|--------------|---------|--------|
| `extern crate` | Outdated (2018+) | Just `use` |
| `#[macro_use]` | Global pollution | Explicit import |
| Wildcard deps `*` | Unpredictable | Specific versions |
| Too many deps | Supply chain risk | Evaluate necessity |
| Vendoring everything | Maintenance burden | Trust crates.io |

---

## Related Skills

| When | See |
|------|-----|
| Error type design | m06-error-handling |
| Trait integration | m04-zero-cost |
| FFI safety | unsafe-checker |
| Resource management | m12-lifecycle |
