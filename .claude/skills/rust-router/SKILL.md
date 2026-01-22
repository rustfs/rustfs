---
name: rust-router
description: "CRITICAL: Use for ALL Rust questions including errors, design, and coding.
Triggers on: Rust, cargo, rustc, crate, Cargo.toml,
意图分析, 问题分析, 语义分析, analyze intent, question analysis,
compile error, borrow error, lifetime error, ownership error, type error, trait error,
value moved, cannot borrow, does not live long enough, mismatched types, not satisfied,
E0382, E0597, E0277, E0308, E0499, E0502, E0596,
async, await, Send, Sync, tokio, unsafe, FFI, concurrency, error handling,
编译错误, compile error, 所有权, ownership, 借用, borrow, 生命周期, lifetime, 类型错误, type error,
异步, async, 并发, concurrency, 错误处理, error handling,
问题, problem, question, 怎么用, how to use, 如何, how to, 为什么, why,
什么是, what is, 帮我写, help me write, 实现, implement, 解释, explain, 区别, difference, 最佳实践, best practice"
globs: ["**/Cargo.toml", "**/*.rs"]
---

# Rust Question Router

> **Version:** 2.0.0 | **Last Updated:** 2026-01-17
>
> **New in v2.0:** Meta-Cognition Routing - Three-layer cognitive model for deeper answers

## Meta-Cognition Framework

### Core Principle

**Don't answer directly. Trace through the cognitive layers first.**

```
Layer 3: Domain Constraints (WHY)
├── Business rules, regulatory requirements
├── domain-fintech, domain-web, domain-cli, etc.
└── "Why is it designed this way?"

Layer 2: Design Choices (WHAT)
├── Architecture patterns, DDD concepts
├── m09-m15 skills
└── "What pattern should I use?"

Layer 1: Language Mechanics (HOW)
├── Ownership, borrowing, lifetimes, traits
├── m01-m07 skills
└── "How do I implement this in Rust?"
```

### Routing by Entry Point

| User Signal | Entry Layer | Direction | First Skill |
|-------------|-------------|-----------|-------------|
| E0xxx error | Layer 1 | Trace UP ↑ | m01-m07 |
| Compile error | Layer 1 | Trace UP ↑ | Error table below |
| "How to design..." | Layer 2 | Check L3, then DOWN ↓ | m09-domain |
| "Building [domain] app" | Layer 3 | Trace DOWN ↓ | domain-* |
| "Best practice..." | Layer 2 | Both directions | m09-m15 |
| Performance issue | Layer 1 → 2 | UP then DOWN | m10-performance |

### CRITICAL: Dual-Skill Loading

**When domain keywords are present, you MUST load BOTH skills:**

| Domain Keywords | L1 Skill | L3 Skill |
|-----------------|----------|----------|
| Web API, HTTP, axum, handler | m07-concurrency | **domain-web** |
| 交易, 支付, trading, payment | m01-ownership | **domain-fintech** |
| CLI, terminal, clap | m07-concurrency | **domain-cli** |
| kubernetes, grpc, microservice | m07-concurrency | **domain-cloud-native** |
| embedded, no_std, MCU | m02-resource | **domain-embedded** |

**Example**: "Web API 报错 Rc cannot be sent"
- Load: m07-concurrency (L1 - Send/Sync error)
- Load: domain-web (L3 - Web state management)
- Answer must include both layers

### Trace Examples

```
User: "My trading system reports E0382"

1. Entry: Layer 1 (E0382 = ownership error)
2. Load: m01-ownership skill
3. Trace UP: What design led to this?
4. Check: domain-fintech (trading = immutable audit data)
5. Answer: Don't clone, use Arc<T> for shared immutable data
```

```
User: "How should I handle user authentication?"

1. Entry: Layer 2 (design question)
2. Trace UP to Layer 3: domain-web constraints
3. Load: domain-web skill (security, stateless HTTP)
4. Trace DOWN: m06-error-handling, m07-concurrency
5. Answer: JWT with proper error types, async handlers
```

---

## INSTRUCTIONS FOR CLAUDE

### Default Project Settings (When Creating Rust Code)

When creating new Rust projects or Cargo.toml files, ALWAYS use:

```toml
[package]
edition = "2024"  # ALWAYS use latest stable edition
rust-version = "1.85"

[lints.rust]
unsafe_code = "warn"

[lints.clippy]
all = "warn"
pedantic = "warn"
```

**Rules:**
- ALWAYS use `edition = "2024"` (not 2021 or earlier)
- Include `rust-version` for MSRV clarity
- Enable clippy lints by default
- DO NOT use WebSearch for Rust questions - use skills and agents

### Meta-Cognition Routing Process

1. **Identify Entry Layer**
   - E0xxx errors → Layer 1
   - Design questions → Layer 2
   - Domain-specific → Layer 3

2. **Load Appropriate Skill**
   - Read the skill file for context
   - Note the "Trace Up" and "Trace Down" sections

3. **Trace Through Layers**
   - Don't stop at surface-level fix
   - Ask "Why?" to trace up
   - Ask "How?" to trace down

4. **Answer with Context**
   - Include the reasoning chain
   - Reference which layers/skills informed the answer

### When User Requests Intent Analysis

User may say: "analyze this question" / "what type of problem is this" / "analyze intent"

**Execute these steps:**

1. **Extract Keywords** - Identify Rust concepts, error codes, crate names
2. **Identify Entry Layer** - Which cognitive layer is this?
3. **Map to Skills** - Which m0x/m1x/domain skills apply?
4. **Report Analysis** - Tell user the layers and suggested trace
5. **Invoke Skill** - Load and apply the matched skill

---

## Layer 1 Skills (Language Mechanics)

| Pattern | Category | Route To |
|---------|----------|----------|
| move, borrow, lifetime, E0382, E0597 | m01 | m01-ownership |
| Box, Rc, Arc, RefCell, Cell | m02 | m02-resource |
| mut, interior mutability, E0499, E0502, E0596 | m03 | m03-mutability |
| generic, trait, inline, monomorphization | m04 | m04-zero-cost |
| type state, phantom, newtype | m05 | m05-type-driven |
| Result, Error, panic, ?, anyhow, thiserror | m06 | m06-error-handling |
| Send, Sync, thread, async, channel | m07 | m07-concurrency |
| unsafe, FFI, extern, raw pointer, transmute | - | **unsafe-checker** |

## Layer 2 Skills (Design Choices)

| Pattern | Category | Route To |
|---------|----------|----------|
| domain model, business logic | m09 | m09-domain |
| performance, optimization, benchmark | m10 | m10-performance |
| integration, interop, bindings | m11 | m11-ecosystem |
| resource lifecycle, RAII, Drop | m12 | m12-lifecycle |
| domain error, recovery strategy | m13 | m13-domain-error |
| mental model, how to think | m14 | m14-mental-model |
| anti-pattern, common mistake, pitfall | m15 | m15-anti-pattern |

## Layer 3 Skills (Domain Constraints)

| Domain Keywords | Route To |
|-----------------|----------|
| fintech, trading, decimal, currency | domain-fintech |
| ml, tensor, model, inference | domain-ml |
| kubernetes, docker, grpc, microservice | domain-cloud-native |
| embedded, sensor, mqtt, iot | domain-iot |
| web server, HTTP, REST, axum, actix | domain-web |
| CLI, command line, clap, terminal | domain-cli |
| no_std, microcontroller, firmware | domain-embedded |

---

## Error Code Routing

| Error Code | Layer | Route To | Common Cause |
|------------|-------|----------|--------------|
| E0382 | L1 | m01-ownership | Use of moved value |
| E0597 | L1 | m01-ownership | Lifetime too short |
| E0506 | L1 | m01-ownership | Cannot assign to borrowed |
| E0507 | L1 | m01-ownership | Cannot move out of borrowed |
| E0515 | L1 | m01-ownership | Return local reference |
| E0716 | L1 | m01-ownership | Temporary value dropped |
| E0106 | L1 | m01-ownership | Missing lifetime specifier |
| E0596 | L1 | m03-mutability | Cannot borrow as mutable |
| E0499 | L1 | m03-mutability | Multiple mutable borrows |
| E0502 | L1 | m03-mutability | Borrow conflict |
| E0277 | L1 | m04/m07 | Trait bound not satisfied |
| E0308 | L1 | m04-zero-cost | Type mismatch |
| E0599 | L1 | m04-zero-cost | No method found |
| E0038 | L1 | m04-zero-cost | Trait not object-safe |
| E0433 | L1 | m11-ecosystem | Cannot find crate/module |

---

## Unsafe-Specific Routing

For **detailed unsafe rules**, route to `unsafe-checker` skill:

| Pattern | Route To |
|---------|----------|
| unsafe code review, SAFETY comment | **unsafe-checker** |
| FFI, extern "C", C interop, libc | **unsafe-checker** |
| raw pointer, *mut, *const, NonNull | **unsafe-checker** |
| transmute, union, repr(C) | **unsafe-checker** |
| MaybeUninit, uninitialized memory | **unsafe-checker** |
| panic safety, double-free | **unsafe-checker** |
| Send impl, Sync impl, manual auto-traits | **unsafe-checker** |

---

## Functional Routing Table

| Pattern | Route To | Action |
|---------|----------|--------|
| latest version, what's new | **rust-learner** | Use agents |
| API, docs, documentation | **docs-researcher** | Use agent |
| Cargo.toml, dependencies | **dynamic-skills** | Suggest `/sync-crate-skills` |
| code style, naming, clippy | **coding-guidelines** | Read skill |
| unsafe code, FFI | **unsafe-checker** | Read skill |
| code review | **os-checker** | Suggest `/rust-review` |

---

## Priority Order

1. **Identify cognitive layer** (L1/L2/L3)
2. **Load entry skill** (m0x/m1x/domain)
3. **Trace through layers** (UP or DOWN)
4. **Cross-reference skills** as indicated in "Trace" sections
5. **Answer with reasoning chain**

## Skill File Paths

### Meta-Cognition Framework
```
_meta/reasoning-framework.md    # How to trace through layers
_meta/layer-definitions.md      # Layer definitions
_meta/externalization.md        # Cognitive externalization
_meta/error-protocol.md         # 3-Strike escalation rule
_meta/hooks-patterns.md         # Automatic triggers
```

### Layer 1 Skills (Language Mechanics)
```
skills/m01-ownership/SKILL.md
skills/m02-resource/SKILL.md
skills/m03-mutability/SKILL.md
skills/m04-zero-cost/SKILL.md
skills/m05-type-driven/SKILL.md
skills/m06-error-handling/SKILL.md
skills/m07-concurrency/SKILL.md
```

### Layer 2 Skills (Design Choices)
```
skills/m09-domain/SKILL.md
skills/m10-performance/SKILL.md
skills/m11-ecosystem/SKILL.md
skills/m12-lifecycle/SKILL.md
skills/m13-domain-error/SKILL.md
skills/m14-mental-model/SKILL.md
skills/m15-anti-pattern/SKILL.md
```

### Layer 3 Skills (Domain Constraints)
```
skills/domain-fintech/SKILL.md
skills/domain-ml/SKILL.md
skills/domain-cloud-native/SKILL.md
skills/domain-iot/SKILL.md
skills/domain-web/SKILL.md
skills/domain-cli/SKILL.md
skills/domain-embedded/SKILL.md
```

---

## OS-Checker Integration

For code review and security auditing:

| Use Case | Command | Tools |
|----------|---------|-------|
| Daily check | `/rust-review` | clippy |
| Security audit | `/audit security` | cargo audit, geiger |
| Unsafe audit | `/audit safety` | miri, rudra |
| Concurrency audit | `/audit concurrency` | lockbud |
| Full audit | `/audit full` | all os-checker tools |

---

## Workflow Example with Meta-Cognition

```
User: "Why am I getting E0382 in my trading system?"

Analysis:
1. Entry: Layer 1 (E0382 = ownership/move error)
2. Load: m01-ownership skill
3. Context: "trading system" → domain-fintech

Trace UP ↑:
- E0382 in trading context
- Check domain-fintech: "immutable audit records"
- Finding: Trading data should be shared, not moved

Response:
"E0382 indicates a value was moved when still needed.
In a trading system (domain-fintech), transaction records
should be immutable and shareable for audit purposes.

Instead of cloning, consider:
- Arc<TradeRecord> for shared immutable access
- This aligns with financial audit requirements

See: m01-ownership (Trace Up section),
     domain-fintech (Audit Requirements)"
```
