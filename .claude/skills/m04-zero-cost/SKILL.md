---
name: m04-zero-cost
description: "CRITICAL: Use for generics, traits, zero-cost abstraction. Triggers: E0277, E0308, E0599, generic, trait, impl, dyn, where, monomorphization, static dispatch, dynamic dispatch, impl Trait, trait bound not satisfied, 泛型, 特征, 零成本抽象, 单态化"
---

# Zero-Cost Abstraction

> **Layer 1: Language Mechanics**

## Core Question

**Do we need compile-time or runtime polymorphism?**

Before choosing between generics and trait objects:
- Is the type known at compile time?
- Is a heterogeneous collection needed?
- What's the performance priority?

---

## Error → Design Question

| Error | Don't Just Say | Ask Instead |
|-------|----------------|-------------|
| E0277 | "Add trait bound" | Is this abstraction at the right level? |
| E0308 | "Fix the type" | Should types be unified or distinct? |
| E0599 | "Import the trait" | Is the trait the right abstraction? |
| E0038 | "Make object-safe" | Do we really need dynamic dispatch? |

---

## Thinking Prompt

Before adding trait bounds:

1. **What abstraction is needed?**
   - Same behavior, different types → trait
   - Different behavior, same type → enum
   - No abstraction needed → concrete type

2. **When is type known?**
   - Compile time → generics (static dispatch)
   - Runtime → trait objects (dynamic dispatch)

3. **What's the trade-off priority?**
   - Performance → generics
   - Compile time → trait objects
   - Flexibility → depends

---

## Trace Up ↑

When type system fights back:

```
E0277 (trait bound not satisfied)
    ↑ Ask: Is the abstraction level correct?
    ↑ Check: m09-domain (what behavior is being abstracted?)
    ↑ Check: m05-type-driven (should use newtype?)
```

| Persistent Error | Trace To | Question |
|-----------------|----------|----------|
| Complex trait bounds | m09-domain | Is the abstraction right? |
| Object safety issues | m05-type-driven | Can typestate help? |
| Type explosion | m10-performance | Accept dyn overhead? |

---

## Trace Down ↓

From design to implementation:

```
"Need to abstract over types with same behavior"
    ↓ Types known at compile time → impl Trait or generics
    ↓ Types determined at runtime → dyn Trait

"Need collection of different types"
    ↓ Closed set → enum
    ↓ Open set → Vec<Box<dyn Trait>>

"Need to return different types"
    ↓ Same type → impl Trait
    ↓ Different types → Box<dyn Trait>
```

---

## Quick Reference

| Pattern | Dispatch | Code Size | Runtime Cost |
|---------|----------|-----------|--------------|
| `fn foo<T: Trait>()` | Static | +bloat | Zero |
| `fn foo(x: &dyn Trait)` | Dynamic | Minimal | vtable lookup |
| `impl Trait` return | Static | +bloat | Zero |
| `Box<dyn Trait>` | Dynamic | Minimal | Allocation + vtable |

## Syntax Comparison

```rust
// Static dispatch - type known at compile time
fn process(x: impl Display) { }      // argument position
fn process<T: Display>(x: T) { }     // explicit generic
fn get() -> impl Display { }         // return position

// Dynamic dispatch - type determined at runtime
fn process(x: &dyn Display) { }      // reference
fn process(x: Box<dyn Display>) { }  // owned
```

## Error Code Reference

| Error | Cause | Quick Fix |
|-------|-------|-----------|
| E0277 | Type doesn't impl trait | Add impl or change bound |
| E0308 | Type mismatch | Check generic params |
| E0599 | No method found | Import trait with `use` |
| E0038 | Trait not object-safe | Use generics or redesign |

---

## Decision Guide

| Scenario | Choose | Why |
|----------|--------|-----|
| Performance critical | Generics | Zero runtime cost |
| Heterogeneous collection | `dyn Trait` | Different types at runtime |
| Plugin architecture | `dyn Trait` | Unknown types at compile |
| Reduce compile time | `dyn Trait` | Less monomorphization |
| Small, known type set | `enum` | No indirection |

---

## Object Safety

A trait is object-safe if it:
- Doesn't have `Self: Sized` bound
- Doesn't return `Self`
- Doesn't have generic methods
- Uses `where Self: Sized` for non-object-safe methods

---

## Anti-Patterns

| Anti-Pattern | Why Bad | Better |
|--------------|---------|--------|
| Over-generic everything | Compile time, complexity | Concrete types when possible |
| `dyn` for known types | Unnecessary indirection | Generics |
| Complex trait hierarchies | Hard to understand | Simpler design |
| Ignore object safety | Limits flexibility | Plan for dyn if needed |

---

## Related Skills

| When | See |
|------|-----|
| Type-driven design | m05-type-driven |
| Domain abstraction | m09-domain |
| Performance concerns | m10-performance |
| Send/Sync bounds | m07-concurrency |
