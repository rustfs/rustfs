# Checklist: Reviewing Unsafe Code

Use this checklist when reviewing code containing `unsafe`.

## 1. Surface-Level Checks

- [ ] Does every `unsafe` block have a `// SAFETY:` comment?
- [ ] Does every `unsafe fn` have `# Safety` documentation?
- [ ] Are the safety comments specific and verifiable, not vague?
- [ ] Is the unsafe code minimized (smallest possible unsafe block)?

## 2. Pointer Validity

For each pointer dereference:

- [ ] **Non-null**: Is null checked before dereference?
- [ ] **Aligned**: Is alignment verified or guaranteed by construction?
- [ ] **Valid**: Does the pointer point to allocated memory?
- [ ] **Initialized**: Is the memory initialized before reading?
- [ ] **Lifetime**: Is the memory valid for the entire use duration?
- [ ] **Unique**: For `&mut`, is there only one mutable reference?

## 3. Memory Safety

- [ ] **No aliasing**: Are `&` and `&mut` never created to the same memory simultaneously?
- [ ] **No use-after-free**: Is memory not accessed after deallocation?
- [ ] **No double-free**: Is memory freed exactly once?
- [ ] **No data races**: Is concurrent access properly synchronized?
- [ ] **Bounds checked**: Are array/slice accesses in bounds?

## 4. Type Safety

- [ ] **Transmute**: Are transmuted types actually compatible?
- [ ] **Repr**: Do FFI types have `#[repr(C)]`?
- [ ] **Enum values**: Are enum discriminants validated from external sources?
- [ ] **Unions**: Is the correct union field accessed?

## 5. Panic Safety

- [ ] What state is the program in if this code panics?
- [ ] Are partially constructed objects properly cleaned up?
- [ ] Do Drop implementations see valid state?
- [ ] Is there a panic guard if needed?

## 6. FFI-Specific Checks

- [ ] **Types**: Do Rust types match C types exactly?
- [ ] **Strings**: Are strings properly null-terminated?
- [ ] **Ownership**: Is it clear who owns/frees memory?
- [ ] **Thread safety**: Are callbacks thread-safe?
- [ ] **Panic boundary**: Are panics caught before crossing FFI?
- [ ] **Error handling**: Are C-style errors properly handled?

## 7. Concurrency Checks

- [ ] **Send/Sync**: Are manual implementations actually sound?
- [ ] **Atomics**: Are memory orderings correct?
- [ ] **Locks**: Is there potential for deadlock?
- [ ] **Data races**: Is all shared mutable state synchronized?

## 8. Red Flags (Require Extra Scrutiny)

| Pattern | Concern |
|---------|---------|
| `transmute` | Type compatibility, provenance |
| `as` on pointers | Alignment, type punning |
| `static mut` | Data races |
| `*const T as *mut T` | Aliasing violation |
| Manual `Send`/`Sync` | Thread safety |
| `assume_init` | Initialization |
| `set_len` on Vec | Uninitialized memory |
| `from_raw_parts` | Lifetime, validity |
| `offset`/`add`/`sub` | Out of bounds |
| FFI callbacks | Panic safety |

## 9. Verification Questions

Ask the author:
- "What would happen if [X invariant] was violated?"
- "How do you know [pointer/reference] is valid here?"
- "What if this panics at [specific line]?"
- "Who is responsible for freeing this memory?"

## 10. Testing Requirements

- [ ] Has this been tested with Miri?
- [ ] Are there unit tests covering edge cases?
- [ ] Are there tests for error conditions?
- [ ] Has concurrent code been tested under stress?

## Review Severity Guide

| Severity | Requires |
|----------|----------|
| `transmute` | Two reviewers, Miri test |
| Manual `Send`/`Sync` | Thread safety expert review |
| FFI | Documentation of C interface |
| `static mut` | Justification for not using atomic/mutex |
| Pointer arithmetic | Bounds proof |

## Sample Review Comments

```
// Good SAFETY comment ✓
// SAFETY: index was checked to be < len on line 42

// Needs improvement ✗
// SAFETY: This is safe because we know it works

// Missing information ✗
// SAFETY: ptr is valid
// (Why is it valid? How do we know?)
```
