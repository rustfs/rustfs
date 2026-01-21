# Checklist: Before Writing Unsafe Code

Use this checklist before writing any `unsafe` block or `unsafe fn`.

## 1. Do You Really Need Unsafe?

- [ ] Have you tried all safe alternatives?
- [ ] Can you restructure the code to satisfy the borrow checker?
- [ ] Would interior mutability (`Cell`, `RefCell`, `Mutex`) solve the problem?
- [ ] Is there a safe crate that already does this?
- [ ] Is the performance gain (if any) worth the safety risk?

**If you answered "no" to all, proceed with unsafe.**

## 2. What Unsafe Operation Do You Need?

Identify which specific unsafe operation you're performing:

- [ ] Dereferencing a raw pointer (`*const T`, `*mut T`)
- [ ] Calling an `unsafe` function
- [ ] Accessing a mutable static variable
- [ ] Implementing an unsafe trait (`Send`, `Sync`, etc.)
- [ ] Accessing fields of a `union`
- [ ] Using `extern "C"` functions (FFI)

## 3. Safety Invariants

For each unsafe operation, document the invariants:

### For Pointer Dereference:
- [ ] Is the pointer non-null?
- [ ] Is the pointer properly aligned for the type?
- [ ] Does the pointer point to valid, initialized memory?
- [ ] Is the memory not being mutated by other code?
- [ ] Will the memory remain valid for the entire duration of use?

### For Mutable Aliasing:
- [ ] Are you creating multiple mutable references to the same memory?
- [ ] Is there any possibility of aliasing `&mut` and `&`?
- [ ] Have you verified no other code can access this memory?

### For FFI:
- [ ] Is the function signature correct (types, ABI)?
- [ ] Are you handling potential null pointers?
- [ ] Are you handling potential panics (catch_unwind)?
- [ ] Is memory ownership clear (who allocates, who frees)?

### For Send/Sync:
- [ ] Is concurrent access properly synchronized?
- [ ] Are there any data races possible?
- [ ] Does the type truly satisfy the trait requirements?

## 4. Panic Safety

- [ ] What happens if this code panics at any line?
- [ ] Are data structures left in a valid state on panic?
- [ ] Do you need a panic guard for cleanup?
- [ ] Could a destructor see invalid state?

## 5. Documentation

- [ ] Have you written a `// SAFETY:` comment explaining:
  - What invariants must hold?
  - Why those invariants are upheld here?

- [ ] For `unsafe fn`, have you written `# Safety` docs explaining:
  - What the caller must guarantee?
  - What happens if requirements are violated?

## 6. Testing and Verification

- [ ] Can you add debug assertions to verify invariants?
- [ ] Have you tested with Miri (`cargo miri test`)?
- [ ] Have you tested with address sanitizer (`RUSTFLAGS="-Zsanitizer=address"`)?
- [ ] Have you considered fuzzing the unsafe code?

## Quick Reference: Common SAFETY Comments

```rust
// SAFETY: We checked that index < len above, so this is in bounds.

// SAFETY: The pointer was created from a valid reference and hasn't been invalidated.

// SAFETY: We hold the lock, guaranteeing exclusive access.

// SAFETY: The type is #[repr(C)] and all fields are initialized.

// SAFETY: Caller guarantees the pointer is non-null and properly aligned.
```

## Decision Flowchart

```
Need unsafe?
     |
     v
Can you use safe Rust? --Yes--> Don't use unsafe
     |
     No
     v
Can you use existing safe abstraction? --Yes--> Use it (std, crates)
     |
     No
     v
Document all invariants
     |
     v
Add SAFETY comments
     |
     v
Write the unsafe code
     |
     v
Test with Miri
```
