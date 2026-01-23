---
id: safety-02
original_id: P.UNS.SAS.02
level: P
impact: CRITICAL
---

# Unsafe Code Authors Must Verify Safety Invariants

## Summary

When writing unsafe code, you are taking responsibility for upholding all safety invariants that the compiler normally enforces.

## Rationale

Unsafe blocks don't disable safety requirements - they transfer responsibility from the compiler to the programmer. You must manually verify what the compiler normally checks.

## Safety Invariants to Verify

1. **Pointer validity**: Non-null, aligned, points to valid memory
2. **Aliasing**: No mutable aliasing (two &mut to same memory)
3. **Initialization**: Memory is initialized before read
4. **Lifetime**: References don't outlive their referents
5. **Type validity**: Data matches the expected type's invariants
6. **Thread safety**: Proper synchronization for concurrent access

## Bad Example

```rust
// DON'T: Blindly trust inputs
unsafe fn process(ptr: *const Data, len: usize) {
    for i in 0..len {
        // No verification that ptr is valid or len is correct!
        let item = &*ptr.add(i);
        process_item(item);
    }
}
```

## Good Example

```rust
// DO: Document and verify invariants
/// Processes a slice of Data items.
///
/// # Safety
///
/// - `ptr` must be non-null and aligned for `Data`
/// - `ptr` must point to `len` consecutive initialized `Data` items
/// - The memory must not be mutated during this call
/// - `len * size_of::<Data>()` must not overflow `isize::MAX`
unsafe fn process(ptr: *const Data, len: usize) {
    debug_assert!(!ptr.is_null(), "ptr must not be null");
    debug_assert!(ptr.is_aligned(), "ptr must be aligned");

    for i in 0..len {
        // SAFETY: Caller guarantees ptr points to len valid items
        let item = &*ptr.add(i);
        process_item(item);
    }
}

// DO: Provide safe wrapper when possible
fn process_slice(data: &[Data]) {
    // SAFETY: slice guarantees all invariants
    unsafe { process(data.as_ptr(), data.len()) }
}
```

## Invariant Documentation Template

```rust
/// # Safety
///
/// The caller must ensure that:
/// - [List each invariant]
/// - [Explain why each matters]
```

## Checklist

- [ ] Have I listed all safety invariants?
- [ ] Can I prove each invariant holds at the call site?
- [ ] Have I added debug assertions where possible?
- [ ] Have I documented invariants in /// # Safety section?

## Related Rules

- `safety-09`: Add SAFETY comment before any unsafe block
- `safety-10`: Add Safety section in docs for public unsafe functions
