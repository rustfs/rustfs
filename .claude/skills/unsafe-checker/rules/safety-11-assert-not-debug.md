---
id: safety-11
original_id: G.UNS.SAS.02
level: G
impact: MEDIUM
clippy: debug_assert_with_mut_call
---

# Use assert! Instead of debug_assert! in Unsafe Functions

## Summary

In `unsafe` functions or functions containing unsafe blocks, prefer `assert!` over `debug_assert!` for checking safety invariants.

## Rationale

`debug_assert!` is compiled out in release builds. If an invariant is important enough to check for safety, it should be checked in all builds to catch violations.

## Bad Example

```rust
// DON'T: Use debug_assert for safety-critical checks
pub unsafe fn get_unchecked(slice: &[i32], index: usize) -> &i32 {
    debug_assert!(index < slice.len());  // Gone in release!
    &*slice.as_ptr().add(index)
}

// DON'T: Rely on debug_assert for FFI safety
pub unsafe fn call_c_function(ptr: *const Data) {
    debug_assert!(!ptr.is_null());  // Won't catch bugs in release
    ffi::process_data(ptr);
}
```

## Good Example

```rust
// DO: Use assert! for safety checks (when performance allows)
pub unsafe fn get_unchecked(slice: &[i32], index: usize) -> &i32 {
    assert!(index < slice.len(), "index {} out of bounds for len {}", index, slice.len());
    &*slice.as_ptr().add(index)
}

// DO: Use debug_assert when CALLER is responsible
/// # Safety
/// index must be less than slice.len()
pub unsafe fn get_unchecked_fast(slice: &[i32], index: usize) -> &i32 {
    // Caller is responsible; debug_assert just helps catch bugs during development
    debug_assert!(index < slice.len());
    &*slice.as_ptr().add(index)
}

// DO: Use assert for internal safety, debug_assert for caller obligations
pub fn get_checked(slice: &[i32], index: usize) -> Option<&i32> {
    if index < slice.len() {
        // SAFETY: We just checked index < len
        // debug_assert is fine here because the if-check is the real guard
        Some(unsafe {
            debug_assert!(index < slice.len()); // Redundant, just for documentation
            &*slice.as_ptr().add(index)
        })
    } else {
        None
    }
}
```

## When to Use Each

| Assertion | Use When |
|-----------|----------|
| `assert!` | Invariant is not already checked; function is called with untrusted input |
| `debug_assert!` | Invariant is the caller's responsibility (documented in `# Safety`); performance-critical |
| No assert | Invariant is enforced by types or prior checks in the same function |

## Hybrid Approach

```rust
// Use cfg to have both safety and performance
pub unsafe fn process(slice: &[u8], index: usize) {
    // Always check in tests and debug
    #[cfg(any(test, debug_assertions))]
    assert!(index < slice.len());

    // Optional: paranoid mode for production
    #[cfg(feature = "paranoid")]
    assert!(index < slice.len());

    // SAFETY: Caller guarantees index < len (checked in debug)
    let ptr = slice.as_ptr().add(index);
    // ...
}
```

## Checklist

- [ ] Is this a safety-critical invariant?
- [ ] Who is responsible for upholding it (caller or this function)?
- [ ] Can the assertion be optimized away when provably true?
- [ ] What's the performance impact of the assertion?

## Related Rules

- `safety-02`: Verify safety invariants
- `safety-09`: SAFETY comments
