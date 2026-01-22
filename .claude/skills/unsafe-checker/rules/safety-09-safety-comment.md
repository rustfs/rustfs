---
id: safety-09
original_id: P.UNS.SAS.09
level: P
impact: CRITICAL
clippy: undocumented_unsafe_blocks
---

# Add SAFETY Comment Before Any Unsafe Block

## Summary

Every `unsafe` block or `unsafe impl` must have a `// SAFETY:` comment explaining why the operation is safe.

## Rationale

SAFETY comments force the author to think about invariants and help reviewers verify correctness. They serve as documentation for future maintainers.

## Bad Example

```rust
// DON'T: Unsafe without explanation
fn get_unchecked(slice: &[i32], index: usize) -> i32 {
    unsafe { *slice.get_unchecked(index) }
}

// DON'T: Vague or unhelpful comments
fn bad_comments(ptr: *const i32) -> i32 {
    // This is unsafe
    unsafe { *ptr }

    // Trust me
    unsafe { *ptr }

    // Safe because I know what I'm doing
    unsafe { *ptr }
}
```

## Good Example

```rust
// DO: Explain the safety invariant
fn get_unchecked(slice: &[i32], index: usize) -> i32 {
    // SAFETY: Caller guarantees index < slice.len()
    unsafe { *slice.get_unchecked(index) }
}

// DO: Be specific about what makes it safe
fn read_header(buffer: &[u8]) -> Header {
    assert!(buffer.len() >= std::mem::size_of::<Header>());

    // SAFETY:
    // - buffer.len() >= size_of::<Header>() (asserted above)
    // - buffer is aligned for u8, which is compatible with any alignment
    // - Header is #[repr(C)] and has no padding requirements
    unsafe {
        std::ptr::read_unaligned(buffer.as_ptr() as *const Header)
    }
}

// DO: Document unsafe impl
struct MySendType(*mut i32);

// SAFETY: The pointer is to thread-local storage that is only accessed
// from the owning thread. MySendType is only sent when the TLS slot
// is being transferred between threads with proper synchronization.
unsafe impl Send for MySendType {}

// DO: Multi-line for complex invariants
fn complex_operation(data: &mut [u8], ranges: &[(usize, usize)]) {
    for &(start, end) in ranges {
        // SAFETY:
        // 1. All ranges were validated to be within data.len()
        //    in the calling function `validate_ranges()`
        // 2. Ranges are non-overlapping (invariant of RangeSet)
        // 3. We have &mut access to data, so no aliasing
        unsafe {
            let ptr = data.as_mut_ptr().add(start);
            std::ptr::write_bytes(ptr, 0, end - start);
        }
    }
}
```

## SAFETY Comment Format

```rust
// SAFETY: <brief explanation>

// Or for complex cases:
// SAFETY:
// - Invariant 1: explanation
// - Invariant 2: explanation
// - Why this is upheld: explanation
```

## What to Include

1. **What invariants must hold** for this to be safe
2. **Why those invariants hold** at this specific call site
3. **What could go wrong** if the invariants were violated (optional but helpful)

## Clippy Configuration

```toml
# clippy.toml
accept-comment-above-statement = true
accept-comment-above-attributes = true
```

## Checklist

- [ ] Does every unsafe block have a SAFETY comment?
- [ ] Does the comment explain WHY it's safe, not just WHAT it does?
- [ ] Are all relevant invariants mentioned?
- [ ] Would a reviewer understand the safety argument?

## Related Rules

- `safety-02`: Verify safety invariants
- `safety-10`: Add Safety section in docs for public unsafe functions
