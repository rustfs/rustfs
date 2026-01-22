---
id: ffi-12
original_id: P.UNS.FFI.12
level: P
impact: MEDIUM
---

# Document Invariant Assumptions for C-Provided Parameters

## Summary

When receiving parameters from C, document what invariants you assume (non-null, alignment, validity, lifetime) and verify them when possible.

## Rationale

- C doesn't enforce invariants at compile time
- Rust code needs to validate or document assumptions
- Debugging FFI bugs is hard without clear documentation

## Bad Example

```rust
// DON'T: Undocumented assumptions
extern "C" {
    fn get_data() -> *mut Data;
}

fn bad_use() -> &'static Data {
    let ptr = unsafe { get_data() };
    // Assumes:
    // - ptr is non-null (not documented)
    // - ptr is aligned (not checked)
    // - Data is valid (not verified)
    // - Lifetime is 'static (just guessing)
    unsafe { &*ptr }
}

// DON'T: Silent assumptions in function signature
#[no_mangle]
pub extern "C" fn process(data: *const Data, len: usize) {
    // What if data is null?
    // What if len is wrong?
    // What if data contains invalid Data?
    let slice = unsafe {
        std::slice::from_raw_parts(data, len)
    };
}
```

## Good Example

```rust
/// Retrieves data from the C library.
///
/// # Invariants Assumed from C
///
/// - Returns a non-null pointer on success, null on failure
/// - Returned pointer is valid for the lifetime of the library
/// - Returned pointer is aligned for `Data`
/// - The `Data` struct is fully initialized
extern "C" {
    fn get_data() -> *mut Data;
}

fn documented_use() -> Option<&'static Data> {
    let ptr = unsafe { get_data() };

    // Verify what we can
    if ptr.is_null() {
        return None;
    }

    // Document what we can't verify
    // SAFETY:
    // - Non-null: checked above
    // - Aligned: documented in C library docs
    // - Valid: C library guarantees initialized Data
    // - Lifetime: C library guarantees static lifetime
    Some(unsafe { &*ptr })
}

/// Processes data provided by C caller.
///
/// # Parameters
///
/// - `data`: Must be non-null, aligned for `Data`, and point to `len` valid `Data` items
/// - `len`: Number of items. Must not exceed `isize::MAX / size_of::<Data>()`
///
/// # Returns
///
/// - `0` on success
/// - `-1` if `data` is null
/// - `-2` if `len` is invalid
///
/// # Thread Safety
///
/// This function is thread-safe. The `data` array must not be mutated during the call.
#[no_mangle]
pub extern "C" fn process_documented(data: *const Data, len: usize) -> i32 {
    // Verify invariants we can check
    if data.is_null() {
        return -1;
    }

    if len > isize::MAX as usize / std::mem::size_of::<Data>() {
        return -2;
    }

    // SAFETY:
    // - Non-null: checked above
    // - Aligned: documented requirement for caller
    // - Valid for len items: documented requirement for caller
    // - Not mutated: documented thread safety requirement
    let slice = unsafe { std::slice::from_raw_parts(data, len) };

    for item in slice {
        // process...
    }

    0
}
```

## Documentation Template

```rust
/// Brief description.
///
/// # Parameters
///
/// - `param`: Description, constraints (non-null, aligned, etc.)
///
/// # Invariants Assumed
///
/// The following invariants are assumed and NOT verified:
/// - Invariant 1: explanation
/// - Invariant 2: explanation
///
/// The following invariants ARE verified at runtime:
/// - Verified 1: how it's checked
///
/// # Safety (for unsafe fn)
///
/// Caller must ensure:
/// - Requirement 1
/// - Requirement 2
///
/// # Errors
///
/// Returns error code when:
/// - Condition 1: error code
```

## Checklist

- [ ] Have I documented all assumptions about C parameters?
- [ ] Which invariants can I verify at runtime?
- [ ] Which must I trust the C caller to uphold?
- [ ] Have I documented error conditions and return values?

## Related Rules

- `safety-02`: Verify safety invariants
- `safety-10`: Document safety requirements
