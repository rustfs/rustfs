---
id: general-03
original_id: G.UNS.01
level: G
impact: MEDIUM
---

# Do Not Create Aliases for Types/Methods Named "Unsafe"

## Summary

Do not create type aliases, re-exports, or wrapper methods that hide the "unsafe" nature of operations.

## Rationale

The word "unsafe" in Rust is a signal to developers that extra scrutiny is required. Hiding this signal makes code review harder and can lead to accidental misuse.

## Bad Example

```rust
// DON'T: Hide unsafe behind an alias
type SafePointer = *mut u8;  // Still unsafe to dereference!

// DON'T: Wrap unsafe in a "safe-looking" name
pub fn get_value(ptr: *const i32) -> i32 {
    unsafe { *ptr }  // Caller doesn't know this is unsafe!
}

// DON'T: Re-export unsafe functions with different names
pub use std::mem::transmute as convert;
```

## Good Example

```rust
// DO: Keep "unsafe" visible in the API
pub unsafe fn get_value_unchecked(ptr: *const i32) -> i32 {
    *ptr
}

// DO: If providing a safe wrapper, make the safety contract clear
/// Returns the value at the pointer.
///
/// # Safety
/// This is safe because the pointer is validated internally.
pub fn get_value_checked(ptr: *const i32) -> Option<i32> {
    if ptr.is_null() {
        None
    } else {
        // SAFETY: We checked for null above
        Some(unsafe { *ptr })
    }
}

// DO: Use clear naming for raw pointer types
type RawHandle = *mut c_void;  // "Raw" signals potential unsafety
```

## Common Violations

1. Creating type aliases that hide pointer types
2. Wrapping unsafe functions in safe-looking functions without proper safety analysis
3. Re-exporting unsafe functions with "friendlier" names

## Checklist

- [ ] Does my API preserve visibility of unsafe operations?
- [ ] If wrapping unsafe code in safe API, is the safety invariant enforced?
- [ ] Are type aliases clearly named to indicate their nature?

## Related Rules

- `safety-06`: Don't expose raw pointers in public APIs
- `safety-09`: Add SAFETY comment before any unsafe block
