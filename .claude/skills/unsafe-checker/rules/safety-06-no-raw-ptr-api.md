---
id: safety-06
original_id: P.UNS.SAS.06
level: P
impact: HIGH
---

# Do Not Expose Raw Pointers in Public APIs

## Summary

Public APIs should use safe abstractions (references, slices, smart pointers) instead of exposing raw pointers.

## Rationale

Raw pointers bypass Rust's safety guarantees. Exposing them in public APIs forces users into unsafe code and makes it easy to create undefined behavior.

## Bad Example

```rust
// DON'T: Expose raw pointers in public API
pub struct Buffer {
    data: *mut u8,
    len: usize,
}

impl Buffer {
    // BAD: Returns raw pointer
    pub fn as_ptr(&self) -> *const u8 {
        self.data
    }

    // BAD: Takes raw pointer as input
    pub fn from_ptr(ptr: *mut u8, len: usize) -> Self {
        Self { data: ptr, len }
    }

    // BAD: Exposes internal pointer mutably
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.data
    }
}
```

## Good Example

```rust
// DO: Use safe abstractions
pub struct Buffer {
    data: Vec<u8>,
}

impl Buffer {
    // Returns a safe reference
    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }

    // Takes safe input
    pub fn from_slice(data: &[u8]) -> Self {
        Self { data: data.to_vec() }
    }

    // Mutable access through safe reference
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.data
    }
}

// DO: If raw pointers are needed, provide unsafe API with documentation
impl Buffer {
    /// Returns a pointer to the buffer's data.
    ///
    /// # Safety
    ///
    /// The pointer is valid for `self.len()` bytes and must not be
    /// used after the Buffer is dropped or reallocated.
    pub fn as_ptr(&self) -> *const u8 {
        self.data.as_ptr()
    }

    /// Creates a Buffer from a raw pointer.
    ///
    /// # Safety
    ///
    /// - `ptr` must point to `len` valid bytes
    /// - The memory must be allocated with the global allocator
    /// - Caller transfers ownership of the memory to Buffer
    pub unsafe fn from_raw_parts(ptr: *mut u8, len: usize, cap: usize) -> Self {
        Self {
            data: Vec::from_raw_parts(ptr, len, cap)
        }
    }
}
```

## Patterns for Safe Pointer APIs

```rust
// Pattern 1: Use NonNull for internal pointers
use std::ptr::NonNull;

pub struct MyBox<T> {
    ptr: NonNull<T>,  // Internal use only
}

impl<T> MyBox<T> {
    // Safe public API
    pub fn get(&self) -> &T {
        // SAFETY: ptr is always valid while MyBox exists
        unsafe { self.ptr.as_ref() }
    }
}

// Pattern 2: Callback-based access
impl Buffer {
    // User can work with pointer in controlled context
    pub fn with_ptr<F, R>(&self, f: F) -> R
    where
        F: FnOnce(*const u8, usize) -> R,
    {
        f(self.data.as_ptr(), self.data.len())
    }
}
```

## Checklist

- [ ] Can this API use references instead of pointers?
- [ ] Can this API use slices instead of pointer + length?
- [ ] If pointers are necessary, is the API marked `unsafe`?
- [ ] Are safety requirements documented?

## Related Rules

- `general-03`: Don't create aliases for unsafe items
- `safety-10`: Document safety requirements for public unsafe functions
