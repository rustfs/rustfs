---
id: ptr-02
original_id: P.UNS.PTR.02
level: P
impact: MEDIUM
---

# Prefer NonNull<T> Over *mut T

## Summary

Use `NonNull<T>` instead of `*mut T` when the pointer should never be null. This enables null pointer optimization and makes the intent clear.

## Rationale

- `NonNull<T>` guarantees non-null at the type level
- Enables niche optimization: `Option<NonNull<T>>` is the same size as `*mut T`
- Makes invariants explicit in the type system
- Covariant over `T` (like `&T`), which is usually what you want

## Bad Example

```rust
// DON'T: Use *mut when pointer is always non-null
struct MyBox<T> {
    ptr: *mut T,  // Invariant: never null, but not enforced
}

impl<T> MyBox<T> {
    pub fn new(value: T) -> Self {
        let ptr = Box::into_raw(Box::new(value));
        // ptr is guaranteed non-null, but type doesn't show it
        Self { ptr }
    }

    pub fn get(&self) -> &T {
        // Must add null check or document the invariant
        unsafe { &*self.ptr }
    }
}
```

## Good Example

```rust
use std::ptr::NonNull;

// DO: Use NonNull when pointer is never null
struct MyBox<T> {
    ptr: NonNull<T>,  // Type guarantees non-null
}

impl<T> MyBox<T> {
    pub fn new(value: T) -> Self {
        let ptr = Box::into_raw(Box::new(value));
        // SAFETY: Box::into_raw never returns null
        let ptr = unsafe { NonNull::new_unchecked(ptr) };
        Self { ptr }
    }

    pub fn get(&self) -> &T {
        // SAFETY: NonNull guarantees ptr is valid
        unsafe { self.ptr.as_ref() }
    }
}

impl<T> Drop for MyBox<T> {
    fn drop(&mut self) {
        // SAFETY: ptr was created from Box::into_raw
        unsafe { drop(Box::from_raw(self.ptr.as_ptr())); }
    }
}

// DO: Niche optimization with Option
struct OptionalBox<T> {
    ptr: Option<NonNull<T>>,  // Same size as *mut T!
}
```

## NonNull API

```rust
use std::ptr::NonNull;

// Creating NonNull
let ptr: NonNull<i32> = NonNull::new(raw_ptr).expect("null pointer");
let ptr: NonNull<i32> = unsafe { NonNull::new_unchecked(raw_ptr) };
let ptr: NonNull<i32> = NonNull::dangling();  // For ZSTs or uninitialized

// Using NonNull
let raw: *mut i32 = ptr.as_ptr();
let reference: &i32 = unsafe { ptr.as_ref() };
let mut_ref: &mut i32 = unsafe { ptr.as_mut() };

// Casting
let ptr: NonNull<u8> = ptr.cast::<u8>();
```

## When to Use *mut T Instead

- When null is a valid/expected value
- FFI with C code that may return null
- When variance matters (NonNull is covariant, sometimes you need invariance)

## Checklist

- [ ] Is my pointer ever null? If no, use NonNull
- [ ] Do I need null pointer optimization?
- [ ] Is the variance correct for my use case?

## Related Rules

- `ptr-03`: Use PhantomData for variance and ownership
- `safety-06`: Don't expose raw pointers in public APIs
