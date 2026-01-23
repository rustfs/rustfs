---
id: safety-03
original_id: P.UNS.SAS.03
level: P
impact: CRITICAL
clippy: uninit_assumed_init
---

# Do Not Expose Uninitialized Memory in Public APIs

## Summary

Public APIs must never return or expose uninitialized memory to callers.

## Rationale

Reading uninitialized memory is undefined behavior in Rust. Safe code should never be able to access uninitialized memory through your API.

## Bad Example

```rust
// DON'T: Expose uninitialized memory
pub struct Buffer {
    data: [u8; 1024],
    len: usize,
}

impl Buffer {
    pub fn new() -> Self {
        // BAD: data is uninitialized
        unsafe {
            Self {
                data: std::mem::MaybeUninit::uninit().assume_init(),
                len: 0,
            }
        }
    }

    // BAD: Returns reference to potentially uninitialized data
    pub fn as_slice(&self) -> &[u8] {
        &self.data[..self.len]  // What if len > initialized portion?
    }
}
```

## Good Example

```rust
use std::mem::MaybeUninit;

// DO: Use MaybeUninit properly and only expose initialized data
pub struct Buffer {
    data: Box<[MaybeUninit<u8>; 1024]>,
    len: usize,  // Invariant: data[0..len] is initialized
}

impl Buffer {
    pub fn new() -> Self {
        Self {
            // MaybeUninit doesn't require initialization
            data: Box::new([MaybeUninit::uninit(); 1024]),
            len: 0,
        }
    }

    pub fn push(&mut self, byte: u8) {
        if self.len < 1024 {
            self.data[self.len].write(byte);
            self.len += 1;
        }
    }

    // Only returns initialized portion
    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: self.len bytes are initialized (invariant)
        unsafe {
            std::slice::from_raw_parts(
                self.data.as_ptr() as *const u8,
                self.len
            )
        }
    }
}

impl Drop for Buffer {
    fn drop(&mut self) {
        // Only drop initialized elements
        // For u8 this is a no-op, but important for Drop types
    }
}
```

## Patterns for Uninitialized Memory

```rust
// Pattern 1: MaybeUninit for delayed initialization
let mut value: MaybeUninit<ExpensiveType> = MaybeUninit::uninit();
initialize_expensive(&mut value);
let value = unsafe { value.assume_init() };

// Pattern 2: Vec::with_capacity for growable buffers
let mut vec = Vec::with_capacity(100);
// vec.len() is 0, capacity is 100
// No uninitialized memory is accessible

// Pattern 3: Box::new_uninit (nightly)
let mut boxed = Box::<[u8; 1024]>::new_uninit();
boxed.write([0u8; 1024]);
let boxed = unsafe { boxed.assume_init() };
```

## Checklist

- [ ] Does my API ever return references to uninitialized memory?
- [ ] Are length/capacity invariants properly maintained?
- [ ] Is MaybeUninit used instead of transmute for uninitialized data?

## Related Rules

- `mem-06`: Use MaybeUninit<T> for uninitialized memory
- `safety-01`: Panic safety with partial initialization
