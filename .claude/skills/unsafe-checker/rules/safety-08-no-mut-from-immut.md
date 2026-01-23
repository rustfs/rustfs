---
id: safety-08
original_id: P.UNS.SAS.08
level: P
impact: CRITICAL
clippy: mut_from_ref
---

# Mutable Return from Immutable Parameter is Wrong

## Summary

A function taking `&self` or `&T` must not return `&mut T` to the same data without interior mutability.

## Rationale

Returning `&mut` from `&` violates Rust's aliasing rules. The caller has an immutable borrow, so they can create additional `&` references. Returning `&mut` creates mutable aliasing, which is undefined behavior.

## Bad Example

```rust
// DON'T: Return &mut from &self
struct Container {
    data: i32,
}

impl Container {
    // WRONG: This is undefined behavior!
    pub fn get_mut(&self) -> &mut i32 {
        unsafe {
            // Creating &mut from & is ALWAYS wrong
            &mut *(&self.data as *const i32 as *mut i32)
        }
    }
}

// DON'T: Transmute & to &mut
fn bad_transmute<T>(reference: &T) -> &mut T {
    unsafe { std::mem::transmute(reference) }  // UB!
}
```

## Good Example

```rust
use std::cell::{Cell, RefCell, UnsafeCell};

// DO: Use interior mutability types
struct Container {
    data: Cell<i32>,          // For Copy types
    complex: RefCell<String>, // For non-Copy with runtime checks
}

impl Container {
    pub fn get(&self) -> i32 {
        self.data.get()
    }

    pub fn set(&self, value: i32) {
        self.data.set(value);
    }

    pub fn modify_complex(&self, f: impl FnOnce(&mut String)) {
        f(&mut self.complex.borrow_mut());
    }
}

// DO: Use UnsafeCell for custom interior mutability
struct MyMutex<T> {
    locked: std::sync::atomic::AtomicBool,
    data: UnsafeCell<T>,
}

impl<T> MyMutex<T> {
    pub fn lock(&self) -> MutexGuard<'_, T> {
        // Acquire lock...
        MutexGuard { mutex: self }
    }
}

struct MutexGuard<'a, T> {
    mutex: &'a MyMutex<T>,
}

impl<T> std::ops::DerefMut for MutexGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        // SAFETY: We hold the lock, so exclusive access is guaranteed
        unsafe { &mut *self.mutex.data.get() }
    }
}
```

## The Only Valid Pattern

The ONLY way to get `&mut` from `&` is through `UnsafeCell`:

```rust
use std::cell::UnsafeCell;

struct ValidInteriorMut {
    data: UnsafeCell<i32>,
}

impl ValidInteriorMut {
    // This is sound ONLY because UnsafeCell opts out of aliasing rules
    // AND we guarantee exclusive access (e.g., through a lock)
    pub fn get_mut(&self) -> &mut i32 {
        // Must ensure no other references exist!
        unsafe { &mut *self.data.get() }
    }
}
```

## Checklist

- [ ] Am I trying to return &mut from a & method?
- [ ] If yes, am I using UnsafeCell or a type built on it?
- [ ] Am I guaranteeing exclusive access before creating &mut?
- [ ] Would Cell, RefCell, or Mutex solve my problem safely?

## Related Rules

- `ptr-05`: Don't manually convert *const to *mut
- `safety-02`: Verify safety invariants
