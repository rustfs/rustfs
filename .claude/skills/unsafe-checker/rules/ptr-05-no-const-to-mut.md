---
id: ptr-05
original_id: G.UNS.PTR.02
level: G
impact: CRITICAL
clippy: cast_ref_to_mut
---

# Do Not Manually Convert Immutable Pointer to Mutable

## Summary

Never cast `*const T` to `*mut T` and dereference it to write. This violates aliasing rules and is undefined behavior.

## Rationale

Creating `*const T` from `&T` implies immutability. Other references might exist. Writing through a `*mut T` created from `*const T` creates mutable aliasing, which is UB.

## Bad Example

```rust
// DON'T: Cast *const to *mut
fn bad_mutate(value: &i32) {
    let ptr = value as *const i32 as *mut i32;
    unsafe { *ptr = 42; }  // UB: Mutating through &
}

// DON'T: Use transmute to convert
fn bad_transmute(value: &i32) -> &mut i32 {
    unsafe { std::mem::transmute(value) }  // UB!
}

// DON'T: "I know this is the only reference"
fn bad_claim(value: &i32) {
    // Even if you "know" there's only one reference,
    // the compiler assumes & means no mutation
    let ptr = value as *const i32 as *mut i32;
    unsafe { *ptr += 1; }  // Still UB - compiler may optimize incorrectly
}
```

## Good Example

```rust
// DO: Take &mut if you need to mutate
fn good_mutate(value: &mut i32) {
    *value = 42;
}

// DO: Use interior mutability
use std::cell::{Cell, RefCell, UnsafeCell};

struct Mutable {
    value: Cell<i32>,  // Interior mutability
}

impl Mutable {
    fn modify(&self) {
        self.value.set(42);  // OK: Cell provides interior mutability
    }
}

// DO: Use UnsafeCell if you need raw unsafe interior mutability
struct RawMutable {
    value: UnsafeCell<i32>,
}

impl RawMutable {
    fn modify(&self) {
        // SAFETY: We ensure exclusive access through external means
        unsafe { *self.value.get() = 42; }
    }
}
```

## The UnsafeCell Exception

`UnsafeCell<T>` is the ONLY valid way to get `*mut T` from `&self`:

```rust
use std::cell::UnsafeCell;

pub struct MyMutex<T> {
    data: UnsafeCell<T>,
    // ... lock state
}

impl<T> MyMutex<T> {
    pub fn lock(&self) -> Guard<'_, T> {
        // acquire lock...

        // SAFETY: UnsafeCell allows this, lock ensures exclusivity
        Guard { data: unsafe { &mut *self.data.get() } }
    }
}
```

## Why This Is Always UB

The compiler assumes:
1. `&T` means no mutation will occur
2. Multiple `&T` can exist simultaneously
3. Optimizations can be made based on these assumptions

When you mutate through cast pointer:
1. Other `&T` references see inconsistent values
2. Compiler may cache/eliminate reads
3. Results are unpredictable

## Checklist

- [ ] Am I trying to mutate through `&`?
- [ ] Should I use `&mut` instead?
- [ ] Should I use `Cell`, `RefCell`, or `UnsafeCell`?
- [ ] Is the original type designed for interior mutability?

## Related Rules

- `safety-08`: Mutable return from immutable parameter is wrong
- `safety-02`: Verify safety invariants
