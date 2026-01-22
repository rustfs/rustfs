---
id: ptr-03
original_id: P.UNS.PTR.03
level: P
impact: HIGH
---

# Use PhantomData<T> for Variance and Ownership with Pointer Generics

## Summary

When a struct contains raw pointers but logically owns or borrows the pointed-to data, use `PhantomData<T>` to tell the compiler about the relationship.

## Rationale

Raw pointers don't carry ownership or lifetime information. `PhantomData` lets you:
- Indicate ownership (for `Drop` check)
- Control variance (covariant, contravariant, invariant)
- Participate in lifetime elision

## Bad Example

```rust
// DON'T: Raw pointer without PhantomData
struct MyVec<T> {
    ptr: *mut T,
    len: usize,
    cap: usize,
}

// Problems:
// 1. Compiler doesn't know we "own" the T values
// 2. T might be incorrectly determined as unused
// 3. Drop check may allow dangling references
```

## Good Example

```rust
use std::marker::PhantomData;
use std::ptr::NonNull;

// DO: Use PhantomData to express ownership
struct MyVec<T> {
    ptr: NonNull<T>,
    len: usize,
    cap: usize,
    _marker: PhantomData<T>,  // We own T values
}

// For owned data: PhantomData<T>
// For borrowed data: PhantomData<&'a T>
// For mutably borrowed: PhantomData<&'a mut T>
// For function pointers: PhantomData<fn(T)> (contravariant)

// DO: Express lifetime relationships
struct Iter<'a, T> {
    ptr: *const T,
    end: *const T,
    _marker: PhantomData<&'a T>,  // Borrows T for 'a
}

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.ptr == self.end {
            None
        } else {
            // SAFETY: ptr < end, so ptr is valid
            // Lifetime is tied to 'a through PhantomData
            let current = unsafe { &*self.ptr };
            self.ptr = unsafe { self.ptr.add(1) };
            Some(current)
        }
    }
}
```

## PhantomData Patterns

| Phantom Type | Meaning | Variance |
|--------------|---------|----------|
| `PhantomData<T>` | Owns T | Covariant |
| `PhantomData<&'a T>` | Borrows T for 'a | Covariant in T, covariant in 'a |
| `PhantomData<&'a mut T>` | Mutably borrows T | Invariant in T, covariant in 'a |
| `PhantomData<*const T>` | Just has pointer | Covariant |
| `PhantomData<*mut T>` | Just has pointer | Invariant |
| `PhantomData<fn(T)>` | Consumes T | Contravariant |
| `PhantomData<fn() -> T>` | Produces T | Covariant |

## Drop Check

```rust
use std::marker::PhantomData;

// This tells the compiler that dropping MyVec may drop T values
struct MyVec<T> {
    ptr: NonNull<T>,
    _marker: PhantomData<T>,
}

impl<T> Drop for MyVec<T> {
    fn drop(&mut self) {
        // Drop all T values...
    }
}

// Without PhantomData<T>, this might compile incorrectly:
// let x = MyVec::new(&local);
// drop(local);  // Would be UB if allowed
// drop(x);      // Tries to access dropped local
```

## Checklist

- [ ] Does my pointer type logically own the pointed-to data?
- [ ] Do I need to express a lifetime relationship?
- [ ] What variance do I need for my generic parameter?
- [ ] Will the type be dropped, and does it need drop check?

## Related Rules

- `ptr-02`: Prefer NonNull over *mut T
- `safety-05`: Send/Sync implementation safety
