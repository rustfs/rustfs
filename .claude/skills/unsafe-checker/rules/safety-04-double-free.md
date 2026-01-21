---
id: safety-04
original_id: P.UNS.SAS.04
level: P
impact: CRITICAL
---

# Avoid Double-Free from Panic Safety Issues

## Summary

Ensure that resources are not freed twice, especially when panics can occur during operations.

## Rationale

Double-free is undefined behavior. Panics during unsafe operations can cause destructors to run on already-freed or partially-constructed data.

## Bad Example

```rust
// DON'T: Potential double-free on panic
impl<T> MyVec<T> {
    pub fn pop(&mut self) -> Option<T> {
        if self.len == 0 {
            None
        } else {
            self.len -= 1;
            unsafe {
                // If something panics after this read but before return,
                // Drop will try to drop this element again
                Some(ptr::read(self.ptr.add(self.len)))
            }
        }
    }
}

// DON'T: Double-free with ManuallyDrop misuse
fn bad_swap<T>(a: &mut T, b: &mut T) {
    unsafe {
        let tmp = ptr::read(a);
        ptr::write(a, ptr::read(b));  // If this panics, tmp leaks
        ptr::write(b, tmp);
    }
}
```

## Good Example

```rust
// DO: Use std::mem::take or swap
fn good_swap<T: Default>(a: &mut T, b: &mut T) {
    std::mem::swap(a, b);  // Safe and correct
}

// DO: Use ManuallyDrop for panic safety
use std::mem::ManuallyDrop;

impl<T> MyVec<T> {
    pub fn pop(&mut self) -> Option<T> {
        if self.len == 0 {
            None
        } else {
            self.len -= 1;  // Decrement first
            unsafe {
                // SAFETY: len was decremented, so this slot won't be
                // dropped again by Vec's Drop impl
                Some(ptr::read(self.ptr.add(self.len)))
            }
        }
    }
}

// DO: Use scopeguard or manual cleanup
fn safe_operation<T: Clone>(data: &mut [T], source: &[T]) {
    // Track what we've written for cleanup on panic
    let mut written = 0;

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        for (i, item) in source.iter().enumerate() {
            data[i] = item.clone();
            written = i + 1;
        }
    }));

    if result.is_err() {
        // Clean up on panic (if T needs special handling)
        // In this case, safe code handles it automatically
    }
}
```

## Patterns to Avoid Double-Free

1. **Decrement length before reading**: Vec's Drop won't touch the read element
2. **Use ManuallyDrop**: Explicitly control when Drop runs
3. **Use std::mem::replace/swap**: Safe alternatives for move semantics
4. **Panic guards**: RAII cleanup on unwind

## Checklist

- [ ] After reading memory, is it marked as "moved"?
- [ ] Will Drop run on this memory? Should it?
- [ ] What happens if this code panics at each point?
- [ ] Are length/count bookkeeping updates ordered correctly?

## Related Rules

- `safety-01`: Panic safety in unsafe code
- `ptr-01`: Don't share raw pointers across threads
