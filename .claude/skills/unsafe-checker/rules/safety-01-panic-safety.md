---
id: safety-01
original_id: P.UNS.SAS.01
level: P
impact: CRITICAL
clippy: panic_in_result_fn
---

# Be Aware of Memory Safety Issues from Panics

## Summary

Panics in unsafe code can leave data structures in an inconsistent state, leading to undefined behavior when the panic is caught.

## Rationale

When a panic occurs, Rust unwinds the stack and runs destructors. If unsafe code has partially modified data, the destructors may observe invalid state.

## Bad Example

```rust
// DON'T: Panic can leave Vec in invalid state
impl<T> MyVec<T> {
    pub fn push(&mut self, value: T) {
        if self.len == self.cap {
            self.grow();  // Might panic during allocation
        }

        unsafe {
            // If Clone::clone() panics after incrementing len,
            // drop will try to drop uninitialized memory
            self.len += 1;
            ptr::write(self.ptr.add(self.len - 1), value.clone());
        }
    }
}
```

## Good Example

```rust
// DO: Ensure panic safety by ordering operations correctly
impl<T> MyVec<T> {
    pub fn push(&mut self, value: T) {
        if self.len == self.cap {
            self.grow();
        }

        unsafe {
            // Write first, then increment len
            // If write somehow panics, len is still valid
            ptr::write(self.ptr.add(self.len), value);
            self.len += 1;  // Only increment after successful write
        }
    }
}

// DO: Use guards for complex operations
impl<T: Clone> MyVec<T> {
    pub fn extend_from_slice(&mut self, slice: &[T]) {
        self.reserve(slice.len());

        let mut guard = PanicGuard {
            vec: self,
            initialized: 0,
        };

        for item in slice {
            unsafe {
                ptr::write(guard.vec.ptr.add(guard.vec.len + guard.initialized), item.clone());
                guard.initialized += 1;
            }
        }

        // Success - update len and forget guard
        self.len += guard.initialized;
        std::mem::forget(guard);
    }
}

struct PanicGuard<'a, T> {
    vec: &'a mut MyVec<T>,
    initialized: usize,
}

impl<T> Drop for PanicGuard<'_, T> {
    fn drop(&mut self) {
        // Clean up partially initialized elements on panic
        unsafe {
            for i in 0..self.initialized {
                ptr::drop_in_place(self.vec.ptr.add(self.vec.len + i));
            }
        }
    }
}
```

## Key Patterns

1. **Update bookkeeping after operations**: Increment length only after writing
2. **Use panic guards**: RAII types that clean up on panic
3. **Order operations carefully**: Ensure invariants hold if panic occurs at any point

## Checklist

- [ ] What happens if this code panics at each line?
- [ ] Are all invariants maintained if we unwind from here?
- [ ] Do I need a panic guard for cleanup?

## Related Rules

- `safety-04`: Avoid double-free from panic safety issues
- `safety-02`: Verify safety invariants
