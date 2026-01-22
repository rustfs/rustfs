---
id: safety-05
original_id: P.UNS.SAS.05
level: P
impact: CRITICAL
clippy: non_send_fields_in_send_ty
---

# Consider Safety When Manually Implementing Auto Traits

## Summary

When manually implementing `Send` or `Sync`, you must ensure thread safety invariants are upheld.

## Rationale

`Send` and `Sync` are unsafe traits because incorrect implementations cause data races, which are undefined behavior. The compiler auto-implements them conservatively, but manual implementations require careful analysis.

## Trait Meanings

- **`Send`**: Safe to transfer ownership to another thread
- **`Sync`**: Safe to share references (`&T`) between threads (i.e., `&T: Send`)

## Bad Example

```rust
// DON'T: Unsafe Send/Sync without thread safety
struct NotThreadSafe {
    ptr: *mut i32,  // Raw pointers are not Send/Sync
}

// BAD: This is unsound!
unsafe impl Send for NotThreadSafe {}
unsafe impl Sync for NotThreadSafe {}

// DON'T: Rc-like type with unsafe Sync
struct MyRc<T> {
    ptr: *mut RcInner<T>,
}

struct RcInner<T> {
    count: usize,  // Not atomic!
    data: T,
}

// BAD: count is not atomic, concurrent access is UB
unsafe impl<T: Send> Sync for MyRc<T> {}
```

## Good Example

```rust
use std::sync::atomic::{AtomicUsize, Ordering};
use std::ptr::NonNull;

// DO: Use atomic operations for thread-safe reference counting
struct MyArc<T> {
    ptr: NonNull<ArcInner<T>>,
}

struct ArcInner<T> {
    count: AtomicUsize,  // Atomic for thread safety
    data: T,
}

// SAFETY: The data is behind atomic reference counting,
// and T: Send + Sync ensures the data itself is thread-safe
unsafe impl<T: Send + Sync> Send for MyArc<T> {}
unsafe impl<T: Send + Sync> Sync for MyArc<T> {}

// DO: Document why it's safe
/// A thread-safe wrapper around a raw file descriptor.
///
/// # Safety
///
/// The file descriptor is valid for the lifetime of this struct,
/// and file descriptors are safe to use from any thread.
struct ThreadSafeFd {
    fd: std::os::unix::io::RawFd,
}

// SAFETY: File descriptors are just integers and can be used
// from any thread. The actual I/O operations are thread-safe
// at the OS level.
unsafe impl Send for ThreadSafeFd {}
unsafe impl Sync for ThreadSafeFd {}
```

## Decision Tree

```
Does your type contain:
  - Raw pointers? → Probably not auto Send/Sync
  - Rc/RefCell? → Not Sync (Rc not Send either)
  - Cell/UnsafeCell? → Not Sync
  - Interior mutability? → Needs synchronization for Sync

To manually implement:
  - Send: Can another thread safely drop this?
  - Sync: Can multiple threads safely call &self methods?
```

## Checklist

- [ ] Does my type contain any non-Send/Sync fields?
- [ ] Is interior mutability properly synchronized (Mutex, atomic)?
- [ ] Would concurrent access cause data races?
- [ ] Have I documented why the implementation is safe?

## Related Rules

- `ptr-01`: Don't share raw pointers across threads
- `safety-02`: Verify safety invariants
