---
id: ptr-01
original_id: P.UNS.PTR.01
level: P
impact: CRITICAL
---

# Do Not Share Raw Pointers Across Threads

## Summary

Raw pointers (`*const T`, `*mut T`) are not `Send` or `Sync` by default. Do not share them across threads without ensuring proper synchronization.

## Rationale

Raw pointers have no synchronization guarantees. Sharing them across threads can lead to data races, which are undefined behavior.

## Bad Example

```rust
use std::thread;

// DON'T: Share raw pointers across threads
fn bad_sharing() {
    let mut data = 42i32;
    let ptr = &mut data as *mut i32;

    let handle = thread::spawn(move || {
        // This is undefined behavior!
        unsafe { *ptr = 100; }
    });

    // Main thread also accesses - data race!
    unsafe { *ptr = 200; }

    handle.join().unwrap();
}

// DON'T: Wrap in struct and impl Send unsafely
struct UnsafePtr(*mut i32);
unsafe impl Send for UnsafePtr {}  // Unsound without synchronization!
```

## Good Example

```rust
use std::sync::{Arc, Mutex, atomic::{AtomicPtr, Ordering}};
use std::thread;

// DO: Use Arc<Mutex<T>> for shared mutable access
fn good_mutex() {
    let data = Arc::new(Mutex::new(42i32));
    let data_clone = Arc::clone(&data);

    let handle = thread::spawn(move || {
        *data_clone.lock().unwrap() = 100;
    });

    *data.lock().unwrap() = 200;
    handle.join().unwrap();
}

// DO: Use AtomicPtr for lock-free pointer sharing
fn good_atomic() {
    let data = Box::into_raw(Box::new(42i32));
    let atomic_ptr = Arc::new(AtomicPtr::new(data));
    let atomic_clone = Arc::clone(&atomic_ptr);

    let handle = thread::spawn(move || {
        let ptr = atomic_clone.load(Ordering::Acquire);
        // SAFETY: We have exclusive access through atomic operations
        unsafe { println!("Value: {}", *ptr); }
    });

    handle.join().unwrap();

    // SAFETY: All threads done, we own the memory
    unsafe { drop(Box::from_raw(atomic_ptr.load(Ordering::Relaxed))); }
}

// DO: If you must use raw pointers, ensure exclusive access
fn good_exclusive() {
    let mut data = vec![1, 2, 3];

    // Send data ownership to thread, not pointer
    let handle = thread::spawn(move || {
        data.push(4);
        data
    });

    let data = handle.join().unwrap();
    println!("{:?}", data);
}
```

## When Raw Pointers Across Threads Are Valid

Only with proper synchronization:
- Through `AtomicPtr` with appropriate memory orderings
- Protected by a `Mutex` (don't share the pointer, share the Mutex)
- Using lock-free algorithms with careful memory ordering

## Checklist

- [ ] Does my pointer cross thread boundaries?
- [ ] Is there synchronization preventing concurrent access?
- [ ] Can I use a higher-level abstraction (Arc, Mutex)?
- [ ] If implementing Send/Sync, is thread safety proven?

## Related Rules

- `safety-05`: Consider safety when implementing Send/Sync
- `safety-02`: Verify safety invariants
