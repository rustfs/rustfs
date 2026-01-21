---
id: ffi-07
original_id: P.UNS.FFI.07
level: P
impact: HIGH
---

# Do Not Implement Drop for Types Passed to External Code

## Summary

If a type will be passed to external code that manages its lifetime, don't implement `Drop`. Otherwise, both Rust and the external code will try to free it.

## Rationale

- External code (C library) may take ownership of the data
- If Rust also tries to drop it, you get double-free
- Need clear ownership boundaries

## Bad Example

```rust
// DON'T: Drop on type that external code will free
#[repr(C)]
struct EventHandler {
    callback: extern "C" fn(i32),
    user_data: *mut c_void,
}

impl Drop for EventHandler {
    fn drop(&mut self) {
        // BAD: What if the C library already freed user_data?
        unsafe { libc::free(self.user_data); }
    }
}

extern "C" {
    // C takes ownership and frees EventHandler when done
    fn register_handler(h: *mut EventHandler);
}

fn bad_register() {
    let handler = EventHandler { /* ... */ };
    let ptr = Box::into_raw(Box::new(handler));
    unsafe {
        register_handler(ptr);
        // If C code frees this, and Rust's Drop runs too = double-free
    }
}
```

## Good Example

```rust
// DO: No Drop for types whose lifetime is managed externally
#[repr(C)]
struct EventHandler {
    callback: extern "C" fn(i32),
    user_data: *mut c_void,
}
// No Drop impl - C library manages lifetime

extern "C" {
    fn register_handler(h: *mut EventHandler);
    fn unregister_handler(h: *mut EventHandler);
}

// DO: Wrap in a Rust type that knows when it's safe to drop
struct RegisteredHandler {
    ptr: *mut EventHandler,
    registered: bool,
}

impl RegisteredHandler {
    fn register(handler: EventHandler) -> Self {
        let ptr = Box::into_raw(Box::new(handler));
        unsafe { register_handler(ptr); }
        Self { ptr, registered: true }
    }

    fn unregister(&mut self) {
        if self.registered {
            unsafe { unregister_handler(self.ptr); }
            self.registered = false;
        }
    }
}

impl Drop for RegisteredHandler {
    fn drop(&mut self) {
        self.unregister();
        // Only free if we still own it
        if !self.registered {
            unsafe { drop(Box::from_raw(self.ptr)); }
        }
    }
}

// DO: Use ManuallyDrop for explicit control
use std::mem::ManuallyDrop;

fn explicit_ownership() {
    let handler = ManuallyDrop::new(EventHandler { /* ... */ });
    let ptr = &*handler as *const EventHandler as *mut EventHandler;
    unsafe {
        register_handler(ptr);
        // C now owns handler, don't drop it in Rust
    }
}
```

## Ownership Patterns

| Pattern | Who Owns | Rust Drop? |
|---------|----------|------------|
| Rust creates, Rust frees | Rust | Yes |
| Rust creates, C frees | C | No |
| C creates, C frees | C | No (use wrapper) |
| C creates, Rust frees | Rust | Yes (in wrapper) |

## Checklist

- [ ] Who will free this type's memory?
- [ ] If external code frees it, am I avoiding Drop?
- [ ] If ownership is conditional, do I track it?
- [ ] Am I using ManuallyDrop or forget() when transferring ownership?

## Related Rules

- `ffi-03`: Implement Drop for wrapped C pointers (opposite case)
- `mem-03`: Don't let String/Vec drop foreign memory
