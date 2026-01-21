---
id: ffi-03
original_id: P.UNS.FFI.03
level: P
impact: CRITICAL
---

# Implement Drop for Rust Types Wrapping Memory-Managing C Pointers

## Summary

When wrapping a C pointer that owns memory, implement `Drop` to call the appropriate C deallocation function.

## Rationale

- C allocated memory must be freed with the matching C function
- Rust's default drop won't clean up foreign memory
- Resource leaks and double-frees are common FFI bugs

## Bad Example

```rust
extern "C" {
    fn create_resource() -> *mut Resource;
    fn free_resource(r: *mut Resource);
}

// DON'T: Wrapper without Drop
struct ResourceHandle {
    ptr: *mut Resource,
}

impl ResourceHandle {
    fn new() -> Self {
        Self {
            ptr: unsafe { create_resource() }
        }
    }
    // Memory leak! ptr is never freed
}

// DON'T: Forget to handle null
impl Drop for BadHandle {
    fn drop(&mut self) {
        unsafe {
            free_resource(self.ptr);  // Crash if ptr is null!
        }
    }
}
```

## Good Example

```rust
use std::ptr::NonNull;

extern "C" {
    fn create_resource() -> *mut Resource;
    fn free_resource(r: *mut Resource);
}

// DO: Proper wrapper with Drop
struct ResourceHandle {
    ptr: NonNull<Resource>,
}

impl ResourceHandle {
    fn new() -> Option<Self> {
        let ptr = unsafe { create_resource() };
        NonNull::new(ptr).map(|ptr| Self { ptr })
    }

    fn as_ptr(&self) -> *mut Resource {
        self.ptr.as_ptr()
    }
}

impl Drop for ResourceHandle {
    fn drop(&mut self) {
        // SAFETY: ptr was allocated by create_resource
        // and hasn't been freed yet
        unsafe {
            free_resource(self.ptr.as_ptr());
        }
    }
}

// Prevent accidental copies that would cause double-free
impl !Clone for ResourceHandle {}

// DO: Document ownership transfer
impl ResourceHandle {
    /// Consumes the handle and returns the raw pointer.
    ///
    /// The caller is responsible for freeing the resource.
    fn into_raw(self) -> *mut Resource {
        let ptr = self.ptr.as_ptr();
        std::mem::forget(self);  // Don't run Drop
        ptr
    }

    /// Creates a handle from a raw pointer.
    ///
    /// # Safety
    ///
    /// ptr must have been allocated by create_resource()
    /// and not yet freed.
    unsafe fn from_raw(ptr: *mut Resource) -> Option<Self> {
        NonNull::new(ptr).map(|ptr| Self { ptr })
    }
}
```

## Complete Pattern with Multiple Resources

```rust
struct Connection {
    handle: NonNull<c_void>,
}

struct Statement<'conn> {
    handle: NonNull<c_void>,
    _conn: std::marker::PhantomData<&'conn Connection>,
}

impl Connection {
    fn prepare(&self, sql: &str) -> Option<Statement<'_>> {
        let handle = unsafe { db_prepare(self.handle.as_ptr(), sql.as_ptr()) };
        NonNull::new(handle).map(|handle| Statement {
            handle,
            _conn: std::marker::PhantomData,
        })
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        // Statements must be dropped before Connection
        // PhantomData ensures this at compile time
        unsafe { db_close(self.handle.as_ptr()); }
    }
}

impl Drop for Statement<'_> {
    fn drop(&mut self) {
        unsafe { db_finalize(self.handle.as_ptr()); }
    }
}
```

## Checklist

- [ ] Does my wrapper own the C resource?
- [ ] Did I implement Drop with the correct C free function?
- [ ] Did I handle null pointers?
- [ ] Did I prevent Clone/Copy to avoid double-free?
- [ ] Did I consider ownership transfer methods (into_raw/from_raw)?

## Related Rules

- `mem-03`: Don't let String/Vec drop foreign memory
- `ffi-07`: Don't implement Drop for types passed to external code
