---
id: ffi-18
original_id: P.UNS.FFI.18
level: P
impact: HIGH
---

# Avoid Passing Trait Objects to C Interfaces

## Summary

Trait objects (`dyn Trait`) have Rust-specific layout (fat pointers with vtable) that is not compatible with C.

## Rationale

- Trait objects are "fat pointers": data ptr + vtable ptr
- C expects thin pointers (single pointer)
- Vtable layout is not stable across Rust versions
- C cannot call Rust vtable methods

## Bad Example

```rust
// DON'T: Pass trait objects to C
trait Handler {
    fn handle(&self, data: i32);
}

extern "C" {
    // This won't work - dyn Handler is a fat pointer!
    fn set_handler(h: *const dyn Handler);
}

// DON'T: Store trait objects in FFI structs
#[repr(C)]
struct BadCallback {
    handler: *const dyn Handler,  // Not C-compatible!
}
```

## Good Example

```rust
use std::os::raw::{c_int, c_void};

// DO: Use function pointers with user_data (trampoline pattern)
type HandlerFn = extern "C" fn(data: c_int, user_data: *mut c_void);

extern "C" {
    fn set_handler(handler: HandlerFn, user_data: *mut c_void);
}

trait Handler {
    fn handle(&self, data: i32);
}

fn register_handler<H: Handler + 'static>(handler: H) {
    // Box the handler
    let boxed: Box<H> = Box::new(handler);
    let user_data = Box::into_raw(boxed) as *mut c_void;

    extern "C" fn trampoline<H: Handler>(data: c_int, user_data: *mut c_void) {
        let handler = unsafe { &*(user_data as *const H) };
        handler.handle(data as i32);
    }

    unsafe {
        set_handler(trampoline::<H>, user_data);
    }
}

// DO: Use concrete types when possible
struct ConcreteHandler {
    multiplier: i32,
}

impl Handler for ConcreteHandler {
    fn handle(&self, data: i32) {
        println!("{}", data * self.multiplier);
    }
}

// DO: Create C-compatible vtable manually if needed
#[repr(C)]
struct HandlerVtable {
    handle: extern "C" fn(this: *const c_void, data: c_int),
    drop: extern "C" fn(this: *mut c_void),
}

#[repr(C)]
struct CCompatibleHandler {
    data: *mut c_void,
    vtable: *const HandlerVtable,
}

impl CCompatibleHandler {
    fn new<H: Handler + 'static>(handler: H) -> Self {
        extern "C" fn handle_impl<H: Handler>(this: *const c_void, data: c_int) {
            let handler = unsafe { &*(this as *const H) };
            handler.handle(data as i32);
        }

        extern "C" fn drop_impl<H: Handler>(this: *mut c_void) {
            unsafe { drop(Box::from_raw(this as *mut H)); }
        }

        static VTABLE: HandlerVtable = HandlerVtable {
            handle: handle_impl::<ConcreteHandler>,  // Need concrete type
            drop: drop_impl::<ConcreteHandler>,
        };

        Self {
            data: Box::into_raw(Box::new(handler)) as *mut c_void,
            vtable: &VTABLE,
        }
    }

    fn handle(&self, data: i32) {
        unsafe {
            ((*self.vtable).handle)(self.data, data as c_int);
        }
    }
}

impl Drop for CCompatibleHandler {
    fn drop(&mut self) {
        unsafe {
            ((*self.vtable).drop)(self.data);
        }
    }
}
```

## Why Trait Objects Don't Work

```
Rust trait object (*const dyn Handler):
[data pointer][vtable pointer]  <- 16 bytes on 64-bit

C pointer (void*):
[pointer]  <- 8 bytes on 64-bit

The sizes don't match!
```

## Alternatives to Trait Objects

| Instead of | Use |
|------------|-----|
| `dyn Trait` | Function pointer + user_data |
| `Box<dyn Trait>` | Boxed concrete type + trampoline |
| `&dyn Trait` | C-compatible vtable struct |
| `Arc<dyn Trait>` | Reference counting wrapper |

## Checklist

- [ ] Am I passing trait objects across FFI?
- [ ] Can I use concrete types instead?
- [ ] Have I used the trampoline pattern for callbacks?
- [ ] If vtable is needed, is it C-compatible?

## Related Rules

- `ffi-16`: Closure to C with trampoline pattern
- `ffi-14`: Types should have stable layout
