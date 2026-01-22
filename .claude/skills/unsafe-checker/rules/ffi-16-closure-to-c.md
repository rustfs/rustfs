---
id: ffi-16
original_id: P.UNS.FFI.16
level: P
impact: HIGH
---

# Separate Data and Code When Passing Rust Closures to C

## Summary

C callbacks are function pointers without captured state. To pass Rust closures to C, separate the function pointer from the closure data using a "trampoline" pattern.

## Rationale

- Rust closures can capture state (like lambdas)
- C function pointers are just addresses, no state
- Must pass state separately via `void*` user_data

## Bad Example

```rust
// DON'T: Try to pass closure directly
extern "C" {
    fn set_callback(cb: fn(i32) -> i32);  // Only works for non-capturing!
}

fn bad_closure() {
    let multiplier = 2;
    let closure = |x| x * multiplier;  // Captures multiplier

    // This won't compile - closure is not fn pointer
    // set_callback(closure);
}

// DON'T: Transmute closure to function pointer
fn bad_transmute() {
    let closure = |x: i32| x * 2;
    let fp: fn(i32) -> i32 = unsafe { std::mem::transmute(closure) };
    // UB: Closure may have non-zero size
}
```

## Good Example

```rust
use std::os::raw::c_void;
use std::ffi::c_int;

// C callback signature with user_data
type CCallback = extern "C" fn(value: c_int, user_data: *mut c_void) -> c_int;

extern "C" {
    fn set_callback(cb: CCallback, user_data: *mut c_void);
    fn remove_callback();
}

// DO: Use trampoline pattern
fn good_closure<F: FnMut(i32) -> i32>(mut closure: F) {
    // Trampoline function that forwards to the closure
    extern "C" fn trampoline<F: FnMut(i32) -> i32>(
        value: c_int,
        user_data: *mut c_void,
    ) -> c_int {
        let closure = unsafe { &mut *(user_data as *mut F) };
        closure(value as i32) as c_int
    }

    let user_data = &mut closure as *mut F as *mut c_void;

    unsafe {
        set_callback(trampoline::<F>, user_data);
        // Important: closure must live until callback is removed!
    }
}

// DO: Box the closure for 'static lifetime
struct CallbackHandle {
    closure: Box<dyn FnMut(i32) -> i32>,
}

impl CallbackHandle {
    fn new<F: FnMut(i32) -> i32 + 'static>(closure: F) -> Self {
        Self { closure: Box::new(closure) }
    }

    fn register(&mut self) {
        extern "C" fn trampoline(value: c_int, user_data: *mut c_void) -> c_int {
            let closure = unsafe { &mut *(user_data as *mut Box<dyn FnMut(i32) -> i32>) };
            closure(value as i32) as c_int
        }

        let user_data = &mut self.closure as *mut _ as *mut c_void;
        unsafe { set_callback(trampoline, user_data); }
    }
}

impl Drop for CallbackHandle {
    fn drop(&mut self) {
        unsafe { remove_callback(); }
        // Now safe to drop closure
    }
}

// Usage
fn example() {
    let multiplier = 2;
    let mut handle = CallbackHandle::new(move |x| x * multiplier);
    handle.register();
    // handle must live until callback is no longer needed
}
```

## Trampoline Pattern

```
Rust Closure: |x| x * captured_value
     |
     v
+-----------------+     +-----------------+
| trampoline fn   | --> | closure data    |
| (no captures)   |     | (captured_value)|
+-----------------+     +-----------------+
     |                         ^
     |    user_data ptr        |
     +-------------------------+

C sees: function pointer + void* user_data
```

## Checklist

- [ ] Does my closure capture any state?
- [ ] Am I using the trampoline pattern?
- [ ] Does the closure data live long enough?
- [ ] Am I unregistering before dropping the closure?

## Related Rules

- `ffi-03`: Implement Drop for resource wrappers
- `ffi-10`: Thread safety for callbacks
