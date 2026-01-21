---
id: ffi-09
original_id: P.UNS.FFI.09
level: P
impact: MEDIUM
---

# Use References Instead of Raw Pointers When Calling Safe C Functions

## Summary

When wrapping C functions that don't need null pointers, use Rust references in the safe wrapper to enforce non-null at compile time.

## Rationale

- References guarantee non-null
- References have lifetime tracking
- Raw pointers should stay in the unsafe FFI layer
- Safe Rust API should use safe types

## Bad Example

```rust
extern "C" {
    fn c_process(data: *const u8, len: usize);
}

// DON'T: Expose raw pointers in safe API
pub fn process(data: *const u8, len: usize) {
    // Caller might pass null!
    unsafe { c_process(data, len); }
}

// DON'T: Unsafe function when it could be safe
pub unsafe fn process_unsafe(data: *const u8, len: usize) {
    // Why force caller to use unsafe?
    c_process(data, len);
}
```

## Good Example

```rust
extern "C" {
    fn c_process(data: *const u8, len: usize);
    fn c_modify(data: *mut Data);
    fn c_optional(data: *const Data);  // Can be null
}

// DO: Use slice reference for safe API
pub fn process(data: &[u8]) {
    // Reference guarantees non-null
    // Slice guarantees valid length
    unsafe { c_process(data.as_ptr(), data.len()); }
}

// DO: Use &mut for exclusive access
pub fn modify(data: &mut Data) {
    // Mutable reference guarantees:
    // - Non-null
    // - Exclusive access
    // - Valid for duration
    unsafe { c_modify(data as *mut Data); }
}

// DO: Use Option<&T> for nullable parameters
pub fn optional(data: Option<&Data>) {
    let ptr = data.map(|d| d as *const Data).unwrap_or(std::ptr::null());
    unsafe { c_optional(ptr); }
}

// DO: Wrap FFI types in safe Rust types
pub struct SafeHandle(*mut c_void);

impl SafeHandle {
    pub fn new() -> Option<Self> {
        let ptr = unsafe { create_handle() };
        if ptr.is_null() {
            None
        } else {
            Some(Self(ptr))
        }
    }

    // Methods take &self or &mut self, not raw pointers
    pub fn do_something(&self) {
        unsafe { handle_operation(self.0); }
    }
}
```

## Converting Between References and Pointers

```rust
// Reference to pointer
fn ref_to_ptr(r: &Data) -> *const Data {
    r as *const Data
}

fn mut_ref_to_ptr(r: &mut Data) -> *mut Data {
    r as *mut Data
}

// Slice to pointer
fn slice_to_ptr(s: &[u8]) -> (*const u8, usize) {
    (s.as_ptr(), s.len())
}

// Pointer to reference (unsafe)
unsafe fn ptr_to_ref<'a>(p: *const Data) -> &'a Data {
    &*p
}

unsafe fn ptr_to_mut<'a>(p: *mut Data) -> &'a mut Data {
    &mut *p
}
```

## When to Use Raw Pointers

- FFI declarations (`extern "C"`)
- Implementing the unsafe boundary layer
- When null is a valid value
- When the pointee might not be valid Rust (e.g., uninitialized)

## Checklist

- [ ] Can this parameter be a reference instead of a pointer?
- [ ] Am I checking for null in the unsafe layer?
- [ ] Is the safe API free of raw pointers?
- [ ] Do I use Option<&T> for nullable references?

## Related Rules

- `safety-06`: Don't expose raw pointers in public APIs
- `ffi-02`: Read std::ffi documentation
