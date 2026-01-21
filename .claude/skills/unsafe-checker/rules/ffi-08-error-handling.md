---
id: ffi-08
original_id: P.UNS.FFI.08
level: P
impact: HIGH
---

# Handle Errors Properly in FFI

## Summary

FFI functions must use C-compatible error handling (return codes, errno, out parameters). Rust's Result/Option don't cross FFI boundaries.

## Rationale

- C doesn't have Result or Option
- Exceptions don't exist in C
- Must use patterns C code understands

## Bad Example

```rust
// DON'T: Return Result across FFI
#[no_mangle]
pub extern "C" fn bad_open(path: *const c_char) -> Result<Handle, Error> {
    // Result is not C-compatible!
    unimplemented!()
}

// DON'T: Return Option across FFI
#[no_mangle]
pub extern "C" fn bad_find(id: i32) -> Option<*mut Data> {
    // Option<*mut T> might work but is confusing
    unimplemented!()
}
```

## Good Example

```rust
use std::os::raw::{c_char, c_int};

// Error codes
const SUCCESS: c_int = 0;
const ERR_NULL_PTR: c_int = 1;
const ERR_INVALID_PATH: c_int = 2;
const ERR_FILE_NOT_FOUND: c_int = 3;
const ERR_PERMISSION: c_int = 4;
const ERR_UNKNOWN: c_int = -1;

// DO: Return error code, output via pointer
#[no_mangle]
pub extern "C" fn open_file(
    path: *const c_char,
    out_handle: *mut *mut Handle
) -> c_int {
    if path.is_null() || out_handle.is_null() {
        return ERR_NULL_PTR;
    }

    let path_str = match unsafe { CStr::from_ptr(path) }.to_str() {
        Ok(s) => s,
        Err(_) => return ERR_INVALID_PATH,
    };

    match File::open(path_str) {
        Ok(file) => {
            let handle = Box::into_raw(Box::new(Handle { file }));
            unsafe { *out_handle = handle; }
            SUCCESS
        }
        Err(e) => {
            match e.kind() {
                std::io::ErrorKind::NotFound => ERR_FILE_NOT_FOUND,
                std::io::ErrorKind::PermissionDenied => ERR_PERMISSION,
                _ => ERR_UNKNOWN,
            }
        }
    }
}

// DO: Use errno for POSIX-style APIs
#[cfg(unix)]
#[no_mangle]
pub extern "C" fn posix_style_read(
    fd: c_int,
    buf: *mut u8,
    count: usize
) -> isize {
    if buf.is_null() {
        unsafe { *libc::__errno_location() = libc::EINVAL; }
        return -1;
    }

    // ... do read ...
    // On error:
    // unsafe { *libc::__errno_location() = error_code; }
    // return -1;

    count as isize
}

// DO: Provide error message function
thread_local! {
    static LAST_ERROR: std::cell::RefCell<Option<String>> = std::cell::RefCell::new(None);
}

#[no_mangle]
pub extern "C" fn get_error_message(buf: *mut c_char, len: usize) -> c_int {
    LAST_ERROR.with(|e| {
        if let Some(msg) = e.borrow().as_ref() {
            let bytes = msg.as_bytes();
            let copy_len = std::cmp::min(bytes.len(), len.saturating_sub(1));
            unsafe {
                std::ptr::copy_nonoverlapping(bytes.as_ptr(), buf as *mut u8, copy_len);
                *buf.add(copy_len) = 0;
            }
            SUCCESS
        } else {
            ERR_UNKNOWN
        }
    })
}
```

## Error Handling Patterns

| Pattern | Usage |
|---------|-------|
| Return code | Simple success/failure |
| Return code + out param | Return value on success |
| errno | POSIX-style APIs |
| Error message function | Detailed error info |
| Last-error thread-local | Windows-style APIs |

## Checklist

- [ ] Am I returning C-compatible error indicators?
- [ ] Are output parameters used for return values?
- [ ] Is there a way to get detailed error info?
- [ ] Am I documenting all possible error codes?

## Related Rules

- `ffi-04`: Handle panics at FFI boundary
- `safety-10`: Document safety requirements
