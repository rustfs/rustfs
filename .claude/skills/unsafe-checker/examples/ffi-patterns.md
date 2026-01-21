# FFI Best Practices and Patterns

Examples of safe and idiomatic Rust-C interoperability.

## Pattern 1: Basic FFI Wrapper

```rust
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_void};
use std::ptr::NonNull;

// Raw C API
mod ffi {
    use super::*;

    extern "C" {
        pub fn lib_create(name: *const c_char) -> *mut c_void;
        pub fn lib_destroy(handle: *mut c_void);
        pub fn lib_process(handle: *mut c_void, data: *const u8, len: usize) -> c_int;
        pub fn lib_get_error() -> *const c_char;
    }
}

// Safe Rust wrapper
pub struct Library {
    handle: NonNull<c_void>,
}

#[derive(Debug)]
pub struct LibraryError(String);

impl Library {
    pub fn new(name: &str) -> Result<Self, LibraryError> {
        let c_name = CString::new(name).map_err(|_| LibraryError("invalid name".into()))?;

        let handle = unsafe { ffi::lib_create(c_name.as_ptr()) };

        NonNull::new(handle)
            .map(|handle| Self { handle })
            .ok_or_else(|| Self::last_error())
    }

    pub fn process(&self, data: &[u8]) -> Result<(), LibraryError> {
        let result = unsafe {
            ffi::lib_process(self.handle.as_ptr(), data.as_ptr(), data.len())
        };

        if result == 0 {
            Ok(())
        } else {
            Err(Self::last_error())
        }
    }

    fn last_error() -> LibraryError {
        let ptr = unsafe { ffi::lib_get_error() };
        if ptr.is_null() {
            LibraryError("unknown error".into())
        } else {
            let msg = unsafe { CStr::from_ptr(ptr) }
                .to_string_lossy()
                .into_owned();
            LibraryError(msg)
        }
    }
}

impl Drop for Library {
    fn drop(&mut self) {
        unsafe { ffi::lib_destroy(self.handle.as_ptr()); }
    }
}

// Prevent accidental copies
impl !Clone for Library {}
```

## Pattern 2: Callback Registration

```rust
use std::os::raw::{c_int, c_void};
use std::panic::{catch_unwind, AssertUnwindSafe};

type CCallback = extern "C" fn(value: c_int, user_data: *mut c_void) -> c_int;

extern "C" {
    fn register_callback(cb: CCallback, user_data: *mut c_void);
    fn unregister_callback();
}

/// Safely register a Rust closure as a C callback.
pub struct CallbackGuard<F> {
    _closure: Box<F>,
}

impl<F: FnMut(i32) -> i32 + 'static> CallbackGuard<F> {
    pub fn register(closure: F) -> Self {
        let boxed = Box::new(closure);
        let user_data = Box::into_raw(boxed) as *mut c_void;

        extern "C" fn trampoline<F: FnMut(i32) -> i32>(
            value: c_int,
            user_data: *mut c_void,
        ) -> c_int {
            let result = catch_unwind(AssertUnwindSafe(|| {
                let closure = unsafe { &mut *(user_data as *mut F) };
                closure(value as i32) as c_int
            }));
            result.unwrap_or(-1)
        }

        unsafe {
            register_callback(trampoline::<F>, user_data);
        }

        Self {
            // SAFETY: We just created this box and need to keep it alive
            _closure: unsafe { Box::from_raw(user_data as *mut F) },
        }
    }
}

impl<F> Drop for CallbackGuard<F> {
    fn drop(&mut self) {
        unsafe { unregister_callback(); }
        // Box in _closure is dropped automatically
    }
}

// Usage
fn example() {
    let multiplier = 2;
    let _guard = CallbackGuard::register(move |x| x * multiplier);
    // Callback is active until _guard is dropped
}
```

## Pattern 3: Opaque Handle Types

```rust
use std::marker::PhantomData;

// Opaque type markers - prevents mixing up handles
#[repr(C)]
pub struct DatabaseHandle {
    _data: [u8; 0],
    _marker: PhantomData<(*mut u8, std::marker::PhantomPinned)>,
}

#[repr(C)]
pub struct ConnectionHandle {
    _data: [u8; 0],
    _marker: PhantomData<(*mut u8, std::marker::PhantomPinned)>,
}

mod ffi {
    use super::*;

    extern "C" {
        pub fn db_open(path: *const c_char) -> *mut DatabaseHandle;
        pub fn db_close(db: *mut DatabaseHandle);
        pub fn db_connect(db: *mut DatabaseHandle) -> *mut ConnectionHandle;
        pub fn conn_close(conn: *mut ConnectionHandle);
        pub fn conn_query(conn: *mut ConnectionHandle, sql: *const c_char) -> c_int;
    }
}

// Type-safe wrappers
pub struct Database {
    handle: NonNull<DatabaseHandle>,
}

pub struct Connection<'db> {
    handle: NonNull<ConnectionHandle>,
    _db: PhantomData<&'db Database>,
}

impl Database {
    pub fn open(path: &str) -> Result<Self, ()> {
        let c_path = CString::new(path).map_err(|_| ())?;
        let handle = unsafe { ffi::db_open(c_path.as_ptr()) };
        NonNull::new(handle).map(|h| Self { handle: h }).ok_or(())
    }

    pub fn connect(&self) -> Result<Connection<'_>, ()> {
        let handle = unsafe { ffi::db_connect(self.handle.as_ptr()) };
        NonNull::new(handle)
            .map(|h| Connection { handle: h, _db: PhantomData })
            .ok_or(())
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        // All Connections must be dropped first (enforced by lifetime)
        unsafe { ffi::db_close(self.handle.as_ptr()); }
    }
}

impl Connection<'_> {
    pub fn query(&self, sql: &str) -> Result<(), ()> {
        let c_sql = CString::new(sql).map_err(|_| ())?;
        let result = unsafe { ffi::conn_query(self.handle.as_ptr(), c_sql.as_ptr()) };
        if result == 0 { Ok(()) } else { Err(()) }
    }
}

impl Drop for Connection<'_> {
    fn drop(&mut self) {
        unsafe { ffi::conn_close(self.handle.as_ptr()); }
    }
}
```

## Pattern 4: Error Handling Across FFI

```rust
use std::os::raw::c_int;

// Error codes for C
pub const SUCCESS: c_int = 0;
pub const ERR_NULL_PTR: c_int = 1;
pub const ERR_INVALID_UTF8: c_int = 2;
pub const ERR_IO: c_int = 3;
pub const ERR_PANIC: c_int = -1;

// Thread-local error storage
thread_local! {
    static LAST_ERROR: std::cell::RefCell<Option<Box<dyn std::error::Error>>> =
        std::cell::RefCell::new(None);
}

fn set_last_error<E: std::error::Error + 'static>(err: E) {
    LAST_ERROR.with(|e| {
        *e.borrow_mut() = Some(Box::new(err));
    });
}

/// Get the last error message. Caller must free with `free_string`.
#[no_mangle]
pub extern "C" fn get_last_error() -> *mut c_char {
    LAST_ERROR.with(|e| {
        e.borrow()
            .as_ref()
            .map(|err| {
                CString::new(err.to_string())
                    .unwrap_or_else(|_| CString::new("error").unwrap())
                    .into_raw()
            })
            .unwrap_or(std::ptr::null_mut())
    })
}

/// Free a string returned by this library.
#[no_mangle]
pub extern "C" fn free_string(s: *mut c_char) {
    if !s.is_null() {
        // SAFETY: String was created by CString::into_raw
        unsafe { drop(CString::from_raw(s)); }
    }
}

/// Example function with proper error handling.
#[no_mangle]
pub extern "C" fn do_operation(data: *const u8, len: usize) -> c_int {
    let result = catch_unwind(AssertUnwindSafe(|| -> Result<(), c_int> {
        if data.is_null() {
            return Err(ERR_NULL_PTR);
        }

        let slice = unsafe { std::slice::from_raw_parts(data, len) };

        std::str::from_utf8(slice)
            .map_err(|e| {
                set_last_error(e);
                ERR_INVALID_UTF8
            })?;

        // Do actual work...

        Ok(())
    }));

    match result {
        Ok(Ok(())) => SUCCESS,
        Ok(Err(code)) => code,
        Err(_) => ERR_PANIC,
    }
}
```

## Pattern 5: Struct with C Layout

```rust
use std::os::raw::{c_char, c_int};

/// A C-compatible configuration struct.
#[repr(C)]
pub struct Config {
    pub version: c_int,
    pub flags: u32,
    pub name: [c_char; 64],
    pub name_len: usize,
}

impl Config {
    pub fn new(version: i32, flags: u32, name: &str) -> Option<Self> {
        if name.len() >= 64 {
            return None;
        }

        let mut config = Self {
            version: version as c_int,
            flags,
            name: [0; 64],
            name_len: name.len(),
        };

        // Copy name bytes
        for (i, byte) in name.bytes().enumerate() {
            config.name[i] = byte as c_char;
        }

        Some(config)
    }

    pub fn name(&self) -> &str {
        let bytes = unsafe {
            std::slice::from_raw_parts(
                self.name.as_ptr() as *const u8,
                self.name_len,
            )
        };
        // SAFETY: We only store valid UTF-8 in new()
        unsafe { std::str::from_utf8_unchecked(bytes) }
    }
}

// Verify layout at compile time
const _: () = {
    assert!(std::mem::size_of::<Config>() == 80);  // 4 + 4 + 64 + 8
    assert!(std::mem::align_of::<Config>() == 8);
};
```

## Key FFI Guidelines

1. **Always use `#[repr(C)]`** for types crossing FFI
2. **Handle null pointers** at the boundary
3. **Catch panics** before returning to C
4. **Document ownership** clearly
5. **Use opaque types** for type safety
6. **Keep unsafe minimal** and well-documented
