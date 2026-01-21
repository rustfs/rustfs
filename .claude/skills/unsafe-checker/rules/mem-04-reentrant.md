---
id: mem-04
original_id: P.UNS.MEM.04
level: P
impact: HIGH
---

# Prefer Reentrant Versions of C-API or Syscalls

## Summary

When calling C functions or system calls, use reentrant (`_r`) versions to avoid data races from global state.

## Rationale

Many C library functions use static buffers or global state, making them unsafe in multithreaded programs. Reentrant versions use caller-provided buffers instead.

## Bad Example

```rust
use std::ffi::CStr;

extern "C" {
    fn strtok(s: *mut i8, delim: *const i8) -> *mut i8;
    fn localtime(time: *const i64) -> *mut Tm;
    fn rand() -> i32;
}

// DON'T: Use non-reentrant functions
fn bad_tokenize(s: &mut [i8]) {
    unsafe {
        let delim = b" \0".as_ptr() as *const i8;
        // strtok uses static buffer - not thread-safe!
        let token = strtok(s.as_mut_ptr(), delim);
    }
}

fn bad_time() {
    unsafe {
        let now: i64 = 0;
        // localtime returns pointer to static buffer
        let tm = localtime(&now);  // Data race if called from multiple threads!
    }
}

fn bad_random() -> i32 {
    // rand() uses global state - not thread-safe
    unsafe { rand() }
}
```

## Good Example

```rust
extern "C" {
    fn strtok_r(s: *mut i8, delim: *const i8, saveptr: *mut *mut i8) -> *mut i8;
    fn localtime_r(time: *const i64, result: *mut Tm) -> *mut Tm;
    fn rand_r(seed: *mut u32) -> i32;
}

// DO: Use reentrant versions
fn good_tokenize(s: &mut [i8]) {
    unsafe {
        let delim = b" \0".as_ptr() as *const i8;
        let mut saveptr: *mut i8 = std::ptr::null_mut();
        // strtok_r uses caller-provided saveptr
        let token = strtok_r(s.as_mut_ptr(), delim, &mut saveptr);
    }
}

fn good_time() {
    unsafe {
        let now: i64 = 0;
        let mut result: Tm = std::mem::zeroed();
        // localtime_r writes to caller-provided buffer
        localtime_r(&now, &mut result);
    }
}

fn good_random(seed: &mut u32) -> i32 {
    // rand_r uses caller-provided seed
    unsafe { rand_r(seed) }
}

// BETTER: Use Rust standard library
fn best_time() {
    use std::time::SystemTime;
    let now = SystemTime::now();  // Thread-safe!
}

fn best_random() -> u32 {
    use rand::Rng;
    rand::thread_rng().gen()  // Thread-safe!
}
```

## Common Non-Reentrant Functions

| Non-Reentrant | Reentrant | Rust Alternative |
|---------------|-----------|------------------|
| `strtok` | `strtok_r` | `str::split` |
| `localtime` | `localtime_r` | `chrono` crate |
| `gmtime` | `gmtime_r` | `chrono` crate |
| `ctime` | `ctime_r` | `chrono` crate |
| `rand` | `rand_r` | `rand` crate |
| `strerror` | `strerror_r` | `std::io::Error` |
| `getenv` | None (inherent race) | `std::env::var` (not atomic) |
| `readdir` | `readdir_r` | `std::fs::read_dir` |
| `gethostbyname` | `getaddrinfo` | `std::net::ToSocketAddrs` |

## Checklist

- [ ] Am I calling a C function that might use global state?
- [ ] Is there a `_r` reentrant version available?
- [ ] Is there a Rust standard library alternative?
- [ ] If neither, do I need synchronization?

## Related Rules

- `ffi-10`: Exported functions must be thread-safe
- `ptr-01`: Don't share raw pointers across threads
