---
id: ffi-10
original_id: P.UNS.FFI.10
level: P
impact: CRITICAL
---

# Exported Rust Functions Must Be Designed for Thread-Safety

## Summary

Functions exported to C with `#[no_mangle] extern "C"` may be called from multiple threads. Ensure they are thread-safe.

## Rationale

- C code doesn't know about Rust's thread safety guarantees
- C may call your function from any thread
- Global state must be synchronized
- Race conditions are undefined behavior

## Bad Example

```rust
// DON'T: Unsynchronized global state
static mut COUNTER: i32 = 0;

#[no_mangle]
pub extern "C" fn increment() -> i32 {
    unsafe {
        COUNTER += 1;  // Data race if called from multiple threads!
        COUNTER
    }
}

// DON'T: Thread-local assuming single thread
thread_local! {
    static CONFIG: RefCell<Config> = RefCell::new(Config::default());
}

#[no_mangle]
pub extern "C" fn set_config(value: i32) {
    // Different threads get different configs!
    // Is that what the C caller expects?
    CONFIG.with(|c| c.borrow_mut().value = value);
}

// DON'T: Non-Send types in globals
static mut HANDLE: Option<Rc<Data>> = None;  // Rc is not Send!
```

## Good Example

```rust
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Mutex, OnceLock};

// DO: Use atomics for simple counters
static COUNTER: AtomicI32 = AtomicI32::new(0);

#[no_mangle]
pub extern "C" fn increment() -> i32 {
    COUNTER.fetch_add(1, Ordering::SeqCst) + 1
}

// DO: Use Mutex for complex state
static CONFIG: OnceLock<Mutex<Config>> = OnceLock::new();

fn get_config() -> &'static Mutex<Config> {
    CONFIG.get_or_init(|| Mutex::new(Config::default()))
}

#[no_mangle]
pub extern "C" fn set_config_value(value: i32) -> i32 {
    match get_config().lock() {
        Ok(mut config) => {
            config.value = value;
            0  // Success
        }
        Err(_) => -1  // Lock poisoned
    }
}

// DO: Document thread safety requirements
/// Initializes the library. NOT thread-safe.
/// Must be called once from main thread before any other function.
#[no_mangle]
pub extern "C" fn init() -> i32 {
    // One-time initialization
    0
}

/// Processes data. Thread-safe.
/// May be called from multiple threads concurrently.
#[no_mangle]
pub extern "C" fn process(data: *const u8, len: usize) -> i32 {
    // Uses only local state or synchronized globals
    0
}

// DO: Make non-thread-safe APIs explicit
/// Handle for single-threaded use only.
///
/// # Thread Safety
///
/// This handle must only be used from the thread that created it.
struct SingleThreadHandle {
    data: *mut Data,
    _not_send: std::marker::PhantomData<*const ()>,  // !Send
}
```

## Synchronization Patterns

| Pattern | Use Case |
|---------|----------|
| `AtomicT` | Simple counters, flags |
| `Mutex<T>` | Complex shared state |
| `RwLock<T>` | Read-heavy shared state |
| `OnceLock<T>` | Lazy one-time init |
| `thread_local!` | Per-thread state (document!) |

## Checklist

- [ ] Does my exported function access global state?
- [ ] Is that state properly synchronized?
- [ ] Have I documented thread-safety guarantees?
- [ ] Are any types !Send/!Sync exposed across FFI?

## Related Rules

- `ptr-01`: Don't share raw pointers across threads
- `safety-05`: Send/Sync implementation safety
