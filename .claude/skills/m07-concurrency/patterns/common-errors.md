# Common Concurrency Errors & Fixes

## E0277: Cannot Send Between Threads

### Error Pattern
```rust
use std::rc::Rc;

let data = Rc::new(42);
std::thread::spawn(move || {
    println!("{}", data);  // ERROR: Rc<i32> cannot be sent between threads
});
```

### Fix Options

**Option 1: Use Arc instead**
```rust
use std::sync::Arc;

let data = Arc::new(42);
let data_clone = Arc::clone(&data);
std::thread::spawn(move || {
    println!("{}", data_clone);  // OK: Arc is Send
});
```

**Option 2: Move owned data**
```rust
let data = 42;  // i32 is Copy and Send
std::thread::spawn(move || {
    println!("{}", data);  // OK
});
```

---

## E0277: Cannot Share Between Threads (Not Sync)

### Error Pattern
```rust
use std::cell::RefCell;
use std::sync::Arc;

let data = Arc::new(RefCell::new(42));
// ERROR: RefCell is not Sync
```

### Fix Options

**Option 1: Use Mutex for thread-safe interior mutability**
```rust
use std::sync::{Arc, Mutex};

let data = Arc::new(Mutex::new(42));
let data_clone = Arc::clone(&data);
std::thread::spawn(move || {
    let mut guard = data_clone.lock().unwrap();
    *guard += 1;
});
```

**Option 2: Use RwLock for read-heavy workloads**
```rust
use std::sync::{Arc, RwLock};

let data = Arc::new(RwLock::new(42));
let data_clone = Arc::clone(&data);
std::thread::spawn(move || {
    let guard = data_clone.read().unwrap();
    println!("{}", *guard);
});
```

---

## Deadlock Patterns

### Pattern 1: Lock Ordering Deadlock
```rust
// DANGER: potential deadlock
use std::sync::{Arc, Mutex};

let a = Arc::new(Mutex::new(1));
let b = Arc::new(Mutex::new(2));

// Thread 1: locks a then b
let a1 = Arc::clone(&a);
let b1 = Arc::clone(&b);
std::thread::spawn(move || {
    let _a = a1.lock().unwrap();
    let _b = b1.lock().unwrap();  // waits for b
});

// Thread 2: locks b then a (opposite order!)
let a2 = Arc::clone(&a);
let b2 = Arc::clone(&b);
std::thread::spawn(move || {
    let _b = b2.lock().unwrap();
    let _a = a2.lock().unwrap();  // waits for a - DEADLOCK
});
```

### Fix: Consistent Lock Ordering
```rust
// SAFE: always lock in same order (a before b)
std::thread::spawn(move || {
    let _a = a1.lock().unwrap();
    let _b = b1.lock().unwrap();
});

std::thread::spawn(move || {
    let _a = a2.lock().unwrap();  // same order
    let _b = b2.lock().unwrap();
});
```

### Pattern 2: Self-Deadlock
```rust
// DANGER: locking same mutex twice
let m = Mutex::new(42);
let _g1 = m.lock().unwrap();
let _g2 = m.lock().unwrap();  // DEADLOCK on std::Mutex

// FIX: use parking_lot::ReentrantMutex if needed
// or restructure code to avoid double locking
```

---

## Mutex Guard Across Await

### Error Pattern
```rust
use std::sync::Mutex;
use tokio::time::sleep;

async fn bad_async() {
    let m = Mutex::new(42);
    let guard = m.lock().unwrap();
    sleep(Duration::from_secs(1)).await;  // WARNING: guard held across await
    println!("{}", *guard);
}
```

### Fix Options

**Option 1: Scope the lock**
```rust
async fn good_async() {
    let m = Mutex::new(42);
    let value = {
        let guard = m.lock().unwrap();
        *guard  // copy value
    };  // guard dropped here
    sleep(Duration::from_secs(1)).await;
    println!("{}", value);
}
```

**Option 2: Use tokio::sync::Mutex**
```rust
use tokio::sync::Mutex;

async fn good_async() {
    let m = Mutex::new(42);
    let guard = m.lock().await;  // async lock
    sleep(Duration::from_secs(1)).await;  // OK with tokio::Mutex
    println!("{}", *guard);
}
```

---

## Data Race Prevention

### Pattern: Missing Synchronization
```rust
// This WON'T compile - Rust prevents data races
use std::sync::Arc;

let data = Arc::new(0);
let d1 = Arc::clone(&data);
let d2 = Arc::clone(&data);

std::thread::spawn(move || {
    // *d1 += 1;  // ERROR: cannot mutate through Arc
});

std::thread::spawn(move || {
    // *d2 += 1;  // ERROR: cannot mutate through Arc
});
```

### Fix: Add Synchronization
```rust
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicI32, Ordering};

// Option 1: Mutex
let data = Arc::new(Mutex::new(0));
let d1 = Arc::clone(&data);
std::thread::spawn(move || {
    *d1.lock().unwrap() += 1;
});

// Option 2: Atomic (for simple types)
let data = Arc::new(AtomicI32::new(0));
let d1 = Arc::clone(&data);
std::thread::spawn(move || {
    d1.fetch_add(1, Ordering::SeqCst);
});
```

---

## Channel Errors

### Disconnected Channel
```rust
use std::sync::mpsc;

let (tx, rx) = mpsc::channel();
drop(tx);  // sender dropped
match rx.recv() {
    Ok(v) => println!("{}", v),
    Err(_) => println!("channel disconnected"),  // this happens
}
```

### Fix: Handle Disconnection
```rust
// Use try_recv for non-blocking
loop {
    match rx.try_recv() {
        Ok(msg) => handle(msg),
        Err(TryRecvError::Empty) => continue,
        Err(TryRecvError::Disconnected) => break,
    }
}

// Or iterate (stops on disconnect)
for msg in rx {
    handle(msg);
}
```

---

## Async Common Errors

### Forgetting to Spawn
```rust
// WRONG: future not polled
async fn fetch_data() -> Result<Data, Error> { ... }

fn process() {
    fetch_data();  // does nothing! returns Future that's dropped
}

// RIGHT: await or spawn
async fn process() {
    let data = fetch_data().await;  // awaited
}

fn process_sync() {
    tokio::spawn(fetch_data());  // spawned
}
```

### Blocking in Async Context
```rust
// WRONG: blocks the executor
async fn bad() {
    std::thread::sleep(Duration::from_secs(1));  // blocks!
    std::fs::read_to_string("file.txt").unwrap();  // blocks!
}

// RIGHT: use async versions
async fn good() {
    tokio::time::sleep(Duration::from_secs(1)).await;
    tokio::fs::read_to_string("file.txt").await.unwrap();
}

// Or spawn_blocking for CPU-bound work
async fn compute() {
    let result = tokio::task::spawn_blocking(|| {
        heavy_computation()  // OK to block here
    }).await.unwrap();
}
```

---

## Thread Panic Handling

### Unhandled Panic
```rust
let handle = std::thread::spawn(|| {
    panic!("oops");
});

// Main thread continues, might miss the error
handle.join().unwrap();  // panics here
```

### Proper Error Handling
```rust
let handle = std::thread::spawn(|| {
    panic!("oops");
});

match handle.join() {
    Ok(result) => println!("Success: {:?}", result),
    Err(e) => println!("Thread panicked: {:?}", e),
}

// For async: use catch_unwind
use std::panic;

async fn safe_task() {
    let result = panic::catch_unwind(|| {
        risky_operation()
    });

    match result {
        Ok(v) => use_value(v),
        Err(_) => log_error("task panicked"),
    }
}
```
