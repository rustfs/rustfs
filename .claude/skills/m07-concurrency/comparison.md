# Concurrency: Comparison with Other Languages

## Rust vs Go

### Concurrency Model

| Aspect | Rust | Go |
|--------|------|-----|
| Model | Ownership + Send/Sync | CSP (Communicating Sequential Processes) |
| Primitives | Arc, Mutex, channels | goroutines, channels |
| Safety | Compile-time | Runtime (race detector) |
| Async | async/await + runtime | Built-in scheduler |

### Goroutines vs Rust Tasks

```rust
// Rust: explicit about thread safety
use std::sync::Arc;
use tokio::sync::Mutex;

let data = Arc::new(Mutex::new(vec![]));
let data_clone = Arc::clone(&data);

tokio::spawn(async move {
    let mut guard = data_clone.lock().await;
    guard.push(1);  // Safe: Mutex protects access
});

// Go: implicit sharing (potential race)
// data := []int{}
// go func() {
//     data = append(data, 1)  // RACE CONDITION!
// }()
```

### Channel Comparison

```rust
// Rust: typed channels with ownership
use tokio::sync::mpsc;

let (tx, mut rx) = mpsc::channel::<String>(100);

tokio::spawn(async move {
    tx.send("hello".to_string()).await.unwrap();
    // tx is moved, can't be used elsewhere
});

// Go: channels are more flexible but less safe
// ch := make(chan string, 100)
// go func() {
//     ch <- "hello"
//     // ch can still be used anywhere
// }()
```

---

## Rust vs Java

### Thread Safety Model

| Aspect | Rust | Java |
|--------|------|------|
| Safety | Compile-time (Send/Sync) | Runtime (synchronized, volatile) |
| Null | No null (Option) | NullPointerException risk |
| Locks | RAII (drop releases) | try-finally or try-with-resources |
| Memory | No GC | GC with stop-the-world |

### Synchronization Comparison

```rust
// Rust: lock is tied to data
use std::sync::Mutex;

let data = Mutex::new(vec![1, 2, 3]);
{
    let mut guard = data.lock().unwrap();
    guard.push(4);
}  // lock released automatically

// Java: lock and data are separate
// List<Integer> data = new ArrayList<>();
// synchronized(data) {
//     data.add(4);
// }  // easy to forget synchronization elsewhere
```

### Thread Pool Comparison

```rust
// Rust: rayon for data parallelism
use rayon::prelude::*;

let sum: i32 = (0..1000)
    .into_par_iter()
    .map(|x| x * x)
    .sum();

// Java: Stream API
// int sum = IntStream.range(0, 1000)
//     .parallel()
//     .map(x -> x * x)
//     .sum();
```

---

## Rust vs C++

### Safety Guarantees

| Aspect | Rust | C++ |
|--------|------|-----|
| Data races | Prevented at compile-time | Undefined behavior |
| Deadlocks | Not prevented (same as C++) | Not prevented |
| Thread safety | Send/Sync traits | Convention only |
| Memory ordering | Explicit Ordering enum | memory_order enum |

### Atomic Comparison

```rust
// Rust: clear memory ordering
use std::sync::atomic::{AtomicI32, Ordering};

let counter = AtomicI32::new(0);
counter.fetch_add(1, Ordering::SeqCst);
let value = counter.load(Ordering::Acquire);

// C++: similar but without safety
// std::atomic<int> counter{0};
// counter.fetch_add(1, std::memory_order_seq_cst);
// int value = counter.load(std::memory_order_acquire);
```

### Mutex Comparison

```rust
// Rust: data protected by Mutex
use std::sync::Mutex;

struct SafeCounter {
    count: Mutex<i32>,  // Mutex contains the data
}

impl SafeCounter {
    fn increment(&self) {
        *self.count.lock().unwrap() += 1;
    }
}

// C++: mutex separate from data (error-prone)
// class Counter {
//     std::mutex mtx;
//     int count;  // NOT protected by type system
// public:
//     void increment() {
//         std::lock_guard<std::mutex> lock(mtx);
//         count++;
//     }
//     void unsafe_increment() {
//         count++;  // Compiles! But wrong.
//     }
// };
```

---

## Async Models Comparison

| Language | Model | Runtime |
|----------|-------|---------|
| Rust | async/await, zero-cost | tokio, async-std (bring your own) |
| Go | goroutines | Built-in scheduler |
| JavaScript | async/await, Promises | Event loop (single-threaded) |
| Python | async/await | asyncio (single-threaded) |
| Java | CompletableFuture, Virtual Threads | ForkJoinPool, Loom |

### Rust vs JavaScript Async

```rust
// Rust: async requires explicit runtime, can use multiple threads
#[tokio::main]
async fn main() {
    let results = tokio::join!(
        fetch("url1"),  // runs concurrently
        fetch("url2"),
    );
}

// JavaScript: single-threaded event loop
// async function main() {
//     const results = await Promise.all([
//         fetch("url1"),
//         fetch("url2"),
//     ]);
// }
```

### Rust vs Python Async

```rust
// Rust: true parallelism possible
#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let handles: Vec<_> = urls
        .into_iter()
        .map(|url| tokio::spawn(fetch(url)))  // spawns on thread pool
        .collect();

    for handle in handles {
        let _ = handle.await;
    }
}

// Python: asyncio is single-threaded (use ProcessPoolExecutor for CPU)
# async def main():
#     tasks = [asyncio.create_task(fetch(url)) for url in urls]
#     await asyncio.gather(*tasks)  # all on same thread
```

---

## Send and Sync: Rust's Unique Feature

No other mainstream language has compile-time thread safety markers:

| Trait | Meaning | Auto-impl |
|-------|---------|-----------|
| `Send` | Safe to transfer between threads | Most types |
| `Sync` | Safe to share `&T` between threads | Types with thread-safe `&` |
| `!Send` | Must stay on one thread | Rc, raw pointers |
| `!Sync` | References can't be shared | RefCell, Cell |

### Why This Matters

```rust
// Rust PREVENTS this at compile time:
use std::rc::Rc;

let rc = Rc::new(42);
std::thread::spawn(move || {
    println!("{}", rc);  // ERROR: Rc is not Send
});

// In other languages, this would be a runtime bug:
// - Go: race detector might catch it
// - Java: undefined behavior
// - Python: GIL usually saves you
// - C++: undefined behavior
```

---

## Performance Characteristics

| Aspect | Rust | Go | Java | C++ |
|--------|------|-----|------|-----|
| Thread overhead | System threads or M:N | M:N (goroutines) | System or virtual | System threads |
| Context switch | OS-level or cooperative | Cheap (goroutines) | OS-level | OS-level |
| Memory | Predictable (no GC) | GC pauses | GC pauses | Predictable |
| Async overhead | Zero-cost futures | Runtime overhead | Boxing overhead | Depends |

### When to Use What

| Scenario | Best Choice |
|----------|-------------|
| CPU-bound parallelism | Rust (rayon), C++ |
| I/O-bound concurrency | Rust (tokio), Go, Node.js |
| Low latency required | Rust, C++ |
| Rapid development | Go, Python |
| Complex concurrent state | Rust (compile-time safety) |

---

## Mental Model Shifts

### From Go

```
Before: "Just use goroutines and channels"
After:  "Explicitly declare what can be shared and how"
```

Key shifts:
- `Arc<Mutex<T>>` instead of implicit sharing
- Compiler enforces thread safety
- Async needs explicit runtime

### From Java

```
Before: "synchronized everywhere, hope for the best"
After:  "Types encode thread safety, compiler enforces"
```

Key shifts:
- No need for synchronized keyword
- Mutex contains data, not separate
- No GC pauses in critical sections

### From C++

```
Before: "Be careful, read the docs, use sanitizers"
After:  "Compiler catches data races, trust the type system"
```

Key shifts:
- Send/Sync replace convention
- RAII locks are mandatory, not optional
- Much harder to write incorrect concurrent code
