# Thread-Based Concurrency Patterns

## Thread Spawning Best Practices

### Basic Thread Spawn
```rust
use std::thread;

fn main() {
    let handle = thread::spawn(|| {
        println!("Hello from thread!");
        42  // return value
    });

    let result = handle.join().unwrap();
    println!("Thread returned: {}", result);
}
```

### Named Threads for Debugging
```rust
use std::thread;

let builder = thread::Builder::new()
    .name("worker-1".to_string())
    .stack_size(32 * 1024);  // 32KB stack

let handle = builder.spawn(|| {
    println!("Thread name: {:?}", thread::current().name());
}).unwrap();
```

### Scoped Threads (No 'static Required)
```rust
use std::thread;

fn process_data(data: &[u32]) -> Vec<u32> {
    thread::scope(|s| {
        let handles: Vec<_> = data
            .chunks(2)
            .map(|chunk| {
                s.spawn(|| {
                    chunk.iter().map(|x| x * 2).collect::<Vec<_>>()
                })
            })
            .collect();

        handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect()
    })
}

fn main() {
    let data = vec![1, 2, 3, 4, 5, 6];
    let result = process_data(&data);  // No 'static needed!
    println!("{:?}", result);
}
```

---

## Shared State Patterns

### Arc + Mutex (Read-Write)
```rust
use std::sync::{Arc, Mutex};
use std::thread;

fn shared_counter() {
    let counter = Arc::new(Mutex::new(0));
    let mut handles = vec![];

    for _ in 0..10 {
        let counter = Arc::clone(&counter);
        let handle = thread::spawn(move || {
            let mut num = counter.lock().unwrap();
            *num += 1;
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    println!("Result: {}", *counter.lock().unwrap());
}
```

### Arc + RwLock (Read-Heavy)
```rust
use std::sync::{Arc, RwLock};
use std::thread;

fn read_heavy_cache() {
    let cache = Arc::new(RwLock::new(vec![1, 2, 3]));

    // Many readers
    for i in 0..5 {
        let cache = Arc::clone(&cache);
        thread::spawn(move || {
            let data = cache.read().unwrap();
            println!("Reader {}: {:?}", i, *data);
        });
    }

    // Occasional writer
    {
        let cache = Arc::clone(&cache);
        thread::spawn(move || {
            let mut data = cache.write().unwrap();
            data.push(4);
            println!("Writer: added element");
        });
    }
}
```

### Atomic for Simple Types
```rust
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

fn atomic_counter() {
    let counter = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for _ in 0..10 {
        let counter = Arc::clone(&counter);
        handles.push(thread::spawn(move || {
            for _ in 0..1000 {
                counter.fetch_add(1, Ordering::SeqCst);
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    println!("Result: {}", counter.load(Ordering::SeqCst));
}
```

---

## Channel Patterns

### MPSC Channel
```rust
use std::sync::mpsc;
use std::thread;

fn producer_consumer() {
    let (tx, rx) = mpsc::channel();

    // Multiple producers
    for i in 0..3 {
        let tx = tx.clone();
        thread::spawn(move || {
            for j in 0..5 {
                tx.send(format!("msg {}-{}", i, j)).unwrap();
            }
        });
    }
    drop(tx);  // Drop original sender

    // Single consumer
    for received in rx {
        println!("Got: {}", received);
    }
}
```

### Sync Channel (Bounded)
```rust
use std::sync::mpsc;
use std::thread;

fn bounded_channel() {
    let (tx, rx) = mpsc::sync_channel(2);  // buffer size 2

    thread::spawn(move || {
        for i in 0..5 {
            println!("Sending {}", i);
            tx.send(i).unwrap();  // blocks if buffer full
            println!("Sent {}", i);
        }
    });

    thread::sleep(std::time::Duration::from_millis(500));
    for received in rx {
        println!("Received: {}", received);
        thread::sleep(std::time::Duration::from_millis(100));
    }
}
```

---

## Thread Pool Patterns

### Using rayon for Parallel Iteration
```rust
use rayon::prelude::*;

fn parallel_map() {
    let numbers: Vec<i32> = (0..1000).collect();

    let squares: Vec<i32> = numbers
        .par_iter()  // parallel iterator
        .map(|x| x * x)
        .collect();

    println!("Processed {} items", squares.len());
}

fn parallel_filter_map() {
    let data: Vec<String> = get_data();

    let results: Vec<_> = data
        .par_iter()
        .filter(|s| !s.is_empty())
        .map(|s| expensive_process(s))
        .collect();
}
```

### Custom Thread Pool with crossbeam
```rust
use crossbeam::channel;
use std::thread;

fn custom_pool(num_workers: usize) {
    let (tx, rx) = channel::bounded::<Box<dyn FnOnce() + Send>>(100);

    // Spawn workers
    let workers: Vec<_> = (0..num_workers)
        .map(|_| {
            let rx = rx.clone();
            thread::spawn(move || {
                while let Ok(task) = rx.recv() {
                    task();
                }
            })
        })
        .collect();

    // Submit tasks
    for i in 0..100 {
        tx.send(Box::new(move || {
            println!("Processing task {}", i);
        })).unwrap();
    }

    drop(tx);  // Close channel

    for worker in workers {
        worker.join().unwrap();
    }
}
```

---

## Synchronization Primitives

### Barrier (Wait for All)
```rust
use std::sync::{Arc, Barrier};
use std::thread;

fn barrier_example() {
    let barrier = Arc::new(Barrier::new(3));
    let mut handles = vec![];

    for i in 0..3 {
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            println!("Thread {} starting", i);
            thread::sleep(std::time::Duration::from_millis(i as u64 * 100));

            barrier.wait();  // All threads wait here

            println!("Thread {} after barrier", i);
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }
}
```

### Condvar (Condition Variable)
```rust
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

fn condvar_example() {
    let pair = Arc::new((Mutex::new(false), Condvar::new()));
    let pair_clone = Arc::clone(&pair);

    // Waiter thread
    let waiter = thread::spawn(move || {
        let (lock, cvar) = &*pair_clone;
        let mut started = lock.lock().unwrap();
        while !*started {
            started = cvar.wait(started).unwrap();
        }
        println!("Waiter: condition met!");
    });

    // Notifier
    thread::sleep(std::time::Duration::from_millis(100));
    let (lock, cvar) = &*pair;
    {
        let mut started = lock.lock().unwrap();
        *started = true;
    }
    cvar.notify_one();

    waiter.join().unwrap();
}
```

### Once (One-Time Initialization)
```rust
use std::sync::Once;

static INIT: Once = Once::new();
static mut CONFIG: Option<Config> = None;

fn get_config() -> &'static Config {
    INIT.call_once(|| {
        unsafe {
            CONFIG = Some(load_config());
        }
    });
    unsafe { CONFIG.as_ref().unwrap() }
}

// Better: use once_cell or lazy_static
use once_cell::sync::Lazy;

static CONFIG: Lazy<Config> = Lazy::new(|| {
    load_config()
});
```

---

## Error Handling in Threads

### Handling Panics
```rust
use std::thread;

fn handle_panic() {
    let handle = thread::spawn(|| {
        panic!("Thread panicked!");
    });

    match handle.join() {
        Ok(_) => println!("Thread completed successfully"),
        Err(e) => {
            if let Some(s) = e.downcast_ref::<&str>() {
                println!("Thread panicked with: {}", s);
            } else if let Some(s) = e.downcast_ref::<String>() {
                println!("Thread panicked with: {}", s);
            } else {
                println!("Thread panicked with unknown error");
            }
        }
    }
}
```

### Catching Panics
```rust
use std::panic;

fn catch_panic() {
    let result = panic::catch_unwind(|| {
        risky_operation()
    });

    match result {
        Ok(value) => println!("Success: {:?}", value),
        Err(_) => println!("Operation panicked, continuing..."),
    }
}
```
