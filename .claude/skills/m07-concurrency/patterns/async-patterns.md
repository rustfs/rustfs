# Async Patterns in Rust

## Task Spawning

### Basic Spawn
```rust
use tokio::task;

#[tokio::main]
async fn main() {
    // Spawn a task that runs concurrently
    let handle = task::spawn(async {
        expensive_computation().await
    });

    // Do other work while task runs
    other_work().await;

    // Wait for result
    let result = handle.await.unwrap();
}
```

### Spawn with Shared State
```rust
use std::sync::Arc;
use tokio::sync::Mutex;

async fn process_with_state() {
    let state = Arc::new(Mutex::new(vec![]));

    let handles: Vec<_> = (0..10)
        .map(|i| {
            let state = Arc::clone(&state);
            tokio::spawn(async move {
                let mut guard = state.lock().await;
                guard.push(i);
            })
        })
        .collect();

    // Wait for all tasks
    for handle in handles {
        handle.await.unwrap();
    }
}
```

---

## Select Pattern

### Racing Multiple Futures
```rust
use tokio::select;
use tokio::time::{sleep, Duration};

async fn first_response() {
    select! {
        result = fetch_from_server_a() => {
            println!("A responded first: {:?}", result);
        }
        result = fetch_from_server_b() => {
            println!("B responded first: {:?}", result);
        }
    }
}
```

### Select with Timeout
```rust
use tokio::time::timeout;

async fn with_timeout() -> Result<Data, Error> {
    select! {
        result = fetch_data() => result,
        _ = sleep(Duration::from_secs(5)) => {
            Err(Error::Timeout)
        }
    }
}

// Or use timeout directly
async fn with_timeout2() -> Result<Data, Error> {
    timeout(Duration::from_secs(5), fetch_data())
        .await
        .map_err(|_| Error::Timeout)?
}
```

### Select with Channel
```rust
use tokio::sync::mpsc;

async fn process_messages(mut rx: mpsc::Receiver<Message>) {
    loop {
        select! {
            Some(msg) = rx.recv() => {
                handle_message(msg).await;
            }
            _ = tokio::signal::ctrl_c() => {
                println!("Shutting down...");
                break;
            }
        }
    }
}
```

---

## Channel Patterns

### MPSC (Multi-Producer, Single-Consumer)
```rust
use tokio::sync::mpsc;

async fn producer_consumer() {
    let (tx, mut rx) = mpsc::channel(100);

    // Spawn producers
    for i in 0..3 {
        let tx = tx.clone();
        tokio::spawn(async move {
            tx.send(format!("Message from {}", i)).await.unwrap();
        });
    }

    // Drop original sender so channel closes
    drop(tx);

    // Consume
    while let Some(msg) = rx.recv().await {
        println!("Received: {}", msg);
    }
}
```

### Oneshot (Single-Shot Response)
```rust
use tokio::sync::oneshot;

async fn request_response() {
    let (tx, rx) = oneshot::channel();

    tokio::spawn(async move {
        let result = compute_something().await;
        tx.send(result).unwrap();
    });

    // Wait for response
    let response = rx.await.unwrap();
}
```

### Broadcast (Multi-Consumer)
```rust
use tokio::sync::broadcast;

async fn pub_sub() {
    let (tx, _) = broadcast::channel(16);

    // Subscribe multiple consumers
    let mut rx1 = tx.subscribe();
    let mut rx2 = tx.subscribe();

    tokio::spawn(async move {
        while let Ok(msg) = rx1.recv().await {
            println!("Consumer 1: {}", msg);
        }
    });

    tokio::spawn(async move {
        while let Ok(msg) = rx2.recv().await {
            println!("Consumer 2: {}", msg);
        }
    });

    // Publish
    tx.send("Hello").unwrap();
}
```

### Watch (Single Latest Value)
```rust
use tokio::sync::watch;

async fn config_updates() {
    let (tx, mut rx) = watch::channel(Config::default());

    // Consumer watches for changes
    tokio::spawn(async move {
        while rx.changed().await.is_ok() {
            let config = rx.borrow();
            apply_config(&config);
        }
    });

    // Update config
    tx.send(Config::new()).unwrap();
}
```

---

## Structured Concurrency

### JoinSet for Task Groups
```rust
use tokio::task::JoinSet;

async fn parallel_fetch(urls: Vec<String>) -> Vec<Result<Response, Error>> {
    let mut set = JoinSet::new();

    for url in urls {
        set.spawn(async move {
            fetch(&url).await
        });
    }

    let mut results = vec![];
    while let Some(res) = set.join_next().await {
        results.push(res.unwrap());
    }
    results
}
```

### Scoped Tasks (no 'static)
```rust
// Using tokio-scoped or async-scoped crate
use async_scoped::TokioScope;

async fn scoped_example(data: &[u32]) {
    let results = TokioScope::scope_and_block(|scope| {
        for item in data {
            scope.spawn(async move {
                process(item).await
            });
        }
    });
}
```

---

## Cancellation Patterns

### Using CancellationToken
```rust
use tokio_util::sync::CancellationToken;

async fn cancellable_task(token: CancellationToken) {
    loop {
        select! {
            _ = token.cancelled() => {
                println!("Task cancelled");
                break;
            }
            _ = do_work() => {
                // Continue working
            }
        }
    }
}

async fn main_with_cancellation() {
    let token = CancellationToken::new();
    let task_token = token.clone();

    let handle = tokio::spawn(cancellable_task(task_token));

    // Cancel after some condition
    tokio::time::sleep(Duration::from_secs(5)).await;
    token.cancel();

    handle.await.unwrap();
}
```

### Graceful Shutdown
```rust
async fn serve_with_shutdown(shutdown: impl Future) {
    let server = TcpListener::bind("0.0.0.0:8080").await.unwrap();

    loop {
        select! {
            Ok((socket, _)) = server.accept() => {
                tokio::spawn(handle_connection(socket));
            }
            _ = &mut shutdown => {
                println!("Shutting down...");
                break;
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let ctrl_c = async {
        tokio::signal::ctrl_c().await.unwrap();
    };

    serve_with_shutdown(ctrl_c).await;
}
```

---

## Backpressure Patterns

### Bounded Channels
```rust
use tokio::sync::mpsc;

async fn with_backpressure() {
    // Buffer of 10 - producers will wait if full
    let (tx, mut rx) = mpsc::channel(10);

    let producer = tokio::spawn(async move {
        for i in 0..1000 {
            // This will wait if channel is full
            tx.send(i).await.unwrap();
        }
    });

    let consumer = tokio::spawn(async move {
        while let Some(item) = rx.recv().await {
            // Slow consumer
            tokio::time::sleep(Duration::from_millis(10)).await;
            process(item);
        }
    });

    let _ = tokio::join!(producer, consumer);
}
```

### Semaphore for Rate Limiting
```rust
use tokio::sync::Semaphore;
use std::sync::Arc;

async fn rate_limited_requests(urls: Vec<String>) {
    let semaphore = Arc::new(Semaphore::new(10));  // max 10 concurrent

    let handles: Vec<_> = urls
        .into_iter()
        .map(|url| {
            let sem = Arc::clone(&semaphore);
            tokio::spawn(async move {
                let _permit = sem.acquire().await.unwrap();
                fetch(&url).await
            })
        })
        .collect();

    for handle in handles {
        handle.await.unwrap();
    }
}
```

---

## Error Handling in Async

### Propagating Errors
```rust
async fn fetch_and_parse(url: &str) -> Result<Data, Error> {
    let response = fetch(url).await?;
    let data = parse(response).await?;
    Ok(data)
}
```

### Handling Task Panics
```rust
async fn robust_spawn() {
    let handle = tokio::spawn(async {
        risky_operation().await
    });

    match handle.await {
        Ok(result) => println!("Success: {:?}", result),
        Err(e) if e.is_panic() => {
            println!("Task panicked: {:?}", e);
        }
        Err(e) => {
            println!("Task cancelled: {:?}", e);
        }
    }
}
```

### Try-Join for Multiple Results
```rust
use tokio::try_join;

async fn fetch_all() -> Result<(A, B, C), Error> {
    // All must succeed, or first error returned
    try_join!(
        fetch_a(),
        fetch_b(),
        fetch_c(),
    )
}
```
