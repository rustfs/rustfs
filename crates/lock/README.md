[![RustFS](https://rustfs.com/images/rustfs-github.png)](https://rustfs.com)

# RustFS Lock - Distributed Locking

<p align="center">
  <strong>Distributed locking and synchronization for RustFS object storage</strong>
</p>

<p align="center">
  <a href="https://github.com/rustfs/rustfs/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/rustfs/rustfs/actions/workflows/ci.yml/badge.svg" /></a>
  <a href="https://docs.rustfs.com/en/">📖 Documentation</a>
  · <a href="https://github.com/rustfs/rustfs/issues">🐛 Bug Reports</a>
  · <a href="https://github.com/rustfs/rustfs/discussions">💬 Discussions</a>
</p>

---

## 📖 Overview

**RustFS Lock** provides distributed locking and synchronization primitives for the [RustFS](https://rustfs.com) distributed object storage system. It ensures data consistency and prevents race conditions in multi-node environments through various locking mechanisms and coordination protocols.

> **Note:** This is a core submodule of RustFS that provides essential distributed locking capabilities for the distributed object storage system. For the complete RustFS experience, please visit the [main RustFS repository](https://github.com/rustfs/rustfs).

## ✨ Features

### 🔒 Distributed Locking

- **Exclusive Locks**: Mutual exclusion across cluster nodes
- **Shared Locks**: Reader-writer lock semantics
- **Timeout Support**: Configurable lock timeouts and expiration
- **Deadlock Prevention**: Automatic deadlock detection and resolution

### 🔄 Synchronization Primitives

- **Distributed Mutex**: Cross-node mutual exclusion
- **Distributed Semaphore**: Resource counting across nodes
- **Distributed Barrier**: Coordination point for multiple nodes
- **Distributed Condition Variables**: Wait/notify across nodes

### 🛡️ Consistency Guarantees

- **Linearizable Operations**: Strong consistency guarantees
- **Fault Tolerance**: Automatic recovery from node failures
- **Network Partition Handling**: CAP theorem aware implementations
- **Consensus Integration**: Raft-based consensus for critical locks

### 🚀 Performance Features

- **Lock Coalescing**: Efficient batching of lock operations
- **Adaptive Timeouts**: Dynamic timeout adjustment
- **Lock Hierarchy**: Hierarchical locking for better scalability
- **Optimistic Locking**: Reduced contention through optimistic approaches

## 📦 Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
rustfs-lock = "0.1.0"
```

## 🔧 Usage

### Basic Distributed Lock

```rust
use rustfs_lock::{DistributedLock, LockManager, LockOptions};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create lock manager
    let lock_manager = LockManager::new("cluster-endpoint").await?;

    // Acquire distributed lock
    let lock_options = LockOptions {
        timeout: Duration::from_secs(30),
        auto_renew: true,
        ..Default::default()
    };

    let lock = lock_manager.acquire_lock("resource-key", lock_options).await?;

    // Critical section
    {
        println!("Lock acquired, performing critical operations...");
        // Your critical code here
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    // Release lock
    lock.release().await?;
    println!("Lock released");

    Ok(())
}
```

### Distributed Mutex

```rust
use rustfs_lock::{DistributedMutex, LockManager};
use std::sync::Arc;

async fn distributed_mutex_example() -> Result<(), Box<dyn std::error::Error>> {
    let lock_manager = Arc::new(LockManager::new("cluster-endpoint").await?);

    // Create distributed mutex
    let mutex = DistributedMutex::new(lock_manager.clone(), "shared-resource");

    // Spawn multiple tasks
    let mut handles = vec![];

    for i in 0..5 {
        let mutex = mutex.clone();
        let handle = tokio::spawn(async move {
            let _guard = mutex.lock().await.unwrap();
            println!("Task {} acquired mutex", i);

            // Simulate work
            tokio::time::sleep(Duration::from_secs(1)).await;

            println!("Task {} releasing mutex", i);
            // Guard is automatically released when dropped
        });

        handles.push(handle);
    }

    // Wait for all tasks to complete
    for handle in handles {
        handle.await?;
    }

    Ok(())
}
```

### Distributed Semaphore

```rust
use rustfs_lock::{DistributedSemaphore, LockManager};
use std::sync::Arc;

async fn distributed_semaphore_example() -> Result<(), Box<dyn std::error::Error>> {
    let lock_manager = Arc::new(LockManager::new("cluster-endpoint").await?);

    // Create distributed semaphore with 3 permits
    let semaphore = DistributedSemaphore::new(
        lock_manager.clone(),
        "resource-pool",
        3
    );

    // Spawn multiple tasks
    let mut handles = vec![];

    for i in 0..10 {
        let semaphore = semaphore.clone();
        let handle = tokio::spawn(async move {
            let _permit = semaphore.acquire().await.unwrap();
            println!("Task {} acquired permit", i);

            // Simulate work
            tokio::time::sleep(Duration::from_secs(2)).await;

            println!("Task {} releasing permit", i);
            // Permit is automatically released when dropped
        });

        handles.push(handle);
    }

    // Wait for all tasks to complete
    for handle in handles {
        handle.await?;
    }

    Ok(())
}
```

### Distributed Barrier

```rust
use rustfs_lock::{DistributedBarrier, LockManager};
use std::sync::Arc;

async fn distributed_barrier_example() -> Result<(), Box<dyn std::error::Error>> {
    let lock_manager = Arc::new(LockManager::new("cluster-endpoint").await?);

    // Create distributed barrier for 3 participants
    let barrier = DistributedBarrier::new(
        lock_manager.clone(),
        "sync-point",
        3
    );

    // Spawn multiple tasks
    let mut handles = vec![];

    for i in 0..3 {
        let barrier = barrier.clone();
        let handle = tokio::spawn(async move {
            println!("Task {} doing work...", i);

            // Simulate different work durations
            tokio::time::sleep(Duration::from_secs(i + 1)).await;

            println!("Task {} waiting at barrier", i);
            barrier.wait().await.unwrap();

            println!("Task {} passed barrier", i);
        });

        handles.push(handle);
    }

    // Wait for all tasks to complete
    for handle in handles {
        handle.await?;
    }

    Ok(())
}
```

### Lock with Automatic Renewal

```rust
use rustfs_lock::{DistributedLock, LockManager, LockOptions};
use std::time::Duration;

async fn auto_renewal_example() -> Result<(), Box<dyn std::error::Error>> {
    let lock_manager = LockManager::new("cluster-endpoint").await?;

    let lock_options = LockOptions {
        timeout: Duration::from_secs(10),
        auto_renew: true,
        renew_interval: Duration::from_secs(3),
        max_renewals: 5,
        ..Default::default()
    };

    let lock = lock_manager.acquire_lock("long-running-task", lock_options).await?;

    // Long-running operation
    for i in 0..20 {
        println!("Working on step {}", i);
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Check if lock is still valid
        if !lock.is_valid().await? {
            println!("Lock lost, aborting operation");
            break;
        }
    }

    lock.release().await?;
    Ok(())
}
```

### Hierarchical Locking

```rust
use rustfs_lock::{LockManager, LockHierarchy, LockOptions};

async fn hierarchical_locking_example() -> Result<(), Box<dyn std::error::Error>> {
    let lock_manager = LockManager::new("cluster-endpoint").await?;

    // Create lock hierarchy
    let hierarchy = LockHierarchy::new(vec![
        "global-lock".to_string(),
        "bucket-lock".to_string(),
        "object-lock".to_string(),
    ]);

    // Acquire locks in hierarchy order
    let locks = lock_manager.acquire_hierarchical_locks(
        hierarchy,
        LockOptions::default()
    ).await?;

    // Critical section with hierarchical locks
    {
        println!("All hierarchical locks acquired");
        // Perform operations that require the full lock hierarchy
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    // Locks are automatically released in reverse order
    locks.release_all().await?;

    Ok(())
}
```

## 🏗️ Architecture

### Lock Architecture

```
Lock Architecture:
┌─────────────────────────────────────────────────────────────┐
│                    Lock API Layer                           │
├─────────────────────────────────────────────────────────────┤
│   Mutex    │   Semaphore   │   Barrier   │   Condition     │
├─────────────────────────────────────────────────────────────┤
│              Lock Manager                                   │
├─────────────────────────────────────────────────────────────┤
│   Consensus   │   Heartbeat   │   Timeout   │   Recovery    │
├─────────────────────────────────────────────────────────────┤
│              Distributed Coordination                       │
└─────────────────────────────────────────────────────────────┘
```

### Lock Types

| Type | Use Case | Guarantees |
|------|----------|------------|
| Exclusive | Critical sections | Mutual exclusion |
| Shared | Reader-writer | Multiple readers |
| Semaphore | Resource pooling | Counting semaphore |
| Barrier | Synchronization | Coordination point |

## 🧪 Testing

Run the test suite:

```bash
# Run all tests
cargo test

# Test distributed locking
cargo test distributed_lock

# Test synchronization primitives
cargo test sync_primitives

# Test fault tolerance
cargo test fault_tolerance
```

## 📋 Requirements

- **Rust**: 1.70.0 or later
- **Platforms**: Linux, macOS, Windows
- **Network**: Cluster connectivity required
- **Consensus**: Raft consensus for critical operations

## 🌍 Related Projects

This module is part of the RustFS ecosystem:

- [RustFS Main](https://github.com/rustfs/rustfs) - Core distributed storage system
- [RustFS Common](../common) - Common types and utilities
- [RustFS Protos](../protos) - Protocol buffer definitions

## 📚 Documentation

For comprehensive documentation, visit:

- [RustFS Documentation](https://docs.rustfs.com)
- [Lock API Reference](https://docs.rustfs.com/lock/)
- [Distributed Systems Guide](https://docs.rustfs.com/distributed/)

## 🔗 Links

- [Documentation](https://docs.rustfs.com) - Complete RustFS manual
- [Changelog](https://github.com/rustfs/rustfs/releases) - Release notes and updates
- [GitHub Discussions](https://github.com/rustfs/rustfs/discussions) - Community support

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](https://github.com/rustfs/rustfs/blob/main/CONTRIBUTING.md) for details.

## 📄 License

Licensed under the Apache License, Version 2.0. See [LICENSE](https://github.com/rustfs/rustfs/blob/main/LICENSE) for details.

---

<p align="center">
  <strong>RustFS</strong> is a trademark of RustFS, Inc.<br>
  All other trademarks are the property of their respective owners.
</p>

<p align="center">
  Made with 🔒 by the RustFS Team
</p>
