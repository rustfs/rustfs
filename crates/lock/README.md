# RustFS Lock - Distributed Locking

**RustFS Lock** provides distributed locking and synchronization primitives for the [RustFS](https://rustfs.com) distributed object storage system. It ensures data consistency and prevents race conditions in multi-node environments through various locking mechanisms and coordination protocols.

## Features

- **Distributed Mutex**: Cross-node mutual exclusion
- **Shared Locks**: Reader-writer lock semantics
- **Timeout Support**: Configurable lock timeouts and expiration
- **Deadlock Prevention**: Automatic deadlock detection and resolution
- **Fault Tolerance**: Automatic recovery from node failures
- **Lock Coalescing**: Efficient batching of lock operations

## Documentation

For complete documentation, examples, and usage information, please visit the main [RustFS repository](https://github.com/rustfs/rustfs).

## License

This project is licensed under the Apache License, Version 2.0.
