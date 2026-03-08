# RustFS S3 Common

`rustfs-s3-common` provides shared types, utilities, and definitions for S3-compatible operations within the RustFS ecosystem. It serves as a foundational crate for handling S3 event notifications, metrics, and operation definitions.

## Features

- **Event Definitions**: Comprehensive `EventName` enum covering standard AWS S3 event notifications (e.g., `s3:ObjectCreated:Put`, `s3:ObjectRemoved:Delete`) and RustFS-specific extensions.
- **S3 Operations**: `S3Operation` enum defining supported S3 API actions, used for metrics tracking and audit logging.
- **Metrics Integration**: Utilities for recording S3 operation metrics (`record_s3_op`) using the `metrics` crate.
- **Type Mapping**: robust mapping between `EventName` and `S3Operation` to bridge the gap between API calls and event notifications.

## Usage

Add this crate to your `Cargo.toml`:

```toml
[dependencies]
rustfs-s3-common = { path = "../s3-common" }
```

### Event Names and Operations

```rust
use rustfs_s3_common::event_name::{EventName, S3Operation};

// Parse an event string
let event = EventName::parse("s3:ObjectCreated:Put").unwrap();
assert_eq!(event, EventName::ObjectCreatedPut);

// Map event to S3 operation
let op = event.to_s3_operation();
assert_eq!(op, Some(S3Operation::PutObject));

// Get string representation
assert_eq!(S3Operation::PutObject.as_str(), "s3:PutObject");
```

### Metrics

Initialize and record metrics for S3 operations:

```rust
use rustfs_s3_common::s3_metrics::{init_s3_metrics, record_s3_op};
use rustfs_s3_common::event_name::S3Operation;

// Initialize metrics (call once)
init_s3_metrics();

// Record an operation
record_s3_op(S3Operation::GetObject, "my-bucket");
```

## License

This project is licensed under the [Apache-2.0 License](../../LICENSE).
