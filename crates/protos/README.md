[![RustFS](https://rustfs.com/images/rustfs-github.png)](https://rustfs.com)

# RustFS Protos - Protocol Buffer Definitions

<p align="center">
  <strong>Protocol buffer definitions and gRPC interfaces for RustFS distributed object storage</strong>
</p>

<p align="center">
  <a href="https://github.com/rustfs/rustfs/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/rustfs/rustfs/actions/workflows/ci.yml/badge.svg" /></a>
  <a href="https://docs.rustfs.com/en/">📖 Documentation</a>
  · <a href="https://github.com/rustfs/rustfs/issues">🐛 Bug Reports</a>
  · <a href="https://github.com/rustfs/rustfs/discussions">💬 Discussions</a>
</p>

---

## 📖 Overview

**RustFS Protos** provides protocol buffer definitions and gRPC service interfaces for the [RustFS](https://rustfs.com) distributed object storage system. It defines the communication protocols, message formats, and service contracts used across all RustFS components.

> **Note:** This is a foundational submodule of RustFS that provides essential communication protocols for the distributed object storage system. For the complete RustFS experience, please visit the [main RustFS repository](https://github.com/rustfs/rustfs).

## ✨ Features

### 📡 gRPC Services

- **Storage Service**: Core storage operations (get, put, delete)
- **Admin Service**: Administrative and management operations
- **Metadata Service**: Metadata management and queries
- **Lock Service**: Distributed locking and coordination

### 📦 Message Types

- **Storage Messages**: Object and bucket operation messages
- **Administrative Messages**: Cluster management messages
- **Metadata Messages**: File and object metadata structures
- **Error Messages**: Standardized error reporting

### 🔧 Protocol Features

- **Versioning**: Protocol version compatibility management
- **Extensions**: Custom field extensions for future expansion
- **Streaming**: Support for streaming large data transfers
- **Compression**: Built-in message compression support

### 🛠️ Code Generation

- **Rust Bindings**: Automatic Rust code generation
- **Type Safety**: Strong typing for all protocol messages
- **Documentation**: Generated API documentation
- **Validation**: Message validation and constraints

## 📦 Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
rustfs-protos = "0.1.0"
```

## 🔧 Usage

### Basic gRPC Client

```rust
use rustfs_protos::storage::{StorageServiceClient, GetObjectRequest, PutObjectRequest};
use tonic::transport::Channel;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to storage service
    let channel = Channel::from_static("http://storage.rustfs.local:9000")
        .connect()
        .await?;

    let mut client = StorageServiceClient::new(channel);

    // Get object
    let request = GetObjectRequest {
        bucket: "example-bucket".to_string(),
        key: "example-object".to_string(),
        version_id: None,
        range: None,
    };

    let response = client.get_object(request).await?;
    let object_data = response.into_inner();

    println!("Retrieved object: {} bytes", object_data.content.len());
    println!("Content type: {}", object_data.content_type);
    println!("ETag: {}", object_data.etag);

    Ok(())
}
```

### Storage Operations

```rust
use rustfs_protos::storage::{
    StorageServiceClient, PutObjectRequest, DeleteObjectRequest,
    ListObjectsRequest, CreateBucketRequest
};

async fn storage_operations_example() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = StorageServiceClient::connect("http://storage.rustfs.local:9000").await?;

    // Create bucket
    let create_bucket_request = CreateBucketRequest {
        bucket: "new-bucket".to_string(),
        region: "us-east-1".to_string(),
        acl: None,
        storage_class: "STANDARD".to_string(),
    };

    client.create_bucket(create_bucket_request).await?;
    println!("Bucket created successfully");

    // Put object
    let put_request = PutObjectRequest {
        bucket: "new-bucket".to_string(),
        key: "test-file.txt".to_string(),
        content: b"Hello, RustFS!".to_vec(),
        content_type: "text/plain".to_string(),
        metadata: std::collections::HashMap::new(),
        storage_class: None,
    };

    let put_response = client.put_object(put_request).await?;
    println!("Object uploaded, ETag: {}", put_response.into_inner().etag);

    // List objects
    let list_request = ListObjectsRequest {
        bucket: "new-bucket".to_string(),
        prefix: None,
        marker: None,
        max_keys: Some(100),
        delimiter: None,
    };

    let list_response = client.list_objects(list_request).await?;
    let objects = list_response.into_inner();

    for object in objects.contents {
        println!("Object: {} (size: {} bytes)", object.key, object.size);
    }

    Ok(())
}
```

### Administrative Operations

```rust
use rustfs_protos::admin::{
    AdminServiceClient, GetServerInfoRequest, AddServerRequest,
    ListServersRequest, ConfigUpdateRequest
};

async fn admin_operations_example() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = AdminServiceClient::connect("http://admin.rustfs.local:9001").await?;

    // Get server information
    let info_request = GetServerInfoRequest {};
    let info_response = client.get_server_info(info_request).await?;
    let server_info = info_response.into_inner();

    println!("Server version: {}", server_info.version);
    println!("Uptime: {} seconds", server_info.uptime_seconds);
    println!("Memory usage: {} MB", server_info.memory_usage_mb);

    // List cluster servers
    let list_request = ListServersRequest {};
    let list_response = client.list_servers(list_request).await?;
    let servers = list_response.into_inner();

    for server in servers.servers {
        println!("Server: {} - Status: {}", server.endpoint, server.status);
    }

    // Add new server
    let add_request = AddServerRequest {
        endpoint: "https://new-node.rustfs.local:9000".to_string(),
        access_key: "node-access-key".to_string(),
        secret_key: "node-secret-key".to_string(),
    };

    client.add_server(add_request).await?;
    println!("New server added to cluster");

    Ok(())
}
```

### Metadata Operations

```rust
use rustfs_protos::metadata::{
    MetadataServiceClient, SearchObjectsRequest, GetObjectMetadataRequest,
    UpdateObjectMetadataRequest
};

async fn metadata_operations_example() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = MetadataServiceClient::connect("http://metadata.rustfs.local:9002").await?;

    // Search objects
    let search_request = SearchObjectsRequest {
        query: "content_type:image/*".to_string(),
        bucket: Some("photos-bucket".to_string()),
        limit: Some(50),
        offset: Some(0),
    };

    let search_response = client.search_objects(search_request).await?;
    let results = search_response.into_inner();

    for object in results.objects {
        println!("Found: {} ({})", object.key, object.content_type);
    }

    // Get object metadata
    let metadata_request = GetObjectMetadataRequest {
        bucket: "photos-bucket".to_string(),
        key: "vacation-photo.jpg".to_string(),
    };

    let metadata_response = client.get_object_metadata(metadata_request).await?;
    let metadata = metadata_response.into_inner();

    println!("Object metadata:");
    for (key, value) in metadata.metadata {
        println!("  {}: {}", key, value);
    }

    Ok(())
}
```

### Lock Service Operations

```rust
use rustfs_protos::lock::{
    LockServiceClient, AcquireLockRequest, ReleaseLockRequest,
    LockType, LockMode
};
use std::time::Duration;

async fn lock_operations_example() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = LockServiceClient::connect("http://lock.rustfs.local:9003").await?;

    // Acquire distributed lock
    let acquire_request = AcquireLockRequest {
        resource_id: "bucket/important-data".to_string(),
        lock_type: LockType::Exclusive as i32,
        timeout_seconds: 30,
        auto_renew: true,
    };

    let acquire_response = client.acquire_lock(acquire_request).await?;
    let lock_token = acquire_response.into_inner().lock_token;

    println!("Lock acquired: {}", lock_token);

    // Perform critical operations
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Release lock
    let release_request = ReleaseLockRequest {
        lock_token: lock_token.clone(),
    };

    client.release_lock(release_request).await?;
    println!("Lock released: {}", lock_token);

    Ok(())
}
```

### Streaming Operations

```rust
use rustfs_protos::storage::{StorageServiceClient, StreamUploadRequest, StreamDownloadRequest};
use tokio_stream::wrappers::ReceiverStream;

async fn streaming_operations_example() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = StorageServiceClient::connect("http://storage.rustfs.local:9000").await?;

    // Streaming upload
    let (tx, rx) = tokio::sync::mpsc::channel(100);
    let request_stream = ReceiverStream::new(rx);

    // Send upload metadata
    tx.send(StreamUploadRequest {
        bucket: "large-files".to_string(),
        key: "big-video.mp4".to_string(),
        content_type: "video/mp4".to_string(),
        chunk: vec![], // Empty chunk for metadata
    }).await?;

    // Send file chunks
    let mut file = tokio::fs::File::open("big-video.mp4").await?;
    let mut buffer = vec![0u8; 64 * 1024]; // 64KB chunks

    tokio::spawn(async move {
        loop {
            match file.read(&mut buffer).await {
                Ok(0) => break, // EOF
                Ok(n) => {
                    let chunk_request = StreamUploadRequest {
                        bucket: String::new(),
                        key: String::new(),
                        content_type: String::new(),
                        chunk: buffer[..n].to_vec(),
                    };

                    if tx.send(chunk_request).await.is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });

    let upload_response = client.stream_upload(request_stream).await?;
    println!("Upload completed: {}", upload_response.into_inner().etag);

    Ok(())
}
```

## 🏗️ Architecture

### Protocol Architecture

```
Protocol Architecture:
┌─────────────────────────────────────────────────────────────┐
│                    gRPC Services                            │
├─────────────────────────────────────────────────────────────┤
│  Storage API  │  Admin API   │  Metadata API │  Lock API   │
├─────────────────────────────────────────────────────────────┤
│              Protocol Buffer Messages                       │
├─────────────────────────────────────────────────────────────┤
│   Requests    │   Responses   │   Streaming   │   Errors    │
├─────────────────────────────────────────────────────────────┤
│              Transport Layer (HTTP/2)                       │
└─────────────────────────────────────────────────────────────┘
```

### Service Definitions

| Service | Purpose | Key Operations |
|---------|---------|----------------|
| Storage | Object operations | Get, Put, Delete, List |
| Admin | Cluster management | Add/Remove nodes, Config |
| Metadata | Metadata queries | Search, Index, Update |
| Lock | Distributed locking | Acquire, Release, Renew |

## 🧪 Testing

Run the test suite:

```bash
# Run all tests
cargo test

# Test protocol buffer compilation
cargo test proto_compilation

# Test service interfaces
cargo test service_interfaces

# Test message serialization
cargo test serialization
```

## 📋 Requirements

- **Rust**: 1.70.0 or later
- **Platforms**: Linux, macOS, Windows
- **Dependencies**: Protocol Buffers compiler (protoc)
- **Network**: gRPC transport support

## 🌍 Related Projects

This module is part of the RustFS ecosystem:

- [RustFS Main](https://github.com/rustfs/rustfs) - Core distributed storage system
- [RustFS Common](../common) - Common types and utilities
- [RustFS Lock](../lock) - Distributed locking

## 📚 Documentation

For comprehensive documentation, visit:

- [RustFS Documentation](https://docs.rustfs.com)
- [Protos API Reference](https://docs.rustfs.com/protos/)
- [gRPC Guide](https://docs.rustfs.com/grpc/)

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
  Made with 📡 by the RustFS Team
</p>
