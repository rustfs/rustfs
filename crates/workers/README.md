[![RustFS](https://rustfs.com/images/rustfs-github.png)](https://rustfs.com)

# RustFS Workers - Background Job Processing

<p align="center">
  <strong>Distributed background job processing system for RustFS object storage</strong>
</p>

<p align="center">
  <a href="https://github.com/rustfs/rustfs/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/rustfs/rustfs/actions/workflows/ci.yml/badge.svg" /></a>
  <a href="https://docs.rustfs.com/en/">📖 Documentation</a>
  · <a href="https://github.com/rustfs/rustfs/issues">🐛 Bug Reports</a>
  · <a href="https://github.com/rustfs/rustfs/discussions">💬 Discussions</a>
</p>

---

## 📖 Overview

**RustFS Workers** provides a distributed background job processing system for the [RustFS](https://rustfs.com) distributed object storage system. It handles asynchronous tasks such as data replication, cleanup, healing, indexing, and other maintenance operations across the cluster.

> **Note:** This is a core submodule of RustFS that provides essential background processing capabilities for the distributed object storage system. For the complete RustFS experience, please visit the [main RustFS repository](https://github.com/rustfs/rustfs).

## ✨ Features

### 🔄 Job Processing

- **Distributed Execution**: Jobs run across multiple cluster nodes
- **Priority Queues**: Multiple priority levels for job scheduling
- **Retry Logic**: Automatic retry with exponential backoff
- **Dead Letter Queue**: Failed job isolation and analysis

### 🛠️ Built-in Workers

- **Replication Worker**: Data replication across nodes
- **Cleanup Worker**: Garbage collection and cleanup
- **Healing Worker**: Data integrity repair
- **Indexing Worker**: Metadata indexing and search
- **Metrics Worker**: Performance metrics collection

### 🚀 Scalability Features

- **Horizontal Scaling**: Add worker nodes dynamically
- **Load Balancing**: Intelligent job distribution
- **Circuit Breaker**: Prevent cascading failures
- **Rate Limiting**: Control resource consumption

### 🔧 Management & Monitoring

- **Job Tracking**: Real-time job status monitoring
- **Health Checks**: Worker health and availability
- **Metrics Collection**: Performance and throughput metrics
- **Administrative Interface**: Job management and control

## 📦 Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
rustfs-workers = "0.1.0"
```

## 🔧 Usage

### Basic Worker Setup

```rust
use rustfs_workers::{WorkerManager, WorkerConfig, JobQueue};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create worker configuration
    let config = WorkerConfig {
        worker_id: "worker-1".to_string(),
        max_concurrent_jobs: 10,
        job_timeout: Duration::from_secs(300),
        retry_limit: 3,
        cleanup_interval: Duration::from_secs(60),
    };

    // Create worker manager
    let worker_manager = WorkerManager::new(config).await?;

    // Start worker processing
    worker_manager.start().await?;

    // Keep running
    tokio::signal::ctrl_c().await?;
    worker_manager.shutdown().await?;

    Ok(())
}
```

### Job Definition and Scheduling

```rust
use rustfs_workers::{Job, JobBuilder, JobPriority, JobQueue};
use serde::{Deserialize, Serialize};
use async_trait::async_trait;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationJob {
    pub source_path: String,
    pub target_nodes: Vec<String>,
    pub replication_factor: u32,
}

#[async_trait]
impl Job for ReplicationJob {
    async fn execute(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Starting replication job for: {}", self.source_path);

        // Perform replication logic
        for node in &self.target_nodes {
            self.replicate_to_node(node).await?;
        }

        println!("Replication job completed successfully");
        Ok(())
    }

    fn job_type(&self) -> &str {
        "replication"
    }

    fn max_retries(&self) -> u32 {
        3
    }
}

impl ReplicationJob {
    async fn replicate_to_node(&self, node: &str) -> Result<(), Box<dyn std::error::Error>> {
        // Implementation for replicating data to a specific node
        println!("Replicating {} to node: {}", self.source_path, node);
        tokio::time::sleep(Duration::from_secs(1)).await; // Simulate work
        Ok(())
    }
}

async fn schedule_replication_job() -> Result<(), Box<dyn std::error::Error>> {
    let job_queue = JobQueue::new().await?;

    // Create replication job
    let job = ReplicationJob {
        source_path: "/bucket/important-file.txt".to_string(),
        target_nodes: vec!["node-2".to_string(), "node-3".to_string()],
        replication_factor: 2,
    };

    // Schedule job with high priority
    let job_id = job_queue.schedule_job(
        Box::new(job),
        JobPriority::High,
        None, // Execute immediately
    ).await?;

    println!("Scheduled replication job with ID: {}", job_id);
    Ok(())
}
```

### Custom Worker Implementation

```rust
use rustfs_workers::{Worker, WorkerContext, JobResult};
use async_trait::async_trait;

pub struct CleanupWorker {
    storage_path: String,
    max_file_age: Duration,
}

#[async_trait]
impl Worker for CleanupWorker {
    async fn process_job(&self, job: Box<dyn Job>, context: &WorkerContext) -> JobResult {
        match job.job_type() {
            "cleanup" => {
                if let Some(cleanup_job) = job.as_any().downcast_ref::<CleanupJob>() {
                    self.execute_cleanup(cleanup_job, context).await
                } else {
                    JobResult::Failed("Invalid job type for cleanup worker".to_string())
                }
            }
            _ => JobResult::Skipped,
        }
    }

    async fn health_check(&self) -> bool {
        // Check if storage is accessible
        tokio::fs::metadata(&self.storage_path).await.is_ok()
    }

    fn worker_type(&self) -> &str {
        "cleanup"
    }
}

impl CleanupWorker {
    pub fn new(storage_path: String, max_file_age: Duration) -> Self {
        Self {
            storage_path,
            max_file_age,
        }
    }

    async fn execute_cleanup(&self, job: &CleanupJob, context: &WorkerContext) -> JobResult {
        println!("Starting cleanup job for: {}", job.target_path);

        match self.cleanup_old_files(&job.target_path).await {
            Ok(cleaned_count) => {
                context.update_metrics("files_cleaned", cleaned_count).await;
                JobResult::Success
            }
            Err(e) => JobResult::Failed(e.to_string()),
        }
    }

    async fn cleanup_old_files(&self, path: &str) -> Result<u64, Box<dyn std::error::Error>> {
        let mut cleaned_count = 0;
        // Implementation for cleaning up old files
        // ... cleanup logic ...
        Ok(cleaned_count)
    }
}
```

### Job Queue Management

```rust
use rustfs_workers::{JobQueue, JobFilter, JobStatus};

async fn job_queue_management() -> Result<(), Box<dyn std::error::Error>> {
    let job_queue = JobQueue::new().await?;

    // List pending jobs
    let pending_jobs = job_queue.list_jobs(JobFilter {
        status: Some(JobStatus::Pending),
        job_type: None,
        priority: None,
        limit: Some(100),
    }).await?;

    println!("Pending jobs: {}", pending_jobs.len());

    // Cancel a job
    let job_id = "job-123";
    job_queue.cancel_job(job_id).await?;

    // Retry failed jobs
    let failed_jobs = job_queue.list_jobs(JobFilter {
        status: Some(JobStatus::Failed),
        job_type: None,
        priority: None,
        limit: Some(50),
    }).await?;

    for job in failed_jobs {
        job_queue.retry_job(&job.id).await?;
    }

    // Get job statistics
    let stats = job_queue.get_statistics().await?;
    println!("Job statistics: {:?}", stats);

    Ok(())
}
```

### Distributed Worker Coordination

```rust
use rustfs_workers::{WorkerCluster, WorkerNode, ClusterConfig};

async fn distributed_worker_setup() -> Result<(), Box<dyn std::error::Error>> {
    let cluster_config = ClusterConfig {
        node_id: "worker-node-1".to_string(),
        cluster_endpoint: "https://cluster.rustfs.local".to_string(),
        heartbeat_interval: Duration::from_secs(30),
        leader_election_timeout: Duration::from_secs(60),
    };

    // Create worker cluster
    let cluster = WorkerCluster::new(cluster_config).await?;

    // Register worker types
    cluster.register_worker_type("replication", Box::new(ReplicationWorkerFactory)).await?;
    cluster.register_worker_type("cleanup", Box::new(CleanupWorkerFactory)).await?;
    cluster.register_worker_type("healing", Box::new(HealingWorkerFactory)).await?;

    // Start cluster participation
    cluster.join().await?;

    // Handle cluster events
    let mut event_receiver = cluster.event_receiver();

    tokio::spawn(async move {
        while let Some(event) = event_receiver.recv().await {
            match event {
                ClusterEvent::NodeJoined(node) => {
                    println!("Worker node joined: {}", node.id);
                }
                ClusterEvent::NodeLeft(node) => {
                    println!("Worker node left: {}", node.id);
                }
                ClusterEvent::LeadershipChanged(new_leader) => {
                    println!("New cluster leader: {}", new_leader);
                }
            }
        }
    });

    Ok(())
}
```

### Job Monitoring and Metrics

```rust
use rustfs_workers::{JobMonitor, WorkerMetrics, AlertConfig};

async fn job_monitoring_setup() -> Result<(), Box<dyn std::error::Error>> {
    let monitor = JobMonitor::new().await?;

    // Set up alerting
    let alert_config = AlertConfig {
        failed_job_threshold: 10,
        worker_down_threshold: Duration::from_secs(300),
        queue_size_threshold: 1000,
        notification_endpoint: "https://alerts.example.com/webhook".to_string(),
    };

    monitor.configure_alerts(alert_config).await?;

    // Start monitoring
    monitor.start_monitoring().await?;

    // Get real-time metrics
    let metrics = monitor.get_metrics().await?;
    println!("Worker metrics: {:?}", metrics);

    // Set up periodic reporting
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));

        loop {
            interval.tick().await;

            if let Ok(metrics) = monitor.get_metrics().await {
                println!("=== Worker Metrics ===");
                println!("Active jobs: {}", metrics.active_jobs);
                println!("Completed jobs: {}", metrics.completed_jobs);
                println!("Failed jobs: {}", metrics.failed_jobs);
                println!("Queue size: {}", metrics.queue_size);
                println!("Worker count: {}", metrics.worker_count);
            }
        }
    });

    Ok(())
}
```

## 🏗️ Architecture

### Workers Architecture

```
Workers Architecture:
┌─────────────────────────────────────────────────────────────┐
│                    Job Management API                       │
├─────────────────────────────────────────────────────────────┤
│   Scheduling  │   Monitoring   │   Queue Mgmt │   Metrics   │
├─────────────────────────────────────────────────────────────┤
│              Worker Coordination                            │
├─────────────────────────────────────────────────────────────┤
│   Job Queue   │   Load Balancer │   Health Check │   Retry  │
├─────────────────────────────────────────────────────────────┤
│              Distributed Execution                          │
└─────────────────────────────────────────────────────────────┘
```

### Worker Types

| Worker Type | Purpose | Characteristics |
|-------------|---------|----------------|
| Replication | Data replication | I/O intensive, network bound |
| Cleanup | Garbage collection | CPU intensive, periodic |
| Healing | Data repair | I/O intensive, high priority |
| Indexing | Metadata indexing | CPU intensive, background |
| Metrics | Performance monitoring | Low resource, continuous |

## 🧪 Testing

Run the test suite:

```bash
# Run all tests
cargo test

# Test job processing
cargo test job_processing

# Test worker coordination
cargo test worker_coordination

# Test distributed scenarios
cargo test distributed

# Integration tests
cargo test --test integration
```

## 📋 Requirements

- **Rust**: 1.70.0 or later
- **Platforms**: Linux, macOS, Windows
- **Network**: Cluster connectivity required
- **Storage**: Persistent queue storage recommended

## 🌍 Related Projects

This module is part of the RustFS ecosystem:

- [RustFS Main](https://github.com/rustfs/rustfs) - Core distributed storage system
- [RustFS Common](../common) - Common types and utilities
- [RustFS Lock](../lock) - Distributed locking

## 📚 Documentation

For comprehensive documentation, visit:

- [RustFS Documentation](https://docs.rustfs.com)
- [Workers API Reference](https://docs.rustfs.com/workers/)
- [Job Processing Guide](https://docs.rustfs.com/jobs/)

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
  Made with 🔄 by the RustFS Team
</p>
