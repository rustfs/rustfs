# RustFS Advanced Health & Metrics (AHM) System Architecture

## Overview

The RustFS AHM system is a newly designed distributed storage health monitoring and repair system that provides intelligent scanning, automatic repair, rich metrics, and policy-driven management capabilities.

## System Architecture

### Overall Architecture Diagram

```
┌─────────────────────────────────────┐
│          API Layer (REST/gRPC)      │
├─────────────────────────────────────┤
│       Policy & Configuration       │
├─────────────────────────────────────┤
│     Core Coordination Engine       │
├─────────────────────────────────────┤
│   Scanner Engine │  Heal Engine    │
├─────────────────────────────────────┤
│        Metrics & Observability     │
├─────────────────────────────────────┤
│         Storage Abstraction        │
└─────────────────────────────────────┘
```

### Module Structure

```
rustfs/crates/ecstore/src/ahm/
├── mod.rs                    # Module entry point and public interfaces
├── core/                     # Core engines
│   ├── coordinator.rs        # Distributed coordinator - event routing and state management
│   ├── scheduler.rs          # Task scheduler - priority queue and work assignment
│   └── lifecycle.rs          # Lifecycle manager - system startup/shutdown control
├── scanner/                  # Scanning system
│   ├── engine.rs            # Scan engine - scan process control
│   ├── object_scanner.rs     # Object scanner - object-level integrity checks
│   ├── disk_scanner.rs       # Disk scanner - disk-level health checks
│   ├── metrics_collector.rs  # Metrics collector - scan process data collection
│   └── bandwidth_limiter.rs  # Bandwidth limiter - I/O resource control
├── heal/                     # Repair system
│   ├── engine.rs            # Heal engine - repair process control
│   ├── priority_queue.rs     # Priority queue - repair task ordering
│   ├── repair_worker.rs      # Repair worker - actual repair execution
│   └── validation.rs         # Repair validator - repair result verification
├── metrics/                  # Metrics system
│   ├── collector.rs          # Metrics collector - real-time data collection
│   ├── aggregator.rs         # Metrics aggregator - data aggregation and computation
│   ├── storage.rs           # Metrics storage - time-series data storage
│   └── reporter.rs          # Metrics reporter - external system export
├── policy/                   # Policy system
│   ├── scan_policy.rs       # Scan policy - scan behavior configuration
│   ├── heal_policy.rs       # Heal policy - repair priority and strategy
│   └── retention_policy.rs  # Retention policy - data lifecycle management
└── api/                     # API interfaces
    ├── admin_api.rs         # Admin API - system management operations
    ├── metrics_api.rs       # Metrics API - metrics query and export
    └── status_api.rs        # Status API - system status monitoring
```

## Core Design Principles

### 1. Event-Driven Architecture

```rust
pub enum SystemEvent {
    ObjectDiscovered { bucket: String, object: String, metadata: ObjectMetadata },
    HealthIssueDetected { issue_type: HealthIssueType, severity: Severity },
    HealCompleted { result: HealResult },
    ScanCycleCompleted { statistics: ScanStatistics },
    ResourceUsageUpdated { usage: ResourceUsage },
}
```

- **Scanner** generates discovery events
- **Heal** responds to repair events  
- **Metrics** collects all event statistics
- **Policy** controls event processing strategies

### 2. Layered Modular Design

#### **API Layer**: REST/gRPC interfaces
- Unified response format
- Comprehensive error handling
- Authentication and authorization support

#### **Policy Layer**: Configurable business rules
- Scan frequency and depth control
- Repair priority policies
- Data retention rules

#### **Coordination Layer**: System coordination and scheduling
- Event routing and distribution
- Resource management and allocation
- Task scheduling and execution

#### **Engine Layer**: Core business logic
- Intelligent scanning algorithms
- Adaptive repair strategies
- Performance optimization control

#### **Metrics Layer**: Observability support
- Real-time metrics collection
- Historical trend analysis
- Multi-format export

### 3. Multi-Mode Scanning Strategies

```rust
pub enum ScanStrategy {
    Full { mode: ScanMode, scope: ScanScope },           // Full scan
    Incremental { since: Instant, mode: ScanMode },     // Incremental scan
    Smart { sample_rate: f64, favor_unscanned: bool },  // Smart sampling
    Targeted { targets: Vec<ObjectTarget>, mode: ScanMode }, // Targeted scan
}

pub enum ScanMode {
    Quick,    // Quick scan - metadata only
    Normal,   // Normal scan - basic integrity verification
    Deep,     // Deep scan - includes bit-rot detection
}
```

### 4. Priority-Based Repair System

```rust
pub enum HealPriority {
    Low = 0,
    Normal = 1,
    High = 2,
    Critical = 3,
    Emergency = 4,
}

pub enum HealMode {
    RealTime,    // Real-time repair - triggered on GET/PUT
    Background,  // Background repair - scheduled tasks
    OnDemand,    // On-demand repair - admin triggered
    Emergency,   // Emergency repair - critical issues
}
```

## API Usage Guide

### 1. System Management API

#### Start AHM System

```http
POST /admin/system/start
Content-Type: application/json

{
    "coordinator": {
        "event_buffer_size": 10000,
        "max_concurrent_operations": 1000
    },
    "scanner": {
        "default_scan_mode": "Normal",
        "scan_interval": "24h"
    },
    "heal": {
        "max_workers": 16,
        "queue_capacity": 50000
    }
}
```

**Response Example:**
```json
{
    "success": true,
    "data": {
        "system_id": "ahm-001",
        "status": "Running",
        "started_at": "2024-01-15T10:30:00Z"
    },
    "timestamp": "2024-01-15T10:30:00Z"
}
```

#### Get System Status

```http
GET /status/health
```

**Response Example:**
```json
{
    "success": true,
    "data": {
        "status": "Running",
        "version": "1.0.0",
        "uptime_seconds": 3600,
        "subsystems": {
            "scanner": {
                "status": "Scanning",
                "last_check": "2024-01-15T10:29:00Z",
                "error_message": null
            },
            "heal": {
                "status": "Idle",
                "last_check": "2024-01-15T10:29:00Z",
                "error_message": null
            },
            "metrics": {
                "status": "Running",
                "last_check": "2024-01-15T10:29:00Z",
                "error_message": null
            }
        }
    },
    "timestamp": "2024-01-15T10:30:00Z"
}
```

### 2. Scan Management API

#### Start Scan Task

```http
POST /admin/scan/start
Content-Type: application/json

{
    "strategy": {
        "type": "Full",
        "mode": "Normal",
        "scope": {
            "buckets": ["important-data", "user-uploads"],
            "include_system_objects": false,
            "max_objects": 1000000
        }
    },
    "priority": "High"
}
```

**Response Example:**
```json
{
    "success": true,
    "data": {
        "scan_id": "scan-12345",
        "status": "Started",
        "estimated_duration": "2h30m",
        "estimated_objects": 850000
    },
    "timestamp": "2024-01-15T10:30:00Z"
}
```

#### Query Scan Status

```http
GET /admin/scan/{scan_id}/status
```

**Response Example:**
```json
{
    "success": true,
    "data": {
        "scan_id": "scan-12345",
        "status": "Scanning",
        "progress": {
            "objects_scanned": 425000,
            "bytes_scanned": 1073741824000,
            "issues_detected": 23,
            "completion_percentage": 50.0,
            "scan_rate_ops": 117.5,
            "scan_rate_bps": 268435456,
            "elapsed_time": "1h15m",
            "estimated_remaining": "1h15m"
        },
        "issues": [
            {
                "issue_type": "MissingShards",
                "severity": "High",
                "bucket": "user-uploads",
                "object": "photos/IMG_001.jpg",
                "description": "Missing 1 data shard",
                "detected_at": "2024-01-15T11:15:00Z"
            }
        ]
    },
    "timestamp": "2024-01-15T11:45:00Z"
}
```

### 3. Heal Management API

#### Submit Heal Request

```http
POST /admin/heal/request
Content-Type: application/json

{
    "bucket": "user-uploads",
    "object": "photos/IMG_001.jpg",
    "version_id": null,
    "priority": "High",
    "mode": "OnDemand",
    "max_retries": 3
}
```

**Response Example:**
```json
{
    "success": true,
    "data": {
        "heal_request_id": "heal-67890",
        "status": "Queued",
        "priority": "High",
        "estimated_start": "2024-01-15T11:50:00Z",
        "queue_position": 5
    },
    "timestamp": "2024-01-15T11:45:00Z"
}
```

#### Query Heal Status

```http
GET /admin/heal/{heal_request_id}/status
```

**Response Example:**
```json
{
    "success": true,
    "data": {
        "heal_request_id": "heal-67890",
        "status": "Completed",
        "result": {
            "success": true,
            "shards_repaired": 1,
            "total_shards": 8,
            "duration": "45s",
            "strategy_used": "ParityShardRepair",
            "validation_results": [
                {
                    "validation_type": "Checksum",
                    "passed": true,
                    "details": "Object checksum verified",
                    "duration": "2s"
                },
                {
                    "validation_type": "ShardCount",
                    "passed": true,
                    "details": "All 8 shards present",
                    "duration": "1s"
                }
            ]
        }
    },
    "timestamp": "2024-01-15T11:46:00Z"
}
```

### 4. Metrics Query API

#### Get System Metrics

```http
GET /metrics/system?period=1h&metrics=objects_total,scan_rate,heal_success_rate
```

**Response Example:**
```json
{
    "success": true,
    "data": {
        "period": "1h",
        "timestamp_range": {
            "start": "2024-01-15T10:45:00Z",
            "end": "2024-01-15T11:45:00Z"
        },
        "metrics": {
            "objects_total": {
                "value": 2500000,
                "unit": "count",
                "labels": {}
            },
            "scan_rate_objects_per_second": {
                "value": 117.5,
                "unit": "ops",
                "labels": {}
            },
            "heal_success_rate": {
                "value": 0.98,
                "unit": "ratio",
                "labels": {}
            }
        }
    },
    "timestamp": "2024-01-15T11:45:00Z"
}
```

#### Export Prometheus Format Metrics

```http
GET /metrics/prometheus
```

**Response Example:**
```
# HELP rustfs_objects_total Total number of objects in the system
# TYPE rustfs_objects_total gauge
rustfs_objects_total 2500000

# HELP rustfs_scan_rate_objects_per_second Object scanning rate
# TYPE rustfs_scan_rate_objects_per_second gauge
rustfs_scan_rate_objects_per_second 117.5

# HELP rustfs_heal_success_rate Healing operation success rate
# TYPE rustfs_heal_success_rate gauge
rustfs_heal_success_rate 0.98

# HELP rustfs_health_issues_total Total health issues detected
# TYPE rustfs_health_issues_total counter
rustfs_health_issues_total{severity="critical"} 0
rustfs_health_issues_total{severity="high"} 3
rustfs_health_issues_total{severity="medium"} 15
rustfs_health_issues_total{severity="low"} 45
```

### 5. Policy Configuration API

#### Update Scan Policy

```http
PUT /admin/policy/scan
Content-Type: application/json

{
    "default_scan_interval": "12h",
    "deep_scan_probability": 0.1,
    "bandwidth_limit_mbps": 100,
    "concurrent_scanners": 4,
    "skip_system_objects": true,
    "priority_buckets": ["critical-data", "user-data"]
}
```

#### Update Heal Policy

```http
PUT /admin/policy/heal
Content-Type: application/json

{
    "max_concurrent_heals": 8,
    "emergency_heal_timeout": "5m",
    "auto_heal_enabled": true,
    "heal_verification_required": true,
    "priority_mapping": {
        "critical_buckets": "Emergency",
        "important_buckets": "High",
        "standard_buckets": "Normal"
    }
}
```

## Usage Examples

### Complete Monitoring and Repair Workflow

```bash
# 1. Start AHM system
curl -X POST http://localhost:9000/admin/system/start \
  -H "Content-Type: application/json" \
  -d '{"scanner": {"default_scan_mode": "Normal"}}'

# 2. Start full scan
SCAN_ID=$(curl -X POST http://localhost:9000/admin/scan/start \
  -H "Content-Type: application/json" \
  -d '{"strategy": {"type": "Full", "mode": "Normal"}}' | \
  jq -r '.data.scan_id')

# 3. Monitor scan progress
watch "curl -s http://localhost:9000/admin/scan/$SCAN_ID/status | jq '.data.progress'"

# 4. View discovered issues
curl -s http://localhost:9000/admin/scan/$SCAN_ID/status | \
  jq '.data.issues[]'

# 5. Start repair for discovered issues
HEAL_ID=$(curl -X POST http://localhost:9000/admin/heal/request \
  -H "Content-Type: application/json" \
  -d '{
    "bucket": "user-uploads",
    "object": "photos/IMG_001.jpg",
    "priority": "High"
  }' | jq -r '.data.heal_request_id')

# 6. Monitor repair progress
watch "curl -s http://localhost:9000/admin/heal/$HEAL_ID/status | jq '.data'"

# 7. View system metrics
curl -s http://localhost:9000/metrics/system?period=1h | jq '.data.metrics'

# 8. Export Prometheus metrics
curl -s http://localhost:9000/metrics/prometheus
```

## Key Features

### 1. Intelligent Scanning
- **Multi-level scan modes**: Quick/Normal/Deep three depths
- **Adaptive sampling**: Intelligent object selection based on historical data
- **Bandwidth control**: Configurable I/O resource limits
- **Incremental scanning**: Timestamp-based change detection

### 2. Intelligent Repair
- **Priority queue**: Repair ordering based on business importance
- **Multiple repair strategies**: Data shard, parity shard, hybrid repair
- **Real-time validation**: Post-repair integrity verification
- **Retry mechanism**: Configurable failure retry policies

### 3. Rich Metrics
- **Real-time statistics**: Object counts, storage usage, performance metrics
- **Historical trends**: Time-series data storage and analysis
- **Multi-format export**: Prometheus, JSON, CSV formats
- **Custom metrics**: Extensible metrics definition framework

### 4. Policy-Driven
- **Configurable policies**: Independent configuration for scan, heal, retention policies
- **Dynamic adjustment**: Runtime policy updates without restart
- **Business alignment**: Differentiated handling based on business importance

## Deployment Recommendations

### 1. Resource Configuration
- **CPU**: Recommended 16+ cores for parallel scanning and repair
- **Memory**: Recommended 32GB+ for metrics cache and task queues
- **Network**: Recommended gigabit+ bandwidth for cross-node data sync
- **Storage**: Recommended SSD for metrics data storage

### 2. Monitoring Integration
- **Prometheus**: Metrics collection and alerting
- **Grafana**: Visualization dashboards
- **ELK Stack**: Log aggregation and analysis
- **Jaeger**: Distributed tracing

### 3. High Availability Deployment
- **Multi-instance deployment**: Avoid single points of failure
- **Load balancing**: API request distribution
- **Data backup**: Metrics and configuration data backup
- **Failover**: Automatic failure detection and switching

This architecture design provides RustFS with modern, scalable, and highly observable health monitoring and repair capabilities that meet the operational requirements of enterprise-grade distributed storage systems. 