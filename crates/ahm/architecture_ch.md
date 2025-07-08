# RustFS Advanced Health & Metrics (AHM) 系统架构设计

## 概述

RustFS AHM 系统是一个全新设计的分布式存储健康监控和修复系统，提供智能扫描、自动修复、丰富指标和策略驱动的管理能力。

## 系统架构

### 整体架构图

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

### 模块结构

```
rustfs/crates/ecstore/src/ahm/
├── mod.rs                    # 模块入口和公共接口
├── core/                     # 核心引擎
│   ├── coordinator.rs        # 分布式协调器 - 事件路由和状态管理
│   ├── scheduler.rs          # 任务调度器 - 优先级队列和工作分配
│   └── lifecycle.rs          # 生命周期管理器 - 系统启停控制
├── scanner/                  # 扫描系统
│   ├── engine.rs            # 扫描引擎 - 扫描流程控制
│   ├── object_scanner.rs     # 对象扫描器 - 对象级完整性检查
│   ├── disk_scanner.rs       # 磁盘扫描器 - 磁盘级健康检查
│   ├── metrics_collector.rs  # 指标收集器 - 扫描过程数据收集
│   └── bandwidth_limiter.rs  # 带宽限制器 - I/O 资源控制
├── heal/                     # 修复系统
│   ├── engine.rs            # 修复引擎 - 修复流程控制
│   ├── priority_queue.rs     # 优先级队列 - 修复任务排序
│   ├── repair_worker.rs      # 修复工作器 - 实际修复执行
│   └── validation.rs         # 修复验证器 - 修复结果验证
├── metrics/                  # 指标系统
│   ├── collector.rs          # 指标收集器 - 实时数据收集
│   ├── aggregator.rs         # 指标聚合器 - 数据聚合计算
│   ├── storage.rs           # 指标存储器 - 时序数据存储
│   └── reporter.rs          # 指标报告器 - 外部系统导出
├── policy/                   # 策略系统
│   ├── scan_policy.rs       # 扫描策略 - 扫描行为配置
│   ├── heal_policy.rs       # 修复策略 - 修复优先级和策略
│   └── retention_policy.rs  # 保留策略 - 数据生命周期管理
└── api/                     # API接口
    ├── admin_api.rs         # 管理API - 系统管理操作
    ├── metrics_api.rs       # 指标API - 指标查询和导出
    └── status_api.rs        # 状态API - 系统状态监控
```

## 核心设计理念

### 1. 事件驱动架构

```rust
pub enum SystemEvent {
    ObjectDiscovered { bucket: String, object: String, metadata: ObjectMetadata },
    HealthIssueDetected { issue_type: HealthIssueType, severity: Severity },
    HealCompleted { result: HealResult },
    ScanCycleCompleted { statistics: ScanStatistics },
    ResourceUsageUpdated { usage: ResourceUsage },
}
```

- **Scanner** 产生发现事件
- **Heal** 响应修复事件  
- **Metrics** 收集所有事件统计
- **Policy** 控制事件处理策略

### 2. 分层模块化设计

#### **API层**: REST/gRPC接口
- 统一的响应格式
- 完整的错误处理
- 认证和授权支持

#### **策略层**: 可配置的业务规则
- 扫描频率和深度控制
- 修复优先级策略
- 数据保留规则

#### **协调层**: 系统协调和调度
- 事件路由分发
- 资源管理分配
- 任务调度执行

#### **引擎层**: 核心业务逻辑
- 智能扫描算法
- 自适应修复策略
- 性能优化控制

#### **指标层**: 可观测性支持
- 实时指标收集
- 历史趋势分析
- 多格式导出

### 3. 多模式扫描策略

```rust
pub enum ScanStrategy {
    Full { mode: ScanMode, scope: ScanScope },           // 全量扫描
    Incremental { since: Instant, mode: ScanMode },     // 增量扫描
    Smart { sample_rate: f64, favor_unscanned: bool },  // 智能采样
    Targeted { targets: Vec<ObjectTarget>, mode: ScanMode }, // 定向扫描
}

pub enum ScanMode {
    Quick,    // 快速扫描 - 仅元数据检查
    Normal,   // 标准扫描 - 基础完整性验证
    Deep,     // 深度扫描 - 包含位腐蚀检测
}
```

### 4. 优先级修复系统

```rust
pub enum HealPriority {
    Low = 0,
    Normal = 1,
    High = 2,
    Critical = 3,
    Emergency = 4,
}

pub enum HealMode {
    RealTime,    // 实时修复 - GET/PUT时触发
    Background,  // 后台修复 - 计划任务
    OnDemand,    // 按需修复 - 管理员触发
    Emergency,   // 紧急修复 - 关键问题
}
```

## API 使用指南

### 1. 系统管理 API

#### 启动 AHM 系统

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

**响应示例:**
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

#### 获取系统状态

```http
GET /status/health
```

**响应示例:**
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

### 2. 扫描管理 API

#### 启动扫描任务

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

**响应示例:**
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

#### 查询扫描状态

```http
GET /admin/scan/{scan_id}/status
```

**响应示例:**
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

### 3. 修复管理 API

#### 提交修复请求

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

**响应示例:**
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

#### 查询修复状态

```http
GET /admin/heal/{heal_request_id}/status
```

**响应示例:**
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

### 4. 指标查询 API

#### 获取系统指标

```http
GET /metrics/system?period=1h&metrics=objects_total,scan_rate,heal_success_rate
```

**响应示例:**
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

#### 导出 Prometheus 格式指标

```http
GET /metrics/prometheus
```

**响应示例:**
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

### 5. 策略配置 API

#### 更新扫描策略

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

#### 更新修复策略

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

## 使用示例

### 完整的监控和修复流程

```bash
# 1. 启动 AHM 系统
curl -X POST http://localhost:9000/admin/system/start \
  -H "Content-Type: application/json" \
  -d '{"scanner": {"default_scan_mode": "Normal"}}'

# 2. 启动全量扫描
SCAN_ID=$(curl -X POST http://localhost:9000/admin/scan/start \
  -H "Content-Type: application/json" \
  -d '{"strategy": {"type": "Full", "mode": "Normal"}}' | \
  jq -r '.data.scan_id')

# 3. 监控扫描进度
watch "curl -s http://localhost:9000/admin/scan/$SCAN_ID/status | jq '.data.progress'"

# 4. 查看发现的问题
curl -s http://localhost:9000/admin/scan/$SCAN_ID/status | \
  jq '.data.issues[]'

# 5. 针对发现的问题启动修复
HEAL_ID=$(curl -X POST http://localhost:9000/admin/heal/request \
  -H "Content-Type: application/json" \
  -d '{
    "bucket": "user-uploads",
    "object": "photos/IMG_001.jpg",
    "priority": "High"
  }' | jq -r '.data.heal_request_id')

# 6. 监控修复进度
watch "curl -s http://localhost:9000/admin/heal/$HEAL_ID/status | jq '.data'"

# 7. 查看系统指标
curl -s http://localhost:9000/metrics/system?period=1h | jq '.data.metrics'

# 8. 导出 Prometheus 指标
curl -s http://localhost:9000/metrics/prometheus
```

## 关键特性

### 1. 智能扫描
- **多级扫描模式**: Quick/Normal/Deep 三种深度
- **自适应采样**: 基于历史数据智能选择扫描对象
- **带宽控制**: 可配置的 I/O 资源限制
- **增量扫描**: 基于时间戳的变化检测

### 2. 智能修复
- **优先级队列**: 基于业务重要性的修复排序
- **多种修复策略**: 数据分片、奇偶校验、混合修复
- **实时验证**: 修复后的完整性验证
- **重试机制**: 可配置的失败重试策略

### 3. 丰富指标
- **实时统计**: 对象数量、存储使用、性能指标
- **历史趋势**: 时序数据存储和分析
- **多格式导出**: Prometheus、JSON、CSV 等格式
- **自定义指标**: 可扩展的指标定义框架

### 4. 策略驱动
- **可配置策略**: 扫描、修复、保留策略独立配置
- **动态调整**: 运行时策略更新，无需重启
- **业务对齐**: 基于业务重要性的差异化处理

## 部署建议

### 1. 资源配置
- **CPU**: 推荐 16+ 核心用于并行扫描和修复
- **内存**: 推荐 32GB+ 用于指标缓存和任务队列
- **网络**: 推荐千兆以上带宽用于跨节点数据同步
- **存储**: 推荐 SSD 用于指标数据存储

### 2. 监控集成
- **Prometheus**: 指标收集和告警
- **Grafana**: 可视化仪表板
- **ELK Stack**: 日志聚合和分析
- **Jaeger**: 分布式链路追踪

### 3. 高可用部署
- **多实例部署**: 避免单点故障
- **负载均衡**: API 请求分发
- **数据备份**: 指标和配置数据备份
- **故障转移**: 自动故障检测和切换

这个架构设计为 RustFS 提供了现代化、可扩展、高可观测的健康监控和修复能力，能够满足企业级分布式存储系统的运维需求。 