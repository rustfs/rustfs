# RustFS 元数据中心多副本架构设计

**版本**: 1.0  
**日期**: 2026-02-24  
**作者**: RustFS Architecture Team  
**状态**: Design Proposal

---

## 目录

1. [执行摘要](#执行摘要)
2. [架构概述](#架构概述)
3. [核心设计原则](#核心设计原则)
4. [多副本架构](#多副本架构)
5. [一致性模型](#一致性模型)
6. [复制协议](#复制协议)
7. [故障处理](#故障处理)
8. [实现方案](#实现方案)
9. [性能优化](#性能优化)
10. [运维管理](#运维管理)

---

## 执行摘要

本文档描述如何为 RustFS 的高性能 KV 元数据中心（SurrealKV + Ferntree + SurrealMX）设计多副本支持，以实现：

- ✅ **高可用性**: 多副本容错，自动故障转移
- ✅ **数据持久性**: 多节点数据冗余
- ✅ **读性能扩展**: 副本分担读负载
- ✅ **一致性保证**: 强一致性或最终一致性可选

### 关键指标

| 指标     | 目标值     | 说明           |
|--------|---------|--------------|
| 副本数量   | 3-5     | 可配置，推荐 3 副本  |
| 写入延迟   | < 10ms  | 主节点确认 + 异步复制 |
| 一致性级别  | 强一致性    | Raft 共识协议    |
| 故障转移时间 | < 30s   | 自动选主 + 状态恢复  |
| 数据同步延迟 | < 100ms | 副本间数据同步      |

### 设计目标

1. **保持单机性能**: 不降低现有元数据引擎的性能
2. **透明复制**: 对上层应用透明，无需修改 API
3. **渐进式迁移**: 支持从单节点平滑升级到多节点
4. **模块化设计**: 复制层与存储层解耦

---

## 架构概述

### 当前架构 (单节点)

```
┌─────────────────────────────────────────────────────────┐
│                  S3 Application Layer                   │
└────────────────────────┬────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────┐
│              LocalMetadataEngine                         │
│  ┌──────────────┐  ┌─────────────┐  ┌────────────────┐ │
│  │  SurrealKV   │  │  Ferntree   │  │   SurrealMX    │ │
│  │   (ACID)     │  │  (B+ Tree)  │  │   (Storage)    │ │
│  └──────────────┘  └─────────────┘  └────────────────┘ │
└─────────────────────────────────────────────────────────┘
                         │
                         ▼
                  Local File System
```

**问题**:

- ❌ 单点故障（SPOF）
- ❌ 无数据冗余
- ❌ 无法水平扩展读能力

### 多副本架构 (目标)

```
                    ┌───────────────────────────────────┐
                    │     S3 Application Layer          │
                    └───────────────┬───────────────────┘
                                    │
                                    ▼
┌───────────────────────────────────────────────────────────────┐
│                    ReplicatedMetadataEngine                   │
│                  (Raft Consensus + Replication)               │
└──────┬──────────────────────┬──────────────────────┬─────────┘
       │                      │                      │
       ▼                      ▼                      ▼
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│   Node 1        │  │   Node 2        │  │   Node 3        │
│   (Leader)      │  │   (Follower)    │  │   (Follower)    │
│                 │  │                 │  │                 │
│ LocalMetadata   │  │ LocalMetadata   │  │ LocalMetadata   │
│ Engine          │  │ Engine          │  │ Engine          │
│  ├─SurrealKV    │  │  ├─SurrealKV    │  │  ├─SurrealKV    │
│  ├─Ferntree     │  │  ├─Ferntree     │  │  ├─Ferntree     │
│  └─SurrealMX    │  │  └─SurrealMX    │  │  └─SurrealMX    │
└─────────────────┘  └─────────────────┘  └─────────────────┘
       │                      │                      │
       ▼                      ▼                      ▼
   Local FS             Local FS             Local FS
```

**优势**:

- ✅ 高可用：任意节点故障不影响服务
- ✅ 数据冗余：多副本保证数据持久性
- ✅ 读扩展：副本分担读负载
- ✅ 一致性：Raft 保证强一致性

---

## 核心设计原则

### 1. 分层设计

```
┌─────────────────────────────────────────────────────────┐
│  Layer 4: Application API                               │
│  (S3 Compatible Interface)                              │
└────────────────────────┬────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────┐
│  Layer 3: Replication Coordination                      │
│  (Raft Consensus + Request Routing)                     │
└────────────────────────┬────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────┐
│  Layer 2: Metadata Engine                               │
│  (LocalMetadataEngine - Existing)                       │
└────────────────────────┬────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────┐
│  Layer 1: Storage Backends                              │
│  (SurrealKV + Ferntree + SurrealMX)                     │
└─────────────────────────────────────────────────────────┘
```

**关键点**:

- Layer 1-2: 保持不变（现有元数据引擎）
- Layer 3: 新增复制协调层
- Layer 4: API 保持兼容

### 2. 复制单元

元数据复制以 **事务（Transaction）** 为单位：

```rust
pub struct MetadataOperation {
    op_id: u64,                    // 操作 ID（单调递增）
    op_type: OperationType,        // 操作类型
    timestamp: i64,                // 时间戳
    data: Vec<u8>,                 // 序列化的操作数据
}

pub enum OperationType {
    PutObject,      // 创建/更新对象
    DeleteObject,   // 删除对象
    UpdateMetadata, // 更新元数据
    BatchOps,       // 批量操作
}
```

### 3. 一致性级别

支持两种一致性模型，可配置：

| 级别        | 写入确认条件 | 读取保证 | 性能 | 适用场景    |
|-----------|--------|------|----|---------|
| **强一致性**  | 多数副本确认 | 最新数据 | 中  | 金融、关键业务 |
| **最终一致性** | 主副本确认  | 可能滞后 | 高  | 日志、监控数据 |

**推荐配置**: 默认使用强一致性，关键元数据不可妥协。

---

## 多副本架构

### 1. 核心组件

#### 1.1 ReplicatedMetadataEngine

复制协调的顶层接口：

```rust
/// ReplicatedMetadataEngine wraps LocalMetadataEngine with replication support.
pub struct ReplicatedMetadataEngine {
    /// Local metadata engine (existing)
    local_engine: Arc<LocalMetadataEngine>,

    /// Raft node for consensus
    raft_node: Arc<RaftNode>,

    /// Replication manager
    replication_manager: Arc<ReplicationManager>,

    /// Configuration
    config: ReplicationConfig,
}

#[async_trait]
impl MetadataEngine for ReplicatedMetadataEngine {
    async fn put_object(...) -> Result<ObjectInfo> {
        // 1. Serialize operation
        let op = MetadataOperation::new(OperationType::PutObject, ...);

        // 2. Propose to Raft
        self.raft_node.propose(op).await?;

        // 3. Wait for commit (majority)
        let result = self.raft_node.wait_committed(op.op_id).await?;

        // 4. Apply to local engine
        self.local_engine.put_object(...).await
    }

    async fn get_object_reader(...) -> Result<GetObjectReader> {
        // Read from local replica (no consensus needed)
        self.local_engine.get_object_reader(...).await
    }

    // ... other methods
}
```

#### 1.2 RaftNode

基于 Raft 共识协议的节点实现：

```rust
use raft::{Config, Node, Storage, RawNode};
use raft::prelude::*;

pub struct RaftNode {
    /// Raft raw node
    raw_node: Arc<Mutex<RawNode<MemStorage>>>,

    /// Node ID
    node_id: u64,

    /// Peer addresses
    peers: HashMap<u64, String>,

    /// Applied operation index
    applied_index: AtomicU64,

    /// Operation log
    op_log: Arc<RwLock<VecDeque<MetadataOperation>>>,

    /// Commit notifier
    commit_notifier: Arc<Notify>,
}

impl RaftNode {
    /// Propose an operation to the Raft cluster
    pub async fn propose(&self, op: MetadataOperation) -> Result<()> {
        let data = serde_json::to_vec(&op)?;

        let mut raw_node = self.raw_node.lock().await;
        raw_node.propose(vec![], data)?;

        Ok(())
    }

    /// Wait for an operation to be committed
    pub async fn wait_committed(&self, op_id: u64) -> Result<()> {
        loop {
            if self.applied_index.load(Ordering::SeqCst) >= op_id {
                return Ok(());
            }

            // Wait for commit notification
            self.commit_notifier.notified().await;
        }
    }

    /// Process Raft ready state
    pub async fn process_ready(&self, local_engine: &LocalMetadataEngine) -> Result<()> {
        let mut raw_node = self.raw_node.lock().await;

        if !raw_node.has_ready() {
            return Ok(());
        }

        let mut ready = raw_node.ready();

        // Send messages to peers
        for msg in ready.take_messages() {
            self.send_to_peer(msg).await?;
        }

        // Apply committed entries
        for entry in ready.take_committed_entries() {
            if entry.data.is_empty() {
                continue;
            }

            let op: MetadataOperation = serde_json::from_slice(&entry.data)?;

            // Apply to local engine
            self.apply_operation(local_engine, op).await?;

            // Update applied index
            self.applied_index.store(entry.index, Ordering::SeqCst);
        }

        // Persist snapshot
        if !ready.snapshot().is_empty() {
            self.save_snapshot(ready.snapshot()).await?;
        }

        // Advance Raft
        let mut light_rd = raw_node.advance(ready);

        // Apply updates
        if let Some(commit) = light_rd.commit_index() {
            // Notify waiters
            self.commit_notifier.notify_waiters();
        }

        Ok(())
    }

    /// Apply operation to local engine
    async fn apply_operation(
        &self,
        engine: &LocalMetadataEngine,
        op: MetadataOperation,
    ) -> Result<()> {
        match op.op_type {
            OperationType::PutObject => {
                // Deserialize and apply
                let put_req: PutObjectRequest = serde_json::from_slice(&op.data)?;
                engine.put_object(
                    &put_req.bucket,
                    &put_req.key,
                    Box::new(Cursor::new(put_req.data)),
                    put_req.size,
                    put_req.opts,
                ).await?;
            }
            OperationType::DeleteObject => {
                let del_req: DeleteObjectRequest = serde_json::from_slice(&op.data)?;
                engine.delete_object(&del_req.bucket, &del_req.key).await?;
            }
            // ... handle other operations
            _ => {}
        }

        Ok(())
    }
}
```

#### 1.3 ReplicationManager

管理副本同步和健康检查：

```rust
pub struct ReplicationManager {
    /// Local node ID
    node_id: u64,

    /// Peer connections
    peers: Arc<RwLock<HashMap<u64, PeerConnection>>>,

    /// Replication state
    state: Arc<RwLock<ReplicationState>>,

    /// Health checker
    health_checker: Arc<HealthChecker>,
}

pub struct PeerConnection {
    node_id: u64,
    address: String,
    client: ReplicationClient,
    last_heartbeat: AtomicI64,
    status: Arc<RwLock<PeerStatus>>,
}

pub enum PeerStatus {
    Healthy,
    Lagging { behind: u64 },
    Unreachable,
    Failed,
}

impl ReplicationManager {
    /// Start replication manager
    pub async fn start(&self, raft_node: Arc<RaftNode>) {
        // Background task: process Raft ready
        tokio::spawn(Self::raft_ready_loop(raft_node.clone()));

        // Background task: health check
        tokio::spawn(Self::health_check_loop(self.health_checker.clone()));

        // Background task: catch-up lagging replicas
        tokio::spawn(Self::catchup_loop(self.clone()));
    }

    async fn raft_ready_loop(raft_node: Arc<RaftNode>) {
        let mut interval = tokio::time::interval(Duration::from_millis(10));

        loop {
            interval.tick().await;

            if let Err(e) = raft_node.process_ready(&local_engine).await {
                error!("Failed to process Raft ready: {}", e);
            }
        }
    }

    async fn health_check_loop(health_checker: Arc<HealthChecker>) {
        let mut interval = tokio::time::interval(Duration::from_secs(1));

        loop {
            interval.tick().await;
            health_checker.check_all_peers().await;
        }
    }
}
```

### 2. 数据流

#### 2.1 写入流程 (Put Object)

```
Client
  │
  │ 1. PUT /bucket/key
  ▼
ReplicatedMetadataEngine (Leader)
  │
  │ 2. Create MetadataOperation
  ▼
RaftNode
  │
  │ 3. Propose to cluster
  ├─────────────────────┬─────────────────────┐
  │                     │                     │
  ▼                     ▼                     ▼
Node 1 (Leader)    Node 2 (Follower)   Node 3 (Follower)
  │                     │                     │
  │ 4. Append to log    │ 4. Append to log    │ 4. Append to log
  │                     │                     │
  │ 5. Commit (majority: 2/3)                 │
  │◄────────────────────┤◄────────────────────┤
  │                     │                     │
  │ 6. Apply to LocalMetadataEngine           │
  ├─────────────────────┼─────────────────────┤
  │                     │                     │
  ▼                     ▼                     ▼
SurrealKV           SurrealKV           SurrealKV
Ferntree            Ferntree            Ferntree
SurrealMX           SurrealMX           SurrealMX
  │                     │                     │
  │ 7. Return success (after majority commit) │
  │◄────────────────────────────────────────────┘
  │
  │ 8. Response to client
  ▼
Client
```

**延迟分析**:

- Raft propose: ~1ms
- Network RTT (2 nodes): ~2ms
- Majority commit: ~3ms
- Apply to local engine: ~5ms
- **Total: ~10ms**

#### 2.2 读取流程 (Get Object)

```
Client
  │
  │ 1. GET /bucket/key
  ▼
ReplicatedMetadataEngine (Any Node)
  │
  │ 2. Read from local replica (no consensus)
  ▼
LocalMetadataEngine
  │
  ▼
SurrealKV (Metadata) + SurrealMX (Data)
  │
  │ 3. Return data
  ▼
Client
```

**优势**:

- ✅ 读取无需共识，直接从本地读
- ✅ 副本分担读负载
- ✅ 延迟极低（~1ms）

---

## 一致性模型

### 1. 强一致性（推荐）

**实现**: 基于 Raft 线性一致性保证

**写入路径**:

```
1. Client → Leader
2. Leader proposes to Raft
3. Wait for majority commit (2/3 nodes)
4. Apply to local engine
5. Return success
```

**读取路径**:

```
Option 1: Read from Leader (强一致性)
  - 直接读取 Leader 本地数据
  - 保证读到最新已提交数据

Option 2: Read from Follower (弱一致性)
  - 可能读到稍旧的数据
  - 延迟极低
  - 适合可容忍短暂不一致的场景
```

### 2. 读取一致性级别

```rust
pub enum ReadConsistency {
    /// 从任意副本读（最快，可能不一致）
    Eventual,

    /// 从 Leader 读（强一致性）
    Linearizable,

    /// 从本地读，但先同步 commit index（折中）
    BoundedStaleness { max_staleness_ms: u64 },
}

impl ReplicatedMetadataEngine {
    pub async fn get_object_with_consistency(
        &self,
        bucket: &str,
        key: &str,
        consistency: ReadConsistency,
    ) -> Result<ObjectInfo> {
        match consistency {
            ReadConsistency::Eventual => {
                // 直接从本地读
                self.local_engine.get_object(bucket, key, ObjectOptions::default()).await
            }
            ReadConsistency::Linearizable => {
                // 确保读到最新数据
                if !self.raft_node.is_leader() {
                    // Forward to leader
                    return self.forward_to_leader(bucket, key).await;
                }

                // Read from leader
                self.local_engine.get_object(bucket, key, ObjectOptions::default()).await
            }
            ReadConsistency::BoundedStaleness { max_staleness_ms } => {
                // 检查本地滞后程度
                let staleness = self.raft_node.staleness_ms();
                if staleness > max_staleness_ms {
                    // Wait for catch-up
                    self.raft_node.wait_catchup(max_staleness_ms).await?;
                }

                self.local_engine.get_object(bucket, key, ObjectOptions::default()).await
            }
        }
    }
}
```

---

## 复制协议

### 1. Raft 共识协议

**选择理由**:

- ✅ 强一致性保证
- ✅ 成熟稳定（etcd、TiKV 使用）
- ✅ Rust 生态完善（`raft-rs` crate）
- ✅ 易于理解和调试

**核心概念**:

```
┌────────────────────────────────────────────────────────┐
│                    Raft Cluster                        │
├────────────────────────────────────────────────────────┤
│                                                        │
│  ┌────────────┐    ┌────────────┐    ┌────────────┐  │
│  │  Leader    │───▶│ Follower   │    │ Follower   │  │
│  │  (Node 1)  │◀───│ (Node 2)   │    │ (Node 3)   │  │
│  │            │    │            │    │            │  │
│  │  Term: 5   │    │  Term: 5   │    │  Term: 5   │  │
│  │  Log: [...]│    │  Log: [...]│    │  Log: [...]│  │
│  └────────────┘    └────────────┘    └────────────┘  │
│                                                        │
│  角色:                                                  │
│  • Leader: 接收客户端请求，复制日志                       │
│  • Follower: 被动接收日志，参与投票                      │
│  • Candidate: 选举中的候选者                            │
│                                                        │
│  日志复制:                                              │
│  1. Leader 接收操作 → Append to local log              │
│  2. Leader → Followers (AppendEntries RPC)            │
│  3. Majority ACK → Commit                             │
│  4. Apply to state machine                            │
│                                                        │
└────────────────────────────────────────────────────────┘
```

### 2. 日志结构

```rust
pub struct LogEntry {
    /// Log index (monotonic increasing)
    index: u64,

    /// Raft term
    term: u64,

    /// Entry type
    entry_type: EntryType,

    /// Serialized metadata operation
    data: Vec<u8>,

    /// Checksum
    checksum: u32,
}

pub enum EntryType {
    Normal,       // Regular operation
    ConfChange,   // Cluster configuration change
    Snapshot,     // Snapshot marker
}
```

### 3. 快照机制

为避免日志无限增长，定期创建快照：

```rust
pub struct MetadataSnapshot {
    /// Snapshot version
    version: u64,

    /// Last included index
    last_index: u64,

    /// Last included term
    last_term: u64,

    /// Full KV store dump
    kv_dump: Vec<u8>,

    /// Index tree dump
    index_dump: Vec<u8>,

    /// Timestamp
    created_at: i64,
}

impl ReplicatedMetadataEngine {
    /// Create snapshot
    pub async fn create_snapshot(&self) -> Result<MetadataSnapshot> {
        // 1. Get current Raft state
        let (last_index, last_term) = self.raft_node.get_applied_state();

        // 2. Export KV store
        let kv_dump = self.export_kv_store().await?;

        // 3. Export index tree
        let index_dump = self.export_index_tree().await?;

        Ok(MetadataSnapshot {
            version: 1,
            last_index,
            last_term,
            kv_dump,
            index_dump,
            created_at: now(),
        })
    }

    /// Apply snapshot
    pub async fn apply_snapshot(&self, snapshot: MetadataSnapshot) -> Result<()> {
        // 1. Clear existing data
        self.local_engine.clear().await?;

        // 2. Import KV store
        self.import_kv_store(&snapshot.kv_dump).await?;

        // 3. Import index tree
        self.import_index_tree(&snapshot.index_dump).await?;

        // 4. Update Raft state
        self.raft_node.set_applied(snapshot.last_index, snapshot.last_term);

        Ok(())
    }
}
```

**触发条件**:

- 日志条目数 > 10,000
- 日志大小 > 100MB
- 或手动触发

---

## 故障处理

### 1. 节点故障

#### 1.1 Follower 故障

```
┌────────────────────────────────────────────┐
│  Scenario: Follower Node 2 crashes         │
└────────────────────────────────────────────┘

Before:
  Leader (Node 1) ─── Follower (Node 2) ✓
                  └── Follower (Node 3) ✓

After:
  Leader (Node 1) ─── Follower (Node 2) ✗ (Down)
                  └── Follower (Node 3) ✓

Impact:
  • Writes still succeed (2/3 majority)
  • Reads from Node 2 fail → Client retry
  • System continues normally

Recovery:
  1. Node 2 restarts
  2. Connects to Leader
  3. Catch up missing logs
  4. Resume normal operation
```

#### 1.2 Leader 故障

```
┌────────────────────────────────────────────┐
│  Scenario: Leader Node 1 crashes           │
└────────────────────────────────────────────┘

Before:
  Leader (Node 1) ✓
    └── Follower (Node 2) ✓
    └── Follower (Node 3) ✓

Failure Detection:
  • Node 2/3 don't receive heartbeat for election_timeout
  • Node 2/3 transition to Candidate

Election:
  1. Node 2 → Candidate (Term: 6)
  2. Node 2 requests vote from Node 3
  3. Node 3 grants vote
  4. Node 2 becomes Leader (Term: 6)

After:
  Leader (Node 2) ✓ (New)
    └── Follower (Node 1) ✗ (Down)
    └── Follower (Node 3) ✓

Recovery Time: < 30s
  • Election timeout: 150-300ms
  • Vote request RTT: 10ms
  • State synchronization: 5-10s
```

#### 1.3 网络分区

```
┌────────────────────────────────────────────┐
│  Scenario: Network split (1 vs 2 nodes)    │
└────────────────────────────────────────────┘

Partition:
  Partition A: Node 1 (Leader)
  Partition B: Node 2, Node 3

Behavior:
  • Partition A (1 node): Cannot achieve majority
    - Writes fail (no quorum)
    - Reads succeed (stale data)
    - Node 1 steps down to Follower
  
  • Partition B (2 nodes): Can elect new leader
    - Node 2 or 3 becomes new Leader
    - Writes succeed (2/3 majority in B)
    - System continues in Partition B

Healing:
  1. Network partition resolves
  2. Old Leader (Node 1) detects higher term
  3. Node 1 becomes Follower
  4. Node 1 catches up logs from new Leader
  5. Cluster reunified
```

### 2. 数据恢复

#### 2.1 日志回放

```rust
impl ReplicatedMetadataEngine {
    /// Recover from crash
    pub async fn recover(&self) -> Result<()> {
        // 1. Load Raft persistent state
        let (hard_state, conf_state) = self.load_raft_state()?;

        // 2. Rebuild Raft node
        let mut raw_node = RawNode::new(
            &self.config.raft_config,
            self.storage.clone(),
            &self.logger,
        )?;

        // 3. Get last applied index
        let last_applied = self.local_engine.get_last_applied_index().await?;

        // 4. Replay uncommitted logs
        for index in (last_applied + 1)..=hard_state.commit {
            let entry = self.storage.get_entry(index)?;
            let op: MetadataOperation = serde_json::from_slice(&entry.data)?;

            self.apply_operation(&op).await?;
        }

        // 5. Resume normal operation
        self.raft_node.set_raw_node(raw_node);

        info!("Metadata engine recovered, last_applied={}", last_applied);
        Ok(())
    }
}
```

#### 2.2 快照恢复

```rust
impl ReplicatedMetadataEngine {
    /// Install snapshot from Leader
    pub async fn install_snapshot(&self, snapshot: MetadataSnapshot) -> Result<()> {
        info!("Installing snapshot, last_index={}", snapshot.last_index);

        // 1. Validate snapshot
        if !self.validate_snapshot(&snapshot) {
            return Err(Error::CorruptedSnapshot);
        }

        // 2. Stop accepting new requests
        self.state.store(EngineState::Recovering, Ordering::SeqCst);

        // 3. Apply snapshot
        self.apply_snapshot(snapshot).await?;

        // 4. Resume service
        self.state.store(EngineState::Running, Ordering::SeqCst);

        info!("Snapshot installed successfully");
        Ok(())
    }
}
```

### 3. 脑裂防护

Raft 协议天然防止脑裂：

```
Scenario: Network partition creates 2 groups

Group A: Node 1 (1 node, minority)
  • Cannot elect Leader (need 2/3 majority)
  • All writes fail
  • System safe but unavailable

Group B: Node 2, Node 3 (2 nodes, majority)
  • Can elect new Leader
  • Writes succeed
  • System available

Key: Only one partition can have majority → No dual-leader
```

---

## 实现方案

### 1. 阶段划分

#### Phase 1: 复制层框架 (2 周)

**目标**: 搭建基础复制层，不影响现有功能

```rust
// Step 1: Define replication interfaces
pub trait ReplicationEngine: MetadataEngine {
    fn add_peer(&self, node_id: u64, address: String) -> Result<()>;
    fn remove_peer(&self, node_id: u64) -> Result<()>;
    fn get_replication_status(&self) -> ReplicationStatus;
}

// Step 2: Implement minimal Raft integration
// - Use raft-rs crate
// - Basic log replication
// - Leader election

// Step 3: Wrapper for existing LocalMetadataEngine
pub struct ReplicatedMetadataEngine {
    local: Arc<LocalMetadataEngine>,
    raft: Arc<RaftNode>,
    // ...
}
```

**验证**:

- 单节点模式正常工作（无回归）
- Raft 节点能启动和选主
- 基本日志复制工作

#### Phase 2: 写入复制 (2 周)

**目标**: 实现写操作的多副本同步

```rust
// Step 1: Serialize write operations
async fn put_object(...) -> Result<ObjectInfo> {
    // 1. Create operation
    let op = MetadataOperation {
        op_type: OperationType::PutObject,
        data: serialize_put_request(...)?,
        ...
    };

    // 2. Propose to Raft
    self.raft.propose(op).await?;

    // 3. Wait for commit
    self.raft.wait_committed(op.op_id).await?;

    // 4. Apply locally
    self.local.put_object(...).await
}

// Step 2: Apply committed operations
async fn apply_operation(op: MetadataOperation) -> Result<()> {
    match op.op_type {
        OperationType::PutObject => { /* apply */ }
        OperationType::DeleteObject => { /* apply */ }
        // ...
    }
}
```

**验证**:

- 写入到 Leader 成功复制到 Followers
- 多数确认后才返回成功
- 故障节点重启后能追赶日志

#### Phase 3: 故障转移 (1 周)

**目标**: 实现自动故障检测和 Leader 切换

```rust
// Step 1: Health monitoring
async fn health_check_loop() {
    loop {
        for peer in peers {
            if !peer.is_healthy() {
                warn!("Peer {} unhealthy", peer.id);
                // Raft handles automatically
            }
        }
        sleep(Duration::from_secs(1)).await;
    }
}

// Step 2: Leader forwarding
async fn handle_request_on_follower(req: Request) -> Result<Response> {
    if self.is_leader() {
        // Process locally
        self.handle_locally(req).await
    } else {
        // Forward to leader
        let leader_addr = self.raft.get_leader_address()?;
        self.forward_to_leader(leader_addr, req).await
    }
}
```

**验证**:

- Leader 节点宕机后自动选主
- 客户端请求自动路由到新 Leader
- 故障转移时间 < 30s

#### Phase 4: 快照与压缩 (1 周)

**目标**: 实现日志压缩和快照传输

```rust
// Step 1: Periodic snapshot
async fn snapshot_task() {
    loop {
        sleep(Duration::from_secs(3600)).await; // Every hour

        if self.should_snapshot() {
            let snapshot = self.create_snapshot().await?;
            self.raft.install_snapshot(snapshot).await?;
        }
    }
}

// Step 2: Snapshot transfer
async fn send_snapshot_to_peer(
    peer_id: u64,
    snapshot: MetadataSnapshot,
) -> Result<()> {
    // Stream snapshot in chunks
    let mut stream = snapshot.into_stream();

    while let Some(chunk) = stream.next().await {
        self.send_to_peer(peer_id, chunk).await?;
    }

    Ok(())
}
```

**验证**:

- 日志达到阈值时自动创建快照
- 新节点加入时通过快照快速同步
- 快照传输不影响正常服务

#### Phase 5: 性能优化 (2 周)

**目标**: 优化延迟和吞吐量

```rust
// Optimization 1: Batch writes
async fn batch_propose(ops: Vec<MetadataOperation>) -> Result<()> {
    let batch = BatchOperation { ops };
    self.raft.propose(batch).await?;
    self.raft.wait_committed(batch.last_op_id).await?;

    // Apply all in one go
    for op in batch.ops {
        self.apply_operation(op).await?;
    }
    Ok(())
}

// Optimization 2: Pipeline
// Allow multiple in-flight proposals
let mut pending = FuturesUnordered::new();
for op in ops {
pending.push( self .raft.propose(op));
}
while let Some(result) = pending.next().await {
result?;
}

// Optimization 3: Zero-copy
// Avoid serialization/deserialization where possible
```

**验证**:

- 批量写入吞吐量 > 10,000 ops/s
- P99 延迟 < 20ms
- 资源占用合理（CPU < 20%, Mem < 1GB）

### 2. 配置示例

```yaml
# rustfs-metadata-replication.yaml

metadata:
  engine_type: replicated  # or "local" for single-node

  replication:
    # Cluster configuration
    nodes:
      - id: 1
        address: "192.168.1.101:7000"
      - id: 2
        address: "192.168.1.102:7000"
      - id: 3
        address: "192.168.1.103:7000"

    # Current node
    node_id: 1

    # Raft configuration
    raft:
      election_timeout_ms: 1000
      heartbeat_interval_ms: 100
      snapshot_interval: 3600  # seconds
      max_log_entries: 10000
      log_dir: "/data/rustfs/raft/log"
      snapshot_dir: "/data/rustfs/raft/snapshot"

    # Consistency
    read_consistency: linearizable  # or "eventual"
    write_quorum: majority          # or "all"

    # Performance
    max_batch_size: 100
    batch_timeout_ms: 10

  # Local engine config (unchanged)
  local:
    kv_path: "/data/rustfs/metadata/kv"
    index_path: "/data/rustfs/metadata/index"
    mx_path: "/data/rustfs/metadata/mx"
```

### 3. API 兼容性

**现有 API 保持不变**:

```rust
// 单节点模式
let engine = LocalMetadataEngine::new(...) ?;

// 多副本模式（透明替换）
let engine = ReplicatedMetadataEngine::new(config) ?;

// 相同接口
engine.put_object(bucket, key, reader, size, opts).await?;
engine.get_object_reader(bucket, key, opts).await?;
engine.list_objects(bucket, prefix,...).await?;
```

**新增管理 API**:

```rust
// 集群管理
engine.add_node(node_id, address) ?;
engine.remove_node(node_id) ?;
engine.transfer_leadership(target_node_id) ?;

// 监控
let status = engine.get_replication_status();
println!("Leader: {}", status.leader_id);
println!("Nodes: {:?}", status.nodes);
println!("Lag: {} ops", status.max_lag);

// 快照
engine.create_snapshot().await?;
engine.restore_from_snapshot(snapshot_path).await?;
```

---

## 性能优化

### 1. 批量写入

```rust
pub struct BatchWriter {
    operations: Vec<MetadataOperation>,
    max_batch_size: usize,
    batch_timeout: Duration,
}

impl BatchWriter {
    pub async fn write(&mut self, op: MetadataOperation) -> Result<()> {
        self.operations.push(op);

        if self.operations.len() >= self.max_batch_size {
            self.flush().await?;
        }

        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        if self.operations.is_empty() {
            return Ok(());
        }

        let batch = std::mem::take(&mut self.operations);

        // Single Raft proposal for entire batch
        self.raft.propose_batch(batch).await?;

        Ok(())
    }
}
```

**收益**:

- 减少 Raft 提案次数
- 提高吞吐量 5-10x
- 降低网络开销

### 2. 并行复制

```rust
// Leader 并行发送到多个 Followers
async fn replicate_to_followers(entry: LogEntry) -> Result<()> {
    let mut futures = Vec::new();

    for follower in self.followers.values() {
        futures.push(follower.send_entry(entry.clone()));
    }

    // Wait for majority
    let mut success_count = 1; // Leader itself
    for result in join_all(futures).await {
        if result.is_ok() {
            success_count += 1;
            if success_count >= self.quorum_size {
                return Ok(()); // Majority achieved
            }
        }
    }

    Err(Error::InsufficientQuorum)
}
```

### 3. 读取优化

```rust
// Read from local replica (no Raft consensus needed)
impl ReplicatedMetadataEngine {
    pub async fn get_object_local(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<ObjectInfo> {
        // Direct local read, O(1) latency
        self.local_engine.get_object(bucket, key, ObjectOptions::default()).await
    }

    // Read from leader (strong consistency)
    pub async fn get_object_consistent(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<ObjectInfo> {
        if self.raft_node.is_leader() {
            // Read from leader
            self.local_engine.get_object(bucket, key, ObjectOptions::default()).await
        } else {
            // Forward to leader
            let leader = self.raft_node.get_leader()?;
            self.rpc_client.get_object(&leader, bucket, key).await
        }
    }
}
```

### 4. 网络优化

```rust
// gRPC streaming for snapshot transfer
pub async fn stream_snapshot(
    snapshot: MetadataSnapshot,
    mut client: SnapshotClient,
) -> Result<()> {
    const CHUNK_SIZE: usize = 1024 * 1024; // 1MB

    let mut offset = 0;
    while offset < snapshot.data.len() {
        let end = std::cmp::min(offset + CHUNK_SIZE, snapshot.data.len());
        let chunk = &snapshot.data[offset..end];

        client.send_chunk(SnapshotChunk {
            offset,
            data: chunk.to_vec(),
        }).await?;

        offset = end;
    }

    client.finish().await?;
    Ok(())
}
```

---

## 运维管理

### 1. 监控指标

```rust
pub struct ReplicationMetrics {
    // Cluster health
    pub leader_id: u64,
    pub node_count: usize,
    pub healthy_nodes: usize,

    // Performance
    pub write_latency_p50: Duration,
    pub write_latency_p99: Duration,
    pub read_latency_p50: Duration,
    pub throughput_ops_per_sec: f64,

    // Replication lag
    pub max_lag_entries: u64,
    pub max_lag_time_ms: u64,

    // Raft state
    pub current_term: u64,
    pub committed_index: u64,
    pub applied_index: u64,
    pub snapshot_index: u64,

    // Resource usage
    pub log_size_bytes: u64,
    pub snapshot_size_bytes: u64,
    pub memory_usage_bytes: u64,
}

impl ReplicatedMetadataEngine {
    pub fn metrics(&self) -> ReplicationMetrics {
        // Collect and return metrics
    }
}
```

**Prometheus 导出**:

```rust
use prometheus::{register_gauge, register_histogram, Gauge, Histogram};

lazy_static! {
    static ref WRITE_LATENCY: Histogram = register_histogram!(
        "rustfs_metadata_write_latency_seconds",
        "Write operation latency"
    ).unwrap();
    
    static ref REPLICATION_LAG: Gauge = register_gauge!(
        "rustfs_metadata_replication_lag_entries",
        "Number of log entries behind leader"
    ).unwrap();
}
```

### 2. 运维命令

```bash
# 查看集群状态
rustfs-admin metadata status

# 添加节点
rustfs-admin metadata add-node --id 4 --address 192.168.1.104:7000

# 移除节点
rustfs-admin metadata remove-node --id 4

# 转移 Leader
rustfs-admin metadata transfer-leadership --to 2

# 创建快照
rustfs-admin metadata snapshot create

# 恢复快照
rustfs-admin metadata snapshot restore --path /backup/snapshot-20260224.snap

# 查看复制延迟
rustfs-admin metadata lag
```

### 3. 告警规则

```yaml
# Prometheus alerting rules

groups:
  - name: rustfs_metadata_replication
    rules:
      # No leader elected
      - alert: MetadataNoLeader
        expr: rustfs_metadata_has_leader == 0
        for: 30s
        annotations:
          summary: "No metadata leader elected"

      # High replication lag
      - alert: MetadataHighLag
        expr: rustfs_metadata_replication_lag_entries > 1000
        for: 1m
        annotations:
          summary: "Metadata replication lag > 1000 entries"

      # Node down
      - alert: MetadataNodeDown
        expr: up{job="rustfs-metadata"} == 0
        for: 30s
        annotations:
          summary: "Metadata node {{ $labels.instance }} is down"

      # High write latency
      - alert: MetadataHighWriteLatency
        expr: histogram_quantile(0.99, rustfs_metadata_write_latency_seconds) > 0.1
        for: 5m
        annotations:
          summary: "Metadata write P99 latency > 100ms"
```

---

## 总结

### 优势

1. **高可用性**
    - 多副本容错（3 副本容忍 1 个故障）
    - 自动故障转移（< 30s）
    - 无单点故障

2. **数据安全**
    - 多节点持久化
    - Raft 保证已提交数据不丢失
    - 快照备份和恢复

3. **性能扩展**
    - 副本分担读负载
    - 写入性能基本不变（~10ms）
    - 读取延迟极低（~1ms 本地读）

4. **运维友好**
    - 在线添加/删除节点
    - 灰度升级支持
    - 丰富的监控指标

### 挑战

1. **复杂度增加**
    - Raft 协议学习曲线
    - 分布式调试困难
    - 需要更多测试覆盖

2. **资源开销**
    - 每个节点需要完整数据副本
    - 网络带宽占用增加
    - 日志和快照存储

3. **运维成本**
    - 需要至少 3 个节点
    - 监控和告警配置
    - 故障排查难度增加

### 后续规划

**短期 (3 个月)**:

- ✅ 实现基础复制框架
- ✅ 完成写入复制和故障转移
- ✅ 生产环境小规模试点

**中期 (6 个月)**:

- 🔄 优化性能和资源占用
- 🔄 完善监控和运维工具
- 🔄 大规模生产验证

**长期 (1 年)**:

- 💡 跨数据中心复制
- 💡 智能负载均衡
- 💡 自动扩缩容

---

**作者**: RustFS Architecture Team  
**审核**: [待审核]  
**版本**: 1.0  
**更新日期**: 2026-02-24

