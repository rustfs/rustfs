//! Explicit lifecycle for real OpenRaft over one durable native capture owner.
use crate::{
    CaptureCommand, CaptureNode, CaptureNodeError, CaptureTypeConfig, CheckpointLimits, NativeLogWriter, NativeRaftLogStore,
    NativeRaftStateMachine, NativeSnapshotError, NativeWriteError, StoreError,
};
use openraft::{
    Config, ConfigError, Raft, RaftMetrics, RaftNetworkFactory,
    error::{ClientWriteError, Fatal, InitializeError, RaftError},
    raft::ClientWriteResponse,
};
use redb::ReadableDatabase;
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::watch;

#[derive(Debug, thiserror::Error)]
pub enum NativeRaftOwnerError {
    #[error(transparent)]
    Config(#[from] Box<ConfigError>),
    #[error(transparent)]
    Store(#[from] NativeWriteError),
    #[error(transparent)]
    Snapshot(#[from] NativeSnapshotError),
    #[error("runtime node ID {runtime} differs from durable node ID {durable}")]
    IdentityMismatch { runtime: u64, durable: u64 },
    #[error("bootstrap must contain the local node")]
    MissingLocalMember,
    #[error(transparent)]
    Node(#[from] CaptureNodeError),
    #[error("native Raft owner is stopping or stopped")]
    Stopped,
    #[error(transparent)]
    Fatal(Box<Fatal<u64>>),
    #[error(transparent)]
    Initialize(Box<RaftError<u64, InitializeError<u64, CaptureNode>>>),
    #[error(transparent)]
    Write(Box<RaftError<u64, ClientWriteError<u64, CaptureNode>>>),
    #[error("native Raft shutdown task failed")]
    Shutdown(#[source] tokio::task::JoinError),
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[error(transparent)]
    Reader(#[from] crate::source_reader::ReaderError),
}

#[derive(Default)]
struct DrainState {
    complete: AtomicBool,
    changed: tokio::sync::Notify,
    #[cfg(test)]
    waiters: Arc<std::sync::atomic::AtomicUsize>,
}
impl DrainState {
    async fn wait(&self) {
        #[cfg(test)]
        let _waiter = DrainWaiter::new(self.waiters.clone());
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.complete.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }
}
#[cfg(test)]
struct DrainWaiter(Arc<std::sync::atomic::AtomicUsize>);
#[cfg(test)]
impl DrainWaiter {
    fn new(waiters: Arc<std::sync::atomic::AtomicUsize>) -> Self {
        waiters.fetch_add(1, Ordering::AcqRel);
        Self(waiters)
    }
}
#[cfg(test)]
impl Drop for DrainWaiter {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

// The real worker, snapshot builders and replication readers share this one lease.
// Its final drop runs after their NativeLogWriter fields have been released.
pub(crate) struct OwnerLease {
    drain: Arc<DrainState>,
}
impl Drop for OwnerLease {
    fn drop(&mut self) {
        self.drain.complete.store(true, Ordering::Release);
        self.drain.changed.notify_waiters();
    }
}

/// A lifecycle owner, not a transport. All storage adapters share its original writer.
pub struct NativeRaftOwner {
    raft: Raft<CaptureTypeConfig>,
    writer: NativeLogWriter,
    node_id: u64,
    accepting: AtomicBool,
    drain: Arc<DrainState>,
    #[cfg(test)]
    pub(crate) snapshot_build_pause: Arc<crate::raft_state_machine::TestPause>,
}
impl NativeRaftOwner {
    pub async fn start<N>(
        writer: NativeLogWriter,
        node_id: u64,
        config: Arc<Config>,
        limits: CheckpointLimits,
        network: N,
    ) -> Result<Self, NativeRaftOwnerError>
    where
        N: RaftNetworkFactory<CaptureTypeConfig>,
    {
        // All operator-input checks precede Raft::new and its background workers.
        let config = Arc::new(
            (*config)
                .clone()
                .validate()
                .map_err(|error| NativeRaftOwnerError::Config(Box::new(error)))?,
        );
        let mut state_machine = NativeRaftStateMachine::new(writer.clone(), limits)?;
        let identity = writer
            .write(|store| {
                if !store.writable {
                    return Err(StoreError::WriteDisabled);
                }
                let transaction = store.database.begin_read()?;
                let meta = transaction.open_table(crate::META)?;
                crate::application::native_application_state(&transaction)?;
                crate::application::read_binding(&meta)?;
                crate::decode_identity(meta.get("identity")?.ok_or(StoreError::CorruptRecord)?.value())
            })
            .await?;
        if identity.node_id != node_id {
            return Err(NativeRaftOwnerError::IdentityMismatch {
                runtime: node_id,
                durable: identity.node_id,
            });
        }
        let drain = Arc::new(DrainState::default());
        let lease = Arc::new(OwnerLease { drain: drain.clone() });
        state_machine.owner_lease = Some(lease.clone());
        let mut logs = NativeRaftLogStore::new(writer.clone());
        logs.owner_lease = Some(lease);
        #[cfg(test)]
        let snapshot_build_pause = state_machine.build_before_ordering.clone();
        let raft = Raft::new(node_id, config, network, logs, state_machine)
            .await
            .map_err(|error| NativeRaftOwnerError::Fatal(Box::new(error)))?;
        Ok(Self {
            raft,
            writer,
            node_id,
            accepting: AtomicBool::new(true),
            drain,
            #[cfg(test)]
            snapshot_build_pause,
        })
    }

    fn require_running(&self) -> Result<(), NativeRaftOwnerError> {
        if self.accepting.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(NativeRaftOwnerError::Stopped)
        }
    }

    /// Only explicit first-cluster formation calls initialize; start never does.
    pub async fn bootstrap(&self, members: BTreeMap<u64, CaptureNode>) -> Result<(), NativeRaftOwnerError> {
        self.require_running()?;
        if !members.contains_key(&self.node_id) {
            return Err(NativeRaftOwnerError::MissingLocalMember);
        }
        for node in members.values() {
            node.validate()?;
        }
        self.raft
            .initialize(members)
            .await
            .map_err(|error| NativeRaftOwnerError::Initialize(Box::new(error)))
    }

    /// The returned LogId and application response come from actual consensus apply.
    pub async fn client_write(
        &self,
        command: CaptureCommand,
    ) -> Result<ClientWriteResponse<CaptureTypeConfig>, NativeRaftOwnerError> {
        self.require_running()?;
        self.raft
            .client_write(command)
            .await
            .map_err(|error| NativeRaftOwnerError::Write(Box::new(error)))
    }

    pub fn metrics(&self) -> watch::Receiver<RaftMetrics<u64, CaptureNode>> {
        self.raft.metrics()
    }

    /// Transport registration only; raw clones remain trusted caller-owned handles.
    pub fn raft(&self) -> &Raft<CaptureTypeConfig> {
        &self.raft
    }

    /// The caller owns serve/shutdown and must drain it before releasing the Store.
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    pub fn bind_reader(
        &self,
        path: impl AsRef<std::path::Path>,
    ) -> Result<(crate::source_reader::SourceReaderServer, crate::source_reader::Shutdown), NativeRaftOwnerError> {
        self.require_running()?;
        Ok(crate::source_reader::SourceReaderServer::bind(path, self.writer.clone())?)
    }

    #[cfg(test)]
    pub(crate) fn shutdown_waiters(&self) -> Arc<std::sync::atomic::AtomicUsize> {
        self.drain.waiters.clone()
    }

    /// Stop admission before signaling Raft; await its real core/tick shutdown.
    pub async fn shutdown(&self) -> Result<(), NativeRaftOwnerError> {
        self.accepting.store(false, Ordering::Release);
        self.raft.shutdown().await.map_err(NativeRaftOwnerError::Shutdown)?;
        // OpenRaft joins core/tick but detaches the SM worker and snapshot builders.
        self.drain.wait().await;
        Ok(())
    }
}
