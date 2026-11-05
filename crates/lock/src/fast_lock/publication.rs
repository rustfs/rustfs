// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! FIFO admission only for the bucket publication fence. Consecutive readers
//! form a group; a later writer cannot overtake them, nor can later readers
//! overtake a queued writer. This does not limit a current holder's I/O time.

use parking_lot::Mutex;
use std::{collections::VecDeque, sync::Arc};
use tokio::sync::Notify;

use super::{
    state::ObjectLockState,
    types::{LockMode, ObjectKey, ObjectLockRequest},
};

pub(super) fn applies(key: &ObjectKey) -> bool {
    key.version.is_none() && key.object.as_ref() == rustfs_common::table_catalog::TABLE_BUCKET_PUBLICATION_LOCK_PATH
}

#[derive(Debug, Default)]
pub(super) struct PublicationQueue {
    entries: Mutex<VecDeque<(Arc<()>, LockMode)>>,
    pub(super) changed: Notify,
}

pub(super) struct PublicationTicket {
    state: Arc<ObjectLockState>,
    pub(super) queue: Arc<PublicationQueue>,
    token: Arc<()>,
    mode: LockMode,
}

impl PublicationTicket {
    // Called while the shard map is locked. The waiter counter pins the map
    // entry until ticket destruction, including cancellation between polls.
    pub(super) fn new(state: Arc<ObjectLockState>, mode: LockMode) -> Option<Self> {
        let counted = match mode {
            LockMode::Shared => state.atomic_state.inc_readers_waiting(),
            LockMode::Exclusive => state.atomic_state.inc_writers_waiting(),
        };
        if !counted {
            return None;
        }
        let queue = state
            .publication
            .get_or_init(|| Arc::new(PublicationQueue::default()))
            .clone();
        let token = Arc::new(());
        queue.entries.lock().push_back((token.clone(), mode));
        Some(Self {
            state,
            queue,
            token,
            mode,
        })
    }

    pub(super) fn try_acquire(&self, request: &ObjectLockRequest) -> bool {
        // Lock order: shard map -> queue -> owner metadata. Release never
        // locks the queue; ticket destruction only locks the queue.
        let entries = self.queue.entries.lock();
        let mut found = false;
        for (token, mode) in entries.iter() {
            if Arc::ptr_eq(token, &self.token) {
                found = true;
                break;
            }
            if self.mode == LockMode::Exclusive || *mode == LockMode::Exclusive {
                return false;
            }
        }
        if !found {
            return false;
        }
        match self.mode {
            LockMode::Shared => self.state.try_acquire_admitted_shared(&request.owner, request.lock_timeout),
            LockMode::Exclusive => self.state.try_acquire_exclusive_fast(&request.owner, request.lock_timeout),
        }
    }
}

impl Drop for PublicationTicket {
    fn drop(&mut self) {
        {
            let mut entries = self.queue.entries.lock();
            if let Some(index) = entries.iter().position(|(token, _)| Arc::ptr_eq(token, &self.token)) {
                entries.remove(index);
            }
        }
        match self.mode {
            LockMode::Shared => self.state.atomic_state.dec_readers_waiting(),
            LockMode::Exclusive => self.state.atomic_state.dec_writers_waiting(),
        }
        self.queue.changed.notify_waiters();
    }
}
