// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::{ObjectOptions, WriteCompletion};
use crate::error::{Error, Result};
use std::sync::Arc;
use std::time::Duration;

/// A real write guard shared with an inner commit, never a lock-loss signal alone.
///
/// Callers cannot fabricate a guard without acquiring its namespace lock.
///
/// ```compile_fail
/// use rustfs_ecstore::api::object::WriteCommitGuard;
/// use std::sync::Arc;
///
/// let guard = WriteCommitGuard {
///     bucket: "bucket".into(),
///     object: "object".into(),
///     guards: Arc::new(Vec::new()),
/// };
/// ```
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct WriteCommitGuard {
    bucket: Arc<str>,
    object: Arc<str>,
    guards: Arc<Vec<Arc<rustfs_lock::NamespaceLockGuard>>>,
}

impl WriteCommitGuard {
    /// Acquire the key recorded by the namespace wrapper and retain its owner.
    pub async fn acquire(
        lock: &rustfs_lock::NamespaceLockWrapper,
        timeout: Duration,
    ) -> std::result::Result<Self, rustfs_lock::LockError> {
        let guard = Arc::new(lock.get_write_lock(timeout).await?);
        let key = lock.resource();
        Ok(Self::from_guard(guard, &key.bucket, &key.object))
    }

    pub(crate) fn from_guard(guard: Arc<rustfs_lock::NamespaceLockGuard>, bucket: &str, object: &str) -> Self {
        Self {
            bucket: bucket.into(),
            object: object.into(),
            guards: Arc::new(vec![guard]),
        }
    }

    pub fn is_lock_lost(&self) -> bool {
        self.guards.iter().any(|guard| guard.is_lock_lost() || guard.is_released())
    }

    fn matches(&self, bucket: &str, object: &str) -> bool {
        self.bucket.as_ref() == bucket && self.object.as_ref() == object
    }
}

/// Opaque boundary capability; only real acquisitions or offline startup construct it.
///
/// ```compile_fail
/// use rustfs_ecstore::object_api::WriteLockContext;
///
/// let context = WriteLockContext {};
/// ```
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct WriteLockContext {
    mode: WriteLockMode,
}

#[derive(Clone, Debug)]
enum WriteLockMode {
    Borrowed(Vec<WriteCommitGuard>),
    OfflineRecovery,
}

pub(crate) enum WriteCommitContext {
    Owned,
    Borrowed(Vec<WriteCommitGuard>),
    Publication,
    OfflineRecovery,
    #[cfg(test)]
    UnlockedFixture,
}

impl WriteCommitContext {
    pub(crate) fn from_options(opts: &ObjectOptions, bucket: &str, object: &str, publication: bool) -> Result<Self> {
        if publication {
            if let Some(WriteLockContext {
                mode: WriteLockMode::Borrowed(guards),
            }) = opts.write_lock_context.as_ref()
                && !guards.iter().any(|guard| guard.matches(bucket, object))
            {
                return Err(Error::InvalidArgument(
                    bucket.to_owned(),
                    object.to_owned(),
                    "publication carries a different borrowed namespace".to_owned(),
                ));
            }
            return Ok(Self::Publication);
        }
        match (opts.no_lock, opts.write_lock_context.as_ref().map(|context| &context.mode)) {
            (false, None | Some(WriteLockMode::Borrowed(_))) => Ok(Self::Owned),
            (true, Some(WriteLockMode::Borrowed(guards))) if guards.iter().any(|guard| guard.matches(bucket, object)) => {
                Ok(Self::Borrowed(guards.clone()))
            }
            (true, Some(WriteLockMode::OfflineRecovery)) => Ok(Self::OfflineRecovery),
            #[cfg(test)]
            (true, None) if !opts.metadata_cache_safe => Ok(Self::UnlockedFixture),
            _ => Err(Error::InvalidArgument(
                bucket.to_owned(),
                object.to_owned(),
                "write lock context does not prove the target namespace".to_owned(),
            )),
        }
    }

    pub(crate) fn acquires_namespace(&self) -> bool {
        matches!(self, Self::Owned)
    }

    pub(crate) fn has_borrowed_owner(&self) -> bool {
        matches!(self, Self::Borrowed(_))
    }

    pub(crate) fn is_lock_lost(&self) -> bool {
        matches!(self, Self::Borrowed(guards) if guards.iter().any(WriteCommitGuard::is_lock_lost))
    }

    pub(crate) fn allows_early_ack(&self, completion: WriteCompletion) -> bool {
        completion == WriteCompletion::Quorum && matches!(self, Self::Owned | Self::Borrowed(_) | Self::Publication)
    }
}

impl ObjectOptions {
    pub fn add_write_commit_guard(&mut self, guard: &WriteCommitGuard) {
        for owner in guard.guards.iter() {
            self.add_namespace_lock_guard(owner);
        }
        let mut guards = match self.write_lock_context.as_ref() {
            Some(WriteLockContext {
                mode: WriteLockMode::Borrowed(existing),
            }) => existing.clone(),
            _ => Vec::new(),
        };
        if let Some(existing) = guards.iter_mut().find(|held| held.matches(&guard.bucket, &guard.object)) {
            let owners = Arc::make_mut(&mut existing.guards);
            for owner in guard.guards.iter() {
                if !owners.iter().any(|held| Arc::ptr_eq(held, owner)) {
                    owners.push(Arc::clone(owner));
                }
            }
        } else {
            guards.push(guard.clone());
        }
        self.no_lock = true;
        self.write_lock_context = Some(WriteLockContext {
            mode: WriteLockMode::Borrowed(guards),
        });
    }

    pub(crate) fn add_owned_write_lock(&mut self, guard: Arc<rustfs_lock::NamespaceLockGuard>, bucket: &str, object: &str) {
        self.add_write_commit_guard(&WriteCommitGuard::from_guard(guard, bucket, object));
    }

    /// Only startup migration may publish without a running namespace owner.
    pub(crate) fn use_offline_recovery_write(&mut self) {
        self.no_lock = true;
        self.write_lock_context = Some(WriteLockContext {
            mode: WriteLockMode::OfflineRecovery,
        });
        self.write_completion = WriteCompletion::TailDrained;
    }
}
