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

//! Persistent attempt fences for ordinary ECStore PUTs. Other publication
//! paths do not yet participate; these records are not authority for GC.

use super::{metadata::BucketMetadata, metadata_sys};
use crate::config::com::{read_config_observation_no_lock, save_config_no_lock, with_config_object_write_lock};
use crate::disk::{BUCKET_META_PREFIX, RUSTFS_META_BUCKET};
use crate::error::{Error, Result};
use crate::object_api::{NamespaceLockFence, ObjectLockConfigSnapshot, ObjectOptions, WriteCommitGuard};
use crate::set_disk::get_lock_acquire_timeout;
use crate::storage_api_contracts::namespace::NamespaceLocking;
use crate::store::{BucketIncarnationFenceGuard, ECStore};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PutSealRecord {
    version: u8,
    bucket: String,
    incarnation_id: Uuid,
    base_prefix: String,
    attempt_prefix: String,
}

/// An ordinary PUT target derived from one authoritative bucket metadata snapshot.
/// Fields are private so ObjectOptions cannot fabricate a different namespace.
#[doc(hidden)]
#[derive(Debug)]
pub struct ArtifactPutTarget {
    record: PutSealRecord,
    object: String,
}

impl ArtifactPutTarget {
    pub(crate) fn from_metadata(metadata: &BucketMetadata, object: &str) -> Result<Option<Arc<Self>>> {
        let Some(base_prefix) = metadata.artifact_base_prefix.as_deref() else {
            return Ok(None);
        };
        let attempt_prefix = attempt_prefix(base_prefix, object)?;
        if object.len() == attempt_prefix.len() {
            return Err(Error::other("artifact PUT requires an object within an attempt"));
        }
        Ok(Some(Arc::new(Self {
            record: PutSealRecord::new(metadata, attempt_prefix)?,
            object: object.to_owned(),
        })))
    }

    pub(crate) async fn acquire(
        &self,
        store: Arc<ECStore>,
        bucket: &str,
        object: &str,
        opts: &ObjectOptions,
    ) -> Result<ArtifactPutGuard> {
        require_local_put_seal_topology(&store).await?;
        if store.ctx.lock_manager().is_disabled() {
            return Err(Error::other("artifact PUT requires namespace locking"));
        }
        if self.record.bucket != bucket
            || self.object != object
            || opts.expected_bucket_incarnation_id != Some(self.record.incarnation_id)
        {
            return Err(Error::other("artifact PUT target identity mismatch"));
        }
        // Lock order: lifecycle read -> attempt record write -> target object write.
        // A destructive PUT already owns the lifecycle and metadata guards in
        // its Object Lock snapshot. Retain that owner rather than queueing a
        // recursive read behind a writer that is waiting for this PUT.
        let (lifecycle, snapshot) = if opts.bucket_lifecycle_lock_fence.is_some() {
            let snapshot = opts
                .object_lock_config_snapshot
                .as_ref()
                .filter(|snapshot| snapshot.owns_bucket_lifecycle_fence(store.id, bucket, self.record.incarnation_id))
                .ok_or_else(|| Error::other("artifact PUT requires an owned lifecycle fence"))?;
            (None, Some(Arc::clone(snapshot)))
        } else {
            (
                Some(
                    store
                        .acquire_bucket_incarnation_fence(bucket, self.record.incarnation_id)
                        .await?,
                ),
                None,
            )
        };
        let path = self.record.path();
        let lock = store.new_ns_lock(RUSTFS_META_BUCKET, &path).await?;
        let attempt = WriteCommitGuard::acquire(&lock, get_lock_acquire_timeout()).await?;
        let guard = ArtifactPutGuard {
            attempt,
            lifecycle,
            _snapshot: snapshot,
            lifecycle_fence: opts.bucket_lifecycle_lock_fence.clone(),
        };
        if guard.is_lock_lost() {
            return Err(Error::other("artifact PUT fence was lost"));
        }
        if self.record.exists(store, &guard.attempt).await? {
            return Err(Error::other("artifact attempt PUTs are sealed"));
        }
        if guard.is_lock_lost() {
            return Err(Error::other("artifact PUT fence was lost"));
        }
        Ok(guard)
    }
}

pub(crate) struct ArtifactPutGuard {
    attempt: WriteCommitGuard,
    lifecycle: Option<BucketIncarnationFenceGuard>,
    _snapshot: Option<Arc<ObjectLockConfigSnapshot>>,
    lifecycle_fence: Option<NamespaceLockFence>,
}

impl ArtifactPutGuard {
    pub(crate) fn is_lock_lost(&self) -> bool {
        self.attempt.is_lock_lost()
            || self.lifecycle.as_ref().is_some_and(BucketIncarnationFenceGuard::is_lock_lost)
            || self.lifecycle_fence.as_ref().is_some_and(NamespaceLockFence::is_lock_lost)
    }
}

fn local_put_seal_endpoints(endpoints: &[crate::layout::endpoint::Endpoint]) -> bool {
    !endpoints.is_empty() && endpoints.iter().all(|endpoint| endpoint.is_local)
}

async fn require_local_put_seal_topology(store: &ECStore) -> Result<()> {
    // Use the configured endpoints retained by every pool, including offline
    // members. Online disk snapshots cannot prove absence of remote writers.
    if store.pools.is_empty()
        || store
            .pools
            .iter()
            .any(|pool| !local_put_seal_endpoints(pool.endpoints.endpoints.as_ref()))
        || store.ctx.is_dist_erasure().await
    {
        return Err(Error::other(
            "artifact PUT sealing requires local-only storage; distributed or remote endpoints are unsupported",
        ));
    }
    Ok(())
}

impl ECStore {
    pub(crate) async fn bind_artifact_write_metadata(
        &self,
        bucket: &str,
        opts: &mut ObjectOptions,
    ) -> Result<Option<Arc<BucketMetadata>>> {
        if super::utils::is_meta_bucketname(bucket) {
            return Ok(None);
        }
        let metadata = metadata_sys::get_bucket_incarnation_metadata_in(&self.ctx, bucket).await?;
        if opts
            .expected_bucket_incarnation_id
            .is_some_and(|expected| expected != metadata.bucket_incarnation_id)
        {
            return Err(Error::BucketNotFound(bucket.to_owned()));
        }
        // The classification and subsequent commit must refer to the same generation.
        opts.expected_bucket_incarnation_id = Some(metadata.bucket_incarnation_id);
        // A loss signal cannot authorize skipping the downstream generation lock.
        // Keep a matching owner in opts, including for ordinary PUT and Copy.
        if let Some(snapshot) = opts.object_lock_config_snapshot.clone() {
            if !snapshot.owns_bucket_lifecycle_fence(self.id, bucket, metadata.bucket_incarnation_id) {
                return Err(Error::other("write requires an owned bucket lifecycle fence"));
            }
            snapshot.add_lock_fences(opts);
        } else if opts.bucket_lifecycle_lock_fence.is_some() {
            return Err(Error::other("write requires an owned bucket lifecycle fence"));
        }
        Ok(Some(metadata))
    }

    pub(crate) async fn prepare_unsupported_artifact_write(
        &self,
        bucket: &str,
        opts: &ObjectOptions,
    ) -> Result<(ObjectOptions, Option<rustfs_lock::NamespaceLockGuard>)> {
        let mut opts = opts.clone();
        let Some(metadata) = self.bind_artifact_write_metadata(bucket, &mut opts).await? else {
            return Ok((opts, None));
        };
        reject_unsupported_artifact_write(&metadata)?;
        // Lifecycle read -> metadata transaction read -> downstream object lock.
        // Binding above verified and retained any supplied snapshot's actual owner.
        let guard = if opts.object_lock_config_snapshot.is_some() {
            None
        } else {
            Some(self.acquire_bucket_lifecycle_read_lock(bucket).await?)
        };
        if let Some(guard) = guard.as_ref() {
            opts.add_bucket_lifecycle_lock_guard(guard);
        }
        let current = metadata_sys::get_bucket_incarnation_id_for_options_in(&self.ctx, bucket, &opts).await?;
        if opts.expected_bucket_incarnation_id != Some(current) {
            return Err(Error::BucketNotFound(bucket.to_owned()));
        }
        if opts
            .bucket_lifecycle_lock_fence
            .as_ref()
            .is_some_and(NamespaceLockFence::is_lock_lost)
        {
            return Err(Error::other("bucket lifecycle lock was lost before the write"));
        }
        Ok((opts, guard))
    }
}

pub(crate) fn reject_unsupported_artifact_write(metadata: &BucketMetadata) -> Result<()> {
    if metadata.artifact_base_prefix.is_some() {
        return Err(Error::other("artifact bucket does not support this write operation"));
    }
    Ok(())
}

impl PutSealRecord {
    fn new(metadata: &BucketMetadata, attempt_prefix: &str) -> Result<Self> {
        let base_prefix = metadata
            .artifact_base_prefix
            .as_ref()
            .ok_or_else(|| Error::other("bucket is not an artifact bucket"))?;
        if metadata.bucket_incarnation_id.is_nil()
            || attempt_prefix.len() >= 1024
            || self::attempt_prefix(base_prefix, attempt_prefix)? != attempt_prefix
        {
            return Err(Error::other("invalid artifact attempt identity"));
        }
        Ok(Self {
            version: 1,
            bucket: metadata.name.clone(),
            incarnation_id: metadata.bucket_incarnation_id,
            base_prefix: base_prefix.clone(),
            attempt_prefix: attempt_prefix.to_owned(),
        })
    }

    fn path(&self) -> String {
        artifact_put_seal_record_path(&self.bucket, self.incarnation_id, &self.attempt_prefix)
    }

    async fn exists(&self, store: Arc<ECStore>, guard: &WriteCommitGuard) -> Result<bool> {
        let bytes = match read_config_observation_no_lock(store, &self.path(), 16 * 1024, guard).await {
            Ok((bytes, _)) => bytes,
            Err(Error::ConfigNotFound) => return Ok(false),
            Err(error) => return Err(error),
        };
        let record: Self = serde_json::from_slice(&bytes)?;
        if record != *self {
            return Err(Error::other("artifact PUT seal identity mismatch"));
        }
        Ok(true)
    }
}

pub(crate) fn artifact_put_seal_record_path(bucket: &str, incarnation: Uuid, attempt_prefix: &str) -> String {
    let digest = hex_simd::encode_to_string(Sha256::digest(attempt_prefix.as_bytes()), hex_simd::AsciiCase::Lower);
    format!("{BUCKET_META_PREFIX}/{bucket}/artifact-put-seals/{incarnation}/{digest}.json")
}

fn attempt_prefix<'a>(base_prefix: &str, object: &'a str) -> Result<&'a str> {
    let invalid = || Error::other("invalid artifact attempt prefix");
    let suffix = object.strip_prefix(base_prefix).ok_or_else(invalid)?;
    let mut components = suffix.splitn(4, '/');
    let tenant = components.next().ok_or_else(invalid)?;
    let run = components.next().ok_or_else(invalid)?;
    let attempt = components.next().ok_or_else(invalid)?;
    components.next().ok_or_else(invalid)?;
    if tenant.is_empty()
        || tenant.len() % 2 != 0
        || !tenant
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(invalid());
    }
    for value in [run, attempt] {
        let id = Uuid::parse_str(value).map_err(|_| invalid())?;
        if id.is_nil() || id.hyphenated().to_string() != value {
            return Err(invalid());
        }
    }
    Ok(&object[..base_prefix.len() + tenant.len() + run.len() + attempt.len() + 3])
}

impl ECStore {
    /// Seal ordinary ECStore PUTs for one artifact attempt. This does not seal
    /// CopyObject, multipart completion, replication, restore or repair paths.
    pub async fn seal_artifact_attempt_puts(
        self: Arc<Self>,
        bucket: &str,
        expected_incarnation: Uuid,
        attempt_prefix: &str,
    ) -> Result<()> {
        require_local_put_seal_topology(&self).await?;
        if self.ctx.lock_manager().is_disabled() {
            return Err(Error::other("artifact PUT sealing requires namespace locking"));
        }
        let metadata = metadata_sys::get_bucket_incarnation_metadata_in(&self.ctx, bucket).await?;
        if expected_incarnation.is_nil() || metadata.name != bucket || metadata.bucket_incarnation_id != expected_incarnation {
            return Err(Error::other("artifact PUT seal bucket incarnation mismatch"));
        }
        let record = PutSealRecord::new(&metadata, attempt_prefix)?;
        let lifecycle = self.acquire_bucket_incarnation_fence(bucket, expected_incarnation).await?;
        let store = Arc::clone(&self);
        // The config transaction owns the closure and lifecycle guard even if
        // its request waiter is cancelled. Its save drains the physical tail.
        with_config_object_write_lock(Arc::clone(&self), record.path(), move |guard| async move {
            if lifecycle.is_lock_lost() || guard.is_lock_lost() {
                return Err(Error::other("artifact PUT seal fence was lost"));
            }
            if !record.exists(Arc::clone(&store), &guard).await? {
                save_config_no_lock(store, &record.path(), serde_json::to_vec(&record)?, &guard).await?;
            }
            if lifecycle.is_lock_lost() || guard.is_lock_lost() {
                return Err(Error::other("artifact PUT seal fence was lost"));
            }
            Ok(())
        })
        .await?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn artifact_put_seal_topology_requires_configured_local_endpoints() {
        use crate::layout::endpoint::Endpoint;

        let local = Endpoint::try_from("/tmp/artifact-local-disk").unwrap();
        // No remote Disk or online connection exists: the configuration alone
        // must reject this member rather than losing it in an online snapshot.
        let remote = Endpoint::try_from("http://192.0.2.1:9000/artifact-remote-disk").unwrap();
        assert!(local_put_seal_endpoints(std::slice::from_ref(&local)));
        assert!(!local_put_seal_endpoints(&[]), "ARTIFACT_PUT_LOCAL_TOPOLOGY_REQUIRED");
        assert!(!local_put_seal_endpoints(&[local, remote]), "ARTIFACT_PUT_LOCAL_TOPOLOGY_REQUIRED");
    }

    #[test]
    fn artifact_put_seal_attempt_identity_requires_canonical_components() {
        let run = "12345678-1234-4234-8234-123456789abc";
        let attempt = "23456789-1234-4234-8234-123456789abc";
        let prefix = format!("outputs/6169/{run}/{attempt}/");
        assert_eq!(attempt_prefix("outputs/", &format!("{prefix}image.png")).unwrap(), prefix);
        assert_eq!(attempt_prefix("outputs/", &prefix).unwrap(), prefix);
        for tenant in ["", "f", "GG", "AB", ".."] {
            assert!(attempt_prefix("outputs/", &format!("outputs/{tenant}/{run}/{attempt}/image.png")).is_err());
        }
        for invalid_run in [
            "00000000-0000-0000-0000-000000000000",
            "12345678123442348234123456789abc",
            "../",
        ] {
            assert!(attempt_prefix("outputs/", &format!("outputs/6169/{invalid_run}/{attempt}/image.png")).is_err());
        }
        assert!(attempt_prefix("outputs/", &format!("elsewhere/6169/{run}/{attempt}/image.png")).is_err());
        assert!(attempt_prefix("outputs/", prefix.trim_end_matches('/')).is_err());
    }
}
