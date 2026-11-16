// Copyright 2026 RustFS Team
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

use crate::table_catalog::store::backup_types::{
    TableCatalogBackupObjectPreparationStatus, TableCatalogBackupStatus, TableCatalogRestoreStatus,
};
use crate::table_catalog::store::object_backup::*;
use crate::table_catalog::*;
use std::collections::{BTreeMap, BTreeSet};

impl<B: TableCatalogObjectBackend> ObjectTableCatalogStore<B> {
    pub(crate) async fn create_object_catalog_backup(
        &self,
        table_bucket: &str,
        expected_catalog_etag: Option<&str>,
    ) -> TableCatalogStoreResult<TableCatalogBackupReport> {
        self.require_table_scope()?;
        let guards = self.acquire_object_backup_fence(table_bucket).await?;
        let mutation_fence = Self::object_backup_mutation_fence(&guards);
        mutation_fence.ensure_held()?;
        self.ensure_no_active_restore(table_bucket).await?;
        let objects = self.capture_object_catalog_backup_objects(table_bucket).await?;
        self.validate_object_catalog_backup_objects(table_bucket, &objects)?;
        self.validate_backup_commit_recovery(table_bucket, &objects)?;
        let maintenance = self.validate_backup_maintenance_authority(table_bucket, &objects)?;
        mutation_fence.ensure_held()?;
        let references = self.capture_object_catalog_backup_references(table_bucket, &objects).await?;
        mutation_fence.ensure_held()?;
        let source_catalog_etag = catalog_backup_fingerprint(&objects)?;
        if expected_catalog_etag.is_some_and(|expected| expected != source_catalog_etag) {
            return Err(TableCatalogStoreError::Conflict(
                "catalog changed before object backup capture".to_string(),
            ));
        }
        let mut created_at = OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .map_err(|error| TableCatalogStoreError::Internal(format!("failed to format backup timestamp: {error}")))?;
        let backup_id = catalog_backup_inventory_identity(table_bucket, &source_catalog_etag, &objects, &references)?;
        let backup = ObjectCatalogBackup {
            version: OBJECT_CATALOG_BACKUP_INVENTORY_VERSION,
            backup_id: backup_id.clone(),
            table_bucket: table_bucket.to_string(),
            created_at: created_at.clone(),
            source_catalog_etag: source_catalog_etag.clone(),
            objects,
            references: Some(references),
        };
        let (verified_object_count, verified_bytes) = catalog_backup_verification_totals(&backup)?;
        let encoded = serde_json::to_vec(&backup)
            .map_err(|error| TableCatalogStoreError::Internal(format!("failed to encode object catalog backup: {error}")))?;
        if encoded.len() > OBJECT_CATALOG_BACKUP_MAX_SIZE {
            return Err(TableCatalogStoreError::Invalid(format!(
                "object catalog backup exceeds the maximum encoded size of {OBJECT_CATALOG_BACKUP_MAX_SIZE} bytes"
            )));
        }
        let path = self.paths.catalog_backup_path(table_bucket, &backup_id);
        let status = match self
            .backend
            .put_object_fenced(RUSTFS_META_BUCKET, &path, encoded, TableCatalogPutPrecondition::IfAbsent, &mutation_fence)
            .await
        {
            Ok(()) => TableCatalogBackupStatus::Created,
            Err(TableCatalogStoreError::Conflict(_)) => {
                let existing = self
                    .backend
                    .read_object_limited(RUSTFS_META_BUCKET, &path, OBJECT_CATALOG_BACKUP_MAX_SIZE)
                    .await?
                    .ok_or_else(|| TableCatalogStoreError::Conflict("catalog backup disappeared after conflict".to_string()))?;
                let existing = self.decode_object_catalog_backup(table_bucket, &existing.data)?;
                let same_identity = existing.version == backup.version
                    && existing.backup_id == backup.backup_id
                    && existing.table_bucket == backup.table_bucket
                    && existing.source_catalog_etag == backup.source_catalog_etag
                    && existing.objects == backup.objects
                    && existing.references == backup.references;
                if !same_identity {
                    return Err(TableCatalogStoreError::Conflict(
                        "catalog backup identity is already occupied by different content".to_string(),
                    ));
                }
                created_at = existing.created_at;
                TableCatalogBackupStatus::AlreadyPresent
            }
            Err(error) => return Err(error),
        };
        mutation_fence.ensure_held()?;
        Ok(TableCatalogBackupReport {
            table_bucket: table_bucket.to_string(),
            backup_id,
            status,
            backup_path: path,
            source_snapshot_etag: source_catalog_etag.clone(),
            source_snapshot_version: backup.version,
            catalog_fingerprint: source_catalog_etag,
            object_count: backup.objects.len(),
            verified_object_count,
            verified_bytes,
            maintenance_object_count: maintenance.object_count,
            created_at,
        })
    }

    async fn current_object_catalog_fingerprint(&self, table_bucket: &str) -> TableCatalogStoreResult<Option<String>> {
        let current = self
            .backend
            .list_objects(self.catalog_bucket(), &self.paths.table_bucket_root_prefix(table_bucket))
            .await?;
        if current
            .iter()
            .all(|object| self.is_catalog_directory_sidecar(table_bucket, object))
        {
            return Ok(None);
        }
        let objects = self.capture_object_catalog_backup_objects(table_bucket).await?;
        self.validate_backup_commit_recovery(table_bucket, &objects)?;
        self.validate_backup_maintenance_authority(table_bucket, &objects)?;
        catalog_backup_fingerprint(&objects).map(Some)
    }

    async fn preflight_object_catalog_restore(
        &self,
        table_bucket: &str,
        backup: &ObjectCatalogBackup,
        allow_replace: bool,
        fence: &TableCatalogObjectMutationFence,
    ) -> TableCatalogStoreResult<(Vec<ObjectCatalogRestoreWrite>, Vec<ObjectCatalogRestoreDelete>)> {
        self.validate_catalog_backup_reference_graph(backup).await?;
        fence.ensure_held()?;
        let target_prefix = self.paths.table_bucket_root_prefix(table_bucket);
        let mut current = self.backend.list_objects(self.catalog_bucket(), &target_prefix).await?;
        current.retain(|object| !self.is_catalog_directory_sidecar(table_bucket, object));
        let target_paths = backup
            .objects
            .iter()
            .filter(|object| object.bucket == self.catalog_bucket())
            .map(|object| object.object.as_str())
            .collect::<BTreeSet<_>>();
        if !allow_replace && current.iter().any(|object| !target_paths.contains(object.as_str())) {
            return Err(TableCatalogStoreError::Conflict(
                "catalog restore would replace existing catalog objects; set allow-replace".to_string(),
            ));
        }
        let mut writes = Vec::new();
        for object in &backup.objects {
            let (expected_etag, already_present) = match self
                .backend
                .read_object_limited(&object.bucket, &object.object, OBJECT_CATALOG_BACKUP_OBJECT_MAX_SIZE)
                .await?
            {
                None => (None, false),
                Some(existing) if existing.data == object.data => (existing.etag, true),
                Some(existing) => {
                    // Replacement applies only to internal catalog state. Iceberg
                    // metadata is immutable, including during disaster recovery.
                    if !allow_replace || object.bucket != self.catalog_bucket() {
                        return Err(TableCatalogStoreError::Conflict(format!(
                            "catalog restore target object changed: {}/{}",
                            object.bucket, object.object
                        )));
                    }
                    (
                        Some(existing.etag.filter(|value| !value.is_empty()).ok_or_else(|| {
                            TableCatalogStoreError::Unavailable("catalog restore target has no usable ETag".to_string())
                        })?),
                        false,
                    )
                }
            };
            writes.push(ObjectCatalogRestoreWrite {
                bucket: object.bucket.clone(),
                object: object.object.clone(),
                expected_etag,
                already_present,
            });
        }
        let mut deletes = Vec::new();
        if allow_replace {
            for object in current {
                if target_paths.contains(object.as_str()) {
                    continue;
                }
                let metadata = self
                    .backend
                    .object_metadata(self.catalog_bucket(), &object)
                    .await?
                    .ok_or_else(|| {
                        TableCatalogStoreError::Conflict("catalog restore target disappeared during preflight".to_string())
                    })?;
                let expected_etag = metadata.etag.filter(|value| !value.is_empty()).ok_or_else(|| {
                    TableCatalogStoreError::Unavailable("catalog restore deletion target has no usable ETag".to_string())
                })?;
                deletes.push(ObjectCatalogRestoreDelete { object, expected_etag });
            }
        }
        deletes.sort_by(|left, right| left.object.cmp(&right.object));
        Ok((writes, deletes))
    }

    #[cfg(test)]
    pub(crate) async fn restore_object_catalog_backup(
        &self,
        table_bucket: &str,
        backup_id: &str,
        expected_catalog_etag: Option<&str>,
        allow_replace: bool,
    ) -> TableCatalogStoreResult<TableCatalogRestoreReport> {
        self.require_table_scope()?;
        self.restore_object_catalog_backup_with_id(table_bucket, backup_id, expected_catalog_etag, allow_replace, None)
            .await
    }

    pub(crate) async fn restore_object_catalog_backup_with_id(
        &self,
        table_bucket: &str,
        backup_id: &str,
        expected_catalog_etag: Option<&str>,
        allow_replace: bool,
        restore_id: Option<&str>,
    ) -> TableCatalogStoreResult<TableCatalogRestoreReport> {
        self.require_table_scope()?;
        let restore_id = restore_id.unwrap_or(backup_id);
        if restore_id.is_empty()
            || restore_id.len() > 256
            || !restore_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"-._".contains(&byte))
            || expected_catalog_etag.is_some_and(str::is_empty)
        {
            return Err(TableCatalogStoreError::Invalid("catalog restore request identity is invalid".to_string()));
        }
        let guards = self.acquire_object_backup_fence(table_bucket).await?;
        let fence = Self::object_backup_mutation_fence(&guards);
        fence.ensure_held()?;
        self.reject_legacy_catalog_control_state(table_bucket).await?;
        let path = self.paths.catalog_backup_path(table_bucket, backup_id);
        let stored = self
            .backend
            .read_object_limited(self.catalog_bucket(), &path, OBJECT_CATALOG_BACKUP_MAX_SIZE)
            .await?
            .ok_or_else(|| TableCatalogStoreError::NotFound(format!("catalog backup {backup_id}")))?;
        let backup = self.decode_object_catalog_backup(table_bucket, &stored.data)?;
        let backup_etag = stored
            .etag
            .filter(|etag| !etag.is_empty())
            .ok_or_else(|| TableCatalogStoreError::Unavailable("catalog backup has no usable ETag".to_string()))?;
        if backup.backup_id != backup_id {
            return Err(TableCatalogStoreError::Invalid(
                "catalog backup identity does not match its request".to_string(),
            ));
        }
        let request_fingerprint =
            restore_request_fingerprint(table_bucket, restore_id, backup_id, expected_catalog_etag, allow_replace)?;
        let current_state = self.read_object_catalog_restore_state(table_bucket).await?;
        let saved = self.read_object_catalog_restore_intent(table_bucket, restore_id).await?;
        if let Some((intent, _)) = saved.as_ref() {
            if intent.request_fingerprint != request_fingerprint || intent.backup_id != backup_id {
                return Err(TableCatalogStoreError::Conflict(
                    "catalog restore-id is already bound to a different request".to_string(),
                ));
            }
            self.validate_restore_plan(&backup, intent)?;
            if intent.state == ObjectCatalogRestoreStateKind::Completed
                && current_state.as_ref().is_some_and(|(state, _)| {
                    state.restore_epoch != intent.restore_epoch || state.state == ObjectCatalogRestoreStateKind::Completed
                })
            {
                // A receipt is a result of this request, not permission to roll back
                // legitimate catalog writes made after its publication.
                fence.ensure_held()?;
                return Ok(self.restore_report_from_state(intent, TableCatalogRestoreStatus::AlreadyRestored));
            }
        }
        if let Some((state, _)) = current_state.as_ref()
            && state.state != ObjectCatalogRestoreStateKind::Completed
            && (state.restore_id != restore_id || state.request_fingerprint != request_fingerprint)
        {
            return Err(TableCatalogStoreError::Unavailable(format!(
                "table bucket {table_bucket} has an active catalog restore for another request"
            )));
        }
        if let Some((state, _)) = current_state.as_ref()
            && state.restore_id == restore_id
            && saved.is_none()
        {
            return Err(TableCatalogStoreError::Unavailable(
                "catalog restore barrier is missing its durable intent".to_string(),
            ));
        }

        // Restore publishes authoritative entries directly; require rollback to those entries
        // before changing them. A completed restore's receipt may still be replayed above.
        if self.read_table_directory_state(table_bucket).await?.is_some() {
            return Err(TableCatalogStoreError::Conflict(
                "disable the table directory before restoring the catalog".to_string(),
            ));
        }
        // Receipt replay above must remain valid after later maintenance work.
        // A new or interrupted restore may publish only stable source authority.
        self.validate_backup_commit_recovery(table_bucket, &backup.objects)?;
        let maintenance = self.validate_backup_maintenance_authority(table_bucket, &backup.objects)?;

        let (mut intent, mut intent_etag) = match saved {
            Some(saved) => saved,
            None => {
                let previous_snapshot_etag = self.current_object_catalog_fingerprint(table_bucket).await?;
                if expected_catalog_etag.is_some_and(|expected| Some(expected) != previous_snapshot_etag.as_deref()) {
                    return Err(TableCatalogStoreError::Conflict(
                        "catalog restore target watermark does not match".to_string(),
                    ));
                }
                let (writes, deletes) = self
                    .preflight_object_catalog_restore(table_bucket, &backup, allow_replace, &fence)
                    .await?;
                let now = OffsetDateTime::now_utc()
                    .format(&time::format_description::well_known::Rfc3339)
                    .map_err(|error| TableCatalogStoreError::Internal(format!("failed to format restore timestamp: {error}")))?;
                let intent = ObjectCatalogRestoreIntent {
                    version: OBJECT_CATALOG_RESTORE_STATE_VERSION,
                    table_bucket: table_bucket.to_string(),
                    restore_epoch: Uuid::new_v4().to_string(),
                    restore_id: restore_id.to_string(),
                    backup_id: backup_id.to_string(),
                    request_fingerprint: request_fingerprint.clone(),
                    expected_catalog_etag: expected_catalog_etag.map(str::to_string),
                    allow_replace,
                    source_catalog_etag: backup.source_catalog_etag.clone(),
                    state: ObjectCatalogRestoreStateKind::Prepared,
                    previous_snapshot_etag,
                    restored_snapshot_etag: backup.source_catalog_etag.clone(),
                    object_count: backup.objects.len(),
                    maintenance_object_count: maintenance.object_count,
                    writes,
                    deletes,
                    created_at: now.clone(),
                    updated_at: now,
                };
                self.validate_restore_plan(&backup, &intent)?;
                let etag = self
                    .write_object_catalog_restore_intent(&intent, TableCatalogPutPrecondition::IfAbsent, &fence)
                    .await?;
                (intent, etag)
            }
        };
        let (mut state, mut state_etag) = match current_state {
            Some((state, etag)) if state.state != ObjectCatalogRestoreStateKind::Completed => {
                if state.restore_epoch != intent.restore_epoch || state.backup_id != intent.backup_id {
                    return Err(TableCatalogStoreError::Unavailable(
                        "catalog restore intent does not own the active barrier".to_string(),
                    ));
                }
                (state, etag)
            }
            previous => {
                // An intent may have persisted immediately before a crash. No target
                // mutation is authorized until its original target is revalidated.
                if intent.state != ObjectCatalogRestoreStateKind::Prepared
                    || self.current_object_catalog_fingerprint(table_bucket).await? != intent.previous_snapshot_etag
                {
                    return Err(TableCatalogStoreError::Conflict(
                        "catalog restore target changed before barrier activation".to_string(),
                    ));
                }
                let state = ObjectCatalogRestoreState {
                    version: OBJECT_CATALOG_RESTORE_STATE_VERSION,
                    table_bucket: table_bucket.to_string(),
                    restore_epoch: intent.restore_epoch.clone(),
                    restore_id: restore_id.to_string(),
                    backup_id: backup_id.to_string(),
                    request_fingerprint,
                    state: ObjectCatalogRestoreStateKind::Prepared,
                    completed_intent_etag: None,
                    backup_etag: None,
                };
                let precondition = previous.map_or(TableCatalogPutPrecondition::IfAbsent, |(_, etag)| {
                    TableCatalogPutPrecondition::IfMatch(etag)
                });
                let etag = self
                    .write_object_catalog_restore_state(table_bucket, &state, precondition, &fence)
                    .await?;
                (state, etag)
            }
        };
        if state.state == ObjectCatalogRestoreStateKind::Prepared {
            state.state = ObjectCatalogRestoreStateKind::Applying;
            state_etag = self
                .write_object_catalog_restore_state(
                    table_bucket,
                    &state,
                    TableCatalogPutPrecondition::IfMatch(state_etag),
                    &fence,
                )
                .await?;
        }
        if intent.state == ObjectCatalogRestoreStateKind::Prepared {
            // Recheck immutable Iceberg references before resuming, including after
            // a process restart. Every mutation retains its first observed ETag.
            self.validate_catalog_backup_reference_graph(&backup).await?;
            self.apply_object_catalog_restore_plan(&backup, &intent, &fence).await?;
            self.verify_restored_catalog(&backup).await?;
            intent.state = ObjectCatalogRestoreStateKind::CatalogApplied;
            intent.updated_at = OffsetDateTime::now_utc().to_string();
            intent_etag = self
                .write_object_catalog_restore_intent(&intent, TableCatalogPutPrecondition::IfMatch(intent_etag), &fence)
                .await?;
        } else {
            self.verify_restored_catalog(&backup).await?;
        }
        if state.state != ObjectCatalogRestoreStateKind::CatalogApplied {
            state.state = ObjectCatalogRestoreStateKind::CatalogApplied;
            state_etag = self
                .write_object_catalog_restore_state(
                    table_bucket,
                    &state,
                    TableCatalogPutPrecondition::IfMatch(state_etag),
                    &fence,
                )
                .await?;
        }
        if intent.state != ObjectCatalogRestoreStateKind::Completed {
            intent.state = ObjectCatalogRestoreStateKind::Completed;
            intent.updated_at = OffsetDateTime::now_utc().to_string();
            intent_etag = self
                .write_object_catalog_restore_intent(&intent, TableCatalogPutPrecondition::IfMatch(intent_etag), &fence)
                .await?;
        }
        // Publication is the final durable write. Until it succeeds all readers
        // and writers reject the bucket, even if every catalog object was applied.
        state.state = ObjectCatalogRestoreStateKind::Completed;
        state.completed_intent_etag = Some(intent_etag);
        state.backup_etag = Some(backup_etag);
        self.write_object_catalog_restore_state(table_bucket, &state, TableCatalogPutPrecondition::IfMatch(state_etag), &fence)
            .await?;
        fence.ensure_held()?;
        Ok(self.restore_report_from_state(&intent, TableCatalogRestoreStatus::Restored))
    }

    async fn verify_restored_catalog(&self, backup: &ObjectCatalogBackup) -> TableCatalogStoreResult<()> {
        for object in &backup.objects {
            let current = self
                .backend
                .read_object_limited(&object.bucket, &object.object, OBJECT_CATALOG_BACKUP_OBJECT_MAX_SIZE)
                .await?;
            if current.is_none_or(|current| current.data != object.data) {
                return Err(TableCatalogStoreError::Conflict(format!(
                    "catalog restore target differs from the backup: {}/{}",
                    object.bucket, object.object
                )));
            }
        }
        let actual = self
            .backend
            .list_objects(self.catalog_bucket(), &self.paths.table_bucket_root_prefix(&backup.table_bucket))
            .await?
            .into_iter()
            .filter(|object| !self.is_catalog_directory_sidecar(&backup.table_bucket, object))
            .collect::<BTreeSet<_>>();
        let expected = backup
            .objects
            .iter()
            .filter(|object| object.bucket == self.catalog_bucket())
            .map(|object| object.object.clone())
            .collect::<BTreeSet<_>>();
        if actual != expected {
            return Err(TableCatalogStoreError::Conflict("catalog restore target inventory changed".to_string()));
        }
        self.validate_catalog_backup_reference_graph(backup).await
    }

    /// Captures the finite object inventory required by one stable catalog payload.
    ///
    /// The caller retains its publication and restore fences throughout capture. The inventory includes current
    /// metadata, its direct metadata-log history, each corresponding snapshot graph and statistics, and current view
    /// metadata. It contains watermarks, not payload copies. Cancellation makes no storage changes.
    pub(in crate::table_catalog) async fn capture_object_catalog_backup_references(
        &self,
        table_bucket: &str,
        objects: &[ObjectCatalogBackupObject],
    ) -> TableCatalogStoreResult<Vec<ObjectCatalogBackupReference>> {
        let keys = self
            .collect_object_catalog_backup_reference_keys(table_bucket, objects)
            .await?;
        let mut references = Vec::with_capacity(keys.len());
        for ((bucket, object), kind) in keys {
            references.push(
                self.object_catalog_backup_reference_fingerprint(&bucket, &object, kind)
                    .await?,
            );
        }
        // Current metadata embedded in the catalog payload must be exactly the object whose graph was inventoried.
        for object in objects.iter().filter(|object| object.bucket == table_bucket) {
            let index = references
                .binary_search_by(|reference| {
                    reference
                        .bucket
                        .cmp(&object.bucket)
                        .then_with(|| reference.object.cmp(&object.object))
                })
                .map_err(|_| TableCatalogStoreError::Invalid("catalog backup embeds an unreferenced table object".to_string()))?;
            let reference = &references[index];
            if u64::try_from(object.data.len()).ok() != Some(reference.size_bytes)
                || reference.sha256.as_deref() != Some(backup_sha256(&object.data).as_str())
            {
                return Err(TableCatalogStoreError::Conflict(
                    "catalog backup embedded metadata differs from the prepared object".to_string(),
                ));
            }
        }
        Ok(references)
    }

    /// Verifies the closed inventory without publishing any catalog state.
    ///
    /// The caller retains its restore/publication fences. Small objects compare size and SHA-256 across targets; their
    /// individual reads still require stable target ETags. An absent current metadata object may be supplied by its
    /// validated inline restore payload; existing different bytes are never accepted. All external objects must
    /// already be prepared. Large objects retain the recorded size and ETag. Invalid references fail closed.
    pub(in crate::table_catalog) async fn validate_object_catalog_backup_references(
        &self,
        table_bucket: &str,
        objects: &[ObjectCatalogBackupObject],
        references: &[ObjectCatalogBackupReference],
    ) -> TableCatalogStoreResult<()> {
        validate_object_catalog_backup_reference_shape(table_bucket, references)?;
        let keys = self
            .collect_object_catalog_backup_reference_keys(table_bucket, objects)
            .await?;
        if keys.len() != references.len() {
            return Err(TableCatalogStoreError::Conflict(
                "catalog backup reference graph does not match the prepared target".to_string(),
            ));
        }
        let embedded = objects
            .iter()
            .filter(|object| object.bucket == table_bucket)
            .map(|object| ((object.bucket.as_str(), object.object.as_str()), object))
            .collect::<BTreeMap<_, _>>();
        for (expected, ((bucket, object), kind)) in references.iter().zip(&keys) {
            if expected.bucket != *bucket || expected.object != *object || expected.kind != *kind {
                return Err(TableCatalogStoreError::Conflict(
                    "catalog backup reference identity does not match its graph".to_string(),
                ));
            }
            if let Some(payload) = embedded.get(&(bucket.as_str(), object.as_str()))
                && self.backend.object_metadata(bucket, object).await?.is_none()
            {
                if u64::try_from(payload.data.len()).ok() != Some(expected.size_bytes)
                    || expected.sha256.as_deref() != Some(backup_sha256(&payload.data).as_str())
                {
                    return Err(TableCatalogStoreError::Conflict(
                        "inline restore metadata does not match its saved inventory".to_string(),
                    ));
                }
                continue;
            }
            let actual = self
                .object_catalog_backup_reference_fingerprint(bucket, object, *kind)
                .await?;
            if expected.size_bytes != actual.size_bytes
                || if expected.size_bytes <= OBJECT_CATALOG_BACKUP_REFERENCE_HASH_MAX_SIZE {
                    expected.sha256 != actual.sha256
                } else {
                    expected.etag != actual.etag
                }
            {
                return Err(TableCatalogStoreError::Conflict(format!(
                    "catalog backup reference watermark does not match the prepared target: {}/{}",
                    expected.bucket, expected.object
                )));
            }
        }
        Ok(())
    }

    /// Prepares one small object referenced by a saved v2 backup without publishing catalog state.
    ///
    /// Only an absent key is written. Exact-byte retries keep the target ETag; different existing bytes are never
    /// replaced. An ambiguous put is confirmed by a stable read of the original bytes. Cancellation may leave that
    /// immutable object prepared; retrying the same input confirms it. Pending restore controls are never changed.
    pub(crate) async fn prepare_object_catalog_backup_object(
        &self,
        table_bucket: &str,
        backup_id: &str,
        object: &str,
        data: Vec<u8>,
    ) -> TableCatalogStoreResult<TableCatalogBackupObjectPreparationReport> {
        self.require_table_scope()?;
        if table_bucket == self.catalog_bucket() || !safe_object_key(object) || data.len() > OBJECT_CATALOG_BACKUP_OBJECT_MAX_SIZE
        {
            return Err(TableCatalogStoreError::Invalid(
                "backup object preparation has an invalid bucket, key, or body size".to_string(),
            ));
        }
        let guards = self.acquire_object_backup_fence(table_bucket).await?;
        let fence = Self::object_backup_mutation_fence(&guards);
        fence.ensure_held()?;
        let backup_path = self.paths.catalog_backup_path(table_bucket, backup_id);
        let artifact = self
            .backend
            .read_object_limited(self.catalog_bucket(), &backup_path, OBJECT_CATALOG_BACKUP_MAX_SIZE)
            .await?
            .ok_or_else(|| TableCatalogStoreError::NotFound(format!("catalog backup {backup_id}")))?;
        let artifact_etag = artifact.etag.as_deref().filter(|etag| !etag.is_empty()).ok_or_else(|| {
            TableCatalogStoreError::Unavailable("catalog backup has no stable ETag for preparation".to_string())
        })?;
        let backup = self.decode_object_catalog_backup(table_bucket, &artifact.data)?;
        if backup.backup_id != backup_id {
            return Err(TableCatalogStoreError::Invalid(
                "catalog backup identity does not match its preparation request".to_string(),
            ));
        }
        let references = backup
            .references
            .as_deref()
            .filter(|_| backup.version == OBJECT_CATALOG_BACKUP_INVENTORY_VERSION)
            .ok_or_else(|| {
                TableCatalogStoreError::Unsupported("object preparation requires a v2 catalog backup inventory".to_string())
            })?;
        validate_object_catalog_backup_reference_shape(table_bucket, references)?;
        let index = references
            .binary_search_by(|reference| {
                reference
                    .bucket
                    .as_str()
                    .cmp(table_bucket)
                    .then_with(|| reference.object.as_str().cmp(object))
            })
            .map_err(|_| {
                TableCatalogStoreError::Invalid("object is not present in this catalog backup reference inventory".to_string())
            })?;
        let reference = &references[index];
        let sha256 = backup_sha256(&data);
        if u64::try_from(data.len()).ok() != Some(reference.size_bytes) || reference.sha256.as_deref() != Some(sha256.as_str()) {
            return Err(TableCatalogStoreError::Invalid(
                "prepared object body does not match its backup size and SHA-256 watermark".to_string(),
            ));
        }
        let gate = self.backup_object_preparation_restore_gate(table_bucket, &backup).await?;
        self.validate_backup_object_preparation_scope(table_bucket, &backup, references, reference, &data, &fence)
            .await?;
        Self::validate_backup_object_format(reference.kind, &data).await?;
        fence.ensure_held()?;
        self.finish_backup_object_preparation_gate(table_bucket, &gate).await?;
        let (status, stored) = if let Some(stored) = self.read_stable_preparation_object(table_bucket, object).await? {
            if stored.data != data {
                return Err(TableCatalogStoreError::Conflict(
                    "backup preparation never replaces different existing object bytes".to_string(),
                ));
            }
            (TableCatalogBackupObjectPreparationStatus::AlreadyPresent, stored)
        } else {
            let result = self
                .backend
                .put_object_fenced(table_bucket, object, data.clone(), TableCatalogPutPrecondition::IfAbsent, &fence)
                .await;
            fence.ensure_held()?;
            let saved = self.read_stable_preparation_object(table_bucket, object).await?;
            match saved {
                Some(saved) if saved.data == data => (
                    if result.is_ok() {
                        TableCatalogBackupObjectPreparationStatus::Prepared
                    } else {
                        TableCatalogBackupObjectPreparationStatus::AlreadyPresent
                    },
                    saved,
                ),
                Some(_) => {
                    return Err(TableCatalogStoreError::Conflict(
                        "backup preparation key is occupied by different object bytes".to_string(),
                    ));
                }
                None => {
                    result?;
                    return Err(TableCatalogStoreError::Unavailable(
                        "prepared object disappeared after its conditional write".to_string(),
                    ));
                }
            }
        };
        fence.ensure_held()?;
        self.finish_backup_object_preparation_gate(table_bucket, &gate).await?;
        let final_artifact = self.backend.object_metadata(self.catalog_bucket(), &backup_path).await?;
        if final_artifact.as_ref().and_then(|metadata| metadata.etag.as_deref()) != Some(artifact_etag) {
            return Err(TableCatalogStoreError::Unavailable(
                "catalog backup changed during object preparation".to_string(),
            ));
        }
        fence.ensure_held()?;
        Ok(TableCatalogBackupObjectPreparationReport {
            table_bucket: table_bucket.to_string(),
            backup_id: backup_id.to_string(),
            object: object.to_string(),
            status,
            size_bytes: reference.size_bytes,
            sha256,
            etag: stored.etag,
        })
    }

    /// Returns the saved v1 artifact bytes without reencoding or applying them.
    ///
    /// This does not export external manifest/data/delete files. Pending restore
    /// or legacy control state is rejected; cancellation leaves catalog state unchanged.
    pub(crate) async fn export_object_catalog_backup(
        &self,
        table_bucket: &str,
        backup_id: &str,
    ) -> TableCatalogStoreResult<Vec<u8>> {
        self.require_table_scope()?;
        let guards = self.acquire_object_backup_fence(table_bucket).await?;
        let fence = Self::object_backup_mutation_fence(&guards);
        fence.ensure_held()?;
        self.ensure_no_active_restore(table_bucket).await?;
        let path = self.paths.catalog_backup_path(table_bucket, backup_id);
        let artifact = self
            .backend
            .read_object_limited(self.catalog_bucket(), &path, OBJECT_CATALOG_BACKUP_MAX_SIZE)
            .await?
            .ok_or_else(|| TableCatalogStoreError::NotFound(format!("catalog backup {backup_id}")))?;
        let backup = self.decode_object_catalog_backup(table_bucket, &artifact.data)?;
        if backup.backup_id != backup_id {
            return Err(TableCatalogStoreError::Invalid(
                "catalog backup identity does not match its request".to_string(),
            ));
        }
        let _validated_tables = self.validate_catalog_backup_metadata_identities(table_bucket, &backup.objects)?;
        fence.ensure_held()?;
        Ok(artifact.data)
    }

    /// Persists one validated versioned artifact without publishing catalog or metadata objects.
    ///
    /// Only the artifact key is conditionally created. Exact retries keep its bytes,
    /// ETag and original creation timestamp; conflicting content is never replaced.
    /// A response lost after storage can be retried to confirm the saved artifact.
    /// Referenced objects must be prepared and validated separately before restore.
    pub(crate) async fn import_object_catalog_backup(
        &self,
        table_bucket: &str,
        data: Vec<u8>,
    ) -> TableCatalogStoreResult<TableCatalogBackupReport> {
        self.require_table_scope()?;
        let backup = self.decode_object_catalog_backup(table_bucket, &data)?;
        let _validated_tables = self.validate_catalog_backup_metadata_identities(table_bucket, &backup.objects)?;
        let (verified_object_count, verified_bytes) = catalog_backup_verification_totals(&backup)?;
        let path = self.paths.catalog_backup_path(table_bucket, &backup.backup_id);
        let guards = self.acquire_object_backup_fence(table_bucket).await?;
        let fence = Self::object_backup_mutation_fence(&guards);
        fence.ensure_held()?;
        self.ensure_no_active_restore(table_bucket).await?;

        let existing = self
            .backend
            .read_object_limited(self.catalog_bucket(), &path, OBJECT_CATALOG_BACKUP_MAX_SIZE)
            .await?;
        let status = if let Some(existing) = existing {
            if existing.data != data {
                return Err(TableCatalogStoreError::Conflict(
                    "catalog backup identity is already occupied by different bytes".to_string(),
                ));
            }
            TableCatalogBackupStatus::AlreadyPresent
        } else {
            let result = self
                .backend
                .put_object_fenced(self.catalog_bucket(), &path, data.clone(), TableCatalogPutPrecondition::IfAbsent, &fence)
                .await;
            fence.ensure_held()?;
            let saved = self
                .backend
                .read_object_limited(self.catalog_bucket(), &path, OBJECT_CATALOG_BACKUP_MAX_SIZE)
                .await?;
            match saved {
                Some(saved) if saved.data == data => {
                    if result.is_ok() {
                        TableCatalogBackupStatus::Created
                    } else {
                        // The read proves an ambiguous write or identical concurrent
                        // import completed; it does not authorize another put.
                        TableCatalogBackupStatus::AlreadyPresent
                    }
                }
                Some(_) => {
                    return Err(TableCatalogStoreError::Conflict(
                        "catalog backup identity is already occupied by different bytes".to_string(),
                    ));
                }
                None => {
                    result?;
                    return Err(TableCatalogStoreError::Unavailable("catalog backup disappeared after import".to_string()));
                }
            }
        };
        fence.ensure_held()?;
        Ok(TableCatalogBackupReport {
            table_bucket: table_bucket.to_string(),
            backup_id: backup.backup_id,
            status,
            backup_path: path,
            source_snapshot_etag: backup.source_catalog_etag.clone(),
            source_snapshot_version: backup.version,
            catalog_fingerprint: backup.source_catalog_etag,
            object_count: backup.objects.len(),
            verified_object_count,
            verified_bytes,
            maintenance_object_count: self.backup_maintenance_object_count(table_bucket, &backup.objects),
            created_at: backup.created_at,
        })
    }
}

impl<B: TableCatalogObjectBackend> ConfiguredTableCatalogStore<B> {
    pub(crate) async fn create_catalog_backup(
        &self,
        table_bucket: &str,
        expected_snapshot_etag: Option<&str>,
    ) -> TableCatalogStoreResult<TableCatalogBackupReport> {
        match self {
            Self::ObjectBacked(store) => store.create_object_catalog_backup(table_bucket, expected_snapshot_etag).await,
        }
    }

    pub(crate) async fn export_catalog_backup(&self, table_bucket: &str, backup_id: &str) -> TableCatalogStoreResult<Vec<u8>> {
        match self {
            Self::ObjectBacked(store) => store.export_object_catalog_backup(table_bucket, backup_id).await,
        }
    }

    pub(crate) async fn import_catalog_backup(
        &self,
        table_bucket: &str,
        data: Vec<u8>,
    ) -> TableCatalogStoreResult<TableCatalogBackupReport> {
        match self {
            Self::ObjectBacked(store) => store.import_object_catalog_backup(table_bucket, data).await,
        }
    }

    pub(crate) async fn prepare_catalog_backup_object(
        &self,
        table_bucket: &str,
        backup_id: &str,
        object: &str,
        data: Vec<u8>,
    ) -> TableCatalogStoreResult<TableCatalogBackupObjectPreparationReport> {
        match self {
            Self::ObjectBacked(store) => {
                store
                    .prepare_object_catalog_backup_object(table_bucket, backup_id, object, data)
                    .await
            }
        }
    }

    pub(crate) async fn restore_catalog_backup_with_id(
        &self,
        table_bucket: &str,
        backup_id: &str,
        expected_snapshot_etag: Option<&str>,
        allow_replace: bool,
        restore_id: Option<&str>,
    ) -> TableCatalogStoreResult<TableCatalogRestoreReport> {
        match self {
            Self::ObjectBacked(store) => {
                store
                    .restore_object_catalog_backup_with_id(
                        table_bucket,
                        backup_id,
                        expected_snapshot_etag,
                        allow_replace,
                        restore_id,
                    )
                    .await
            }
        }
    }
}
