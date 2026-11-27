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

use super::*;
use tokio_util::sync::DropGuard;

pub(super) struct AbandonedFolders {
    names: std::collections::hash_set::IntoIter<String>,
    previous_bucket: String,
}

impl AbandonedFolders {
    pub(super) fn new(names: HashSet<String>) -> Self {
        Self {
            names: names.into_iter(),
            previous_bucket: String::new(),
        }
    }
}

impl FolderScanner {
    #[inline(never)]
    pub(super) async fn next_abandoned_child(
        &mut self,
        ctx: &CancellationToken,
        frame: &FolderFrame,
        abandoned: &mut AbandonedFolders,
    ) -> Result<Option<(CachedFolder, DropGuard)>, ScannerError> {
        for name in abandoned.names.by_ref() {
            if !self.try_should_heal().await? {
                break;
            }

            let (bucket, prefix) = path2_bucket_object(&name);
            if bucket != abandoned.previous_bucket {
                self.send_required_scanner_heal_request(
                    PendingScannerHealKind::Bucket,
                    bucket.clone(),
                    None,
                    None,
                    build_bucket_heal_request(bucket.clone(), HealChannelPriority::High),
                )
                .await?;
                abandoned.previous_bucket.clone_from(&bucket);
            }

            let child_ctx = ctx.child_token();
            let cancel_on_exit = child_ctx.clone().drop_guard();
            if self.discover_abandoned_child(child_ctx.clone(), bucket, prefix).await? {
                let child = CachedFolder {
                    name,
                    parent: Some(frame.hash.clone()),
                    object_heal_prob_div: 1,
                };
                return Ok(Some((child, cancel_on_exit)));
            }
        }
        Ok(None)
    }

    #[inline(never)]
    pub(super) async fn accept_abandoned_child(
        &mut self,
        ctx: &CancellationToken,
        frame: &mut FolderFrame,
        child: &FolderFrame,
        result: Result<FolderScanOutcome, ScannerError>,
        shared: bool,
    ) -> Result<(), ScannerError> {
        if shared {
            result?;
            self.send_update_for_entry(&frame.hash, &frame.folder.parent, &frame.entry)
                .await;
            tokio::task::yield_now().await;
            return Ok(());
        }

        if let Err(error) = result {
            if matches!(error, ScannerError::SourceUnavailable(_)) {
                return Err(error);
            }
            if ctx.is_cancelled() || matches!(&error, ScannerError::ResourceDeferred) {
                self.preserve_partial_child_progress(&child.folder.parent, &child.hash, &mut frame.entry, &child.entry)
                    .await;
                return Err(error);
            }
            warn!(
                target: "rustfs::scanner::folder",
                event = EVENT_SCANNER_FOLDER_STATE,
                component = LOG_COMPONENT_SCANNER,
                subsystem = LOG_SUBSYSTEM_FOLDER,
                folder = %frame.folder.name,
                child_folder = %child.folder.name,
                state = "heal_child_scan_failed",
                error = %error,
                "Scanner heal child folder scan failed"
            );
            return Ok(());
        }

        tokio::task::yield_now().await;
        frame.entry.add_child(&child.hash);
        self.refresh_usage_subtree(&child.hash, &child.folder.parent);
        self.send_update().await;
        self.maybe_send_checkpoint(&frame.folder, &frame.hash, &frame.entry);
        Ok(())
    }

    #[inline(never)]
    async fn discover_abandoned_child(
        &mut self,
        child_ctx: CancellationToken,
        bucket: String,
        prefix: String,
    ) -> Result<bool, ScannerError> {
        let (agreed_tx, mut agreed_rx) = mpsc::channel::<String>(1);
        let (partial_tx, mut partial_rx) = mpsc::channel::<MetaCacheEntries>(1);
        let (finished_tx, mut finished_rx) = mpsc::channel::<Vec<Option<DiskError>>>(1);

        let disks = self.disks.iter().cloned().map(Some).collect();
        let disks_quorum = self.disks_quorum;
        let bucket_clone = bucket.clone();
        let prefix_clone = prefix.clone();
        let child_ctx_clone = child_ctx.clone();
        #[cfg(test)]
        let list_path_raw_options_observer = self.list_path_raw_options_observer.clone();

        tokio::spawn(async move {
            let options = ListPathRawOptions {
                disks,
                bucket: bucket_clone.clone(),
                path: prefix_clone.clone(),
                recursive: true,
                report_not_found: true,
                min_disks: disks_quorum,
                agreed: Some(Box::new(move |entry: MetaCacheEntry| {
                    let entry_name = entry.name.clone();
                    let agreed_tx = agreed_tx.clone();
                    Box::pin(async move {
                        if let Err(e) = agreed_tx.send(entry_name).await {
                            error!(
                                target: "rustfs::scanner::folder",
                                event = EVENT_SCANNER_FOLDER_STATE,
                                component = LOG_COMPONENT_SCANNER,
                                subsystem = LOG_SUBSYSTEM_FOLDER,
                                entry = %entry.name,
                                state = "list_path_agreed_send_failed",
                                error = %e,
                                "Scanner list_path_raw agreed callback failed"
                            );
                        }
                        Ok(())
                    })
                })),
                partial: Some(Box::new(move |entries: MetaCacheEntries, _: &[Option<DiskError>]| {
                    let partial_tx = partial_tx.clone();
                    Box::pin(async move {
                        if let Err(e) = partial_tx.send(entries).await {
                            error!(
                                target: "rustfs::scanner::folder",
                                event = EVENT_SCANNER_FOLDER_STATE,
                                component = LOG_COMPONENT_SCANNER,
                                subsystem = LOG_SUBSYSTEM_FOLDER,
                                state = "list_path_partial_send_failed",
                                error = %e,
                                "Scanner list_path_raw partial callback failed"
                            );
                        }
                    })
                })),
                finished: Some(Box::new(move |errs: &[Option<DiskError>]| {
                    let finished_tx = finished_tx.clone();
                    let errs_clone = errs.to_vec();
                    Box::pin(async move {
                        if let Err(e) = finished_tx.send(errs_clone).await {
                            error!(
                                target: "rustfs::scanner::folder",
                                event = EVENT_SCANNER_FOLDER_STATE,
                                component = LOG_COMPONENT_SCANNER,
                                subsystem = LOG_SUBSYSTEM_FOLDER,
                                state = "list_path_finished_send_failed",
                                error = %e,
                                "Scanner list_path_raw finished callback failed"
                            );
                        }
                    })
                })),
                ..scanner_abandoned_child_list_options()
            };
            #[cfg(test)]
            if let Some(observer) = list_path_raw_options_observer {
                let _ =
                    observer.send((options.skip_walkdir_total_timeout, options.walkdir_timeout, options.walkdir_stall_timeout));
            }
            if let Err(e) = crate::storage_api::ecstore_with_background_disk_io(
                child_ctx_clone.clone(),
                list_path_raw(child_ctx_clone.clone(), options),
            )
            .await
            {
                if is_missing_path_disk_error(&e) {
                    debug!(
                        target: "rustfs::scanner::folder",
                        event = EVENT_SCANNER_FOLDER_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_FOLDER,
                        bucket = %bucket_clone,
                        prefix = %prefix_clone,
                        state = "list_path_missing",
                        error = %e,
                        "Scanner list_path_raw missing path skipped"
                    );
                } else {
                    error!(
                        target: "rustfs::scanner::folder",
                        event = EVENT_SCANNER_FOLDER_STATE,
                        component = LOG_COMPONENT_SCANNER,
                        subsystem = LOG_SUBSYSTEM_FOLDER,
                        bucket = %bucket_clone,
                        prefix = %prefix_clone,
                        state = "list_path_failed",
                        error = %e,
                        "Scanner list_path_raw failed"
                    );
                }
            }
        });

        let mut found_objects = false;
        let mut agreed_closed = false;
        let mut partial_closed = false;
        let mut finished_closed = false;
        let mut seen_heal_candidates: HashSet<(String, Option<String>, MetaCacheHealCandidateKind)> = HashSet::new();
        let mut seen_truncated_objects: HashSet<String> = HashSet::new();

        loop {
            if child_ctx.is_cancelled() || (agreed_closed && partial_closed && finished_closed) {
                break;
            }
            #[cfg(test)]
            let defer_entries = self.defer_abandoned_entries_until_finished && !finished_closed;
            #[cfg(not(test))]
            let defer_entries = false;

            select! {
                entry_name = agreed_rx.recv(), if !agreed_closed && !defer_entries => {
                    let Some(entry_name) = entry_name else {
                        agreed_closed = true;
                        continue;
                    };
                    (self.update_current_path)(&entry_name).await;
                }
                entries = partial_rx.recv(), if !partial_closed && !defer_entries => {
                    let Some(entries) = entries else {
                        partial_closed = true;
                        continue;
                    };
                    if !self.try_should_heal().await? {
                        child_ctx.cancel();
                        break;
                    }

                    #[cfg(test)]
                    let candidate_limit = self.partial_heal_candidate_limit.unwrap_or(MAX_META_CACHE_HEAL_CANDIDATES);
                    #[cfg(not(test))]
                    let candidate_limit = MAX_META_CACHE_HEAL_CANDIDATES;
                    let discovery = entries.discover_heal_candidates(&bucket, candidate_limit);
                    #[cfg(test)]
                    if let Some(observer) = &self.partial_heal_discovery_observer {
                        let _ = observer.send((discovery.candidates.len(), discovery.truncated_candidates.len()));
                    }
                    counter!(METRIC_SCANNER_HEAL_DISCOVERY_CANDIDATES_TOTAL)
                        .increment(u64::try_from(discovery.candidates.len()).unwrap_or(u64::MAX));
                    counter!(METRIC_SCANNER_HEAL_DISCOVERY_SUB_QUORUM_TOTAL).increment(
                        u64::try_from(
                            discovery
                                .candidates
                                .iter()
                                .filter(|candidate| candidate.replica_count < disks_quorum)
                                .count(),
                        )
                        .unwrap_or(u64::MAX),
                    );
                    counter!(METRIC_SCANNER_HEAL_DISCOVERY_UNVERIFIED_TOTAL).increment(
                        u64::try_from(discovery.unverified_count).unwrap_or(u64::MAX),
                    );
                    if discovery.truncated {
                        counter!(METRIC_SCANNER_HEAL_DISCOVERY_TRUNCATED_TOTAL).increment(1);
                    }

                    for candidate in discovery.candidates {
                        let sub_quorum_candidate = candidate.replica_count < disks_quorum;
                        let version_id = candidate.validated_version().map(|id| id.to_string());
                        let identity = (candidate.object.clone(), version_id.clone(), candidate.kind.clone());
                        if seen_heal_candidates.len() >= MAX_META_CACHE_HEAL_CANDIDATES
                            && !seen_heal_candidates.contains(&identity)
                        {
                            continue;
                        }
                        if !seen_heal_candidates.insert(identity) {
                            continue;
                        }
                        let request = if candidate.is_unversioned() {
                            build_non_destructive_object_heal_request(
                                bucket.clone(),
                                candidate.object.clone(),
                                self.scan_mode,
                                HealChannelPriority::High,
                            )
                        } else {
                            build_object_heal_request(
                                bucket.clone(),
                                candidate.object.clone(),
                                version_id.clone(),
                                self.scan_mode,
                                HealChannelPriority::High,
                            )
                        };
                        (self.update_current_path)(&candidate.object).await;
                        let admission = self.send_required_scanner_heal_request(
                            PendingScannerHealKind::Object,
                            bucket.clone(),
                            Some(candidate.object.clone()),
                            version_id.clone(),
                            request,
                        )
                        .await?;
                        if admission.is_admitted() {
                            counter!(METRIC_SCANNER_HEAL_DISCOVERY_QUEUED_TOTAL).increment(1);
                        } else if sub_quorum_candidate {
                            self.mark_pending_scanner_heal_reason(
                                PendingScannerHealKind::Object,
                                &bucket,
                                Some(&candidate.object),
                                version_id.as_deref(),
                                "sub_quorum_metadata",
                            );
                        }
                        found_objects = true;
                    }

                    // Candidates beyond the main cap remain exact
                    // version requests; never downgrade them to a
                    // latest-version (version_id=None) heal.
                    for candidate in discovery.truncated_candidates {
                        let version_id = candidate.validated_version().map(|id| id.to_string());
                        let identity = (candidate.object.clone(), version_id.clone(), candidate.kind.clone());
                        if seen_truncated_objects.len() >= MAX_META_CACHE_HEAL_TRUNCATED_OBJECTS
                            && !seen_truncated_objects.contains(&candidate.object)
                        {
                            continue;
                        }
                        seen_truncated_objects.insert(candidate.object.clone());
                        if !seen_heal_candidates.insert(identity) {
                            continue;
                        }
                        let request = if candidate.is_unversioned() {
                            build_non_destructive_object_heal_request(
                                bucket.clone(),
                                candidate.object.clone(),
                                self.scan_mode,
                                HealChannelPriority::High,
                            )
                        } else {
                            build_object_heal_request(
                                bucket.clone(),
                                candidate.object.clone(),
                                version_id.clone(),
                                self.scan_mode,
                                HealChannelPriority::High,
                            )
                        };
                        (self.update_current_path)(&candidate.object).await;
                        let admission = self
                            .send_required_scanner_heal_request(
                                PendingScannerHealKind::Object,
                                bucket.clone(),
                                Some(candidate.object.clone()),
                                version_id,
                                request,
                            )
                            .await?;
                        if admission.is_admitted() {
                            counter!(METRIC_SCANNER_HEAL_DISCOVERY_QUEUED_TOTAL).increment(1);
                        }
                        found_objects = true;
                    }


                }
                errs = finished_rx.recv(), if !finished_closed => {
                    let Some(errs) = errs else {
                        finished_closed = true;
                        continue;
                    };
                    finished_closed = true;
                    // The callback preserves the input disk order. A failing sibling does
                    // not invalidate this walk, but the selected source's rejection does.
                    if let Some(error) = self.disks.iter().position(|disk| Arc::ptr_eq(disk, &self.local_disk))
                        .and_then(|idx| errs.get(idx)).and_then(Option::as_ref)
                    {
                        let error = StorageError::from(error.clone());
                        if is_scanner_source_unavailable(&error) {
                            return Err(ScannerError::source_unavailable(error));
                        }
                    }
                    if disk_errors_are_only_missing_paths(&errs) {
                        debug!(
                            target: "rustfs::scanner::folder",
                            event = EVENT_SCANNER_FOLDER_STATE,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_FOLDER,
                            state = "list_path_finished_missing_paths",
                            errors = ?errs,
                            "Scanner list_path_raw finished with missing paths"
                        );
                    } else {
                        error!(
                            target: "rustfs::scanner::folder",
                            event = EVENT_SCANNER_FOLDER_STATE,
                            component = LOG_COMPONENT_SCANNER,
                            subsystem = LOG_SUBSYSTEM_FOLDER,
                            state = "list_path_finished_with_errors",
                            errors = ?errs,
                            "Scanner list_path_raw finished with disk errors"
                        );
                    }
                    // The producer awaited callback sends, not their
                    // consumption. Drain the remaining candidate and
                    // agreed frames before closing this child walk.
                }
                _ = child_ctx.cancelled() => {
                    break;
                }
            }
        }

        Ok(found_objects)
    }
}
