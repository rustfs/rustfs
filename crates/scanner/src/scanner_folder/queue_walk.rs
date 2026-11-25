// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::*;

impl FolderScanner {
    pub(super) async fn next_queued_child(
        &mut self,
        ctx: &CancellationToken,
        frame: &mut FolderFrame,
        listing: &mut FolderListing,
    ) -> Result<Option<ChildRequest>, ScannerError> {
        let folder = &frame.folder;
        let this_hash = &frame.hash;
        let into = &mut frame.entry;
        let mut work = listing.dir_reader.as_mut().and_then(ScannerDirectory::work);
        loop {
            if let Some(work) = &mut work
                && let Some((name, linked)) = listing.acknowledge_previous.take()
            {
                work.gap |= self.coverage_gap;
                work.acknowledge(&name, linked, into.compacted);
            }
            if ctx.is_cancelled() {
                return Err(ScannerError::Other("Operation cancelled".to_string()));
            }
            let queued_folder = if let Some(work) = &mut work {
                let Some(name) =
                    wait_for_scanner_io(ctx, run_scanner_node_operation(self.node_admission.as_ref(), ctx, work.next_name()))
                        .await???
                else {
                    return Ok(None);
                };
                let name = path_join_buf(&[&folder.name, &name]);
                let source = if !listing.force_compact && self.old_cache.find(&name).is_some() {
                    FolderScanSource::Existing
                } else {
                    FolderScanSource::New
                };
                let h = hash_path(&name);
                if source == FolderScanSource::Existing
                    && let Some(updates) = &mut self.usage_updates
                {
                    updates
                        .cache
                        .copy_with_children(&self.old_cache, &h, &Some(this_hash.clone()));
                }
                QueuedFolder {
                    folder: CachedFolder {
                        name,
                        parent: Some(this_hash.clone()),
                        object_heal_prob_div: folder.object_heal_prob_div,
                    },
                    source,
                }
            } else {
                let Some(queued) = listing.queued_folders.next() else { return Ok(None) };
                queued
            };
            let mut folder_item = queued_folder.folder;
            let h = hash_path(&folder_item.name);
            if work.is_some() {
                listing.acknowledge_previous = Some((folder_item.name.clone(), false));
            }
            if work.is_some()
                && queued_folder.source == FolderScanSource::Existing
                && self.can_reuse_scoped_child(folder, into, &folder_item.name, &h)
            {
                self.reuse_clean_root_child(&folder_item, &h, into);
                if let Some((_, linked)) = &mut listing.acknowledge_previous {
                    *linked = true;
                }
                continue;
            }
            let reuse_covered = self.can_reuse_covered_namespace(&folder_item.name, into.compacted);
            let mut child_mode = if self.is_erasure_mode && is_erasure_candidate(&folder_item.name) {
                FolderScanMode::ErasureCandidate {
                    reuse_namespace: reuse_covered,
                    parent_has_metadata: listing.found_object_metadata,
                }
            } else {
                FolderScanMode::Namespace
            };
            if child_mode == FolderScanMode::Namespace && reuse_covered {
                let started = Instant::now();
                self.new_cache.copy_with_children(&self.old_cache, &h, &folder_item.parent);
                into.add_child(&h);
                global_metrics()
                    .scan_observation()
                    .record_cache_reuse(ScannerCacheReuseKind::CoveredNamespace, started.elapsed());
                if let Some((_, linked)) = &mut listing.acknowledge_previous {
                    *linked = true;
                }
                continue;
            }

            match queued_folder.source {
                FolderScanSource::New => {
                    // Add new folders to the update tree so totals update for these.
                    if !into.compacted
                        && let Some(updates) = &mut self.usage_updates
                    {
                        let mut found_any = false;
                        let mut parent = clone_cache_value(this_hash);
                        let update_root_hash = hash_path(&updates.cache.info.name);

                        while parent != update_root_hash {
                            let parent_key = clone_cache_value(&parent.0);
                            let e = updates.cache.find(&parent_key);
                            if e.is_none_or(|v| v.compacted) {
                                found_any = true;
                                break;
                            }
                            if let Some(next) = updates.cache.search_parent(&parent) {
                                parent = next;
                            } else {
                                found_any = true;
                                break;
                            }
                        }
                        if !found_any {
                            // Add non-compacted empty entry.
                            updates
                                .cache
                                .replace_hashed(&h, &Some(clone_cache_value(this_hash)), &DataUsageEntry::default());
                        }
                    }
                }
                FolderScanSource::Existing => {
                    // Usage sampling is not proof that a Deep check ran.
                    if let Some(reuse) = self.sampled_namespace_reuse(&h, into.compacted, listing.forward_sweep) {
                        if reuse {
                            if child_mode == FolderScanMode::Namespace {
                                let started = Instant::now();
                                self.new_cache.copy_with_children(&self.old_cache, &h, &folder_item.parent);
                                into.add_child(&h);
                                global_metrics()
                                    .scan_observation()
                                    .record_cache_reuse(ScannerCacheReuseKind::SampledNamespace, started.elapsed());
                                self.record_scan_resume_hint(&folder_item.name);
                                continue;
                            }
                            // Sampling may skip namespace work, never the
                            // classification that protects its parent object.
                            child_mode = FolderScanMode::ErasureCandidate {
                                reuse_namespace: true,
                                parent_has_metadata: listing.found_object_metadata,
                            };
                        } else {
                            folder_item.object_heal_prob_div = data_usage_update_dir_cycles();
                        }
                    }
                }
            }

            (self.update_current_path)(&folder_item.name).await;

            // Keep a compacted namespace child's accounting local until it
            // succeeds. Ordinary child errors can then discard it without
            // rolling back totals already committed by healthy siblings.
            let isolate_compacted_child =
                into.compacted && listing.isolate_compacted_children && child_mode == FolderScanMode::Namespace;
            // A parked queue owns only committed subtree totals. Run the
            // active child in its own accumulator so cancellation cannot
            // count a partial child twice at the next admission.
            let child_summary = isolate_compacted_child || (into.compacted && work.is_some());
            let accounting = if child_summary {
                ChildAccounting::Summary {
                    isolated: isolate_compacted_child,
                }
            } else if into.compacted {
                ChildAccounting::Shared
            } else {
                ChildAccounting::Linked
            };
            return Ok(Some(ChildRequest {
                folder: folder_item,
                mode: child_mode,
                accounting,
                origin: ChildOrigin::Queued,
            }));
        }
    }

    pub(super) async fn accept_queued_child(
        &mut self,
        ctx: &CancellationToken,
        frame: &mut FolderFrame,
        listing: &mut FolderListing,
        child: &FolderFrame,
        accounting: ChildAccounting,
        result: Result<FolderScanOutcome, ScannerError>,
    ) -> Result<bool, ScannerError> {
        let mut work = listing.dir_reader.as_mut().and_then(ScannerDirectory::work);
        let into = &mut frame.entry;
        let folder = &child.folder;
        let hash = &child.hash;
        let outcome = match (accounting, result) {
            (ChildAccounting::Shared, result) => result?,
            (ChildAccounting::Summary { .. }, Ok(outcome)) => {
                into.merge(&child.entry);
                if let Some(work) = &mut work {
                    if outcome == FolderScanOutcome::Namespace && !self.coverage_gap && child.entry.failed_objects == 0 {
                        work.compacted_total.merge(&child.entry);
                        work.acknowledge(&folder.name, false, true);
                    } else if child.entry.failed_objects != 0 {
                        self.coverage_gap = true;
                    }
                }
                outcome
            }
            (ChildAccounting::Linked, Ok(outcome)) => outcome,
            (accounting, Err(error)) => {
                if matches!(&error, ScannerError::DirectoryChanged | ScannerError::AbandonedChildUnverified) {
                    return Err(error);
                }
                if ctx.is_cancelled() || matches!(&error, ScannerError::SourceUnavailable(_) | ScannerError::ResourceDeferred) {
                    if matches!(accounting, ChildAccounting::Summary { .. }) {
                        // Propagate the counted prefix before yielding the source.
                        into.merge(&child.entry);
                    } else {
                        self.preserve_partial_child_progress(&folder.parent, hash, into, &child.entry)
                            .await;
                    }
                    return Err(error);
                }
                if child.mode != FolderScanMode::Namespace
                    || (into.compacted && !matches!(accounting, ChildAccounting::Summary { isolated: true }))
                {
                    return Err(error);
                }
                warn!(
                    target: "rustfs::scanner::folder",
                    event = EVENT_SCANNER_FOLDER_STATE,
                    component = LOG_COMPONENT_SCANNER,
                    subsystem = LOG_SUBSYSTEM_FOLDER,
                    state = "child_scan_failed",
                    folder = %frame.folder.name,
                    child_folder = %folder.name,
                    error = %error,
                    "Scanner child folder scan failed"
                );
                // A skipped namespace is a gap even outside a checkpointed sweep.
                self.coverage_gap = true;
                self.finish_raw_enumeration_parent(&folder.name);
                return Ok(true);
            }
        };

        self.finish_raw_enumeration_parent(&folder.name);
        if outcome == FolderScanOutcome::Vanished || outcome == FolderScanOutcome::ErasureData {
            self.new_cache.delete_recursive(hash);
            if let Some(updates) = &mut self.usage_updates {
                updates.cache.delete_recursive(hash);
            }
            into.children.remove(&clone_cache_value(&hash.0));
            if outcome == FolderScanOutcome::Vanished || listing.found_object_metadata {
                return Ok(true);
            }
            // Siblings that completed cannot certify an unresolved parent object.
            self.coverage_gap |= listing.forward_sweep;
            if self.coverage_frontier.as_deref().is_some_and(|frontier| {
                frontier
                    .strip_prefix(&frame.folder.name)
                    .is_some_and(|suffix| suffix.starts_with('/'))
            }) {
                self.coverage_frontier = None;
            }
            self.record_missing_erasure_metadata(&frame.folder, into).await?;
            return Ok(false);
        }

        if into.compacted {
            self.record_completed_child(&folder.name);
            if !frame.was_compacted && work.is_some() {
                self.maybe_send_owned_checkpoint(&frame.folder, &frame.hash, into, work.as_deref());
            }
            self.send_update_for_entry(&frame.hash, &frame.folder.parent, into).await;
            tokio::task::yield_now().await;
        } else {
            into.add_child(hash);
            if let Some(work) = &mut work {
                work.acknowledge(&folder.name, true, false);
            }
            self.record_completed_child(&folder.name);
            tokio::task::yield_now().await;
            self.maybe_send_checkpoint(&frame.folder, &frame.hash, into);
            self.refresh_usage_subtree(hash, &folder.parent);
            self.send_update().await;
        }
        Ok(true)
    }
}
