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

/// The current folder owns its accumulator even when its parent lent it a compacted total.
pub(super) struct FolderFrame {
    pub(super) folder: CachedFolder,
    pub(super) mode: FolderScanMode,
    pub(super) hash: DataUsageHash,
    pub(super) entry: DataUsageEntry,
    pub(super) was_compacted: bool,
    pub(super) completed_budgeted_object: bool,
    pub(super) started: Instant,
    pub(super) trace_started_at: Option<Instant>,
}

impl FolderFrame {
    fn new(folder: CachedFolder, entry: DataUsageEntry, mode: FolderScanMode) -> Self {
        Self {
            mode,
            hash: hash_path(&folder.name),
            folder,
            was_compacted: entry.compacted,
            entry,
            completed_budgeted_object: false,
            started: Instant::now(),
            trace_started_at: trace_start_instant(),
        }
    }
}

pub(super) struct FolderListing {
    pub(super) dir_reader: Option<ScannerDirectory>,
    pub(super) queued_folders: std::vec::IntoIter<QueuedFolder>,
    pub(super) acknowledge_previous: Option<(String, bool)>,
    pub(super) force_compact: bool,
    pub(super) isolate_compacted_children: bool,
    pub(super) found_object_metadata: bool,
    pub(super) forward_sweep: bool,
    pub(super) object_to_finish: Option<(ScannerItem, Option<u64>)>,
    pub(super) abandoned_children: DataUsageHashMap,
    pub(super) healing: Option<AbandonedFolders>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum ChildAccounting {
    Linked,
    Shared,
    Summary { isolated: bool },
}

pub(super) enum ChildOrigin {
    Queued,
    Abandoned(tokio_util::sync::DropGuard),
}

pub(super) struct ChildRequest {
    pub(super) folder: CachedFolder,
    pub(super) mode: FolderScanMode,
    pub(super) accounting: ChildAccounting,
    pub(super) origin: ChildOrigin,
}

pub(super) enum WalkAction {
    Enter(bool),
    Advance(Box<FolderListing>),
    Descend(Box<FolderListing>, ChildRequest),
    Finish,
    Return(Result<FolderScanOutcome, ScannerError>),
}

struct SuspendedFolder {
    frame: FolderFrame,
    listing: Box<FolderListing>,
    accounting: ChildAccounting,
    origin: ChildOrigin,
    checkpointed: bool,
    suspended_updates: Option<ScannerUsageUpdates>,
}

impl FolderScanner {
    pub(super) fn suspend_checkpoint_parent(
        &mut self,
        hash: &mut DataUsageHash,
        entry: &mut DataUsageEntry,
    ) -> Result<bool, ScannerError> {
        if self.checkpoint_tx.is_none() || self.old_cache.info.scan_progress.is_none() {
            return Ok(false);
        }
        let depth = self.checkpoint_depth;
        if depth >= MAX_DATA_USAGE_CACHE_DEPTH {
            return Err(ScannerError::Other("scanner checkpoint ancestor depth limit exceeded".to_string()));
        }
        if self.checkpoint_ancestors.len() == depth {
            self.checkpoint_ancestors.push(ScannerCheckpointAncestor::default());
        }
        let slot = &mut self.checkpoint_ancestors[depth];
        std::mem::swap(hash, &mut slot.hash);
        std::mem::swap(entry, &mut slot.entry);
        self.checkpoint_depth += 1;
        Ok(true)
    }

    async fn advance_folder(
        &mut self,
        ctx: &CancellationToken,
        frame: &mut FolderFrame,
        mut listing: Box<FolderListing>,
    ) -> Result<WalkAction, ScannerError> {
        if listing.healing.is_none() {
            if let Some(child) = self.next_queued_child(ctx, frame, &mut listing).await? {
                return Ok(WalkAction::Descend(listing, child));
            }
            if !self.prepare_abandoned_children(ctx, frame, &mut listing).await? {
                return Ok(WalkAction::Finish);
            }
        }
        if let Some(abandoned) = &mut listing.healing
            && let Some((folder, guard)) = self.next_abandoned_child(ctx, frame, abandoned).await?
        {
            let accounting = if frame.entry.compacted {
                ChildAccounting::Shared
            } else {
                ChildAccounting::Linked
            };
            return Ok(WalkAction::Descend(
                listing,
                ChildRequest {
                    folder,
                    mode: FolderScanMode::Namespace,
                    accounting,
                    origin: ChildOrigin::Abandoned(guard),
                },
            ));
        }
        Ok(WalkAction::Finish)
    }

    pub(super) async fn scan_folder_inner(
        &mut self,
        ctx: CancellationToken,
        folder: CachedFolder,
        into: &mut DataUsageEntry,
        mode: FolderScanMode,
        isolate_compacted_namespace_errors: bool,
    ) -> Result<FolderScanOutcome, ScannerError> {
        let mut parents = Vec::<SuspendedFolder>::new();
        // A compacted child borrows the entire parent accumulator. Reuse the
        // empty replacement across siblings, including its histogram allocations.
        let mut shared_slots = Vec::new();
        let mut current = FolderFrame::new(folder, std::mem::take(into), mode);
        let mut action = WalkAction::Enter(isolate_compacted_namespace_errors);
        loop {
            #[cfg(test)]
            tests::checkpoint_fixture::deep_compacted::observe_folder_frame_depth(parents.len() + 1);
            action = match action {
                WalkAction::Enter(isolate) => self.enumerate_folder(&ctx, &mut current, isolate).await,
                WalkAction::Advance(listing) => self.advance_folder(&ctx, &mut current, listing).await,
                WalkAction::Finish => Ok(WalkAction::Return(self.finish_folder(&ctx, &mut current).await)),
                WalkAction::Descend(listing, child) => {
                    let checkpoint = if child.accounting == ChildAccounting::Linked {
                        self.suspend_checkpoint_parent(&mut current.hash, &mut current.entry)
                    } else {
                        Ok(false)
                    };
                    let isolated = child.accounting == ChildAccounting::Summary { isolated: true };
                    let suspended_updates = if isolated { self.usage_updates.take() } else { None };
                    let entry = match child.accounting {
                        ChildAccounting::Shared => std::mem::replace(&mut current.entry, shared_slots.pop().unwrap_or_default()),
                        ChildAccounting::Linked => DataUsageEntry::default(),
                        ChildAccounting::Summary { .. } => DataUsageEntry {
                            compacted: true,
                            ..Default::default()
                        },
                    };
                    parents.push(SuspendedFolder {
                        frame: current,
                        listing,
                        accounting: child.accounting,
                        origin: child.origin,
                        checkpointed: matches!(checkpoint, Ok(true)),
                        suspended_updates,
                    });
                    current = FolderFrame::new(child.folder, entry, child.mode);
                    checkpoint.map(|_| WalkAction::Enter(isolated))
                }
                WalkAction::Return(result) => {
                    let Some(mut parent) = parents.pop() else {
                        *into = current.entry;
                        return result;
                    };
                    if parent.checkpointed {
                        self.checkpoint_depth -= 1;
                        let slot = &mut self.checkpoint_ancestors[self.checkpoint_depth];
                        std::mem::swap(&mut parent.frame.hash, &mut slot.hash);
                        std::mem::swap(&mut parent.frame.entry, &mut slot.entry);
                    }
                    if parent.accounting == ChildAccounting::Shared {
                        std::mem::swap(&mut parent.frame.entry, &mut current.entry);
                    }
                    if parent.accounting == (ChildAccounting::Summary { isolated: true }) {
                        self.usage_updates = parent.suspended_updates;
                    }
                    // Settle even errors from entering a child: the parent owns the
                    // decision to propagate a partial result or isolate a coverage gap.
                    let resume = match parent.origin {
                        ChildOrigin::Queued => {
                            self.accept_queued_child(
                                &ctx,
                                &mut parent.frame,
                                &mut parent.listing,
                                &current,
                                parent.accounting,
                                result,
                            )
                            .await
                        }
                        ChildOrigin::Abandoned(_cancel_on_exit) => self
                            .accept_abandoned_child(
                                &ctx,
                                &mut parent.frame,
                                &current,
                                result,
                                parent.accounting == ChildAccounting::Shared,
                            )
                            .await
                            .map(|()| true),
                    };
                    if parent.accounting == ChildAccounting::Shared {
                        shared_slots.push(current.entry);
                    }
                    current = parent.frame;
                    resume.map(|more| {
                        if more {
                            WalkAction::Advance(parent.listing)
                        } else {
                            WalkAction::Finish
                        }
                    })
                }
            }
            .unwrap_or_else(|error| WalkAction::Return(Err(error)));
        }
    }
}
