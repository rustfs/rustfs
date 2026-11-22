// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::*;

/// A linked tree paused on a different physical replica. It contains only
/// traversal fields, so dormant sources cannot recursively contain each other.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SourceTree {
    bucket: String,
    identity: DataUsageScanIdentity,
    started_plan: DataUsageScanPlanDigest,
    #[serde(serialize_with = "ordered_entries")]
    entries: HashMap<String, DataUsageEntry>,
    resume_after: Option<String>,
    checkpoint: Option<DataUsageScanCheckpoint>,
    coverage: Option<DataUsageScanCoverageReceipt>,
    cursor: Option<DataUsageRawEnumerationCursor>,
    index: Option<RawEnumerationPageIndex>,
    ancestors: Vec<RawEnumerationPageIndex>,
}

impl SourceTree {
    /// An inactive receipt may preserve an existing source position. Only an
    /// active, verified traversal can contribute new progress.
    pub(super) fn frontier(&self, info: &DataUsageCacheInfo) -> Option<&str> {
        if self.bucket != info.name
            || Some(self.identity) != info.scan_identity
            || Some(self.started_plan) != info.scan_progress.map(|p| p.started_plan)
        {
            return None;
        }
        self.coverage.as_ref().map(|receipt| receipt.through.as_str())
    }
}

// Dormant trees live in the checkpoint header stream. A cold process has
// fresh HashMap seeds; preserve stable bytes for immutable Merkle pages.
fn ordered_entries<S: serde::Serializer>(
    entries: &HashMap<String, DataUsageEntry>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    let mut sorted = entries.iter().collect::<Vec<_>>();
    sorted.sort_unstable_by(|a, b| a.0.cmp(b.0));
    serializer.collect_map(sorted.into_iter().map(|(name, entry)| (name, entry.ordered_encoding())))
}

impl DataUsageCache {
    pub(crate) fn activate_directory_source(&mut self, source: String, endpoints: &HashSet<String>) {
        let (Some(identity), Some(progress)) = (self.info.scan_identity, self.info.scan_progress) else {
            return;
        };
        let previous = self.info.scan_directory_source.replace(source.clone());
        // Every production endpoint has an erasure-set slot. A disk replacement
        // cannot retain a backup under the same slot or under a retired endpoint.
        let current_location = directory_source_location(&source);
        self.info.scan_directory_work.retain(|work| {
            work.endpoint() == source
                || endpoints.contains(directory_source_location(work.endpoint()))
                    && directory_source_location(work.endpoint()) != current_location
        });
        self.info.scan_source_trees.retain(|key, tree| {
            tree.bucket == self.info.name
                && tree.identity == identity
                && tree.started_plan == progress.started_plan
                && (key == &source
                    || endpoints.contains(directory_source_location(key)) && directory_source_location(key) != current_location)
        });
        let Some(previous) = previous else {
            // Pre-source checkpoints identify an erasure set but never recorded
            // which disk supplied the partial tree. The same keys can have
            // different metadata on another replica. Keep the certified usage,
            // repair obligations and observations outside this traversal.
            if self.has_scan_position() {
                self.clear_active_directory_traversal();
            }
            return;
        };
        if previous == source {
            return;
        }
        let entries = std::mem::take(&mut self.cache);
        let linked = entries.get(&hash_path(&self.info.name).0).is_some_and(|root| !root.compacted);
        let previous_tree = SourceTree {
            bucket: self.info.name.clone(),
            identity,
            started_plan: progress.started_plan,
            entries,
            resume_after: self.info.scan_resume_after.take(),
            checkpoint: self.info.scan_checkpoint.take(),
            coverage: self.info.scan_coverage_receipt.take(),
            cursor: self.info.scan_raw_enumeration_cursor.take(),
            index: self.info.scan_raw_enumeration_page_index.take(),
            ancestors: std::mem::take(&mut self.info.scan_raw_enumeration_path_page_indexes),
        };
        if linked
            && endpoints.contains(directory_source_location(&previous))
            && directory_source_location(&previous) != current_location
        {
            self.info.scan_source_trees.insert(previous, Arc::new(previous_tree));
        }
        if let Some(tree) = self.info.scan_source_trees.remove(&source) {
            let tree = Arc::unwrap_or_clone(tree);
            self.cache = tree.entries;
            self.info.scan_resume_after = tree.resume_after;
            self.info.scan_coverage_receipt = tree.coverage;
            self.info.scan_checkpoint = tree.checkpoint;
            self.info.scan_raw_enumeration_cursor = tree.cursor;
            self.info.scan_raw_enumeration_page_index = tree.index;
            self.info.scan_raw_enumeration_path_page_indexes = tree.ancestors;
            // A dormant tree must provide the same coverage/graph guarantees
            // required of an active checkpoint before its child links are used.
            if self.checked_flatten_complete_scope(&self.info.name).is_none()
                || self.info.scan_coverage_receipt.is_some() && self.validated_scan_frontier().is_none()
            {
                self.clear_active_directory_traversal();
            }
        }
    }
    fn clear_active_directory_traversal(&mut self) {
        self.cache.clear();
        self.info.scan_resume_after = None;
        self.info.scan_checkpoint = None;
        self.info.scan_coverage_receipt = None;
        self.info.scan_raw_enumeration_cursor = None;
        self.info.scan_raw_enumeration_page_index = None;
        self.info.scan_raw_enumeration_path_page_indexes.clear();
    }
}

fn directory_source_location(source: &str) -> &str {
    source.split_once('|').map_or(source, |(_, endpoint)| endpoint)
}
