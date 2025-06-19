use crate::arn::TargetID;
use std::collections::HashSet;

/// TargetIDSet - A collection representation of TargetID.
pub type TargetIdSet = HashSet<TargetID>;

/// Provides a Go-like method for TargetIdSet (can be implemented as trait if needed)
#[allow(dead_code)]
pub(crate) fn new_target_id_set(target_ids: Vec<TargetID>) -> TargetIdSet {
    target_ids.into_iter().collect()
}

// HashSet has built-in clone, union, difference and other operations.
// But the Go version of the method returns a new Set, and the HashSet method is usually iterator or modify itself.
// If you need to exactly match Go's API style, you can add wrapper functions.
