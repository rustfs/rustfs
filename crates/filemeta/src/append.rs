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

use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

const APPEND_STATE_META_KEY: &str = "x-rustfs-internal-append-state";

/// Tracks the state of append-enabled objects.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct AppendState {
    pub state: AppendStateKind,
    pub epoch: u64,
    pub committed_length: i64,
    pub pending_segments: Vec<AppendSegment>,
}

/// Represents individual append segments that still need consolidation.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct AppendSegment {
    pub offset: i64,
    pub length: i64,
    pub data_dir: Option<Uuid>,
    pub etag: Option<String>,
    pub epoch: u64,
}

/// Possible append lifecycle states for an object version.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub enum AppendStateKind {
    #[default]
    Disabled,
    Inline,
    InlinePendingSpill,
    SegmentedActive,
    SegmentedSealed,
}

/// Persist the provided append state into object metadata.
pub fn set_append_state(metadata: &mut HashMap<String, String>, state: &AppendState) -> Result<()> {
    let encoded = serde_json::to_string(state).map_err(Error::other)?;
    metadata.insert(APPEND_STATE_META_KEY.to_string(), encoded);
    Ok(())
}

/// Remove the append state marker from metadata.
pub fn clear_append_state(metadata: &mut HashMap<String, String>) {
    metadata.remove(APPEND_STATE_META_KEY);
}

/// Load append state stored in metadata, if any.
pub fn get_append_state(metadata: &HashMap<String, String>) -> Result<Option<AppendState>> {
    let raw = match metadata.get(APPEND_STATE_META_KEY) {
        Some(val) if !val.is_empty() => val,
        _ => return Ok(None),
    };

    let decoded = serde_json::from_str(raw).map_err(Error::other)?;
    Ok(Some(decoded))
}

/// Complete append operations by consolidating pending segments and sealing the object
pub fn complete_append_operation(state: &mut AppendState) -> Result<()> {
    match state.state {
        AppendStateKind::SegmentedActive => {
            // Move all pending segments data to main parts and seal
            state.committed_length += state.pending_segments.iter().map(|s| s.length).sum::<i64>();
            state.pending_segments.clear();
            state.state = AppendStateKind::SegmentedSealed;
            state.epoch = state.epoch.saturating_add(1);
            Ok(())
        }
        AppendStateKind::Inline => {
            // Inline objects are always immediately committed, just seal them
            state.state = AppendStateKind::SegmentedSealed; // Transition to sealed
            state.epoch = state.epoch.saturating_add(1);
            Ok(())
        }
        AppendStateKind::InlinePendingSpill => {
            // Wait for spill to complete, then seal
            // In practice, this might need to trigger the spill completion first
            state.state = AppendStateKind::SegmentedSealed;
            state.pending_segments.clear();
            state.epoch = state.epoch.saturating_add(1);
            Ok(())
        }
        AppendStateKind::SegmentedSealed | AppendStateKind::Disabled => {
            // Already sealed or disabled
            Err(Error::other("Cannot complete append on sealed or disabled object"))
        }
    }
}

/// Abort append operations by discarding pending segments and returning to sealed state
pub fn abort_append_operation(state: &mut AppendState) -> Result<()> {
    match state.state {
        AppendStateKind::SegmentedActive => {
            // Discard all pending segments and seal
            state.pending_segments.clear();
            state.state = AppendStateKind::SegmentedSealed;
            state.epoch = state.epoch.saturating_add(1);
            Ok(())
        }
        AppendStateKind::Inline => {
            // Inline data is already committed, just seal
            state.state = AppendStateKind::SegmentedSealed;
            state.epoch = state.epoch.saturating_add(1);
            Ok(())
        }
        AppendStateKind::InlinePendingSpill => {
            // Cancel spill and keep inline data, then seal
            state.state = AppendStateKind::SegmentedSealed;
            state.pending_segments.clear();
            state.epoch = state.epoch.saturating_add(1);
            Ok(())
        }
        AppendStateKind::SegmentedSealed | AppendStateKind::Disabled => {
            // Already sealed or disabled
            Err(Error::other("Cannot abort append on sealed or disabled object"))
        }
    }
}

/// Check if an append operation can be completed
pub fn can_complete_append(state: &AppendState) -> bool {
    matches!(
        state.state,
        AppendStateKind::Inline | AppendStateKind::InlinePendingSpill | AppendStateKind::SegmentedActive
    )
}

/// Check if an append operation can be aborted
pub fn can_abort_append(state: &AppendState) -> bool {
    matches!(
        state.state,
        AppendStateKind::Inline | AppendStateKind::InlinePendingSpill | AppendStateKind::SegmentedActive
    )
}

/// Verify epoch for optimistic concurrency control
pub fn verify_append_epoch(current_state: &AppendState, expected_epoch: u64) -> Result<()> {
    if current_state.epoch != expected_epoch {
        Err(Error::other(format!(
            "Append operation conflict: expected epoch {}, found {}",
            expected_epoch, current_state.epoch
        )))
    } else {
        Ok(())
    }
}

/// Prepare next append operation by incrementing epoch
pub fn prepare_next_append(state: &mut AppendState) {
    state.epoch = state.epoch.saturating_add(1);
}

/// Validate that a new append segment doesn't conflict with existing segments
pub fn validate_new_segment(state: &AppendState, new_offset: i64, new_length: i64) -> Result<()> {
    let new_end = new_offset + new_length;

    // Check it doesn't overlap with committed data
    if new_offset < state.committed_length {
        return Err(Error::other(format!(
            "New segment overlaps with committed data: offset {} < committed_length {}",
            new_offset, state.committed_length
        )));
    }

    // Check it doesn't overlap with existing pending segments
    for existing in &state.pending_segments {
        let existing_start = existing.offset;
        let existing_end = existing.offset + existing.length;

        // Check for any overlap
        if new_offset < existing_end && new_end > existing_start {
            return Err(Error::other(format!(
                "New segment [{}, {}) overlaps with existing segment [{}, {})",
                new_offset, new_end, existing_start, existing_end
            )));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fileinfo::FileInfo;

    #[test]
    fn append_state_roundtrip_in_metadata() {
        let mut metadata = HashMap::new();
        let state = AppendState {
            state: AppendStateKind::SegmentedActive,
            epoch: 42,
            committed_length: 2048,
            pending_segments: vec![AppendSegment {
                offset: 2048,
                length: 512,
                data_dir: Some(Uuid::new_v4()),
                etag: Some("abc123".to_string()),
                epoch: 0,
            }],
        };

        set_append_state(&mut metadata, &state).expect("persist append state");
        assert!(metadata.contains_key(APPEND_STATE_META_KEY));

        let decoded = get_append_state(&metadata)
            .expect("decode append state")
            .expect("state present");
        assert_eq!(decoded, state);

        clear_append_state(&mut metadata);
        assert!(!metadata.contains_key(APPEND_STATE_META_KEY));
        assert!(get_append_state(&metadata).unwrap().is_none());
    }

    #[test]
    fn fileinfo_append_state_migration_compatibility() {
        // Test old inline data object
        let mut inline_fi = FileInfo {
            size: 1024,
            ..Default::default()
        };
        inline_fi.set_inline_data();

        let state = inline_fi.get_append_state();
        assert_eq!(state.state, AppendStateKind::Inline);
        assert_eq!(state.committed_length, 1024);
        assert!(state.pending_segments.is_empty());
        assert!(inline_fi.is_appendable());
        assert!(!inline_fi.has_pending_appends());

        // Test old regular object
        let regular_fi = FileInfo {
            size: 2048,
            ..Default::default()
        };
        // No inline_data marker

        let state = regular_fi.get_append_state();
        assert_eq!(state.state, AppendStateKind::SegmentedSealed);
        assert_eq!(state.committed_length, 2048);
        assert!(state.pending_segments.is_empty());
        assert!(!regular_fi.is_appendable());
        assert!(!regular_fi.has_pending_appends());

        // Test explicit append state
        let mut append_fi = FileInfo::default();
        let explicit_state = AppendState {
            state: AppendStateKind::SegmentedActive,
            epoch: 5,
            committed_length: 1500,
            pending_segments: vec![AppendSegment {
                offset: 1500,
                length: 300,
                data_dir: Some(Uuid::new_v4()),
                etag: Some("def456".to_string()),
                epoch: 0,
            }],
        };

        append_fi.set_append_state(&explicit_state).expect("set explicit state");
        let retrieved_state = append_fi.get_append_state();
        assert_eq!(retrieved_state, explicit_state);
        assert!(append_fi.is_appendable());
        assert!(append_fi.has_pending_appends());
    }

    #[test]
    fn append_state_transitions() {
        // Test state transition validation
        assert_eq!(AppendStateKind::default(), AppendStateKind::Disabled);

        let inline_state = AppendState {
            state: AppendStateKind::Inline,
            ..Default::default()
        };

        let spill_state = AppendState {
            state: AppendStateKind::InlinePendingSpill,
            ..Default::default()
        };

        let active_state = AppendState {
            state: AppendStateKind::SegmentedActive,
            ..Default::default()
        };

        let sealed_state = AppendState {
            state: AppendStateKind::SegmentedSealed,
            ..Default::default()
        };

        // Verify serialization works for all states
        for state in [inline_state, spill_state, active_state, sealed_state] {
            let mut metadata = HashMap::new();
            set_append_state(&mut metadata, &state).expect("serialize state");
            let decoded = get_append_state(&metadata).unwrap().unwrap();
            assert_eq!(decoded, state);
        }
    }

    #[test]
    fn complete_append_transitions() {
        // Test completing SegmentedActive with pending segments
        let mut active_state = AppendState {
            state: AppendStateKind::SegmentedActive,
            epoch: 5,
            committed_length: 1000,
            pending_segments: vec![
                AppendSegment {
                    offset: 1000,
                    length: 200,
                    data_dir: Some(Uuid::new_v4()),
                    etag: Some("abc123".to_string()),
                    epoch: 0,
                },
                AppendSegment {
                    offset: 1200,
                    length: 300,
                    data_dir: Some(Uuid::new_v4()),
                    etag: Some("def456".to_string()),
                    epoch: 0,
                },
            ],
        };

        assert!(can_complete_append(&active_state));
        complete_append_operation(&mut active_state).expect("complete should succeed");

        assert_eq!(active_state.state, AppendStateKind::SegmentedSealed);
        assert_eq!(active_state.committed_length, 1500); // 1000 + 200 + 300
        assert!(active_state.pending_segments.is_empty());
        assert_eq!(active_state.epoch, 6);

        // Test completing Inline state
        let mut inline_state = AppendState {
            state: AppendStateKind::Inline,
            epoch: 2,
            committed_length: 500,
            ..Default::default()
        };

        assert!(can_complete_append(&inline_state));
        complete_append_operation(&mut inline_state).expect("complete should succeed");

        assert_eq!(inline_state.state, AppendStateKind::SegmentedSealed);
        assert_eq!(inline_state.committed_length, 500); // Unchanged
        assert_eq!(inline_state.epoch, 3);

        // Test completing already sealed state should fail
        let mut sealed_state = AppendState {
            state: AppendStateKind::SegmentedSealed,
            ..Default::default()
        };

        assert!(!can_complete_append(&sealed_state));
        assert!(complete_append_operation(&mut sealed_state).is_err());
    }

    #[test]
    fn abort_append_transitions() {
        // Test aborting SegmentedActive with pending segments
        let mut active_state = AppendState {
            state: AppendStateKind::SegmentedActive,
            epoch: 3,
            committed_length: 800,
            pending_segments: vec![AppendSegment {
                offset: 800,
                length: 400,
                data_dir: Some(Uuid::new_v4()),
                etag: Some("xyz789".to_string()),
                epoch: 0,
            }],
        };

        assert!(can_abort_append(&active_state));
        abort_append_operation(&mut active_state).expect("abort should succeed");

        assert_eq!(active_state.state, AppendStateKind::SegmentedSealed);
        assert_eq!(active_state.committed_length, 800); // Unchanged, pending discarded
        assert!(active_state.pending_segments.is_empty());
        assert_eq!(active_state.epoch, 4);

        // Test aborting InlinePendingSpill
        let mut spill_state = AppendState {
            state: AppendStateKind::InlinePendingSpill,
            epoch: 1,
            committed_length: 100,
            pending_segments: vec![],
        };

        assert!(can_abort_append(&spill_state));
        abort_append_operation(&mut spill_state).expect("abort should succeed");

        assert_eq!(spill_state.state, AppendStateKind::SegmentedSealed);
        assert_eq!(spill_state.committed_length, 100);
        assert_eq!(spill_state.epoch, 2);

        // Test aborting disabled state should fail
        let mut disabled_state = AppendState {
            state: AppendStateKind::Disabled,
            ..Default::default()
        };

        assert!(!can_abort_append(&disabled_state));
        assert!(abort_append_operation(&mut disabled_state).is_err());
    }

    #[test]
    fn epoch_validation() {
        let state = AppendState {
            state: AppendStateKind::SegmentedActive,
            epoch: 10,
            committed_length: 1000,
            pending_segments: vec![],
        };

        // Valid epoch should succeed
        assert!(verify_append_epoch(&state, 10).is_ok());

        // Invalid epoch should fail
        assert!(verify_append_epoch(&state, 9).is_err());
        assert!(verify_append_epoch(&state, 11).is_err());

        // Error message should contain epoch information
        let error = verify_append_epoch(&state, 5).unwrap_err();
        let error_msg = error.to_string();
        assert!(error_msg.contains("expected epoch 5"));
        assert!(error_msg.contains("found 10"));
    }

    #[test]
    fn next_append_preparation() {
        let mut state = AppendState {
            state: AppendStateKind::SegmentedActive,
            epoch: 5,
            committed_length: 1000,
            pending_segments: vec![],
        };

        prepare_next_append(&mut state);
        assert_eq!(state.epoch, 6);

        // Test saturation behavior
        let mut max_state = AppendState {
            epoch: u64::MAX,
            ..Default::default()
        };

        prepare_next_append(&mut max_state);
        assert_eq!(max_state.epoch, u64::MAX); // Should saturate, not overflow
    }

    #[test]
    fn segment_validation() {
        let state = AppendState {
            state: AppendStateKind::SegmentedActive,
            epoch: 3,
            committed_length: 1000,
            pending_segments: vec![
                AppendSegment {
                    offset: 1000,
                    length: 200,
                    data_dir: Some(Uuid::new_v4()),
                    etag: Some("abc123".to_string()),
                    epoch: 0,
                },
                AppendSegment {
                    offset: 1300,
                    length: 300,
                    data_dir: Some(Uuid::new_v4()),
                    etag: Some("def456".to_string()),
                    epoch: 0,
                },
            ],
        };

        // Valid segment after existing segments
        assert!(validate_new_segment(&state, 1600, 100).is_ok());

        // Valid segment filling gap between committed and first pending
        assert!(validate_new_segment(&state, 1200, 100).is_ok());

        // Invalid segment overlapping with committed data
        assert!(validate_new_segment(&state, 900, 200).is_err());
        let error = validate_new_segment(&state, 900, 200).unwrap_err();
        assert!(error.to_string().contains("overlaps with committed data"));

        // Invalid segment overlapping with first pending segment
        assert!(validate_new_segment(&state, 1100, 100).is_err());
        let error = validate_new_segment(&state, 1100, 100).unwrap_err();
        assert!(error.to_string().contains("overlaps with existing segment"));

        // Invalid segment overlapping with second pending segment
        assert!(validate_new_segment(&state, 1400, 100).is_err());

        // Edge case: segment exactly touching committed data (should be valid)
        assert!(validate_new_segment(&state, 1000, 0).is_ok());

        // Edge case: segment exactly touching existing segment (should be valid)
        assert!(validate_new_segment(&state, 1200, 0).is_ok());
    }

    #[test]
    fn segment_validation_edge_cases() {
        let empty_state = AppendState {
            state: AppendStateKind::SegmentedActive,
            epoch: 1,
            committed_length: 500,
            pending_segments: vec![],
        };

        // First segment after committed data
        assert!(validate_new_segment(&empty_state, 500, 100).is_ok());
        assert!(validate_new_segment(&empty_state, 600, 200).is_ok());

        // Zero-length segments (edge case)
        assert!(validate_new_segment(&empty_state, 500, 0).is_ok());

        // Segment exactly at committed boundary
        assert!(validate_new_segment(&empty_state, 499, 1).is_err());
        assert!(validate_new_segment(&empty_state, 500, 1).is_ok());
    }
}
