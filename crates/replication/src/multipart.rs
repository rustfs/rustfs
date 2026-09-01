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

use std::collections::HashMap;
use std::fmt;

use crate::http::{SUFFIX_ACTUAL_SIZE, get_internal_metadata};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicationMultipartPartInput {
    pub offset: i64,
    pub part_number: usize,
    pub part_size: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicationMultipartPartPlan {
    pub part_number: i32,
    pub part_size: i64,
    pub range: ReplicationMultipartRange,
    pub next_offset: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicationMultipartRange {
    pub start: i64,
    pub end: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationMultipartPlanError {
    InvalidOffset { offset: i64 },
    InvalidPartSize { part_size: i64 },
    PartRangeOverflow { offset: i64, part_size: i64 },
    PartOffsetOverflow { offset: i64, part_size: i64 },
    PartNumberOverflow { part_number: usize },
}

impl fmt::Display for ReplicationMultipartPlanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidOffset { offset } => write!(f, "invalid multipart replication part offset {offset}"),
            Self::InvalidPartSize { part_size } => write!(f, "invalid multipart replication part size {part_size}"),
            Self::PartRangeOverflow { offset, part_size } => {
                write!(f, "multipart replication part range overflows for offset {offset} and size {part_size}")
            }
            Self::PartOffsetOverflow { offset, part_size } => {
                write!(f, "multipart replication next offset overflows for offset {offset} and size {part_size}")
            }
            Self::PartNumberOverflow { part_number } => {
                write!(f, "multipart replication part number {part_number} overflows i32")
            }
        }
    }
}

impl std::error::Error for ReplicationMultipartPlanError {}

pub fn replication_multipart_part_plan(
    input: ReplicationMultipartPartInput,
) -> Result<ReplicationMultipartPartPlan, ReplicationMultipartPlanError> {
    if input.offset < 0 {
        return Err(ReplicationMultipartPlanError::InvalidOffset { offset: input.offset });
    }
    if input.part_size <= 0 {
        return Err(ReplicationMultipartPlanError::InvalidPartSize {
            part_size: input.part_size,
        });
    }

    let part_number = i32::try_from(input.part_number).map_err(|_| ReplicationMultipartPlanError::PartNumberOverflow {
        part_number: input.part_number,
    })?;
    let end = input
        .offset
        .checked_add(input.part_size - 1)
        .ok_or(ReplicationMultipartPlanError::PartRangeOverflow {
            offset: input.offset,
            part_size: input.part_size,
        })?;
    let next_offset = end.checked_add(1).ok_or(ReplicationMultipartPlanError::PartOffsetOverflow {
        offset: input.offset,
        part_size: input.part_size,
    })?;

    Ok(ReplicationMultipartPartPlan {
        part_number,
        part_size: input.part_size,
        range: ReplicationMultipartRange {
            start: input.offset,
            end,
        },
        next_offset,
    })
}

pub fn replication_multipart_complete_actual_size(user_defined: &HashMap<String, String>) -> String {
    get_internal_metadata(user_defined, SUFFIX_ACTUAL_SIZE).unwrap_or_default()
}

/// Largest body S3 accepts on a single `PutObject`. Anything above this has to
/// be uploaded as multipart; the limit is part of the S3 API, not a RustFS
/// tunable, so every generic S3 target enforces it.
pub const REPLICATION_MAX_SINGLE_PUT_SIZE: i64 = 5 * 1024 * 1024 * 1024;

/// Reject a single-`PutObject` replication transfer the target can never accept.
///
/// Replication mirrors the object's *source-side storage shape*: an object
/// written to the source with one `PutObject` replicates with one `PutObject`
/// whatever its size, and a multipart object replays the source's own part
/// layout. So a source object larger than [`REPLICATION_MAX_SINGLE_PUT_SIZE`]
/// that was not written as multipart can never reach a generic S3 target — the
/// remote rejects it with `EntityTooLarge`, but only after the whole body has
/// been streamed to it (rustfs#6825).
///
/// Returning the failure up front turns an unbounded wasted transfer plus an
/// opaque remote error into a stated, diagnosable limit. RustFS deliberately
/// does not re-chunk such an object into multipart on the replication side:
/// the target's part layout is the source's, and rewriting it would break the
/// ETag/part identity that heal and delete convergence address.
pub fn replication_single_put_size_error(is_multipart: bool, transfer_size: i64) -> Option<String> {
    if is_multipart || transfer_size <= REPLICATION_MAX_SINGLE_PUT_SIZE {
        return None;
    }
    Some(format!(
        "object of {transfer_size} bytes was not written as multipart on the source and exceeds the \
         {REPLICATION_MAX_SINGLE_PUT_SIZE} byte single-PutObject limit of an S3 target; \
         re-upload it with multipart to make it replicable"
    ))
}

#[cfg(test)]
mod tests {
    use super::{
        REPLICATION_MAX_SINGLE_PUT_SIZE, ReplicationMultipartPartInput, ReplicationMultipartPartPlan,
        ReplicationMultipartPlanError, ReplicationMultipartRange, replication_multipart_complete_actual_size,
        replication_multipart_part_plan, replication_single_put_size_error,
    };
    use crate::http::{SUFFIX_ACTUAL_SIZE, insert_internal_metadata};
    use std::collections::HashMap;

    const MIB: i64 = 1024 * 1024;

    #[test]
    fn multipart_part_plan_builds_range_and_next_offset() {
        assert_eq!(
            replication_multipart_part_plan(ReplicationMultipartPartInput {
                offset: 0,
                part_number: 1,
                part_size: 10
            }),
            Ok(ReplicationMultipartPartPlan {
                part_number: 1,
                part_size: 10,
                range: ReplicationMultipartRange { start: 0, end: 9 },
                next_offset: 10
            })
        );

        assert_eq!(
            replication_multipart_part_plan(ReplicationMultipartPartInput {
                offset: 10,
                part_number: 2,
                part_size: 5
            }),
            Ok(ReplicationMultipartPartPlan {
                part_number: 2,
                part_size: 5,
                range: ReplicationMultipartRange { start: 10, end: 14 },
                next_offset: 15
            })
        );
    }

    #[test]
    fn multipart_part_plan_rejects_invalid_offsets_and_sizes() {
        assert_eq!(
            replication_multipart_part_plan(ReplicationMultipartPartInput {
                offset: -1,
                part_number: 1,
                part_size: 10
            }),
            Err(ReplicationMultipartPlanError::InvalidOffset { offset: -1 })
        );
        assert_eq!(
            replication_multipart_part_plan(ReplicationMultipartPartInput {
                offset: 0,
                part_number: 1,
                part_size: 0
            }),
            Err(ReplicationMultipartPlanError::InvalidPartSize { part_size: 0 })
        );
    }

    #[test]
    fn multipart_part_plan_rejects_overflow() {
        assert_eq!(
            replication_multipart_part_plan(ReplicationMultipartPartInput {
                offset: i64::MAX - 5,
                part_number: 1,
                part_size: 10
            }),
            Err(ReplicationMultipartPlanError::PartRangeOverflow {
                offset: i64::MAX - 5,
                part_size: 10
            })
        );
        assert_eq!(
            replication_multipart_part_plan(ReplicationMultipartPartInput {
                offset: i64::MAX,
                part_number: 1,
                part_size: 1
            }),
            Err(ReplicationMultipartPlanError::PartOffsetOverflow {
                offset: i64::MAX,
                part_size: 1
            })
        );
    }

    #[test]
    fn multipart_part_plan_rejects_part_number_overflow() {
        let overflowing_part_number = usize::MAX;

        assert_eq!(
            replication_multipart_part_plan(ReplicationMultipartPartInput {
                offset: 0,
                part_number: overflowing_part_number,
                part_size: 10
            }),
            Err(ReplicationMultipartPlanError::PartNumberOverflow {
                part_number: overflowing_part_number
            })
        );
    }

    #[test]
    fn multipart_complete_actual_size_reads_compatible_metadata() {
        let mut user_defined = HashMap::new();
        insert_internal_metadata(&mut user_defined, SUFFIX_ACTUAL_SIZE, "123".to_string());

        assert_eq!(replication_multipart_complete_actual_size(&user_defined), "123");
        assert!(replication_multipart_complete_actual_size(&HashMap::new()).is_empty());
    }

    #[test]
    fn single_put_size_guard_admits_transfers_a_target_can_accept() {
        for size in [
            0,
            1,
            MIB,
            REPLICATION_MAX_SINGLE_PUT_SIZE - 1,
            REPLICATION_MAX_SINGLE_PUT_SIZE,
        ] {
            assert_eq!(
                replication_single_put_size_error(false, size),
                None,
                "single PUT of {size} bytes is within the S3 limit and must not be rejected"
            );
        }
    }

    #[test]
    fn single_put_size_guard_rejects_an_oversized_single_put() {
        let size = REPLICATION_MAX_SINGLE_PUT_SIZE + 1;
        let err = replication_single_put_size_error(false, size).expect("oversized single PUT must be rejected");

        // The message is the operator's diagnosis: it has to name the actual
        // size, the limit, and the reason the object is on this route at all.
        assert!(err.contains(&size.to_string()), "message must name the object size: {err}");
        assert!(
            err.contains(&REPLICATION_MAX_SINGLE_PUT_SIZE.to_string()),
            "message must name the limit: {err}"
        );
        assert!(err.contains("multipart"), "message must name the remedy: {err}");
    }

    #[test]
    fn single_put_size_guard_never_rejects_the_multipart_route() {
        // Multipart replays the source part layout, so object size alone says
        // nothing about whether the target will accept it; the per-part limits
        // are the target's to enforce.
        assert_eq!(replication_single_put_size_error(true, REPLICATION_MAX_SINGLE_PUT_SIZE * 1024), None);
    }
}
