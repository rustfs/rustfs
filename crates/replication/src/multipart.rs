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

#[cfg(test)]
mod tests {
    use super::{
        ReplicationMultipartPartInput, ReplicationMultipartPartPlan, ReplicationMultipartPlanError, ReplicationMultipartRange,
        replication_multipart_complete_actual_size, replication_multipart_part_plan,
    };
    use crate::http::{SUFFIX_ACTUAL_SIZE, insert_internal_metadata};
    use std::collections::HashMap;

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
}
