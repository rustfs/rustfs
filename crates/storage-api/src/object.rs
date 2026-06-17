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

use std::fmt;
use time::OffsetDateTime;

#[derive(Debug, Default, Clone)]
pub struct HTTPPreconditions {
    pub if_match: Option<String>,
    pub if_none_match: Option<String>,
    pub if_modified_since: Option<OffsetDateTime>,
    pub if_unmodified_since: Option<OffsetDateTime>,
}

impl HTTPPreconditions {
    pub fn if_match_value(&self) -> Option<&str> {
        non_empty_condition_value(self.if_match.as_deref())
    }

    pub fn if_none_match_value(&self) -> Option<&str> {
        non_empty_condition_value(self.if_none_match.as_deref())
    }
}

#[derive(Debug, Default, Clone)]
pub struct ObjectLockRetentionOptions {
    pub mode: Option<String>,
    pub retain_until: Option<OffsetDateTime>,
    pub bypass_governance: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HTTPRangeError {
    InvalidRangeSpec(String),
}

impl fmt::Display for HTTPRangeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRangeSpec(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for HTTPRangeError {}

#[derive(Debug, Clone)]
pub struct HTTPRangeSpec {
    pub is_suffix_length: bool,
    pub start: i64,
    pub end: i64,
}

impl HTTPRangeSpec {
    pub fn from_part_sizes(object_size: i64, part_number: usize, part_sizes: impl IntoIterator<Item = i64>) -> Option<Self> {
        if object_size == 0 || part_number == 0 {
            return None;
        }

        let mut start = 0_i64;
        let mut end = -1_i64;
        let mut parts = part_sizes.into_iter();
        for _ in 0..part_number {
            let part_size = parts.next()?;
            start = end.checked_add(1)?;
            end = start.checked_add(part_size)?.checked_sub(1)?;
        }

        Some(Self {
            is_suffix_length: false,
            start,
            end,
        })
    }

    pub fn get_offset_length(&self, res_size: i64) -> Result<(usize, i64), HTTPRangeError> {
        let len = self.get_length(res_size)?;

        let mut start = self.start;
        if self.is_suffix_length {
            let suffix_len = if self.start < 0 {
                self.start
                    .checked_neg()
                    .ok_or_else(|| HTTPRangeError::InvalidRangeSpec("range value invalid: suffix length overflow".to_string()))?
            } else {
                self.start
            };
            start = res_size - suffix_len;
            if start < 0 {
                start = 0;
            }
        }
        let offset = usize::try_from(start)
            .map_err(|_| HTTPRangeError::InvalidRangeSpec("range value invalid: start offset overflow".to_string()))?;
        Ok((offset, len))
    }

    pub fn get_length(&self, res_size: i64) -> Result<i64, HTTPRangeError> {
        if res_size < 0 {
            return Err(HTTPRangeError::InvalidRangeSpec("The requested range is not satisfiable".to_string()));
        }

        if self.is_suffix_length {
            let specified_len = if self.start < 0 {
                self.start
                    .checked_neg()
                    .ok_or_else(|| HTTPRangeError::InvalidRangeSpec("range value invalid: suffix length overflow".to_string()))?
            } else {
                self.start
            };
            let mut range_length = specified_len;

            if specified_len > res_size {
                range_length = res_size;
            }

            return Ok(range_length);
        }

        if self.start >= res_size {
            return Err(HTTPRangeError::InvalidRangeSpec("The requested range is not satisfiable".to_string()));
        }

        if self.end > -1 {
            let mut end = self.end;
            if res_size <= end {
                end = res_size - 1;
            }

            let range_length = end - self.start + 1;
            return Ok(range_length);
        }

        if self.end == -1 {
            let range_length = res_size - self.start;
            return Ok(range_length);
        }

        Err(HTTPRangeError::InvalidRangeSpec(format!(
            "range value invalid: start={}, end={}, expected start <= end and end >= -1",
            self.start, self.end
        )))
    }
}

fn non_empty_condition_value(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_preconditions_ignore_empty_etag_headers() {
        let opts = HTTPPreconditions {
            if_match: Some("  ".to_owned()),
            if_none_match: Some(" * ".to_owned()),
            ..Default::default()
        };

        assert_eq!(opts.if_match_value(), None);
        assert_eq!(opts.if_none_match_value(), Some("*"));
    }

    #[test]
    fn object_lock_retention_defaults_preserve_false_bypass() {
        let opts = ObjectLockRetentionOptions::default();

        assert!(opts.mode.is_none());
        assert!(opts.retain_until.is_none());
        assert!(!opts.bypass_governance);
    }

    #[test]
    fn http_range_spec_offset_length_handles_suffix_and_bounds() {
        let range = HTTPRangeSpec {
            is_suffix_length: false,
            start: 5,
            end: 10,
        };

        assert_eq!(range.get_offset_length(20).expect("range should fit"), (5, 6));

        let suffix = HTTPRangeSpec {
            is_suffix_length: true,
            start: -5,
            end: -1,
        };

        assert_eq!(suffix.get_offset_length(20).expect("suffix range should fit"), (15, 5));
    }

    #[test]
    fn http_range_spec_from_part_sizes_keeps_part_boundaries() {
        let spec = HTTPRangeSpec::from_part_sizes(100, 3, [10, 15, 20]).expect("third part should exist");

        assert_eq!(spec.start, 25);
        assert_eq!(spec.end, 44);
        assert!(HTTPRangeSpec::from_part_sizes(100, 0, [10]).is_none());
        assert!(HTTPRangeSpec::from_part_sizes(100, 2, [10]).is_none());
    }
}
