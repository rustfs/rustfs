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

//! Framework-neutral source of HTTP trailer fields, such as the
//! `x-amz-checksum-*` trailers of an aws-chunked upload.
//!
//! Timing contract:
//! 1. A source is attached to a `HashReader` while request headers are
//!    processed, before any trailer can have arrived, so it starts `Pending`.
//! 2. The body decoder that owns the source publishes the trailer section
//!    after the final chunk has been decoded and before the decoded body
//!    reports EOF.
//! 3. `HashReader` reads the trailer only once its inner reader reports EOF,
//!    and `HashReader::content_crc` is only meaningful after that point.
//!
//! Once a lookup has returned anything other than [`TrailerValue::Pending`],
//! the source must never report `Pending` again.
//!
//! `HashReader` currently treats `Pending` at EOF the same as `Missing`: the
//! declared trailing checksum fails verification at EOF and is omitted from
//! `content_crc`.

use std::sync::Arc;

/// Outcome of looking up one trailer field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrailerValue {
    /// The trailer section has not been received yet.
    Pending,
    /// The trailer section was received without a usable value for the field
    /// (absent, or not visible ASCII).
    Missing,
    /// The field value exactly as received.
    Present(String),
}

impl TrailerValue {
    /// Returns the received value, if any.
    pub fn into_present(self) -> Option<String> {
        match self {
            Self::Present(value) => Some(value),
            Self::Pending | Self::Missing => None,
        }
    }
}

/// Read-only view of a request's trailer section. See the module docs for
/// the timing contract implementations must follow.
pub trait TrailerSource: Send + Sync {
    /// Looks up the lower-case field `name` in the trailer section.
    fn lookup(&self, name: &str) -> TrailerValue;
}

/// Shared handle to a [`TrailerSource`]. Clones observe the same trailer.
pub type SharedTrailerSource = Arc<dyn TrailerSource>;
