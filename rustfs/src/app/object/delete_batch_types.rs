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

//! Owned input and result fields for the batch deletion engine.
//!
//! Keys and versions stay raw so legacy refusals can echo entries that did not pass validation.

#[derive(Debug, Default)]
pub(crate) struct DeleteBatchInput {
    pub(crate) bucket: String,
    pub(crate) objects: Vec<DeleteBatchEntry>,
    pub(crate) quiet: Option<bool>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct DeleteBatchEntry {
    pub(crate) key: String,
    pub(crate) key_too_long: bool,
    pub(crate) version_id: Option<String>,
    pub(crate) e_tag: Option<DeleteBatchETag>,
    /// Seconds since the Unix epoch and nanoseconds within that second.
    pub(crate) _last_modified_time: Option<(i64, u32)>,
    pub(crate) _size: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DeleteBatchETag {
    pub(crate) value: String,
    pub(crate) weak: bool,
}

#[derive(Debug, Default)]
pub(crate) struct DeleteBatchOutput {
    pub(crate) deleted: Option<Vec<DeleteBatchSuccess>>,
    pub(crate) errors: Option<Vec<DeleteBatchError>>,
}

#[derive(Debug, Default)]
pub(crate) struct DeleteBatchSuccess {
    pub(crate) delete_marker: Option<bool>,
    pub(crate) delete_marker_version_id: Option<String>,
    pub(crate) key: Option<String>,
    pub(crate) version_id: Option<String>,
}

#[derive(Debug, Default, Clone)]
pub(crate) struct DeleteBatchError {
    pub(crate) code: Option<String>,
    pub(crate) key: Option<String>,
    pub(crate) message: Option<String>,
    pub(crate) version_id: Option<String>,
}
