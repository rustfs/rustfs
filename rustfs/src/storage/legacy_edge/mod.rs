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

//! The bodies of `impl S3 for FS`, one file per batch of the s3s-to-gateway migration
//! (rustfs/backlog#2765), so that parallel batches edit disjoint files. `ecfs.rs` keeps the
//! trait impl, which delegates every method here, and the helpers several batches share.
//! `scripts/codemods/split_ecfs_legacy_edge.py` produced this split from the unsplit file.

mod bucket_access;
mod bucket_configs;
mod bucket_core;
mod bucket_replication_lifecycle;
mod listing_multipart;

#[cfg(test)]
pub(crate) use listing_multipart::list_objects_v2_through_legacy_edge;
mod object_meta;
mod object_misc;

#[cfg(test)]
pub(crate) use object_misc::complete_multipart_upload_input;
mod object_read;
mod object_write;

#[cfg(test)]
mod tests;
