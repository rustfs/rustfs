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

//! Cases the operation batches of rustfs/backlog#2765 add to the script, one file per batch.
//!
//! They run after the base script in `super`, in the fixed order below, so the base script's
//! ordered `(op, case)` list never moves. A batch adds its cases to its own file only, on buckets
//! and keys of its own; it does not edit this file or the base script.

use super::Recorder;
use aws_sdk_s3::Client;

mod bucket_access;
mod bucket_configs;
mod bucket_core;
mod bucket_replication_lifecycle;
mod listing_multipart;
mod object_commit;
mod object_meta;
mod object_misc;
mod object_read;
mod object_write;

pub(super) async fn cases(s: &mut Recorder, c: &Client) {
    Box::pin(bucket_core::cases(s, c)).await;
    Box::pin(bucket_configs::cases(s, c)).await;
    Box::pin(bucket_access::cases(s, c)).await;
    Box::pin(bucket_replication_lifecycle::cases(s, c)).await;
    Box::pin(object_meta::cases(s, c)).await;
    Box::pin(listing_multipart::cases(s, c)).await;
    Box::pin(object_read::cases(s, c)).await;
    Box::pin(object_write::cases(s, c)).await;
    Box::pin(object_commit::cases(s, c)).await;
    Box::pin(object_misc::cases(s, c)).await;
}
