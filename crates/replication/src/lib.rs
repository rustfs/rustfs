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

pub mod mrf;
pub mod resync;

pub use mrf::{MrfOpKind, MrfReplicateEntry, decode_mrf_file, encode_mrf_file};
pub use resync::{
    BucketReplicationResyncStatus, Error, Result, ResyncOpts, ResyncStatusType, TargetReplicationResyncStatus,
    decode_resync_file, encode_resync_file,
};
