// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! ECStore facade boundary for the protocols integration tests.
//!
//! The Swift tests need to drive bucket-metadata reloads the way the peer
//! `LoadBucketMetadata` RPC does. Everything they touch from `rustfs_ecstore`
//! is aliased here so the tests themselves hold no raw facade subpaths, the
//! same boundary `crates/protocols/src/swift/storage_api.rs` provides for the
//! Swift implementation.

pub use rustfs_ecstore::api::bucket::metadata::load_bucket_metadata;
pub use rustfs_ecstore::api::bucket::metadata_sys::{get as get_bucket_metadata, set_bucket_metadata};
pub use rustfs_ecstore::api::runtime::object_store_handle;
