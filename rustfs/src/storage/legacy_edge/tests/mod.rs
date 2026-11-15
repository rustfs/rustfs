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

//! Tests of the legacy-edge handlers, one module per migration batch, and the fixtures they
//! share.

use crate::storage::ecfs::*;
use http::{Extensions, HeaderMap, Method, Uri};

fn build_request<T>(input: T, method: Method) -> S3Request<T> {
    S3Request {
        input,
        method,
        uri: Uri::from_static("/"),
        headers: HeaderMap::new(),
        extensions: Extensions::new(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

/// Premise guard for the `*_returns_internal_error_when_store_uninitialized`
/// tests (backlog#1830): they assert the error path taken while the global
/// object layer is absent. Under nextest — the authoritative runner — every
/// test owns its process, so the premise always holds and the assertion
/// always runs. Under the documented shared-process `cargo test` fallback a
/// sibling test may have initialized the store first; the premise is then
/// unattainable, so the test skips instead of asserting against a scenario
/// it does not describe.
fn store_uninitialized_premise_holds() -> bool {
    if crate::runtime_sources::current_object_store_handle().is_some() {
        eprintln!("skipping store-uninitialized assertion: a sibling test already initialized the global object layer");
        return false;
    }
    true
}

mod bucket_access;
mod bucket_core;
mod bucket_replication_lifecycle;
mod listing_multipart;
mod object_commit;
mod object_meta;
mod object_read;
