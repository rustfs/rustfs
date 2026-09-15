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

//! The single bridge from the HTTP framework's trailer handle to the
//! framework-neutral `rustfs_rio::TrailerSource`.

use crate::app::storage_api::s3::TrailingHeaders;
use rustfs_rio::{SharedTrailerSource, TrailerSource, TrailerValue};
use std::sync::Arc;

// RUSTFS_COMPAT_TODO(s3gate-trailer-adapter): s3s still decodes aws-chunked bodies and publishes their trailers through its own handle, so the object write path adapts that handle to rio's TrailerSource here. Remove after the gateway stack replaces s3s as the request body decoder.
#[derive(Clone, Debug)]
struct S3sTrailerSource(TrailingHeaders);

impl TrailerSource for S3sTrailerSource {
    fn lookup(&self, name: &str) -> TrailerValue {
        // s3s fills the handle after the final chunk has been decoded and
        // before the decoded body ends, so an unfilled handle is `Pending`.
        match self
            .0
            .read(|headers| headers.get(name).and_then(|value| value.to_str().ok()).map(str::to_owned))
        {
            None => TrailerValue::Pending,
            Some(None) => TrailerValue::Missing,
            Some(Some(value)) => TrailerValue::Present(value),
        }
    }
}

/// Adapts the request's trailer handle, if the request declared one.
pub(crate) fn trailer_source(trailing_headers: Option<TrailingHeaders>) -> Option<SharedTrailerSource> {
    trailing_headers.map(|headers| Arc::new(S3sTrailerSource(headers)) as SharedTrailerSource)
}
