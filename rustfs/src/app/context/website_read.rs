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


//! Marker a website-hosting read carries in the request extensions.
//!
//! Not responsible for resolving the website route. Upstream: the website route that
//! inserts it; downstream: `RequestEnvelope::website_read`.

/// Marks a request as an internal website-hosting read.
#[derive(Clone)]
pub(crate) struct WebsiteRead;
