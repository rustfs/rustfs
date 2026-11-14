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

//! A use case's answer when it is more than its output: a status other than the operation's
//! declared one, or response headers the output cannot express.
//!
//! Responsible for: [`Answered`], which both edges write unchanged onto their response.
//! NOT responsible for: choosing the headers (the use case writes exactly what the legacy body put
//! on its response, nothing new), or checking them (the gateway refuses a name it owns or that its
//! encoder wrote, with a `500`). A use case that answers neither keeps returning its bare output.
//! Upstream: the use cases in `crate::app`. Downstream: `storage::ecfs::through_gateway_seam_answered`
//! and the gateway handlers' `resp`.

use http::{HeaderMap, StatusCode};

/// An output with the status and response headers it is answered with.
#[derive(Debug)]
pub(crate) struct Answered<O> {
    pub(crate) output: O,
    /// `None` answers the operation's declared status.
    pub(crate) status: Option<StatusCode>,
    pub(crate) headers: HeaderMap,
}

impl<O> Answered<O> {
    /// `output` with the declared status and no extra header.
    pub(crate) fn new(output: O) -> Self {
        Self {
            output,
            status: None,
            headers: HeaderMap::new(),
        }
    }
}
