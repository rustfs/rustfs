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


//! A request fact a permission probe is told the caller already supplied.
//!
//! Not responsible for evaluating the probe. Upstream: the admin permission probe and
//! `RequestEnvelope`; downstream: the probe's known-facts mask.

/// A request fact the caller supplied, as a bit in the probe's known-facts mask.
#[derive(Clone, Copy)]
pub(crate) enum PermissionProbeRequestFact {
    CopySource = 1,
    IfMatch = 2,
    IfNoneMatch = 4,
}
