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

use crate::bucket::lifecycle::lifecycle;

#[derive(Debug, Clone, Default)]
pub enum LcEventSrc {
    #[default]
    None,
    Heal,
    Scanner,
    Decom,
    Rebal,
    S3HeadObject,
    S3GetObject,
    S3ListObjects,
    S3PutObject,
    S3CopyObject,
    S3CompleteMultipartUpload,
}

#[derive(Clone, Debug, Default)]
pub struct LcAuditEvent {
    pub event: lifecycle::Event,
    pub source: LcEventSrc,
}

impl LcAuditEvent {
    pub fn new(event: lifecycle::Event, source: LcEventSrc) -> Self {
        Self { event, source }
    }
}
