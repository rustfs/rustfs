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

use crate::target::TargetType;
use serde::{Deserialize, Serialize};

/// Logical target domains supported by RustFS target plugins.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TargetDomain {
    Notify,
    Audit,
}

impl TargetDomain {
    #[inline]
    pub fn runtime_target_type(self) -> TargetType {
        match self {
            TargetDomain::Notify => TargetType::NotifyEvent,
            TargetDomain::Audit => TargetType::AuditLog,
        }
    }
}

impl From<TargetType> for TargetDomain {
    fn from(value: TargetType) -> Self {
        match value {
            TargetType::NotifyEvent => TargetDomain::Notify,
            TargetType::AuditLog => TargetDomain::Audit,
        }
    }
}
