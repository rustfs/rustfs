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

use crate::moka_backend::MokaBackend;
use crate::noop::NoopBackend;

/// Backend dispatch for the object data cache.
#[derive(Debug)]
pub enum ObjectDataCacheBackendKind {
    /// No-op backend used while the feature is disabled.
    Noop(NoopBackend),
    /// Moka backend used when cache lookups are enabled.
    Moka(MokaBackend),
}

impl Default for ObjectDataCacheBackendKind {
    fn default() -> Self {
        Self::Noop(NoopBackend)
    }
}

impl ObjectDataCacheBackendKind {
    pub(crate) const fn as_metric_label(&self) -> &'static str {
        match self {
            Self::Noop(_) => "noop",
            Self::Moka(_) => "moka",
        }
    }
}
