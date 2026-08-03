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

const UNKNOWN_FIELD_NAME_MAX_BYTES: usize = 128;
pub(crate) const UNKNOWN_FIELDS_METRIC: &str = "rustfs_kms_persisted_unknown_fields_total";

pub(crate) struct BoundedUnknownFieldName {
    value: String,
    truncated: bool,
}

impl BoundedUnknownFieldName {
    pub(crate) fn new(value: &str) -> Self {
        if value.len() <= UNKNOWN_FIELD_NAME_MAX_BYTES {
            return Self {
                value: value.to_owned(),
                truncated: false,
            };
        }

        let mut end = UNKNOWN_FIELD_NAME_MAX_BYTES;
        while !value.is_char_boundary(end) {
            end -= 1;
        }
        Self {
            value: value[..end].to_owned(),
            truncated: true,
        }
    }
}

#[derive(Default)]
pub(crate) struct UnknownFieldSummary {
    count: u64,
    first: Option<BoundedUnknownFieldName>,
}

impl UnknownFieldSummary {
    pub(crate) fn observe(&mut self, field: BoundedUnknownFieldName) {
        self.count = self.count.saturating_add(1);
        if self.first.is_none() {
            self.first = Some(field);
        }
    }

    pub(crate) fn record(&self, record_kind: &'static str) -> Option<(&str, bool, u64)> {
        let field = self.first.as_ref()?;
        metrics::counter!(UNKNOWN_FIELDS_METRIC, "record_kind" => record_kind).increment(self.count);
        Some((&field.value, field.truncated, self.count))
    }
}
