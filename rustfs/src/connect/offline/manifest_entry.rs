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

//! A bounded, already-redacted offline diagnostic manifest entry.

use std::time::Instant;

use serde::Serialize;
use serde_json::{Map, Value};
use tokio_util::sync::CancellationToken;

use super::collectors::{CollectorError, DataClassification, OfflineCollector};
use super::redaction::{REDACTION_VERSION, RULESET_HASH, RedactionSource, redact};

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ManifestEntry {
    pub field_id: &'static str,
    pub classification: DataClassification,
    pub canonical_json: String,
    pub redaction_version: &'static str,
    pub ruleset_hash: &'static str,
    pub redacted_count: usize,
}

impl ManifestEntry {
    pub(super) fn from_value(
        collector: OfflineCollector,
        value: Value,
        cancel: &CancellationToken,
    ) -> Result<Self, CollectorError> {
        let started = Instant::now();
        if cancel.is_cancelled() {
            return Err(CollectorError::Cancelled);
        }
        let mut document = Map::new();
        document.insert(collector.field_name().to_owned(), value);
        let input_size = serde_json::to_vec(&document)
            .map_err(|_| CollectorError::NotRepresentable)?
            .len();
        if input_size > collector.max_entry_bytes() {
            return Err(CollectorError::EntryTooLarge {
                field_id: collector.field_id(),
                limit: collector.max_entry_bytes(),
            });
        }
        let result = redact(RedactionSource::OfflineDiagnostic, &document)?;
        if started.elapsed() > collector.timeout() {
            return Err(CollectorError::TimedOut);
        }
        if cancel.is_cancelled() {
            return Err(CollectorError::Cancelled);
        }
        Ok(Self {
            field_id: collector.field_id(),
            classification: collector.classification(),
            canonical_json: result.canonical_json,
            redaction_version: REDACTION_VERSION,
            ruleset_hash: RULESET_HASH,
            redacted_count: result.redacted_count,
        })
    }
}
