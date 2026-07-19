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

//! Ingest accounting. Every capped or skipped input is disclosed here —
//! silent truncation would read as "covered everything" when it didn't.

use crate::model::ParseStats;
use serde::Serialize;

/// Why an input (file or archive entry) was not parsed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SkipReason {
    /// Binary sniff tripped (> 1% NUL bytes in the head).
    Binary,
    /// Archive nesting exceeded `IngestOptions::max_depth`.
    TooDeep,
    /// Archive had more entries than `max_entries_per_archive`.
    EntryCap,
    /// Global `max_total_bytes` budget exhausted.
    ByteCap,
    /// A line exceeded `max_line_bytes`; its tail was discarded (recorded once
    /// per file — the truncated prefix is still parsed).
    LineTooLong,
    /// A nested archive needed in-memory buffering beyond `max_in_memory_archive`.
    ArchiveTooLargeForMemory,
    /// IO/format error opening or reading this input.
    Unreadable,
}

#[derive(Debug, Default, Clone, Serialize)]
pub struct IngestReport {
    pub files_parsed: u64,
    /// Decompressed bytes fed to parsers.
    pub bytes_fed: u64,
    /// (provenance, reason) for every skipped input.
    pub skipped: Vec<(String, SkipReason)>,
    /// Parse accounting merged across all files.
    pub stats: ParseStats,
}

impl IngestReport {
    /// Merges accounting from another ingest run (multiple CLI paths).
    pub fn merge(&mut self, other: IngestReport) {
        self.files_parsed += other.files_parsed;
        self.bytes_fed += other.bytes_fed;
        self.skipped.extend(other.skipped);
        self.stats.merge(&other.stats);
    }
}
