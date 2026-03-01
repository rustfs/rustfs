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

//! Shared types used across the log-cleanup sub-modules.

use std::path::PathBuf;
use std::time::SystemTime;

/// Strategy for matching log files against a pattern.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileMatchMode {
    /// The filename must start with the pattern (e.g. "app.log." matches "app.log.2024-01-01").
    Prefix,
    /// The filename must end with the pattern (e.g. ".log" matches "2024-01-01.log").
    Suffix,
}

/// Metadata for a single log file discovered by the scanner.
///
/// Carries enough information to make cleanup decisions (sort by age, compare
/// size against limits, etc.) without re-reading filesystem metadata on every
/// operation.
#[derive(Debug, Clone)]
pub(super) struct FileInfo {
    /// Absolute path to the file.
    pub path: PathBuf,
    /// File size in bytes at the time of discovery.
    pub size: u64,
    /// Last-modification timestamp from the filesystem.
    pub modified: SystemTime,
}
