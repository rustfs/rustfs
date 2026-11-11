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

//! Store core of the `kv-cache` bucket engine.
//!
//! Entries are immutable values keyed by content hashes, stored whole in
//! append-only segment files on local drives. A segment is one preallocated
//! file written by a single writer and sealed with a footer that lists its
//! entries; the in-memory index is rebuilt from footers (and a scan of open
//! segments) at startup, so there is no metadata store and no write-ahead
//! log. Values are checksummed with CRC32C and a mismatch is a miss.
//!
//! This crate is synchronous and free of the object layer and the async
//! runtime; the engine in the server drives it. See
//! `docs/architecture/kv-cache-buckets.md`.

mod aligned;
mod capacity;
mod error;
mod format;
mod segment;
mod store;

pub use aligned::{AlignedBuf, BLOCK_SIZE};
pub use capacity::{CapacityPolicy, DiskUsage, FsStats, StatvfsFs, Watermarks};
pub use error::{KvError, KvResult};
pub use format::{EntryMeta, MAX_KEY_LEN, entry_footprint};
pub use segment::{ScannedSegment, SegmentFile, SegmentId, SegmentWriter, WriterOptions, scan_segment, segment_file_name};
pub use store::{EntryInfo, EvictionReport, KvStore, ListItem, PutOutcome, RecoveryReport, SegmentStat, StoreOptions, Value};
