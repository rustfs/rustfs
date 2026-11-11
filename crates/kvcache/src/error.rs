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

use thiserror::Error;

pub type KvResult<T> = Result<T, KvError>;

#[derive(Debug, Error)]
pub enum KvError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("key is {len} bytes, more than the {max}-byte limit")]
    KeyTooLong { len: usize, max: usize },
    #[error("value of {len} bytes does not fit in a segment of {segment_bytes} bytes")]
    ValueTooLarge { len: u64, segment_bytes: u64 },
    #[error("checksum mismatch")]
    ChecksumMismatch,
    #[error("corrupt segment data: {0}")]
    Corrupt(String),
    #[error("segment is sealed")]
    Sealed,
    /// The drive is above its emergency watermark; the write is skipped.
    #[error("cache full on drive {drive}")]
    CacheFull { drive: u16 },
}
