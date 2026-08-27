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

mod error;
mod fileinfo;
mod filemeta;
mod filemeta_inline;
// pub mod headers;
mod metacache;
mod replication;

pub mod test_data;

/// High-performance HashMap type alias using ahash instead of SipHash.
pub type AHashMap<K, V> = ahash::AHashMap<K, V>;

/// High-performance HashSet type alias using ahash.
pub type AHashSet<K> = ahash::AHashSet<K>;

pub use error::*;
pub use fileinfo::*;
pub use filemeta::*;
pub use filemeta_inline::*;
pub use metacache::*;
pub use replication::*;
