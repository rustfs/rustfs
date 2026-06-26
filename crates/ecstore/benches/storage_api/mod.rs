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

#![allow(unused_imports)]

pub(crate) use rustfs_ecstore::api::erasure::{
    BitrotReader, BitrotWriter, BitrotWriterWrapper, CustomWriter, Erasure, calc_shard_size,
};

pub(crate) mod comparison {
    pub(crate) use super::Erasure;
}

pub(crate) mod erasure {
    pub(crate) use super::{BitrotReader, BitrotWriter, Erasure, calc_shard_size};
}

pub(crate) mod single_block_non_inline {
    pub(crate) use super::{BitrotWriterWrapper, CustomWriter, Erasure};
}
