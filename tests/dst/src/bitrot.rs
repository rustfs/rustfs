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

//! Single-block HighwayHash256S framing.
//!
//! This is the on-disk `[hash][data]` prefix that `BitrotWriter` writes for
//! `HashAlgorithm::HighwayHash256S`. The async reader is not used here; the
//! hash function is the production one.

use rustfs_utils::HashAlgorithm;

const HASH_LEN: usize = 32;

pub fn frame_shard(shard: &[u8]) -> Vec<u8> {
    let digest = HashAlgorithm::HighwayHash256S.hash_encode(shard);
    let mut framed = Vec::with_capacity(HASH_LEN + shard.len());
    framed.extend_from_slice(digest.as_ref());
    framed.extend_from_slice(shard);
    framed
}

pub fn unframe_shard(framed: &[u8]) -> Option<Vec<u8>> {
    if framed.len() < HASH_LEN {
        return None;
    }
    let (digest, shard) = framed.split_at(HASH_LEN);
    let actual = HashAlgorithm::HighwayHash256S.hash_encode(shard);
    if actual.as_ref() != digest {
        return None;
    }
    Some(shard.to_vec())
}

#[cfg(test)]
mod tests {
    use super::{frame_shard, unframe_shard};

    #[test]
    fn roundtrip_and_single_byte_corruption_fail_closed() {
        let framed = frame_shard(b"shard-bytes");
        assert_eq!(unframe_shard(&framed).as_deref(), Some(b"shard-bytes".as_slice()));
        let mut corrupt = framed;
        let last = corrupt.len() - 1;
        corrupt[last] ^= 0xff;
        assert!(unframe_shard(&corrupt).is_none());
    }
}
