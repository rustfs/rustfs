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

use crate::erasure_coding::Erasure;
use std::io;

pub(crate) trait DecodeWorkspace: Send + Sync + 'static {
    fn shard_len(&self) -> usize;
}

pub(crate) trait ErasureDecodeEngine: Send + Sync + 'static {
    type Workspace: DecodeWorkspace;

    fn data_shards(&self) -> usize;
    fn parity_shards(&self) -> usize;
    fn block_size(&self) -> usize;

    fn supports_progressive_decode(&self) -> bool;
    fn supports_aligned_shards(&self) -> bool;

    fn prepare_workspace(&self, shard_len: usize) -> io::Result<Self::Workspace>;

    fn reconstruct_into(&self, shards: &mut [Option<Vec<u8>>], workspace: &mut Self::Workspace) -> io::Result<()>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LegacyDecodeWorkspace {
    shard_len: usize,
}

impl LegacyDecodeWorkspace {
    fn new(shard_len: usize) -> Self {
        Self { shard_len }
    }
}

impl DecodeWorkspace for LegacyDecodeWorkspace {
    fn shard_len(&self) -> usize {
        self.shard_len
    }
}

#[derive(Clone)]
pub(crate) struct LegacyEcDecodeEngine {
    erasure: Erasure,
}

impl LegacyEcDecodeEngine {
    pub(crate) fn new(erasure: Erasure) -> Self {
        Self { erasure }
    }
}

impl ErasureDecodeEngine for LegacyEcDecodeEngine {
    type Workspace = LegacyDecodeWorkspace;

    fn data_shards(&self) -> usize {
        self.erasure.data_shards
    }

    fn parity_shards(&self) -> usize {
        self.erasure.parity_shards
    }

    fn block_size(&self) -> usize {
        self.erasure.block_size
    }

    fn supports_progressive_decode(&self) -> bool {
        false
    }

    fn supports_aligned_shards(&self) -> bool {
        false
    }

    fn prepare_workspace(&self, shard_len: usize) -> io::Result<Self::Workspace> {
        Ok(LegacyDecodeWorkspace::new(shard_len))
    }

    fn reconstruct_into(&self, shards: &mut [Option<Vec<u8>>], _workspace: &mut Self::Workspace) -> io::Result<()> {
        self.erasure.decode_data(shards)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_decode_engine_reports_erasure_shape() {
        let engine = LegacyEcDecodeEngine::new(Erasure::new(4, 2, 1 << 20));

        assert_eq!(engine.data_shards(), 4);
        assert_eq!(engine.parity_shards(), 2);
        assert_eq!(engine.block_size(), 1 << 20);
        assert!(!engine.supports_progressive_decode());
        assert!(!engine.supports_aligned_shards());
    }

    #[test]
    fn legacy_decode_engine_reconstructs_missing_data_shard() {
        let erasure = Erasure::new(4, 2, 16);
        let encoded = erasure
            .encode_data(b"codec bridge keeps current reconstruction behavior")
            .expect("encode should succeed");
        let mut shards = encoded.into_iter().map(|shard| Some(shard.to_vec())).collect::<Vec<_>>();
        shards[1] = None;

        let engine = LegacyEcDecodeEngine::new(erasure);
        let mut workspace = engine.prepare_workspace(4).expect("workspace should be prepared");
        engine
            .reconstruct_into(&mut shards, &mut workspace)
            .expect("legacy engine should reconstruct through current erasure path");

        assert_eq!(workspace.shard_len(), 4);
        assert!(shards.iter().take(engine.data_shards()).all(Option::is_some));
    }
}
