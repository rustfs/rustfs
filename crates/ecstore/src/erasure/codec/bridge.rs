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

use crate::erasure::codec::workspace::RustfsCodecDecodeWorkspace;
use crate::erasure::coding::Erasure;
use reed_solomon_erasure::galois_8::ReedSolomon;
use std::io;
use std::sync::{Arc, OnceLock};

pub(crate) const GET_CODEC_STREAMING_ENGINE_LEGACY: &str = "legacy";
pub(crate) const GET_CODEC_STREAMING_ENGINE_RUSTFS: &str = "rustfs";
pub(crate) const GET_RECONSTRUCT_OUTCOME_LEGACY_CALLED: &str = "legacy_called";
pub(crate) const GET_RECONSTRUCT_OUTCOME_RUSTFS_CALLED: &str = "rustfs_called";
pub(crate) const GET_RECONSTRUCT_OUTCOME_SKIP_DATA_COMPLETE: &str = "skip_data_complete";
pub(crate) const GET_RECONSTRUCT_OUTCOME_SKIP_EMPTY_PAYLOAD: &str = "skip_empty_payload";

pub(crate) trait DecodeWorkspace: Send + Sync + 'static {
    fn shard_len(&self) -> usize;
}

pub(crate) trait ErasureDecodeEngine: Send + Sync + 'static {
    type Workspace: DecodeWorkspace;

    fn data_shards(&self) -> usize;
    fn parity_shards(&self) -> usize;
    fn block_size(&self) -> usize;
    fn engine_name(&self) -> &'static str;

    fn supports_progressive_decode(&self) -> bool;
    fn supports_aligned_shards(&self) -> bool;

    fn prepare_workspace(&self, shard_len: usize) -> io::Result<Self::Workspace>;

    fn reconstruct_into(&self, shards: &mut [Option<Vec<u8>>], workspace: &mut Self::Workspace) -> io::Result<&'static str>;
}

fn data_shards_complete(shards: &[Option<Vec<u8>>], data_shards: usize) -> bool {
    shards.len() >= data_shards && shards.iter().take(data_shards).all(Option::is_some)
}

fn recover_empty_payload_data_shards(
    shards: &mut [Option<Vec<u8>>],
    data_shards: usize,
    parity_shards: usize,
) -> io::Result<bool> {
    let expected_shards = data_shards + parity_shards;
    if shards.len() != expected_shards {
        return Err(io::Error::other(format!(
            "invalid shard count: got {}, expected {}",
            shards.len(),
            expected_shards
        )));
    }

    let mut present_shards = 0usize;
    for shard in shards.iter().filter_map(Option::as_ref) {
        present_shards += 1;
        if !shard.is_empty() {
            return Ok(false);
        }
    }
    if present_shards == 0 || present_shards < data_shards {
        return Ok(false);
    }

    for shard in shards.iter_mut().take(data_shards) {
        if shard.is_none() {
            *shard = Some(Vec::new());
        }
    }

    Ok(true)
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

impl DecodeWorkspace for RustfsCodecDecodeWorkspace {
    fn shard_len(&self) -> usize {
        RustfsCodecDecodeWorkspace::shard_len(self)
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

    fn engine_name(&self) -> &'static str {
        GET_CODEC_STREAMING_ENGINE_LEGACY
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

    fn reconstruct_into(&self, shards: &mut [Option<Vec<u8>>], _workspace: &mut Self::Workspace) -> io::Result<&'static str> {
        if data_shards_complete(shards, self.erasure.data_shards) {
            return Ok(GET_RECONSTRUCT_OUTCOME_SKIP_DATA_COMPLETE);
        }

        self.erasure.decode_data_with_reconstruction_verification(shards)?;
        Ok(GET_RECONSTRUCT_OUTCOME_LEGACY_CALLED)
    }
}

#[derive(Clone)]
pub(crate) struct RustfsCodecDecodeEngine {
    data_shards: usize,
    parity_shards: usize,
    block_size: usize,
    codec: OnceLock<Arc<ReedSolomon>>,
}

impl RustfsCodecDecodeEngine {
    pub(crate) fn new(erasure: &Erasure) -> io::Result<Self> {
        Ok(Self {
            data_shards: erasure.data_shards,
            parity_shards: erasure.parity_shards,
            block_size: erasure.block_size,
            codec: OnceLock::new(),
        })
    }

    fn codec(&self) -> io::Result<Option<Arc<ReedSolomon>>> {
        if self.parity_shards == 0 {
            return Ok(None);
        }

        if let Some(codec) = self.codec.get() {
            return Ok(Some(Arc::clone(codec)));
        }

        let codec = Arc::new(
            ReedSolomon::new(self.data_shards, self.parity_shards)
                .map_err(|err| io::Error::other(format!("Failed to create RustFS codec decode engine: {err:?}")))?,
        );
        if self.codec.set(Arc::clone(&codec)).is_err() {
            return Ok(self.codec.get().map(Arc::clone).or(Some(codec)));
        }
        Ok(Some(codec))
    }

    fn needs_source_parity_verification(&self, shards: &[Option<Vec<u8>>]) -> bool {
        let missing_data_source = shards.iter().take(self.data_shards).any(|shard| shard.is_none());
        let available_shards = shards.iter().filter(|shard| shard.is_some()).count();
        missing_data_source && available_shards > self.data_shards
    }

    fn verify_source_parity(&self, codec: &ReedSolomon, shards: &[Option<Vec<u8>>], needs_verification: bool) -> io::Result<()> {
        if !needs_verification {
            return Ok(());
        }

        // All slots must be present here: the verification path reconstructs
        // missing parity alongside data (see `reconstruct_into`), so a `None`
        // slot is an internal invariant violation, not a degraded-read state.
        let mut shard_refs = Vec::with_capacity(self.data_shards + self.parity_shards);
        for (index, shard) in shards.iter().enumerate() {
            let shard = shard.as_ref().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("missing shard {index} after RustFS codec reconstruction"),
                )
            })?;
            shard_refs.push(shard.as_slice());
        }

        let valid = codec
            .verify(&shard_refs)
            .map_err(|err| io::Error::other(format!("RustFS codec verify failed: {err:?}")))?;
        if !valid {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "inconsistent read source shards"));
        }

        Ok(())
    }

    #[cfg(test)]
    fn codec_is_initialized(&self) -> bool {
        self.codec.get().is_some()
    }
}

impl ErasureDecodeEngine for RustfsCodecDecodeEngine {
    type Workspace = RustfsCodecDecodeWorkspace;

    fn data_shards(&self) -> usize {
        self.data_shards
    }

    fn parity_shards(&self) -> usize {
        self.parity_shards
    }

    fn block_size(&self) -> usize {
        self.block_size
    }

    fn engine_name(&self) -> &'static str {
        GET_CODEC_STREAMING_ENGINE_RUSTFS
    }

    fn supports_progressive_decode(&self) -> bool {
        false
    }

    fn supports_aligned_shards(&self) -> bool {
        false
    }

    fn prepare_workspace(&self, shard_len: usize) -> io::Result<Self::Workspace> {
        Ok(RustfsCodecDecodeWorkspace::new(shard_len))
    }

    fn reconstruct_into(&self, shards: &mut [Option<Vec<u8>>], _workspace: &mut Self::Workspace) -> io::Result<&'static str> {
        if data_shards_complete(shards, self.data_shards) {
            return Ok(GET_RECONSTRUCT_OUTCOME_SKIP_DATA_COMPLETE);
        }
        if recover_empty_payload_data_shards(shards, self.data_shards, self.parity_shards)? {
            return Ok(GET_RECONSTRUCT_OUTCOME_SKIP_EMPTY_PAYLOAD);
        }

        if let Some(codec) = self.codec()? {
            let needs_source_parity_verification = self.needs_source_parity_verification(shards);
            if needs_source_parity_verification {
                // Rebuild missing parity together with missing data so the
                // shard set is complete for `verify`. Rebuilt parity is
                // consistent with the reconstructed data by construction, so
                // the verification below is equivalent to checking only the
                // originally-present source parity — the same semantics as the
                // legacy `decode_data_with_reconstruction_verification` path.
                // Rebuilding only data here would leave missing parity slots
                // as `None` and misreport a recoverable degraded read (for
                // example one missing data shard plus one missing parity
                // shard) as an inconsistent-source failure.
                codec
                    .reconstruct_opt(shards)
                    .map_err(|err| io::Error::other(format!("RustFS codec reconstruct failed: {err:?}")))?;
            } else {
                codec
                    .reconstruct_data_opt(shards)
                    .map_err(|err| io::Error::other(format!("RustFS codec reconstruct failed: {err:?}")))?;
            }
            self.verify_source_parity(&codec, shards, needs_source_parity_verification)?;
        }

        Ok(GET_RECONSTRUCT_OUTCOME_RUSTFS_CALLED)
    }
}

#[derive(Clone)]
pub(crate) enum CodecStreamingDecodeEngine {
    Legacy(Box<LegacyEcDecodeEngine>),
    Rustfs(Box<RustfsCodecDecodeEngine>),
}

impl CodecStreamingDecodeEngine {
    pub(crate) fn legacy(erasure: Erasure) -> Self {
        Self::Legacy(Box::new(LegacyEcDecodeEngine::new(erasure)))
    }

    pub(crate) fn rustfs(erasure: &Erasure) -> io::Result<Self> {
        RustfsCodecDecodeEngine::new(erasure).map(Box::new).map(Self::Rustfs)
    }
}

pub(crate) enum CodecStreamingDecodeWorkspace {
    Legacy(LegacyDecodeWorkspace),
    Rustfs(RustfsCodecDecodeWorkspace),
}

impl DecodeWorkspace for CodecStreamingDecodeWorkspace {
    fn shard_len(&self) -> usize {
        match self {
            Self::Legacy(workspace) => workspace.shard_len(),
            Self::Rustfs(workspace) => workspace.shard_len(),
        }
    }
}

impl ErasureDecodeEngine for CodecStreamingDecodeEngine {
    type Workspace = CodecStreamingDecodeWorkspace;

    fn data_shards(&self) -> usize {
        match self {
            Self::Legacy(engine) => engine.data_shards(),
            Self::Rustfs(engine) => engine.data_shards(),
        }
    }

    fn parity_shards(&self) -> usize {
        match self {
            Self::Legacy(engine) => engine.parity_shards(),
            Self::Rustfs(engine) => engine.parity_shards(),
        }
    }

    fn block_size(&self) -> usize {
        match self {
            Self::Legacy(engine) => engine.block_size(),
            Self::Rustfs(engine) => engine.block_size(),
        }
    }

    fn engine_name(&self) -> &'static str {
        match self {
            Self::Legacy(engine) => engine.engine_name(),
            Self::Rustfs(engine) => engine.engine_name(),
        }
    }

    fn supports_progressive_decode(&self) -> bool {
        match self {
            Self::Legacy(engine) => engine.supports_progressive_decode(),
            Self::Rustfs(engine) => engine.supports_progressive_decode(),
        }
    }

    fn supports_aligned_shards(&self) -> bool {
        match self {
            Self::Legacy(engine) => engine.supports_aligned_shards(),
            Self::Rustfs(engine) => engine.supports_aligned_shards(),
        }
    }

    fn prepare_workspace(&self, shard_len: usize) -> io::Result<Self::Workspace> {
        match self {
            Self::Legacy(engine) => engine.prepare_workspace(shard_len).map(CodecStreamingDecodeWorkspace::Legacy),
            Self::Rustfs(engine) => engine.prepare_workspace(shard_len).map(CodecStreamingDecodeWorkspace::Rustfs),
        }
    }

    fn reconstruct_into(&self, shards: &mut [Option<Vec<u8>>], workspace: &mut Self::Workspace) -> io::Result<&'static str> {
        match (self, workspace) {
            (Self::Legacy(engine), CodecStreamingDecodeWorkspace::Legacy(workspace)) => {
                engine.reconstruct_into(shards, workspace)
            }
            (Self::Rustfs(engine), CodecStreamingDecodeWorkspace::Rustfs(workspace)) => {
                engine.reconstruct_into(shards, workspace)
            }
            _ => Err(io::Error::other("codec streaming decode engine/workspace mismatch")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encoded_shards(erasure: &Erasure, data: &[u8]) -> Vec<Option<Vec<u8>>> {
        erasure
            .encode_data(data)
            .expect("test stripe should encode")
            .into_iter()
            .map(|shard| Some(shard.to_vec()))
            .collect()
    }

    fn reconstruct_with<E>(engine: &E, shards: &mut [Option<Vec<u8>>]) -> io::Result<()>
    where
        E: ErasureDecodeEngine,
    {
        let mut workspace = engine.prepare_workspace(4)?;
        engine.reconstruct_into(shards, &mut workspace).map(|_| ())
    }

    #[test]
    fn data_shards_complete_ignores_parity_slots() {
        let shards = vec![Some(vec![1]), Some(vec![2]), None];

        assert!(data_shards_complete(&shards, 2));
    }

    #[test]
    fn data_shards_complete_requires_all_data_slots() {
        let missing_data = vec![Some(vec![1]), None, Some(vec![3])];
        let short = vec![Some(vec![1])];

        assert!(!data_shards_complete(&missing_data, 2));
        assert!(!data_shards_complete(&short, 2));
    }

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

    #[test]
    fn rustfs_codec_decode_engine_reports_erasure_shape() {
        let erasure = Erasure::new(4, 2, 1 << 20);
        let engine = RustfsCodecDecodeEngine::new(&erasure).expect("engine should be created");

        assert_eq!(engine.data_shards(), 4);
        assert_eq!(engine.parity_shards(), 2);
        assert_eq!(engine.block_size(), 1 << 20);
        assert!(!engine.supports_progressive_decode());
        assert!(!engine.supports_aligned_shards());
    }

    #[test]
    fn rustfs_codec_decode_engine_keeps_complete_data_shards() {
        let erasure = Erasure::new(4, 2, 16);
        let mut shards = encoded_shards(&erasure, b"all data shards are present");
        let before = shards.clone();

        let engine = RustfsCodecDecodeEngine::new(&erasure).expect("engine should be created");
        assert!(!engine.codec_is_initialized());
        reconstruct_with(&engine, &mut shards).expect("complete data shards should not reconstruct");

        assert!(!engine.codec_is_initialized());
        assert_eq!(shards, before);
    }

    #[test]
    fn rustfs_codec_decode_engine_reconstructs_missing_data_like_legacy() {
        let erasure = Erasure::new(4, 2, 16);
        let mut legacy_shards = encoded_shards(&erasure, b"missing data shard must match legacy output");
        let mut rustfs_shards = legacy_shards.clone();
        legacy_shards[1] = None;
        rustfs_shards[1] = None;

        let legacy = LegacyEcDecodeEngine::new(erasure.clone());
        let rustfs = RustfsCodecDecodeEngine::new(&erasure).expect("engine should be created");

        reconstruct_with(&legacy, &mut legacy_shards).expect("legacy should reconstruct");
        reconstruct_with(&rustfs, &mut rustfs_shards).expect("rustfs codec should reconstruct");

        assert_eq!(rustfs_shards, legacy_shards);
    }

    #[test]
    fn rustfs_codec_decode_engine_leaves_missing_parity_unreconstructed() {
        let erasure = Erasure::new(4, 2, 16);
        let mut shards = encoded_shards(&erasure, b"parity-only missing should not touch output data");
        let before_data = shards.iter().take(erasure.data_shards).cloned().collect::<Vec<_>>();
        shards[erasure.data_shards] = None;

        let engine = RustfsCodecDecodeEngine::new(&erasure).expect("engine should be created");
        reconstruct_with(&engine, &mut shards).expect("missing parity should not fail");

        assert_eq!(shards.iter().take(erasure.data_shards).cloned().collect::<Vec<_>>(), before_data);
        assert!(shards[erasure.data_shards].is_none());
    }

    #[test]
    fn rustfs_codec_decode_engine_errors_on_insufficient_shards_like_legacy() {
        let erasure = Erasure::new(4, 2, 16);
        let mut legacy_shards = encoded_shards(&erasure, b"insufficient shards must fail");
        let mut rustfs_shards = legacy_shards.clone();
        for index in [0, 1, 2] {
            legacy_shards[index] = None;
            rustfs_shards[index] = None;
        }

        let legacy = LegacyEcDecodeEngine::new(erasure.clone());
        let rustfs = RustfsCodecDecodeEngine::new(&erasure).expect("engine should be created");

        assert!(reconstruct_with(&legacy, &mut legacy_shards).is_err());
        assert!(reconstruct_with(&rustfs, &mut rustfs_shards).is_err());
    }

    #[test]
    fn rustfs_codec_decode_engine_recovers_missing_data_and_parity() {
        // backlog#868 item 2: one missing data shard plus one missing parity
        // shard is recoverable, but the parity verification path used to
        // require every shard slot to be present after data-only
        // reconstruction and misreported this degraded read as a failure.
        let erasure = Erasure::new(6, 4, 96);
        let mut shards = encoded_shards(&erasure, &(0u8..=191u8).collect::<Vec<_>>());
        let expected_data = shards.iter().take(erasure.data_shards).cloned().collect::<Vec<_>>();
        shards[1] = None;
        shards[erasure.data_shards + 1] = None;

        let engine = RustfsCodecDecodeEngine::new(&erasure).expect("engine should be created");
        reconstruct_with(&engine, &mut shards).expect("one missing data shard plus one missing parity shard is recoverable");

        assert_eq!(shards.iter().take(erasure.data_shards).cloned().collect::<Vec<_>>(), expected_data);
        assert!(shards[erasure.data_shards + 1].is_some(), "missing parity is rebuilt for verification");
    }

    #[test]
    fn rustfs_codec_decode_engine_matches_legacy_on_missing_data_and_parity() {
        let erasure = Erasure::new(6, 4, 96);
        let mut legacy_shards = encoded_shards(&erasure, &(0u8..=191u8).rev().collect::<Vec<_>>());
        let mut rustfs_shards = legacy_shards.clone();
        for shards in [&mut legacy_shards, &mut rustfs_shards] {
            shards[2] = None;
            shards[erasure.data_shards] = None;
        }

        let legacy = LegacyEcDecodeEngine::new(erasure.clone());
        let rustfs = RustfsCodecDecodeEngine::new(&erasure).expect("engine should be created");

        reconstruct_with(&legacy, &mut legacy_shards).expect("legacy should recover missing data plus parity");
        reconstruct_with(&rustfs, &mut rustfs_shards).expect("rustfs codec should recover missing data plus parity");

        assert_eq!(
            rustfs_shards.iter().take(erasure.data_shards).cloned().collect::<Vec<_>>(),
            legacy_shards.iter().take(erasure.data_shards).cloned().collect::<Vec<_>>()
        );
    }

    #[test]
    fn rustfs_codec_decode_engine_rejects_corrupt_parity_with_missing_data_and_parity() {
        // The degraded-read allowance must not weaken inconsistent-source
        // rejection: with one data and one parity shard missing, a corrupted
        // surviving parity shard still fails verification.
        let erasure = Erasure::new(6, 4, 96);
        let mut shards = encoded_shards(&erasure, &(0u8..=191u8).collect::<Vec<_>>());
        shards[0] = None;
        shards[erasure.data_shards] = None;
        let Some(corrupt_parity) = shards[erasure.data_shards + 1].as_mut() else {
            panic!("test parity shard should be present");
        };
        corrupt_parity[0] ^= 0x80;

        let engine = RustfsCodecDecodeEngine::new(&erasure).expect("engine should be created");
        let err = reconstruct_with(&engine, &mut shards).expect_err("corrupt surviving parity must still be rejected");

        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("inconsistent read source shards"));
    }

    #[test]
    fn rustfs_codec_decode_engine_rejects_stale_data_with_missing_data_and_parity() {
        let erasure = Erasure::new(6, 4, 96);
        let mut shards = encoded_shards(&erasure, &(0u8..=191u8).collect::<Vec<_>>());
        shards[0] = None;
        shards[erasure.data_shards] = None;
        let Some(stale_data) = shards[1].as_mut() else {
            panic!("test data shard should be present");
        };
        stale_data[0] ^= 0x40;

        let engine = RustfsCodecDecodeEngine::new(&erasure).expect("engine should be created");
        let err = reconstruct_with(&engine, &mut shards).expect_err("stale surviving data must still be rejected");

        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("inconsistent read source shards"));
    }

    #[test]
    fn rustfs_codec_decode_engine_rejects_inconsistent_reconstruction_sources() {
        let erasure = Erasure::new(2, 2, 32);
        let encoded = erasure
            .encode_data(&(0u8..64u8).collect::<Vec<_>>())
            .expect("test stripe should encode");
        let mut shards = encoded.into_iter().map(|shard| Some(shard.to_vec())).collect::<Vec<_>>();
        shards[0] = None;
        let Some(corrupt_parity) = shards[erasure.data_shards].as_mut() else {
            panic!("test parity shard should be present");
        };
        corrupt_parity[0] ^= 0x80;

        let engine = RustfsCodecDecodeEngine::new(&erasure).expect("engine should be created");
        let err = reconstruct_with(&engine, &mut shards).expect_err("rustfs codec should reject inconsistent sources");

        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("inconsistent read source shards"));
    }

    #[test]
    fn rustfs_codec_decode_engine_rejects_stale_data_source() {
        let erasure = Erasure::new(4, 2, 32);
        let encoded = erasure
            .encode_data(&(0u8..128u8).collect::<Vec<_>>())
            .expect("test stripe should encode");
        let mut shards = encoded.into_iter().map(|shard| Some(shard.to_vec())).collect::<Vec<_>>();
        shards[0] = None;
        let Some(stale_data) = shards[1].as_mut() else {
            panic!("test data shard should be present");
        };
        stale_data[0] ^= 0x40;

        let engine = RustfsCodecDecodeEngine::new(&erasure).expect("engine should be created");
        let err = reconstruct_with(&engine, &mut shards).expect_err("rustfs codec should reject stale data sources");

        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("inconsistent read source shards"));
    }

    #[test]
    fn rustfs_codec_decode_engine_recovers_empty_data_shard() {
        let erasure = Erasure::new(4, 2, 16);
        let mut shards = encoded_shards(&erasure, b"");
        shards[0] = None;

        let engine = RustfsCodecDecodeEngine::new(&erasure).expect("engine should be created");
        reconstruct_with(&engine, &mut shards).expect("empty shard should reconstruct");

        assert_eq!(shards[0], Some(Vec::new()));
        assert!(shards.iter().take(erasure.data_shards).all(Option::is_some));
    }

    #[test]
    fn codec_streaming_decode_engine_dispatches_to_rustfs_workspace() {
        let erasure = Erasure::new(4, 2, 16);
        let mut shards = encoded_shards(&erasure, b"dispatch keeps rustfs engine byte-identical");
        shards[2] = None;

        let engine = CodecStreamingDecodeEngine::rustfs(&erasure).expect("engine should be created");
        let mut workspace = engine.prepare_workspace(4).expect("workspace should be prepared");
        engine
            .reconstruct_into(&mut shards, &mut workspace)
            .expect("enum engine should dispatch reconstruction");

        assert!(shards.iter().take(engine.data_shards()).all(Option::is_some));
    }
}
