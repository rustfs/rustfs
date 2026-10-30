// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Shared, bounded native multipart publication and recovery evidence.

use super::error::{DiskError, Result};
use bytes::Bytes;
use parking_lot::RwLock;
use rustfs_filemeta::FileInfo;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use uuid::Uuid;

pub const MULTIPART_SOURCE_PROTOCOL_VERSION: u16 = 1;
pub const MULTIPART_SOURCE_MAX_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;
pub const MULTIPART_SOURCE_CONDITION_SUFFIX: &str = "multipart-source-condition";
const PUBLICATION_SUFFIX: &str = "multipart-source-publication";
const STATUS_SUFFIX: &str = "multipart-source-status";
#[path = "multipart_source_abort.rs"]
mod abort;
pub use abort::*;

const PROOF_DOMAIN: &[u8] = b"rustfs-mpu-source-native-proof-v1\0";

/// A native-only proof scope, bound to one complete local instance topology.
/// It is neither serialized nor backed by RPC credentials.
#[derive(Debug)]
pub struct LocalMultipartProofAuthority {
    endpoints: RwLock<HashSet<super::endpoint::Endpoint>>,
}

/// Explicit object-graph authority; an uninitialized instance stays RPC-only.
#[derive(Clone, Debug, Default)]
pub enum ProofVerificationContext {
    #[default]
    RpcOnly,
    Local(Arc<LocalMultipartProofAuthority>),
}
impl ProofVerificationContext {
    // Production registration goes through InstanceContext's complete registry
    // validation. Hermetic per-set overrides may use nonzero logical pool IDs.
    pub(crate) fn for_topology(pools: &crate::layout::endpoints::EndpointServerPools) -> Result<([u8; 32], Self)> {
        if pools.as_ref().is_empty() {
            return Err(DiskError::InconsistentDisk);
        }
        let mut topology = Vec::with_capacity(pools.as_ref().len());
        let mut endpoints = HashSet::new();
        let mut pool_ids = HashSet::new();
        let mut all_local = true;
        for pool in pools.as_ref() {
            let count = pool
                .set_count
                .checked_mul(pool.drives_per_set)
                .ok_or(DiskError::InconsistentDisk)?;
            if count == 0 || pool.endpoints.as_ref().len() != count {
                return Err(DiskError::InconsistentDisk);
            }
            let pool_id = pool.endpoints.as_ref()[0].pool_idx;
            if pool_id < 0 || !pool_ids.insert(pool_id) {
                return Err(DiskError::InconsistentDisk);
            }
            let mut slots = HashSet::with_capacity(count);
            let mut entries = Vec::with_capacity(count);
            for endpoint in pool.endpoints.as_ref() {
                let set = usize::try_from(endpoint.set_idx).map_err(|_| DiskError::InconsistentDisk)?;
                let slot = usize::try_from(endpoint.disk_idx).map_err(|_| DiskError::InconsistentDisk)?;
                if endpoint.pool_idx != pool_id
                    || set >= pool.set_count
                    || slot >= pool.drives_per_set
                    || !slots.insert((set, slot))
                    || !endpoints.insert(endpoint.clone())
                {
                    return Err(DiskError::InconsistentDisk);
                }
                all_local &= endpoint.is_local;
                entries.push((set, slot, endpoint.to_string(), endpoint.is_local));
            }
            entries.sort_unstable();
            topology.push((pool_id, pool.set_count, pool.drives_per_set, entries));
        }
        topology.sort_unstable();
        let topology_sha256 = digest_value(&topology)?;
        let authority = if all_local {
            Self::Local(Arc::new(LocalMultipartProofAuthority {
                endpoints: RwLock::new(endpoints),
            }))
        } else {
            Self::RpcOnly
        };
        Ok((topology_sha256, authority))
    }

    pub(crate) fn same_authority(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::RpcOnly, Self::RpcOnly) => true,
            (Self::Local(left), Self::Local(right)) => Arc::ptr_eq(left, right),
            _ => false,
        }
    }

    /// The candidate is freshly constructed from one complete validated topology.
    /// Its endpoint permissions do not authenticate any source or quorum vote.
    pub(crate) fn register_topology(&self, candidate: &Self) -> Result<()> {
        match (self, candidate) {
            (Self::RpcOnly, Self::RpcOnly) => Ok(()),
            (Self::Local(existing), Self::Local(next)) => {
                if !Arc::ptr_eq(existing, next) {
                    // The candidate is private to this registration; only the
                    // established authority is shared between binders.
                    let endpoints = next.endpoints.read();
                    existing.endpoints.write().extend(endpoints.iter().cloned());
                }
                Ok(())
            }
            _ => Err(DiskError::InconsistentDisk),
        }
    }

    pub(crate) fn validate_endpoint(&self, endpoint: &super::endpoint::Endpoint) -> Result<()> {
        if let Self::Local(authority) = self
            && (!endpoint.is_local || !authority.endpoints.read().contains(endpoint))
        {
            return Err(DiskError::InconsistentDisk);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "class", content = "mac", rename_all = "snake_case", deny_unknown_fields)]
enum ProofAuthentication {
    NativeLocal,
    ClusterHmac([u8; 32]),
}

struct NativeOriginWitness {
    authority: Arc<LocalMultipartProofAuthority>,
    body_sha256: [u8; 32],
}
impl std::fmt::Debug for NativeOriginWitness {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("NativeOriginWitness(<native>)")
    }
}

/// Bind newly opened/reconnected physical handles before using native proofs.
/// The complete local topology was validated when the instance scope was made.
pub(crate) fn bind_multipart_proof_snapshot(
    disks: &[Option<super::DiskStore>],
    authority: &ProofVerificationContext,
) -> Result<()> {
    for (slot, disk) in disks.iter().enumerate() {
        let Some(disk) = disk else { continue };
        match disk.as_ref() {
            super::Disk::Local(wrapper) => {
                let local = wrapper.get_disk();
                if usize::try_from(local.endpoint.disk_idx).ok() != Some(slot) {
                    return Err(DiskError::InconsistentDisk);
                }
                local.bind_multipart_proof_authority(authority)?;
            }
            super::Disk::Remote(_) if matches!(authority, ProofVerificationContext::Local(_)) => {
                return Err(DiskError::InconsistentDisk);
            }
            super::Disk::Remote(_) => {}
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceGeneration {
    pub epoch: u64,
    pub nonce: Uuid,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceGeometry {
    pub data_blocks: usize,
    pub parity_blocks: usize,
    pub block_size: usize,
    pub distribution: Vec<usize>,
}
impl SourceGeometry {
    pub fn disks(&self) -> usize {
        self.data_blocks + self.parity_blocks
    }
    pub fn write_quorum(&self) -> usize {
        self.data_blocks + usize::from(self.data_blocks == self.parity_blocks)
    }
    pub fn negative_quorum(&self) -> usize {
        self.disks() - self.write_quorum() + 1
    }
    pub fn validate(&self) -> Result<()> {
        let n = self
            .data_blocks
            .checked_add(self.parity_blocks)
            .ok_or(DiskError::FileCorrupt)?;
        if self.data_blocks == 0 || n > 16 || n == 0 || self.block_size == 0 || self.distribution.len() != n {
            return Err(DiskError::FileCorrupt);
        }
        let mut seen = [false; 16];
        for &index in &self.distribution {
            if index == 0 || index > n || std::mem::replace(&mut seen[index - 1], true) {
                return Err(DiskError::FileCorrupt);
            }
        }
        Ok(())
    }
}

// rmp-serde encodes i128 as a 16-byte binary value. Tagged enum buffering
// uses Serde's ContentDeserializer, which cannot deserialize a bare i128.
// Read that same encoding through deserialize_any without changing its bytes.
fn deserialize_native_nanoseconds<'de, D: serde::Deserializer<'de>>(deserializer: D) -> std::result::Result<i128, D::Error> {
    struct Nanoseconds;
    impl<'de> serde::de::Visitor<'de> for Nanoseconds {
        type Value = i128;
        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a 16-byte signed nanosecond value or an integer")
        }
        fn visit_bytes<E: serde::de::Error>(self, bytes: &[u8]) -> std::result::Result<i128, E> {
            let fixed: [u8; 16] = bytes.try_into().map_err(|_| E::invalid_length(bytes.len(), &self))?;
            Ok(i128::from_be_bytes(fixed))
        }
        fn visit_i64<E: serde::de::Error>(self, value: i64) -> std::result::Result<i128, E> {
            Ok(i128::from(value))
        }
        fn visit_u64<E: serde::de::Error>(self, value: u64) -> std::result::Result<i128, E> {
            Ok(i128::from(value))
        }
    }
    deserializer.deserialize_any(Nanoseconds)
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceLineage {
    pub bucket: String,
    pub object: String,
    pub path: String,
    pub original_data_dir: Uuid,
    pub original_version_id: Option<Uuid>,
    #[serde(deserialize_with = "deserialize_native_nanoseconds")]
    pub created_unix_nanos: i128,
    pub original_identity: [u8; 32],
    pub geometry: SourceGeometry,
}
impl SourceLineage {
    pub fn digest(&self) -> Result<[u8; 32]> {
        digest_value(self)
    }
    pub fn validate(&self) -> Result<()> {
        validate_path(&self.path)?;
        self.geometry.validate()?;
        if self.bucket.is_empty() || self.object.is_empty() || self.original_data_dir.is_nil() {
            return Err(DiskError::FileCorrupt);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceConditionV1 {
    pub lineage: SourceLineage,
    pub generation: SourceGeneration,
    pub attempt: Uuid,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicationIdentity {
    pub lineage: SourceLineage,
    pub source_generation: SourceGeneration,
    pub attempt: Uuid,
    pub version_id: Option<Uuid>,
    pub final_data_dir: Uuid,
    #[serde(deserialize_with = "deserialize_native_nanoseconds")]
    pub modified_unix_nanos: i128,
    pub completion_identity: [u8; 32],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MultipartSourcePhase {
    Active,
    Prepared,
    ConsumedUnsealed,
    UndoIntent,
    Recovering,
    LocalTerminalSeal,
    GlobalTerminalCertified,
    RootMissing,
    Aborting,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MultipartSourceSnapshot {
    pub disk_id: Uuid,
    /// Zero-based physical slot, obtained from validated source distribution.
    pub physical_slot: usize,
    pub lineage: SourceLineage,
    pub generation: SourceGeneration,
    pub revision: u64,
    pub phase: MultipartSourcePhase,
    pub state_sha256: [u8; 32],
    pub head_sha256: Option<[u8; 32]>,
    pub inventory_sha256: [u8; 32],
    pub capability_sha256: Option<[u8; 32]>,
    pub publication: Option<PublicationIdentity>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MultipartRecoveryRound {
    pub lineage: SourceLineage,
    pub from_generation: SourceGeneration,
    pub old_attempt: Uuid,
    pub generation: SourceGeneration,
    pub round: Uuid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MultipartProofKind {
    Published,
    Claimed,
    Restored,
    Activated,
    TerminalTarget,
    TerminalConsumed,
    RootMissingTerminal,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MultipartSourceProof {
    pub kind: MultipartProofKind,
    pub challenge: Uuid,
    pub recovery: Option<MultipartRecoveryRound>,
    pub snapshot: MultipartSourceSnapshot,
    authentication: ProofAuthentication,
    #[serde(skip)]
    native_origin: Option<Arc<NativeOriginWitness>>,
}
impl PartialEq for MultipartSourceProof {
    fn eq(&self, other: &Self) -> bool {
        self.kind == other.kind
            && self.challenge == other.challenge
            && self.recovery == other.recovery
            && self.snapshot == other.snapshot
            && self.authentication == other.authentication
    }
}
impl Eq for MultipartSourceProof {}

impl MultipartSourceProof {
    fn body(&self) -> Result<Vec<u8>> {
        let mut body = PROOF_DOMAIN.to_vec();
        body.extend_from_slice(
            &rmp_serde::to_vec_named(&(
                MULTIPART_SOURCE_PROTOCOL_VERSION,
                self.kind,
                self.challenge,
                &self.recovery,
                &self.snapshot,
            ))
            .map_err(super::Error::other)?,
        );
        Ok(body)
    }

    fn validate_identity(&self) -> Result<()> {
        self.snapshot.lineage.validate()?;
        if self.snapshot.disk_id.is_nil()
            || self.snapshot.physical_slot >= self.snapshot.lineage.geometry.disks()
            || self.challenge.is_nil()
            || self.snapshot.generation.nonce.is_nil()
        {
            return Err(DiskError::FileCorrupt);
        }
        Ok(())
    }

    /// The wire-facing default never accepts an in-process origin marker.
    pub fn verify(&self) -> Result<()> {
        self.verify_with_authority(&ProofVerificationContext::RpcOnly)
    }

    pub fn verify_with_authority(&self, authority: &ProofVerificationContext) -> Result<()> {
        self.verify_with_mac_verifier(authority, |body, mac| {
            crate::cluster::rpc::verify_tonic_rpc_response_proof(body, mac).map_err(DiskError::from)
        })
    }

    fn verify_with_mac_verifier(
        &self,
        authority: &ProofVerificationContext,
        verify_mac: impl FnOnce(&[u8], &[u8; 32]) -> Result<()>,
    ) -> Result<()> {
        self.validate_identity()?;
        let body = self.body()?;
        match &self.authentication {
            ProofAuthentication::ClusterHmac(mac) => verify_mac(&body, mac),
            ProofAuthentication::NativeLocal => {
                let ProofVerificationContext::Local(expected) = authority else {
                    return Err(DiskError::FileCorrupt);
                };
                let origin = self.native_origin.as_ref().ok_or(DiskError::FileCorrupt)?;
                if !Arc::ptr_eq(expected, &origin.authority) || origin.body_sha256 != <[u8; 32]>::from(Sha256::digest(&body)) {
                    return Err(DiskError::FileCorrupt);
                }
                Ok(())
            }
        }
    }

    /// Only a native outcome or a privately validated owned record may issue evidence.
    pub(super) fn issue(
        authority: &ProofVerificationContext,
        kind: MultipartProofKind,
        challenge: Uuid,
        recovery: Option<MultipartRecoveryRound>,
        snapshot: MultipartSourceSnapshot,
    ) -> Result<Self> {
        Self::issue_with_mac_signer(authority, kind, challenge, recovery, snapshot, |body| {
            crate::cluster::rpc::sign_tonic_rpc_response_proof(body)
                .map_err(DiskError::from)?
                .try_into()
                .map_err(|_| DiskError::FileCorrupt)
        })
    }

    fn issue_with_mac_signer(
        authority: &ProofVerificationContext,
        kind: MultipartProofKind,
        challenge: Uuid,
        recovery: Option<MultipartRecoveryRound>,
        snapshot: MultipartSourceSnapshot,
        sign_mac: impl FnOnce(&[u8]) -> Result<[u8; 32]>,
    ) -> Result<Self> {
        let mut proof = Self {
            kind,
            challenge,
            recovery,
            snapshot,
            authentication: ProofAuthentication::NativeLocal,
            native_origin: None,
        };
        proof.validate_identity()?;
        match authority {
            ProofVerificationContext::Local(local) => proof.attach_native_origin(local)?,
            ProofVerificationContext::RpcOnly => {
                proof.authentication = ProofAuthentication::ClusterHmac(sign_mac(&proof.body()?)?)
            }
        }
        Ok(proof)
    }

    fn attach_native_origin(&mut self, authority: &Arc<LocalMultipartProofAuthority>) -> Result<()> {
        self.validate_identity()?;
        self.native_origin = Some(Arc::new(NativeOriginWitness {
            authority: authority.clone(),
            body_sha256: Sha256::digest(self.body()?).into(),
        }));
        Ok(())
    }

    fn prepare_wire(&mut self) -> Result<()> {
        self.validate_identity()?;
        match &self.authentication {
            ProofAuthentication::ClusterHmac(_) => self.verify(),
            ProofAuthentication::NativeLocal => {
                let origin = self.native_origin.as_ref().ok_or(DiskError::FileCorrupt)?;
                self.verify_with_authority(&ProofVerificationContext::Local(origin.authority.clone()))?;
                let mac = crate::cluster::rpc::sign_tonic_rpc_response_proof(&self.body()?)
                    .map_err(DiskError::from)?
                    .try_into()
                    .map_err(|_| DiskError::FileCorrupt)?;
                self.authentication = ProofAuthentication::ClusterHmac(mac);
                self.native_origin = None;
                Ok(())
            }
        }
    }
}

/// The token can only be constructed by the native pinned-record loader. Its
/// historical certificate fact never substitutes for a mutation's current CAS.
pub(in crate::disk) fn hydrate_owned_certificate(
    certificate: &mut MultipartCertificate,
    authority: &ProofVerificationContext,
    validated: &super::local::multipart_source_state::ValidatedOwnedCertificate,
) -> Result<()> {
    validated.validate_certificate(certificate)?;
    if let ProofVerificationContext::Local(local) = authority {
        for proof in &mut certificate.proofs {
            match proof.authentication {
                ProofAuthentication::NativeLocal => proof.attach_native_origin(local)?,
                ProofAuthentication::ClusterHmac(_) => proof.verify()?,
            }
        }
    }
    Ok(())
}

/// A secret is moved into exactly one fanout decision owner, never cloned.
#[derive(Serialize, Deserialize)]
#[serde(transparent)]
pub struct MultipartUndoCapability([u8; 32]);
impl std::fmt::Debug for MultipartUndoCapability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MultipartUndoCapability(<redacted>)")
    }
}
impl MultipartUndoCapability {
    pub(super) fn new() -> Self {
        let mut bytes = [0; 32];
        bytes[..16].copy_from_slice(Uuid::new_v4().as_bytes());
        bytes[16..].copy_from_slice(Uuid::new_v4().as_bytes());
        Self(bytes)
    }
    pub(super) fn digest(&self) -> [u8; 32] {
        Sha256::digest(self.0).into()
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MultipartPublicationResult {
    pub publication: PublicationIdentity,
    pub proof: MultipartSourceProof,
    pub undo_capability: MultipartUndoCapability,
}
impl MultipartPublicationResult {
    pub fn verify(&self) -> Result<()> {
        self.verify_with_authority(&ProofVerificationContext::RpcOnly)
    }
    pub fn verify_with_authority(&self, authority: &ProofVerificationContext) -> Result<()> {
        self.proof.verify_with_authority(authority)?;
        if self.proof.kind != MultipartProofKind::Published
            || self.proof.snapshot.publication.as_ref() != Some(&self.publication)
            || self.proof.snapshot.capability_sha256 != Some(self.undo_capability.digest())
        {
            return Err(DiskError::FileCorrupt);
        }
        Ok(())
    }
    pub fn prepare_wire(&mut self) -> Result<()> {
        self.proof.prepare_wire()?;
        self.verify()
    }
    pub fn validate_wire(&self) -> Result<()> {
        self.verify()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MultipartCertificate {
    pub round: MultipartRecoveryRound,
    /// The coordinator obtains this exact order from its configured physical set.
    pub disk_ids: Vec<Uuid>,
    pub proofs: Vec<MultipartSourceProof>,
}
impl MultipartCertificate {
    pub fn verify(&self, kind: MultipartProofKind, quorum: usize) -> Result<()> {
        self.verify_with_authority(&ProofVerificationContext::RpcOnly, kind, quorum)
    }
    pub fn verify_with_authority(
        &self,
        authority: &ProofVerificationContext,
        kind: MultipartProofKind,
        quorum: usize,
    ) -> Result<()> {
        self.round.lineage.validate()?;
        let n = self.round.lineage.geometry.disks();
        if quorum == 0
            || quorum > n
            || self.disk_ids.len() != n
            || self.proofs.len() < quorum
            || self.proofs.len() > n
            || self.disk_ids.iter().any(Uuid::is_nil)
        {
            return Err(DiskError::FileCorrupt);
        }
        let mut slots = [false; 16];
        for proof in &self.proofs {
            proof.verify_with_authority(authority)?;
            let slot = proof.snapshot.physical_slot;
            let valid_phase = match kind {
                MultipartProofKind::Claimed | MultipartProofKind::Restored => {
                    proof.snapshot.phase == MultipartSourcePhase::Recovering
                }
                MultipartProofKind::Activated => proof.snapshot.phase == MultipartSourcePhase::Active,
                _ => return Err(DiskError::FileCorrupt),
            };
            if !valid_phase
                || proof.challenge != self.round.round
                || proof.snapshot.generation != self.round.generation
                || proof.snapshot.head_sha256.is_none()
                || proof.snapshot.capability_sha256.is_some()
                || proof.kind != kind
                || proof.recovery.as_ref() != Some(&self.round)
                || proof.snapshot.lineage != self.round.lineage
                || self.disk_ids[slot] != proof.snapshot.disk_id
                || std::mem::replace(&mut slots[slot], true)
            {
                return Err(DiskError::FileCorrupt);
            }
        }
        let mut ids = self.disk_ids.clone();
        ids.sort_unstable();
        if ids.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(DiskError::FileCorrupt);
        }
        Ok(())
    }
}

impl MultipartCertificate {
    fn prepare_wire(&mut self) -> Result<()> {
        if self.proofs.len() > 16 {
            return Err(DiskError::FileCorrupt);
        }
        for proof in &mut self.proofs {
            proof.prepare_wire()?;
        }
        Ok(())
    }
    fn validate_wire(&self) -> Result<()> {
        if self.proofs.len() > 16 {
            return Err(DiskError::FileCorrupt);
        }
        for proof in &self.proofs {
            proof.verify()?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MultipartSourceInspection {
    pub disk_id: Uuid,
    pub snapshot: Option<MultipartSourceSnapshot>,
    pub original: Option<Bytes>,
    pub recovery: Option<MultipartRecoveryRound>,
    pub negative_certificate: Option<MultipartCertificate>,
    pub ready_certificate: Option<MultipartCertificate>,
    pub terminal_certificate: Vec<MultipartSourceProof>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub root_missing: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub abort_round: Option<MultipartAbortRound>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub abort_certificate: Option<MultipartAbortCertificate>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum MultipartSourceOperation {
    Inspect {
        lineage: Option<SourceLineage>,
    },
    UpgradeLegacy {
        expected: MultipartSourceSnapshot,
    },
    PrepareAbort {
        expected: MultipartSourceSnapshot,
        round: MultipartAbortRound,
    },
    CommitAbort {
        expected: MultipartSourceSnapshot,
        certificate: MultipartAbortCertificate,
    },
    DeleteAborted {
        expected: MultipartSourceSnapshot,
        certificate: MultipartAbortCertificate,
    },
    ConfirmCleanupTarget {
        expected: MultipartSourceSnapshot,
        publication: PublicationIdentity,
        challenge: Uuid,
    },
    Claim {
        expected: MultipartSourceSnapshot,
        round: MultipartRecoveryRound,
    },
    UndoAndClaim {
        expected: Box<MultipartSourceSnapshot>,
        publication: PublicationIdentity,
        capability: MultipartUndoCapability,
        round: MultipartRecoveryRound,
    },
    Restore {
        expected: MultipartSourceSnapshot,
        certificate: MultipartCertificate,
    },
    Activate {
        expected: MultipartSourceSnapshot,
        certificate: MultipartCertificate,
    },
    ConfirmTerminal {
        expected: Option<MultipartSourceSnapshot>,
        publication: PublicationIdentity,
        challenge: Uuid,
    },
    SealConsumedTerminal {
        expected: MultipartSourceSnapshot,
        publication: PublicationIdentity,
        challenge: Uuid,
    },
}
impl MultipartSourceOperation {
    pub fn is_mutation(&self) -> bool {
        !matches!(self, Self::Inspect { .. })
    }
    pub fn max_wire_payload_bytes() -> usize {
        MULTIPART_SOURCE_MAX_PAYLOAD_BYTES
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MultipartSourceRequest {
    pub protocol_version: u16,
    pub path: String,
    pub operation: MultipartSourceOperation,
}
impl MultipartSourceRequest {
    pub fn prepare_wire(&mut self) -> Result<()> {
        self.validate()?;
        match &mut self.operation {
            MultipartSourceOperation::Restore { certificate, .. } | MultipartSourceOperation::Activate { certificate, .. } => {
                certificate.prepare_wire()
            }
            MultipartSourceOperation::CommitAbort { certificate, .. }
            | MultipartSourceOperation::DeleteAborted { certificate, .. } => certificate.prepare_wire(),
            _ => Ok(()),
        }
    }
    pub fn validate_wire(&self) -> Result<()> {
        self.validate()?;
        match &self.operation {
            MultipartSourceOperation::Restore { certificate, .. } | MultipartSourceOperation::Activate { certificate, .. } => {
                certificate.validate_wire()
            }
            MultipartSourceOperation::CommitAbort { certificate, .. }
            | MultipartSourceOperation::DeleteAborted { certificate, .. } => certificate.validate_wire(),
            _ => Ok(()),
        }
    }
    pub fn validate(&self) -> Result<()> {
        if self.protocol_version != MULTIPART_SOURCE_PROTOCOL_VERSION {
            return Err(DiskError::UnsupportedDisk);
        }
        validate_path(&self.path)?;
        match &self.operation {
            MultipartSourceOperation::PrepareAbort { expected, round } => {
                round.validate()?;
                if expected.lineage != round.lineage {
                    return Err(DiskError::ConditionalDeleteConflict);
                }
            }
            MultipartSourceOperation::CommitAbort { expected, certificate }
            | MultipartSourceOperation::DeleteAborted { expected, certificate } => {
                certificate.validate_shape()?;
                if expected.lineage != certificate.round.lineage || expected.phase != MultipartSourcePhase::Aborting {
                    return Err(DiskError::ConditionalDeleteConflict);
                }
            }
            MultipartSourceOperation::ConfirmCleanupTarget {
                expected,
                publication,
                challenge,
            } => {
                publication.lineage.validate()?;
                if expected.lineage != publication.lineage
                    || expected.phase != MultipartSourcePhase::Aborting
                    || challenge.is_nil()
                {
                    return Err(DiskError::ConditionalDeleteConflict);
                }
            }
            _ => {}
        }
        let lineage = match &self.operation {
            MultipartSourceOperation::Inspect { lineage } => lineage.as_ref(),
            MultipartSourceOperation::UpgradeLegacy { expected }
            | MultipartSourceOperation::PrepareAbort { expected, .. }
            | MultipartSourceOperation::CommitAbort { expected, .. }
            | MultipartSourceOperation::DeleteAborted { expected, .. }
            | MultipartSourceOperation::ConfirmCleanupTarget { expected, .. }
            | MultipartSourceOperation::Claim { expected, .. }
            | MultipartSourceOperation::Restore { expected, .. }
            | MultipartSourceOperation::Activate { expected, .. }
            | MultipartSourceOperation::SealConsumedTerminal { expected, .. } => Some(&expected.lineage),
            MultipartSourceOperation::UndoAndClaim { expected, .. } => Some(&expected.lineage),
            MultipartSourceOperation::ConfirmTerminal { publication, .. } => Some(&publication.lineage),
        };
        if let Some(lineage) = lineage {
            lineage.validate()?;
            if lineage.path != self.path {
                return Err(DiskError::ConditionalDeleteConflict);
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "result", content = "value", rename_all = "snake_case", deny_unknown_fields)]
pub enum MultipartSourceResponse {
    Inspected(MultipartSourceInspection),
    LegacyUpgraded(MultipartSourceInspection),
    AbortPrepared(MultipartAbortProof),
    AbortCommitted(MultipartAbortProof),
    AbortDeleted(MultipartAbortProof),
    CleanupTargetConfirmed(MultipartAbortProof),
    Claimed(MultipartSourceProof),
    Restored(MultipartSourceProof),
    Activated(MultipartSourceProof),
    TerminalConfirmed(MultipartSourceProof),
}
impl MultipartSourceResponse {
    pub fn prepare_wire(&mut self) -> Result<()> {
        match self {
            Self::Inspected(inspection) | Self::LegacyUpgraded(inspection) => {
                if let Some(certificate) = &mut inspection.negative_certificate {
                    certificate.prepare_wire()?;
                }
                if let Some(certificate) = &mut inspection.ready_certificate {
                    certificate.prepare_wire()?;
                }
                if let Some(certificate) = &mut inspection.abort_certificate {
                    certificate.prepare_wire()?;
                }
                if inspection.terminal_certificate.len() > 16 {
                    return Err(DiskError::FileCorrupt);
                }
                for proof in &mut inspection.terminal_certificate {
                    proof.prepare_wire()?;
                }
                Ok(())
            }
            Self::AbortPrepared(proof)
            | Self::AbortCommitted(proof)
            | Self::AbortDeleted(proof)
            | Self::CleanupTargetConfirmed(proof) => proof.prepare_wire(),
            Self::Claimed(proof) | Self::Restored(proof) | Self::Activated(proof) | Self::TerminalConfirmed(proof) => {
                proof.prepare_wire()
            }
        }
    }
    pub fn validate_wire(&self) -> Result<()> {
        match self {
            Self::Inspected(inspection) | Self::LegacyUpgraded(inspection) => {
                if let Some(certificate) = &inspection.negative_certificate {
                    certificate.validate_wire()?;
                }
                if let Some(certificate) = &inspection.ready_certificate {
                    certificate.validate_wire()?;
                }
                if let Some(certificate) = &inspection.abort_certificate {
                    certificate.validate_wire()?;
                }
                if inspection.terminal_certificate.len() > 16 {
                    return Err(DiskError::FileCorrupt);
                }
                for proof in &inspection.terminal_certificate {
                    proof.verify()?;
                }
                Ok(())
            }
            Self::AbortPrepared(proof)
            | Self::AbortCommitted(proof)
            | Self::AbortDeleted(proof)
            | Self::CleanupTargetConfirmed(proof) => proof.verify(),
            Self::Claimed(proof) | Self::Restored(proof) | Self::Activated(proof) | Self::TerminalConfirmed(proof) => {
                proof.verify()
            }
        }
    }
    pub fn validate_for(&self, request: &MultipartSourceRequest) -> Result<()> {
        self.validate_for_with_authority(request, &ProofVerificationContext::RpcOnly)
    }
    pub fn validate_for_with_authority(
        &self,
        request: &MultipartSourceRequest,
        authority: &ProofVerificationContext,
    ) -> Result<()> {
        request.validate()?;
        match (self, &request.operation) {
            (Self::LegacyUpgraded(inspection), MultipartSourceOperation::UpgradeLegacy { expected })
                if inspection.disk_id == expected.disk_id
                    && inspection.snapshot.as_ref().is_some_and(|s| {
                        s.lineage == expected.lineage
                            && s.disk_id == expected.disk_id
                            && s.physical_slot == expected.physical_slot
                    }) =>
            {
                Ok(())
            }
            (Self::AbortPrepared(proof), MultipartSourceOperation::PrepareAbort { round, expected })
                if proof.kind == MultipartAbortProofKind::Prepared
                    && proof.round == *round
                    && proof.challenge == round.round
                    && proof.snapshot.disk_id == expected.disk_id
                    && proof.snapshot.physical_slot == expected.physical_slot =>
            {
                proof.verify_with_authority(authority)
            }
            (Self::AbortCommitted(proof), MultipartSourceOperation::CommitAbort { certificate, expected })
            | (Self::AbortDeleted(proof), MultipartSourceOperation::DeleteAborted { certificate, expected })
                if proof.round == certificate.round
                    && proof.challenge == certificate.round.round
                    && proof.snapshot.disk_id == expected.disk_id
                    && proof.snapshot.physical_slot == expected.physical_slot
                    && proof.certificate_sha256 == Some(certificate.digest()?)
                    && matches!(
                        (self, proof.kind),
                        (Self::AbortCommitted(_), MultipartAbortProofKind::Committed)
                            | (Self::AbortDeleted(_), MultipartAbortProofKind::Deleted)
                    ) =>
            {
                proof.verify_with_authority(authority)
            }
            (
                Self::CleanupTargetConfirmed(proof),
                MultipartSourceOperation::ConfirmCleanupTarget {
                    expected,
                    publication,
                    challenge,
                },
            ) if proof.kind == MultipartAbortProofKind::CleanupTarget
                && proof.challenge == *challenge
                && proof.publication.as_ref() == Some(publication)
                && proof.snapshot == *expected =>
            {
                proof.verify_with_authority(authority)
            }
            (Self::Inspected(inspection), MultipartSourceOperation::Inspect { .. }) if !inspection.disk_id.is_nil() => {
                if inspection
                    .snapshot
                    .as_ref()
                    .is_some_and(|snapshot| snapshot.lineage.path != request.path || snapshot.disk_id != inspection.disk_id)
                {
                    return Err(DiskError::FileCorrupt);
                }
                Ok(())
            }
            (
                Self::Claimed(proof),
                MultipartSourceOperation::Claim { round, .. } | MultipartSourceOperation::UndoAndClaim { round, .. },
            ) if proof.kind == MultipartProofKind::Claimed && proof.recovery.as_ref() == Some(round) => {
                proof.verify_with_authority(authority)
            }
            (Self::Restored(proof), MultipartSourceOperation::Restore { certificate, .. })
                if proof.kind == MultipartProofKind::Restored && proof.recovery.as_ref() == Some(&certificate.round) =>
            {
                proof.verify_with_authority(authority)
            }
            (Self::Activated(proof), MultipartSourceOperation::Activate { certificate, .. })
                if proof.kind == MultipartProofKind::Activated && proof.recovery.as_ref() == Some(&certificate.round) =>
            {
                proof.verify_with_authority(authority)
            }
            (
                Self::TerminalConfirmed(proof),
                MultipartSourceOperation::ConfirmTerminal {
                    publication, challenge, ..
                },
            ) if matches!(proof.kind, MultipartProofKind::TerminalTarget | MultipartProofKind::RootMissingTerminal)
                && proof.challenge == *challenge
                && proof.snapshot.publication.as_ref() == Some(publication) =>
            {
                proof.verify_with_authority(authority)
            }
            (
                Self::TerminalConfirmed(proof),
                MultipartSourceOperation::SealConsumedTerminal {
                    publication, challenge, ..
                },
            ) if proof.kind == MultipartProofKind::TerminalConsumed
                && proof.challenge == *challenge
                && proof.snapshot.publication.as_ref() == Some(publication) =>
            {
                proof.verify_with_authority(authority)
            }
            _ => Err(DiskError::FileCorrupt),
        }
    }
}

fn validate_path(path: &str) -> Result<()> {
    if path.is_empty()
        || path.len() > 4096
        || path.starts_with('/')
        || path.contains('\\')
        || path.split('/').any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Err(DiskError::InvalidPath);
    }
    Ok(())
}
fn digest_value(value: &impl Serialize) -> Result<[u8; 32]> {
    // JSON Values recursively order named maps; canonicalize explicitly so a
    // dependency's preserve_order feature cannot change the proof identity.
    fn canonical(value: serde_json::Value) -> serde_json::Value {
        match value {
            serde_json::Value::Object(map) => {
                let mut entries = map.into_iter().collect::<Vec<_>>();
                entries.sort_unstable_by(|a, b| a.0.cmp(&b.0));
                serde_json::Value::Object(entries.into_iter().map(|(key, value)| (key, canonical(value))).collect())
            }
            serde_json::Value::Array(values) => serde_json::Value::Array(values.into_iter().map(canonical).collect()),
            value => value,
        }
    }
    let value = canonical(serde_json::to_value(value).map_err(super::Error::other)?);
    Ok(Sha256::digest(serde_json::to_vec(&value).map_err(super::Error::other)?).into())
}
// Empty values are meaningful for acknowledged movement and cleared metadata;
// conflicting compatibility aliases must remain part of the content mismatch.
fn consistent_completion_internal_value<'a>(metadata: &'a HashMap<String, String>, suffix: &str) -> Option<&'a str> {
    let mut values = metadata
        .iter()
        .filter(|(key, _)| rustfs_utils::http::metadata_compat::has_internal_suffix(key, suffix))
        .map(|(_, value)| value.as_str());
    let first = values.next()?;
    values.all(|value| value == first).then_some(first)
}

fn normalize_completion_metadata(fi: &mut FileInfo) {
    use rustfs_filemeta::metadata_keys::{
        OBJECT_LOCK_LEGAL_HOLD, OBJECT_LOCK_MODE, OBJECT_LOCK_RETAIN_UNTIL_DATE, REPLICATION_STATUS,
    };
    use rustfs_utils::http::headers::AMZ_OBJECT_TAGGING;
    use rustfs_utils::http::metadata_compat::{
        SUFFIX_CRC, SUFFIX_DATA_MOVED, SUFFIX_OBJECTLOCK_LEGALHOLD_TIMESTAMP, SUFFIX_OBJECTLOCK_RETENTION_TIMESTAMP,
        SUFFIX_REPLICATION_GENERATION, SUFFIX_REPLICATION_STATUS, SUFFIX_REPLICATION_TARGET_VERSION_ARN_PREFIX,
        SUFFIX_REPLICATION_TIMESTAMP, SUFFIX_TAGGING_TIMESTAMP, has_internal_suffix, internal_key_strip_suffix_prefix,
        replication_target_versions,
    };

    // These producers rewrite the same object version: object tagging/lock
    // metadata and replication status write-back. Source identity stays full.
    let mutable_suffixes = [
        SUFFIX_TAGGING_TIMESTAMP,
        SUFFIX_OBJECTLOCK_RETENTION_TIMESTAMP,
        SUFFIX_OBJECTLOCK_LEGALHOLD_TIMESTAMP,
        SUFFIX_REPLICATION_GENERATION,
        SUFFIX_REPLICATION_TIMESTAMP,
        SUFFIX_REPLICATION_STATUS,
    ];
    let removable = mutable_suffixes.map(|suffix| consistent_completion_internal_value(&fi.metadata, suffix).is_some());
    let acknowledged_movement = consistent_completion_internal_value(&fi.metadata, SUFFIX_DATA_MOVED)
        .is_some_and(|value| value.is_empty() || value == "true");
    let valid_target_ledger = !replication_target_versions(&fi.metadata).1;
    // FileMeta also exposes its binary checksum as UTF-8 metadata (empty for
    // non-UTF-8 bytes). Remove only that exact duplicate; the bytes stay bound.
    let checksum_metadata_is_mirror = fi.checksum.as_ref().is_some_and(|checksum| {
        consistent_completion_internal_value(&fi.metadata, SUFFIX_CRC) == Some(std::str::from_utf8(checksum).unwrap_or_default())
    });
    fi.metadata.retain(|key, _| {
        if [
            AMZ_OBJECT_TAGGING,
            OBJECT_LOCK_MODE,
            OBJECT_LOCK_RETAIN_UNTIL_DATE,
            OBJECT_LOCK_LEGAL_HOLD,
        ]
        .iter()
        .any(|mutable| key.eq_ignore_ascii_case(mutable))
        {
            return false;
        }
        // FileMeta derives this public header from the internal status. It
        // has no independent content meaning when that status is projected.
        if removable[5] && key.eq_ignore_ascii_case(REPLICATION_STATUS)
            || checksum_metadata_is_mirror && has_internal_suffix(key, SUFFIX_CRC)
        {
            return false;
        }
        if mutable_suffixes
            .iter()
            .zip(removable)
            .any(|(suffix, remove)| remove && has_internal_suffix(key, suffix))
            || acknowledged_movement && has_internal_suffix(key, SUFFIX_DATA_MOVED)
        {
            return false;
        }
        !(valid_target_ledger && internal_key_strip_suffix_prefix(key, SUFFIX_REPLICATION_TARGET_VERSION_ARN_PREFIX).is_some())
    });

    // This is only the mirror of the two removed status metadata categories.
    // Preserve replica/purge/reset state and every other non-content control.
    if let Some(state) = &mut fi.replication_state_internal {
        if removable[4] {
            state.replication_timestamp = None;
        }
        if removable[5] {
            state.replication_status_internal = None;
            state.targets.clear();
        }
        if (removable[4] || removable[5])
            && *state
                == crate::bucket::replication::replication_state_to_filemeta(
                    &crate::bucket::replication::ReplicationState::default(),
                )
        {
            fi.replication_state_internal = None;
        }
    }
}

fn normalized_info(fi: &FileInfo, completion: bool) -> FileInfo {
    let mut fi = fi.clone();
    fi.volume.clear();
    fi.name.clear();
    fi.is_latest = false;
    fi.num_versions = 0;
    fi.successor_mod_time = None;
    fi.fresh = false;
    fi.idx = 0;
    fi.data = None;
    fi.versioned = false;
    // FileMeta persists the null version as a nil UUID and decodes it as None.
    fi.version_id = fi.version_id.filter(|version| !version.is_nil());
    fi.uses_legacy_checksum = false;
    fi.overwrite_tier_free_version_id = None;
    fi.erasure.index = 0;
    strip_native_controls(&mut fi.metadata);
    rustfs_utils::http::metadata_compat::remove_str(&mut fi.metadata, super::QUOTA_MUTATION_FENCE_METADATA_SUFFIX);
    if completion {
        normalize_completion_metadata(&mut fi);
        fi.erasure.checksums.clear();
        for part in &mut fi.parts {
            // Native xl.meta retains the object mtime, not per-part staging mtimes.
            part.mod_time = None;
            // xl.meta derives this mirror from persisted part-checksum metadata;
            // that metadata and the object checksum remain authoritative below.
            part.checksums = None;
            part.index = None;
            part.integrity = None;
        }
    }
    fi
}

pub fn capture_source_condition(
    bucket: &str,
    object: &str,
    path: &str,
    fi: &FileInfo,
    attempt: Uuid,
) -> Result<SourceConditionV1> {
    fi.validate_for_metadata_read()?;
    if !is_active_source(fi)? || attempt.is_nil() {
        return Err(DiskError::ConditionalDeleteConflict);
    }
    native_source_protocol(fi)?;
    let original = normalized_info(fi, false);
    let source_dir = fi.data_dir.filter(|dir| !dir.is_nil()).ok_or(DiskError::FileCorrupt)?;
    let generation = source_status(fi)?.map_or(
        SourceGeneration {
            epoch: 0,
            nonce: source_dir,
        },
        |status| status.generation,
    );
    let lineage = SourceLineage {
        bucket: bucket.to_owned(),
        object: object.to_owned(),
        path: path.to_owned(),
        original_data_dir: source_dir,
        original_version_id: fi.version_id.filter(|version| !version.is_nil()),
        created_unix_nanos: fi.mod_time.ok_or(DiskError::FileCorrupt)?.unix_timestamp_nanos(),
        original_identity: digest_value(&original)?,
        geometry: SourceGeometry {
            data_blocks: fi.erasure.data_blocks,
            parity_blocks: fi.erasure.parity_blocks,
            block_size: fi.erasure.block_size,
            distribution: fi.erasure.distribution.clone(),
        },
    };
    lineage.validate()?;
    Ok(SourceConditionV1 {
        lineage,
        generation,
        attempt,
    })
}
impl SourceConditionV1 {
    pub fn validate(&self) -> Result<()> {
        self.lineage.validate()?;
        if self.generation.nonce.is_nil()
            || self.attempt.is_nil()
            || (self.generation.epoch == 0 && self.generation.nonce != self.lineage.original_data_dir)
        {
            return Err(DiskError::FileCorrupt);
        }
        Ok(())
    }
    /// Adds the common persisted tuple and the transport-only precondition.
    pub fn attach(&self, fi: &mut FileInfo) -> Result<PublicationIdentity> {
        native_source_protocol(fi)?;
        rustfs_utils::http::metadata_compat::remove_str(&mut fi.metadata, NATIVE_SOURCE_PROTOCOL_SUFFIX);

        self.validate()?;
        let publication = PublicationIdentity {
            lineage: self.lineage.clone(),
            source_generation: self.generation,
            attempt: self.attempt,
            version_id: fi.version_id.filter(|version| !version.is_nil()),
            final_data_dir: fi.data_dir.filter(|dir| !dir.is_nil()).ok_or(DiskError::FileCorrupt)?,
            modified_unix_nanos: fi.mod_time.ok_or(DiskError::FileCorrupt)?.unix_timestamp_nanos(),
            completion_identity: digest_value(&normalized_info(fi, true))?,
        };
        insert_metadata(&mut fi.metadata, MULTIPART_SOURCE_CONDITION_SUFFIX, self)?;
        insert_metadata(&mut fi.metadata, PUBLICATION_SUFFIX, &publication)?;
        Ok(publication)
    }
    pub(super) fn take(fi: &mut FileInfo) -> Result<Option<Self>> {
        let condition = decode_metadata(&fi.metadata, MULTIPART_SOURCE_CONDITION_SUFFIX)?;
        strip_transport_condition(&mut fi.metadata);
        Ok(condition)
    }
    pub(super) fn matches_original(&self, fi: &FileInfo) -> Result<bool> {
        native_source_protocol(fi)?;
        Ok(digest_value(&normalized_info(fi, false))? == self.lineage.original_identity)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SourceStatus {
    pub generation: SourceGeneration,
    pub recovering: bool,
    pub round: Option<Uuid>,
    pub ready_sha256: Option<[u8; 32]>,
}
pub(super) fn source_status(fi: &FileInfo) -> Result<Option<SourceStatus>> {
    decode_metadata(&fi.metadata, STATUS_SUFFIX)
}
pub fn is_active_source(fi: &FileInfo) -> Result<bool> {
    native_source_protocol(fi)?;
    if publication_identity(fi)?.is_some() {
        return Ok(false);
    }
    Ok(source_status(fi)?
        .is_none_or(|status| !status.recovering && (status.generation.epoch == 0 || status.ready_sha256.is_some())))
}
pub fn require_source_condition(fi: &FileInfo) -> Result<SourceConditionV1> {
    let condition: SourceConditionV1 =
        decode_metadata(&fi.metadata, MULTIPART_SOURCE_CONDITION_SUFFIX)?.ok_or(DiskError::ConditionalDeleteConflict)?;
    condition.validate()?;
    Ok(condition)
}
pub fn publication_identity(fi: &FileInfo) -> Result<Option<PublicationIdentity>> {
    publication_identity_from_metadata(&fi.metadata)
}
pub(crate) fn publication_identity_from_metadata(metadata: &HashMap<String, String>) -> Result<Option<PublicationIdentity>> {
    decode_metadata(metadata, PUBLICATION_SUFFIX)
}
pub(crate) fn is_publication_metadata_key(key: &str) -> bool {
    rustfs_utils::http::metadata_compat::has_internal_suffix(key, PUBLICATION_SUFFIX)
}
pub fn copy_publication_controls(from: &HashMap<String, String>, to: &mut HashMap<String, String>) -> Result<()> {
    let condition: SourceConditionV1 = decode_metadata(from, MULTIPART_SOURCE_CONDITION_SUFFIX)?.ok_or(DiskError::FileCorrupt)?;
    condition.validate()?;
    let publication: PublicationIdentity = decode_metadata(from, PUBLICATION_SUFFIX)?.ok_or(DiskError::FileCorrupt)?;
    if publication.lineage != condition.lineage
        || publication.source_generation != condition.generation
        || publication.attempt != condition.attempt
    {
        return Err(DiskError::FileCorrupt);
    }
    insert_metadata(to, MULTIPART_SOURCE_CONDITION_SUFFIX, &condition)?;
    insert_metadata(to, PUBLICATION_SUFFIX, &publication)
}
pub fn strip_transport_condition(metadata: &mut HashMap<String, String>) {
    rustfs_utils::http::metadata_compat::remove_str(metadata, MULTIPART_SOURCE_CONDITION_SUFFIX);
}
pub fn strip_native_controls(metadata: &mut HashMap<String, String>) {
    for suffix in [
        MULTIPART_SOURCE_CONDITION_SUFFIX,
        PUBLICATION_SUFFIX,
        STATUS_SUFFIX,
        NATIVE_SOURCE_PROTOCOL_SUFFIX,
    ] {
        rustfs_utils::http::metadata_compat::remove_str(metadata, suffix);
    }
}
pub(super) fn set_source_status(fi: &mut FileInfo, status: &SourceStatus) -> Result<()> {
    insert_metadata(&mut fi.metadata, STATUS_SUFFIX, status)
}
fn insert_metadata(metadata: &mut HashMap<String, String>, suffix: &str, value: &impl Serialize) -> Result<()> {
    let bytes = rmp_serde::to_vec_named(value).map_err(super::Error::other)?;
    if bytes.len() > MULTIPART_SOURCE_MAX_PAYLOAD_BYTES {
        return Err(DiskError::FileCorrupt);
    }
    rustfs_utils::http::metadata_compat::insert_str(
        metadata,
        suffix,
        hex_simd::encode_to_string(bytes, hex_simd::AsciiCase::Lower),
    );
    Ok(())
}
fn decode_metadata<T: serde::de::DeserializeOwned>(metadata: &HashMap<String, String>, suffix: &str) -> Result<Option<T>> {
    use rustfs_utils::http::metadata_compat::{contains_key_str, get_consistent_str};
    let Some(value) = get_consistent_str(metadata, suffix) else {
        return if contains_key_str(metadata, suffix) {
            Err(DiskError::FileCorrupt)
        } else {
            Ok(None)
        };
    };
    if value.len() > MULTIPART_SOURCE_MAX_PAYLOAD_BYTES.saturating_mul(2) {
        return Err(DiskError::FileCorrupt);
    }
    let bytes = hex_simd::decode_to_vec(value).map_err(|_| DiskError::FileCorrupt)?;
    let mut decoder = rmp_serde::Deserializer::new(std::io::Cursor::new(&bytes));
    let value = T::deserialize(&mut decoder).map_err(|_| DiskError::FileCorrupt)?;
    if decoder.position() != bytes.len() as u64 {
        return Err(DiskError::FileCorrupt);
    }
    Ok(Some(value))
}

#[cfg(test)]
mod multipart_proof_authority_tests {
    use super::*;
    use crate::layout::endpoints::{EndpointServerPools, PoolEndpoints};

    fn topology(remote_later_pool: bool) -> EndpointServerPools {
        let pools = (0..if remote_later_pool { 2 } else { 1 })
            .map(|pool| {
                let endpoints = (0..4)
                    .map(|slot| {
                        let mut endpoint = super::super::endpoint::Endpoint::try_from(
                            format!("/tmp/multipart-authority-fixture-{pool}-{slot}").as_str(),
                        )
                        .expect("fixture endpoint should parse");
                        endpoint.set_pool_index(pool);
                        endpoint.set_set_index(0);
                        endpoint.set_disk_index(slot);
                        endpoint.is_local = !(remote_later_pool && pool == 1 && slot == 3);
                        endpoint
                    })
                    .collect::<Vec<_>>();
                PoolEndpoints {
                    legacy: false,
                    set_count: 1,
                    drives_per_set: 4,
                    endpoints: endpoints.into(),
                    cmd_line: String::new(),
                    platform: String::new(),
                }
            })
            .collect();
        EndpointServerPools(pools)
    }

    pub(super) fn local_authority() -> ProofVerificationContext {
        ProofVerificationContext::for_topology(&topology(false))
            .expect("complete local topology")
            .1
    }

    pub(super) fn fixture() -> (MultipartRecoveryRound, MultipartSourceSnapshot) {
        let original_data_dir = Uuid::new_v4();
        let lineage = SourceLineage {
            bucket: "bucket".into(),
            object: "object".into(),
            path: "hash/upload".into(),
            original_data_dir,
            original_version_id: None,
            created_unix_nanos: 1,
            original_identity: [1; 32],
            geometry: SourceGeometry {
                data_blocks: 2,
                parity_blocks: 2,
                block_size: 1024,
                distribution: vec![1, 2, 3, 4],
            },
        };
        let from_generation = SourceGeneration {
            epoch: 0,
            nonce: original_data_dir,
        };
        let generation = SourceGeneration {
            epoch: 1,
            nonce: Uuid::new_v4(),
        };
        let round = MultipartRecoveryRound {
            lineage: lineage.clone(),
            from_generation,
            old_attempt: Uuid::new_v4(),
            generation,
            round: Uuid::new_v4(),
        };
        let snapshot = MultipartSourceSnapshot {
            disk_id: Uuid::new_v4(),
            physical_slot: 0,
            lineage,
            generation,
            revision: 2,
            phase: MultipartSourcePhase::Recovering,
            state_sha256: [2; 32],
            head_sha256: Some([3; 32]),
            inventory_sha256: [4; 32],
            capability_sha256: None,
            publication: None,
        };
        (round, snapshot)
    }

    fn issue_claim(
        authority: &ProofVerificationContext,
        round: MultipartRecoveryRound,
        snapshot: MultipartSourceSnapshot,
    ) -> MultipartSourceProof {
        MultipartSourceProof::issue_with_mac_signer(
            authority,
            MultipartProofKind::Claimed,
            round.round,
            Some(round),
            snapshot,
            |_| panic!("native-only issue must not resolve RPC credentials"),
        )
        .expect("native proof should issue without a key")
    }

    #[test]
    fn multipart_local_proof_never_resolves_rpc_credentials_and_is_instance_scoped() {
        let authority = local_authority();
        let (round, snapshot) = fixture();
        let proof = issue_claim(&authority, round, snapshot);
        assert!(matches!(proof.authentication, ProofAuthentication::NativeLocal));
        proof
            .verify_with_mac_verifier(&authority, |_, _| panic!("native-only verification must not resolve RPC credentials"))
            .expect("native origin should verify");
        assert!(
            proof.verify_with_authority(&local_authority()).is_err(),
            "a separate instance must not share authority"
        );
        assert!(proof.verify().is_err(), "the RPC-facing default must reject local origin");
        assert!(proof.verify_with_authority(&authority).is_ok());
    }

    #[test]
    fn multipart_local_origin_clone_cannot_authenticate_changed_body_fields() {
        let authority = local_authority();
        let (round, snapshot) = fixture();
        let proof = issue_claim(&authority, round, snapshot);
        let mut changed = Vec::new();
        let mut p = proof.clone();
        p.kind = MultipartProofKind::Restored;
        changed.push(p);
        let mut p = proof.clone();
        p.challenge = Uuid::new_v4();
        changed.push(p);
        let mut p = proof.clone();
        p.recovery.as_mut().unwrap().round = Uuid::new_v4();
        changed.push(p);
        let mut p = proof.clone();
        p.snapshot.disk_id = Uuid::new_v4();
        changed.push(p);
        let mut p = proof.clone();
        p.snapshot.physical_slot = 1;
        changed.push(p);
        let mut p = proof.clone();
        p.snapshot.lineage.object.push('x');
        changed.push(p);
        let mut p = proof.clone();
        p.snapshot.generation.epoch += 1;
        changed.push(p);
        let mut p = proof.clone();
        p.snapshot.revision += 1;
        changed.push(p);
        let mut p = proof.clone();
        p.snapshot.phase = MultipartSourcePhase::ConsumedUnsealed;
        changed.push(p);
        let mut p = proof.clone();
        p.snapshot.state_sha256[0] ^= 1;
        changed.push(p);
        let mut p = proof.clone();
        p.snapshot.head_sha256 = None;
        changed.push(p);
        let mut p = proof.clone();
        p.snapshot.inventory_sha256[0] ^= 1;
        changed.push(p);
        let mut p = proof;
        p.snapshot.capability_sha256 = Some([8; 32]);
        changed.push(p);
        for altered in changed {
            assert!(altered.verify_with_authority(&authority).is_err());
        }
    }

    #[test]
    fn multipart_local_proof_deserialize_never_restores_origin_or_wire_authority() {
        let authority = local_authority();
        let (round, snapshot) = fixture();
        let proof = issue_claim(&authority, round, snapshot);
        let encoded = rmp_serde::to_vec_named(&proof).expect("serialize local proof for native persistence");
        let mut decoded: MultipartSourceProof = rmp_serde::from_slice(&encoded).expect("decode local marker");
        assert!(decoded.native_origin.is_none());
        assert!(decoded.verify_with_authority(&authority).is_err());
        assert!(decoded.verify().is_err());
        assert!(decoded.prepare_wire().is_err(), "raw bytes must not become an RPC signing oracle");
        let mut edited = proof;
        edited.snapshot.revision += 1;
        assert!(
            edited.prepare_wire().is_err(),
            "an edited witness must fail before any RPC key resolution"
        );
    }

    fn certificate(authority: &ProofVerificationContext, kind: MultipartProofKind) -> MultipartCertificate {
        let (round, snapshot) = fixture();
        let disk_ids = (0..4).map(|_| Uuid::new_v4()).collect::<Vec<_>>();
        let proofs = disk_ids
            .iter()
            .enumerate()
            .map(|(slot, id)| {
                let mut snapshot = snapshot.clone();
                snapshot.disk_id = *id;
                snapshot.physical_slot = slot;
                if kind == MultipartProofKind::Activated {
                    snapshot.phase = MultipartSourcePhase::Active;
                }
                MultipartSourceProof::issue_with_mac_signer(authority, kind, round.round, Some(round.clone()), snapshot, |_| {
                    panic!("local certificate must not use RPC credentials")
                })
                .expect("issue local certificate fact")
            })
            .collect();
        MultipartCertificate { round, disk_ids, proofs }
    }

    #[test]
    fn multipart_native_certificate_checks_round_slot_and_phase_semantics() {
        let authority = local_authority();
        for kind in [
            MultipartProofKind::Claimed,
            MultipartProofKind::Restored,
            MultipartProofKind::Activated,
        ] {
            let cert = certificate(&authority, kind);
            cert.verify_with_authority(&authority, kind, 4)
                .expect("native certificate should verify");
            assert!(cert.verify_with_authority(&local_authority(), kind, 4).is_err());
            let mut wrong_slot = cert.clone();
            wrong_slot.disk_ids.swap(0, 1);
            assert!(wrong_slot.verify_with_authority(&authority, kind, 4).is_err());
            let mut duplicated = cert.clone();
            duplicated.proofs[1] = duplicated.proofs[0].clone();
            assert!(duplicated.verify_with_authority(&authority, kind, 4).is_err());
            for bad in 0..5 {
                let mut forged = cert.clone();
                let proof = &mut forged.proofs[0];
                match bad {
                    0 => proof.challenge = Uuid::new_v4(),
                    1 => proof.snapshot.phase = MultipartSourcePhase::ConsumedUnsealed,
                    2 => proof.snapshot.generation.epoch += 1,
                    3 => proof.snapshot.head_sha256 = None,
                    _ => proof.snapshot.capability_sha256 = Some([5; 32]),
                }
                // Even an authentic native statement of the wrong semantic
                // class cannot be counted toward this certificate's quorum.
                if let ProofVerificationContext::Local(local) = &authority {
                    proof.attach_native_origin(local).unwrap();
                }
                assert!(forged.verify_with_authority(&authority, kind, 4).is_err());
            }
        }
    }

    #[test]
    fn multipart_rpc_rejects_local_markers_in_every_nested_certificate_vector() {
        let authority = local_authority();
        let cert = certificate(&authority, MultipartProofKind::Claimed);
        let snapshot = cert.proofs[0].snapshot.clone();
        let mut request = MultipartSourceRequest {
            protocol_version: MULTIPART_SOURCE_PROTOCOL_VERSION,
            path: cert.round.lineage.path.clone(),
            operation: MultipartSourceOperation::Restore {
                expected: snapshot.clone(),
                certificate: cert.clone(),
            },
        };
        let bytes = rmp_serde::to_vec_named(&request).unwrap();
        request = rmp_serde::from_slice(&bytes).unwrap();
        assert!(request.validate_wire().is_err());
        assert!(request.prepare_wire().is_err());
        for nested in 0..3 {
            let inspection = MultipartSourceInspection {
                disk_id: snapshot.disk_id,
                snapshot: Some(snapshot.clone()),
                original: None,
                recovery: Some(cert.round.clone()),
                negative_certificate: (nested == 0).then(|| cert.clone()),
                ready_certificate: (nested == 1).then(|| cert.clone()),
                terminal_certificate: if nested == 2 { cert.proofs.clone() } else { Vec::new() },
                root_missing: false,
                abort_round: None,
                abort_certificate: None,
            };
            let response = MultipartSourceResponse::Inspected(inspection);
            let bytes = rmp_serde::to_vec_named(&response).unwrap();
            let mut decoded: MultipartSourceResponse = rmp_serde::from_slice(&bytes).unwrap();
            assert!(decoded.validate_wire().is_err());
            assert!(decoded.prepare_wire().is_err());
        }
    }

    #[test]
    fn multipart_native_scope_requires_complete_topology_and_rejects_remote_later_pool() {
        let (local_id, local) = ProofVerificationContext::for_topology(&topology(false)).unwrap();
        assert!(matches!(local, ProofVerificationContext::Local(_)));
        let (_, remote) = ProofVerificationContext::for_topology(&topology(true)).unwrap();
        assert!(matches!(remote, ProofVerificationContext::RpcOnly));
        let mut incomplete = topology(false);
        incomplete.0[0].endpoints.as_mut().pop();
        assert!(ProofVerificationContext::for_topology(&incomplete).is_err());
        let mut duplicate = topology(false);
        duplicate.0[0].endpoints.as_mut()[1] = duplicate.0[0].endpoints.as_ref()[0].clone();
        assert!(ProofVerificationContext::for_topology(&duplicate).is_err());
        assert!(ProofVerificationContext::for_topology(&EndpointServerPools::default()).is_err());
        let mut foreign = topology(false).0[0].endpoints.as_ref()[0].clone();
        foreign.set_pool_index(5);
        assert!(local.validate_endpoint(&foreign).is_err());
        let (other_id, other) = ProofVerificationContext::for_topology(&topology(false)).unwrap();
        assert_eq!(local_id, other_id);
        assert!(
            !local.same_authority(&other),
            "equivalent topology in another instance is a different authority"
        );
    }

    #[test]
    fn multipart_native_publication_binds_capability_without_rpc_credentials() {
        let authority = local_authority();
        let (_, mut snapshot) = fixture();
        let publication = PublicationIdentity {
            lineage: snapshot.lineage.clone(),
            source_generation: snapshot.generation,
            attempt: Uuid::new_v4(),
            version_id: None,
            final_data_dir: Uuid::new_v4(),
            modified_unix_nanos: 2,
            completion_identity: [6; 32],
        };
        let capability = MultipartUndoCapability::new();
        snapshot.phase = MultipartSourcePhase::ConsumedUnsealed;
        snapshot.head_sha256 = None;
        snapshot.capability_sha256 = Some(capability.digest());
        snapshot.publication = Some(publication.clone());
        let proof = MultipartSourceProof::issue_with_mac_signer(
            &authority,
            MultipartProofKind::Published,
            publication.attempt,
            None,
            snapshot,
            |_| panic!("native publication must not resolve RPC credentials"),
        )
        .unwrap();
        let mut result = MultipartPublicationResult {
            publication,
            proof,
            undo_capability: capability,
        };
        result
            .verify_with_authority(&authority)
            .expect("local publication result should verify");
        assert!(result.validate_wire().is_err());
        let bytes = rmp_serde::to_vec_named(&result).unwrap();
        let mut decoded: MultipartPublicationResult = rmp_serde::from_slice(&bytes).unwrap();
        assert!(decoded.verify_with_authority(&authority).is_err());
        assert!(decoded.prepare_wire().is_err());
        result.undo_capability = MultipartUndoCapability::new();
        assert!(
            result.verify_with_authority(&authority).is_err(),
            "the response must own the exact native capability"
        );
        assert!(format!("{:?}", result.undo_capability).contains("<redacted>"));
    }

    fn persist_null_fixture(fi: &FileInfo) -> FileInfo {
        use rustfs_filemeta::{FileInfoOpts, FileMeta, get_file_info};
        let mut metadata = FileMeta::new();
        metadata.add_version(fi.clone()).expect("persist native fixture metadata");
        get_file_info(
            &metadata.marshal_msg().expect("serialize actual xl.meta"),
            "bucket",
            "object",
            &fi.version_id.unwrap_or_default().to_string(),
            FileInfoOpts {
                data: false,
                include_free_versions: false,
                include_part_checksums: true,
            },
        )
        .expect("read the explicit stored version")
    }

    #[test]
    fn multipart_null_version_filemeta_roundtrip_preserves_publication_hash_and_native_proof() {
        let authority = local_authority();
        let mut original = FileInfo::new("object", 2, 2);
        original.erasure.index = 1;
        original.data_dir = Some(Uuid::new_v4());
        original.mod_time = Some(time::OffsetDateTime::now_utc());
        original.version_id = Some(Uuid::nil());
        let attempt = Uuid::new_v4();
        let condition = capture_source_condition("bucket", "object", "hash/upload", &original, attempt).unwrap();
        assert_eq!(condition.lineage.original_version_id, None);
        assert_eq!(
            original.version_id,
            Some(Uuid::nil()),
            "capturing native identity must preserve the incoming FI"
        );
        let read_original = persist_null_fixture(&original);
        assert_eq!(read_original.version_id, None, "the real FileMeta decoder canonicalizes null");
        assert!(condition.matches_original(&read_original).unwrap());
        let reread_condition = capture_source_condition("bucket", "object", "hash/upload", &read_original, attempt).unwrap();
        assert_eq!(condition, reread_condition);

        let mut completed = original.clone();
        completed.data_dir = Some(Uuid::new_v4());
        completed.mod_time = Some(original.mod_time.unwrap() + time::Duration::seconds(1));
        completed.size = 15;
        completed.add_object_part(1, "etag".into(), 15, completed.mod_time, 15, None, None);
        let publication = condition.attach(&mut completed).unwrap();
        assert_eq!(publication.version_id, None);
        assert_eq!(
            completed.version_id,
            Some(Uuid::nil()),
            "native controls must preserve replicated-null FI semantics"
        );
        let mut read_completed = persist_null_fixture(&completed);
        assert_eq!(read_completed.version_id, None);
        let reread_publication = reread_condition.attach(&mut read_completed).unwrap();
        assert_eq!(
            publication, reread_publication,
            "publication identity and completion hash must survive actual xl.meta decoding"
        );

        let (_, mut snapshot) = fixture();
        let capability = MultipartUndoCapability::new();
        snapshot.lineage = condition.lineage;
        snapshot.generation = condition.generation;
        snapshot.phase = MultipartSourcePhase::ConsumedUnsealed;
        snapshot.head_sha256 = None;
        snapshot.capability_sha256 = Some(capability.digest());
        snapshot.publication = Some(publication);
        let proof = MultipartSourceProof::issue_with_mac_signer(
            &authority,
            MultipartProofKind::Published,
            attempt,
            None,
            snapshot.clone(),
            |_| panic!("local native proof must not resolve RPC credentials"),
        )
        .unwrap();
        snapshot.publication = Some(reread_publication);
        let reread_proof = MultipartSourceProof::issue_with_mac_signer(
            &authority,
            MultipartProofKind::Published,
            attempt,
            None,
            snapshot,
            |_| panic!("local native proof must not resolve RPC credentials"),
        )
        .unwrap();
        assert_eq!(proof.body().unwrap(), reread_proof.body().unwrap());
        proof.verify_with_authority(&authority).unwrap();
        reread_proof.verify_with_authority(&authority).unwrap();
    }

    #[test]
    fn multipart_native_version_normalization_preserves_concrete_versions_and_rejects_nil_data_dir() {
        let mut original = FileInfo::new("object", 2, 2);
        original.erasure.index = 1;
        original.data_dir = Some(Uuid::new_v4());
        original.mod_time = Some(time::OffsetDateTime::now_utc());
        let concrete = Uuid::new_v4();
        original.version_id = Some(concrete);
        let condition = capture_source_condition("bucket", "object", "hash/upload", &original, Uuid::new_v4()).unwrap();
        assert_eq!(condition.lineage.original_version_id, Some(concrete));
        let mut completed = original.clone();
        completed.data_dir = Some(Uuid::new_v4());
        let publication = condition.attach(&mut completed).unwrap();
        assert_eq!(publication.version_id, Some(concrete));
        let mut null_completed = completed.clone();
        null_completed.version_id = Some(Uuid::nil());
        let null_publication = condition.attach(&mut null_completed).unwrap();
        assert_eq!(null_publication.version_id, None);
        assert_ne!(publication.completion_identity, null_publication.completion_identity);
        original.data_dir = Some(Uuid::nil());
        assert!(capture_source_condition("bucket", "object", "hash/upload", &original, Uuid::new_v4()).is_err());
        completed.data_dir = Some(Uuid::nil());
        assert!(condition.attach(&mut completed).is_err());
    }

    #[test]
    fn multipart_completion_tag_update_preserves_content_proof_but_not_source_identity() {
        use rustfs_utils::http::headers::AMZ_OBJECT_TAGGING;
        let authority = local_authority();
        let mut source = FileInfo::new("object", 2, 2);
        source.erasure.index = 1;
        source.data_dir = Some(Uuid::new_v4());
        source.mod_time = Some(time::OffsetDateTime::now_utc());
        source.metadata.insert(AMZ_OBJECT_TAGGING.to_string(), "tag=old".to_string());
        let condition = capture_source_condition("bucket", "object", "hash/upload", &source, Uuid::new_v4()).unwrap();
        let mut changed_source = source.clone();
        changed_source
            .metadata
            .insert(AMZ_OBJECT_TAGGING.to_string(), "tag=new".to_string());
        assert!(
            !condition.matches_original(&changed_source).unwrap(),
            "source generation identity still binds every original tag"
        );

        let mut completed = source.clone();
        completed.data_dir = Some(Uuid::new_v4());
        let before = condition.attach(&mut completed).unwrap();
        let mut tagged = completed.clone();
        tagged.metadata.insert(AMZ_OBJECT_TAGGING.to_string(), "tag=new".to_string());
        let after = condition.attach(&mut tagged).unwrap();
        assert_eq!(before, after);
        assert_eq!(tagged.metadata.get(AMZ_OBJECT_TAGGING).map(String::as_str), Some("tag=new"));
        assert_eq!(completed.mod_time, tagged.mod_time);
        assert_eq!(completed.data_dir, tagged.data_dir);
        assert_eq!(completed.version_id, tagged.version_id);
        assert_eq!(completed.parts, tagged.parts);

        let (_, mut snapshot) = fixture();
        snapshot.lineage = condition.lineage;
        snapshot.generation = condition.generation;
        snapshot.phase = MultipartSourcePhase::LocalTerminalSeal;
        snapshot.head_sha256 = None;
        snapshot.publication = Some(before);
        let first = MultipartSourceProof::issue_with_mac_signer(
            &authority,
            MultipartProofKind::TerminalTarget,
            condition.attempt,
            None,
            snapshot.clone(),
            |_| panic!("local proof must not read RPC credentials"),
        )
        .unwrap();
        snapshot.publication = Some(after);
        let second = MultipartSourceProof::issue_with_mac_signer(
            &authority,
            MultipartProofKind::TerminalTarget,
            condition.attempt,
            None,
            snapshot,
            |_| panic!("local proof must not read RPC credentials"),
        )
        .unwrap();
        assert_eq!(first.body().unwrap(), second.body().unwrap());
        first.verify_with_authority(&authority).unwrap();
        second.verify_with_authority(&authority).unwrap();
        assert_ne!(
            digest_value(&completed).unwrap(),
            digest_value(&tagged).unwrap(),
            "the complete target still changes, so its separate exact-target Undo condition must remain strict"
        );
    }

    fn mutable_projection_fixture() -> FileInfo {
        let mut fi = FileInfo::new("object", 2, 2);
        fi.erasure.index = 1;
        fi.data_dir = Some(Uuid::new_v4());
        fi.mod_time = Some(time::OffsetDateTime::now_utc());
        fi.version_id = Some(Uuid::new_v4());
        fi
    }

    fn assert_mutable_metadata_preserves_only_completion_identity(before: &FileInfo, after: &FileInfo) {
        let authority = local_authority();
        let condition = capture_source_condition("bucket", "object", "hash/upload", before, Uuid::new_v4()).unwrap();
        assert!(
            !condition.matches_original(after).unwrap(),
            "original source identity must continue to bind the mutated metadata"
        );
        let mut completed_before = before.clone();
        let mut completed_after = after.clone();
        let final_dir = Uuid::new_v4();
        completed_before.data_dir = Some(final_dir);
        completed_after.data_dir = Some(final_dir);
        let before_publication = condition.attach(&mut completed_before).unwrap();
        let after_publication = condition.attach(&mut completed_after).unwrap();
        assert_eq!(before_publication, after_publication);
        let (_, mut snapshot) = fixture();
        snapshot.lineage = condition.lineage;
        snapshot.generation = condition.generation;
        snapshot.phase = MultipartSourcePhase::LocalTerminalSeal;
        snapshot.head_sha256 = None;
        snapshot.publication = Some(before_publication);
        let before_proof = MultipartSourceProof::issue(
            &authority,
            MultipartProofKind::TerminalTarget,
            condition.attempt,
            None,
            snapshot.clone(),
        )
        .unwrap();
        snapshot.publication = Some(after_publication);
        let after_proof =
            MultipartSourceProof::issue(&authority, MultipartProofKind::TerminalTarget, condition.attempt, None, snapshot)
                .unwrap();
        assert_eq!(before_proof.body().unwrap(), after_proof.body().unwrap());
        before_proof.verify_with_authority(&authority).unwrap();
        after_proof.verify_with_authority(&authority).unwrap();
        assert_ne!(
            digest_value(&completed_before).unwrap(),
            digest_value(&completed_after).unwrap(),
            "the whole target identity used by conditional Undo remains different"
        );
    }

    #[test]
    fn multipart_completion_retention_and_legal_hold_updates_preserve_content_identity_only() {
        use rustfs_filemeta::metadata_keys::{OBJECT_LOCK_LEGAL_HOLD, OBJECT_LOCK_MODE, OBJECT_LOCK_RETAIN_UNTIL_DATE};
        use rustfs_utils::http::{SUFFIX_OBJECTLOCK_LEGALHOLD_TIMESTAMP, SUFFIX_OBJECTLOCK_RETENTION_TIMESTAMP, insert_str};
        for retention in [false, true] {
            let before = mutable_projection_fixture();
            let mut after = before.clone();
            if retention {
                after.metadata.insert(OBJECT_LOCK_MODE.into(), "GOVERNANCE".into());
                after
                    .metadata
                    .insert(OBJECT_LOCK_RETAIN_UNTIL_DATE.into(), "2030-01-01T00:00:00Z".into());
                insert_str(&mut after.metadata, SUFFIX_OBJECTLOCK_RETENTION_TIMESTAMP, "2026-01-01T00:00:00Z".into());
            } else {
                after.metadata.insert(OBJECT_LOCK_LEGAL_HOLD.into(), "ON".into());
                insert_str(&mut after.metadata, SUFFIX_OBJECTLOCK_LEGALHOLD_TIMESTAMP, "2026-01-01T00:00:00Z".into());
            }
            after.acknowledge_data_movement();
            assert_mutable_metadata_preserves_only_completion_identity(&before, &after);
        }
    }

    #[test]
    fn multipart_completion_replication_writeback_normalizes_exact_metadata_and_derived_mirror() {
        use rustfs_utils::http::{
            SUFFIX_REPLICATION_GENERATION, SUFFIX_REPLICATION_STATUS, SUFFIX_REPLICATION_TARGET_VERSION_ARN_PREFIX,
            SUFFIX_REPLICATION_TIMESTAMP, SUFFIX_TAGGING_TIMESTAMP, insert_str,
        };
        let mut pending = mutable_projection_fixture();
        insert_str(&mut pending.metadata, SUFFIX_REPLICATION_GENERATION, Uuid::new_v4().to_string());
        insert_str(&mut pending.metadata, SUFFIX_REPLICATION_TIMESTAMP, "2026-01-01T00:00:00Z".into());
        insert_str(&mut pending.metadata, SUFFIX_REPLICATION_STATUS, "arn:test=PENDING;".into());
        pending.replication_state_internal = rustfs_filemeta::get_internal_replication_state(&pending.metadata);
        let mut completed = pending.clone();
        insert_str(&mut completed.metadata, SUFFIX_REPLICATION_STATUS, "arn:test=COMPLETED;".into());
        insert_str(
            &mut completed.metadata,
            &format!("{SUFFIX_REPLICATION_TARGET_VERSION_ARN_PREFIX}arn:test"),
            Uuid::new_v4().to_string(),
        );
        completed.replication_state_internal = rustfs_filemeta::get_internal_replication_state(&completed.metadata);
        completed.acknowledge_data_movement();
        assert_mutable_metadata_preserves_only_completion_identity(&pending, &completed);

        let before_tag = completed.clone();
        let mut retagged = before_tag.clone();
        retagged
            .metadata
            .insert(rustfs_utils::http::headers::AMZ_OBJECT_TAGGING.into(), "changed=tag".into());
        insert_str(&mut retagged.metadata, SUFFIX_REPLICATION_GENERATION, Uuid::new_v4().to_string());
        insert_str(&mut retagged.metadata, SUFFIX_REPLICATION_TIMESTAMP, "2026-01-02T00:00:00Z".into());
        insert_str(&mut retagged.metadata, SUFFIX_REPLICATION_STATUS, "arn:test=PENDING;".into());
        insert_str(&mut retagged.metadata, SUFFIX_TAGGING_TIMESTAMP, "2026-01-02T00:00:00Z".into());
        retagged.replication_state_internal = rustfs_filemeta::get_internal_replication_state(&retagged.metadata);
        assert_mutable_metadata_preserves_only_completion_identity(&before_tag, &retagged);

        // Replica state is a different category with no demonstrated in-place
        // producer in this projection, so it must remain an identity change.
        let mut replica_changed = completed.clone();
        replica_changed.replication_state_internal.as_mut().unwrap().replica_timestamp = Some(time::OffsetDateTime::now_utc());
        assert_ne!(
            digest_value(&normalized_info(&completed, true)).unwrap(),
            digest_value(&normalized_info(&replica_changed, true)).unwrap()
        );
    }

    #[test]
    fn multipart_completion_data_movement_ack_preserves_lineage_and_unknown_flags() {
        use rustfs_utils::http::{SUFFIX_DATA_MOVED, SUFFIX_DATA_MOVEMENT_UPLOAD, insert_str};
        let before = mutable_projection_fixture();
        let mut empty_ack = before.clone();
        empty_ack.acknowledge_data_movement();
        assert_mutable_metadata_preserves_only_completion_identity(&before, &empty_ack);
        let mut moved = before.clone();
        insert_str(&mut moved.metadata, SUFFIX_DATA_MOVEMENT_UPLOAD, "v1:transfer-owned-lineage".into());
        moved.set_data_moved();
        let mut acknowledged = moved.clone();
        acknowledged.acknowledge_data_movement();
        assert_mutable_metadata_preserves_only_completion_identity(&moved, &acknowledged);
        let mut foreign_transfer = acknowledged.clone();
        insert_str(&mut foreign_transfer.metadata, SUFFIX_DATA_MOVEMENT_UPLOAD, "v1:other-lineage".into());
        assert_ne!(
            digest_value(&normalized_info(&acknowledged, true)).unwrap(),
            digest_value(&normalized_info(&foreign_transfer, true)).unwrap()
        );
        let mut unknown = before.clone();
        insert_str(&mut unknown.metadata, SUFFIX_DATA_MOVED, "unknown".into());
        assert_ne!(
            digest_value(&normalized_info(&before, true)).unwrap(),
            digest_value(&normalized_info(&unknown, true)).unwrap()
        );
    }

    #[test]
    fn multipart_completion_projection_keeps_conflicting_aliases_and_immutable_content_bound() {
        use rustfs_utils::http::{
            MINIO_INTERNAL_PREFIX, SUFFIX_DATA_MOVED, SUFFIX_REPLICATION_STATUS, SUFFIX_REPLICATION_TARGET_VERSION_ARN_PREFIX,
            insert_str,
        };
        let before = mutable_projection_fixture();
        let expected = digest_value(&normalized_info(&before, true)).unwrap();
        let mut invalids = Vec::new();
        let mut fi = before.clone();
        fi.data_dir = Some(Uuid::new_v4());
        invalids.push(fi);
        let mut fi = before.clone();
        fi.mod_time = Some(fi.mod_time.unwrap() + time::Duration::seconds(1));
        invalids.push(fi);
        let mut fi = before.clone();
        fi.version_id = Some(Uuid::new_v4());
        invalids.push(fi);
        let mut fi = before.clone();
        fi.size += 1;
        invalids.push(fi);
        let mut fi = before.clone();
        fi.metadata.insert("content-type".into(), "text/plain".into());
        invalids.push(fi);
        let mut fi = before.clone();
        fi.metadata.insert("x-rustfs-encryption-original-size".into(), "100".into());
        invalids.push(fi);
        let mut fi = before.clone();
        fi.metadata.insert("x-amz-server-side-encryption".into(), "AES256".into());
        invalids.push(fi);
        let mut fi = before.clone();
        insert_str(&mut fi.metadata, rustfs_utils::http::SUFFIX_COMPRESSION_SIZE, "100".into());
        invalids.push(fi);
        let mut fi = before.clone();
        insert_str(&mut fi.metadata, SUFFIX_REPLICATION_STATUS, "arn:test=PENDING;".into());
        fi.metadata.insert(
            format!("{MINIO_INTERNAL_PREFIX}{SUFFIX_REPLICATION_STATUS}"),
            "arn:test=COMPLETED;".into(),
        );
        invalids.push(fi);
        let mut fi = before.clone();
        insert_str(&mut fi.metadata, SUFFIX_DATA_MOVED, "true".into());
        fi.metadata
            .insert(format!("{MINIO_INTERNAL_PREFIX}{SUFFIX_DATA_MOVED}"), String::new());
        invalids.push(fi);
        let mut fi = before;
        let suffix = format!("{SUFFIX_REPLICATION_TARGET_VERSION_ARN_PREFIX}arn:test");
        insert_str(&mut fi.metadata, &suffix, "version-a".into());
        fi.metadata
            .insert(format!("{MINIO_INTERNAL_PREFIX}{suffix}"), "version-b".into());
        invalids.push(fi);
        for invalid in invalids {
            assert_ne!(
                expected,
                digest_value(&normalized_info(&invalid, true)).unwrap(),
                "unproven content fields and conflicting aliases must not be projected away"
            );
        }
    }

    #[test]
    fn multipart_nanoseconds_preserve_encoding_in_tagged_request_and_response_roundtrips() {
        #[derive(Serialize, Deserialize)]
        struct Nanoseconds(#[serde(deserialize_with = "deserialize_native_nanoseconds")] i128);
        let authority = local_authority();
        for nanos in [
            i128::MIN,
            -1,
            0,
            time::OffsetDateTime::now_utc().unix_timestamp_nanos(),
            i128::MAX,
        ] {
            assert_eq!(
                rmp_serde::to_vec(&Nanoseconds(nanos)).unwrap(),
                rmp_serde::to_vec(&nanos).unwrap(),
                "custom decoding must not alter the signed/persisted wire encoding"
            );
            let (mut round, mut snapshot) = fixture();
            round.lineage.created_unix_nanos = nanos;
            snapshot.lineage = round.lineage.clone();
            let request = MultipartSourceRequest {
                protocol_version: MULTIPART_SOURCE_PROTOCOL_VERSION,
                path: round.lineage.path.clone(),
                operation: MultipartSourceOperation::Claim {
                    expected: snapshot.clone(),
                    round: round.clone(),
                },
            };
            let bytes = rmp_serde::to_vec_named(&request).unwrap();
            let decoded: MultipartSourceRequest =
                rmp_serde::from_slice(&bytes).expect("tagged native request must decode nanoseconds");
            let MultipartSourceOperation::Claim {
                expected,
                round: actual_round,
            } = decoded.operation
            else {
                panic!("Claim variant must be preserved")
            };
            assert_eq!(expected, snapshot);
            assert_eq!(actual_round, round);
            let publication = PublicationIdentity {
                lineage: round.lineage.clone(),
                source_generation: snapshot.generation,
                attempt: Uuid::new_v4(),
                version_id: None,
                final_data_dir: Uuid::new_v4(),
                modified_unix_nanos: nanos,
                completion_identity: [6; 32],
            };
            snapshot.publication = Some(publication.clone());
            let response = MultipartSourceResponse::TerminalConfirmed(
                MultipartSourceProof::issue(
                    &authority,
                    MultipartProofKind::TerminalTarget,
                    Uuid::new_v4(),
                    None,
                    snapshot.clone(),
                )
                .unwrap(),
            );
            let bytes = rmp_serde::to_vec_named(&response).unwrap();
            let decoded: MultipartSourceResponse =
                rmp_serde::from_slice(&bytes).expect("tagged native response must decode nanoseconds");
            let MultipartSourceResponse::TerminalConfirmed(proof) = decoded else {
                panic!("terminal result must be preserved")
            };
            assert_eq!(proof.snapshot, snapshot);
            assert!(
                proof.verify_with_authority(&authority).is_err(),
                "decoding must not restore the native witness"
            );
        }
        for length in [0, 15, 17] {
            let bytes = rmp_serde::to_vec(&Bytes::from(vec![0; length])).unwrap();
            assert!(
                rmp_serde::from_slice::<Nanoseconds>(&bytes).is_err(),
                "nanoseconds require exactly 16 binary bytes"
            );
        }
    }

    #[test]
    fn multipart_completion_part_timestamp_is_transient_but_original_source_generation_stays_bound() {
        let mut source = mutable_projection_fixture();
        source.size = 15;
        source.add_object_part(1, "etag".into(), 15, source.mod_time, 15, None, None);
        let condition = capture_source_condition("bucket", "object", "hash/upload", &source, Uuid::new_v4()).unwrap();
        let mut newer_source = source.clone();
        newer_source.parts[0].mod_time = Some(source.mod_time.unwrap() + time::Duration::seconds(1));
        assert!(
            !condition.matches_original(&newer_source).unwrap(),
            "source FI still binds the exact part generation"
        );
        let mut completed = source;
        completed.data_dir = Some(Uuid::new_v4());
        let expected = condition.attach(&mut completed).unwrap();
        let mut reread = persist_null_fixture(&completed);
        assert!(reread.parts[0].mod_time.is_none(), "native FileMeta does not persist staging part mtimes");
        assert_eq!(condition.attach(&mut reread).unwrap(), expected);
    }

    #[test]
    fn multipart_completion_checksum_mirrors_roundtrip_without_dropping_authoritative_records() {
        use rustfs_utils::http::{SUFFIX_PART_CHECKSUMS, insert_str};
        for persisted_part_map in [false, true] {
            let mut source = mutable_projection_fixture();
            source.size = 15;
            source.add_object_part(1, "etag".into(), 15, source.mod_time, 15, None, None);
            let condition = capture_source_condition("bucket", "object", "hash/upload", &source, Uuid::new_v4()).unwrap();
            let mut completed = source.clone();
            completed.data_dir = Some(Uuid::new_v4());
            if persisted_part_map {
                insert_str(&mut completed.metadata, SUFFIX_PART_CHECKSUMS, r#"[[1,[["CRC32C","AAAAAA=="]]]]"#.into());
                assert!(completed.parts[0].checksums.is_none());
            } else {
                completed.parts[0].checksums = Some(HashMap::from([("CRC32C".into(), "AAAAAA==".into())]));
                completed.checksum = Some(
                    rustfs_rio::Checksum::new_from_data(rustfs_rio::ChecksumType::CRC32C, b"checksum bytes")
                        .expect("real object checksum")
                        .to_bytes(&[]),
                );
            }
            let expected = condition.attach(&mut completed).unwrap();
            let mut reread = persist_null_fixture(&completed);
            assert_eq!(
                reread.checksum, completed.checksum,
                "the exact persisted object checksum stays authoritative"
            );
            assert_eq!(
                reread.parts[0].checksums.is_some(),
                persisted_part_map,
                "native part checksums are derived only from their persisted metadata record"
            );
            assert_eq!(condition.attach(&mut reread).unwrap(), expected);
            if persisted_part_map {
                insert_str(&mut reread.metadata, SUFFIX_PART_CHECKSUMS, r#"[[1,[["CRC32C","AQAAAA=="]]]]"#.into());
            } else {
                let mut bytes = reread.checksum.as_ref().unwrap().to_vec();
                bytes[0] ^= 1;
                reread.checksum = Some(Bytes::from(bytes));
            }
            assert_ne!(
                condition.attach(&mut reread).unwrap().completion_identity,
                expected.completion_identity,
                "changing either authoritative checksum representation must change content identity"
            );
            let mut changed_source = source;
            changed_source.parts[0].checksums = Some(HashMap::from([("CRC32C".into(), "AQAAAA==".into())]));
            assert!(
                !condition.matches_original(&changed_source).unwrap(),
                "original source identity remains complete"
            );
        }
    }

    #[test]
    fn multipart_completion_replication_public_mirror_and_crc_aliases_are_exact_projections() {
        use rustfs_filemeta::metadata_keys::REPLICATION_STATUS;
        use rustfs_utils::http::{MINIO_INTERNAL_PREFIX, SUFFIX_CRC, SUFFIX_REPLICATION_STATUS, insert_str};
        let mut source = mutable_projection_fixture();
        insert_str(&mut source.metadata, SUFFIX_REPLICATION_STATUS, "arn:test=PENDING;".into());
        source.replication_state_internal = rustfs_filemeta::get_internal_replication_state(&source.metadata);
        assert!(!source.metadata.contains_key(REPLICATION_STATUS));
        let condition = capture_source_condition("bucket", "object", "hash/upload", &source, Uuid::new_v4()).unwrap();
        let mut completed = source.clone();
        completed.data_dir = Some(Uuid::new_v4());
        let expected = condition.attach(&mut completed).unwrap();
        let mut reread = persist_null_fixture(&completed);
        assert_eq!(
            reread.metadata.get(REPLICATION_STATUS).map(String::as_str),
            Some("PENDING"),
            "the actual FileMeta reader adds the public composite status mirror"
        );
        assert_eq!(condition.attach(&mut reread).unwrap(), expected);

        let mut independent_header = mutable_projection_fixture();
        let unchanged = digest_value(&normalized_info(&independent_header, true)).unwrap();
        independent_header
            .metadata
            .insert(REPLICATION_STATUS.into(), "PENDING".into());
        assert_ne!(
            digest_value(&normalized_info(&independent_header, true)).unwrap(),
            unchanged,
            "a standalone header without the corresponding internal authority is not ignored"
        );
        let mut checksum = mutable_projection_fixture();
        checksum.checksum = Some(Bytes::from_static(b"actual-checksum"));
        let hash = digest_value(&normalized_info(&checksum, true)).unwrap();
        insert_str(&mut checksum.metadata, SUFFIX_CRC, "actual-checksum".into());
        assert_eq!(digest_value(&normalized_info(&checksum, true)).unwrap(), hash);
        checksum
            .metadata
            .insert(format!("{MINIO_INTERNAL_PREFIX}{SUFFIX_CRC}"), "different-checksum".into());
        assert_ne!(
            digest_value(&normalized_info(&checksum, true)).unwrap(),
            hash,
            "mismatched or conflicting checksum metadata must remain an identity mismatch"
        );
    }

    #[tokio::test]
    async fn multipart_bound_native_handle_uses_same_authority_without_endpoint_lock() {
        let directory = tempfile::tempdir().unwrap();
        let mut endpoint = super::super::endpoint::Endpoint::try_from(directory.path().to_str().unwrap()).unwrap();
        endpoint.set_pool_index(0);
        endpoint.set_set_index(0);
        endpoint.set_disk_index(0);
        let topology = EndpointServerPools(vec![PoolEndpoints {
            legacy: false,
            set_count: 1,
            drives_per_set: 1,
            endpoints: vec![endpoint.clone()].into(),
            cmd_line: String::new(),
            platform: String::new(),
        }]);
        let (_, authority) = ProofVerificationContext::for_topology(&topology).unwrap();
        let (_, foreign) = ProofVerificationContext::for_topology(&topology).unwrap();
        let disk = Arc::new(super::super::local::LocalDisk::new(&endpoint, false).await.unwrap());
        disk.bind_multipart_proof_authority(&authority).unwrap();
        let ProofVerificationContext::Local(local) = &authority else { panic!("local fixture authority") };
        let ProofVerificationContext::Local(other) = &foreign else { panic!("foreign local authority") };
        let worker_authority = authority.clone();
        let worker_foreign = foreign.clone();
        std::thread::scope(|scope| {
            let local_lock = local.endpoints.write();
            let foreign_lock = other.endpoints.write();
            let (tx, rx) = std::sync::mpsc::channel();
            let worker = scope.spawn(move || {
                let result = disk.bind_multipart_proof_authority(&worker_authority);
                let foreign_rejected = disk.bind_multipart_proof_authority(&worker_foreign).is_err();
                tx.send((result, foreign_rejected)).unwrap();
            });
            let result = rx.recv_timeout(std::time::Duration::from_secs(2));
            drop(local_lock);
            drop(foreign_lock);
            worker.join().unwrap();
            let (same, foreign_rejected) = result.expect("bound handle must not acquire the endpoint registration lock");
            same.unwrap();
            assert!(foreign_rejected, "a distinct instance must still be rejected");
        });
    }

    #[test]
    fn multipart_rpc_only_proof_does_not_fallback_when_its_signer_is_unavailable() {
        let (round, snapshot) = fixture();
        let result = MultipartSourceProof::issue_with_mac_signer(
            &ProofVerificationContext::RpcOnly,
            MultipartProofKind::Claimed,
            round.round,
            Some(round),
            snapshot,
            |_| Err(DiskError::UnsupportedDisk),
        );
        assert!(matches!(result, Err(DiskError::UnsupportedDisk)));
    }
}

#[cfg(test)]
mod boxed_wire_layout_tests {
    use super::multipart_proof_authority_tests::{fixture, local_authority};
    use super::*;

    #[derive(Serialize)]
    #[serde(tag = "operation", rename_all = "snake_case")]
    enum UnboxedOperation<'a> {
        UndoAndClaim {
            expected: &'a MultipartSourceSnapshot,
            publication: &'a PublicationIdentity,
            capability: &'a MultipartUndoCapability,
            round: &'a MultipartRecoveryRound,
        },
    }

    #[test]
    fn boxed_failed_undo_expected_preserves_exact_legacy_msgpack_and_json_bytes() {
        let (round, snapshot) = fixture();
        let publication = PublicationIdentity {
            lineage: round.lineage.clone(),
            source_generation: round.from_generation,
            attempt: round.old_attempt,
            version_id: Some(Uuid::new_v4()),
            final_data_dir: Uuid::new_v4(),
            modified_unix_nanos: 1,
            completion_identity: [7; 32],
        };
        let operation = MultipartSourceOperation::UndoAndClaim {
            expected: Box::new(snapshot),
            publication,
            capability: MultipartUndoCapability::new(),
            round,
        };
        let MultipartSourceOperation::UndoAndClaim {
            expected,
            publication,
            capability,
            round,
        } = &operation
        else {
            unreachable!()
        };
        let legacy = UnboxedOperation::UndoAndClaim {
            expected,
            publication,
            capability,
            round,
        };
        let encoded = rmp_serde::to_vec_named(&operation).expect("boxed operation encoding");
        assert_eq!(encoded, rmp_serde::to_vec_named(&legacy).expect("old unboxed operation encoding"));
        assert_eq!(serde_json::to_vec(&operation).unwrap(), serde_json::to_vec(&legacy).unwrap());
        let decoded: MultipartSourceOperation = rmp_serde::from_slice(&encoded).expect("existing wire bytes remain accepted");
        assert_eq!(rmp_serde::to_vec_named(&decoded).unwrap(), encoded);
    }

    #[derive(Serialize)]
    struct UnboxedRenameResponse<'a> {
        #[serde(skip_serializing_if = "Option::is_none")]
        multipart_source: Option<&'a MultipartPublicationResult>,
        old_data_dir: Option<Uuid>,
        rollback_data_dir: Option<Uuid>,
        cleanup_data_dir: Option<Uuid>,
        sign: Option<&'a Vec<u8>>,
        old_current_size: Option<super::super::OldCurrentSize>,
        old_current_source_checked: bool,
        old_current_source: Option<&'a FileInfo>,
    }

    #[test]
    fn boxed_native_result_preserves_response_bytes_and_linear_capability() {
        let authority = local_authority();
        let (round, mut snapshot) = fixture();
        let publication = PublicationIdentity {
            lineage: round.lineage,
            source_generation: snapshot.generation,
            attempt: Uuid::new_v4(),
            version_id: None,
            final_data_dir: Uuid::new_v4(),
            modified_unix_nanos: 1,
            completion_identity: [9; 32],
        };
        let capability = MultipartUndoCapability::new();
        snapshot.phase = MultipartSourcePhase::ConsumedUnsealed;
        snapshot.publication = Some(publication.clone());
        snapshot.head_sha256 = None;
        snapshot.capability_sha256 = Some(capability.digest());
        let proof =
            MultipartSourceProof::issue(&authority, MultipartProofKind::Published, publication.attempt, None, snapshot).unwrap();
        let response = super::super::RenameDataResp {
            multipart_source: Some(Box::new(MultipartPublicationResult {
                publication,
                proof,
                undo_capability: capability,
            })),
            ..Default::default()
        };
        response
            .multipart_source
            .as_ref()
            .unwrap()
            .verify_with_authority(&authority)
            .unwrap();
        let legacy = UnboxedRenameResponse {
            multipart_source: response.multipart_source.as_deref(),
            old_data_dir: response.old_data_dir,
            rollback_data_dir: response.rollback_data_dir,
            cleanup_data_dir: response.cleanup_data_dir,
            sign: response.sign.as_ref(),
            old_current_size: response.old_current_size,
            old_current_source_checked: response.old_current_source_checked,
            old_current_source: response.old_current_source.as_ref(),
        };
        let encoded = rmp_serde::to_vec_named(&response).unwrap();
        assert_eq!(encoded, rmp_serde::to_vec_named(&legacy).unwrap());
        assert_eq!(serde_json::to_vec(&response).unwrap(), serde_json::to_vec(&legacy).unwrap());
        let decoded: super::super::RenameDataResp = rmp_serde::from_slice(&encoded).unwrap();
        assert_eq!(
            decoded.multipart_source.as_ref().unwrap().undo_capability.digest(),
            response.multipart_source.as_ref().unwrap().undo_capability.digest()
        );
        assert!(
            decoded
                .multipart_source
                .as_ref()
                .unwrap()
                .verify_with_authority(&authority)
                .is_err(),
            "serde indirection must not reconstruct native proof origin"
        );
        assert!(
            std::mem::size_of::<super::super::RenameDataResp>() < std::mem::size_of::<MultipartPublicationResult>(),
            "ordinary responses must not reserve the native publication payload inline"
        );
    }
}
