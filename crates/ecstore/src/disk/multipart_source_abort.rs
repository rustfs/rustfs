// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Independent all-participant source closure evidence. It is never an EC vote.
use super::*;

pub const NATIVE_SOURCE_PROTOCOL_SUFFIX: &str = "multipart-source-protocol";
const NATIVE_SOURCE_PROTOCOL_VALUE: &str = "native-v1";
const ABORT_PROOF_DOMAIN: &[u8] = b"rustfs-mpu-source-abort-proof-v1\0";

pub fn native_source_protocol(fi: &FileInfo) -> Result<bool> {
    use rustfs_utils::http::metadata_compat::{contains_key_str, get_consistent_str};
    match get_consistent_str(&fi.metadata, NATIVE_SOURCE_PROTOCOL_SUFFIX) {
        Some(NATIVE_SOURCE_PROTOCOL_VALUE) => Ok(true),
        Some(_) => Err(DiskError::FileCorrupt),
        None if contains_key_str(&fi.metadata, NATIVE_SOURCE_PROTOCOL_SUFFIX) => Err(DiskError::FileCorrupt),
        None => Ok(false),
    }
}
pub fn mark_native_source(fi: &mut FileInfo) {
    rustfs_utils::http::metadata_compat::insert_str(
        &mut fi.metadata,
        NATIVE_SOURCE_PROTOCOL_SUFFIX,
        NATIVE_SOURCE_PROTOCOL_VALUE.to_owned(),
    );
}
pub fn recognized_native_source_controls(fi: &FileInfo) -> Result<bool> {
    let marker = native_source_protocol(fi)?;
    let state = has_nonbirth_native_controls(fi)?;
    Ok(marker || state)
}
pub(in crate::disk) fn has_nonbirth_native_controls(fi: &FileInfo) -> Result<bool> {
    native_source_protocol(fi)?;
    let condition: Option<SourceConditionV1> = decode_metadata(&fi.metadata, MULTIPART_SOURCE_CONDITION_SUFFIX)?;
    if let Some(condition) = &condition {
        condition.validate()?;
    }
    let publication: Option<PublicationIdentity> = decode_metadata(&fi.metadata, PUBLICATION_SUFFIX)?;
    if let Some(publication) = &publication {
        publication.lineage.validate()?;
    }
    let status = source_status(fi)?;
    Ok(condition.is_some() || publication.is_some() || status.is_some())
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MultipartAbortRound {
    pub lineage: SourceLineage,
    pub generation: SourceGeneration,
    pub round: Uuid,
}
impl MultipartAbortRound {
    pub fn validate(&self) -> Result<()> {
        self.lineage.validate()?;
        if self.round.is_nil()
            || self.generation.epoch == 0
            || self.generation.nonce.is_nil()
            || self.generation.nonce == self.lineage.original_data_dir
        {
            return Err(DiskError::FileCorrupt);
        }
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MultipartAbortProofKind {
    Prepared,
    Committed,
    Deleted,
    CleanupTarget,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MultipartAbortProof {
    pub kind: MultipartAbortProofKind,
    pub challenge: Uuid,
    pub round: MultipartAbortRound,
    pub snapshot: MultipartSourceSnapshot,
    pub certificate_sha256: Option<[u8; 32]>,
    pub publication: Option<PublicationIdentity>,
    authentication: ProofAuthentication,
    #[serde(skip)]
    native_origin: Option<Arc<NativeOriginWitness>>,
}
impl PartialEq for MultipartAbortProof {
    fn eq(&self, other: &Self) -> bool {
        self.kind == other.kind
            && self.challenge == other.challenge
            && self.round == other.round
            && self.snapshot == other.snapshot
            && self.certificate_sha256 == other.certificate_sha256
            && self.publication == other.publication
            && self.authentication == other.authentication
    }
}
impl Eq for MultipartAbortProof {}
impl MultipartAbortProof {
    fn body(&self) -> Result<Vec<u8>> {
        let mut body = ABORT_PROOF_DOMAIN.to_vec();
        body.extend_from_slice(
            &rmp_serde::to_vec_named(&(
                MULTIPART_SOURCE_PROTOCOL_VERSION,
                self.kind,
                self.challenge,
                &self.round,
                &self.snapshot,
                self.certificate_sha256,
                &self.publication,
            ))
            .map_err(super::super::Error::other)?,
        );
        Ok(body)
    }
    fn validate(&self) -> Result<()> {
        self.round.validate()?;
        let snapshot = &self.snapshot;
        if snapshot.disk_id.is_nil()
            || snapshot.lineage != self.round.lineage
            || snapshot.physical_slot >= snapshot.lineage.geometry.disks()
            || self.challenge.is_nil()
            || snapshot.phase != MultipartSourcePhase::Aborting
            || snapshot.head_sha256.is_some()
            || snapshot.capability_sha256.is_some()
            || snapshot.generation.epoch < self.round.generation.epoch
            || (snapshot.generation.epoch == self.round.generation.epoch && snapshot.generation != self.round.generation)
            || (self.kind == MultipartAbortProofKind::Prepared
                && (self.challenge != self.round.round
                    || snapshot.generation != self.round.generation
                    || self.certificate_sha256.is_some()
                    || self.publication.is_some()))
            || (matches!(self.kind, MultipartAbortProofKind::Committed | MultipartAbortProofKind::Deleted)
                && (self.challenge != self.round.round || self.certificate_sha256.is_none() || self.publication.is_some()))
            || (self.kind == MultipartAbortProofKind::CleanupTarget
                && (self.certificate_sha256.is_none()
                    || self
                        .publication
                        .as_ref()
                        .is_none_or(|publication| publication.lineage != self.round.lineage)))
        {
            return Err(DiskError::FileCorrupt);
        }
        Ok(())
    }
    pub fn verify(&self) -> Result<()> {
        self.verify_with_authority(&ProofVerificationContext::RpcOnly)
    }
    pub fn verify_with_authority(&self, authority: &ProofVerificationContext) -> Result<()> {
        self.validate()?;
        let body = self.body()?;
        match &self.authentication {
            ProofAuthentication::ClusterHmac(mac) => {
                crate::cluster::rpc::verify_tonic_rpc_response_proof(&body, mac).map_err(DiskError::from)
            }
            ProofAuthentication::NativeLocal => {
                let ProofVerificationContext::Local(expected) = authority else {
                    return Err(DiskError::FileCorrupt);
                };
                let origin = self.native_origin.as_ref().ok_or(DiskError::FileCorrupt)?;
                if !Arc::ptr_eq(expected, &origin.authority) || origin.body_sha256 != <[u8; 32]>::from(Sha256::digest(body)) {
                    return Err(DiskError::FileCorrupt);
                }
                Ok(())
            }
        }
    }
    pub(in crate::disk) fn issue(
        authority: &ProofVerificationContext,
        kind: MultipartAbortProofKind,
        challenge: Uuid,
        round: MultipartAbortRound,
        snapshot: MultipartSourceSnapshot,
        certificate_sha256: Option<[u8; 32]>,
        publication: Option<PublicationIdentity>,
    ) -> Result<Self> {
        let mut proof = Self {
            kind,
            challenge,
            round,
            snapshot,
            certificate_sha256,
            publication,
            authentication: ProofAuthentication::NativeLocal,
            native_origin: None,
        };
        proof.validate()?;
        match authority {
            ProofVerificationContext::Local(local) => proof.attach_origin(local)?,
            ProofVerificationContext::RpcOnly => {
                proof.authentication = ProofAuthentication::ClusterHmac(
                    crate::cluster::rpc::sign_tonic_rpc_response_proof(&proof.body()?)
                        .map_err(DiskError::from)?
                        .try_into()
                        .map_err(|_| DiskError::FileCorrupt)?,
                )
            }
        }
        Ok(proof)
    }
    fn attach_origin(&mut self, authority: &Arc<LocalMultipartProofAuthority>) -> Result<()> {
        self.validate()?;
        self.native_origin = Some(Arc::new(NativeOriginWitness {
            authority: authority.clone(),
            body_sha256: Sha256::digest(self.body()?).into(),
        }));
        Ok(())
    }
    pub(super) fn prepare_wire(&mut self) -> Result<()> {
        self.validate()?;
        if let ProofAuthentication::NativeLocal = self.authentication {
            let origin = self.native_origin.as_ref().ok_or(DiskError::FileCorrupt)?;
            self.verify_with_authority(&ProofVerificationContext::Local(origin.authority.clone()))?;
            self.authentication = ProofAuthentication::ClusterHmac(
                crate::cluster::rpc::sign_tonic_rpc_response_proof(&self.body()?)
                    .map_err(DiskError::from)?
                    .try_into()
                    .map_err(|_| DiskError::FileCorrupt)?,
            );
            self.native_origin = None;
        }
        self.verify()
    }
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MultipartAbortCertificate {
    pub round: MultipartAbortRound,
    pub disk_ids: Vec<Uuid>,
    pub proofs: Vec<MultipartAbortProof>,
}
impl MultipartAbortCertificate {
    pub fn digest(&self) -> Result<[u8; 32]> {
        Ok(Sha256::digest(rmp_serde::to_vec_named(self).map_err(super::super::Error::other)?).into())
    }
    pub fn verify(&self) -> Result<()> {
        self.verify_with_authority(&ProofVerificationContext::RpcOnly)
    }
    pub fn verify_with_authority(&self, authority: &ProofVerificationContext) -> Result<()> {
        self.validate_shape()?;
        for proof in &self.proofs {
            proof.verify_with_authority(authority)?;
        }
        Ok(())
    }
    pub(super) fn validate_shape(&self) -> Result<()> {
        self.round.validate()?;
        let n = self.round.lineage.geometry.disks();
        if self.disk_ids.len() != n || self.proofs.len() != n || self.disk_ids.iter().any(Uuid::is_nil) {
            return Err(DiskError::FileCorrupt);
        }
        let mut ids = self.disk_ids.clone();
        ids.sort_unstable();
        if ids.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(DiskError::FileCorrupt);
        }
        let mut slots = [false; 16];
        for (expected_slot, proof) in self.proofs.iter().enumerate() {
            proof.validate()?;
            let slot = proof.snapshot.physical_slot;
            if slot != expected_slot
                || proof.kind != MultipartAbortProofKind::Prepared
                || proof.round != self.round
                || self.disk_ids.get(slot) != Some(&proof.snapshot.disk_id)
                || std::mem::replace(&mut slots[slot], true)
            {
                return Err(DiskError::FileCorrupt);
            }
        }
        Ok(())
    }
    pub(super) fn prepare_wire(&mut self) -> Result<()> {
        if self.proofs.len() > 16 {
            return Err(DiskError::FileCorrupt);
        }
        for proof in &mut self.proofs {
            proof.prepare_wire()?;
        }
        self.verify()
    }
    pub(super) fn validate_wire(&self) -> Result<()> {
        self.verify()
    }
    pub(in crate::disk) fn hydrate_owned(
        &mut self,
        authority: &ProofVerificationContext,
        validated: &super::super::local::multipart_source_abort::ValidatedAbortCertificate,
    ) -> Result<()> {
        validated.validate_certificate(self)?;
        if let ProofVerificationContext::Local(local) = authority {
            for proof in &mut self.proofs {
                match proof.authentication {
                    ProofAuthentication::NativeLocal => proof.attach_origin(local)?,
                    ProofAuthentication::ClusterHmac(_) => proof.verify()?,
                }
            }
        }
        self.verify_with_authority(authority)
    }
}

#[cfg(test)]
mod tests {
    use super::super::multipart_proof_authority_tests::{fixture, local_authority};
    use super::*;
    fn certificate() -> (ProofVerificationContext, MultipartAbortCertificate) {
        let authority = local_authority();
        let (recovery, mut snapshot) = fixture();
        let round = MultipartAbortRound {
            lineage: recovery.lineage,
            generation: SourceGeneration {
                epoch: 10,
                nonce: Uuid::new_v4(),
            },
            round: Uuid::new_v4(),
        };
        snapshot.phase = MultipartSourcePhase::Aborting;
        snapshot.generation = round.generation;
        snapshot.head_sha256 = None;
        snapshot.capability_sha256 = None;
        snapshot.publication = None;
        let disk_ids = (0..4).map(|_| Uuid::new_v4()).collect::<Vec<_>>();
        let proofs = disk_ids
            .iter()
            .enumerate()
            .map(|(slot, id)| {
                let mut snapshot = snapshot.clone();
                snapshot.disk_id = *id;
                snapshot.physical_slot = slot;
                MultipartAbortProof::issue(
                    &authority,
                    MultipartAbortProofKind::Prepared,
                    round.round,
                    round.clone(),
                    snapshot,
                    None,
                    None,
                )
                .unwrap()
            })
            .collect();
        (authority, MultipartAbortCertificate { round, disk_ids, proofs })
    }
    #[test]
    fn abort_certificate_requires_all_actual_ordered_participants_and_native_provenance() {
        let (authority, certificate) = certificate();
        certificate.verify_with_authority(&authority).unwrap();
        let mut partial = certificate.clone();
        partial.proofs.pop();
        assert!(partial.verify_with_authority(&authority).is_err());
        let mut reordered = certificate.clone();
        reordered.proofs.swap(0, 1);
        assert!(reordered.verify_with_authority(&authority).is_err());
        let mut duplicate = certificate.clone();
        duplicate.disk_ids[1] = duplicate.disk_ids[0];
        assert!(duplicate.verify_with_authority(&authority).is_err());
        let raw = rmp_serde::to_vec_named(&certificate).unwrap();
        let decoded: MultipartAbortCertificate = rmp_serde::from_slice(&raw).unwrap();
        assert_eq!(certificate.digest().unwrap(), decoded.digest().unwrap());
        assert!(
            decoded.verify_with_authority(&authority).is_err(),
            "bare local bytes cannot issue closure authority"
        );
        let mut forged = certificate.clone();
        forged.round.generation.epoch += 1;
        assert!(forged.verify_with_authority(&authority).is_err());
        assert!(certificate.verify_with_authority(&local_authority()).is_err());
    }
    #[test]
    fn abort_target_proof_requires_committed_certificate_and_binds_historical_target() {
        let (authority, certificate) = certificate();
        let mut snapshot = certificate.proofs[0].snapshot.clone();
        snapshot.generation.epoch += 1;
        snapshot.generation.nonce = Uuid::new_v4();
        let publication = PublicationIdentity {
            lineage: certificate.round.lineage.clone(),
            source_generation: SourceGeneration {
                epoch: 0,
                nonce: certificate.round.lineage.original_data_dir,
            },
            attempt: Uuid::new_v4(),
            version_id: Some(Uuid::new_v4()),
            final_data_dir: Uuid::new_v4(),
            modified_unix_nanos: 1,
            completion_identity: [7; 32],
        };
        let challenge = Uuid::new_v4();
        assert!(
            MultipartAbortProof::issue(
                &authority,
                MultipartAbortProofKind::CleanupTarget,
                challenge,
                certificate.round.clone(),
                snapshot.clone(),
                None,
                Some(publication.clone())
            )
            .is_err()
        );
        let proof = MultipartAbortProof::issue(
            &authority,
            MultipartAbortProofKind::CleanupTarget,
            challenge,
            certificate.round.clone(),
            snapshot,
            Some(certificate.digest().unwrap()),
            Some(publication),
        )
        .unwrap();
        proof.verify_with_authority(&authority).unwrap();
        let mut rebound = proof.clone();
        rebound.publication.as_mut().unwrap().final_data_dir = Uuid::new_v4();
        assert!(rebound.verify_with_authority(&authority).is_err());
        let mut replay = proof;
        replay.challenge = Uuid::new_v4();
        assert!(replay.verify_with_authority(&authority).is_err());
    }
    #[test]
    fn birth_marker_is_strict_dual_family_source_only_and_does_not_change_lineage() {
        use rustfs_utils::http::metadata_compat::{get_consistent_str, insert_str};
        let mut source = FileInfo::default();
        assert!(!native_source_protocol(&source).unwrap());
        let before = digest_value(&normalized_info(&source, false)).unwrap();
        mark_native_source(&mut source);
        assert!(native_source_protocol(&source).unwrap());
        assert_eq!(
            get_consistent_str(&source.metadata, NATIVE_SOURCE_PROTOCOL_SUFFIX),
            Some(NATIVE_SOURCE_PROTOCOL_VALUE)
        );
        assert_eq!(before, digest_value(&normalized_info(&source, false)).unwrap());
        insert_str(&mut source.metadata, "user-visible-identity", "changed".into());
        assert_ne!(
            before,
            digest_value(&normalized_info(&source, false)).unwrap(),
            "only the source protocol control is projected"
        );
        source.metadata.insert(
            format!("{}{}", rustfs_utils::http::MINIO_INTERNAL_PREFIX, NATIVE_SOURCE_PROTOCOL_SUFFIX),
            "native-v2".into(),
        );
        assert!(native_source_protocol(&source).is_err());
        assert!(recognized_native_source_controls(&source).is_err());
        mark_native_source(&mut source);
        strip_native_controls(&mut source.metadata);
        assert!(!native_source_protocol(&source).unwrap());
    }
    #[test]
    fn omitted_abort_fields_preserve_existing_inspection_encoding() {
        let inspection = MultipartSourceInspection {
            disk_id: Uuid::new_v4(),
            snapshot: None,
            original: None,
            recovery: None,
            negative_certificate: None,
            ready_certificate: None,
            terminal_certificate: Vec::new(),
            root_missing: false,
            abort_round: None,
            abort_certificate: None,
        };
        let json = serde_json::to_value(&inspection).unwrap();
        assert!(json.get("root_missing").is_none());
        assert!(json.get("abort_round").is_none());
        assert!(json.get("abort_certificate").is_none());
        let decoded: MultipartSourceInspection = serde_json::from_value(json).unwrap();
        assert!(!decoded.root_missing);
        assert!(decoded.abort_certificate.is_none());
    }
}
