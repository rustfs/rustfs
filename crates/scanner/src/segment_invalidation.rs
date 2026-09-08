// Copyright 2026 RustFS Team
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

use crate::{DATA_USAGE_CACHE_KEY_FORMAT, DataUsageCacheSource, DataUsageScanPlanDigest};
use std::collections::BTreeSet;
use uuid::Uuid;

pub const MAX_SEGMENT_INVALIDATION_ENTRIES: usize = 4;
pub const MAX_SEGMENT_INVALIDATION_BYTES: usize = 128;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SegmentInvalidationError {
    EntryLimit,
    ByteLimit,
    InvalidProof,
    InvalidKey,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum SegmentInvalidationProducer {
    Put,
    Delete,
    DeleteMarker,
    Multipart,
    Replication,
    Tier,
    DirectoryObject,
}

impl SegmentInvalidationProducer {
    pub const REQUIRED: [Self; 7] = [
        Self::Put,
        Self::Delete,
        Self::DeleteMarker,
        Self::Multipart,
        Self::Replication,
        Self::Tier,
        Self::DirectoryObject,
    ];
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SegmentInvalidationDomain {
    LocalSingleSet,
    DistributedEc,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentInvalidationEnvelope {
    pub source: DataUsageCacheSource,
    pub bucket_incarnation: Uuid,
    pub key_format: u16,
    pub baseline_scan_plan_digest: DataUsageScanPlanDigest,
    pub process_epoch: String,
    pub generation_start: u64,
    pub generation_end: u64,
    pub restart_gap: bool,
    pub overflow: bool,
    pub producers: BTreeSet<SegmentInvalidationProducer>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentInvalidationProof {
    pub source: DataUsageCacheSource,
    pub bucket_incarnation: Uuid,
    pub key_format: u16,
    pub baseline_scan_plan_digest: DataUsageScanPlanDigest,
    pub process_epoch: String,
    pub generation_start: u64,
    pub generation_end: u64,
    pub durable_producer_identity: bool,
    pub invalidation_domain: SegmentInvalidationDomain,
    pub distributed_ec_invalidation: bool,
    pub cold_zero_walk_oracle: bool,
}

pub fn admit_segment_invalidation<I, K>(
    envelope: &SegmentInvalidationEnvelope,
    proof: &SegmentInvalidationProof,
    keys: I,
) -> Result<BTreeSet<String>, SegmentInvalidationError>
where
    I: IntoIterator<Item = K>,
    K: AsRef<str>,
{
    validate_segment_invalidation_proof(envelope, proof)?;
    segment_invalidation_top_level_entries(keys)
}

fn validate_segment_invalidation_proof(
    envelope: &SegmentInvalidationEnvelope,
    proof: &SegmentInvalidationProof,
) -> Result<(), SegmentInvalidationError> {
    if envelope.source != proof.source
        || envelope.bucket_incarnation.is_nil()
        || envelope.bucket_incarnation != proof.bucket_incarnation
        || envelope.key_format != DATA_USAGE_CACHE_KEY_FORMAT
        || envelope.key_format != proof.key_format
        || envelope.baseline_scan_plan_digest != proof.baseline_scan_plan_digest
        || envelope.process_epoch.is_empty()
        || envelope.process_epoch != proof.process_epoch
        || envelope.generation_start != proof.generation_start
        || envelope.generation_end != proof.generation_end
        || !proof.durable_producer_identity
        || !proof.cold_zero_walk_oracle
        || (proof.invalidation_domain == SegmentInvalidationDomain::DistributedEc && !proof.distributed_ec_invalidation)
        || envelope.generation_start == 0
        || envelope.generation_end < envelope.generation_start
        || proof.generation_start == 0
        || proof.generation_end < proof.generation_start
        || envelope.restart_gap
        || envelope.overflow
        || !SegmentInvalidationProducer::REQUIRED
            .iter()
            .all(|producer| envelope.producers.contains(producer))
    {
        return Err(SegmentInvalidationError::InvalidProof);
    }
    Ok(())
}

fn segment_invalidation_top_level_entries<I, K>(keys: I) -> Result<BTreeSet<String>, SegmentInvalidationError>
where
    I: IntoIterator<Item = K>,
    K: AsRef<str>,
{
    let mut segments = BTreeSet::new();
    let mut bytes = 0usize;
    for key in keys {
        let segment = top_level_segment(key.as_ref())?;
        if segments.contains(segment) {
            continue;
        }
        if segments.len() == MAX_SEGMENT_INVALIDATION_ENTRIES {
            return Err(SegmentInvalidationError::EntryLimit);
        }
        if segment.len() > MAX_SEGMENT_INVALIDATION_BYTES - bytes {
            return Err(SegmentInvalidationError::ByteLimit);
        }
        bytes += segment.len();
        segments.insert(segment.to_string());
    }
    Ok(segments)
}

fn top_level_segment(key: &str) -> Result<&str, SegmentInvalidationError> {
    if key.is_empty()
        || key.starts_with('/')
        || key.contains(['\\', '\0'])
        || key.split('/').any(|part| matches!(part, "" | "." | ".."))
    {
        return Err(SegmentInvalidationError::InvalidKey);
    }
    Ok(key.split('/').next().expect("validated nonempty key"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn producers() -> BTreeSet<SegmentInvalidationProducer> {
        SegmentInvalidationProducer::REQUIRED.into_iter().collect()
    }

    fn envelope() -> SegmentInvalidationEnvelope {
        SegmentInvalidationEnvelope {
            source: DataUsageCacheSource::new(2, 3),
            bucket_incarnation: Uuid::from_u128(0x12345678123456781234567812345678),
            key_format: DATA_USAGE_CACHE_KEY_FORMAT,
            baseline_scan_plan_digest: DataUsageScanPlanDigest([9; 32]),
            process_epoch: "epoch-a".to_string(),
            generation_start: 11,
            generation_end: 13,
            restart_gap: false,
            overflow: false,
            producers: producers(),
        }
    }

    fn proof() -> SegmentInvalidationProof {
        let envelope = envelope();
        SegmentInvalidationProof {
            source: envelope.source,
            bucket_incarnation: envelope.bucket_incarnation,
            key_format: envelope.key_format,
            baseline_scan_plan_digest: envelope.baseline_scan_plan_digest,
            process_epoch: envelope.process_epoch,
            generation_start: envelope.generation_start,
            generation_end: envelope.generation_end,
            durable_producer_identity: true,
            invalidation_domain: SegmentInvalidationDomain::LocalSingleSet,
            distributed_ec_invalidation: false,
            cold_zero_walk_oracle: true,
        }
    }

    #[test]
    fn segment_invalidation_admits_only_complete_identity_proof() {
        let envelope = envelope();
        let proof = proof();
        assert_eq!(
            admit_segment_invalidation(&envelope, &proof, ["hot/one", "hot/two", "archive/delete-marker"]),
            Ok(BTreeSet::from(["archive".to_string(), "hot".to_string()]))
        );

        let mut wrong_source = envelope.clone();
        wrong_source.source = DataUsageCacheSource::new(2, 4);
        assert_eq!(
            admit_segment_invalidation(&wrong_source, &proof, ["hot/one"]),
            Err(SegmentInvalidationError::InvalidProof)
        );

        let mut missing_incarnation = envelope.clone();
        missing_incarnation.bucket_incarnation = Uuid::nil();
        assert_eq!(
            admit_segment_invalidation(&missing_incarnation, &proof, ["hot/one"]),
            Err(SegmentInvalidationError::InvalidProof)
        );

        let mut wrong_key_format = envelope.clone();
        wrong_key_format.key_format = DATA_USAGE_CACHE_KEY_FORMAT.saturating_add(1);
        assert_eq!(
            admit_segment_invalidation(&wrong_key_format, &proof, ["hot/one"]),
            Err(SegmentInvalidationError::InvalidProof)
        );

        let mut wrong_baseline = envelope.clone();
        wrong_baseline.baseline_scan_plan_digest = DataUsageScanPlanDigest([8; 32]);
        assert_eq!(
            admit_segment_invalidation(&wrong_baseline, &proof, ["hot/one"]),
            Err(SegmentInvalidationError::InvalidProof)
        );

        let mut wrong_epoch = envelope.clone();
        wrong_epoch.process_epoch = "epoch-b".to_string();
        assert_eq!(
            admit_segment_invalidation(&wrong_epoch, &proof, ["hot/one"]),
            Err(SegmentInvalidationError::InvalidProof)
        );

        let mut wrong_generation_start = proof.clone();
        wrong_generation_start.generation_start = wrong_generation_start.generation_start.saturating_sub(1);
        assert_eq!(
            admit_segment_invalidation(&envelope, &wrong_generation_start, ["hot/one"]),
            Err(SegmentInvalidationError::InvalidProof)
        );

        let mut wrong_generation_end = proof.clone();
        wrong_generation_end.generation_end = wrong_generation_end.generation_end.saturating_add(1);
        assert_eq!(
            admit_segment_invalidation(&envelope, &wrong_generation_end, ["hot/one"]),
            Err(SegmentInvalidationError::InvalidProof)
        );

        let mut no_durable_identity = proof.clone();
        no_durable_identity.durable_producer_identity = false;
        assert_eq!(
            admit_segment_invalidation(&envelope, &no_durable_identity, ["hot/one"]),
            Err(SegmentInvalidationError::InvalidProof)
        );

        let mut restart_gap = envelope.clone();
        restart_gap.restart_gap = true;
        assert_eq!(
            admit_segment_invalidation(&restart_gap, &proof, ["hot/one"]),
            Err(SegmentInvalidationError::InvalidProof)
        );

        let mut overflow = envelope.clone();
        overflow.overflow = true;
        assert_eq!(
            admit_segment_invalidation(&overflow, &proof, ["hot/one"]),
            Err(SegmentInvalidationError::InvalidProof)
        );

        let mut generation_gap = envelope.clone();
        generation_gap.generation_end = generation_gap.generation_start - 1;
        assert_eq!(
            admit_segment_invalidation(&generation_gap, &proof, ["hot/one"]),
            Err(SegmentInvalidationError::InvalidProof)
        );

        let mut missing_producer = envelope.clone();
        missing_producer.producers.remove(&SegmentInvalidationProducer::Replication);
        assert_eq!(
            admit_segment_invalidation(&missing_producer, &proof, ["hot/one"]),
            Err(SegmentInvalidationError::InvalidProof)
        );

        let mut missing_zero_walk_oracle = proof.clone();
        missing_zero_walk_oracle.cold_zero_walk_oracle = false;
        assert_eq!(
            admit_segment_invalidation(&envelope, &missing_zero_walk_oracle, ["hot/one"]),
            Err(SegmentInvalidationError::InvalidProof)
        );

        let mut distributed_without_invalidation = proof;
        distributed_without_invalidation.invalidation_domain = SegmentInvalidationDomain::DistributedEc;
        assert_eq!(
            admit_segment_invalidation(&envelope, &distributed_without_invalidation, ["hot/one"]),
            Err(SegmentInvalidationError::InvalidProof)
        );

        let mut distributed_with_invalidation = distributed_without_invalidation;
        distributed_with_invalidation.distributed_ec_invalidation = true;
        assert_eq!(
            admit_segment_invalidation(&envelope, &distributed_with_invalidation, ["hot/one"]),
            Ok(BTreeSet::from(["hot".to_string()]))
        );
    }

    #[test]
    fn segment_invalidation_entries_are_bounded_and_key_checked() {
        let envelope = envelope();
        let proof = proof();
        assert_eq!(
            admit_segment_invalidation(&envelope, &proof, ["hot/one", "hot/two"]),
            Ok(BTreeSet::from(["hot".to_string()]))
        );
        assert_eq!(
            admit_segment_invalidation(&envelope, &proof, ["a", "b", "c", "d"])
                .expect("entry boundary")
                .len(),
            MAX_SEGMENT_INVALIDATION_ENTRIES
        );
        assert_eq!(
            admit_segment_invalidation(&envelope, &proof, ["a", "b", "c", "d", "e"]),
            Err(SegmentInvalidationError::EntryLimit)
        );
        let exact = "x".repeat(MAX_SEGMENT_INVALIDATION_BYTES);
        assert!(admit_segment_invalidation(&envelope, &proof, [&exact]).is_ok());
        assert_eq!(
            admit_segment_invalidation(&envelope, &proof, [&exact, "y"]),
            Err(SegmentInvalidationError::ByteLimit)
        );
        let oversized = "x".repeat(MAX_SEGMENT_INVALIDATION_BYTES + 1);
        assert_eq!(
            admit_segment_invalidation(&envelope, &proof, [&oversized]),
            Err(SegmentInvalidationError::ByteLimit)
        );
        for key in ["", "/hot", "hot/../cold", "hot//one", "hot\\one", "hot/\0"] {
            assert_eq!(
                admit_segment_invalidation(&envelope, &proof, [key]),
                Err(SegmentInvalidationError::InvalidKey)
            );
        }
    }
}
