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

use std::{collections::HashSet, sync::Arc};

use rustfs_utils::crypto::{hex_sha256, is_sha256_checksum};
use serde::{Deserialize, Serialize};

use super::config_boundary;
use super::recovery_control::{
    IlmRecoveryClassification, IlmRecoveryControl, IlmRecoveryProtocol, IlmRecoverySourceCopy, IlmRecoverySourceGeneration,
    MAX_ILM_RECOVERY_CONTROL_SIZE, ObservedIlmRecoveryControl, ObservedIlmRecoverySource, recovery_control_record_object_name,
};
use super::tier_delete_journal::{
    TIER_DELETE_JOURNAL_V1_RECOVERY_SCHEMA, TIER_DELETE_JOURNAL_V2_RECOVERY_SCHEMA, validate_legacy_tier_delete_recovery_source,
};
use crate::disk::RUSTFS_META_BUCKET;
use crate::error::{Error, Result};
use crate::object_api::{ObjectOptions, WriteCompletion};
use crate::services::notification_sys::{
    acquire_ilm_recovery_export_fleet_proof, ilm_recovery_export_fleet_proof_matches, ilm_recovery_export_member_epochs_sha256,
    ilm_recovery_export_topology_generation,
};
use crate::storage_api_contracts::{list::ListOperations as _, namespace::NamespaceLocking as _, object::HTTPPreconditions};
use crate::store::ECStore;

pub const ILM_RECOVERY_EXPORT_SCHEMA: &str = "rustfs-ilm-recovery-export-v1";
pub const ILM_RECOVERY_EXPORT_PREFIX: &str = "ilm/recovery-exports";
pub const MAX_ILM_RECOVERY_EXPORT_SIZE: usize = 128 * 1024;
const MAX_ILM_RECOVERY_EXPORTS: usize = 10_000;
const MAX_ILM_RECOVERY_EXPORT_BYTES: u64 = 1024 * 1024 * 1024;
const MAX_ACTOR_EXPORTS_PER_MINUTE: usize = 10;
const MAX_CLUSTER_EXPORTS_PER_MINUTE: usize = 100;
const EXPORT_RETENTION_NANOS: i64 = 90 * 24 * 60 * 60 * 1_000_000_000;
const EXPORT_ADMISSION_LOCK: &str = "ilm/recovery-admission/export.lock";
const MAX_LEGACY_TIER_DELETE_SOURCE_SIZE: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IlmRecoveryExportObservation {
    pub control_id: String,
    pub protocol: IlmRecoveryProtocol,
    pub control_etag: String,
    pub control_revision: u64,
    pub classification: IlmRecoveryClassification,
    pub canonical_source_path: String,
    pub source_generation: IlmRecoverySourceGeneration,
    pub topology_generation: String,
    pub member_epochs_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IlmRecoveryExport {
    pub export_id: String,
    pub control_id: String,
    pub protocol: IlmRecoveryProtocol,
    pub control_etag: String,
    pub control_revision: u64,
    pub classification: IlmRecoveryClassification,
    pub canonical_source_path: String,
    pub source_generation: IlmRecoverySourceGeneration,
    pub topology_generation: String,
    pub member_epochs_sha256: String,
    pub creator_sha256: String,
    pub created_at_unix_nanos: i64,
    pub retain_until_unix_nanos: i64,
    pub source_bytes_base64: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedIlmRecoveryExport {
    schema: String,
    content_sha256: String,
    export: IlmRecoveryExport,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct IlmRecoveryExportCreated {
    pub export_id: String,
    pub content_sha256: String,
    pub encoded: Vec<u8>,
    pub replayed: bool,
}

impl IlmRecoveryExport {
    fn validate(&self) -> Result<()> {
        self.source_generation.validate().map_err(Error::other)?;
        validate_sha256(&self.export_id, "ILM recovery export ID is invalid")?;
        validate_sha256(&self.control_id, "ILM recovery export control ID is invalid")?;
        validate_sha256(&self.topology_generation, "ILM recovery export topology generation is invalid")?;
        validate_sha256(&self.member_epochs_sha256, "ILM recovery export member epoch digest is invalid")?;
        validate_sha256(&self.creator_sha256, "ILM recovery export creator digest is invalid")?;
        if self.protocol != IlmRecoveryProtocol::TierDeleteJournal
            || self.classification != IlmRecoveryClassification::RetainedAmbiguous
            || !is_legacy_export_schema(&self.source_generation.source_schema)
        {
            return Err(Error::other("ILM recovery export source is not an exportable legacy journal"));
        }
        if self.control_etag.trim().is_empty() || self.control_revision == 0 {
            return Err(Error::other("ILM recovery export control generation is invalid"));
        }
        if self.canonical_source_path.is_empty()
            || self.canonical_source_path.starts_with('/')
            || self.canonical_source_path.ends_with('/')
            || self.canonical_source_path.split('/').any(str::is_empty)
        {
            return Err(Error::other("ILM recovery export source path is invalid"));
        }
        if self.created_at_unix_nanos <= 0
            || self.retain_until_unix_nanos < self.created_at_unix_nanos.saturating_add(EXPORT_RETENTION_NANOS)
        {
            return Err(Error::other("ILM recovery export retention is invalid"));
        }
        let source = base64_simd::STANDARD
            .decode_to_vec(self.source_bytes_base64.as_bytes())
            .map_err(|_| Error::other("ILM recovery export source encoding is invalid"))?;
        validate_legacy_tier_delete_recovery_source(&self.canonical_source_path, &self.source_generation.source_schema, &source)?;
        let encoded_len = u64::try_from(source.len()).map_err(|_| Error::other("ILM recovery export source length overflow"))?;
        if source.is_empty()
            || source.len() > MAX_LEGACY_TIER_DELETE_SOURCE_SIZE
            || hex_sha256(&source, ToOwned::to_owned) != self.source_generation.content_sha256
            || self.source_generation.copies.iter().any(|copy| {
                copy.canonical_path != self.canonical_source_path
                    || copy.etag != self.source_generation.source_etag
                    || copy.content_sha256 != self.source_generation.content_sha256
                    || copy.encoded_len != encoded_len
            })
        {
            return Err(Error::other("ILM recovery export source bytes do not match the observed generation"));
        }
        if recovery_export_id(&self.control_id, &self.source_generation)? != self.export_id {
            return Err(Error::other("ILM recovery export ID does not match its source generation"));
        }
        Ok(())
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let export_bytes = serde_json::to_vec(self).map_err(Error::other)?;
        let persisted = PersistedIlmRecoveryExport {
            schema: ILM_RECOVERY_EXPORT_SCHEMA.to_string(),
            content_sha256: hex_sha256(&export_bytes, ToOwned::to_owned),
            export: self.clone(),
        };
        let encoded = serde_json::to_vec(&persisted).map_err(Error::other)?;
        if encoded.len() > MAX_ILM_RECOVERY_EXPORT_SIZE {
            return Err(Error::other("encoded ILM recovery export exceeds maximum size"));
        }
        Ok(encoded)
    }

    pub fn decode(expected_export_id: &str, data: &[u8]) -> Result<Self> {
        validate_sha256(expected_export_id, "ILM recovery export ID is invalid")?;
        if data.len() > MAX_ILM_RECOVERY_EXPORT_SIZE {
            return Err(Error::other("encoded ILM recovery export exceeds maximum size"));
        }
        let persisted: PersistedIlmRecoveryExport = serde_json::from_slice(data).map_err(Error::other)?;
        if persisted.schema != ILM_RECOVERY_EXPORT_SCHEMA {
            return Err(Error::other("ILM recovery export schema is unsupported"));
        }
        validate_sha256(&persisted.content_sha256, "ILM recovery export checksum is invalid")?;
        let export_bytes = serde_json::to_vec(&persisted.export).map_err(Error::other)?;
        if hex_sha256(&export_bytes, ToOwned::to_owned) != persisted.content_sha256 {
            return Err(Error::other("ILM recovery export checksum mismatch"));
        }
        persisted.export.validate()?;
        if persisted.export.export_id != expected_export_id {
            return Err(Error::other("ILM recovery export ID does not match record key"));
        }
        Ok(persisted.export)
    }
}

pub fn recovery_export_record_object_name(protocol: IlmRecoveryProtocol, export_id: &str) -> Result<String> {
    validate_sha256(export_id, "ILM recovery export ID is invalid")?;
    Ok(format!(
        "{}/{}/{}/{}/{}.json",
        ILM_RECOVERY_EXPORT_PREFIX,
        protocol.as_str(),
        &export_id[..2],
        &export_id[2..4],
        export_id
    ))
}

pub fn recovery_export_id_from_record_object_name(object: &str) -> Result<(IlmRecoveryProtocol, String)> {
    let suffix = object
        .strip_prefix(ILM_RECOVERY_EXPORT_PREFIX)
        .and_then(|suffix| suffix.strip_prefix('/'))
        .ok_or_else(|| Error::other("ILM recovery export path has wrong prefix"))?;
    let mut parts = suffix.split('/');
    let protocol = match parts.next() {
        Some("tier_delete_journal") => IlmRecoveryProtocol::TierDeleteJournal,
        _ => return Err(Error::other("ILM recovery export protocol is invalid")),
    };
    let shard_a = parts
        .next()
        .ok_or_else(|| Error::other("ILM recovery export path is incomplete"))?;
    let shard_b = parts
        .next()
        .ok_or_else(|| Error::other("ILM recovery export path is incomplete"))?;
    let export_id = parts
        .next()
        .and_then(|name| name.strip_suffix(".json"))
        .ok_or_else(|| Error::other("ILM recovery export suffix is invalid"))?;
    if parts.next().is_some() {
        return Err(Error::other("ILM recovery export path is not canonical"));
    }
    validate_sha256(export_id, "ILM recovery export ID is invalid")?;
    if shard_a != &export_id[..2] || shard_b != &export_id[2..4] {
        return Err(Error::other("ILM recovery export shard does not match export ID"));
    }
    Ok((protocol, export_id.to_string()))
}

pub async fn inspect_recovery_export_observation(api: Arc<ECStore>, control_id: &str) -> Result<IlmRecoveryExportObservation> {
    let proof = acquire_ilm_recovery_export_fleet_proof()
        .await
        .ok_or_else(|| Error::other("ILM recovery export fleet proof is unavailable"))?;
    let observed_control = load_exportable_control(api.clone(), control_id).await?;
    let observed_source = observe_export_source(
        api,
        &observed_control.control.identity.canonical_source_path,
        &observed_control.control.observed_source_generation.source_schema,
    )
    .await?;
    if !observed_source.is_consistent()
        || observed_source.generation != observed_control.control.observed_source_generation
        || !ilm_recovery_export_fleet_proof_matches(&proof).await
    {
        return Err(Error::other("ILM recovery export observation changed or is incomplete"));
    }
    Ok(IlmRecoveryExportObservation {
        control_id: control_id.to_string(),
        protocol: observed_control.control.identity.protocol,
        control_etag: observed_control.etag,
        control_revision: observed_control.control.revision,
        classification: observed_control.control.classification,
        canonical_source_path: observed_control.control.identity.canonical_source_path,
        source_generation: observed_source.generation,
        topology_generation: ilm_recovery_export_topology_generation(&proof),
        member_epochs_sha256: ilm_recovery_export_member_epochs_sha256(&proof),
    })
}

pub async fn create_recovery_export(
    api: Arc<ECStore>,
    observation: &IlmRecoveryExportObservation,
    creator_sha256: &str,
) -> Result<IlmRecoveryExportCreated> {
    validate_sha256(creator_sha256, "ILM recovery export creator digest is invalid")?;
    let lock = api.new_ns_lock(RUSTFS_META_BUCKET, EXPORT_ADMISSION_LOCK).await?;
    let admission_guard = lock.get_write_lock(crate::set_disk::get_lock_acquire_timeout()).await?;

    let proof = acquire_ilm_recovery_export_fleet_proof()
        .await
        .ok_or_else(|| Error::other("ILM recovery export fleet proof is unavailable"))?;
    if ilm_recovery_export_topology_generation(&proof) != observation.topology_generation
        || ilm_recovery_export_member_epochs_sha256(&proof) != observation.member_epochs_sha256
    {
        return Err(Error::PreconditionFailed);
    }
    let control_object = recovery_control_record_object_name(IlmRecoveryProtocol::TierDeleteJournal, &observation.control_id)
        .map_err(Error::other)?;
    let control_lock = api.new_ns_lock(RUSTFS_META_BUCKET, &control_object).await?;
    let control_guard = control_lock
        .get_read_lock(crate::set_disk::get_lock_acquire_timeout())
        .await?;
    let source_lock = api
        .new_ns_lock(RUSTFS_META_BUCKET, &observation.canonical_source_path)
        .await?;
    let source_guard = source_lock.get_read_lock(crate::set_disk::get_lock_acquire_timeout()).await?;
    let locks_current = || !admission_guard.is_lock_lost() && !control_guard.is_lock_lost() && !source_guard.is_lock_lost();
    let (current, current_source_bytes) = current_observation_under_proof_no_lock(api.clone(), observation, &proof).await?;
    if &current != observation || !locks_current() {
        return Err(Error::PreconditionFailed);
    }
    let current_source_base64 = base64_simd::STANDARD.encode_to_string(current_source_bytes);
    let candidate_export_id = recovery_export_id(&current.control_id, &current.source_generation)?;
    let object = recovery_export_record_object_name(current.protocol, &candidate_export_id)?;
    match load_recovery_export_decoded(api.clone(), &candidate_export_id).await {
        Ok((existing, export)) if export_matches_observation(&export, observation) => {
            if !locks_current() || !ilm_recovery_export_fleet_proof_matches(&proof).await {
                return Err(Error::PreconditionFailed);
            }
            api.record_durable_ilm_decommission_progress(&object, &existing.encoded)
                .await?;
            if !locks_current() {
                return Err(Error::PreconditionFailed);
            }
            return Ok(existing.with_replayed());
        }
        Ok(_) => return Err(Error::PreconditionFailed),
        Err(Error::ConfigNotFound) => {}
        Err(err) => return Err(err),
    }
    let inventory = collect_export_inventory(api.clone()).await?;
    if !locks_current() || !ilm_recovery_export_fleet_proof_matches(&proof).await {
        return Err(Error::PreconditionFailed);
    }
    let created_at_unix_nanos = now_unix_nanos()?;
    let export = build_export_from_source(&current, creator_sha256, created_at_unix_nanos, &current_source_base64)?;
    let encoded = export.encode()?;
    inventory.check(creator_sha256, encoded.len(), created_at_unix_nanos)?;

    let mut write_options = ObjectOptions {
        max_parity: true,
        write_completion: WriteCompletion::TailDrained,
        http_preconditions: Some(HTTPPreconditions {
            if_none_match: Some("*".to_string()),
            ..Default::default()
        }),
        ..Default::default()
    };
    write_options.add_namespace_lock_guard(&admission_guard);
    write_options.add_namespace_lock_guard(&control_guard);
    write_options.add_namespace_lock_guard(&source_guard);
    if !locks_current() {
        return Err(Error::PreconditionFailed);
    }
    let write_result = config_boundary::save_config_with_opts(api.clone(), &object, encoded.clone(), &write_options).await;
    let stored = match load_recovery_export(api.clone(), &export.export_id).await {
        Ok(stored) if stored.encoded == encoded => stored,
        Ok(_) => return Err(Error::PreconditionFailed),
        Err(read_err) => return Err(write_result.err().unwrap_or(read_err)),
    };
    if !locks_current() || !ilm_recovery_export_fleet_proof_matches(&proof).await {
        return Err(Error::PreconditionFailed);
    }
    api.record_durable_ilm_decommission_progress(&object, &encoded).await?;
    if !locks_current() {
        return Err(Error::PreconditionFailed);
    }
    Ok(stored)
}

pub async fn load_recovery_export(api: Arc<ECStore>, export_id: &str) -> Result<IlmRecoveryExportCreated> {
    let (created, _) = load_recovery_export_decoded(api, export_id).await?;
    Ok(created)
}

async fn load_recovery_export_decoded(
    api: Arc<ECStore>,
    export_id: &str,
) -> Result<(IlmRecoveryExportCreated, IlmRecoveryExport)> {
    let object = recovery_export_record_object_name(IlmRecoveryProtocol::TierDeleteJournal, export_id)?;
    let encoded = config_boundary::read_config_limited_preserve_empty(api, &object, MAX_ILM_RECOVERY_EXPORT_SIZE).await?;
    let export = IlmRecoveryExport::decode(export_id, &encoded)?;
    let content_sha256 = hex_sha256(&encoded, ToOwned::to_owned);
    Ok((
        IlmRecoveryExportCreated {
            export_id: export.export_id.clone(),
            content_sha256,
            encoded,
            replayed: false,
        },
        export,
    ))
}

impl IlmRecoveryExportCreated {
    fn with_replayed(mut self) -> Self {
        self.replayed = true;
        self
    }
}

async fn load_exportable_control(api: Arc<ECStore>, control_id: &str) -> Result<ObservedIlmRecoveryControl> {
    load_exportable_control_with_options(api, control_id, &ObjectOptions::default()).await
}

async fn load_exportable_control_no_lock(api: Arc<ECStore>, control_id: &str) -> Result<ObservedIlmRecoveryControl> {
    load_exportable_control_with_options(
        api,
        control_id,
        &ObjectOptions {
            no_lock: true,
            ..Default::default()
        },
    )
    .await
}

async fn load_exportable_control_with_options(
    api: Arc<ECStore>,
    control_id: &str,
    options: &ObjectOptions,
) -> Result<ObservedIlmRecoveryControl> {
    let object = recovery_control_record_object_name(IlmRecoveryProtocol::TierDeleteJournal, control_id).map_err(Error::other)?;
    let (data, metadata) =
        config_boundary::read_config_limited_preserve_empty_with_metadata(api, &object, options, MAX_ILM_RECOVERY_CONTROL_SIZE)
            .await?;
    let etag = metadata
        .etag
        .filter(|etag| !etag.trim().is_empty())
        .ok_or_else(|| Error::other("ILM recovery control is missing an ETag"))?;
    let control = IlmRecoveryControl::decode(control_id, &data).map_err(Error::other)?;
    if control.identity.protocol != IlmRecoveryProtocol::TierDeleteJournal
        || control.classification != IlmRecoveryClassification::RetainedAmbiguous
        || !is_legacy_export_schema(&control.observed_source_generation.source_schema)
    {
        return Err(Error::other("ILM recovery control is not exportable"));
    }
    Ok(ObservedIlmRecoveryControl { control, etag })
}

async fn current_observation_under_proof_no_lock(
    api: Arc<ECStore>,
    expected: &IlmRecoveryExportObservation,
    proof: &crate::services::notification_sys::IlmRecoveryExportFleetProofToken,
) -> Result<(IlmRecoveryExportObservation, Vec<u8>)> {
    let observed_control = load_exportable_control_no_lock(api.clone(), &expected.control_id).await?;
    let observed_source = observe_export_source_no_lock(
        api,
        &observed_control.control.identity.canonical_source_path,
        &observed_control.control.observed_source_generation.source_schema,
    )
    .await?;
    let source_bytes = observed_source
        .canonical_data
        .clone()
        .ok_or_else(|| Error::other("ILM recovery export source copies diverge"))?;
    if !observed_source.is_consistent()
        || observed_source.generation != observed_control.control.observed_source_generation
        || !ilm_recovery_export_fleet_proof_matches(proof).await
    {
        return Err(Error::PreconditionFailed);
    }
    Ok((
        IlmRecoveryExportObservation {
            control_id: expected.control_id.clone(),
            protocol: observed_control.control.identity.protocol,
            control_etag: observed_control.etag,
            control_revision: observed_control.control.revision,
            classification: observed_control.control.classification,
            canonical_source_path: observed_control.control.identity.canonical_source_path,
            source_generation: observed_source.generation,
            topology_generation: ilm_recovery_export_topology_generation(proof),
            member_epochs_sha256: ilm_recovery_export_member_epochs_sha256(proof),
        },
        source_bytes,
    ))
}

async fn observe_export_source(
    api: Arc<ECStore>,
    canonical_path: &str,
    source_schema: &str,
) -> Result<ObservedIlmRecoverySource> {
    if canonical_path.is_empty()
        || canonical_path.starts_with('/')
        || canonical_path.ends_with('/')
        || canonical_path.split('/').any(str::is_empty)
        || !is_legacy_export_schema(source_schema)
    {
        return Err(Error::other("ILM recovery export source identity is invalid"));
    }
    let lock = api.new_ns_lock(RUSTFS_META_BUCKET, canonical_path).await?;
    let _guard = lock.get_read_lock(crate::set_disk::get_lock_acquire_timeout()).await?;
    observe_export_source_no_lock(api, canonical_path, source_schema).await
}

async fn observe_export_source_no_lock(
    api: Arc<ECStore>,
    canonical_path: &str,
    source_schema: &str,
) -> Result<ObservedIlmRecoverySource> {
    let mut copies = Vec::new();
    let mut canonical: Option<(String, String, Vec<u8>)> = None;
    let mut consistent = true;
    for set in api.all_set_disks() {
        let authority = format!("pool-{}/set-{}", set.pool_index, set.set_index);
        let result = config_boundary::read_config_limited_preserve_empty_with_metadata(
            set,
            canonical_path,
            &ObjectOptions {
                no_lock: true,
                ..Default::default()
            },
            MAX_LEGACY_TIER_DELETE_SOURCE_SIZE,
        )
        .await;
        match result {
            Ok((data, metadata)) => {
                if data.is_empty() || data.len() > MAX_LEGACY_TIER_DELETE_SOURCE_SIZE {
                    return Err(Error::other("ILM recovery export source exceeds its protocol size limit"));
                }
                validate_legacy_tier_delete_recovery_source(canonical_path, source_schema, &data)?;
                let etag = metadata
                    .etag
                    .filter(|etag| !etag.trim().is_empty())
                    .ok_or_else(|| Error::other("ILM recovery export source copy is missing an ETag"))?;
                let content_sha256 = hex_sha256(&data, ToOwned::to_owned);
                let encoded_len =
                    u64::try_from(data.len()).map_err(|_| Error::other("ILM recovery export source length does not fit u64"))?;
                copies.push(IlmRecoverySourceCopy {
                    authority,
                    canonical_path: canonical_path.to_string(),
                    etag: etag.clone(),
                    encoded_len,
                    content_sha256: content_sha256.clone(),
                });
                match canonical.as_ref() {
                    Some((first_etag, first_digest, first_data)) => {
                        consistent &= first_etag == &etag && first_digest == &content_sha256 && first_data == &data;
                    }
                    None => canonical = Some((etag, content_sha256, data)),
                }
            }
            Err(err) if export_source_is_missing(&err) => {}
            Err(err) => return Err(err),
        }
    }
    let Some((source_etag, content_sha256, source_bytes)) = canonical else {
        return Err(Error::ConfigNotFound);
    };
    let generation =
        IlmRecoverySourceGeneration::new(source_schema, source_etag, content_sha256, copies).map_err(Error::other)?;
    Ok(ObservedIlmRecoverySource {
        generation,
        canonical_data: consistent.then_some(source_bytes),
    })
}

fn export_source_is_missing(err: &Error) -> bool {
    matches!(
        err,
        Error::ConfigNotFound | Error::FileNotFound | Error::ObjectNotFound(_, _) | Error::VersionNotFound(_, _, _)
    )
}

fn build_export_from_source(
    observation: &IlmRecoveryExportObservation,
    creator_sha256: &str,
    created_at_unix_nanos: i64,
    source_bytes_base64: &str,
) -> Result<IlmRecoveryExport> {
    let retain_until_unix_nanos = created_at_unix_nanos
        .checked_add(EXPORT_RETENTION_NANOS)
        .ok_or_else(|| Error::other("ILM recovery export retention timestamp overflow"))?;
    let export = IlmRecoveryExport {
        export_id: recovery_export_id(&observation.control_id, &observation.source_generation)?,
        control_id: observation.control_id.clone(),
        protocol: observation.protocol,
        control_etag: observation.control_etag.clone(),
        control_revision: observation.control_revision,
        classification: observation.classification,
        canonical_source_path: observation.canonical_source_path.clone(),
        source_generation: observation.source_generation.clone(),
        topology_generation: observation.topology_generation.clone(),
        member_epochs_sha256: observation.member_epochs_sha256.clone(),
        creator_sha256: creator_sha256.to_string(),
        created_at_unix_nanos,
        retain_until_unix_nanos,
        source_bytes_base64: source_bytes_base64.to_string(),
    };
    export.validate()?;
    Ok(export)
}

pub(crate) fn recovery_export_id(control_id: &str, generation: &IlmRecoverySourceGeneration) -> Result<String> {
    validate_sha256(control_id, "ILM recovery export control ID is invalid")?;
    validate_sha256(&generation.content_sha256, "ILM recovery export source checksum is invalid")?;
    validate_sha256(&generation.copy_set_sha256, "ILM recovery export copy-set checksum is invalid")?;
    let mut data = Vec::new();
    for part in [control_id, &generation.content_sha256, &generation.copy_set_sha256] {
        data.extend_from_slice(&(part.len() as u64).to_be_bytes());
        data.extend_from_slice(part.as_bytes());
    }
    Ok(hex_sha256(&data, ToOwned::to_owned))
}

fn export_matches_observation(export: &IlmRecoveryExport, observation: &IlmRecoveryExportObservation) -> bool {
    export.control_id == observation.control_id
        && export.protocol == observation.protocol
        && export.classification == observation.classification
        && export.canonical_source_path == observation.canonical_source_path
        && export.source_generation == observation.source_generation
}

#[derive(Debug, Default)]
struct IlmRecoveryExportInventory {
    count: usize,
    bytes: u64,
    creations: Vec<(i64, String)>,
}

impl IlmRecoveryExportInventory {
    fn check(&self, creator_sha256: &str, candidate_len: usize, now: i64) -> Result<()> {
        let recent_after = now.saturating_sub(60 * 1_000_000_000);
        let cluster_recent = self
            .creations
            .iter()
            .filter(|(created_at, _)| *created_at > recent_after)
            .count();
        let actor_recent = self
            .creations
            .iter()
            .filter(|(created_at, creator)| *created_at > recent_after && creator == creator_sha256)
            .count();
        check_export_admission(self.count, self.bytes, actor_recent, cluster_recent, candidate_len)
    }
}

async fn collect_export_inventory(api: Arc<ECStore>) -> Result<IlmRecoveryExportInventory> {
    let mut marker = None;
    let mut seen_markers = HashSet::new();
    let mut inventory = IlmRecoveryExportInventory::default();
    loop {
        let page = api
            .clone()
            .list_objects_v2(
                RUSTFS_META_BUCKET,
                &format!("{ILM_RECOVERY_EXPORT_PREFIX}/"),
                marker.clone(),
                None,
                1_000,
                false,
                None,
                false,
            )
            .await?;
        for object in page.objects {
            let (_, export_id) = recovery_export_id_from_record_object_name(&object.name)?;
            let (stored, export) = load_recovery_export_decoded(api.clone(), &export_id).await?;
            inventory.count = inventory
                .count
                .checked_add(1)
                .ok_or_else(|| Error::other("ILM recovery export count overflow"))?;
            inventory.bytes = inventory
                .bytes
                .checked_add(u64::try_from(stored.encoded.len()).map_err(|_| Error::other("ILM recovery export size overflow"))?)
                .ok_or_else(|| Error::other("ILM recovery export byte total overflow"))?;
            inventory
                .creations
                .push((export.created_at_unix_nanos, export.creator_sha256));
        }
        if !page.is_truncated {
            break;
        }
        let next = page
            .next_continuation_token
            .ok_or_else(|| Error::other("ILM recovery export inventory omitted its continuation marker"))?;
        marker = Some(record_export_inventory_marker(&mut seen_markers, next)?);
    }
    Ok(inventory)
}

fn record_export_inventory_marker(seen_markers: &mut HashSet<String>, next: String) -> Result<String> {
    if !seen_markers.insert(next.clone()) {
        return Err(Error::other("ILM recovery export inventory repeated its continuation marker"));
    }
    Ok(next)
}

fn check_export_admission(
    count: usize,
    bytes: u64,
    actor_recent: usize,
    cluster_recent: usize,
    candidate_len: usize,
) -> Result<()> {
    let candidate_len = u64::try_from(candidate_len).map_err(|_| Error::other("ILM recovery export size does not fit u64"))?;
    if count >= MAX_ILM_RECOVERY_EXPORTS
        || bytes
            .checked_add(candidate_len)
            .is_none_or(|total| total > MAX_ILM_RECOVERY_EXPORT_BYTES)
        || actor_recent >= MAX_ACTOR_EXPORTS_PER_MINUTE
        || cluster_recent >= MAX_CLUSTER_EXPORTS_PER_MINUTE
    {
        return Err(Error::SlowDown);
    }
    Ok(())
}

fn is_legacy_export_schema(schema: &str) -> bool {
    matches!(schema, TIER_DELETE_JOURNAL_V1_RECOVERY_SCHEMA | TIER_DELETE_JOURNAL_V2_RECOVERY_SCHEMA)
}

fn validate_sha256(value: &str, message: &'static str) -> Result<()> {
    if !is_sha256_checksum(value)
        || value
            .bytes()
            .any(|byte| byte.is_ascii_hexdigit() && byte.is_ascii_uppercase())
    {
        return Err(Error::other(message));
    }
    Ok(())
}

fn now_unix_nanos() -> Result<i64> {
    i64::try_from(time::OffsetDateTime::now_utc().unix_timestamp_nanos())
        .map_err(|_| Error::other("ILM recovery export timestamp does not fit i64"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bucket::lifecycle::recovery_control::IlmRecoverySourceCopy;

    const PINNED_V1_EXPORT: &[u8] = br#"{"schema":"rustfs-ilm-recovery-export-v1","content_sha256":"3dfb3ec3892256e909de1211c1a963ca7008963ff32b3a869f7161a7b9b44028","export":{"export_id":"2b78e7a825bfc2edbf7f773d0b6ed3bf93e360ff1702d73a449109c11bfaa105","control_id":"0fcd568a5cb9bdb4677b69354b11ee415af8f784519cff3da49a26f84eaee7f2","protocol":"tier_delete_journal","control_etag":"control-etag","control_revision":1,"classification":"retained_ambiguous","canonical_source_path":"ilm/tier-delete-journal/872072554f66ab326f10ce7adbae11422b7a4b0663aa7112d6061a8f6ed41b94.json","source_generation":{"source_schema":"rustfs-tier-delete-journal-v1","source_etag":"etag-a","content_sha256":"0e0b010ebdeeb7b41473fe8575e989d6bb1303c0ca551dd984e9400f0ae306bd","copy_set_sha256":"5a7406115b6c3923ffe79dcd1f43ccae7beed786e557163f019dd10ec409a653","copies":[{"authority":"pool-0/set-0","canonical_path":"ilm/tier-delete-journal/872072554f66ab326f10ce7adbae11422b7a4b0663aa7112d6061a8f6ed41b94.json","etag":"etag-a","encoded_len":81,"content_sha256":"0e0b010ebdeeb7b41473fe8575e989d6bb1303c0ca551dd984e9400f0ae306bd"}]},"topology_generation":"e6e2b826e31fca5c36125c48f130dcb6f961e698ff8a8776a1f290cf0892e8e6","member_epochs_sha256":"612dd8a861161819a4ad8f6f3e2a0567602877c043a2353ca933a13c78dc0ed4","creator_sha256":"50c9c4aeb40b5b206b6d98f516f8b8c0efd29ce2e56a76b345fb9240c225a1b7","created_at_unix_nanos":1000000000,"retain_until_unix_nanos":7776001000000000,"source_bytes_base64":"eyJ2ZXJzaW9uIjoxLCJvYmpfbmFtZSI6ImxlZ2FjeS9yZW1vdGUiLCJ2ZXJzaW9uX2lkIjoib3BhcXVlIiwidGllcl9uYW1lIjoiV0FSTSJ9"}}"#;

    fn legacy_source() -> Vec<u8> {
        br#"{"version":1,"obj_name":"legacy/remote","version_id":"opaque","tier_name":"WARM"}"#.to_vec()
    }

    fn observation() -> IlmRecoveryExportObservation {
        let source = legacy_source();
        let source_path = super::super::tier_delete_journal::tier_delete_journal_object_name(
            &super::super::tier_delete_journal::decode_tier_delete_journal_entry(&source).expect("legacy fixture should decode"),
        );
        let source_sha256 = hex_sha256(&source, ToOwned::to_owned);
        let generation = IlmRecoverySourceGeneration::new(
            TIER_DELETE_JOURNAL_V1_RECOVERY_SCHEMA,
            "etag-a",
            source_sha256.clone(),
            vec![IlmRecoverySourceCopy {
                authority: "pool-0/set-0".to_string(),
                canonical_path: source_path.clone(),
                etag: "etag-a".to_string(),
                encoded_len: source.len() as u64,
                content_sha256: source_sha256,
            }],
        )
        .expect("generation should be valid");
        IlmRecoveryExportObservation {
            control_id: hex_sha256(b"control", ToOwned::to_owned),
            protocol: IlmRecoveryProtocol::TierDeleteJournal,
            control_etag: "control-etag".to_string(),
            control_revision: 1,
            classification: IlmRecoveryClassification::RetainedAmbiguous,
            canonical_source_path: source_path,
            source_generation: generation,
            topology_generation: hex_sha256(b"topology", ToOwned::to_owned),
            member_epochs_sha256: hex_sha256(b"epochs", ToOwned::to_owned),
        }
    }

    #[test]
    fn recovery_export_round_trip_is_strict_and_deterministic() {
        let observed = observation();
        let creator = hex_sha256(b"actor", ToOwned::to_owned);
        let export = build_export_from_source(
            &observed,
            &creator,
            1_000_000_000,
            &base64_simd::STANDARD.encode_to_string(legacy_source()),
        )
        .expect("export should be valid");
        assert_eq!(
            export.export_id,
            recovery_export_id(&observed.control_id, &observed.source_generation).unwrap()
        );
        let encoded = export.encode().expect("export should encode");
        assert_eq!(encoded, PINNED_V1_EXPORT, "v1 export wire format must remain pinned");
        assert_eq!(IlmRecoveryExport::decode(&export.export_id, &encoded).unwrap(), export);
        assert_eq!(
            IlmRecoveryExport::decode("2b78e7a825bfc2edbf7f773d0b6ed3bf93e360ff1702d73a449109c11bfaa105", PINNED_V1_EXPORT)
                .unwrap(),
            export,
        );

        let path = recovery_export_record_object_name(export.protocol, &export.export_id).unwrap();
        let durable = super::super::durable_namespace::validate_durable_ilm_record(&path, &encoded)
            .expect("export should be registered as a durable ILM record");
        assert_eq!(durable.namespace, "recovery-export");
        assert_eq!(durable.id_kind, "export_id");
        assert_eq!(durable.id, export.export_id);

        let mut wrong_source = export.clone();
        wrong_source.source_bytes_base64 = base64_simd::STANDARD.encode_to_string(b"changed");
        assert!(wrong_source.encode().is_err());

        let mut persisted: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
        persisted["unknown"] = serde_json::json!(true);
        assert!(IlmRecoveryExport::decode(&export.export_id, &serde_json::to_vec(&persisted).unwrap()).is_err());
    }

    #[test]
    fn export_inventory_rejects_non_adjacent_continuation_cycles() {
        let mut seen = HashSet::new();
        assert_eq!(record_export_inventory_marker(&mut seen, "a".to_string()).unwrap(), "a");
        assert_eq!(record_export_inventory_marker(&mut seen, "b".to_string()).unwrap(), "b");
        record_export_inventory_marker(&mut seen, "a".to_string())
            .expect_err("a non-adjacent continuation marker cycle must fail closed");
    }

    #[test]
    fn recovery_export_path_rejects_noncanonical_shards() {
        let id = hex_sha256(b"export", ToOwned::to_owned);
        let path = recovery_export_record_object_name(IlmRecoveryProtocol::TierDeleteJournal, &id).unwrap();
        assert_eq!(recovery_export_id_from_record_object_name(&path).unwrap().1, id);
        let wrong_shard = path.replacen(&format!("/{}/", &id[..2]), "/zz/", 1);
        assert!(recovery_export_id_from_record_object_name(&wrong_shard).is_err());
    }

    #[test]
    fn canonical_replay_survives_fleet_rotation_but_not_source_change() {
        let observed = observation();
        let creator = hex_sha256(b"actor", ToOwned::to_owned);
        let export = build_export_from_source(
            &observed,
            &creator,
            1_000_000_000,
            &base64_simd::STANDARD.encode_to_string(legacy_source()),
        )
        .unwrap();
        let mut rotated = observed;
        rotated.control_etag = "new-control-etag".to_string();
        rotated.control_revision += 1;
        rotated.topology_generation = hex_sha256(b"new-topology", ToOwned::to_owned);
        rotated.member_epochs_sha256 = hex_sha256(b"new-members", ToOwned::to_owned);
        assert!(export_matches_observation(&export, &rotated));

        rotated.source_generation.content_sha256 = hex_sha256(b"changed", ToOwned::to_owned);
        assert!(!export_matches_observation(&export, &rotated));
    }

    #[test]
    fn export_admission_enforces_exact_count_byte_and_rate_boundaries() {
        assert!(check_export_admission(9_999, MAX_ILM_RECOVERY_EXPORT_BYTES - 1, 9, 99, 1).is_ok());
        assert!(check_export_admission(10_000, 0, 0, 0, 1).is_err());
        assert!(check_export_admission(0, MAX_ILM_RECOVERY_EXPORT_BYTES, 0, 0, 1).is_err());
        assert!(check_export_admission(0, 0, 10, 0, 1).is_err());
        assert!(check_export_admission(0, 0, 0, 100, 1).is_err());
    }
}
