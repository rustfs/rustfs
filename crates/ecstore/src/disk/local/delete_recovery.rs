use super::*;
use crate::disk::{
    DELETE_RECOVERY_PROTOCOL_VERSION, DeleteRecoveryAction, DeleteRecoveryCursor, DeleteRecoveryDigest, DeleteRecoveryInspection,
    DeleteRecoveryJournal, DeleteRecoveryPage, DeleteRecoveryState, DeleteTransactionId,
};
use sha2::{Digest, Sha256};

pub(super) const PREPARED: &str = "delete-prepared";
pub(super) const TARGET: &str = "delete-target";
pub(super) const FINISHED: &str = "delete-commit-finished";
const MAX_METADATA: u64 = 64 * 1024 * 1024;
struct DiscoverySession {
    disk_root: PathBuf,
    volume: String,
    object: Option<String>,
    root: PathBuf,
    _mount: std::fs::File,
    frames: Vec<(String, std::fs::ReadDir)>,
    touched: Instant,
}

static DISCOVERY_SESSIONS: std::sync::LazyLock<ParkingLotMutex<HashMap<Uuid, DiscoverySession>>> =
    std::sync::LazyLock::new(|| ParkingLotMutex::new(HashMap::new()));

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct CommitReceipt {
    journal: DeleteRecoveryJournal,
    finished: bool,
}

pub(super) struct DeletePreparationContext {
    pub transaction: Uuid,
    pub bucket_incarnation: Option<Uuid>,
    pub namespace_owner: Option<Arc<dyn Send + Sync>>,
}

pub(super) fn digest(bytes: &[u8]) -> DeleteRecoveryDigest {
    Sha256::digest(bytes).into()
}

pub(in crate::disk) fn generation(mut canonical: FileInfo) -> Result<crate::disk::DeleteRecoveryVersionIdentity> {
    canonical.data = None;
    canonical.erasure.index = 0;
    canonical.erasure.checksums.clear();
    canonical.is_latest = false;
    canonical.num_versions = 0;
    canonical.successor_mod_time = None;
    rustfs_filemeta::shard_integrity::clear_integrity_metadata(&mut canonical.metadata);
    // Only schema-declared string fields and the inline presence marker are
    // aliases here. Unknown suffixes and opaque binary fields stay distinct.
    use rustfs_utils::http::metadata_compat as compat;
    for suffix in [
        compat::SUFFIX_INLINE_DATA,
        compat::SUFFIX_COMPRESSION,
        compat::SUFFIX_COMPRESSION_SIZE,
        compat::SUFFIX_ACTUAL_SIZE,
        compat::SUFFIX_ACTUAL_OBJECT_SIZE,
        compat::SUFFIX_ACTUAL_OBJECT_SIZE_CAP,
        compat::SUFFIX_TRANSITION_STATUS,
        compat::SUFFIX_TRANSITIONED_OBJECTNAME,
        compat::SUFFIX_TRANSITION_TIER,
        compat::SUFFIX_TRANSITIONED_VERSION_STATE,
        compat::SUFFIX_TRANSITION_TIER_DESTINATION_ID,
        compat::SUFFIX_PURGESTATUS,
        compat::SUFFIX_REPLICA_STATUS,
        compat::SUFFIX_REPLICATION_STATUS,
        compat::SUFFIX_REPLICA_TIMESTAMP,
        compat::SUFFIX_REPLICATION_TIMESTAMP,
        compat::SUFFIX_TAGGING_TIMESTAMP,
        compat::SUFFIX_OBJECTLOCK_RETENTION_TIMESTAMP,
        compat::SUFFIX_OBJECTLOCK_LEGALHOLD_TIMESTAMP,
    ] {
        if !compat::contains_key_str(&canonical.metadata, suffix) {
            continue;
        }
        // The inline marker is a presence flag whose persisted value is empty.
        // Compare both declared aliases explicitly before normalizing it; the
        // general compatibility reader rejects empty strings by design.
        let value = if suffix == compat::SUFFIX_INLINE_DATA {
            let rustfs_key = compat::internal_key_rustfs(suffix);
            let minio_key = format!("{}{suffix}", compat::MINIO_INTERNAL_PREFIX);
            let mut value = None;
            for (key, candidate) in &canonical.metadata {
                if !key.eq_ignore_ascii_case(&rustfs_key) && !key.eq_ignore_ascii_case(&minio_key) {
                    continue;
                }
                if value.is_some_and(|current: &String| current != candidate) {
                    return Err(DiskError::FileCorrupt);
                }
                value = Some(candidate);
            }
            value.ok_or(DiskError::FileCorrupt)?.clone()
        } else {
            compat::get_consistent_str(&canonical.metadata, suffix)
                .ok_or(DiskError::FileCorrupt)?
                .to_owned()
        };
        if value.is_empty() && suffix != compat::SUFFIX_INLINE_DATA {
            continue;
        }
        compat::insert_str(&mut canonical.metadata, suffix, value);
    }
    fn ordered(value: serde_json::Value) -> serde_json::Value {
        match value {
            serde_json::Value::Object(map) => {
                let sorted: std::collections::BTreeMap<_, _> = map.into_iter().collect();
                serde_json::Value::Object(sorted.into_iter().map(|(key, value)| (key, ordered(value))).collect())
            }
            serde_json::Value::Array(values) => serde_json::Value::Array(values.into_iter().map(ordered).collect()),
            other => other,
        }
    }
    Ok(crate::disk::DeleteRecoveryVersionIdentity {
        version_id: canonical.version_id,
        mod_time: canonical.mod_time,
        data_dir: canonical.data_dir,
        deleted: canonical.deleted,
        metadata_digest: digest(&serde_json::to_vec(&ordered(serde_json::to_value(canonical)?))?),
    })
}

fn safe_path(path: &str, empty: bool) -> bool {
    (empty && path.is_empty())
        || (!path.is_empty() && !path.starts_with('/') && crate::bucket::utils::is_valid_object_prefix(path))
}

fn regular_metadata_bytes(path: &Path) -> Result<Option<Vec<u8>>> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.is_file() => Ok(Some(std::fs::read(path).map_err(to_file_error)?)),
        Ok(_) => Err(DiskError::FileCorrupt),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(None),
        Err(err) => Err(to_file_error(err).into()),
    }
}

fn regular_bytes(path: &Path) -> Result<Option<Vec<u8>>> {
    if std::fs::symlink_metadata(path).is_ok_and(|meta| meta.len() > MAX_METADATA) {
        return Err(DiskError::FileCorrupt);
    }
    regular_metadata_bytes(path)
}

/// A write/heal backup is not a delete transaction when the live metadata
/// still owns its directory and retains every backed-up generation. Any delete
/// evidence wins; uncertain backups remain discoverable and fail closed.
fn live_write_backup(root: &Path, transaction: Uuid) -> Result<bool> {
    let rollback = root.join(transaction.to_string());
    for name in [
        PREPARED,
        TARGET,
        FINISHED,
        DELETE_MARKER_ROLLBACK_FILE,
        DELETE_QUORUM_COMMITTED_FILE,
    ] {
        if regular_bytes(&rollback.join(name))?.is_some() {
            return Ok(false);
        }
    }
    for prefix in [
        DELETE_DATA_DIR_MARKER_PREFIX,
        RESERVED_DELETE_DATA_DIR_MARKER_PREFIX,
        RECOVERY_DELETE_DATA_DIR_MARKER_PREFIX,
    ] {
        let name = format!("{prefix}{transaction}");
        if regular_bytes(&root.join(&name))?.is_some() || regular_bytes(&rollback.join(name))?.is_some() {
            return Ok(false);
        }
    }
    let Some(current) = regular_metadata_bytes(&root.join(STORAGE_FORMAT_FILE))? else {
        return Ok(false);
    };
    let Some(backup) = regular_metadata_bytes(&rollback.join(STORAGE_FORMAT_FILE_BACKUP))? else {
        return Ok(false);
    };
    live_write_backup_matches(&FileMeta::load(&current)?, &FileMeta::load(&backup)?, transaction)
}

fn live_write_backup_matches(current: &FileMeta, backup: &FileMeta, transaction: Uuid) -> Result<bool> {
    let mut owns_directory = false;
    let used_data_dirs: HashSet<_> = current.get_data_dirs()?.into_iter().flatten().collect();
    let mut live = HashMap::with_capacity(current.versions.len());
    for version in &current.versions {
        let mut info = version.into_fileinfo("", "", true)?;
        let id = info.version_id.unwrap_or_default();
        owns_directory |=
            info.data_dir == Some(transaction) || inline_metadata_rollback_dir_with_used_dirs(id, &used_data_dirs) == transaction;
        rustfs_utils::http::metadata_compat::remove_str(&mut info.metadata, rustfs_utils::http::metadata_compat::SUFFIX_HEALING);
        if live.insert(id, generation(info)?).is_some() {
            return Err(DiskError::FileCorrupt);
        }
    }
    if !owns_directory || backup.versions.is_empty() {
        return Ok(false);
    }
    let mut seen = HashSet::with_capacity(backup.versions.len());
    for version in &backup.versions {
        let mut info = version.into_fileinfo("", "", true)?;
        let id = info.version_id.unwrap_or_default();
        if !seen.insert(id) {
            return Err(DiskError::FileCorrupt);
        }
        rustfs_utils::http::metadata_compat::remove_str(&mut info.metadata, rustfs_utils::http::metadata_compat::SUFFIX_HEALING);
        if live.get(&id) != Some(&generation(info)?) {
            return Ok(false);
        }
    }
    Ok(true)
}

fn journal_bytes(journal: &DeleteRecoveryJournal) -> Result<Vec<u8>> {
    let bytes = serde_json::to_vec(journal)?;
    // The same journal is carried by adoption requests and decision receipts.
    if bytes.len() > usize::try_from(MAX_METADATA).map_err(|_| DiskError::FileCorrupt)? - 128 {
        return Err(DiskError::FileCorrupt);
    }
    let projected = crate::disk::DeleteRecoveryRpcReply::Inspected(DeleteRecoveryInspection {
        protocol_version: DELETE_RECOVERY_PROTOCOL_VERSION,
        id: DeleteTransactionId {
            object: journal.object.clone(),
            transaction: journal.transaction,
        },
        state: DeleteRecoveryState::NewPrepared,
        journal: Some(journal.clone()),
        current_digest: journal.original_digest,
        current_versions: journal.original_versions.clone(),
        backup_digest: journal.original_digest,
        rollback_data_complete: true,
        commit_data_safe: true,
        explicit_delete_safe: false,
        fingerprint: [0; 32],
    });
    if rmp_serde::to_vec_named(&projected)?.len() > rustfs_protos::DELETE_RECOVERY_MAX_PAYLOAD_BYTES - 4096 {
        return Err(DiskError::FileCorrupt);
    }
    Ok(bytes)
}

fn regular_marker(path: &Path) -> Result<bool> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.is_file() && meta.len() == 0 => Ok(true),
        Ok(_) => Err(DiskError::FileCorrupt),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(false),
        Err(err) => Err(to_file_error(err).into()),
    }
}

fn complete_part_sources(live: &Path, staged: &Path, info: &FileInfo) -> Result<bool> {
    let layout = info.validate(ValidationMode::RequireErasure)?.ok_or(DiskError::FileCorrupt)?;
    for part in &info.parts {
        let expected = u64::try_from(layout.shard_file_size(part.size).ok_or(DiskError::FileCorrupt)?)
            .map_err(|_| DiskError::FileCorrupt)?;
        let name = format!("part.{}", part.number);
        if ![live, staged]
            .iter()
            .any(|root| std::fs::symlink_metadata(root.join(&name)).is_ok_and(|meta| meta.is_file() && meta.len() >= expected))
        {
            return Ok(false);
        }
        if let Some(integrity) = &part.integrity {
            let name = integrity.file_name();
            let length = u64::try_from(integrity.index_size()?).map_err(|_| DiskError::FileCorrupt)?;
            if ![live, staged]
                .iter()
                .any(|root| std::fs::symlink_metadata(root.join(&name)).is_ok_and(|meta| meta.is_file() && meta.len() == length))
            {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

async fn verify_recovery_integrity_sources(
    live: &Path,
    staged: &Path,
    integrity: &rustfs_filemeta::shard_integrity::PartIntegrity,
    coding_index: usize,
    algorithm: HashAlgorithm,
    owner: Option<Arc<dyn Send + Sync>>,
) -> Result<()> {
    let lease = os::acquire_namespace_mutation_lease_with_owner(live, owner).await;
    let roots = [live.to_path_buf(), staged.to_path_buf()];
    let integrity = integrity.clone();
    os::run_blocking_namespace_operation(lease, move || {
        Ok((|| {
            integrity.validate()?;
            if coding_index >= integrity.layout.shards()
                || !matches!(algorithm, HashAlgorithm::HighwayHash256S | HashAlgorithm::HighwayHash256SLegacy)
            {
                return Err(DiskError::FileCorrupt);
            }
            let codec = coding::Erasure::try_new_with_options(
                usize::from(integrity.layout.data),
                usize::from(integrity.layout.parity),
                usize::try_from(integrity.layout.block_size).map_err(|_| DiskError::FileCorrupt)?,
                integrity.layout.legacy,
            )
            .map_err(DiskError::from)?;
            let shard_size = codec.shard_size();
            let part_size =
                usize::try_from(codec.shard_file_size(i64::try_from(integrity.size).map_err(|_| DiskError::FileCorrupt)?))
                    .map_err(|_| DiskError::FileCorrupt)?;
            let encoded_size = u64::try_from(coding::bitrot_shard_file_size(part_size, shard_size, algorithm.clone()))
                .map_err(|_| DiskError::FileCorrupt)?;
            let expected = u64::try_from(integrity.index_size()?).map_err(|_| DiskError::FileCorrupt)?;
            let header = integrity.index_header();
            let mut checked = false;
            for root in &roots {
                let path = root.join(integrity.file_name());
                match std::fs::symlink_metadata(&path) {
                    Ok(metadata) if metadata.is_file() && metadata.len() == expected => {}
                    Ok(_) => return Err(DiskError::FileCorrupt),
                    Err(err) if err.kind() == ErrorKind::NotFound => continue,
                    Err(err) => return Err(to_file_error(err).into()),
                }
                let mut file = std::fs::File::open(path).map_err(to_file_error)?;
                let mut actual = [0u8; rustfs_filemeta::shard_integrity::INDEX_HEADER_SIZE];
                std::io::Read::read_exact(&mut file, &mut actual).map_err(to_file_error)?;
                if actual != header {
                    return Err(DiskError::FileCorrupt);
                }
                // An interrupted restore can split payload and proof between
                // live and staged roots. Verify every present payload copy
                // against the declared proof, independently of its location.
                let mut shards = Vec::new();
                for root in &roots {
                    let path = root.join(format!("part.{}", integrity.number));
                    match std::fs::symlink_metadata(&path) {
                        Ok(metadata) if metadata.is_file() && metadata.len() == encoded_size => {
                            shards.push(std::fs::File::open(path).map_err(to_file_error)?);
                        }
                        Ok(_) => return Err(DiskError::FileCorrupt),
                        Err(err) if err.kind() == ErrorKind::NotFound => {}
                        Err(err) => return Err(to_file_error(err).into()),
                    }
                }
                if shards.is_empty() {
                    return Err(DiskError::FileCorrupt);
                }
                let mut record = vec![0; integrity.record_size()];
                let mut payload = vec![0; shard_size];
                let mut bitrot_hash = vec![0; algorithm.size()];
                let mut remaining = part_size;
                for stripe in 0..integrity.stripes {
                    std::io::Read::read_exact(&mut file, &mut record).map_err(to_file_error)?;
                    integrity.verify_record(stripe, &record)?;
                    let length = remaining.min(shard_size);
                    if length == 0 {
                        return Err(DiskError::FileCorrupt);
                    }
                    for shard in &mut shards {
                        std::io::Read::read_exact(shard, &mut bitrot_hash).map_err(to_file_error)?;
                        std::io::Read::read_exact(shard, &mut payload[..length]).map_err(to_file_error)?;
                        integrity.verify_shard(stripe, coding_index, &payload[..length], &record)?;
                    }
                    remaining -= length;
                }
                if remaining != 0 {
                    return Err(DiskError::FileCorrupt);
                }
                if integrity.stripes == 0 && integrity.root != integrity.root_digest(&integrity.padding_digest(0)) {
                    return Err(DiskError::FileCorrupt);
                }
                checked = true;
            }
            if !checked {
                return Err(DiskError::FileCorrupt);
            }
            Ok(())
        })())
    })
    .await
    .map_err(to_file_error)?
}

struct ExplicitDeletePlan {
    data_dirs: Vec<PathBuf>,
    proof_files: Vec<PathBuf>,
}

fn explicit_delete_plan(
    root: &Path,
    transaction: Uuid,
    original: Option<&FileMeta>,
    journal: Option<&DeleteRecoveryJournal>,
    current_missing: bool,
) -> Result<Option<ExplicitDeletePlan>> {
    if !current_missing
        || journal
            .is_some_and(|journal| journal.target_digest.is_some() || (journal.original_digest.is_some() && original.is_none()))
    {
        return Ok(None);
    }
    let mut referenced = HashSet::new();
    if let Some(metadata) = original {
        if metadata.versions.len() != 1 || metadata.versions[0].header.version_type != rustfs_filemeta::VersionType::Object {
            return Ok(None);
        }
        let info = metadata.versions[0].into_fileinfo("", "", false)?;
        if info.version_id.is_some_and(|id| !id.is_nil())
            || info.deleted
            || info.is_remote()
            || info.metadata.keys().any(|key| {
                let key = key.to_ascii_lowercase();
                key.starts_with("x-amz-object-lock-")
                    || rustfs_utils::http::metadata_compat::internal_key_strip_suffix_prefix(&key, "objectlock-").is_some()
            })
        {
            return Ok(None);
        }
        referenced.extend(metadata.get_data_dirs()?.into_iter().flatten().filter(|id| !id.is_nil()));
    }
    let rollback = root.join(transaction.to_string());
    let marker_names = [
        DELETE_DATA_DIR_MARKER_PREFIX,
        RESERVED_DELETE_DATA_DIR_MARKER_PREFIX,
        RECOVERY_DELETE_DATA_DIR_MARKER_PREFIX,
    ]
    .map(|prefix| format!("{prefix}{transaction}"));
    let mut data_dirs = Vec::new();
    let mut proof_files = Vec::new();
    let mut candidates = Vec::new();
    match std::fs::read_dir(&rollback) {
        Ok(entries) => {
            for entry in entries {
                let entry = entry.map_err(to_file_error)?;
                let kind = entry.file_type().map_err(to_file_error)?;
                let name = entry.file_name().into_string().map_err(|_| DiskError::FileCorrupt)?;
                if kind.is_dir() && Uuid::parse_str(&name).is_ok_and(|id| referenced.contains(&id)) {
                    candidates.push(entry.path());
                } else if kind.is_file()
                    && [
                        STORAGE_FORMAT_FILE_BACKUP,
                        DELETE_MARKER_ROLLBACK_FILE,
                        DELETE_QUORUM_COMMITTED_FILE,
                        PREPARED,
                        TARGET,
                        FINISHED,
                    ]
                    .contains(&name.as_str())
                {
                    proof_files.push(entry.path());
                } else if kind.is_file()
                    && (marker_names.contains(&name) || (delete_recovery_part_file(&name) && referenced.contains(&transaction)))
                {
                    candidates.push(rollback.clone());
                } else {
                    return Ok(None);
                }
            }
        }
        Err(err) if err.kind() == ErrorKind::NotFound => {}
        Err(err) => return Err(to_file_error(err).into()),
    }
    match std::fs::read_dir(root) {
        Ok(entries) => {
            for entry in entries {
                let entry = entry.map_err(to_file_error)?;
                let name = entry.file_name().into_string().map_err(|_| DiskError::FileCorrupt)?;
                let Some(id) = Uuid::parse_str(&name).ok().filter(|id| !id.is_nil()) else {
                    continue;
                };
                if !entry.file_type().map_err(to_file_error)?.is_dir() {
                    continue;
                }
                let own_marker = marker_names
                    .iter()
                    .any(|name| std::fs::symlink_metadata(entry.path().join(name)).is_ok_and(|metadata| metadata.is_file()));
                if referenced.contains(&id) || own_marker {
                    candidates.push(entry.path());
                }
            }
        }
        Err(err) if err.kind() == ErrorKind::NotFound => {}
        Err(err) => return Err(to_file_error(err).into()),
    }
    candidates.sort();
    candidates.dedup();
    for dir in candidates {
        let mut has_parts = false;
        for entry in std::fs::read_dir(&dir).map_err(to_file_error)? {
            let entry = entry.map_err(to_file_error)?;
            if !entry.file_type().map_err(to_file_error)?.is_file() {
                return Ok(None);
            }
            let name = entry.file_name().into_string().map_err(|_| DiskError::FileCorrupt)?;
            if delete_recovery_part_file(&name) {
                has_parts = true;
            } else if !(marker_names.contains(&name)
                || (dir == rollback
                    && [
                        STORAGE_FORMAT_FILE_BACKUP,
                        DELETE_MARKER_ROLLBACK_FILE,
                        DELETE_QUORUM_COMMITTED_FILE,
                        PREPARED,
                        TARGET,
                        FINISHED,
                    ]
                    .contains(&name.as_str())))
            {
                return Ok(None);
            }
        }
        if has_parts && original.is_none() {
            return Ok(None);
        }
        data_dirs.push(dir);
    }
    data_dirs.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
    Ok(Some(ExplicitDeletePlan { data_dirs, proof_files }))
}

fn inspection(root: &Path, volume: &str, object: &str, transaction: Uuid) -> Result<DeleteRecoveryInspection> {
    let rollback = root.join(transaction.to_string());
    let before = regular_metadata_bytes(&rollback.join(STORAGE_FORMAT_FILE_BACKUP))?;
    let current = regular_metadata_bytes(&root.join(STORAGE_FORMAT_FILE))?;
    let prepared = regular_bytes(&rollback.join(PREPARED))?;
    let target = regular_metadata_bytes(&rollback.join(TARGET))?;
    let receipt_bytes = regular_bytes(&rollback.join(DELETE_QUORUM_COMMITTED_FILE))?;
    let receipt: Option<CommitReceipt> = receipt_bytes
        .as_deref()
        .filter(|bytes| !bytes.is_empty())
        .map(serde_json::from_slice)
        .transpose()?;
    let committed = receipt.is_some();
    let finished = regular_marker(&rollback.join(FINISHED))? || receipt.as_ref().is_some_and(|receipt| receipt.finished);
    let journal: Option<DeleteRecoveryJournal> = prepared
        .as_deref()
        .map(serde_json::from_slice)
        .transpose()?
        .or_else(|| receipt.as_ref().map(|receipt| receipt.journal.clone()));
    if receipt
        .as_ref()
        .is_some_and(|receipt| journal.as_ref() != Some(&receipt.journal))
    {
        return Err(DiskError::FileCorrupt);
    }
    if let Some(journal) = &journal
        && (journal.protocol_version != DELETE_RECOVERY_PROTOCOL_VERSION
            || journal.volume != volume
            || journal.object != object
            || journal.transaction != transaction
            || journal.transaction.is_nil()
            || journal.data_dirs.iter().any(|id| id.is_nil() || *id == transaction))
    {
        return Err(DiskError::FileCorrupt);
    }
    let backup_digest = before.as_deref().map(digest);
    let current_digest = current.as_deref().map(digest);
    let prepared_digest = prepared.as_deref().map(digest);
    let target_digest = target.as_deref().map(digest);
    let receipt_digest = receipt_bytes.as_deref().map(digest);
    if let Some(journal) = &journal {
        if (!finished
            && journal.target_digest != target_digest
            && !(target.is_none()
                && !committed
                && before.is_none()
                && journal.original_digest.is_some()
                && current_digest == journal.original_digest))
            || target_digest.is_some_and(|hash| journal.target_digest != Some(hash))
            || backup_digest.is_some_and(|hash| journal.original_digest != Some(hash))
        {
            return Err(DiskError::FileCorrupt);
        }
        if let Some(bytes) = &target {
            FileMeta::load(bytes)?;
        }
    }
    let current_meta = current.as_deref().map(FileMeta::load).transpose()?;
    let before_meta = before.as_deref().map(FileMeta::load).transpose()?;
    let mut fingerprint = Sha256::new();
    for hash in [prepared_digest, target_digest, backup_digest, current_digest, receipt_digest] {
        fingerprint.update([u8::from(hash.is_some())]);
        if let Some(hash) = hash {
            fingerprint.update(hash);
        }
    }
    fingerprint.update([u8::from(committed), u8::from(finished)]);
    let mut any_marker = false;
    let mut descriptions = Vec::new();
    let entries = match std::fs::read_dir(root) {
        Ok(entries) => Some(entries),
        Err(err) if err.kind() == ErrorKind::NotFound => None,
        Err(err) => return Err(to_file_error(err).into()),
    };
    if let Some(entries) = entries {
        for entry in entries {
            let entry = entry.map_err(to_file_error)?;
            let kind = entry.file_type().map_err(to_file_error)?;
            if !kind.is_file() && !kind.is_dir() {
                return Err(DiskError::InvalidPath);
            }
            let name = entry.file_name().into_string().map_err(|_| DiskError::FileCorrupt)?;
            descriptions.push((name.clone(), kind.is_dir(), entry.metadata().map_err(to_file_error)?.len()));
            if kind.is_dir() && Uuid::parse_str(&name).is_ok_and(|id| !id.is_nil()) {
                for child in std::fs::read_dir(entry.path()).map_err(to_file_error)? {
                    let child = child.map_err(to_file_error)?;
                    let kind = child.file_type().map_err(to_file_error)?;
                    if !kind.is_dir() && !kind.is_file() {
                        return Err(DiskError::InvalidPath);
                    }
                    let child_name = child.file_name().into_string().map_err(|_| DiskError::FileCorrupt)?;
                    let metadata = child.metadata().map_err(to_file_error)?;
                    descriptions.push((format!("{name}/{child_name}"), kind.is_dir(), metadata.len()));
                    if kind.is_file()
                        && [
                            DELETE_DATA_DIR_MARKER_PREFIX,
                            RESERVED_DELETE_DATA_DIR_MARKER_PREFIX,
                            RECOVERY_DELETE_DATA_DIR_MARKER_PREFIX,
                        ]
                        .iter()
                        .any(|prefix| child_name == format!("{prefix}{transaction}"))
                    {
                        any_marker = true;
                    }
                    let staged_id = Uuid::parse_str(&child_name).ok();
                    let known_staged = kind.is_dir()
                        && staged_id.is_some_and(|dir| {
                            journal.as_ref().is_some_and(|journal| journal.data_dirs.contains(&dir))
                                || before_meta
                                    .as_ref()
                                    .is_some_and(|metadata| metadata.get_data_dirs().is_ok_and(|dirs| dirs.contains(&Some(dir))))
                        });
                    if name == transaction.to_string()
                        && !(kind.is_dir() && known_staged)
                        && !(journal.is_none()
                            && kind.is_file()
                            && (delete_recovery_part_file(&child_name)
                                || [DELETE_DATA_DIR_MARKER_PREFIX, RESERVED_DELETE_DATA_DIR_MARKER_PREFIX]
                                    .iter()
                                    .any(|prefix| child_name == format!("{prefix}{transaction}"))))
                        && (kind.is_dir()
                            || ![
                                PREPARED,
                                TARGET,
                                FINISHED,
                                STORAGE_FORMAT_FILE_BACKUP,
                                DELETE_MARKER_ROLLBACK_FILE,
                                DELETE_QUORUM_COMMITTED_FILE,
                            ]
                            .contains(&child_name.as_str()))
                    {
                        return Err(DiskError::FileCorrupt);
                    }
                    if name == transaction.to_string() && kind.is_dir() {
                        for file in std::fs::read_dir(child.path()).map_err(to_file_error)? {
                            let file = file.map_err(to_file_error)?;
                            let kind = file.file_type().map_err(to_file_error)?;
                            let file_name = file.file_name().into_string().map_err(|_| DiskError::FileCorrupt)?;
                            if !kind.is_file()
                                || (!delete_recovery_part_file(&file_name)
                                    && ![
                                        DELETE_DATA_DIR_MARKER_PREFIX,
                                        RESERVED_DELETE_DATA_DIR_MARKER_PREFIX,
                                        RECOVERY_DELETE_DATA_DIR_MARKER_PREFIX,
                                    ]
                                    .iter()
                                    .any(|prefix| file_name == format!("{prefix}{transaction}")))
                            {
                                return Err(DiskError::FileCorrupt);
                            }
                            descriptions.push((
                                format!("{name}/{child_name}/{file_name}"),
                                false,
                                file.metadata().map_err(to_file_error)?.len(),
                            ));
                        }
                    }
                }
            }
        }
    }
    descriptions.sort();
    fingerprint.update(serde_json::to_vec(&descriptions)?);
    let mut rollback_data_complete = false;
    let mut commit_data_safe = false;
    if let Some(journal) = &journal {
        rollback_data_complete = (journal.original_digest == backup_digest || current_digest == journal.original_digest)
            && (current_digest == journal.original_digest || current_digest == journal.target_digest);
        if let Some(meta) = before_meta
            .as_ref()
            .or_else(|| current_meta.as_ref().filter(|_| current_digest == journal.original_digest))
        {
            let inline: HashMap<&str, &[u8]> = if meta.data.as_slice().is_empty() {
                HashMap::new()
            } else {
                if !meta.data.version_ok() {
                    return Err(DiskError::FileCorrupt);
                }
                rmp_serde::from_slice(meta.data.after_version())?
            };
            for version in &meta.versions {
                if version.header.version_type != rustfs_filemeta::VersionType::Object {
                    continue;
                }
                let mut info = version.into_fileinfo(volume, object, true)?;
                info.hydrate_shard_integrity()?;
                if info.is_remote() {
                    continue;
                }
                if info.inline_data() {
                    let id = info.version_id.unwrap_or_default();
                    rollback_data_complete &=
                        inline.contains_key(id.to_string().as_str()) || (id.is_nil() && inline.contains_key("null"));
                    continue;
                }
                let Some(data_dir) = info.data_dir.filter(|id| !id.is_nil()) else {
                    rollback_data_complete = false;
                    continue;
                };
                rollback_data_complete &=
                    complete_part_sources(&root.join(data_dir.to_string()), &rollback.join(data_dir.to_string()), &info)?;
            }
        }
        commit_data_safe = current_digest == journal.original_digest || current_digest == journal.target_digest;
        if current_digest != journal.original_digest && current_digest != journal.target_digest {
            // A later certified delete can remove the remaining metadata of
            // this committed version delete. Absence has no live references,
            // but cannot itself authorize an undecided transaction.
            commit_data_safe = (committed && current.is_none())
                || current_meta.as_ref().is_some_and(|meta| {
                    meta.get_data_dirs()
                        .is_ok_and(|dirs| !dirs.into_iter().flatten().any(|dir| journal.data_dirs.contains(&dir)))
                        && !meta.versions.iter().any(|version| {
                            version
                                .header
                                .version_id
                                .is_some_and(|id| !id.is_nil() && journal.deleted_versions.contains(&id))
                        })
                });
        }
    }
    // In-place shard repair leaves a backup in the live data directory.
    // Preserve it without interpreting it as an unfinished delete.
    let live_write_backup = journal.is_none()
        && !any_marker
        && !regular_marker(&rollback.join(DELETE_MARKER_ROLLBACK_FILE))?
        && prepared.is_none()
        && target.is_none()
        && receipt_bytes.is_none()
        && !finished
        && match (current_meta.as_ref(), before_meta.as_ref()) {
            (Some(current), Some(backup)) => live_write_backup_matches(current, backup, transaction)?,
            _ => false,
        };
    let state = if live_write_backup {
        DeleteRecoveryState::Absent
    } else if journal.is_some() {
        if committed {
            if finished {
                DeleteRecoveryState::CommitFinished
            } else {
                DeleteRecoveryState::QuorumCommitted
            }
        } else {
            DeleteRecoveryState::NewPrepared
        }
    } else if before.is_some() || any_marker || receipt_bytes.is_some() || prepared.is_some() || target.is_some() {
        DeleteRecoveryState::LegacyAmbiguous
    } else {
        DeleteRecoveryState::Absent
    };
    let explicit_delete_safe =
        explicit_delete_plan(root, transaction, before_meta.as_ref(), journal.as_ref(), current.is_none())?.is_some();
    Ok(DeleteRecoveryInspection {
        protocol_version: DELETE_RECOVERY_PROTOCOL_VERSION,
        id: DeleteTransactionId {
            object: object.to_owned(),
            transaction,
        },
        state,
        journal,
        current_digest,
        backup_digest,
        rollback_data_complete,
        commit_data_safe,
        explicit_delete_safe,
        current_versions: current_meta
            .as_ref()
            .map(|meta| {
                meta.versions
                    .iter()
                    .map(|version| generation(version.into_fileinfo(volume, object, true)?))
                    .collect::<Result<Vec<_>>>()
            })
            .transpose()?
            .unwrap_or_default(),
        fingerprint: fingerprint.finalize().into(),
    })
}

impl LocalDisk {
    async fn delete_recovery_predecessors(
        &self,
        volume: &str,
        object: &str,
        transaction: Uuid,
        incarnation: Option<Uuid>,
        owner: Option<Arc<dyn Send + Sync>>,
    ) -> Result<Vec<Uuid>> {
        let root = self.io_get_object_path(volume, object)?;
        let volume = volume.to_owned();
        let object = object.to_owned();
        let lease = os::acquire_namespace_mutation_lease_with_owner(&root, owner).await;
        os::run_blocking_namespace_operation(lease, move || {
            Ok((|| {
                let entries = match std::fs::read_dir(&root) {
                    Ok(entries) => entries,
                    Err(err) if err.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
                    Err(err) => return Err(to_file_error(err).into()),
                };
                let mut predecessors = BTreeSet::new();
                let mut visited = 0usize;
                for entry in entries {
                    visited += 1;
                    if visited > 65536 {
                        return Err(DiskError::DiskOngoingReq);
                    }
                    let entry = entry.map_err(to_file_error)?;
                    let Some(id) = entry
                        .file_name()
                        .to_str()
                        .and_then(|name| Uuid::parse_str(name).ok())
                        .filter(|id| !id.is_nil() && *id != transaction)
                    else {
                        continue;
                    };
                    if !entry.file_type().map_err(to_file_error)?.is_dir() {
                        return Err(DiskError::InvalidPath);
                    }
                    let prepared = regular_bytes(&entry.path().join(PREPARED))?;
                    let receipt = regular_bytes(&entry.path().join(DELETE_QUORUM_COMMITTED_FILE))?;
                    let mut records = Vec::new();
                    if let Some(bytes) = prepared {
                        records.push(serde_json::from_slice::<DeleteRecoveryJournal>(&bytes)?);
                    }
                    if let Some(bytes) = receipt.filter(|bytes| !bytes.is_empty()) {
                        records.push(serde_json::from_slice::<CommitReceipt>(&bytes)?.journal);
                    }
                    for record in records {
                        if record.protocol_version != DELETE_RECOVERY_PROTOCOL_VERSION
                            || record.volume != volume
                            || record.object != object
                            || record.transaction != id
                            || (incarnation.is_some()
                                && record.bucket_incarnation.is_some()
                                && record.bucket_incarnation != incarnation)
                            || record.predecessors.iter().any(|before| before.is_nil() || *before == id)
                        {
                            return Err(DiskError::FileCorrupt);
                        }
                        predecessors.insert(id);
                    }
                }
                Ok(predecessors.into_iter().collect())
            })())
        })
        .await
        .map_err(to_file_error)?
    }
    pub(super) async fn delete_recovery_discover(
        &self,
        volume: &str,
        object: Option<&str>,
        cursor: Option<DeleteRecoveryCursor>,
        limit: u32,
    ) -> Result<DeleteRecoveryPage> {
        if !(1..=256).contains(&limit)
            || !safe_path(volume, false)
            || volume.contains('/')
            || object.is_some_and(|object| !safe_path(object, false))
        {
            return Err(DiskError::InvalidPath);
        }
        let bucket_path = match object {
            Some(object) => self.io_get_object_path(volume, object)?,
            None => self.io_get_bucket_path(volume)?,
        };
        let disk_root = self.io_root().to_path_buf();
        let (token, mut session) = if let Some(cursor) = cursor {
            let session = DISCOVERY_SESSIONS
                .lock()
                .remove(&cursor.token)
                .ok_or(DiskError::FileNotFound)?;
            if session.disk_root != disk_root
                || session.volume != volume
                || session.object.as_deref() != object
                || session.touched.elapsed() > Duration::from_secs(300)
            {
                return Err(DiskError::InvalidPath);
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                let before = session._mount.metadata().map_err(to_file_error)?;
                #[cfg(target_os = "linux")]
                let current = self.mount_lease.metadata().map_err(to_file_error)?;
                #[cfg(not(target_os = "linux"))]
                let current = std::fs::metadata(self.io_root()).map_err(to_file_error)?;
                if before.dev() != current.dev() || before.ino() != current.ino() {
                    return Err(DiskError::InvalidPath);
                }
            }
            (cursor.token, session)
        } else {
            #[cfg(target_os = "linux")]
            let (mount, root) = {
                use std::os::fd::AsRawFd;
                let mount = self.mount_lease.try_clone().map_err(to_file_error)?;
                let root = PathBuf::from(format!("/proc/self/fd/{}/.", mount.as_raw_fd())).join(volume);
                let root = match object {
                    Some(object) => root.join(object),
                    None => root,
                };
                (mount, root)
            };
            #[cfg(windows)]
            let (mount, root) = {
                use std::os::windows::fs::OpenOptionsExt;
                use windows_sys::Win32::Storage::FileSystem::{
                    FILE_FLAG_BACKUP_SEMANTICS, FILE_READ_ATTRIBUTES, FILE_SHARE_READ, FILE_SHARE_WRITE, FILE_TRAVERSE,
                };
                // Match the existing publication-root directory open contract.
                let mount = std::fs::OpenOptions::new()
                    .access_mode(FILE_TRAVERSE | FILE_READ_ATTRIBUTES)
                    .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
                    .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
                    .open(self.io_root())
                    .map_err(to_file_error)?;
                (mount, bucket_path.clone())
            };
            #[cfg(all(not(target_os = "linux"), not(windows)))]
            let (mount, root) = (std::fs::File::open(self.io_root()).map_err(to_file_error)?, bucket_path.clone());
            (
                Uuid::new_v4(),
                DiscoverySession {
                    disk_root,
                    volume: volume.to_owned(),
                    object: object.map(str::to_owned),
                    root,
                    _mount: mount,
                    frames: Vec::new(),
                    touched: Instant::now(),
                },
            )
        };
        let lease = os::acquire_namespace_mutation_lease_with_owner(&bucket_path, None).await;
        os::run_blocking_namespace_operation(lease, move || {
            if session.frames.is_empty() {
                match std::fs::read_dir(&session.root) {
                    Ok(entries) => session.frames.push((String::new(), entries)),
                    Err(err) if err.kind() == ErrorKind::NotFound => {
                        return Ok(DeleteRecoveryPage {
                            transactions: Vec::new(),
                            next: None,
                        });
                    }
                    Err(err) => return Err(err),
                }
            }
            let mut transactions = BTreeSet::new();
            let mut visited = 0usize;
            while !session.frames.is_empty()
                && transactions.len() < usize::try_from(limit).map_err(|_| std::io::Error::from(ErrorKind::InvalidInput))?
                && visited < 4096
            {
                let (parent, entries) = session
                    .frames
                    .last_mut()
                    .ok_or_else(|| std::io::Error::from(ErrorKind::InvalidData))?;
                let entry = match entries.next() {
                    Some(entry) => entry?,
                    None => {
                        session.frames.pop();
                        continue;
                    }
                };
                let name = entry
                    .file_name()
                    .into_string()
                    .map_err(|_| std::io::Error::from(ErrorKind::InvalidData))?;
                if !safe_path(&name, false) || name.contains('/') {
                    return Err(std::io::Error::from(ErrorKind::InvalidData));
                }
                let relative = if parent.is_empty() {
                    name.clone()
                } else {
                    format!("{parent}/{name}")
                };
                let path = session.root.join(&relative);
                reject_local_disk_symlink_components(&session.root, &path).map_err(std::io::Error::from)?;
                let kind = match std::fs::symlink_metadata(&path) {
                    Ok(metadata) => metadata,
                    Err(err) if err.kind() == ErrorKind::NotFound => continue,
                    Err(err) => return Err(err),
                };
                if !kind.is_dir() && !kind.is_file() {
                    return Err(std::io::Error::from(ErrorKind::InvalidData));
                }
                visited += 1;
                if kind.is_file() {
                    let bound = match session.object.as_deref() {
                        Some(object) if !parent.is_empty() && !parent.contains('/') => Some((object, parent.as_str())),
                        Some(_) => None,
                        None => parent.rsplit_once('/'),
                    };
                    if let Some((object, id)) = bound {
                        let transaction =
                            if [PREPARED, STORAGE_FORMAT_FILE_BACKUP, DELETE_QUORUM_COMMITTED_FILE].contains(&name.as_str()) {
                                Uuid::parse_str(id).ok().filter(|id| !id.is_nil())
                            } else {
                                [
                                    DELETE_DATA_DIR_MARKER_PREFIX,
                                    RESERVED_DELETE_DATA_DIR_MARKER_PREFIX,
                                    RECOVERY_DELETE_DATA_DIR_MARKER_PREFIX,
                                ]
                                .iter()
                                .find_map(|prefix| delete_transaction_marker_id(&name, prefix))
                            };
                        if let Some(transaction) = transaction {
                            let physical = match session.object.as_ref() {
                                Some(_) => session.root.clone(),
                                None => session.root.join(object),
                            };
                            if name == STORAGE_FORMAT_FILE_BACKUP
                                && live_write_backup(&physical, transaction).map_err(std::io::Error::from)?
                            {
                                continue;
                            }
                            let proof_root = physical.join(transaction.to_string());
                            let prepared = regular_bytes(&proof_root.join(PREPARED)).map_err(std::io::Error::from)?;
                            let receipt =
                                regular_bytes(&proof_root.join(DELETE_QUORUM_COMMITTED_FILE)).map_err(std::io::Error::from)?;
                            let record = match prepared {
                                Some(bytes) => {
                                    Some(serde_json::from_slice::<DeleteRecoveryJournal>(&bytes).map_err(std::io::Error::other)?)
                                }
                                None => receipt
                                    .filter(|bytes| !bytes.is_empty())
                                    .map(|bytes| serde_json::from_slice::<CommitReceipt>(&bytes).map(|receipt| receipt.journal))
                                    .transpose()
                                    .map_err(std::io::Error::other)?,
                            };
                            let logical = if let Some(journal) = record {
                                let bound_path = match session.object.as_ref() {
                                    Some(scope) if scope == &journal.object => physical.clone(),
                                    Some(_) => return Err(std::io::Error::from(ErrorKind::InvalidData)),
                                    None => build_local_disk_object_path(&session.root, "", &journal.object).1,
                                };
                                if journal.protocol_version != DELETE_RECOVERY_PROTOCOL_VERSION
                                    || journal.volume != session.volume
                                    || journal.transaction != transaction
                                    || !safe_path(&journal.object, false)
                                    || bound_path != physical
                                {
                                    return Err(std::io::Error::from(ErrorKind::InvalidData));
                                }
                                journal.object
                            } else {
                                object.to_owned()
                            };
                            transactions.insert((logical, transaction));
                        }
                    }
                } else {
                    if session.object.is_some() && (!parent.is_empty() || !Uuid::parse_str(&name).is_ok_and(|id| !id.is_nil())) {
                        continue;
                    }
                    // A valid 1024-byte key can exceed 256 one-byte path
                    // components; allow its transaction/data directories too.
                    if session.frames.len() >= 1024 {
                        return Err(std::io::Error::from(ErrorKind::InvalidData));
                    }
                    session.frames.push((relative, std::fs::read_dir(path)?));
                }
            }
            let next = if session.frames.is_empty() {
                None
            } else {
                session.touched = Instant::now();
                let mut sessions = DISCOVERY_SESSIONS.lock();
                sessions.retain(|_, session| session.touched.elapsed() <= Duration::from_secs(300));
                if sessions.len() >= 128 {
                    return Err(std::io::Error::from(ErrorKind::WouldBlock));
                }
                sessions.insert(token, session);
                Some(DeleteRecoveryCursor { token })
            };
            Ok(DeleteRecoveryPage {
                transactions: transactions
                    .into_iter()
                    .map(|(object, transaction)| DeleteTransactionId { object, transaction })
                    .collect(),
                next,
            })
        })
        .await
        .map_err(|err| to_file_error(err).into())
    }

    pub(super) async fn delete_recovery_inspect(
        &self,
        volume: &str,
        object: &str,
        transaction: Uuid,
    ) -> Result<DeleteRecoveryInspection> {
        if !safe_path(object, false) || transaction.is_nil() {
            return Err(DiskError::InvalidPath);
        }
        let object_path = self.get_object_path(volume, object)?;
        let owner: Option<Arc<dyn Send + Sync>> = Some(os::acquire_metadata_mutation_lease(&object_path, None).await);
        self.delete_recovery_inspect_owned(volume, object, transaction, owner).await
    }

    pub(super) async fn delete_recovery_inspect_owned(
        &self,
        volume: &str,
        object: &str,
        transaction: Uuid,
        owner: Option<Arc<dyn Send + Sync>>,
    ) -> Result<DeleteRecoveryInspection> {
        let root = self.io_get_object_path(volume, object)?;
        self.io_get_object_path(volume, &format!("{object}/{transaction}"))?;
        let volume = volume.to_owned();
        let object = object.to_owned();
        let lease = os::acquire_namespace_mutation_lease_with_owner(&root, owner).await;
        os::run_blocking_namespace_operation(lease, move || Ok(inspection(&root, &volume, &object, transaction)))
            .await
            .map_err(to_file_error)?
    }

    async fn activate_delete_preparation(
        &self,
        journal: &DeleteRecoveryJournal,
        original: Option<&[u8]>,
        target: Option<Vec<u8>>,
        owner: Option<Arc<dyn Send + Sync>>,
    ) -> Result<Option<Vec<u8>>> {
        let volume = journal.volume.as_str();
        let object = journal.object.as_str();
        let transaction = journal.transaction;
        let journal = journal_bytes(journal)?;
        self.wait_for_startup_cleanup().await;
        let temporary = self.io_get_bucket_path(RUSTFS_META_TMP_BUCKET)?;
        let root = self.io_get_object_path(volume, object)?;
        let destination = root.join(transaction.to_string());
        let original = original.map(<[u8]>::to_vec);
        let sync = effective_durability(volume).syncs_commit_metadata();
        #[cfg(test)]
        let record_destination = destination.clone();
        let staging_root = temporary.clone();
        let lease = os::acquire_namespace_mutation_lease_with_owner(&destination, owner.clone()).await;
        let (staged, target) = os::run_blocking_namespace_operation(lease, move || {
            std::fs::create_dir_all(&staging_root)?;
            let staged = Arc::new(
                tempfile::Builder::new()
                    .prefix("delete-preparation-")
                    .tempdir_in(&staging_root)?,
            );
            let missing_marker = original.is_none() && target.is_some();
            for (name, bytes) in [
                (STORAGE_FORMAT_FILE_BACKUP, original.as_deref()),
                (TARGET, target.as_deref()),
                (PREPARED, Some(journal.as_slice())),
                (DELETE_MARKER_ROLLBACK_FILE, missing_marker.then_some(&[][..])),
            ] {
                let Some(bytes) = bytes else {
                    continue;
                };
                #[cfg(test)]
                run_owned_file_write_before_open(&record_destination.join(name));
                let mut file = std::fs::File::create(staged.path().join(name))?;
                std::io::Write::write_all(&mut file, bytes)?;
                if sync {
                    file.sync_all()?;
                }
            }
            if sync {
                os::fsync_dir_std(staged.path())?;
            }
            Ok((staged, target))
        })
        .await
        .map_err(to_file_error)?;
        self.delete_recovery_barrier(volume, object, transaction, "PREACTIVATION")
            .await?;
        match fs::symlink_metadata(&destination).await {
            Ok(_) => return Err(DiskError::OutdatedXLMeta),
            Err(err) if err.kind() == ErrorKind::NotFound => {}
            Err(err) => return Err(to_file_error(err).into()),
        }
        // The directory owner accompanies the blocking rename, so cancelling
        // its waiter cannot delete staged files while activation is running.
        let publication_owner: Option<Arc<dyn Send + Sync>> = Some(Arc::new((owner.clone(), staged.clone())));
        os::rename_all_with_owner(
            staged.path(),
            &destination,
            &self.io_get_bucket_path(volume)?,
            &self.publication_root,
            publication_owner,
        )
        .await?;
        if sync {
            let bucket = self.io_get_bucket_path(volume)?;
            let mut path = root.as_path();
            loop {
                os::fsync_dir_with_owner(path, owner.clone()).await.map_err(to_file_error)?;
                if path == bucket {
                    break;
                }
                path = path.parent().ok_or(DiskError::InvalidPath)?;
            }
            os::fsync_dir_with_owner(&temporary, owner.clone())
                .await
                .map_err(to_file_error)?;
        }
        self.delete_recovery_barrier(volume, object, transaction, "PREPARED").await?;
        Ok(target)
    }

    pub(super) async fn prepare_delete_recovery(
        &self,
        volume: &str,
        object: &str,
        original: Option<&[u8]>,
        requests: &[FileInfo],
        authority: Option<&DeleteRecoveryJournal>,
        context: DeletePreparationContext,
    ) -> Result<Option<Vec<u8>>> {
        let DeletePreparationContext {
            transaction,
            bucket_incarnation: incarnation,
            namespace_owner: owner,
        } = context;
        let mut plan = original.map(FileMeta::load).transpose()?.unwrap_or_else(FileMeta::new);
        let mut deleted_headers: Vec<_> = plan
            .versions
            .iter()
            .map(|version| (version.header.version_id, std::mem::discriminant(&version.header.version_type)))
            .collect();
        let mut original_versions = Vec::new();
        for version in &plan.versions {
            original_versions.push(generation(version.into_fileinfo(volume, object, true)?)?);
        }
        let mut data_dirs = Vec::with_capacity(requests.len());
        for request in requests {
            if original.is_none() {
                plan.add_version(request.clone())?;
                continue;
            }
            match plan.delete_version(request) {
                Ok(Some(dir)) => {
                    let _ = plan.data.remove(vec![request.version_id.unwrap_or_default(), dir]);
                    data_dirs.push(dir);
                }
                Ok(None) => {}
                Err(err) => {
                    let err: DiskError = err.into();
                    if request.deleted || (err != DiskError::FileVersionNotFound && err != DiskError::FileNotFound) {
                        return Err(err);
                    }
                }
            }
        }
        if data_dirs.iter().any(|dir| *dir == transaction || dir.is_nil()) {
            return Err(DiskError::FileCorrupt);
        }
        let target = if plan.versions.is_empty() {
            None
        } else {
            Some(plan.marshal_msg()?)
        };
        let predecessors = match authority {
            Some(authority) => authority.predecessors.clone(),
            None => {
                self.delete_recovery_predecessors(volume, object, transaction, incarnation, owner.clone())
                    .await?
            }
        };
        let surviving: HashSet<_> = plan
            .versions
            .iter()
            .map(|version| (version.header.version_id, std::mem::discriminant(&version.header.version_type)))
            .collect();
        deleted_headers.retain(|header| !surviving.contains(header));
        let deleted_versions = deleted_headers.into_iter().map(|(id, _)| id.unwrap_or_default()).collect();
        let requests = requests
            .iter()
            .map(|request| {
                let mut sparse = request.clone();
                sparse.data = None;
                sparse
            })
            .collect();
        let mut journal = DeleteRecoveryJournal {
            protocol_version: DELETE_RECOVERY_PROTOCOL_VERSION,
            volume: volume.to_owned(),
            object: object.to_owned(),
            transaction,
            bucket_incarnation: incarnation,
            original_digest: original.map(digest),
            target_digest: target.as_deref().map(digest),
            data_dirs,
            deleted_versions,
            requests,
            original_versions,
            predecessors,
        };
        if let Some(authority) = authority {
            if authority.volume != volume
                || authority.object != object
                || authority.transaction != transaction
                || authority.bucket_incarnation != incarnation
            {
                return Err(DiskError::OutdatedXLMeta);
            }
            journal.original_versions.clone_from(&authority.original_versions);
            journal.deleted_versions.clone_from(&authority.deleted_versions);
        }
        drop(plan);
        self.activate_delete_preparation(&journal, original, target, owner).await
    }

    pub(crate) async fn delete_recovery_settle(
        &self,
        volume: &str,
        object: &str,
        transaction: Uuid,
        expected_fingerprint: DeleteRecoveryDigest,
        action: DeleteRecoveryAction,
        namespace_owner: Option<Arc<dyn Send + Sync>>,
    ) -> Result<DeleteRecoveryInspection> {
        if !safe_path(object, false) || transaction.is_nil() {
            return Err(DiskError::InvalidPath);
        }
        let object_path = self.get_object_path(volume, object)?;
        let metadata_owner = os::acquire_metadata_mutation_lease(&object_path, namespace_owner).await;
        let volume_guard = os::disk_volume_mutation_lock(&self.root, volume).read_owned().await;
        let owner: Option<Arc<dyn Send + Sync>> = Some(Arc::new((metadata_owner, volume_guard)));
        self.delete_recovery_settle_owned(volume, object, transaction, expected_fingerprint, action, owner)
            .await
    }

    pub(super) async fn rollback_prepared_delete(
        &self,
        volume: &str,
        object: &str,
        transaction: Uuid,
        owner: Option<Arc<dyn Send + Sync>>,
    ) -> Result<bool> {
        let prepared = self.io_get_object_path(volume, &format!("{object}/{transaction}/{PREPARED}"))?;
        if regular_bytes(&prepared)?.is_none() {
            return Ok(false);
        }
        let state = self
            .delete_recovery_inspect_owned(volume, object, transaction, owner.clone())
            .await?;
        self.delete_recovery_settle_owned(volume, object, transaction, state.fingerprint, DeleteRecoveryAction::Rollback, owner)
            .await?;
        Ok(true)
    }

    async fn delete_recovery_settle_owned(
        &self,
        volume: &str,
        object: &str,
        transaction: Uuid,
        expected_fingerprint: DeleteRecoveryDigest,
        action: DeleteRecoveryAction,
        owner: Option<Arc<dyn Send + Sync>>,
    ) -> Result<DeleteRecoveryInspection> {
        let observed = self
            .delete_recovery_inspect_owned(volume, object, transaction, owner.clone())
            .await?;
        if observed.fingerprint != expected_fingerprint {
            return Err(DiskError::OutdatedXLMeta);
        }
        if let DeleteRecoveryAction::AdoptCommit { authority } = &action {
            if authority.protocol_version != DELETE_RECOVERY_PROTOCOL_VERSION
                || authority.volume != volume
                || authority.object != object
                || authority.transaction != transaction
            {
                return Err(DiskError::OutdatedXLMeta);
            }
            if observed.state == DeleteRecoveryState::NewPrepared {
                let local = observed.journal.as_ref().ok_or(DiskError::FileCorrupt)?;
                if observed.current_digest.is_some() || !local.matches_prepared_authority(authority) {
                    return Err(DiskError::OutdatedXLMeta);
                }
                // The coordinator supplies an inspected same-transaction
                // receipt. Retain this drive's immutable hashes, data dirs and
                // causal history when completing its partial publication.
                let root = self.io_get_object_path(volume, object)?;
                let receipt = CommitReceipt {
                    journal: local.clone(),
                    finished: false,
                };
                write_delete_rollback_file(
                    self.io_root(),
                    volume,
                    &root,
                    transaction,
                    DELETE_QUORUM_COMMITTED_FILE,
                    &serde_json::to_vec(&receipt)?,
                    owner.clone(),
                )
                .await?;
                self.delete_recovery_barrier(volume, object, transaction, "DECISION").await?;
                return self.delete_recovery_inspect_owned(volume, object, transaction, owner).await;
            }
            if observed.state != DeleteRecoveryState::Absent {
                return Err(DiskError::OutdatedXLMeta);
            }
            if observed.current_digest.is_none() {
                // A replay witness inherits the original decision's causal
                // history; observing later records cannot backdate new edges.
                let mut witness = authority.as_ref().clone();
                witness.original_digest = None;
                witness.target_digest = None;
                witness.data_dirs.clear();
                let root = self.io_get_object_path(volume, object)?;
                let _ = self.activate_delete_preparation(&witness, None, None, owner.clone()).await?;
                let receipt = CommitReceipt {
                    journal: witness,
                    finished: false,
                };
                write_delete_rollback_file(
                    self.io_root(),
                    volume,
                    &root,
                    transaction,
                    DELETE_QUORUM_COMMITTED_FILE,
                    &serde_json::to_vec(&receipt)?,
                    owner.clone(),
                )
                .await?;
                self.delete_recovery_barrier(volume, object, transaction, "DECISION").await?;
                return self.delete_recovery_inspect_owned(volume, object, transaction, owner).await;
            }
            if !authority.matches_adoption_source(&observed.current_versions) {
                return Err(DiskError::OutdatedXLMeta);
            }
            let root = self.io_get_object_path(volume, object)?;
            let bytes = self.read_all(volume, &format!("{object}/{STORAGE_FORMAT_FILE}")).await?;
            let _ = self
                .prepare_delete_recovery(
                    volume,
                    object,
                    Some(&bytes),
                    &authority.requests,
                    Some(authority.as_ref()),
                    DeletePreparationContext {
                        transaction,
                        bucket_incarnation: authority.bucket_incarnation,
                        namespace_owner: owner.clone(),
                    },
                )
                .await?;
            let adopted = self
                .delete_recovery_inspect_owned(volume, object, transaction, owner.clone())
                .await?;
            let adopted_journal = adopted.journal.ok_or(DiskError::FileCorrupt)?;
            let receipt = CommitReceipt {
                journal: adopted_journal,
                finished: false,
            };
            write_delete_rollback_file(
                self.io_root(),
                volume,
                &root,
                transaction,
                DELETE_QUORUM_COMMITTED_FILE,
                &serde_json::to_vec(&receipt)?,
                owner.clone(),
            )
            .await?;
            self.delete_recovery_barrier(volume, object, transaction, "DECISION").await?;
            return self.delete_recovery_inspect_owned(volume, object, transaction, owner).await;
        }
        if observed.state == DeleteRecoveryState::Absent {
            if action == DeleteRecoveryAction::PublishCommit {
                if observed.current_digest.is_some() {
                    return Err(DiskError::OutdatedXLMeta);
                }
                self.prepare_delete_recovery(
                    volume,
                    object,
                    None,
                    &[],
                    None,
                    DeletePreparationContext {
                        transaction,
                        bucket_incarnation: None,
                        namespace_owner: owner.clone(),
                    },
                )
                .await?;
            } else {
                return Ok(observed);
            }
        }
        let observed = self
            .delete_recovery_inspect_owned(volume, object, transaction, owner.clone())
            .await?;
        if action == DeleteRecoveryAction::DiscardLegacy {
            if observed.state != DeleteRecoveryState::LegacyAmbiguous || !observed.explicit_delete_safe {
                return Err(DiskError::FileCorrupt);
            }
            let root = self.io_get_object_path(volume, object)?;
            let rollback = root.join(transaction.to_string());
            let plan_root = root.clone();
            let missing = observed.current_digest.is_none();
            let lease = os::acquire_namespace_mutation_lease_with_owner(&root, owner.clone()).await;
            let plan = os::run_blocking_namespace_operation(lease, move || {
                Ok((|| {
                    let before =
                        regular_metadata_bytes(&plan_root.join(transaction.to_string()).join(STORAGE_FORMAT_FILE_BACKUP))?
                            .as_deref()
                            .map(FileMeta::load)
                            .transpose()?;
                    explicit_delete_plan(&plan_root, transaction, before.as_ref(), None, missing)?.ok_or(DiskError::FileCorrupt)
                })())
            })
            .await
            .map_err(to_file_error)??;
            for dir in plan.data_dirs {
                let relative = dir.strip_prefix(&root).map_err(|_| DiskError::InvalidPath)?;
                let data_dir = relative
                    .file_name()
                    .and_then(|name| name.to_str())
                    .and_then(|name| Uuid::parse_str(name).ok())
                    .ok_or(DiskError::FileCorrupt)?;
                let parent = relative.parent().filter(|parent| !parent.as_os_str().is_empty());
                let staged_object =
                    parent.map_or_else(|| object.to_owned(), |parent| format!("{object}/{}", parent.to_string_lossy()));
                self.reserve_version_delete_with_namespace_owner(volume, &staged_object, data_dir, transaction, owner.clone())
                    .await?;
                let reserved = dir.join(format!("{RESERVED_DELETE_DATA_DIR_MARKER_PREFIX}{transaction}"));
                let private = dir.join(format!("{RECOVERY_DELETE_DATA_DIR_MARKER_PREFIX}{transaction}"));
                match os::rename_with_namespace_owner(&reserved, &private, owner.clone()).await {
                    Ok(()) => {}
                    Err(err) if err.kind() == ErrorKind::NotFound => {}
                    Err(err) => return Err(to_file_error(err).into()),
                }
                if effective_durability(volume).syncs_commit_metadata() {
                    os::fsync_dir_with_owner(&dir, owner.clone()).await.map_err(to_file_error)?;
                }
                if self
                    .delete_data_dir_with_namespace_owner(
                        volume,
                        &format!("{object}/{}", relative.to_string_lossy()),
                        DeleteOptions {
                            recursive: true,
                            old_data_dir: Some(transaction),
                            recoverable_delete: true,
                            ..Default::default()
                        },
                        owner.clone(),
                    )
                    .await?
                    == DataDirDeleteStatus::Deferred
                {
                    return Err(DiskError::DiskOngoingReq);
                }
            }
            for file in plan.proof_files {
                match os::remove_file_with_owner(file, owner.clone()).await {
                    Ok(()) => {}
                    Err(err) if err.kind() == ErrorKind::NotFound => {}
                    Err(err) => return Err(to_file_error(err).into()),
                }
            }
            if effective_durability(volume).syncs_commit_metadata() && fs::try_exists(&rollback).await.map_err(to_file_error)? {
                os::fsync_dir_with_owner(&rollback, owner.clone())
                    .await
                    .map_err(to_file_error)?;
            }
            self.delete_unleased_with_namespace_owner(
                volume,
                &format!("{object}/{transaction}"),
                &DeleteOptions {
                    immediate: true,
                    ..Default::default()
                },
                owner.clone(),
            )
            .await?;
            return self.delete_recovery_inspect_owned(volume, object, transaction, owner).await;
        }
        let journal = observed.journal.as_ref().ok_or(DiskError::FileCorrupt)?;
        let root = self.io_get_object_path(volume, object)?;
        let rollback = root.join(transaction.to_string());
        match action {
            DeleteRecoveryAction::PublishCommit => {
                if !observed.commit_data_safe {
                    return Err(DiskError::OutdatedXLMeta);
                }
                if matches!(observed.state, DeleteRecoveryState::QuorumCommitted | DeleteRecoveryState::CommitFinished) {
                    return Ok(observed);
                }
                let receipt = CommitReceipt {
                    journal: journal.clone(),
                    finished: false,
                };
                write_delete_rollback_file(
                    self.io_root(),
                    volume,
                    &root,
                    transaction,
                    DELETE_QUORUM_COMMITTED_FILE,
                    &serde_json::to_vec(&receipt)?,
                    owner.clone(),
                )
                .await?;
                self.delete_recovery_barrier(volume, object, transaction, "DECISION").await?;
            }
            DeleteRecoveryAction::FinishCommit => {
                if !matches!(observed.state, DeleteRecoveryState::QuorumCommitted | DeleteRecoveryState::CommitFinished)
                    || !observed.commit_data_safe
                {
                    return Err(DiskError::OutdatedXLMeta);
                }
                if observed.state == DeleteRecoveryState::CommitFinished {
                    return Ok(observed);
                }
                for dir in &journal.data_dirs {
                    self.reserve_version_delete_with_namespace_owner(volume, object, *dir, transaction, owner.clone())
                        .await?;
                }
                if observed.current_digest == journal.original_digest {
                    if journal.target_digest.is_some() {
                        let target = self.read_all(volume, &format!("{object}/{transaction}/{TARGET}")).await?;
                        self.write_all_meta_with_namespace_owner(
                            volume,
                            &format!("{object}/{STORAGE_FORMAT_FILE}"),
                            &target,
                            true,
                            owner.clone(),
                        )
                        .await?;
                    } else {
                        self.delete_file_with_namespace_owner(
                            &self.io_get_bucket_path(volume)?,
                            &root.join(STORAGE_FORMAT_FILE),
                            false,
                            false,
                            owner.clone(),
                        )
                        .await?;
                        if effective_durability(volume).syncs_commit_metadata() {
                            os::fsync_dir_with_owner(&root, owner.clone()).await.map_err(to_file_error)?;
                        }
                    }
                }
                self.commit_reserved_version_delete_with_namespace_owner(volume, object, transaction, owner.clone())
                    .await?;
                for name in [STORAGE_FORMAT_FILE_BACKUP, DELETE_MARKER_ROLLBACK_FILE] {
                    match os::remove_file_with_owner(rollback.join(name), owner.clone()).await {
                        Ok(()) => {}
                        Err(err) if err.kind() == ErrorKind::NotFound => {}
                        Err(err) => return Err(to_file_error(err).into()),
                    }
                }
                if effective_durability(volume).syncs_commit_metadata() {
                    os::fsync_dir_with_owner(&rollback, owner.clone())
                        .await
                        .map_err(to_file_error)?;
                }
                let (_, complete) = self
                    .finish_version_delete(volume, object, transaction, true, owner.clone())
                    .await?;
                if !complete {
                    return Err(DiskError::DiskOngoingReq);
                }
                for dir in &journal.data_dirs {
                    let staged = rollback.join(dir.to_string());
                    match fs::symlink_metadata(&staged).await {
                        Ok(metadata) if metadata.is_dir() => {
                            let staged_object = format!("{object}/{transaction}");
                            self.reserve_version_delete_with_namespace_owner(
                                volume,
                                &staged_object,
                                *dir,
                                transaction,
                                owner.clone(),
                            )
                            .await?;
                            let source = staged.join(format!("{RESERVED_DELETE_DATA_DIR_MARKER_PREFIX}{transaction}"));
                            let destination = staged.join(format!("{RECOVERY_DELETE_DATA_DIR_MARKER_PREFIX}{transaction}"));
                            match os::rename_with_namespace_owner(&source, &destination, owner.clone()).await {
                                Ok(()) => {}
                                Err(err) if err.kind() == ErrorKind::NotFound => {}
                                Err(err) => return Err(to_file_error(err).into()),
                            }
                            if effective_durability(volume).syncs_commit_metadata() {
                                os::fsync_dir_with_owner(&staged, owner.clone())
                                    .await
                                    .map_err(to_file_error)?;
                            }
                            if self
                                .delete_data_dir_with_namespace_owner(
                                    volume,
                                    &format!("{staged_object}/{dir}"),
                                    DeleteOptions {
                                        recursive: true,
                                        old_data_dir: Some(transaction),
                                        ..Default::default()
                                    },
                                    owner.clone(),
                                )
                                .await?
                                == DataDirDeleteStatus::Deferred
                            {
                                return Err(DiskError::DiskOngoingReq);
                            }
                        }
                        Ok(_) => return Err(DiskError::FileCorrupt),
                        Err(err) if err.kind() == ErrorKind::NotFound => {}
                        Err(err) => return Err(to_file_error(err).into()),
                    }
                }
                write_delete_rollback_file(self.io_root(), volume, &root, transaction, FINISHED, &[], owner.clone()).await?;
                let receipt = CommitReceipt {
                    journal: journal.clone(),
                    finished: true,
                };
                write_delete_rollback_file(
                    self.io_root(),
                    volume,
                    &root,
                    transaction,
                    DELETE_QUORUM_COMMITTED_FILE,
                    &serde_json::to_vec(&receipt)?,
                    owner.clone(),
                )
                .await?;
            }
            DeleteRecoveryAction::FinalizeCommit => {
                if observed.state != DeleteRecoveryState::CommitFinished {
                    return Err(DiskError::DiskOngoingReq);
                }
                for name in [TARGET, PREPARED, FINISHED] {
                    match os::remove_file_with_owner(rollback.join(name), owner.clone()).await {
                        Ok(()) => {}
                        Err(err) if err.kind() == ErrorKind::NotFound => {}
                        Err(err) => return Err(to_file_error(err).into()),
                    }
                }
                if effective_durability(volume).syncs_commit_metadata() {
                    os::fsync_dir_with_owner(&rollback, owner.clone())
                        .await
                        .map_err(to_file_error)?;
                }
                os::remove_file_with_owner(rollback.join(DELETE_QUORUM_COMMITTED_FILE), owner.clone())
                    .await
                    .map_err(to_file_error)?;
                self.delete_unleased_with_namespace_owner(
                    volume,
                    &format!("{object}/{transaction}"),
                    &DeleteOptions {
                        immediate: true,
                        ..Default::default()
                    },
                    owner.clone(),
                )
                .await?;
            }
            DeleteRecoveryAction::Rollback => {
                if observed.state != DeleteRecoveryState::NewPrepared || !observed.rollback_data_complete {
                    return Err(DiskError::FileCorrupt);
                }
                let source = if observed.backup_digest.is_some() {
                    format!("{object}/{transaction}/{STORAGE_FORMAT_FILE_BACKUP}")
                } else {
                    format!("{object}/{STORAGE_FORMAT_FILE}")
                };
                if journal.original_digest.is_some() {
                    let bytes = self.read_all(volume, &source).await?;
                    let metadata = FileMeta::load(&bytes)?;
                    for version in &metadata.versions {
                        if version.header.version_type != rustfs_filemeta::VersionType::Object {
                            continue;
                        }
                        let mut info = version.into_fileinfo(volume, object, true)?;
                        info.hydrate_shard_integrity()?;
                        if info.inline_data() || info.is_remote() {
                            continue;
                        }
                        let dir = info.data_dir.filter(|id| !id.is_nil()).ok_or(DiskError::FileCorrupt)?;
                        let codec = coding::Erasure::try_new_with_options(
                            info.erasure.data_blocks,
                            info.erasure.parity_blocks,
                            info.erasure.block_size,
                            info.uses_legacy_checksum,
                        )
                        .map_err(DiskError::from)?;
                        let live = self.io_get_object_path(volume, &format!("{object}/{dir}"))?;
                        let staged = self.io_get_object_path(volume, &format!("{object}/{transaction}/{dir}"))?;
                        for part in &info.parts {
                            let name = format!("part.{}", part.number);
                            let checksum = info.erasure.get_checksum_info(part.number);
                            let algo = if info.uses_legacy_checksum && checksum.algorithm == HashAlgorithm::HighwayHash256S {
                                HashAlgorithm::HighwayHash256SLegacy
                            } else {
                                checksum.algorithm
                            };
                            if let Some(integrity) = &part.integrity {
                                verify_recovery_integrity_sources(
                                    &live,
                                    &staged,
                                    integrity,
                                    info.erasure.index.checked_sub(1).ok_or(DiskError::FileCorrupt)?,
                                    algo.clone(),
                                    owner.clone(),
                                )
                                .await?;
                            }
                            let mut checked = false;
                            for part_root in [&live, &staged] {
                                let path = part_root.join(&name);
                                match fs::symlink_metadata(&path).await {
                                    Ok(metadata) if metadata.is_file() => {}
                                    Ok(_) => return Err(DiskError::InvalidPath),
                                    Err(err) if err.kind() == ErrorKind::NotFound => continue,
                                    Err(err) => return Err(to_file_error(err).into()),
                                }
                                self.bitrot_verify(
                                    &path,
                                    usize::try_from(
                                        codec.shard_file_size(i64::try_from(part.size).map_err(|_| DiskError::FileCorrupt)?),
                                    )
                                    .map_err(|_| DiskError::FileCorrupt)?,
                                    algo.clone(),
                                    codec.shard_size(),
                                )
                                .await?;
                                checked = true;
                            }
                            if !checked {
                                return Err(DiskError::FileCorrupt);
                            }
                        }
                    }
                }
                if journal.original_digest.is_none() && journal.target_digest.is_none() {
                    for name in [TARGET, PREPARED, FINISHED] {
                        match os::remove_file_with_owner(rollback.join(name), owner.clone()).await {
                            Ok(()) => {}
                            Err(err) if err.kind() == ErrorKind::NotFound => {}
                            Err(err) => return Err(to_file_error(err).into()),
                        }
                    }
                    self.delete_unleased_with_namespace_owner(
                        volume,
                        &format!("{object}/{transaction}"),
                        &DeleteOptions {
                            immediate: true,
                            ..Default::default()
                        },
                        owner.clone(),
                    )
                    .await?;
                } else {
                    restore_delete_rollback_with_namespace_owner(
                        volume,
                        &root,
                        &root.join(STORAGE_FORMAT_FILE),
                        transaction,
                        &self.publication_root,
                        owner.clone(),
                    )
                    .await?;
                }
            }
            DeleteRecoveryAction::AdoptCommit { .. } => return Err(DiskError::MethodNotAllowed),
            DeleteRecoveryAction::DiscardLegacy => return Err(DiskError::MethodNotAllowed),
        }
        self.delete_recovery_inspect_owned(volume, object, transaction, owner).await
    }

    pub(super) async fn delete_recovery_barrier(&self, volume: &str, object: &str, transaction: Uuid, stage: &str) -> Result<()> {
        barrier(volume, object, transaction, stage, &self.root).await
    }
}

#[cfg(not(feature = "e2e-test-hooks"))]
async fn barrier(_volume: &str, _object: &str, _transaction: Uuid, _stage: &str, _drive: &Path) -> Result<()> {
    Ok(())
}

#[cfg(feature = "e2e-test-hooks")]
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct BarrierArm {
    nonce: String,
    bucket: String,
    object: String,
    stage: String,
    #[serde(default)]
    disk: Option<String>,
}

#[cfg(feature = "e2e-test-hooks")]
async fn barrier(volume: &str, object: &str, transaction: Uuid, stage: &str, drive: &Path) -> Result<()> {
    let Some(directory) = std::env::var_os("RUSTFS_DELETE_RECOVERY_BARRIER_DIR").map(PathBuf::from) else {
        return Ok(());
    };
    let bytes = match fs::read(directory.join("arm.json")).await {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(to_file_error(err).into()),
    };
    let arm: BarrierArm = serde_json::from_slice(&bytes)?;
    if arm.bucket != volume
        || arm.object != object
        || arm.stage != stage
        || arm.disk.as_deref().is_some_and(|disk| Path::new(disk) != drive)
    {
        return Ok(());
    }
    if arm.nonce.is_empty()
        || arm.nonce.len() > 64
        || !arm
            .nonce
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
    {
        return Err(DiskError::InvalidPath);
    }
    let drive_hash = rustfs_utils::crypto::hex(digest(drive.to_string_lossy().as_bytes()));
    let observation = serde_json::json!({"transaction": transaction, "bucket": volume, "object": object, "stage": stage, "pid": std::process::id(), "drive": drive});
    fs::write(
        directory.join(format!("reached.{}.{}.{}.json", arm.nonce, std::process::id(), &drive_hash[..16])),
        serde_json::to_vec(&observation)?,
    )
    .await
    .map_err(to_file_error)?;
    let deadline = Instant::now() + Duration::from_secs(60);
    while !fs::try_exists(directory.join(format!("release.{}", arm.nonce)))
        .await
        .map_err(to_file_error)?
    {
        if Instant::now() >= deadline {
            return Err(DiskError::Timeout);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    if fs::read(directory.join(format!("release.{}", arm.nonce)))
        .await
        .map_err(to_file_error)?
        == b"io_error"
    {
        return Err(DiskError::Io(std::io::Error::other("injected delete recovery barrier failure")));
    }
    Ok(())
}

pub(super) async fn restore_barrier(volume: &str, object_dir: &Path, transaction: Uuid) -> Result<()> {
    #[cfg(feature = "e2e-test-hooks")]
    if let Some(directory) = std::env::var_os("RUSTFS_DELETE_RECOVERY_BARRIER_DIR").map(PathBuf::from) {
        let bytes = match fs::read(directory.join("arm.json")).await {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(()),
            Err(err) => return Err(to_file_error(err).into()),
        };
        let arm: BarrierArm = serde_json::from_slice(&bytes)?;
        if arm.bucket == volume && arm.stage == "RESTORE" && object_dir.ends_with(Path::new(volume).join(&arm.object)) {
            let suffix = Path::new(volume).join(&arm.object);
            let canonical = fs::canonicalize(object_dir).await.map_err(to_file_error)?;
            let mut drive = canonical;
            for _ in suffix.components() {
                drive.pop();
            }
            return barrier(volume, &arm.object, transaction, "RESTORE", &drive).await;
        }
    }
    let _ = (volume, object_dir, transaction);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disk::local::test::{ensure_test_volume, test_file_info, test_meta};
    use tokio::io::AsyncReadExt;

    #[test]
    fn delete_recovery_native_sidecar_classifier_is_bounded() {
        assert!(delete_recovery_part_file("part.1"));
        assert!(delete_recovery_part_file("part.1.meta"));
        assert!(delete_recovery_part_file(&format!("part.1.integrity.{}", Uuid::new_v4())));
        for name in [
            "part.0.meta",
            "part.1.meta.extra",
            "part.1.integrity.invalid",
            "xl.meta",
            "child/",
        ] {
            assert!(!delete_recovery_part_file(name), "unknown sidecar {name} must stay protected");
        }
        assert!(!delete_recovery_part_file(&format!("part.1.integrity.{}", Uuid::nil())));
        assert!(!metadata_less_part_file("part.1.meta"), "the heal shard classifier remains unchanged");
    }

    #[tokio::test]
    #[serial_test::serial(shard_integrity_rollout)]
    async fn delete_recovery_multipart_rollback_moves_native_sidecars_and_preserves_proofs() {
        use crate::bucket::metadata_sys::{init_bucket_metadata_sys, test_support::isolated_store_over_temp_disks};
        use crate::object_api::{ObjectOptions, PutObjReader};
        use crate::storage_api_contracts::bucket::{BucketOperations, MakeBucketOptions};
        use crate::storage_api_contracts::multipart::{CompletePart, MultipartOperations};
        use crate::storage_api_contracts::object::ObjectIO;
        use http::HeaderMap;

        temp_env::async_with_vars(
            [
                (rustfs_config::ENV_SHARD_INTEGRITY_WRITE, Some("true")),
                (rustfs_config::ENV_SHARD_INTEGRITY_FLEET_CONFIRMED, Some("true")),
            ],
            async {
                for damage in [None, Some("missing"), Some("corrupt"), Some("payload")] {
                    let (dirs, store) = isolated_store_over_temp_disks().await;
                    init_bucket_metadata_sys(store.clone(), Vec::new()).await;
                    let bucket = "recovery-multipart";
                    let object = "object";
                    store.make_bucket(bucket, &MakeBucketOptions::default()).await.unwrap();
                    let upload = store
                        .new_multipart_upload(bucket, object, &ObjectOptions::default())
                        .await
                        .unwrap();
                    let payload = vec![63; 6 * 1024 * 1024];
                    let mut reader = PutObjReader::from_vec(payload.clone());
                    let part = store
                        .put_object_part(bucket, object, &upload.upload_id, 1, &mut reader, &ObjectOptions::default())
                        .await
                        .unwrap();
                    store
                        .clone()
                        .complete_multipart_upload(
                            bucket,
                            object,
                            &upload.upload_id,
                            vec![CompletePart {
                                part_num: part.part_num,
                                etag: part.etag,
                                ..Default::default()
                            }],
                            &ObjectOptions::default(),
                        )
                        .await
                        .unwrap();
                    let disks = store.pools[0].disk_set[0].disks.read().await.clone();
                    let transaction = Uuid::new_v4();
                    let mut originals = Vec::new();
                    for (dir, disk) in dirs.iter().zip(&disks) {
                        let root = dir.path().join(bucket).join(object);
                        let original = fs::read(root.join(STORAGE_FORMAT_FILE)).await.unwrap();
                        let metadata = FileMeta::load(&original).unwrap();
                        let mut info = metadata.versions[0].into_fileinfo(bucket, object, true).unwrap();
                        info.hydrate_shard_integrity().unwrap();
                        let data = info.data_dir.unwrap();
                        let proof_name = info.parts[0]
                            .integrity
                            .as_ref()
                            .expect("real protected MPU proof")
                            .file_name();
                        assert!(root.join(data.to_string()).join("part.1.meta").is_file());
                        disk.as_ref()
                            .unwrap()
                            .delete_version_recoverable(
                                bucket,
                                object,
                                FileInfo {
                                    name: object.to_owned(),
                                    ..Default::default()
                                },
                                false,
                                DeleteOptions {
                                    old_data_dir: Some(transaction),
                                    ..Default::default()
                                },
                            )
                            .await
                            .unwrap();
                        let staged = root.join(transaction.to_string()).join(data.to_string());
                        fs::rename(root.join(data.to_string()), &staged).await.unwrap();
                        originals.push((root, data, proof_name, original));
                    }
                    let (root, data, proof, _) = &originals[0];
                    let proof_path = root.join(transaction.to_string()).join(data.to_string()).join(proof);
                    if let Some(damage) = damage {
                        if damage == "missing" {
                            fs::remove_file(&proof_path).await.unwrap();
                        } else if damage == "corrupt" {
                            let mut bytes = fs::read(&proof_path).await.unwrap();
                            bytes[rustfs_filemeta::shard_integrity::INDEX_HEADER_SIZE] ^= 1;
                            fs::write(&proof_path, bytes).await.unwrap();
                        } else {
                            let metadata = FileMeta::load(&originals[0].3).unwrap();
                            let info = metadata.versions[0].into_fileinfo(bucket, object, true).unwrap();
                            let codec = coding::Erasure::try_new_with_options(
                                info.erasure.data_blocks,
                                info.erasure.parity_blocks,
                                info.erasure.block_size,
                                info.uses_legacy_checksum,
                            )
                            .unwrap();
                            let algorithm = info.erasure.get_checksum_info(1).algorithm;
                            let path = proof_path.parent().unwrap().join("part.1");
                            let mut bytes = fs::read(&path).await.unwrap();
                            let hash_size = algorithm.size();
                            let part_size = usize::try_from(codec.shard_file_size(info.parts[0].size as i64)).unwrap();
                            let length = part_size.min(codec.shard_size());
                            bytes[hash_size] ^= 1;
                            let rewritten = algorithm.hash_encode(&bytes[hash_size..hash_size + length]).as_ref().to_vec();
                            bytes[..hash_size].copy_from_slice(&rewritten);
                            coding::bitrot_verify(
                                std::io::Cursor::new(bytes.clone()),
                                bytes.len(),
                                part_size,
                                algorithm,
                                codec.shard_size(),
                            )
                            .await
                            .expect("recomputed self-described bitrot checksum passes the former rollback gate");
                            fs::write(path, bytes).await.unwrap();
                        }
                        let disk = disks[0].as_ref().unwrap();
                        let observed = disk.inspect_delete_transaction(bucket, object, transaction).await.unwrap();
                        let backup = fs::read(root.join(transaction.to_string()).join(STORAGE_FORMAT_FILE_BACKUP))
                            .await
                            .unwrap();
                        assert!(matches!(
                            disk.settle_delete_transaction(
                                bucket,
                                object,
                                transaction,
                                observed.fingerprint,
                                DeleteRecoveryAction::Rollback
                            )
                            .await,
                            Err(DiskError::FileCorrupt)
                        ));
                        assert!(!root.join(STORAGE_FORMAT_FILE).exists());
                        assert!(root.join(transaction.to_string()).join(PREPARED).is_file());
                        assert_eq!(
                            fs::read(root.join(transaction.to_string()).join(STORAGE_FORMAT_FILE_BACKUP))
                                .await
                                .unwrap(),
                            backup
                        );
                        continue;
                    }
                    // Both directions of a partially restored native layout:
                    // proof still staged with live payload, and the inverse.
                    for (index, (root, data, proof, _)) in originals.iter().take(2).enumerate() {
                        let live = root.join(data.to_string());
                        fs::create_dir(&live).await.unwrap();
                        let name = if index == 0 { "part.1" } else { proof.as_str() };
                        fs::rename(root.join(transaction.to_string()).join(data.to_string()).join(name), live.join(name))
                            .await
                            .unwrap();
                    }
                    for (disk, (root, data, proof, original)) in disks.iter().zip(originals) {
                        let disk = disk.as_ref().unwrap();
                        let observed = disk.inspect_delete_transaction(bucket, object, transaction).await.unwrap();
                        assert!(observed.rollback_data_complete);
                        disk.settle_delete_transaction(
                            bucket,
                            object,
                            transaction,
                            observed.fingerprint,
                            DeleteRecoveryAction::Rollback,
                        )
                        .await
                        .expect("rollback restores native multipart sidecars");
                        assert_eq!(fs::read(root.join(STORAGE_FORMAT_FILE)).await.unwrap(), original);
                        assert!(root.join(data.to_string()).join("part.1.meta").is_file());
                        assert!(root.join(data.to_string()).join(proof).is_file());
                        assert!(!root.join(transaction.to_string()).exists());
                    }
                    let mut reader = store.pools[0].disk_set[0]
                        .get_object_reader(bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
                        .await
                        .unwrap();
                    let mut restored = Vec::new();
                    reader.stream.read_to_end(&mut restored).await.unwrap();
                    assert_eq!(restored, payload, "restored protected MPU remains readable through integrity checks");
                }
            },
        )
        .await;
    }

    async fn fixture() -> (tempfile::TempDir, LocalDisk, FileInfo, Vec<u8>) {
        let temp = tempfile::tempdir().expect("temporary disk");
        let endpoint = Endpoint::try_from(temp.path().to_str().expect("UTF-8 disk path")).expect("endpoint");
        let disk = LocalDisk::new(&endpoint, false).await.expect("local disk");
        ensure_test_volume(&disk, "recovery").await;
        let mut info = test_file_info("object", Uuid::nil(), None, Some(Bytes::from_static(b"complete original inline payload")));
        info.set_inline_data();
        let bytes = test_meta(info.clone());
        let path = disk.io_get_object_path("recovery", "object/xl.meta").expect("metadata path");
        fs::create_dir_all(path.parent().expect("object directory"))
            .await
            .expect("object directory");
        fs::write(path, &bytes).await.expect("original metadata");
        (temp, disk, info, bytes)
    }

    #[tokio::test]
    async fn delete_recovery_live_write_backup_preserves_delete_evidence() {
        let (_temp, disk, info, original) = fixture().await;
        let root = disk.io_get_object_path("recovery", "object").unwrap();
        let meta = FileMeta::load(&original).unwrap();
        let transaction = inline_metadata_rollback_dir(Uuid::nil(), &meta);
        let rollback = root.join(transaction.to_string());
        fs::create_dir_all(&rollback).await.unwrap();
        fs::write(rollback.join(STORAGE_FORMAT_FILE_BACKUP), &original).await.unwrap();
        let mut healing = info.clone();
        healing.set_healing();
        fs::write(root.join(STORAGE_FORMAT_FILE), test_meta(healing)).await.unwrap();
        assert!(live_write_backup(&root, transaction).unwrap());
        let page = disk
            .delete_recovery_discover("recovery", Some("object"), None, 128)
            .await
            .unwrap();
        assert!(page.transactions.is_empty());
        assert_eq!(fs::read(rollback.join(STORAGE_FORMAT_FILE_BACKUP)).await.unwrap(), original);
        fs::write(rollback.join(DELETE_QUORUM_COMMITTED_FILE), []).await.unwrap();
        assert!(
            !live_write_backup(&root, transaction).unwrap(),
            "even an empty legacy receipt is delete evidence"
        );
        fs::remove_file(rollback.join(DELETE_QUORUM_COMMITTED_FILE)).await.unwrap();
        let mut changed_parts = info.clone();
        changed_parts.parts[0].actual_size += 1;
        fs::write(root.join(STORAGE_FORMAT_FILE), test_meta(changed_parts))
            .await
            .unwrap();
        assert!(
            !matches!(live_write_backup(&root, transaction), Ok(true)),
            "changed part metadata cannot hide recovery evidence"
        );
        let mut changed = info;
        changed.metadata.insert("x-amz-object-lock-legal-hold".into(), "ON".into());
        fs::write(root.join(STORAGE_FORMAT_FILE), test_meta(changed)).await.unwrap();
        assert!(
            !live_write_backup(&root, transaction).unwrap(),
            "protected metadata differences remain meaningful"
        );
        fs::remove_file(root.join(STORAGE_FORMAT_FILE)).await.unwrap();
        assert!(
            !live_write_backup(&root, transaction).unwrap(),
            "hidden legacy deletion remains discoverable"
        );
    }

    #[tokio::test]
    async fn delete_recovery_live_external_backup_rejects_partial_version_delete() {
        let (_temp, disk, mut info, _) = fixture().await;
        let root = disk.io_get_object_path("recovery", "object").unwrap();
        let transaction = Uuid::new_v4();
        info.data = None;
        rustfs_utils::http::metadata_compat::remove_str(
            &mut info.metadata,
            rustfs_utils::http::metadata_compat::SUFFIX_INLINE_DATA,
        );
        info.data_dir = Some(transaction);
        info.version_id = Some(Uuid::new_v4());
        let mut meta = FileMeta::new();
        meta.add_version(info.clone()).unwrap();
        let original = meta.marshal_msg().unwrap();
        let rollback = root.join(transaction.to_string());
        fs::create_dir_all(&rollback).await.unwrap();
        fs::write(rollback.join(STORAGE_FORMAT_FILE_BACKUP), &original).await.unwrap();
        info.set_healing();
        fs::write(root.join(STORAGE_FORMAT_FILE), test_meta(info.clone()))
            .await
            .unwrap();
        assert!(live_write_backup(&root, transaction).unwrap());
        let mut sibling = info.clone();
        sibling.version_id = Some(Uuid::new_v4());
        sibling.data_dir = Some(Uuid::new_v4());
        meta.add_version(sibling).unwrap();
        fs::write(rollback.join(STORAGE_FORMAT_FILE_BACKUP), meta.marshal_msg().unwrap())
            .await
            .unwrap();
        assert!(
            !live_write_backup(&root, transaction).unwrap(),
            "a missing backed-up sibling retains delete recovery"
        );
        assert_eq!(
            disk.inspect_delete_transaction("recovery", "object", transaction)
                .await
                .unwrap()
                .state,
            DeleteRecoveryState::LegacyAmbiguous
        );
        assert!(
            disk.discover_delete_transactions("recovery", Some("object"), None, 128)
                .await
                .unwrap()
                .transactions
                .iter()
                .any(|id| id.transaction == transaction)
        );
    }

    fn request() -> FileInfo {
        FileInfo {
            name: "object".to_owned(),
            version_id: Some(Uuid::nil()),
            ..Default::default()
        }
    }

    #[test]
    fn delete_recovery_generation_known_aliases_are_equal_unknowns_stay_distinct() {
        use rustfs_utils::http::metadata_compat as compat;
        let mut legacy = request();
        legacy
            .metadata
            .insert(format!("{}{}", compat::MINIO_INTERNAL_PREFIX, compat::SUFFIX_INLINE_DATA), String::new());
        legacy
            .metadata
            .insert(format!("{}{}", compat::MINIO_INTERNAL_PREFIX, compat::SUFFIX_COMPRESSION), "gzip".into());
        let mut modern = legacy.clone();
        compat::insert_str(&mut modern.metadata, compat::SUFFIX_INLINE_DATA, String::new());
        compat::insert_str(&mut modern.metadata, compat::SUFFIX_COMPRESSION, "gzip".into());
        assert_eq!(
            generation(legacy.clone()).expect("legacy identity"),
            generation(modern.clone()).expect("modern identity")
        );
        modern.metadata.insert(
            format!("{}{}", compat::RUSTFS_INTERNAL_PREFIX, compat::SUFFIX_COMPRESSION),
            "different".into(),
        );
        assert!(matches!(generation(modern), Err(DiskError::FileCorrupt)));
        let mut unknown = legacy.clone();
        legacy
            .metadata
            .insert("x-minio-internal-unknown-extension".into(), "same".into());
        unknown
            .metadata
            .insert("x-rustfs-internal-unknown-extension".into(), "same".into());
        assert_ne!(
            generation(legacy).expect("legacy unknown field"),
            generation(unknown).expect("modern unknown field")
        );
    }

    #[tokio::test]
    async fn delete_recovery_inactive_preparation_artifacts_use_existing_tmp_cleanup() {
        let (_temp, disk, _, original) = fixture().await;
        let temporary = disk.io_get_bucket_path(RUSTFS_META_TMP_BUCKET).unwrap();
        fs::create_dir_all(temporary.join("delete-preparation-interrupted"))
            .await
            .unwrap();
        fs::write(temporary.join("delete-preparation-interrupted").join(TARGET), b"unfinished target")
            .await
            .unwrap();
        fs::create_dir_all(temporary.join(".delete-rollback-interrupted"))
            .await
            .unwrap();
        fs::write(
            temporary.join(".delete-rollback-interrupted").join("metadata"),
            b"unpublished replacement",
        )
        .await
        .unwrap();
        assert!(
            disk.discover_delete_transactions("recovery", Some("object"), None, 16)
                .await
                .unwrap()
                .transactions
                .is_empty()
        );
        let ready = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let notify = Arc::new(Notify::new());
        LocalDisk::cleanup_tmp_on_startup(disk.io_root(), &disk.publication_root, ready.clone(), notify)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while ready.load(Ordering::Acquire) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(!temporary.join("delete-preparation-interrupted").exists());
        assert!(!temporary.join(".delete-rollback-interrupted").exists());
        assert!(!disk.io_root().join(RUSTFS_META_TMP_OLD_BUCKET).exists());
        assert!(temporary.join(".trash").is_dir(), "startup recreates the active trash directory");
        assert_eq!(disk.read_all("recovery", "object/xl.meta").await.unwrap().as_ref(), original);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn delete_recovery_activation_is_complete_before_metadata_mutation_even_after_cancel() {
        use crate::disk::os::prepared_publication_test_hooks as hooks;
        use futures::FutureExt;
        let (_temp, disk, _, original) = fixture().await;
        let disk = Arc::new(disk);
        let transaction = Uuid::new_v4();
        let destination = disk.io_get_object_path("recovery", &format!("object/{transaction}")).unwrap();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let hook = hooks::install_at(hooks::Stage::Rename, &destination, move || {
            let _ = entered_tx.send(());
            let _ = release_rx.recv();
        });
        let deleting = disk.clone();
        let mut request = request();
        request.expire_restored = true;
        let task = tokio::spawn(async move {
            deleting
                .delete_version_recoverable(
                    "recovery",
                    "object",
                    request,
                    false,
                    DeleteOptions {
                        old_data_dir: Some(transaction),
                        ..Default::default()
                    },
                )
                .await
        });
        let observations = std::panic::AssertUnwindSafe(async {
            tokio::time::timeout(Duration::from_secs(10), entered_rx)
                .await
                .unwrap()
                .unwrap();
            assert!(!destination.exists());
            assert_eq!(disk.read_all("recovery", "object/xl.meta").await.unwrap().as_ref(), original);
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
        })
        .catch_unwind()
        .await;
        let _ = release_tx.send(());
        drop(hook);
        tokio::time::timeout(Duration::from_secs(5), async {
            while !destination.exists() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        for name in [PREPARED, TARGET, STORAGE_FORMAT_FILE_BACKUP] {
            assert!(destination.join(name).is_file());
        }
        let state = disk
            .inspect_delete_transaction("recovery", "object", transaction)
            .await
            .unwrap();
        assert_eq!(state.state, DeleteRecoveryState::NewPrepared);
        assert_eq!(state.current_digest, Some(digest(&original)));
        if let Err(panic) = observations {
            std::panic::resume_unwind(panic);
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn delete_recovery_receiver_volume_guard_outlives_cancelled_restore() {
        use crate::disk::os::prepared_publication_test_hooks as hooks;
        use futures::FutureExt;
        let (_temp, disk, _, _) = fixture().await;
        let disk = Arc::new(disk);
        let transaction = Uuid::new_v4();
        let state = apply(&disk, transaction).await;
        let metadata = disk.io_get_object_path("recovery", "object/xl.meta").unwrap();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let hook = hooks::install_at(hooks::Stage::Rename, &metadata, move || {
            let _ = entered_tx.send(());
            let _ = release_rx.recv();
        });
        let restoring = disk.clone();
        let task = tokio::spawn(async move {
            restoring
                .settle_delete_transaction("recovery", "object", transaction, state.fingerprint, DeleteRecoveryAction::Rollback)
                .await
        });
        let mut deletion = None;
        let observations = std::panic::AssertUnwindSafe(async {
            tokio::time::timeout(Duration::from_secs(10), entered_rx)
                .await
                .unwrap()
                .unwrap();
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
            let removing = disk.clone();
            deletion = Some(tokio::spawn(async move { removing.delete_volume("recovery", true).await }));
            assert!(
                tokio::time::timeout(Duration::from_millis(100), deletion.as_mut().unwrap())
                    .await
                    .is_err()
            );
        })
        .catch_unwind()
        .await;
        let _ = release_tx.send(());
        drop(hook);
        if let Some(deletion) = deletion {
            tokio::time::timeout(Duration::from_secs(5), deletion)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        }
        disk.make_volume("recovery").await.unwrap();
        assert!(!metadata.exists(), "late restore must finish before volume recreation");
        if let Err(panic) = observations {
            std::panic::resume_unwind(panic);
        }
    }

    async fn apply(disk: &LocalDisk, transaction: Uuid) -> DeleteRecoveryInspection {
        disk.delete_version_recoverable(
            "recovery",
            "object",
            request(),
            false,
            DeleteOptions {
                old_data_dir: Some(transaction),
                ..Default::default()
            },
        )
        .await
        .expect("prepared metadata application");
        let inspected = disk
            .inspect_delete_transaction("recovery", "object", transaction)
            .await
            .expect("inspect applied transaction");
        assert_eq!(
            inspected.current_digest,
            inspected.journal.as_ref().unwrap().target_digest,
            "planned target must match the unchanged mutation loop"
        );
        inspected
    }

    #[tokio::test]
    async fn delete_recovery_uncommitted_inline_rolls_back_exact_bytes() {
        let (_temp, disk, _info, original) = fixture().await;
        let transaction = Uuid::new_v4();
        let observed = apply(&disk, transaction).await;
        assert_eq!(observed.state, DeleteRecoveryState::NewPrepared);
        assert!(observed.rollback_data_complete);
        let restored = disk
            .settle_delete_transaction("recovery", "object", transaction, observed.fingerprint, DeleteRecoveryAction::Rollback)
            .await
            .expect("rollback intact prepared metadata");
        assert_eq!(restored.state, DeleteRecoveryState::Absent);
        assert_eq!(
            disk.read_all("recovery", "object/xl.meta")
                .await
                .expect("readable original metadata")
                .as_ref(),
            original
        );
    }

    #[tokio::test]
    async fn delete_recovery_preparation_records_existing_decision_before_new_generation() {
        let (_temp, disk, info, original) = fixture().await;
        let first = Uuid::new_v4();
        let applied = apply(&disk, first).await;
        disk.settle_delete_transaction("recovery", "object", first, applied.fingerprint, DeleteRecoveryAction::PublishCommit)
            .await
            .expect("persist first decision");
        let mut newer = info;
        newer.mod_time = newer.mod_time.map(|time| time + time::Duration::seconds(1));
        let newer_bytes = test_meta(newer);
        assert_ne!(newer_bytes, original);
        fs::write(disk.io_get_object_path("recovery", "object/xl.meta").unwrap(), &newer_bytes)
            .await
            .unwrap();
        let second = Uuid::new_v4();
        let second_state = apply(&disk, second).await;
        assert_eq!(second_state.journal.unwrap().predecessors, vec![first]);
    }

    #[tokio::test]
    async fn delete_recovery_large_intent_fails_before_metadata_application() {
        let (_temp, disk, _info, original) = fixture().await;
        let transaction = Uuid::new_v4();
        let mut large = request();
        large
            .metadata
            .insert("oversized-intent".to_owned(), "x".repeat(usize::try_from(MAX_METADATA).unwrap()));
        assert!(
            disk.delete_version_recoverable(
                "recovery",
                "object",
                large,
                false,
                DeleteOptions {
                    old_data_dir: Some(transaction),
                    ..Default::default()
                }
            )
            .await
            .is_err()
        );
        assert_eq!(disk.read_all("recovery", "object/xl.meta").await.unwrap().as_ref(), original);
        assert!(
            !disk
                .io_get_object_path("recovery", &format!("object/{transaction}/{PREPARED}"))
                .unwrap()
                .exists()
        );
    }

    #[tokio::test]
    async fn delete_recovery_metadata_updates_publish_exact_prepared_target_bytes() {
        for batch in [false, true] {
            for transition in [false, true] {
                let (_temp, disk, mut info, _) = fixture().await;
                info.metadata.insert("x-amz-meta-first".into(), "retained-a".into());
                info.metadata.insert("x-amz-meta-second".into(), "retained-b".into());
                info.metadata
                    .insert(rustfs_filemeta::metadata_keys::RESTORE.into(), "ongoing-request=\"false\"".into());
                let original = test_meta(info);
                fs::write(disk.io_get_object_path("recovery", "object/xl.meta").unwrap(), &original)
                    .await
                    .unwrap();
                let transaction = Uuid::new_v4();
                let mut change = request();
                if transition {
                    change.transition_status = "complete".into();
                    change.transition_tier = "test-tier".into();
                    change.transitioned_objname = "remote/object".into();
                } else {
                    change.expire_restored = true;
                }
                let opts = DeleteOptions {
                    old_data_dir: Some(transaction),
                    ..Default::default()
                };
                if batch {
                    let errors = disk
                        .delete_versions_recoverable(
                            "recovery",
                            vec![FileInfoVersions {
                                name: "object".into(),
                                volume: "recovery".into(),
                                versions: vec![change],
                                ..Default::default()
                            }],
                            opts,
                        )
                        .await;
                    assert_eq!(errors, vec![None]);
                } else {
                    disk.delete_version_recoverable("recovery", "object", change, false, opts)
                        .await
                        .unwrap();
                }
                let observed = disk
                    .inspect_delete_transaction("recovery", "object", transaction)
                    .await
                    .unwrap();
                assert_eq!(observed.current_digest, observed.journal.unwrap().target_digest);
                let applied = disk.read_all("recovery", "object/xl.meta").await.unwrap();
                let prepared = disk
                    .read_all("recovery", &format!("object/{transaction}/{TARGET}"))
                    .await
                    .unwrap();
                assert_eq!(applied, prepared, "batch={batch}, transition={transition}");
                assert_ne!(applied.as_ref(), original);
            }
        }
    }

    async fn legacy_fixture(disk: &LocalDisk, original: &[u8], transaction: Uuid) -> PathBuf {
        let root = disk.io_get_object_path("recovery", "object").unwrap();
        fs::remove_file(root.join(STORAGE_FORMAT_FILE)).await.unwrap();
        fs::create_dir(root.join(transaction.to_string())).await.unwrap();
        fs::write(root.join(transaction.to_string()).join(STORAGE_FORMAT_FILE_BACKUP), original)
            .await
            .unwrap();
        root
    }

    #[tokio::test]
    async fn delete_recovery_live_repair_backup_preserves_legacy_delete_guards() {
        for shape in ["live", "rollback-marker", "missing-current", "foreign-backup"] {
            let (_temp, disk, mut info, _) = fixture().await;
            let transaction = Uuid::new_v4();
            info.data_dir = Some(transaction);
            let root = disk.io_get_object_path("recovery", "object").unwrap();
            let current = test_meta(info.clone());
            fs::write(root.join(STORAGE_FORMAT_FILE), &current).await.unwrap();
            let rollback = root.join(transaction.to_string());
            fs::create_dir(&rollback).await.unwrap();
            if shape == "foreign-backup" {
                info.data_dir = Some(Uuid::new_v4());
            }
            let backup = test_meta(info);
            fs::write(rollback.join(STORAGE_FORMAT_FILE_BACKUP), &backup).await.unwrap();
            if shape == "rollback-marker" {
                fs::write(rollback.join(DELETE_MARKER_ROLLBACK_FILE), []).await.unwrap();
            } else if shape == "missing-current" {
                fs::remove_file(root.join(STORAGE_FORMAT_FILE)).await.unwrap();
            }
            let inspection = disk
                .inspect_delete_transaction("recovery", "object", transaction)
                .await
                .unwrap();
            assert_eq!(
                inspection.state,
                if shape == "live" {
                    DeleteRecoveryState::Absent
                } else {
                    DeleteRecoveryState::LegacyAmbiguous
                },
                "{shape}"
            );
            assert_eq!(fs::read(rollback.join(STORAGE_FORMAT_FILE_BACKUP)).await.unwrap(), backup);
            if shape != "missing-current" {
                assert_eq!(fs::read(root.join(STORAGE_FORMAT_FILE)).await.unwrap(), current);
            }
        }
    }

    #[tokio::test]
    async fn delete_recovery_explicit_legacy_discard_accepts_old_empty_receipt() {
        let (_temp, disk, _info, original) = fixture().await;
        let transaction = Uuid::new_v4();
        let root = legacy_fixture(&disk, &original, transaction).await;
        fs::write(root.join(transaction.to_string()).join(DELETE_QUORUM_COMMITTED_FILE), [])
            .await
            .unwrap();
        let state = disk
            .inspect_delete_transaction("recovery", "object", transaction)
            .await
            .unwrap();
        assert_eq!(state.state, DeleteRecoveryState::LegacyAmbiguous);
        assert!(state.explicit_delete_safe);
        let cleared = disk
            .settle_delete_transaction("recovery", "object", transaction, state.fingerprint, DeleteRecoveryAction::DiscardLegacy)
            .await
            .unwrap();
        assert_eq!(cleared.state, DeleteRecoveryState::Absent);
        assert!(!root.join(transaction.to_string()).exists());
    }

    #[tokio::test]
    async fn delete_recovery_prepared_explicit_delete_checks_original_header() {
        for protected in ["none", "version", "lock", "internal-lock"] {
            let (_temp, disk, mut info, _) = fixture().await;
            match protected {
                "version" => info.version_id = Some(Uuid::new_v4()),
                "lock" => {
                    info.metadata.insert("x-amz-object-lock-mode".into(), "COMPLIANCE".into());
                }
                "internal-lock" => rustfs_utils::http::metadata_compat::insert_str(
                    &mut info.metadata,
                    rustfs_utils::http::metadata_compat::SUFFIX_OBJECTLOCK_LEGALHOLD_TIMESTAMP,
                    "known-lock".to_owned(),
                ),
                _ => {}
            }
            fs::write(disk.io_get_object_path("recovery", "object/xl.meta").unwrap(), test_meta(info.clone()))
                .await
                .unwrap();
            let transaction = Uuid::new_v4();
            disk.delete_version_recoverable(
                "recovery",
                "object",
                FileInfo {
                    version_id: info.version_id,
                    ..request()
                },
                false,
                DeleteOptions {
                    old_data_dir: Some(transaction),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            let state = disk
                .inspect_delete_transaction("recovery", "object", transaction)
                .await
                .unwrap();
            assert_eq!(state.state, DeleteRecoveryState::NewPrepared);
            assert_eq!(state.explicit_delete_safe, protected == "none", "original header {protected}");
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn delete_recovery_cancelled_restore_retains_actual_key_and_bucket_guards() {
        use crate::bucket::metadata_sys::{init_bucket_metadata_sys, test_support::isolated_store_over_temp_disks};
        use crate::disk::os::prepared_publication_test_hooks as hooks;
        use crate::object_api::{ObjectOptions, PutObjReader};
        use crate::storage_api_contracts::bucket::{BucketOperations, MakeBucketOptions};
        use crate::storage_api_contracts::object::ObjectIO;
        use futures::FutureExt;
        use rustfs_heal_contracts::heal_channel::HealOpts;

        let (_dirs, store) = isolated_store_over_temp_disks().await;
        init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let bucket = "recover-owned-timeout";
        store.make_bucket(bucket, &MakeBucketOptions::default()).await.unwrap();
        store
            .put_object(bucket, "object", &mut PutObjReader::from_vec(vec![61; 4096]), &ObjectOptions::default())
            .await
            .unwrap();
        let disks = store.pools[0].disk_set[0].disk_inventory().await;
        let transaction = Uuid::new_v4();
        let incarnation = store.bucket_incarnation_id_from_disk(bucket).await.unwrap();
        for disk in disks.iter().flatten() {
            disk.delete_version_recoverable(
                bucket,
                "object",
                request(),
                false,
                DeleteOptions {
                    old_data_dir: Some(transaction),
                    delete_recovery_bucket_incarnation: Some(incarnation),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        }
        let metadata = disks[0]
            .as_ref()
            .unwrap()
            .get_object_path_for_io_if_local(bucket, "object/xl.meta")
            .unwrap()
            .unwrap();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let hook = hooks::install_at(hooks::Stage::Rename, &metadata, move || {
            let _ = entered_tx.send(());
            let _ = release_rx.recv();
        });
        let before = store.ctx.namespace_commit_generation();
        let recovering = store.clone();
        let mut recovery =
            tokio::spawn(async move { recovering.recover_delete_transactions(bucket, &HealOpts::default()).await });
        let mut writer = None;
        let mut bucket_writer = None;
        let observations = std::panic::AssertUnwindSafe(async {
            tokio::time::timeout(Duration::from_secs(10), entered_rx)
                .await
                .unwrap()
                .unwrap();
            assert!(tokio::time::timeout(Duration::from_millis(100), &mut recovery).await.is_err());
            assert!(store.ctx.namespace_commits_pending());
            assert_eq!(store.ctx.namespace_commit_generation(), before + 1);
            recovery.abort();
            assert!(recovery.await.unwrap_err().is_cancelled());
            assert!(store.ctx.namespace_commits_pending(), "late restore must retain namespace accounting");
            let writing = store.clone();
            writer = Some(tokio::spawn(async move {
                writing
                    .put_object(bucket, "object", &mut PutObjReader::from_vec(vec![62; 4096]), &ObjectOptions::default())
                    .await
            }));
            assert!(
                tokio::time::timeout(Duration::from_millis(100), writer.as_mut().unwrap())
                    .await
                    .is_err(),
                "same-key PUT must wait for the late restore"
            );
            let removing = store.clone();
            bucket_writer = Some(tokio::spawn(async move { removing.acquire_bucket_lifecycle_write_lock(bucket).await }));
            assert!(
                tokio::time::timeout(Duration::from_millis(100), bucket_writer.as_mut().unwrap())
                    .await
                    .is_err(),
                "bucket incarnation mutation must wait for the late restore"
            );
        })
        .catch_unwind()
        .await;
        let _ = release_tx.send(());
        drop(hook);
        if let Some(writer) = writer {
            tokio::time::timeout(Duration::from_secs(10), writer)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        }
        if let Some(writer) = bucket_writer {
            drop(
                tokio::time::timeout(Duration::from_secs(10), writer)
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap(),
            );
        }
        if let Err(panic) = observations {
            std::panic::resume_unwind(panic);
        }
    }

    #[tokio::test]
    async fn delete_recovery_explicit_legacy_discard_preserves_versions_locks_and_live_children() {
        for protected in ["version", "lock", "internal-lock", "child", "unknown"] {
            let (_temp, disk, mut info, original) = fixture().await;
            let transaction = Uuid::new_v4();
            let root = legacy_fixture(&disk, &original, transaction).await;
            let rollback = root.join(transaction.to_string());
            match protected {
                "version" => {
                    info.version_id = Some(Uuid::new_v4());
                }
                "lock" => {
                    info.metadata.insert("X-Amz-Object-Lock-Legal-Hold".into(), "ON".into());
                }
                "internal-lock" => {
                    rustfs_utils::http::metadata_compat::insert_str(
                        &mut info.metadata,
                        rustfs_utils::http::metadata_compat::SUFFIX_OBJECTLOCK_RETENTION_TIMESTAMP,
                        "old-lock-state".to_owned(),
                    );
                }
                "child" => {
                    fs::create_dir(rollback.join("live-child")).await.unwrap();
                }
                _ => {
                    fs::write(rollback.join("unknown"), b"preserve").await.unwrap();
                }
            }
            fs::write(rollback.join(STORAGE_FORMAT_FILE_BACKUP), test_meta(info))
                .await
                .unwrap();
            match disk.inspect_delete_transaction("recovery", "object", transaction).await {
                Ok(state) => {
                    assert!(!state.explicit_delete_safe, "protected shape {protected}");
                    assert!(
                        disk.settle_delete_transaction(
                            "recovery",
                            "object",
                            transaction,
                            state.fingerprint,
                            DeleteRecoveryAction::DiscardLegacy
                        )
                        .await
                        .is_err()
                    );
                }
                Err(error) => assert!(matches!(error, DiskError::FileCorrupt | DiskError::InvalidPath)),
            }
            assert!(rollback.join(STORAGE_FORMAT_FILE_BACKUP).exists());
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn delete_recovery_explicit_legacy_discard_rejects_selected_symlink() {
        let (temp, disk, _info, original) = fixture().await;
        let transaction = Uuid::new_v4();
        let root = legacy_fixture(&disk, &original, transaction).await;
        std::os::unix::fs::symlink(temp.path(), root.join(transaction.to_string()).join("linked")).unwrap();
        assert_eq!(
            disk.inspect_delete_transaction("recovery", "object", transaction)
                .await
                .unwrap_err(),
            DiskError::InvalidPath
        );
        assert!(root.join(transaction.to_string()).join(STORAGE_FORMAT_FILE_BACKUP).exists());
    }

    #[tokio::test]
    async fn delete_recovery_commit_retains_proof_until_explicit_finalization() {
        let (_temp, disk, _info, _original) = fixture().await;
        let transaction = Uuid::new_v4();
        let applied = apply(&disk, transaction).await;
        let committed = disk
            .settle_delete_transaction(
                "recovery",
                "object",
                transaction,
                applied.fingerprint,
                DeleteRecoveryAction::PublishCommit,
            )
            .await
            .expect("durable set decision");
        let finished = disk
            .settle_delete_transaction(
                "recovery",
                "object",
                transaction,
                committed.fingerprint,
                DeleteRecoveryAction::FinishCommit,
            )
            .await
            .expect("physical cleanup");
        assert_eq!(finished.state, DeleteRecoveryState::CommitFinished);
        assert_eq!(
            disk.finish_internal_delete_residue("recovery", "object", None).await,
            0,
            "legacy retry must not GC new decision proof"
        );
        let receipt = disk
            .io_get_object_path("recovery", &format!("object/{transaction}/{DELETE_QUORUM_COMMITTED_FILE}"))
            .expect("receipt path");
        assert!(receipt.is_file());
        let settled = disk
            .settle_delete_transaction(
                "recovery",
                "object",
                transaction,
                finished.fingerprint,
                DeleteRecoveryAction::FinalizeCommit,
            )
            .await
            .expect("finalize after coordinator convergence proof");
        assert_eq!(settled.state, DeleteRecoveryState::Absent);
        assert!(!disk.io_get_object_path("recovery", "object").expect("object path").exists());
    }

    #[tokio::test]
    async fn delete_recovery_fingerprint_rejects_new_generation() {
        let (_temp, disk, mut info, _original) = fixture().await;
        let transaction = Uuid::new_v4();
        let applied = apply(&disk, transaction).await;
        info.mod_time = Some(OffsetDateTime::now_utc());
        info.data = Some(Bytes::from_static(b"new generation"));
        let newer = test_meta(info);
        fs::write(disk.io_get_object_path("recovery", "object/xl.meta").expect("metadata path"), &newer)
            .await
            .expect("concurrent new generation");
        assert!(matches!(
            disk.settle_delete_transaction(
                "recovery",
                "object",
                transaction,
                applied.fingerprint,
                DeleteRecoveryAction::PublishCommit
            )
            .await,
            Err(DiskError::OutdatedXLMeta)
        ));
        assert_eq!(
            disk.read_all("recovery", "object/xl.meta")
                .await
                .expect("new generation remains")
                .as_ref(),
            newer
        );
    }

    #[tokio::test]
    async fn delete_recovery_untouched_peer_adopts_only_matching_generation() {
        let (_temp, source, info, original) = fixture().await;
        let (_peer_temp, peer, _peer_info, _) = fixture().await;
        fs::write(peer.io_get_object_path("recovery", "object/xl.meta").expect("peer metadata"), &original)
            .await
            .expect("same original generation");
        let transaction = Uuid::new_v4();
        let applied = apply(&source, transaction).await;
        let committed = source
            .settle_delete_transaction(
                "recovery",
                "object",
                transaction,
                applied.fingerprint,
                DeleteRecoveryAction::PublishCommit,
            )
            .await
            .expect("source durable decision");
        let authority = committed.journal.expect("source intent");
        let untouched = peer
            .inspect_delete_transaction("recovery", "object", transaction)
            .await
            .expect("untouched peer");
        let adopted = peer
            .settle_delete_transaction(
                "recovery",
                "object",
                transaction,
                untouched.fingerprint,
                DeleteRecoveryAction::AdoptCommit {
                    authority: Box::new(authority.clone()),
                },
            )
            .await
            .expect("generation-bound adoption");
        assert_eq!(adopted.state, DeleteRecoveryState::QuorumCommitted);
        let finished = peer
            .settle_delete_transaction("recovery", "object", transaction, adopted.fingerprint, DeleteRecoveryAction::FinishCommit)
            .await
            .expect("converge untouched peer");
        assert_eq!(finished.state, DeleteRecoveryState::CommitFinished);
        assert!(finished.current_digest.is_none());
        let (_foreign_temp, foreign, _, _) = fixture().await;
        let mut replacement = info;
        replacement.mod_time = Some(OffsetDateTime::now_utc());
        let newer = test_meta(replacement);
        fs::write(
            foreign
                .io_get_object_path("recovery", "object/xl.meta")
                .expect("foreign metadata"),
            &newer,
        )
        .await
        .expect("foreign generation");
        let observed = foreign
            .inspect_delete_transaction("recovery", "object", transaction)
            .await
            .expect("inspect foreign generation");
        assert!(matches!(
            foreign
                .settle_delete_transaction(
                    "recovery",
                    "object",
                    transaction,
                    observed.fingerprint,
                    DeleteRecoveryAction::AdoptCommit {
                        authority: Box::new(authority)
                    }
                )
                .await,
            Err(DiskError::OutdatedXLMeta)
        ));
        assert_eq!(
            foreign
                .read_all("recovery", "object/xl.meta")
                .await
                .expect("protected foreign generation")
                .as_ref(),
            newer
        );
    }

    #[tokio::test]
    async fn delete_recovery_adoption_preserves_unrelated_new_version() {
        let (_source_temp, source, original_info, original) = fixture().await;
        let (_peer_temp, peer, _, _) = fixture().await;
        let transaction = Uuid::new_v4();
        let applied = apply(&source, transaction).await;
        let committed = source
            .settle_delete_transaction(
                "recovery",
                "object",
                transaction,
                applied.fingerprint,
                DeleteRecoveryAction::PublishCommit,
            )
            .await
            .unwrap();
        let mut unrelated = original_info;
        unrelated.version_id = Some(Uuid::new_v4());
        unrelated.mod_time = unrelated.mod_time.map(|time| time + time::Duration::seconds(1));
        let mut peer_meta = FileMeta::load(&original).unwrap();
        peer_meta.add_version(unrelated.clone()).unwrap();
        fs::write(
            peer.io_get_object_path("recovery", "object/xl.meta").unwrap(),
            peer_meta.marshal_msg().unwrap(),
        )
        .await
        .unwrap();
        let observed = peer
            .inspect_delete_transaction("recovery", "object", transaction)
            .await
            .unwrap();
        let adopted = peer
            .settle_delete_transaction(
                "recovery",
                "object",
                transaction,
                observed.fingerprint,
                DeleteRecoveryAction::AdoptCommit {
                    authority: Box::new(committed.journal.unwrap()),
                },
            )
            .await
            .unwrap();
        peer.settle_delete_transaction(
            "recovery",
            "object",
            transaction,
            adopted.fingerprint,
            DeleteRecoveryAction::FinishCommit,
        )
        .await
        .unwrap();
        let remaining = FileMeta::load(&peer.read_all("recovery", "object/xl.meta").await.unwrap()).unwrap();
        assert_eq!(remaining.versions.len(), 1);
        assert_eq!(remaining.versions[0].header.version_id, unrelated.version_id);
    }

    #[tokio::test]
    async fn delete_recovery_absent_replay_witness_cannot_acquire_future_predecessors() {
        let (_source_temp, source, _, _) = fixture().await;
        let (_peer_temp, peer, _, _) = fixture().await;
        let transaction = Uuid::new_v4();
        let source_state = apply(&source, transaction).await;
        let committed = source
            .settle_delete_transaction(
                "recovery",
                "object",
                transaction,
                source_state.fingerprint,
                DeleteRecoveryAction::PublishCommit,
            )
            .await
            .unwrap();
        let authority = committed.journal.unwrap();
        assert!(authority.predecessors.is_empty());
        let future = Uuid::new_v4();
        apply(&peer, future).await;
        let absent = peer
            .inspect_delete_transaction("recovery", "object", transaction)
            .await
            .unwrap();
        assert_eq!(absent.state, DeleteRecoveryState::Absent);
        assert!(absent.current_digest.is_none());
        let adopted = peer
            .settle_delete_transaction(
                "recovery",
                "object",
                transaction,
                absent.fingerprint,
                DeleteRecoveryAction::AdoptCommit {
                    authority: Box::new(authority.clone()),
                },
            )
            .await
            .unwrap();
        let witness = adopted.journal.unwrap();
        assert_eq!(witness.predecessors, authority.predecessors);
        assert!(!witness.predecessors.contains(&future));
        assert_eq!(witness.original_versions, authority.original_versions);
        assert_eq!(witness.deleted_versions, authority.deleted_versions);
        assert_eq!(witness.bucket_incarnation, authority.bucket_incarnation);
        assert!(witness.original_digest.is_none() && witness.target_digest.is_none() && witness.data_dirs.is_empty());
    }

    #[tokio::test]
    async fn delete_recovery_raw_discovery_paginates_and_binds_exact_key_scope() {
        let (temp, disk, _info, _original) = fixture().await;
        for index in 0..512 {
            let path = temp
                .path()
                .join("recovery")
                .join(format!("key-{index:04}"))
                .join(Uuid::new_v4().to_string());
            fs::create_dir_all(&path).await.expect("hidden transaction directory");
            fs::write(path.join(STORAGE_FORMAT_FILE_BACKUP), b"legacy backup")
                .await
                .expect("hidden backup");
        }
        let mut cursor = None;
        let mut found = BTreeSet::new();
        loop {
            let page = disk
                .discover_delete_transactions("recovery", None, cursor, 13)
                .await
                .expect("bounded raw discovery");
            found.extend(page.transactions.into_iter().map(|id| id.object));
            cursor = page.next;
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(found.len(), 512, "every hidden legacy transaction must be discovered across pages");
        let scoped = disk
            .discover_delete_transactions("recovery", Some("key-0000"), None, 1)
            .await
            .expect("shallow exact-key discovery");
        assert_eq!(scoped.transactions.len(), 1);
        assert_eq!(scoped.transactions[0].object, "key-0000");
        if let Some(cursor) = scoped.next {
            assert!(
                disk.discover_delete_transactions("recovery", Some("key-0001"), Some(cursor), 1)
                    .await
                    .is_err(),
                "a cursor cannot cross object scopes"
            );
        }
    }

    #[tokio::test]
    async fn delete_recovery_gc_interruption_replays_finished_receipt_without_journal() {
        let (_temp, disk, _info, _) = fixture().await;
        let transaction = Uuid::new_v4();
        let applied = apply(&disk, transaction).await;
        let committed = disk
            .settle_delete_transaction(
                "recovery",
                "object",
                transaction,
                applied.fingerprint,
                DeleteRecoveryAction::PublishCommit,
            )
            .await
            .expect("record decision");
        disk.settle_delete_transaction(
            "recovery",
            "object",
            transaction,
            committed.fingerprint,
            DeleteRecoveryAction::FinishCommit,
        )
        .await
        .expect("finish physical cleanup");
        let root = disk
            .io_get_object_path("recovery", &format!("object/{transaction}"))
            .expect("transaction root");
        fs::remove_file(root.join(PREPARED))
            .await
            .expect("simulate partially completed proof GC");
        fs::remove_file(root.join(FINISHED))
            .await
            .expect("simulate removed completion marker");
        let observed = disk
            .inspect_delete_transaction("recovery", "object", transaction)
            .await
            .expect("receipt preserves completed intent");
        assert_eq!(observed.state, DeleteRecoveryState::CommitFinished);
        let repeated = disk
            .settle_delete_transaction(
                "recovery",
                "object",
                transaction,
                observed.fingerprint,
                DeleteRecoveryAction::FinishCommit,
            )
            .await
            .expect("finish is idempotent after interrupted GC");
        let finalized = disk
            .settle_delete_transaction(
                "recovery",
                "object",
                transaction,
                repeated.fingerprint,
                DeleteRecoveryAction::FinalizeCommit,
            )
            .await
            .expect("finish proof GC");
        assert_eq!(finalized.state, DeleteRecoveryState::Absent);
    }

    #[tokio::test]
    async fn delete_recovery_completed_restore_without_backup_can_finish_rollback() {
        let (_temp, disk, _info, original) = fixture().await;
        let transaction = Uuid::new_v4();
        apply(&disk, transaction).await;
        let root = disk.io_get_object_path("recovery", "object").expect("object root");
        fs::rename(
            root.join(transaction.to_string()).join(STORAGE_FORMAT_FILE_BACKUP),
            root.join(STORAGE_FORMAT_FILE),
        )
        .await
        .expect("simulate crash after rollback metadata rename");
        let observed = disk
            .inspect_delete_transaction("recovery", "object", transaction)
            .await
            .expect("already restored generation");
        assert_eq!(observed.state, DeleteRecoveryState::NewPrepared);
        assert!(
            observed.rollback_data_complete,
            "restored original replaces consumed backup as rollback evidence"
        );
        disk.settle_delete_transaction("recovery", "object", transaction, observed.fingerprint, DeleteRecoveryAction::Rollback)
            .await
            .expect("finish interrupted rollback");
        assert_eq!(
            disk.read_all("recovery", "object/xl.meta")
                .await
                .expect("original remains readable")
                .as_ref(),
            original
        );
    }

    #[tokio::test]
    async fn delete_recovery_completed_restore_without_target_can_finish_versioned_rollback() {
        let (_temp, disk, info, original) = fixture().await;
        let mut remaining = info;
        remaining.version_id = Some(Uuid::new_v4());
        remaining.mod_time = remaining.mod_time.map(|time| time + time::Duration::seconds(1));
        let mut metadata = FileMeta::load(&original).unwrap();
        metadata.add_version(remaining).unwrap();
        let original = metadata.marshal_msg().unwrap();
        let root = disk.io_get_object_path("recovery", "object").unwrap();
        fs::write(root.join(STORAGE_FORMAT_FILE), &original).await.unwrap();
        let transaction = Uuid::new_v4();
        let observed = apply(&disk, transaction).await;
        assert!(observed.journal.as_ref().unwrap().target_digest.is_some());
        let rollback = root.join(transaction.to_string());
        let target = fs::read(rollback.join(TARGET)).await.unwrap();
        fs::remove_file(rollback.join(TARGET)).await.unwrap();
        assert!(matches!(
            disk.inspect_delete_transaction("recovery", "object", transaction).await,
            Err(DiskError::FileCorrupt)
        ));
        fs::write(rollback.join(TARGET), target).await.unwrap();
        fs::rename(rollback.join(STORAGE_FORMAT_FILE_BACKUP), root.join(STORAGE_FORMAT_FILE))
            .await
            .unwrap();
        fs::remove_file(rollback.join(TARGET)).await.unwrap();
        let receipt_path = rollback.join(DELETE_QUORUM_COMMITTED_FILE);
        fs::write(
            &receipt_path,
            serde_json::to_vec(&CommitReceipt {
                journal: observed.journal.unwrap(),
                finished: false,
            })
            .unwrap(),
        )
        .await
        .unwrap();
        assert!(matches!(
            disk.inspect_delete_transaction("recovery", "object", transaction).await,
            Err(DiskError::FileCorrupt)
        ));
        fs::remove_file(receipt_path).await.unwrap();
        let retry = disk
            .inspect_delete_transaction("recovery", "object", transaction)
            .await
            .unwrap();
        assert_eq!(retry.state, DeleteRecoveryState::NewPrepared);
        assert!(retry.rollback_data_complete);
        disk.settle_delete_transaction("recovery", "object", transaction, retry.fingerprint, DeleteRecoveryAction::Rollback)
            .await
            .expect("finish cleanup of an exactly restored versioned generation");
        assert_eq!(fs::read(root.join(STORAGE_FORMAT_FILE)).await.unwrap(), original);
        assert!(!rollback.exists());
    }

    #[tokio::test]
    async fn delete_recovery_absent_remaining_metadata_requires_existing_commit_receipt() {
        let (_temp, disk, info, original) = fixture().await;
        let mut remaining = info;
        remaining.version_id = Some(Uuid::new_v4());
        remaining.mod_time = remaining.mod_time.map(|time| time + time::Duration::seconds(1));
        let mut metadata = FileMeta::load(&original).unwrap();
        metadata.add_version(remaining).unwrap();
        let root = disk.io_get_object_path("recovery", "object").unwrap();
        fs::write(root.join(STORAGE_FORMAT_FILE), metadata.marshal_msg().unwrap())
            .await
            .unwrap();
        let transaction = Uuid::new_v4();
        let applied = apply(&disk, transaction).await;
        assert!(applied.journal.as_ref().unwrap().target_digest.is_some());
        let rollback = root.join(transaction.to_string());
        fs::remove_file(root.join(STORAGE_FORMAT_FILE)).await.unwrap();
        let undecided = disk
            .inspect_delete_transaction("recovery", "object", transaction)
            .await
            .unwrap();
        assert_eq!(undecided.state, DeleteRecoveryState::NewPrepared);
        assert!(
            !undecided.commit_data_safe,
            "absence cannot authorize a target-Some undecided transaction"
        );
        assert!(matches!(
            disk.settle_delete_transaction(
                "recovery",
                "object",
                transaction,
                undecided.fingerprint,
                DeleteRecoveryAction::PublishCommit
            )
            .await,
            Err(DiskError::OutdatedXLMeta)
        ));
        assert!(rollback.join(PREPARED).is_file());
        assert!(rollback.join(STORAGE_FORMAT_FILE_BACKUP).is_file());
        assert!(!rollback.join(DELETE_QUORUM_COMMITTED_FILE).exists());
        fs::copy(rollback.join(TARGET), root.join(STORAGE_FORMAT_FILE)).await.unwrap();
        let applied = disk
            .inspect_delete_transaction("recovery", "object", transaction)
            .await
            .unwrap();
        disk.settle_delete_transaction(
            "recovery",
            "object",
            transaction,
            applied.fingerprint,
            DeleteRecoveryAction::PublishCommit,
        )
        .await
        .unwrap();
        fs::remove_file(root.join(STORAGE_FORMAT_FILE)).await.unwrap();
        let certified = disk
            .inspect_delete_transaction("recovery", "object", transaction)
            .await
            .unwrap();
        assert_eq!(certified.state, DeleteRecoveryState::QuorumCommitted);
        assert!(certified.commit_data_safe);
        let finished = disk
            .settle_delete_transaction(
                "recovery",
                "object",
                transaction,
                certified.fingerprint,
                DeleteRecoveryAction::FinishCommit,
            )
            .await
            .expect("certified obsolete version cleanup has no live metadata references");
        assert_eq!(finished.state, DeleteRecoveryState::CommitFinished);
        assert!(
            !root.join(STORAGE_FORMAT_FILE).exists(),
            "cleanup must never republish the old remaining version"
        );
        assert!(
            rollback.join(DELETE_QUORUM_COMMITTED_FILE).is_file(),
            "set-wide GC still owns the final witness"
        );
    }

    #[tokio::test]
    async fn delete_recovery_partial_publication_adopts_same_intent_without_replacing_local_journal() {
        let (_temp, disk, mut original, _) = fixture().await;
        let (_peer_temp, peer, _, _) = fixture().await;
        let mut remaining = original.clone();
        remaining.version_id = Some(Uuid::new_v4());
        remaining.mod_time = remaining.mod_time.map(|time| time + time::Duration::seconds(1));
        for (index, target) in [(1, &disk), (2, &peer)] {
            original.erasure.index = index;
            remaining.erasure.index = index;
            let mut metadata = FileMeta::load(&test_meta(original.clone())).unwrap();
            metadata.add_version(remaining.clone()).unwrap();
            fs::write(
                target.io_get_object_path("recovery", "object/xl.meta").unwrap(),
                metadata.marshal_msg().unwrap(),
            )
            .await
            .unwrap();
        }
        let transaction = Uuid::new_v4();
        let local = apply(&disk, transaction).await;
        let root = disk.io_get_object_path("recovery", "object").unwrap();
        let rollback = root.join(transaction.to_string());
        let prepared = fs::read(rollback.join(PREPARED)).await.unwrap();
        let target = fs::read(rollback.join(TARGET)).await.unwrap();
        fs::remove_file(root.join(STORAGE_FORMAT_FILE)).await.unwrap();
        let applied = apply(&peer, transaction).await;
        let committed = peer
            .settle_delete_transaction(
                "recovery",
                "object",
                transaction,
                applied.fingerprint,
                DeleteRecoveryAction::PublishCommit,
            )
            .await
            .unwrap();
        let authority = committed.journal.unwrap();
        assert_ne!(
            local.journal.as_ref().unwrap().original_digest,
            authority.original_digest,
            "each drive owns distinct raw metadata"
        );
        let mut invalid = Vec::new();
        let mut changed = authority.clone();
        changed.bucket_incarnation = Some(Uuid::new_v4());
        invalid.push(changed);
        let mut changed = authority.clone();
        changed.deleted_versions.push(Uuid::new_v4());
        invalid.push(changed);
        let mut changed = authority.clone();
        changed.requests[0].version_id = Some(Uuid::new_v4());
        invalid.push(changed);
        let mut changed = authority.clone();
        changed
            .original_versions
            .iter_mut()
            .find(|version| version.version_id.unwrap_or_default().is_nil())
            .unwrap()
            .metadata_digest[0] ^= 1;
        invalid.push(changed);
        for authority in invalid {
            let observed = disk
                .inspect_delete_transaction("recovery", "object", transaction)
                .await
                .unwrap();
            assert!(matches!(
                disk.settle_delete_transaction(
                    "recovery",
                    "object",
                    transaction,
                    observed.fingerprint,
                    DeleteRecoveryAction::AdoptCommit {
                        authority: Box::new(authority)
                    }
                )
                .await,
                Err(DiskError::OutdatedXLMeta)
            ));
            assert!(!rollback.join(DELETE_QUORUM_COMMITTED_FILE).exists());
        }
        let observed = disk
            .inspect_delete_transaction("recovery", "object", transaction)
            .await
            .unwrap();
        assert!(!observed.commit_data_safe);
        let adopted = disk
            .settle_delete_transaction(
                "recovery",
                "object",
                transaction,
                observed.fingerprint,
                DeleteRecoveryAction::AdoptCommit {
                    authority: Box::new(authority),
                },
            )
            .await
            .expect("same-T certified intent completes partial publication");
        assert_eq!(
            adopted.journal, local.journal,
            "receipt preserves local hashes and data-directory ownership"
        );
        assert_eq!(fs::read(rollback.join(PREPARED)).await.unwrap(), prepared);
        assert_eq!(fs::read(rollback.join(TARGET)).await.unwrap(), target);
        disk.settle_delete_transaction(
            "recovery",
            "object",
            transaction,
            adopted.fingerprint,
            DeleteRecoveryAction::FinishCommit,
        )
        .await
        .unwrap();
        assert!(!root.join(STORAGE_FORMAT_FILE).exists());
        assert!(rollback.join(DELETE_QUORUM_COMMITTED_FILE).is_file());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn delete_recovery_rollback_marker_sync_failure_retains_journal() {
        for mode in [DurabilityMode::Strict, DurabilityMode::None] {
            let _mode = durability_mode_override::set(mode);
            let (_temp, disk, _info, original) = fixture().await;
            let transaction = Uuid::new_v4();
            apply(&disk, transaction).await;
            let root = disk.io_get_object_path("recovery", "object").unwrap();
            let data = root.join(Uuid::new_v4().to_string());
            fs::create_dir(&data).await.unwrap();
            let marker = data.join(format!("{RECOVERY_DELETE_DATA_DIR_MARKER_PREFIX}{transaction}"));
            fs::write(&marker, []).await.unwrap();
            let prepared = root.join(transaction.to_string()).join(PREPARED);
            let observed = disk
                .inspect_delete_transaction("recovery", "object", transaction)
                .await
                .unwrap();
            os::fsync_dir_recorder::set_failure(&data, ErrorKind::Other);
            let result = disk
                .settle_delete_transaction(
                    "recovery",
                    "object",
                    transaction,
                    observed.fingerprint,
                    DeleteRecoveryAction::Rollback,
                )
                .await;
            assert_eq!(fs::read(root.join(STORAGE_FORMAT_FILE)).await.unwrap(), original);
            assert!(!marker.exists(), "the unlink precedes the durability barrier");
            if mode == DurabilityMode::Strict {
                assert!(result.is_err(), "a failed marker-directory sync must stop proof removal");
                assert!(prepared.exists(), "rollback evidence survives the sync failure");
                let retry = disk
                    .inspect_delete_transaction("recovery", "object", transaction)
                    .await
                    .unwrap();
                disk.settle_delete_transaction(
                    "recovery",
                    "object",
                    transaction,
                    retry.fingerprint,
                    DeleteRecoveryAction::Rollback,
                )
                .await
                .expect("retry syncs the directory even though its marker is already absent");
                assert!(os::fsync_dir_recorder::was_fsynced(&data));
            } else {
                result.expect("none mode does not add a marker durability barrier");
                assert!(!os::fsync_dir_recorder::was_fsynced(&data));
                assert!(os::fsync_dir_recorder::take_failure(&data).is_some());
            }
            assert!(!prepared.exists());
        }
    }

    #[tokio::test]
    async fn delete_recovery_validates_staged_parts_before_restoring_metadata() {
        use crate::erasure::coding::BitrotWriter;
        let (_temp, disk, _info, _) = fixture().await;
        let data_dir = Uuid::new_v4();
        let payload = Bytes::from_static(b"complete staged external payload");
        let mut info = test_file_info("object", Uuid::nil(), Some(data_dir), None);
        info.size = i64::try_from(payload.len()).expect("payload length");
        info.erasure.block_size = payload.len();
        info.parts[0].size = payload.len();
        info.parts[0].actual_size = info.size;
        let mut second = info.parts[0].clone();
        second.number = 2;
        info.parts.push(second);
        info.size *= 2;
        let original = test_meta(info.clone());
        let root = disk.io_get_object_path("recovery", "object").expect("object root");
        fs::write(root.join(STORAGE_FORMAT_FILE), &original)
            .await
            .expect("external original metadata");
        let mut writer = BitrotWriter::new(
            std::io::Cursor::new(Vec::new()),
            info.erasure.shard_size(),
            HashAlgorithm::HighwayHash256S,
        );
        assert_eq!(writer.write(&payload).await.expect("encode intact external shard"), payload.len());
        writer.shutdown().await.expect("flush shard encoding");
        let encoded = writer.into_inner().into_inner();
        fs::create_dir_all(root.join(data_dir.to_string()))
            .await
            .expect("external data directory");
        fs::write(root.join(data_dir.to_string()).join("part.1"), &encoded)
            .await
            .expect("intact external part");
        fs::write(root.join(data_dir.to_string()).join("part.2"), &encoded)
            .await
            .unwrap();
        let transaction = Uuid::new_v4();
        let _ = disk
            .prepare_delete_recovery(
                "recovery",
                "object",
                Some(&original),
                &[request()],
                None,
                DeletePreparationContext {
                    transaction,
                    bucket_incarnation: None,
                    namespace_owner: None,
                },
            )
            .await
            .expect("prepared intent");
        fs::rename(
            root.join(data_dir.to_string()),
            root.join(transaction.to_string()).join(data_dir.to_string()),
        )
        .await
        .expect("simulate staged legacy-compatible data");
        fs::create_dir(root.join(data_dir.to_string())).await.unwrap();
        fs::rename(
            root.join(transaction.to_string()).join(data_dir.to_string()).join("part.1"),
            root.join(data_dir.to_string()).join("part.1"),
        )
        .await
        .unwrap();
        fs::remove_file(root.join(STORAGE_FORMAT_FILE))
            .await
            .expect("simulate applied metadata removal");
        let observed = disk
            .inspect_delete_transaction("recovery", "object", transaction)
            .await
            .expect("inspect complete staged data");
        assert!(observed.rollback_data_complete);
        disk.settle_delete_transaction("recovery", "object", transaction, observed.fingerprint, DeleteRecoveryAction::Rollback)
            .await
            .expect("verified staged rollback");
        assert_eq!(
            disk.read_all("recovery", "object/xl.meta")
                .await
                .expect("restored metadata")
                .as_ref(),
            original
        );
        assert_eq!(
            disk.read_all("recovery", &format!("object/{data_dir}/part.1"))
                .await
                .expect("restored shard")
                .as_ref(),
            encoded
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn delete_recovery_raw_discovery_rejects_symlink_and_forged_cursor() {
        use std::os::unix::fs::symlink;
        let (temp, disk, _info, _) = fixture().await;
        let outside = tempfile::tempdir().expect("outside sentinel");
        fs::write(outside.path().join("sentinel"), b"outside bytes")
            .await
            .expect("outside sentinel");
        symlink(outside.path(), temp.path().join("recovery").join("untrusted-prefix")).expect("untrusted symlink");
        assert!(disk.discover_delete_transactions("recovery", None, None, 16).await.is_err());
        assert!(
            disk.discover_delete_transactions("recovery", None, Some(DeleteRecoveryCursor { token: Uuid::new_v4() }), 16)
                .await
                .is_err()
        );
        assert_eq!(
            fs::read(outside.path().join("sentinel"))
                .await
                .expect("outside remains intact"),
            b"outside bytes"
        );
    }
}
