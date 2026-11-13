use super::*;
use crate::disk::{
    DELETE_RECOVERY_PROTOCOL_VERSION, DeleteRecoveryAction, DeleteRecoveryCursor, DeleteRecoveryDigest, DeleteRecoveryInspection,
    DeleteRecoveryJournal, DeleteRecoveryPage, DeleteRecoveryState, DeleteTransactionId,
};
use sha2::{Digest, Sha256};

pub(super) struct RecoveryOperationScope {
    pub(super) budget: Arc<crate::object_api::DeleteRecoveryWorkBudget>,
    may_publish_receipt: bool,
    receipt_published: std::sync::atomic::AtomicBool,
}

tokio::task_local! {
    static RECOVERY_OPERATION: Arc<RecoveryOperationScope>;
}

pub(super) fn recovery_operation_scope() -> Option<Arc<RecoveryOperationScope>> {
    RECOVERY_OPERATION.try_with(Arc::clone).ok()
}

impl RecoveryOperationScope {
    pub(super) fn receipt_published(&self) {
        if self.may_publish_receipt {
            self.receipt_published.store(true, std::sync::atomic::Ordering::Release);
        }
    }
}

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

/// Names an object-scoped scan skips without a stat (plain children of the
/// key's directory, and entries of a UUID child that cannot be delete evidence)
/// cost one budget entry per this many names: enough to bound a hostile prefix
/// without making a real folder prefix undeletable.
const PLAIN_CHILDREN_PER_BUDGET_ENTRY: u32 = 64;

/// Entries one step of the resumable (background) discovery may visit. The
/// foreground bounded protocol has its own, larger per-call allowance.
const DISCOVERY_STEP_ENTRIES: u32 = 4096;

/// Wall-clock allowance of one bounded discovery call: a third of the drive
/// timeout that wraps the call (including its wait for a lane worker), capped at
/// ten seconds, so an incomplete walk returns before the call can time out.
fn bounded_discovery_time_limit() -> std::time::Duration {
    (crate::disk::disk_store::get_max_timeout_duration() / 3).min(std::time::Duration::from_secs(10))
}

/// Whether an entry inside a UUID child of an object-scoped root can name one
/// of the key's delete transactions: a journal file of a rollback directory or
/// a data-directory marker. Every other name is inert to discovery.
fn may_be_delete_evidence(name: &str) -> bool {
    [PREPARED, STORAGE_FORMAT_FILE_BACKUP, DELETE_QUORUM_COMMITTED_FILE].contains(&name)
        || [
            DELETE_DATA_DIR_MARKER_PREFIX,
            RESERVED_DELETE_DATA_DIR_MARKER_PREFIX,
            RECOVERY_DELETE_DATA_DIR_MARKER_PREFIX,
        ]
        .iter()
        .any(|prefix| delete_transaction_marker_id(name, prefix).is_some())
}

impl DiscoverySession {
    /// Consume one bounded step, returning ownership only when traversal remains.
    fn advance(self, limit: u32) -> std::io::Result<(Vec<DeleteTransactionId>, Option<Self>)> {
        self.advance_bounded(limit, DISCOVERY_STEP_ENTRIES, &|| false)
            .map(|(transactions, next, _)| (transactions, next))
    }

    /// Walk until `limit` transactions, `max_entries` charged entries, the end
    /// of the scope, or `should_stop` (checked before every entry); any early
    /// stop returns the remaining traversal as continuation state.
    fn advance_bounded(
        mut self,
        limit: u32,
        max_entries: u32,
        should_stop: &dyn Fn() -> bool,
    ) -> std::io::Result<(Vec<DeleteTransactionId>, Option<Self>, u32)> {
        if self.frames.is_empty() {
            match std::fs::read_dir(&self.root) {
                Ok(entries) => self.frames.push((String::new(), entries)),
                Err(err) if err.kind() == ErrorKind::NotFound => {
                    return Ok((Vec::new(), None, 0));
                }
                Err(err) => return Err(err),
            }
        }
        let mut transactions = BTreeSet::new();
        let mut seen_candidates = HashSet::new();
        let mut visited = 0u32;
        let mut skipped_by_name = 0u32;
        while !self.frames.is_empty()
            && transactions.len() < usize::try_from(limit).map_err(|_| std::io::Error::from(ErrorKind::InvalidInput))?
            && visited < max_entries
            && !should_stop()
        {
            let (parent, entries) = self
                .frames
                .last_mut()
                .ok_or_else(|| std::io::Error::from(ErrorKind::InvalidData))?;
            let entry = match entries.next() {
                Some(entry) => entry?,
                None => {
                    self.frames.pop();
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
            // An object-scoped scan only ever descends into transaction (UUID)
            // directories at the root, and inside them only journal files and
            // data-directory markers can be evidence. Every other name (plain
            // child keys of a prefix directory, and the `xl.meta`, data
            // directories or children of a UUID-named child key) is skipped by
            // name without a stat and charged at one entry per readdir batch, so
            // the work stays bounded while deleting a missing key that names a
            // folder prefix with many children, UUID-named or not, can complete.
            if self.object.is_some()
                && if parent.is_empty() {
                    !canonical_uuid(&name).is_some_and(|id| !id.is_nil())
                } else {
                    !may_be_delete_evidence(&name)
                }
            {
                skipped_by_name += 1;
                if skipped_by_name.is_multiple_of(PLAIN_CHILDREN_PER_BUDGET_ENTRY) {
                    visited += 1;
                }
                continue;
            }
            visited += 1;
            let relative = if parent.is_empty() {
                name.clone()
            } else {
                format!("{parent}/{name}")
            };
            let path = self.root.join(&relative);
            reject_local_disk_symlink_components(&self.root, &path).map_err(std::io::Error::from)?;
            let kind = match std::fs::symlink_metadata(&path) {
                Ok(metadata) => metadata,
                Err(err) if err.kind() == ErrorKind::NotFound => continue,
                Err(err) => return Err(err),
            };
            if !kind.is_dir() && !kind.is_file() {
                return Err(std::io::Error::from(ErrorKind::InvalidData));
            }
            if kind.is_file() {
                let bound = match self.object.as_deref() {
                    Some(object) if !parent.is_empty() && !parent.contains('/') => Some((object, parent.as_str())),
                    Some(_) => None,
                    None => parent.rsplit_once('/'),
                };
                if let Some((object, id)) = bound {
                    let transaction =
                        if [PREPARED, STORAGE_FORMAT_FILE_BACKUP, DELETE_QUORUM_COMMITTED_FILE].contains(&name.as_str()) {
                            canonical_uuid(id).filter(|id| !id.is_nil())
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
                        if !seen_candidates.insert((object.to_owned(), transaction)) {
                            continue;
                        }
                        let physical = match self.object.as_ref() {
                            Some(_) => self.root.clone(),
                            None => self.root.join(object),
                        };
                        if name == STORAGE_FORMAT_FILE_BACKUP
                            && live_write_backup(&physical, transaction).map_err(std::io::Error::from)?
                        {
                            continue;
                        }
                        let proof_root = physical.join(transaction.to_string());
                        if self.object.is_some() {
                            regular_file_exists(&proof_root.join(PREPARED)).map_err(std::io::Error::from)?;
                            regular_file_exists(&proof_root.join(DELETE_QUORUM_COMMITTED_FILE)).map_err(std::io::Error::from)?;
                            transactions.insert((object.to_owned(), transaction));
                            continue;
                        }
                        let prepared = regular_bytes(&proof_root.join(PREPARED)).map_err(std::io::Error::from)?;
                        // Only a fully bound journal can name a logical
                        // key. Other records remain discoverable by their
                        // physical key for canonical inspection to reject.
                        let record = match prepared {
                            Some(bytes) => serde_json::from_slice::<DeleteRecoveryJournal>(&bytes).ok(),
                            None => regular_bytes(&proof_root.join(DELETE_QUORUM_COMMITTED_FILE))
                                .map_err(std::io::Error::from)?
                                .filter(|bytes| !bytes.is_empty())
                                .and_then(|bytes| serde_json::from_slice::<CommitReceipt>(&bytes).ok())
                                .map(|receipt| receipt.journal),
                        };
                        let logical = record
                            .filter(|journal| {
                                journal.protocol_version == DELETE_RECOVERY_PROTOCOL_VERSION
                                    && journal.volume == self.volume
                                    && journal.transaction == transaction
                                    && safe_path(&journal.object, false)
                                    && match self.object.as_ref() {
                                        Some(scope) => scope == &journal.object,
                                        None => build_local_disk_object_path(&self.root, "", &journal.object).1 == physical,
                                    }
                            })
                            .map_or_else(|| object.to_owned(), |journal| journal.object);
                        transactions.insert((logical, transaction));
                    }
                }
            } else {
                if self.object.is_some() && (!parent.is_empty() || !canonical_uuid(&name).is_some_and(|id| !id.is_nil())) {
                    continue;
                }
                // A valid 1024-byte key can exceed 256 one-byte path
                // components; allow its transaction/data directories too.
                if self.frames.len() >= 1024 {
                    return Err(std::io::Error::from(ErrorKind::InvalidData));
                }
                self.frames.push((relative, std::fs::read_dir(path)?));
            }
        }

        let transactions = transactions
            .into_iter()
            .map(|(object, transaction)| DeleteTransactionId { object, transaction })
            .collect();
        let next = (!self.frames.is_empty()).then_some(self);
        Ok((transactions, next, visited))
    }

    async fn has_evidence(mut self, disk: &LocalDisk, path: &Path) -> Result<bool> {
        loop {
            let (transactions, next) = disk.advance_delete_discovery(path, self, 1).await?;
            if !transactions.is_empty() {
                return Ok(true);
            }
            let Some(mut next) = next else {
                return Ok(false);
            };
            next.touched = Instant::now();
            self = next;
        }
    }
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
        if canonical.metadata.is_empty() {
            break;
        }
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
    fn sort_json_in_place(value: &mut serde_json::Value) {
        match value {
            serde_json::Value::Object(map) => {
                map.sort_keys();
                map.values_mut().for_each(sort_json_in_place);
            }
            serde_json::Value::Array(values) => values.iter_mut().for_each(sort_json_in_place),
            _ => {}
        }
    }
    struct Sha256Writer(Sha256);
    impl std::io::Write for Sha256Writer {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.update(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut json_value = serde_json::to_value(&canonical)?;
    sort_json_in_place(&mut json_value);
    let mut hasher = Sha256Writer(Sha256::new());
    serde_json::to_writer(&mut hasher, &json_value)?;
    Ok(crate::disk::DeleteRecoveryVersionIdentity {
        version_id: canonical.version_id,
        mod_time: canonical.mod_time,
        data_dir: canonical.data_dir,
        deleted: canonical.deleted,
        metadata_digest: hasher.0.finalize().into(),
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

fn regular_file_exists(path: &Path) -> Result<bool> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.is_file() && meta.len() <= MAX_METADATA => Ok(true),
        Ok(_) => Err(DiskError::FileCorrupt),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(false),
        Err(err) => Err(to_file_error(err).into()),
    }
}

fn regular_bytes(path: &Path) -> Result<Option<Vec<u8>>> {
    regular_file_exists(path)?
        .then(|| std::fs::read(path).map_err(|err| DiskError::from(to_file_error(err))))
        .transpose()
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
        if regular_file_exists(&rollback.join(name))? {
            return Ok(false);
        }
    }
    for prefix in [
        DELETE_DATA_DIR_MARKER_PREFIX,
        RESERVED_DELETE_DATA_DIR_MARKER_PREFIX,
        RECOVERY_DELETE_DATA_DIR_MARKER_PREFIX,
    ] {
        let name = format!("{prefix}{transaction}");
        if regular_file_exists(&root.join(&name))? || regular_file_exists(&rollback.join(name))? {
            return Ok(false);
        }
    }
    let Some(current) = regular_metadata_bytes(&root.join(STORAGE_FORMAT_FILE))? else {
        return Ok(false);
    };
    let Some(backup) = regular_metadata_bytes(&rollback.join(STORAGE_FORMAT_FILE_BACKUP))? else {
        return Ok(false);
    };
    // Corrupt decoded metadata cannot prove that this is a live write backup.
    // Keep it discoverable; filesystem and structural errors above still fail.
    match (|| live_write_backup_matches(&FileMeta::load(&current)?, &FileMeta::load(&backup)?, transaction))() {
        Err(DiskError::FileCorrupt) => Ok(false),
        result => result,
    }
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
    // MessagePack encoding of `DeleteRecoveryRpcReply::Inspected` is more compact
    // than JSON; only construct and serialize `projected` when the JSON size plus
    // `current_versions` and fixed envelope could approach the RPC payload limit.
    let rpc_budget = rustfs_protos::DELETE_RECOVERY_MAX_PAYLOAD_BYTES.saturating_sub(4096);
    if bytes.len().saturating_mul(2).saturating_add(4096) <= rpc_budget {
        return Ok(bytes);
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
    if rmp_serde::to_vec_named(&projected)?.len() > rpc_budget {
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

fn explicit_delete_referenced_data_dirs(
    original: Option<&FileMeta>,
    journal: Option<&DeleteRecoveryJournal>,
    current_missing: bool,
) -> Result<Option<HashSet<Uuid>>> {
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
    Ok(Some(referenced))
}

pub(super) async fn next_recovery_entry(entries: &mut tokio::fs::ReadDir) -> std::io::Result<Option<tokio::fs::DirEntry>> {
    if let Some(scope) = recovery_operation_scope() {
        scope.budget.reserve(1).map_err(std::io::Error::from)?;
    }
    entries.next_entry().await
}

pub(super) fn recovery_entries(
    mut entries: std::fs::ReadDir,
    budget: Option<&crate::object_api::DeleteRecoveryWorkBudget>,
) -> impl Iterator<Item = std::io::Result<std::fs::DirEntry>> + '_ {
    let mut finished = false;
    std::iter::from_fn(move || {
        if finished {
            return None;
        }
        if let Some(budget) = budget
            && let Err(error) = budget.reserve(1)
        {
            finished = true;
            return Some(Err(error.into()));
        }
        let entry = entries.next();
        finished = entry.is_none();
        entry
    })
}

fn explicit_delete_plan_bounded(
    root: &Path,
    transaction: Uuid,
    original: Option<&FileMeta>,
    journal: Option<&DeleteRecoveryJournal>,
    current_missing: bool,
    budget: Option<&crate::object_api::DeleteRecoveryWorkBudget>,
) -> Result<Option<ExplicitDeletePlan>> {
    let Some(referenced) = explicit_delete_referenced_data_dirs(original, journal, current_missing)? else {
        return Ok(None);
    };
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
            for entry in recovery_entries(entries, budget) {
                let entry = entry.map_err(to_file_error)?;
                let kind = entry.file_type().map_err(to_file_error)?;
                let name = entry.file_name().into_string().map_err(|_| DiskError::FileCorrupt)?;
                if kind.is_dir() && canonical_uuid(&name).is_some_and(|id| referenced.contains(&id)) {
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
            for entry in recovery_entries(entries, budget) {
                let entry = entry.map_err(to_file_error)?;
                let name = entry.file_name().into_string().map_err(|_| DiskError::FileCorrupt)?;
                let Some(id) = canonical_uuid(&name).filter(|id| !id.is_nil()) else {
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
        for entry in recovery_entries(std::fs::read_dir(&dir).map_err(to_file_error)?, budget) {
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

/// Protocol directories always use the canonical hyphenated spelling;
/// `Uuid::parse_str` also accepts simple, braced and URN forms, which can only
/// name independent child keys here.
fn canonical_uuid(name: &str) -> Option<Uuid> {
    Uuid::parse_str(name).ok().filter(|id| name == id.hyphenated().to_string())
}

pub(super) fn missing_marker_preserves_child_prefix(journal: &DeleteRecoveryJournal) -> bool {
    journal.original_digest.is_none()
        && journal.original_versions.is_empty()
        && journal.data_dirs.is_empty()
        && journal.target_digest.is_some()
        && !journal.requests.is_empty()
        && journal
            .requests
            .iter()
            .all(|request| request.deleted && request.version_id.is_some_and(|id| !id.is_nil()))
}

pub(super) fn inspection_with_markers_bounded(
    root: &Path,
    volume: &str,
    object: &str,
    transaction: Uuid,
    budget: Option<&crate::object_api::DeleteRecoveryWorkBudget>,
) -> Result<(DeleteRecoveryInspection, bool)> {
    let rollback = root.join(transaction.to_string());
    let before = regular_metadata_bytes(&rollback.join(STORAGE_FORMAT_FILE_BACKUP))?;
    let prepared = regular_bytes(&rollback.join(PREPARED))?;
    let target = regular_metadata_bytes(&rollback.join(TARGET))?;
    let receipt_bytes = regular_bytes(&rollback.join(DELETE_QUORUM_COMMITTED_FILE))?;
    let receipt: Option<CommitReceipt> = receipt_bytes
        .as_deref()
        .filter(|bytes| !bytes.is_empty())
        .map(serde_json::from_slice)
        .transpose()
        .map_err(|_| DiskError::FileCorrupt)?;
    let committed = receipt.is_some();
    let finished = regular_marker(&rollback.join(FINISHED))? || receipt.as_ref().is_some_and(|receipt| receipt.finished);
    let journal: Option<DeleteRecoveryJournal> = prepared
        .as_deref()
        .map(serde_json::from_slice)
        .transpose()
        .map_err(|_| DiskError::FileCorrupt)?
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
    // A failed creation of a marker over a child named `xl.meta` did not
    // replace that directory. Older releases nevertheless left a journal.
    // Only this bound, originally absent generation may treat it as absence.
    let current_path = root.join(STORAGE_FORMAT_FILE);
    let current = if !committed
        && before.is_none()
        && (journal.as_ref().is_some_and(missing_marker_preserves_child_prefix)
            || (journal.is_none() && target.is_none() && receipt_bytes.is_none()))
        && std::fs::symlink_metadata(&current_path).is_ok_and(|metadata| metadata.is_dir())
    {
        None
    } else {
        regular_metadata_bytes(&current_path)?
    };
    let backup_digest = before.as_deref().map(digest);
    let current_digest = current.as_deref().map(digest);
    let prepared_digest = prepared.as_deref().map(digest);
    let target_digest = target.as_deref().map(digest);
    let receipt_digest = receipt_bytes.as_deref().map(digest);
    let finished = finished
        || (committed
            && prepared.is_none()
            && before.is_none()
            && target.is_none()
            && current_digest == journal.as_ref().and_then(|j| j.target_digest));
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
    let mut has_other_uuid_dir = false;
    let mut has_staged_dir = false;
    let mut descriptions = Vec::new();
    let entries = match std::fs::read_dir(root) {
        Ok(entries) => Some(entries),
        Err(err) if err.kind() == ErrorKind::NotFound => None,
        Err(err) => return Err(to_file_error(err).into()),
    };
    if let Some(entries) = entries {
        for entry in recovery_entries(entries, budget) {
            let entry = entry.map_err(to_file_error)?;
            let kind = entry.file_type().map_err(to_file_error)?;
            if !kind.is_file() && !kind.is_dir() {
                return Err(DiskError::InvalidPath);
            }
            let name = entry.file_name().into_string().map_err(|_| DiskError::FileCorrupt)?;
            let id = canonical_uuid(&name);
            if kind.is_dir() && id.is_none() {
                // A non-UUID directory is only a prefix of independent child
                // keys, which settle under their own exact-key locks and may
                // vanish or appear concurrently. Settlement never touches it.
                continue;
            }
            descriptions.push((name.clone(), kind.is_dir(), entry.metadata().map_err(to_file_error)?.len()));
            if kind.is_dir() && id.is_some_and(|id| !id.is_nil()) {
                has_other_uuid_dir |= id != Some(transaction);
                for child in recovery_entries(std::fs::read_dir(entry.path()).map_err(to_file_error)?, budget) {
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
                    let staged_id = canonical_uuid(&child_name);
                    let known_staged = kind.is_dir()
                        && staged_id.is_some_and(|dir| {
                            journal.as_ref().is_some_and(|journal| journal.data_dirs.contains(&dir))
                                || before_meta
                                    .as_ref()
                                    .is_some_and(|metadata| metadata.get_data_dirs().is_ok_and(|dirs| dirs.contains(&Some(dir))))
                        });
                    if name == transaction.to_string()
                        && !(kind.is_dir() && known_staged)
                        && !(journal.is_none() && kind.is_file() && is_metadata_less_data_dir_entry(&child_name))
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
                        has_staged_dir = true;
                        for file in recovery_entries(std::fs::read_dir(child.path()).map_err(to_file_error)?, budget) {
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
    let has_physical_data_dirs_or_markers = any_marker || has_other_uuid_dir || has_staged_dir;
    let explicit_delete_safe = match state {
        DeleteRecoveryState::NewPrepared if !has_physical_data_dirs_or_markers => {
            explicit_delete_referenced_data_dirs(before_meta.as_ref(), journal.as_ref(), current.is_none())?
                .is_some_and(|referenced| !referenced.contains(&transaction))
        }
        DeleteRecoveryState::LegacyAmbiguous | DeleteRecoveryState::NewPrepared => {
            explicit_delete_plan_bounded(root, transaction, before_meta.as_ref(), journal.as_ref(), current.is_none(), budget)?
                .is_some()
        }
        _ => false,
    };
    let inspection = DeleteRecoveryInspection {
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
    };
    Ok((inspection, has_physical_data_dirs_or_markers))
}

#[derive(Clone)]
struct PreparedDeleteReceiptEntry {
    object_path: PathBuf,
    bucket_path: PathBuf,
    file_path: PathBuf,
    staging_root: PathBuf,
    journal_raw: Vec<u8>,
    journal: DeleteRecoveryJournal,
    sole_transaction: bool,
    had_old_data_dir: bool,
    rollback_dir_fd: Option<Arc<std::fs::File>>,
    file_path_dir: Option<Arc<std::fs::File>>,
}

#[derive(Clone)]
struct PublishedDeleteReceiptEntry {
    object_path: PathBuf,
    bucket_path: PathBuf,
    file_path: PathBuf,
    staging_root: PathBuf,
    journal_raw: Vec<u8>,
    had_old_data_dir: bool,
    rollback_dir_fd: Option<Arc<std::fs::File>>,
    file_path_dir: Option<Arc<std::fs::File>>,
}

#[cfg(unix)]
fn read_child_regular_bytes(
    parent_fd: Option<&std::fs::File>,
    child_name: &str,
    fallback_path: &Path,
) -> std::io::Result<Vec<u8>> {
    if let Some(dir) = parent_fd {
        use rustix::fs::{Mode, OFlags, openat};
        let fd = openat(dir, child_name, OFlags::RDONLY | OFlags::CLOEXEC, Mode::empty()).map_err(std::io::Error::from)?;
        let mut file = std::fs::File::from(fd);
        let cap = file
            .metadata()
            .ok()
            .and_then(|m| usize::try_from(m.len()).ok())
            .unwrap_or(0)
            .min(64 * 1024);
        let mut buf = Vec::with_capacity(cap);
        std::io::Read::read_to_end(&mut file, &mut buf)?;
        return Ok(buf);
    }
    std::fs::read(fallback_path)
}

#[cfg(not(unix))]
fn read_child_regular_bytes(
    _parent_fd: Option<&std::fs::File>,
    _child_name: &str,
    fallback_path: &Path,
) -> std::io::Result<Vec<u8>> {
    std::fs::read(fallback_path)
}

#[cfg(unix)]
fn create_and_write_child_file(
    parent_fd: Option<&std::fs::File>,
    child_name: &str,
    fallback_path: &Path,
    bytes: &[u8],
) -> std::io::Result<std::fs::File> {
    let mut file = if let Some(dir) = parent_fd {
        use rustix::fs::{Mode, OFlags, openat};
        let fd = openat(
            dir,
            child_name,
            OFlags::WRONLY | OFlags::CREATE | OFlags::TRUNC | OFlags::CLOEXEC,
            Mode::from_raw_mode(0o666),
        )
        .map_err(std::io::Error::from)?;
        std::fs::File::from(fd)
    } else {
        std::fs::File::create(fallback_path)?
    };
    std::io::Write::write_all(&mut file, bytes)?;
    Ok(file)
}

#[cfg(not(unix))]
fn create_and_write_child_file(
    _parent_fd: Option<&std::fs::File>,
    _child_name: &str,
    fallback_path: &Path,
    bytes: &[u8],
) -> std::io::Result<std::fs::File> {
    let mut file = std::fs::File::create(fallback_path)?;
    std::io::Write::write_all(&mut file, bytes)?;
    Ok(file)
}

#[cfg(unix)]
fn link_child_files(
    src_dir_fd: Option<&std::fs::File>,
    src_name: &str,
    src_fallback: &Path,
    dst_dir_fd: Option<&std::fs::File>,
    dst_name: &str,
    dst_fallback: &Path,
) -> std::io::Result<()> {
    if let (Some(s_fd), Some(d_fd)) = (src_dir_fd, dst_dir_fd) {
        return rustix::fs::linkat(s_fd, src_name, d_fd, dst_name, rustix::fs::AtFlags::empty()).map_err(std::io::Error::from);
    }
    std::fs::hard_link(src_fallback, dst_fallback)
}

#[cfg(not(unix))]
fn link_child_files(
    _src_dir_fd: Option<&std::fs::File>,
    _src_name: &str,
    src_fallback: &Path,
    _dst_dir_fd: Option<&std::fs::File>,
    _dst_name: &str,
    dst_fallback: &Path,
) -> std::io::Result<()> {
    std::fs::hard_link(src_fallback, dst_fallback)
}

#[cfg(unix)]
fn link_from_dir_fd(
    src_dir_fd: Option<&std::fs::File>,
    src_name: &str,
    src_fallback: &Path,
    dst_path: &Path,
) -> std::io::Result<()> {
    if let Some(s_fd) = src_dir_fd {
        return rustix::fs::linkat(s_fd, src_name, rustix::fs::CWD, dst_path, rustix::fs::AtFlags::empty())
            .map_err(std::io::Error::from);
    }
    std::fs::hard_link(src_fallback, dst_path)
}

#[cfg(not(unix))]
fn link_from_dir_fd(
    _src_dir_fd: Option<&std::fs::File>,
    _src_name: &str,
    src_fallback: &Path,
    dst_path: &Path,
) -> std::io::Result<()> {
    std::fs::hard_link(src_fallback, dst_path)
}

#[cfg(unix)]
fn rename_into_dir_fd(
    src_path: &Path,
    dst_dir_fd: Option<&std::fs::File>,
    dst_name: &str,
    dst_fallback: &Path,
) -> std::io::Result<()> {
    if let Some(d_fd) = dst_dir_fd {
        return rustix::fs::renameat(rustix::fs::CWD, src_path, d_fd, dst_name).map_err(std::io::Error::from);
    }
    std::fs::rename(src_path, dst_fallback)
}

#[cfg(not(unix))]
fn rename_into_dir_fd(
    src_path: &Path,
    _dst_dir_fd: Option<&std::fs::File>,
    _dst_name: &str,
    dst_fallback: &Path,
) -> std::io::Result<()> {
    std::fs::rename(src_path, dst_fallback)
}

#[cfg(unix)]
fn child_regular_file_exists(dir_fd: Option<&std::fs::File>, child_name: &str, fallback_path: &Path) -> bool {
    if let Some(d_fd) = dir_fd {
        return rustix::fs::statat(d_fd, child_name, rustix::fs::AtFlags::SYMLINK_NOFOLLOW)
            .is_ok_and(|st| (st.st_mode & libc::S_IFMT) == libc::S_IFREG);
    }
    fallback_path.is_file()
}

#[cfg(not(unix))]
fn child_regular_file_exists(_dir_fd: Option<&std::fs::File>, _child_name: &str, fallback_path: &Path) -> bool {
    fallback_path.is_file()
}

#[cfg(unix)]
fn unlink_child_file(dir_fd: Option<&std::fs::File>, child_name: &str, fallback_path: &Path) -> std::io::Result<()> {
    if let Some(d_fd) = dir_fd {
        return rustix::fs::unlinkat(d_fd, child_name, rustix::fs::AtFlags::empty()).map_err(std::io::Error::from);
    }
    std::fs::remove_file(fallback_path)
}

#[cfg(not(unix))]
fn unlink_child_file(_dir_fd: Option<&std::fs::File>, _child_name: &str, fallback_path: &Path) -> std::io::Result<()> {
    std::fs::remove_file(fallback_path)
}

#[cfg(unix)]
fn rmdir_child_dir(dir_fd: Option<&std::fs::File>, child_name: &str, fallback_path: &Path) -> std::io::Result<()> {
    if let Some(d_fd) = dir_fd {
        return rustix::fs::unlinkat(d_fd, child_name, rustix::fs::AtFlags::REMOVEDIR).map_err(std::io::Error::from);
    }
    std::fs::remove_dir(fallback_path)
}

#[cfg(not(unix))]
fn rmdir_child_dir(_dir_fd: Option<&std::fs::File>, _child_name: &str, fallback_path: &Path) -> std::io::Result<()> {
    std::fs::remove_dir(fallback_path)
}

const PREPARED_DELETE_RECEIPTS_PER_SHARD: usize = 64;

static PREPARED_DELETE_RECEIPTS: std::sync::LazyLock<os::ShardedPathCache<PreparedDeleteReceiptEntry>> =
    std::sync::LazyLock::new(|| os::ShardedPathCache::new(PREPARED_DELETE_RECEIPTS_PER_SHARD));

static PUBLISHED_DELETE_RECEIPTS: std::sync::LazyLock<os::ShardedPathCache<PublishedDeleteReceiptEntry>> =
    std::sync::LazyLock::new(|| os::ShardedPathCache::new(PREPARED_DELETE_RECEIPTS_PER_SHARD));

fn put_prepared_delete_receipt(rollback: impl AsRef<Path>, entry: PreparedDeleteReceiptEntry) {
    PREPARED_DELETE_RECEIPTS.insert(rollback.as_ref(), entry);
}

fn take_prepared_delete_receipt(rollback: &Path) -> Option<PreparedDeleteReceiptEntry> {
    PREPARED_DELETE_RECEIPTS.remove(rollback)
}

fn put_published_delete_receipt(rollback: impl AsRef<Path>, entry: PublishedDeleteReceiptEntry) {
    PUBLISHED_DELETE_RECEIPTS.insert(rollback.as_ref(), entry);
}

fn take_published_delete_receipt(rollback: &Path) -> Option<PublishedDeleteReceiptEntry> {
    PUBLISHED_DELETE_RECEIPTS.remove(rollback)
}

pub(super) fn clear_delete_receipts_under(io_root: &Path, root: &Path) {
    for prefix in [io_root, root] {
        PREPARED_DELETE_RECEIPTS.retain_outside_prefix(prefix);
        PUBLISHED_DELETE_RECEIPTS.retain_outside_prefix(prefix);
    }
}

#[cfg(unix)]
fn open_dir_fd(path: &Path) -> std::io::Result<std::fs::File> {
    use rustix::fs::{Mode, OFlags, open};
    open(path, OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC, Mode::empty())
        .map(std::fs::File::from)
        .map_err(std::io::Error::from)
}

fn format_commit_receipt_bytes(journal_raw: &[u8], finished: bool) -> Vec<u8> {
    let suffix: &[u8] = if finished {
        b",\"finished\":true}"
    } else {
        b",\"finished\":false}"
    };
    let mut out = Vec::with_capacity(11 + journal_raw.len() + suffix.len());
    out.extend_from_slice(b"{\"journal\":");
    out.extend_from_slice(journal_raw);
    out.extend_from_slice(suffix);
    out
}

impl LocalDisk {
    // Native MPU source closure has its own conditional protocol. Generic
    // object recovery must never restore its head or acquire SourceRW under
    // an already-owned object metadata lease.
    fn reject_multipart_delete_recovery_mutation(&self, volume: &str, object: &str) -> Result<()> {
        multipart_staging::reject_noncanonical_scope(&self.root, self.io_root(), volume, object)?;
        if volume == RUSTFS_META_MULTIPART_BUCKET {
            return Err(DiskError::ConditionalDeleteConflict);
        }
        Ok(())
    }

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
        let recovery_scope = recovery_operation_scope();
        os::run_blocking_namespace_operation(lease, move || {
            Ok((|| {
                let entries = match std::fs::read_dir(&root) {
                    Ok(entries) => entries,
                    Err(err) if err.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
                    Err(err) => return Err(to_file_error(err).into()),
                };
                let mut predecessors = BTreeSet::new();
                let mut visited = 0usize;
                for entry in recovery_entries(entries, recovery_scope.as_ref().map(|scope| scope.budget.as_ref())) {
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
                        records
                            .push(serde_json::from_slice::<DeleteRecoveryJournal>(&bytes).map_err(|_| DiskError::FileCorrupt)?);
                    }
                    if let Some(bytes) = receipt.filter(|bytes| !bytes.is_empty()) {
                        records.push(
                            serde_json::from_slice::<CommitReceipt>(&bytes)
                                .map_err(|_| DiskError::FileCorrupt)?
                                .journal,
                        );
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
    /// Local maintenance may pause a page behind recovery backpressure. It
    /// retains only its own bounded set of cursors; remote callers still have
    /// the ordinary idle deadline and every resumed page rechecks mount identity.
    pub(crate) fn renew_delete_recovery_cursor(&self, volume: &str, cursor: &DeleteRecoveryCursor) -> bool {
        let mut sessions = DISCOVERY_SESSIONS.lock();
        let Some(session) = sessions.get_mut(&cursor.token) else {
            return false;
        };
        if session.disk_root != self.io_root()
            || session.volume != volume
            || session.object.is_some()
            || session.touched.elapsed() > Duration::from_secs(300)
        {
            return false;
        }
        session.touched = Instant::now();
        true
    }

    fn delete_discovery_path(&self, volume: &str, object: Option<&str>) -> Result<PathBuf> {
        if !safe_path(volume, false) || volume.contains('/') || object.is_some_and(|object| !safe_path(object, false)) {
            return Err(DiskError::InvalidPath);
        }
        match object {
            Some(object) => self.io_get_object_path(volume, object),
            None => self.io_get_bucket_path(volume),
        }
    }

    fn new_delete_discovery_session(&self, volume: &str, object: Option<&str>, path: &Path) -> Result<DiscoverySession> {
        #[cfg(target_os = "linux")]
        let (mount, root) = {
            use std::os::fd::AsRawFd;
            let mount = self.mount_lease.try_clone().map_err(to_file_error)?;
            let root = PathBuf::from(format!("/proc/self/fd/{}/.", mount.as_raw_fd())).join(volume);
            let root = match object {
                Some(object) => root.join(object),
                None => root,
            };
            let _ = path;
            (mount, root)
        };
        #[cfg(windows)]
        let (mount, root) = {
            use std::os::windows::fs::OpenOptionsExt;
            use windows_sys::Win32::Storage::FileSystem::{
                FILE_FLAG_BACKUP_SEMANTICS, FILE_READ_ATTRIBUTES, FILE_SHARE_READ, FILE_SHARE_WRITE, FILE_TRAVERSE,
            };
            let mount = std::fs::OpenOptions::new()
                .access_mode(FILE_TRAVERSE | FILE_READ_ATTRIBUTES)
                .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
                .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
                .open(self.io_root())
                .map_err(to_file_error)?;
            (mount, path.to_owned())
        };
        #[cfg(all(not(target_os = "linux"), not(windows)))]
        let (mount, root) = (std::fs::File::open(self.io_root()).map_err(to_file_error)?, path.to_owned());
        Ok(DiscoverySession {
            disk_root: self.io_root().to_path_buf(),
            volume: volume.to_owned(),
            object: object.map(str::to_owned),
            root,
            _mount: mount,
            frames: Vec::new(),
            touched: Instant::now(),
        })
    }

    fn validate_delete_discovery_owner(&self, session: &DiscoverySession) -> Result<()> {
        if session.disk_root != self.io_root() || session.touched.elapsed() > Duration::from_secs(300) {
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
        Ok(())
    }

    async fn advance_delete_discovery(
        &self,
        path: &Path,
        session: DiscoverySession,
        limit: u32,
    ) -> Result<(Vec<DeleteTransactionId>, Option<DiscoverySession>)> {
        let lease = os::acquire_namespace_mutation_lease_with_owner(path, None).await;
        self.validate_delete_discovery_owner(&session)?;
        os::run_blocking_namespace_operation(lease, move || session.advance(limit))
            .await
            .map_err(|error| to_file_error(error).into())
    }

    #[cfg(test)]
    pub(crate) fn public_delete_discovery_sessions_for_test(&self) -> usize {
        DISCOVERY_SESSIONS
            .lock()
            .values()
            .filter(|session| session.disk_root == self.io_root())
            .count()
    }

    pub(super) async fn delete_recovery_discover_bounded(
        &self,
        volume: &str,
        object: &str,
        max_entries: u32,
    ) -> Result<super::super::BoundedDeleteRecoveryDiscovery> {
        if !(1..=super::super::DELETE_RECOVERY_MAX_DISCOVERY_ENTRIES).contains(&max_entries) {
            return Err(DiskError::InvalidPath);
        }
        let path = self.delete_discovery_path(volume, Some(object))?;
        let session = self.new_delete_discovery_session(volume, Some(object), &path)?;
        let lease = os::acquire_namespace_mutation_lease_with_owner(&path, None).await;
        self.validate_delete_discovery_owner(&session)?;
        // The blocking owner drops every directory handle even if the RPC waiter disappears.
        // A partial inventory is never evidence of absence and never enters DISCOVERY_SESSIONS.
        // A large folder prefix may take a while to walk, so the probe rides the
        // durable-mutation lane like bounded inspection, never the one-worker
        // foreground lane that every other mutation on the drive queues behind.
        // It also stops, incomplete, when its waiter goes away or well before the
        // drive timeout, so a slow walk answers SlowDown instead of marking the
        // drive failed or holding a lane worker after the request is gone.
        let deadline = Instant::now() + bounded_discovery_time_limit();
        os::run_cancellable_blocking_namespace_operation_with_sync(lease, true, move |is_cancelled| {
            let should_stop = || is_cancelled() || Instant::now() >= deadline;
            let (transactions, next, visited) = session.advance_bounded(256, max_entries, &should_stop)?;
            Ok(super::super::BoundedDeleteRecoveryDiscovery {
                transactions: if next.is_none() { transactions } else { Vec::new() },
                visited,
                complete: next.is_none(),
            })
        })
        .await
        .map_err(|error| to_file_error(error).into())
    }

    pub(super) async fn delete_recovery_discover(
        &self,
        volume: &str,
        object: Option<&str>,
        cursor: Option<DeleteRecoveryCursor>,
        limit: u32,
    ) -> Result<DeleteRecoveryPage> {
        if !(1..=256).contains(&limit) {
            return Err(DiskError::InvalidPath);
        }
        let path = self.delete_discovery_path(volume, object)?;
        let (token, session) = if let Some(cursor) = cursor {
            let session = DISCOVERY_SESSIONS
                .lock()
                .remove(&cursor.token)
                .ok_or(DiskError::FileNotFound)?;
            if session.volume != volume || session.object.as_deref() != object {
                return Err(DiskError::InvalidPath);
            }
            (cursor.token, session)
        } else {
            (Uuid::new_v4(), self.new_delete_discovery_session(volume, object, &path)?)
        };
        let (transactions, session) = self.advance_delete_discovery(&path, session, limit).await?;
        let next = if let Some(mut session) = session {
            session.touched = Instant::now();
            let mut sessions = DISCOVERY_SESSIONS.lock();
            sessions.retain(|_, session| session.touched.elapsed() <= Duration::from_secs(300));
            if sessions.len() >= 128 {
                return Err(DiskError::Io(std::io::Error::from(ErrorKind::WouldBlock)));
            }
            sessions.insert(token, session);
            Some(DeleteRecoveryCursor { token })
        } else {
            None
        };
        Ok(DeleteRecoveryPage { transactions, next })
    }

    /// Existence probes retain their traversal privately, including across cancellation.
    pub(super) async fn delete_recovery_has_evidence(&self, volume: &str, object: &str) -> Result<bool> {
        let path = self.delete_discovery_path(volume, Some(object))?;
        self.new_delete_discovery_session(volume, Some(object), &path)?
            .has_evidence(self, &path)
            .await
    }

    pub(super) async fn delete_recovery_inspect_bounded(
        &self,
        volume: &str,
        object: &str,
        transaction: Uuid,
        max_entries: u32,
    ) -> Result<super::super::BoundedDeleteRecoveryInspection> {
        if !safe_path(object, false)
            || transaction.is_nil()
            || !(1..=super::super::DELETE_RECOVERY_MAX_INSPECTION_ENTRIES).contains(&max_entries)
        {
            return Err(DiskError::InvalidPath);
        }
        let root = self.get_object_path(volume, object)?;
        let owner = os::acquire_metadata_mutation_lease(&root, None).await;
        let lease = os::acquire_namespace_mutation_lease_with_owner(&root, Some(owner)).await;
        let volume = volume.to_owned();
        let object = object.to_owned();
        // Same lane as the unbounded inspection: a bulk delete inspects one
        // transaction per key on every drive, and the one-worker foreground
        // namespace lane would serialize those behind every other mutation.
        os::run_blocking_namespace_operation_with_sync(lease, true, move || {
            let budget = crate::object_api::DeleteRecoveryWorkBudget::new(max_entries);
            Ok(
                inspection_with_markers_bounded(&root, &volume, &object, transaction, Some(&budget)).map(|(inspection, _)| {
                    super::super::BoundedDeleteRecoveryInspection {
                        inspection,
                        visited: max_entries - budget.remaining(),
                    }
                }),
            )
        })
        .await
        .map_err(to_file_error)?
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
        Ok(self
            .delete_recovery_inspect_owned_with_markers(volume, object, transaction, owner)
            .await?
            .0)
    }

    async fn delete_recovery_inspect_owned_with_markers(
        &self,
        volume: &str,
        object: &str,
        transaction: Uuid,
        owner: Option<Arc<dyn Send + Sync>>,
    ) -> Result<(DeleteRecoveryInspection, bool)> {
        let root = self.io_get_object_path(volume, object)?;
        self.io_get_object_path(volume, &format!("{object}/{transaction}"))?;
        let volume = volume.to_owned();
        let object = object.to_owned();
        let lease = os::acquire_namespace_mutation_lease_with_owner(&root, owner).await;
        let scope = recovery_operation_scope();
        // Journal inspections ride the durable-mutation lane (the fused delete
        // prepare already does): a bulk delete publishes one transaction per
        // key, and the one-worker foreground namespace lane would serialize
        // those per-drive publishes behind every other mutation.
        os::run_blocking_namespace_operation_with_sync(lease, true, move || {
            Ok(inspection_with_markers_bounded(
                &root,
                &volume,
                &object,
                transaction,
                scope.as_ref().map(|scope| scope.budget.as_ref()),
            ))
        })
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
        let (bucket, root) = self.io_get_bucket_and_object_path(volume, object)?;
        let destination = root.join(transaction.to_string());
        let destination_was_absent = original.is_none();
        let original = original.map(<[u8]>::to_vec);
        let sync = effective_durability(volume).syncs_commit_metadata();
        #[cfg(test)]
        let record_destination = destination.clone();
        let staging_root = temporary;
        let lease = os::acquire_namespace_mutation_lease_with_owner(&destination, owner.clone()).await;
        let (staged, target) = os::run_blocking_namespace_operation_with_sync(lease, sync, move || {
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
                run_owned_file_before_open(&record_destination.join(name));
                let mut file = std::fs::File::create(staged.path().join(name))?;
                std::io::Write::write_all(&mut file, bytes)?;
                if sync && !bytes.is_empty() {
                    file.sync_data()?;
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
            Ok(metadata) if metadata.is_dir() => {
                // Proof GC may persist before its final rmdir. Only an empty
                // leaf can be retired; nested prefixes and new evidence fail
                // closed under the same namespace ownership as publication.
                match os::remove_dir_with_owner(&destination, owner.clone()).await {
                    Ok(()) => {}
                    Err(err) if err.kind() == ErrorKind::NotFound => {}
                    Err(err) if is_dir_not_empty_error(&err) => return Err(DiskError::OutdatedXLMeta),
                    Err(err) => return Err(to_file_error(err).into()),
                }
            }
            Ok(_) => return Err(DiskError::OutdatedXLMeta),
            Err(err) if err.kind() == ErrorKind::NotFound => {}
            Err(err) => return Err(to_file_error(err).into()),
        }
        // The directory owner accompanies the blocking rename, so cancelling
        // its waiter cannot delete staged files while activation is running.
        let publication_owner: Option<Arc<dyn Send + Sync>> = Some(Arc::new((owner.clone(), staged.clone())));
        os::rename_all_with_owner(staged.path(), &destination, &bucket, &self.publication_root, publication_owner).await?;
        if sync {
            let mut dirs_to_sync = Vec::new();
            let mut path = root.as_path();
            loop {
                dirs_to_sync.push(path.to_path_buf());
                if !destination_was_absent || path == bucket {
                    break;
                }
                path = path.parent().ok_or(DiskError::InvalidPath)?;
            }
            let sync_owner = owner.clone();
            self.blocking_queue
                .run_fsync_cancellable(move |_| {
                    let _owner = sync_owner;
                    for dir in dirs_to_sync {
                        os::fsync_dir_std(&dir)?;
                    }
                    Ok(())
                })
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
        self.reject_multipart_delete_recovery_mutation(volume, object)?;

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
        let data_dirs = if original.is_none() {
            for request in requests {
                plan.add_version(request.clone())?;
            }
            Vec::new()
        } else {
            plan.delete_versions(requests)?.into_iter().flatten().collect::<Vec<_>>()
        };
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

    pub(super) async fn delete_recovery_settle_bounded(
        &self,
        volume: &str,
        request: super::super::BoundedDeleteRecoverySettlement,
        owner: Option<Arc<dyn Send + Sync>>,
    ) -> Result<super::super::BoundedDeleteRecoverySettlementReply> {
        if !(1..=super::super::DELETE_RECOVERY_MAX_INSPECTION_ENTRIES).contains(&request.max_entries) {
            return Err(DiskError::InvalidPath);
        }
        let scope = Arc::new(RecoveryOperationScope {
            budget: Arc::new(crate::object_api::DeleteRecoveryWorkBudget::new(request.max_entries)),
            may_publish_receipt: matches!(
                &request.action,
                DeleteRecoveryAction::PublishCommit | DeleteRecoveryAction::AdoptCommit { .. }
            ),
            receipt_published: std::sync::atomic::AtomicBool::new(false),
        });
        let result = RECOVERY_OPERATION
            .scope(
                Arc::clone(&scope),
                self.delete_recovery_settle(
                    volume,
                    &request.object,
                    request.transaction,
                    request.expected_fingerprint,
                    request.action,
                    owner,
                ),
            )
            .await;
        let outcome = match result {
            Ok(inspection) => super::super::DeleteRecoverySettlementOutcome::Inspected(Box::new(inspection)),
            Err(DiskError::DeleteRecoveryWorkLimit) if scope.receipt_published.load(std::sync::atomic::Ordering::Acquire) => {
                super::super::DeleteRecoverySettlementOutcome::ReceiptPublishedInspectionDeferred
            }
            Err(error) => return Err(error),
        };
        Ok(super::super::BoundedDeleteRecoverySettlementReply {
            outcome,
            visited: request.max_entries - scope.budget.remaining(),
        })
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
        self.reject_multipart_delete_recovery_mutation(volume, object)?;

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
        self.reject_multipart_delete_recovery_mutation(volume, object)?;
        if let Ok(root) = self.io_get_object_path(volume, object) {
            let tx_dir = root.join(transaction.to_string());
            let _ = take_prepared_delete_receipt(&tx_dir);
            let _ = take_published_delete_receipt(&tx_dir);
        }

        let (mut observed, mut has_data_dir_work) = self
            .delete_recovery_inspect_owned_with_markers(volume, object, transaction, owner.clone())
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
                (observed, has_data_dir_work) = self
                    .delete_recovery_inspect_owned_with_markers(volume, object, transaction, owner.clone())
                    .await?;
            } else {
                return Ok(observed);
            }
        }
        if action == DeleteRecoveryAction::DiscardLegacy {
            if observed.state != DeleteRecoveryState::LegacyAmbiguous || !observed.explicit_delete_safe {
                return Err(DiskError::FileCorrupt);
            }
            let root = self.io_get_object_path(volume, object)?;
            let rollback = root.join(transaction.to_string());
            let plan_root = root.clone();
            let missing = observed.current_digest.is_none();
            let lease = os::acquire_namespace_mutation_lease_with_owner(&root, owner.clone()).await;
            let scope = recovery_operation_scope();
            // Same durable-mutation lane as the journal inspection above: this
            // plan closure runs inside every settle on the bulk-delete publish
            // path.
            let plan = os::run_blocking_namespace_operation_with_sync(lease, true, move || {
                Ok((|| {
                    let before =
                        regular_metadata_bytes(&plan_root.join(transaction.to_string()).join(STORAGE_FORMAT_FILE_BACKUP))?
                            .as_deref()
                            .map(FileMeta::load)
                            .transpose()?;
                    explicit_delete_plan_bounded(
                        &plan_root,
                        transaction,
                        before.as_ref(),
                        None,
                        missing,
                        scope.as_ref().map(|scope| scope.budget.as_ref()),
                    )?
                    .ok_or(DiskError::FileCorrupt)
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
                None,
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
                let inspect_after = (!cfg!(feature = "e2e-test-hooks")).then_some(object);
                let inspected = write_delete_rollback_files(
                    self.io_root(),
                    volume,
                    &root,
                    transaction,
                    &[],
                    &[(DELETE_QUORUM_COMMITTED_FILE, &serde_json::to_vec(&receipt)?)],
                    inspect_after,
                    owner.clone(),
                )
                .await?;
                self.delete_recovery_barrier(volume, object, transaction, "DECISION").await?;
                if let Some(inspected) = inspected {
                    return Ok(inspected);
                }
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
                for dir in journal.data_dirs.iter().filter(|_| has_data_dir_work) {
                    self.reserve_version_delete_with_namespace_owner(volume, object, *dir, transaction, owner.clone())
                        .await?;
                }
                if observed.current_digest == journal.original_digest {
                    if journal.target_digest.is_some() {
                        let target = self.read_all(volume, &format!("{object}/{transaction}/{TARGET}")).await?;
                        self.write_all_meta_with_namespace_owner(
                            volume,
                            &format!("{object}/{STORAGE_FORMAT_FILE}"),
                            target,
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
                            None,
                        )
                        .await?;
                        if effective_durability(volume).syncs_commit_metadata() {
                            os::fsync_dir_with_owner(&root, owner.clone()).await.map_err(to_file_error)?;
                        }
                    }
                }
                if has_data_dir_work {
                    self.commit_reserved_version_delete_with_namespace_owner(volume, object, transaction, owner.clone())
                        .await?;
                    os::remove_files_and_fsync_with_owner(
                        &rollback,
                        &[STORAGE_FORMAT_FILE_BACKUP, DELETE_MARKER_ROLLBACK_FILE],
                        effective_durability(volume).syncs_commit_metadata(),
                        None,
                        owner.clone(),
                    )
                    .await
                    .map_err(to_file_error)?;
                    let (_, complete) = self
                        .finish_version_delete(volume, object, transaction, true, owner.clone(), None)
                        .await?;
                    if !complete {
                        return Err(DiskError::DiskOngoingReq);
                    }
                }
                for dir in journal.data_dirs.iter().filter(|_| has_data_dir_work) {
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
                let receipt = CommitReceipt {
                    journal: journal.clone(),
                    finished: true,
                };
                let receipt_bytes = serde_json::to_vec(&receipt)?;
                let remove_before_publish: &[&'static str] = if has_data_dir_work {
                    &[]
                } else {
                    &[STORAGE_FORMAT_FILE_BACKUP, DELETE_MARKER_ROLLBACK_FILE]
                };
                if let Some(inspected) = write_delete_rollback_files(
                    self.io_root(),
                    volume,
                    &root,
                    transaction,
                    remove_before_publish,
                    &[(FINISHED, &[]), (DELETE_QUORUM_COMMITTED_FILE, &receipt_bytes)],
                    Some(object),
                    owner.clone(),
                )
                .await?
                {
                    return Ok(inspected);
                }
            }
            DeleteRecoveryAction::FinalizeCommit => {
                if observed.state != DeleteRecoveryState::CommitFinished {
                    return Err(DiskError::DiskOngoingReq);
                }
                let bucket_path = self.io_get_bucket_path(volume)?;
                let mut file_leases = Vec::with_capacity(5);
                for name in [
                    STORAGE_FORMAT_FILE_BACKUP,
                    DELETE_MARKER_ROLLBACK_FILE,
                    TARGET,
                    PREPARED,
                    FINISHED,
                ] {
                    let path = rollback.join(name);
                    let lease = os::acquire_namespace_mutation_lease_with_owner(&path, owner.clone()).await;
                    file_leases.push((path, lease));
                }
                let receipt_path = rollback.join(DELETE_QUORUM_COMMITTED_FILE);
                let receipt_lease = os::acquire_namespace_mutation_lease_with_owner(&receipt_path, owner.clone()).await;
                let prune_paths: Vec<_> = rollback
                    .ancestors()
                    .take_while(|dir| *dir != bucket_path.as_path() && dir.starts_with(&bucket_path))
                    .map(Path::to_path_buf)
                    .collect();
                let mut prune_dirs = Vec::with_capacity(prune_paths.len());
                for dir in prune_paths {
                    let lease = os::acquire_namespace_mutation_lease_with_owner(&dir, owner.clone()).await;
                    prune_dirs.push((dir, lease));
                }
                let sync = effective_durability(volume).syncs_commit_metadata();
                let (rollback_buf, root_buf, volume_str, object_str) =
                    (rollback.clone(), root.clone(), volume.to_owned(), object.to_owned());
                let publication_root = self.publication_root.clone();
                let scope = recovery_operation_scope();
                return os::run_blocking_namespace_operation_with_sync(receipt_lease, sync, move || {
                    let _publication_root = publication_root;
                    for (path, _) in &file_leases {
                        #[cfg(all(any(test, feature = "test-util"), not(windows)))]
                        os::prepared_publication_test_hooks::run(os::prepared_publication_test_hooks::Stage::Remove, path);
                        match std::fs::remove_file(path) {
                            Ok(()) => {}
                            Err(err) if err.kind() == ErrorKind::NotFound => {}
                            Err(err) => return Err(err),
                        }
                    }
                    if sync {
                        os::fsync_dir_std(&rollback_buf)?;
                    }
                    #[cfg(all(any(test, feature = "test-util"), not(windows)))]
                    os::prepared_publication_test_hooks::run(os::prepared_publication_test_hooks::Stage::Remove, &receipt_path);
                    std::fs::remove_file(&receipt_path)?;
                    for (dir_path, _) in &prune_dirs {
                        #[cfg(all(any(test, feature = "test-util"), not(windows)))]
                        os::prepared_publication_test_hooks::run(os::prepared_publication_test_hooks::Stage::Remove, dir_path);
                        if let Err(err) = std::fs::remove_dir(dir_path) {
                            if is_dir_not_empty_error(&err) {
                                break;
                            }
                            if !is_benign_object_rmdir_error(&err) {
                                return Err(std::io::Error::other(FileAccessDeniedWithContext {
                                    path: dir_path.clone(),
                                    source: err,
                                }));
                            }
                        }
                    }
                    let inspected = inspection_with_markers_bounded(
                        &root_buf,
                        &volume_str,
                        &object_str,
                        transaction,
                        scope.as_ref().map(|scope| scope.budget.as_ref()),
                    )
                    .map(|(state, _)| state);
                    drop((file_leases, prune_dirs));
                    Ok(inspected)
                })
                .await
                .map_err(to_file_error)?;
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
                        None,
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

    #[cfg(test)]
    pub(super) async fn try_fused_recoverable_delete_version(
        &self,
        volume: &str,
        object: &str,
        fi: &FileInfo,
        opts: &DeleteOptions,
        transaction: Uuid,
        owner: Option<Arc<dyn Send + Sync>>,
    ) -> Result<Option<()>> {
        self.try_fused_recoverable_delete_version_with_paths(volume, object, fi, opts, transaction, owner, None)
            .await
    }

    #[expect(clippy::too_many_arguments)]
    pub(super) async fn try_fused_recoverable_delete_version_with_paths(
        &self,
        volume: &str,
        object: &str,
        fi: &FileInfo,
        opts: &DeleteOptions,
        transaction: Uuid,
        owner: Option<Arc<dyn Send + Sync>>,
        prevalidated_paths: Option<(PathBuf, PathBuf, PathBuf)>,
    ) -> Result<Option<()>> {
        if !opts.recoverable_delete
            || opts.undo_write
            || opts.expected_delete_marker.is_some()
            || pool_meta_native::is_object(&self.root, volume, object)
            || has_delete_version_test_failpoints()
        {
            return Ok(None);
        }
        self.reject_multipart_delete_recovery_mutation(volume, object)?;
        self.wait_for_startup_cleanup().await;

        let (object_path, bucket_path, file_path) = match prevalidated_paths {
            Some(paths) => paths,
            None => {
                let obj = self.get_object_path(volume, object)?;
                let (b, f) = self.io_get_bucket_and_object_path(volume, object)?;
                check_path_length(f.to_string_lossy().as_ref())?;
                (obj, b, f)
            }
        };
        let xl_path = path_join(&[file_path.as_path(), Path::new(STORAGE_FORMAT_FILE)]);
        let destination = file_path.join(transaction.to_string());
        let staging_root = if cfg!(target_os = "linux") {
            self.io_root().join(RUSTFS_META_TMP_BUCKET)
        } else {
            self.io_get_bucket_path(RUSTFS_META_TMP_BUCKET)?
        };
        let sync = effective_durability(volume).syncs_commit_metadata();
        let incarnation = opts.delete_recovery_bucket_incarnation;
        let lease = os::acquire_namespace_mutation_lease_with_owner(&destination, owner).await;
        let fi_clone = fi.clone();
        let volume_str = volume.to_owned();
        let object_str = object.to_owned();

        let recovery_scope = recovery_operation_scope();
        type FusedStage1Outcome = Option<(Option<Uuid>, PathBuf, PreparedDeleteReceiptEntry)>;
        let res = os::run_blocking_namespace_operation_with_sync(
            lease,
            sync,
            move || -> std::io::Result<Result<FusedStage1Outcome>> {
                #[cfg(unix)]
                let file_path_dir = match open_dir_fd(&file_path) {
                    Ok(dir) => Some(Arc::new(dir)),
                    Err(err) if matches!(err.kind(), ErrorKind::NotFound | ErrorKind::NotADirectory) => {
                        return Ok(Ok(None));
                    }
                    Err(_) => None,
                };
                #[cfg(not(unix))]
                let file_path_dir: Option<Arc<std::fs::File>> = None;
                let buf = match read_child_regular_bytes(file_path_dir.as_deref(), STORAGE_FORMAT_FILE, &xl_path) {
                    Ok(buf) => buf,
                    Err(err) if err.kind() == ErrorKind::NotFound => return Ok(Ok(None)),
                    Err(err) => return Err(err),
                };
                type PreparedDeleteTuple =
                    (Option<Uuid>, Option<Vec<u8>>, Vec<u8>, DeleteRecoveryJournal, bool, bool, Option<bool>);
                let (old_dir, target, journal_raw, journal, sole_transaction, destination_missing, old_data_dir_state) =
                    match (|| -> Result<Option<PreparedDeleteTuple>> {
                        let mut meta = FileMeta::load(&buf)?;
                        let mut deleted_headers: Vec<_> = meta
                            .versions
                            .iter()
                            .map(|v| (v.header.version_id, std::mem::discriminant(&v.header.version_type)))
                            .collect();
                        let original_versions: Vec<_> = meta
                            .versions
                            .iter()
                            .map(|v| generation(v.into_fileinfo(&volume_str, &object_str, true)?))
                            .collect::<Result<_>>()?;
                        let old_dir = meta.delete_version(&fi_clone)?;
                        if old_dir.is_some_and(|dir| dir == transaction || dir.is_nil()) {
                            return Err(DiskError::FileCorrupt);
                        }
                        let mut old_path_to_verify = None;
                        if let Some(uuid) = old_dir {
                            meta.data.remove(vec![fi_clone.version_id.unwrap_or_default(), uuid])?;
                            let old_path = path_join(&[file_path.as_path(), Path::new(uuid.to_string().as_str())]);
                            check_path_length(old_path.to_string_lossy().as_ref())?;
                            old_path_to_verify = Some(old_path);
                        }
                        let surviving: HashSet<_> = meta
                            .versions
                            .iter()
                            .map(|v| (v.header.version_id, std::mem::discriminant(&v.header.version_type)))
                            .collect();
                        deleted_headers.retain(|header| !surviving.contains(header));
                        let deleted_versions = deleted_headers.into_iter().map(|(id, _)| id.unwrap_or_default()).collect();
                        let target = (!meta.versions.is_empty()).then(|| meta.marshal_msg()).transpose()?;
                        let mut predecessors = BTreeSet::new();
                        let mut sole_transaction = true;
                        let mut destination_exists = false;
                        let mut old_data_dir_exists = false;
                        let mut scanned_file_path = false;
                        if let Ok(entries) = std::fs::read_dir(&file_path) {
                            scanned_file_path = true;
                            for (visited, entry) in
                                recovery_entries(entries, recovery_scope.as_ref().map(|scope| scope.budget.as_ref())).enumerate()
                            {
                                if visited >= 65536 {
                                    return Err(DiskError::DiskOngoingReq);
                                }
                                let entry = entry.map_err(to_file_error)?;
                                let file_name = entry.file_name();
                                let Some(name_str) = file_name.to_str() else {
                                    sole_transaction = false;
                                    continue;
                                };
                                let Some(id) = Uuid::parse_str(name_str).ok().filter(|id| !id.is_nil()) else {
                                    continue;
                                };
                                if id == transaction {
                                    destination_exists = true;
                                    continue;
                                }
                                if !entry.file_type().map_err(to_file_error)?.is_dir() {
                                    return Err(DiskError::InvalidPath);
                                }
                                if old_dir == Some(id) && name_str == id.hyphenated().to_string() {
                                    old_data_dir_exists = true;
                                }
                                let dir_path = entry.path();
                                let prepared = regular_bytes(&dir_path.join(PREPARED))?;
                                let receipt = regular_bytes(&dir_path.join(DELETE_QUORUM_COMMITTED_FILE))?;
                                if sole_transaction && name_str == id.hyphenated().to_string() && old_dir != Some(id) {
                                    let is_surviving_data_dir = original_versions.iter().any(|v| v.data_dir == Some(id))
                                        && prepared.is_none()
                                        && receipt.is_none()
                                        && !dir_path.join(STORAGE_FORMAT_FILE_BACKUP).exists();
                                    if !is_surviving_data_dir {
                                        sole_transaction = false;
                                    }
                                }
                                let mut records = Vec::new();
                                if let Some(bytes) = prepared {
                                    records.push(
                                        serde_json::from_slice::<DeleteRecoveryJournal>(&bytes)
                                            .map_err(|_| DiskError::FileCorrupt)?,
                                    );
                                }
                                if let Some(bytes) = receipt.filter(|bytes| !bytes.is_empty()) {
                                    records.push(
                                        serde_json::from_slice::<CommitReceipt>(&bytes)
                                            .map_err(|_| DiskError::FileCorrupt)?
                                            .journal,
                                    );
                                }
                                for record in records {
                                    if record.protocol_version != DELETE_RECOVERY_PROTOCOL_VERSION
                                        || record.volume != volume_str
                                        || record.object != object_str
                                        || record.transaction != id
                                        || incarnation.zip(record.bucket_incarnation).is_some_and(|(a, b)| a != b)
                                        || record.predecessors.iter().any(|before| before.is_nil() || *before == id)
                                    {
                                        return Err(DiskError::FileCorrupt);
                                    }
                                    predecessors.insert(id);
                                }
                            }
                        }
                        if let Some(old_path) = old_path_to_verify
                            && (!scanned_file_path || old_data_dir_exists)
                            && old_path.join(STORAGE_FORMAT_FILE).exists()
                        {
                            return Ok(None);
                        }
                        sole_transaction &= predecessors.is_empty();
                        let mut sparse = fi_clone;
                        sparse.data = None;
                        let journal = DeleteRecoveryJournal {
                            protocol_version: DELETE_RECOVERY_PROTOCOL_VERSION,
                            volume: volume_str,
                            object: object_str,
                            transaction,
                            bucket_incarnation: incarnation,
                            original_digest: Some(digest(&buf)),
                            target_digest: target.as_deref().map(digest),
                            data_dirs: old_dir.into_iter().collect(),
                            deleted_versions,
                            requests: vec![sparse],
                            original_versions,
                            predecessors: predecessors.into_iter().collect(),
                        };
                        let raw = journal_bytes(&journal)?;
                        Ok(Some((
                            old_dir,
                            target,
                            raw,
                            journal,
                            sole_transaction,
                            scanned_file_path && !destination_exists,
                            scanned_file_path.then_some(old_data_dir_exists),
                        )))
                    })() {
                        Ok(Some(tuple)) => tuple,
                        Ok(None) => return Ok(Ok(None)),
                        Err(err) => return Ok(Err(err)),
                    };
                let staged = match tempfile::Builder::new().prefix("delete-prep-").tempdir_in(&staging_root) {
                    Ok(dir) => dir,
                    Err(err) if err.kind() == ErrorKind::NotFound => {
                        std::fs::create_dir_all(&staging_root)?;
                        tempfile::Builder::new().prefix("delete-prep-").tempdir_in(&staging_root)?
                    }
                    Err(err) => return Err(err),
                };
                let staged_dir = staged.path().to_path_buf();
                #[cfg(unix)]
                let rollback_dir_fd = open_dir_fd(&staged_dir).ok().map(Arc::new);
                #[cfg(not(unix))]
                let rollback_dir_fd: Option<Arc<std::fs::File>> = None;
                let backup_path = staged_dir.join(STORAGE_FORMAT_FILE_BACKUP);
                if link_child_files(
                    file_path_dir.as_deref(),
                    STORAGE_FORMAT_FILE,
                    &xl_path,
                    rollback_dir_fd.as_deref(),
                    STORAGE_FORMAT_FILE_BACKUP,
                    &backup_path,
                )
                .is_err()
                {
                    let file =
                        create_and_write_child_file(rollback_dir_fd.as_deref(), STORAGE_FORMAT_FILE_BACKUP, &backup_path, &buf)?;
                    if sync && !buf.is_empty() {
                        file.sync_data()?;
                    }
                }
                let mut written_files = Vec::with_capacity(2);
                for (name, bytes) in [(TARGET, target.as_deref()), (PREPARED, Some(journal_raw.as_slice()))] {
                    let Some(bytes) = bytes else {
                        continue;
                    };
                    let file = create_and_write_child_file(rollback_dir_fd.as_deref(), name, &staged_dir.join(name), bytes)?;
                    if sync && !bytes.is_empty() {
                        written_files.push(file);
                    }
                }
                for file in written_files {
                    file.sync_data()?;
                }
                if sync {
                    os::fsync_directory_handle_std(&staged_dir, rollback_dir_fd.as_deref())?;
                }
                let tx_name = transaction.to_string();
                if !destination_missing {
                    match std::fs::symlink_metadata(&destination) {
                        Ok(metadata) if metadata.is_dir() => {
                            match rmdir_child_dir(file_path_dir.as_deref(), &tx_name, &destination) {
                                Ok(()) => {}
                                Err(err) if err.kind() == ErrorKind::NotFound => {}
                                Err(err) if is_dir_not_empty_error(&err) => return Ok(Err(DiskError::OutdatedXLMeta)),
                                Err(err) => return Err(err),
                            }
                        }
                        Ok(_) => return Ok(Err(DiskError::OutdatedXLMeta)),
                        Err(err) if err.kind() == ErrorKind::NotFound => {}
                        Err(err) => return Err(err),
                    }
                }
                let staged_dir = staged.keep();
                os::record_native_publication(&destination);
                if let Err(err) = rename_into_dir_fd(&staged_dir, file_path_dir.as_deref(), &tx_name, &destination) {
                    let _ = std::fs::remove_dir_all(&staged_dir);
                    return Err(err);
                }
                let old_data_path = old_dir
                    .map(|uuid| file_path.join(uuid.to_string()))
                    .filter(|p| old_data_dir_state.unwrap_or_else(|| p.is_dir()));
                if sync && let Err(err) = os::fsync_directory_handle_std(&file_path, file_path_dir.as_deref()) {
                    let _ = std::fs::remove_dir_all(&destination);
                    return Err(err);
                }
                #[cfg(all(test, unix))]
                fused_prepare_checkpoint::run(&file_path);
                let mut reserved_data_path = None;
                if let Some(old_path) = old_data_path {
                    let reserved_marker = old_path.join(format!("{RESERVED_DELETE_DATA_DIR_MARKER_PREFIX}{transaction}"));
                    #[cfg(unix)]
                    let old_dir_file = sync.then(|| open_dir_fd(&old_path).ok()).flatten();
                    #[cfg(not(unix))]
                    let old_dir_file: Option<std::fs::File> = None;
                    let reserve_res = (|| -> std::io::Result<()> {
                        let marker = std::fs::File::create(&reserved_marker)?;
                        if sync {
                            marker.sync_all()?;
                            os::fsync_directory_handle_std(&old_path, old_dir_file.as_ref())?;
                        }
                        Ok(())
                    })();
                    if let Err(err) = reserve_res {
                        let _ = std::fs::remove_file(&reserved_marker);
                        let _ = std::fs::remove_dir_all(&destination);
                        return Err(err);
                    }
                    reserved_data_path = Some((old_path, reserved_marker, old_dir_file));
                }
                if let Some(ref target_bytes) = target {
                    // A provisional head must stay outside the published recovery transaction.
                    let target_in_rollback = destination.join(TARGET);
                    let linked_xl = staging_root.join(format!("delete-xl-{transaction}"));
                    os::record_native_publication(&xl_path);
                    let linked = match link_from_dir_fd(rollback_dir_fd.as_deref(), TARGET, &target_in_rollback, &linked_xl) {
                        Ok(()) => true,
                        Err(err) if err.kind() == ErrorKind::AlreadyExists => {
                            let _ = std::fs::remove_file(&linked_xl);
                            link_from_dir_fd(rollback_dir_fd.as_deref(), TARGET, &target_in_rollback, &linked_xl).is_ok()
                        }
                        Err(_) => false,
                    };
                    let write_res = if linked {
                        let rename_res = rename_into_dir_fd(&linked_xl, file_path_dir.as_deref(), STORAGE_FORMAT_FILE, &xl_path);
                        if rename_res.is_err() {
                            let _ = std::fs::remove_file(&linked_xl);
                        }
                        rename_res
                    } else {
                        (|| -> std::io::Result<()> {
                            let mut staged_xl = tempfile::NamedTempFile::new_in(&staging_root)?;
                            std::io::Write::write_all(staged_xl.as_file_mut(), target_bytes)?;
                            if sync && !target_bytes.is_empty() {
                                staged_xl.as_file().sync_data()?;
                            }
                            rename_into_dir_fd(staged_xl.path(), file_path_dir.as_deref(), STORAGE_FORMAT_FILE, &xl_path)
                        })()
                    };
                    if let Err(err) = write_res {
                        if let Some((_, reserved_marker, _)) = &reserved_data_path {
                            let _ = std::fs::remove_file(reserved_marker);
                        }
                        let _ = std::fs::remove_dir_all(&destination);
                        return Err(err);
                    }
                } else if let Err(err) = unlink_child_file(file_path_dir.as_deref(), STORAGE_FORMAT_FILE, &xl_path)
                    && err.kind() != ErrorKind::NotFound
                {
                    if let Some((_, reserved_marker, _)) = &reserved_data_path {
                        let _ = std::fs::remove_file(reserved_marker);
                    }
                    let _ = std::fs::remove_dir_all(&destination);
                    return Err(err);
                }
                if sync {
                    os::fsync_directory_handle_std(&file_path, file_path_dir.as_deref())?;
                }
                let had_old_data_dir = reserved_data_path.is_some();
                if let Some((old_path, reserved_marker, old_dir_file)) = reserved_data_path {
                    let recovery_marker = old_path.join(format!("{RECOVERY_DELETE_DATA_DIR_MARKER_PREFIX}{transaction}"));
                    match std::fs::rename(&reserved_marker, &recovery_marker) {
                        Ok(()) if sync => os::fsync_directory_handle_std(&old_path, old_dir_file.as_ref())?,
                        Ok(()) => {}
                        Err(err) if err.kind() == ErrorKind::NotFound => {}
                        Err(err) => return Err(err),
                    }
                }
                Ok(Ok(Some((
                    old_dir.filter(|_| had_old_data_dir),
                    destination,
                    PreparedDeleteReceiptEntry {
                        object_path,
                        bucket_path,
                        file_path,
                        staging_root,
                        journal_raw,
                        journal,
                        sole_transaction,
                        had_old_data_dir,
                        rollback_dir_fd,
                        file_path_dir,
                    },
                ))))
            },
        )
        .await
        .map_err(to_file_error)??;

        let Some((old_dir, destination, receipt_entry)) = res else {
            return Ok(None);
        };
        put_prepared_delete_receipt(destination, receipt_entry);
        if let Some(uuid) = old_dir {
            self.io_backend
                .invalidate_cached_fds_under(volume, &format!("{object}/{uuid}"));
        }
        Ok(Some(()))
    }

    #[cfg(test)]
    pub(crate) async fn try_publish_prepared_delete_local(
        &self,
        volume: &str,
        object: &str,
        transaction: Uuid,
        namespace_owner: Option<Arc<dyn Send + Sync>>,
    ) -> Result<Option<(DeleteRecoveryJournal, bool, bool)>> {
        self.try_publish_prepared_delete_local_with_budget(volume, object, transaction, namespace_owner, None)
            .await
    }

    pub(crate) async fn try_publish_prepared_delete_local_with_budget(
        &self,
        volume: &str,
        object: &str,
        transaction: Uuid,
        namespace_owner: Option<Arc<dyn Send + Sync>>,
        work_budget: Option<Arc<crate::object_api::DeleteRecoveryWorkBudget>>,
    ) -> Result<Option<(DeleteRecoveryJournal, bool, bool)>> {
        if transaction.is_nil() {
            return Err(DiskError::InvalidPath);
        }
        let rollback_open_path = if volume != RUSTFS_META_MULTIPART_BUCKET {
            self.io_get_object_open_path(volume, object)
                .ok()
                .map(|p| p.join(transaction.to_string()))
        } else {
            None
        };
        let cached_prepared = rollback_open_path.as_deref().and_then(take_prepared_delete_receipt);
        let (object_path, bucket_path, root, staging_root, cached_receipt) = match cached_prepared {
            Some(entry) => (
                entry.object_path,
                Some(entry.bucket_path),
                entry.file_path,
                entry.staging_root,
                Some((
                    entry.journal_raw,
                    entry.journal,
                    entry.sole_transaction,
                    entry.had_old_data_dir,
                    entry.rollback_dir_fd,
                    entry.file_path_dir,
                )),
            ),
            None => {
                self.reject_multipart_delete_recovery_mutation(volume, object)?;
                if !safe_path(object, false) {
                    return Err(DiskError::InvalidPath);
                }
                let obj_path = self.get_object_path(volume, object)?;
                (
                    obj_path,
                    None,
                    self.io_get_object_path(volume, object)?,
                    self.io_get_bucket_path(RUSTFS_META_TMP_BUCKET)?,
                    None,
                )
            }
        };
        let metadata_owner = os::acquire_metadata_mutation_lease(&object_path, namespace_owner).await;
        let volume_guard = os::disk_volume_mutation_lock(&self.root, volume).read_owned().await;
        let owner: Option<Arc<dyn Send + Sync>> = Some(Arc::new((metadata_owner, volume_guard)));
        let rollback = root.join(transaction.to_string());
        let receipt_path = rollback.join(DELETE_QUORUM_COMMITTED_FILE);
        let sync = effective_durability(volume).syncs_commit_metadata();
        let lease = os::acquire_namespace_mutation_lease_with_owner(&receipt_path, owner).await;
        let volume_str = volume.to_owned();
        let object_str = object.to_owned();

        type PublishDeleteFastReturn = (DeleteRecoveryJournal, bool, bool, Option<(PathBuf, PublishedDeleteReceiptEntry)>);
        let res = os::run_blocking_namespace_operation_with_sync(
            lease,
            sync,
            move || -> std::io::Result<Result<Option<PublishDeleteFastReturn>>> {
                if let Some((journal_raw, journal, sole_transaction, had_old_data_dir, rollback_dir_fd, file_path_dir)) =
                    cached_receipt
                    && child_regular_file_exists(rollback_dir_fd.as_deref(), PREPARED, &rollback.join(PREPARED))
                {
                    let tmp_receipt = staging_root.join(format!("receipt-{transaction}.tmp"));
                    let receipt_bytes = format_commit_receipt_bytes(&journal_raw, false);
                    let write_res = (|| -> std::io::Result<()> {
                        let mut file = std::fs::File::create(&tmp_receipt)?;
                        std::io::Write::write_all(&mut file, &receipt_bytes)?;
                        if sync && !receipt_bytes.is_empty() {
                            file.sync_data()?;
                        }
                        os::record_native_publication(&receipt_path);
                        rename_into_dir_fd(
                            &tmp_receipt,
                            rollback_dir_fd.as_deref(),
                            DELETE_QUORUM_COMMITTED_FILE,
                            &receipt_path,
                        )?;
                        if sync {
                            os::fsync_directory_handle_std(&rollback, rollback_dir_fd.as_deref())?;
                        }
                        Ok(())
                    })();
                    if let Err(err) = write_res {
                        let _ = std::fs::remove_file(&tmp_receipt);
                        return Err(err);
                    }
                    let published_entry = bucket_path.map(|bucket_path| {
                        (
                            rollback,
                            PublishedDeleteReceiptEntry {
                                object_path,
                                bucket_path,
                                file_path: root,
                                staging_root,
                                journal_raw,
                                had_old_data_dir,
                                rollback_dir_fd,
                                file_path_dir,
                            },
                        )
                    });
                    return Ok(Ok(Some((journal, sole_transaction, true, published_entry))));
                }
                let prepared = match regular_bytes(&rollback.join(PREPARED)) {
                    Ok(Some(bytes)) => bytes,
                    Ok(None) => return Ok(Ok(None)),
                    Err(e) => return Ok(Err(e)),
                };
                let Ok(journal) = serde_json::from_slice::<DeleteRecoveryJournal>(&prepared) else {
                    return Ok(Err(DiskError::FileCorrupt));
                };
                if journal.protocol_version != DELETE_RECOVERY_PROTOCOL_VERSION
                    || journal.volume != volume_str
                    || journal.object != object_str
                    || journal.transaction != transaction
                    || journal.transaction.is_nil()
                    || journal.data_dirs.iter().any(|id| id.is_nil() || *id == transaction)
                {
                    return Ok(Err(DiskError::FileCorrupt));
                }
                let target_satisfied = match journal.target_digest {
                    None => match regular_file_exists(&root.join(STORAGE_FORMAT_FILE)) {
                        Ok(exists) => !exists,
                        Err(e) => return Ok(Err(e)),
                    },
                    Some(expected_target) => match regular_bytes(&root.join(STORAGE_FORMAT_FILE)) {
                        Ok(Some(bytes)) => digest(&bytes) == expected_target,
                        Ok(None) => false,
                        Err(e) => return Ok(Err(e)),
                    },
                };
                if !target_satisfied {
                    return Ok(Ok(None));
                }
                let mut sole_transaction = journal.predecessors.is_empty();
                if sole_transaction {
                    match std::fs::read_dir(&root) {
                        Ok(entries) => {
                            for entry in recovery_entries(entries, work_budget.as_deref()) {
                                let entry = entry?;
                                let Ok(name) = entry.file_name().into_string() else {
                                    sole_transaction = false;
                                    break;
                                };
                                if let Some(id) = canonical_uuid(&name).filter(|id| !id.is_nil())
                                    && id != transaction
                                    && !journal.data_dirs.contains(&id)
                                {
                                    let dir_path = entry.path();
                                    let is_surviving_data_dir = journal.original_versions.iter().any(|v| v.data_dir == Some(id))
                                        && !dir_path.join(PREPARED).exists()
                                        && !dir_path.join(DELETE_QUORUM_COMMITTED_FILE).exists()
                                        && !dir_path.join(STORAGE_FORMAT_FILE_BACKUP).exists();
                                    if !is_surviving_data_dir {
                                        sole_transaction = false;
                                        break;
                                    }
                                }
                            }
                        }
                        Err(err) if err.kind() == ErrorKind::NotFound => {}
                        Err(err) => return Err(err),
                    }
                }
                let receipt = CommitReceipt {
                    journal: journal.clone(),
                    finished: false,
                };
                let receipt_bytes = serde_json::to_vec(&receipt).map_err(std::io::Error::other)?;
                std::fs::create_dir_all(&staging_root)?;
                let mut staged = tempfile::NamedTempFile::new_in(&staging_root)?;
                std::io::Write::write_all(staged.as_file_mut(), &receipt_bytes)?;
                if sync && !receipt_bytes.is_empty() {
                    staged.as_file().sync_data()?;
                }
                os::record_native_publication(&receipt_path);
                std::fs::rename(staged.path(), &receipt_path)?;
                if sync {
                    os::fsync_dir_std(&rollback)?;
                }
                Ok(Ok(Some((journal, sole_transaction, target_satisfied, None))))
            },
        )
        .await
        .map_err(to_file_error)?;

        let Some((journal, sole_transaction, target_satisfied, published_entry)) = res? else {
            return Ok(None);
        };
        if let Some((rollback, entry)) = published_entry {
            put_published_delete_receipt(rollback, entry);
        }
        Ok(Some((journal, sole_transaction, target_satisfied)))
    }

    #[cfg(test)]
    pub(crate) async fn try_finish_and_finalize_prepared_delete_local(
        &self,
        volume: &str,
        object: &str,
        transaction: Uuid,
        journal: &DeleteRecoveryJournal,
        namespace_owner: Option<Arc<dyn Send + Sync>>,
    ) -> Result<bool> {
        self.try_finish_and_finalize_prepared_delete_local_with_budget(
            volume,
            object,
            transaction,
            journal,
            namespace_owner,
            None,
        )
        .await
    }

    pub(crate) async fn try_finish_and_finalize_prepared_delete_local_with_budget(
        &self,
        volume: &str,
        object: &str,
        transaction: Uuid,
        journal: &DeleteRecoveryJournal,
        namespace_owner: Option<Arc<dyn Send + Sync>>,
        work_budget: Option<Arc<crate::object_api::DeleteRecoveryWorkBudget>>,
    ) -> Result<bool> {
        fn ignore_not_found(res: std::io::Result<()>) -> std::io::Result<()> {
            match res {
                Ok(()) => Ok(()),
                Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
                Err(err) => Err(err),
            }
        }
        if transaction.is_nil() {
            return Err(DiskError::InvalidPath);
        }
        let rollback_open_path = if volume != RUSTFS_META_MULTIPART_BUCKET {
            self.io_get_object_open_path(volume, object)
                .ok()
                .map(|p| p.join(transaction.to_string()))
        } else {
            None
        };
        let cached_published = rollback_open_path.as_deref().and_then(take_published_delete_receipt);
        if cached_published.is_none()
            && let Some(ref tx_open) = rollback_open_path
        {
            let _ = take_prepared_delete_receipt(tx_open);
        }
        let (
            object_path,
            bucket_path,
            staging_root,
            root,
            cached_journal_raw,
            cached_had_old_data_dir,
            mut rollback_dir_fd,
            mut file_path_dir,
        ) = match cached_published {
            Some(entry) => (
                entry.object_path,
                entry.bucket_path,
                entry.staging_root,
                entry.file_path,
                Some(entry.journal_raw),
                Some(entry.had_old_data_dir),
                entry.rollback_dir_fd,
                entry.file_path_dir,
            ),
            None => {
                self.reject_multipart_delete_recovery_mutation(volume, object)?;
                if !safe_path(object, false) {
                    return Err(DiskError::InvalidPath);
                }
                let obj_path = self.get_object_path(volume, object)?;
                (
                    obj_path,
                    self.io_get_bucket_path(volume)?,
                    self.io_get_bucket_path(RUSTFS_META_TMP_BUCKET)?,
                    self.io_get_object_path(volume, object)?,
                    None,
                    None,
                    None,
                    None,
                )
            }
        };
        let metadata_owner = os::acquire_metadata_mutation_lease(&object_path, namespace_owner).await;
        let volume_guard = os::disk_volume_mutation_lock(&self.root, volume).read_owned().await;
        let owner: Option<Arc<dyn Send + Sync>> = Some(Arc::new((metadata_owner, volume_guard)));
        let rollback = root.join(transaction.to_string());
        let receipt_path = rollback.join(DELETE_QUORUM_COMMITTED_FILE);
        let sync = effective_durability(volume).syncs_commit_metadata();

        let mut claims: Vec<SnapshotLeaseDeleteClaim> = Vec::with_capacity(journal.data_dirs.len());
        for data_dir in &journal.data_dirs {
            let key = SnapshotLeaseKey {
                volume: volume.to_owned(),
                path: format!("{object}/{data_dir}"),
            };
            let opts = DeleteOptions {
                recursive: true,
                old_data_dir: Some(transaction),
                ..Default::default()
            };
            let Some(claim) = self.claim_snapshot_delete(key, SnapshotPendingDelete::Ordinary(opts)) else {
                for prior in claims {
                    prior.finish(false);
                }
                return Ok(false);
            };
            claims.push(claim);
        }

        let data_dirs = journal.data_dirs.clone();
        let has_original = journal.original_digest.is_some();
        let has_target = journal.target_digest.is_some();
        let journal_for_receipt = cached_journal_raw.is_none().then(|| journal.clone());
        let lease = os::acquire_namespace_mutation_lease_with_owner(&receipt_path, owner).await;
        let prune_paths: Vec<_> = if has_target {
            vec![rollback.clone()]
        } else {
            rollback
                .ancestors()
                .take_while(|dir| *dir != bucket_path.as_path() && dir.starts_with(&bucket_path))
                .map(Path::to_path_buf)
                .collect()
        };

        let res = os::run_blocking_namespace_operation_with_sync(lease, sync, move || -> std::io::Result<()> {
            let persist_receipt = |finished: bool| -> std::io::Result<()> {
                let receipt_bytes = match &cached_journal_raw {
                    Some(raw) => format_commit_receipt_bytes(raw, finished),
                    None => {
                        let receipt = CommitReceipt {
                            journal: journal_for_receipt
                                .clone()
                                .ok_or_else(|| std::io::Error::other("missing journal for receipt"))?,
                            finished,
                        };
                        serde_json::to_vec(&receipt).map_err(std::io::Error::other)?
                    }
                };
                std::fs::create_dir_all(&staging_root)?;
                let mut staged_receipt = tempfile::NamedTempFile::new_in(&staging_root)?;
                std::io::Write::write_all(staged_receipt.as_file_mut(), &receipt_bytes)?;
                if sync && !receipt_bytes.is_empty() {
                    staged_receipt.as_file().sync_data()?;
                }
                os::record_native_publication(&receipt_path);
                std::fs::rename(staged_receipt.path(), &receipt_path)?;
                if sync {
                    let _ = os::fsync_dir_std(&rollback);
                }
                Ok(())
            };
            let committed = format!("{DELETE_DATA_DIR_MARKER_PREFIX}{transaction}");
            let recovery_committed = format!("{RECOVERY_DELETE_DATA_DIR_MARKER_PREFIX}{transaction}");
            let reserved = format!("{RESERVED_DELETE_DATA_DIR_MARKER_PREFIX}{transaction}");
            let mut had_physical_data_dirs = false;
            let fast_cleanup = cached_journal_raw.is_some();
            if cached_had_old_data_dir != Some(false) {
                let bases: &[&Path] = if fast_cleanup { &[&root] } else { &[&root, &rollback] };
                for data_dir in &data_dirs {
                    for base in bases {
                        let dir_path = base.join(data_dir.to_string());
                        match std::fs::read_dir(&dir_path) {
                            Ok(entries) => {
                                had_physical_data_dirs = true;
                                let mut removable = Vec::new();
                                let mut preserve_dir = false;
                                for entry in recovery_entries(entries, work_budget.as_deref()) {
                                    let entry = entry?;
                                    let name = entry.file_name();
                                    let Some(name) = name.to_str() else {
                                        preserve_dir = true;
                                        continue;
                                    };
                                    if entry.file_type()?.is_file()
                                        && (delete_recovery_part_file(name)
                                            || name == committed
                                            || name == recovery_committed
                                            || name == reserved
                                            || is_delete_transaction_marker(name, RESERVED_DELETE_DATA_DIR_MARKER_PREFIX))
                                    {
                                        removable.push(entry.path());
                                    } else {
                                        preserve_dir = true;
                                    }
                                }
                                for path in removable {
                                    ignore_not_found(std::fs::remove_file(path))?;
                                }
                                let mut dir_removed = false;
                                if !preserve_dir {
                                    match std::fs::remove_dir(&dir_path) {
                                        Ok(()) => dir_removed = true,
                                        Err(err) if err.kind() == ErrorKind::NotFound => dir_removed = true,
                                        Err(err) if is_dir_not_empty_error(&err) => {}
                                        Err(err) => return Err(err),
                                    }
                                }
                                if !dir_removed && sync {
                                    os::fsync_dir_std(&dir_path)?;
                                }
                            }
                            Err(err) if err.kind() == ErrorKind::NotFound => {}
                            Err(err) => return Err(err),
                        }
                    }
                }
            }
            if had_physical_data_dirs && sync && root.is_dir() {
                os::fsync_directory_handle_std(&root, file_path_dir.as_deref())?;
            }
            if fast_cleanup {
                let backup_or_marker = if has_original {
                    Some(STORAGE_FORMAT_FILE_BACKUP)
                } else if has_target {
                    Some(DELETE_MARKER_ROLLBACK_FILE)
                } else {
                    None
                };
                if let Some(name) = backup_or_marker
                    && let Err(err) = ignore_not_found(unlink_child_file(rollback_dir_fd.as_deref(), name, &rollback.join(name)))
                {
                    drop(rollback_dir_fd.take());
                    drop(file_path_dir.take());
                    if has_target || had_physical_data_dirs {
                        let _ = persist_receipt(true);
                    }
                    return Err(err);
                }
                for name in [PREPARED, FINISHED].into_iter().chain(has_target.then_some(TARGET)) {
                    let path = rollback.join(name);
                    #[cfg(all(any(test, feature = "test-util"), not(windows)))]
                    os::prepared_publication_test_hooks::run(os::prepared_publication_test_hooks::Stage::Remove, &path);
                    if let Err(err) = ignore_not_found(unlink_child_file(rollback_dir_fd.as_deref(), name, &path)) {
                        drop(rollback_dir_fd.take());
                        drop(file_path_dir.take());
                        if has_target || had_physical_data_dirs {
                            let _ = persist_receipt(true);
                        }
                        return Err(err);
                    }
                }
            } else {
                for name in [STORAGE_FORMAT_FILE_BACKUP, DELETE_MARKER_ROLLBACK_FILE] {
                    if let Err(err) = ignore_not_found(unlink_child_file(rollback_dir_fd.as_deref(), name, &rollback.join(name)))
                    {
                        drop(rollback_dir_fd.take());
                        drop(file_path_dir.take());
                        if has_target || had_physical_data_dirs {
                            let _ = persist_receipt(true);
                        }
                        return Err(err);
                    }
                }
                for name in [PREPARED, TARGET, FINISHED] {
                    let path = rollback.join(name);
                    #[cfg(all(any(test, feature = "test-util"), not(windows)))]
                    os::prepared_publication_test_hooks::run(os::prepared_publication_test_hooks::Stage::Remove, &path);
                    if let Err(err) = ignore_not_found(unlink_child_file(rollback_dir_fd.as_deref(), name, &path)) {
                        drop(rollback_dir_fd.take());
                        drop(file_path_dir.take());
                        if has_target || had_physical_data_dirs {
                            let _ = persist_receipt(true);
                        }
                        return Err(err);
                    }
                }
            }
            if sync && let Err(err) = os::fsync_directory_handle_std(&rollback, rollback_dir_fd.as_deref()) {
                drop(rollback_dir_fd.take());
                drop(file_path_dir.take());
                if has_target || had_physical_data_dirs {
                    let _ = persist_receipt(true);
                }
                return Err(err);
            }
            #[cfg(all(any(test, feature = "test-util"), not(windows)))]
            os::prepared_publication_test_hooks::run(os::prepared_publication_test_hooks::Stage::Remove, &receipt_path);
            let receipt_unlink_res =
                ignore_not_found(unlink_child_file(rollback_dir_fd.as_deref(), DELETE_QUORUM_COMMITTED_FILE, &receipt_path));
            drop(rollback_dir_fd.take());
            if let Err(err) = receipt_unlink_res {
                drop(file_path_dir.take());
                if has_target || had_physical_data_dirs {
                    let _ = persist_receipt(true);
                }
                return Err(err);
            }
            let tx_name = transaction.to_string();
            for (idx, dir_path) in prune_paths.iter().enumerate() {
                #[cfg(all(any(test, feature = "test-util"), not(windows)))]
                os::prepared_publication_test_hooks::run(os::prepared_publication_test_hooks::Stage::Remove, dir_path);
                let rmdir_res = if idx == 0 && *dir_path == rollback {
                    let r = rmdir_child_dir(file_path_dir.as_deref(), &tx_name, dir_path);
                    drop(file_path_dir.take());
                    r
                } else {
                    drop(file_path_dir.take());
                    std::fs::remove_dir(dir_path)
                };
                if let Err(err) = rmdir_res {
                    if is_dir_not_empty_error(&err) {
                        break;
                    }
                    if !is_benign_object_rmdir_error(&err) {
                        return Err(std::io::Error::other(FileAccessDeniedWithContext {
                            path: dir_path.clone(),
                            source: err,
                        }));
                    }
                }
            }
            Ok(())
        })
        .await;

        let ok = res.is_ok();
        for claim in claims {
            claim.finish(ok);
        }
        res.map(|()| true).map_err(|err| to_file_error(err).into())
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

#[cfg(all(test, unix))]
mod fused_prepare_checkpoint {
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use std::sync::{LazyLock, Mutex};

    type Hook = Box<dyn FnOnce() + Send>;
    static HOOKS: LazyLock<Mutex<HashMap<PathBuf, Hook>>> = LazyLock::new(|| Mutex::new(HashMap::new()));

    pub(super) struct Guard(PathBuf);

    impl Drop for Guard {
        fn drop(&mut self) {
            HOOKS.lock().expect("checkpoint hooks").remove(&self.0);
        }
    }

    pub(super) fn install(root: &Path, hook: impl FnOnce() + Send + 'static) -> Guard {
        assert!(
            HOOKS
                .lock()
                .expect("checkpoint hooks")
                .insert(root.to_path_buf(), Box::new(hook))
                .is_none()
        );
        Guard(root.to_path_buf())
    }

    pub(super) fn run(root: &Path) {
        let hook = HOOKS.lock().expect("checkpoint hooks").remove(root);
        if let Some(hook) = hook {
            hook();
        }
    }
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

    #[test]
    fn delete_recovery_batch_parallelism_preserves_overlapping_key_order() {
        let opts = DeleteOptions {
            recoverable_delete: true,
            ..Default::default()
        };
        for names in [
            vec!["a", "a"],
            vec!["a", "a-0", "a/child"],
            vec!["a//b", "a/b"],
            vec!["a/./b", "a/b"],
            vec!["a/../b", "b"],
        ] {
            let groups: Vec<_> = names
                .into_iter()
                .map(|name| super::super::FileInfoVersions {
                    name: name.to_owned(),
                    ..Default::default()
                })
                .collect();
            assert_eq!(
                super::super::delete_version_batch_concurrency(&groups, &opts),
                1,
                "overlapping or noncanonical keys retain serial execution"
            );
        }
        let groups: Vec<_> = ["a/one", "a/two"]
            .into_iter()
            .map(|name| super::super::FileInfoVersions {
                name: name.to_owned(),
                ..Default::default()
            })
            .collect();
        let expected = if cfg!(target_os = "linux") {
            super::super::DELETE_VERSION_PREPARE_CONCURRENCY
        } else {
            1
        };
        assert_eq!(super::super::delete_version_batch_concurrency(&groups, &opts), expected);
        for options in [
            DeleteOptions::default(),
            DeleteOptions {
                undo_write: true,
                ..opts.clone()
            },
            DeleteOptions {
                undo_delete: true,
                ..opts
            },
        ] {
            assert_eq!(
                super::super::delete_version_batch_concurrency(&groups, &options),
                1,
                "legacy and rollback ordering is unchanged"
            );
        }
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn delete_recovery_batch_prepares_independent_keys_while_one_metadata_lease_is_held() {
        let (temp, disk, info, _) = fixture().await;
        let disk = Arc::new(disk);
        let mut healthy = info.clone();
        healthy.name = "healthy".to_owned();
        let healthy_root = temp.path().join("recovery/healthy");
        fs::create_dir_all(&healthy_root).await.expect("healthy object directory");
        fs::write(healthy_root.join(STORAGE_FORMAT_FILE), test_meta(healthy))
            .await
            .expect("healthy source");
        let corrupt_root = temp.path().join("recovery/corrupt");
        fs::create_dir_all(&corrupt_root).await.expect("corrupt object directory");
        fs::write(corrupt_root.join(STORAGE_FORMAT_FILE), b"invalid xl.meta")
            .await
            .expect("corrupt source");
        let held = os::acquire_metadata_mutation_lease(&temp.path().join("recovery/object"), None).await;
        let groups = ["object", "corrupt", "healthy"]
            .into_iter()
            .map(|name| super::super::FileInfoVersions {
                name: name.to_owned(),
                versions: vec![FileInfo {
                    name: name.to_owned(),
                    ..Default::default()
                }],
                ..Default::default()
            })
            .collect();
        let worker_disk = disk.clone();
        let worker = tokio::spawn(async move {
            worker_disk
                .delete_versions_with_namespace_owner(
                    "recovery",
                    groups,
                    DeleteOptions {
                        recoverable_delete: true,
                        old_data_dir: Some(Uuid::new_v4()),
                        ..Default::default()
                    },
                    None,
                )
                .await
        });
        let prepared = tokio::time::timeout(Duration::from_secs(5), async {
            while fs::try_exists(healthy_root.join(STORAGE_FORMAT_FILE))
                .await
                .expect("inspect healthy metadata")
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        assert!(temp.path().join("recovery/object/xl.meta").exists(), "the locked source must stay intact");
        drop(held);
        let results = worker.await.expect("complete every disk result");
        assert_eq!(
            results,
            vec![None, Some(DiskError::FileCorrupt), None],
            "completion order cannot change per-key error positions"
        );
        assert!(
            prepared.is_ok(),
            "one key's metadata lease cannot serialize unrelated transaction preparation"
        );
    }

    #[tokio::test]
    async fn delete_recovery_absent_publish_refreshes_prepared_witness() {
        let (_temp, disk, _info, _original) = fixture().await;
        let root = disk.io_get_object_path("recovery", "object").expect("object path");
        fs::remove_file(root.join(STORAGE_FORMAT_FILE)).await.expect("absent object");
        let transaction = Uuid::new_v4();
        let absent = disk
            .inspect_delete_transaction("recovery", "object", transaction)
            .await
            .expect("inspect absent transaction");
        assert_eq!(absent.state, DeleteRecoveryState::Absent);
        let mut observed = absent;
        for (action, expected) in [
            (DeleteRecoveryAction::PublishCommit, DeleteRecoveryState::QuorumCommitted),
            (DeleteRecoveryAction::PublishCommit, DeleteRecoveryState::QuorumCommitted),
            (DeleteRecoveryAction::FinishCommit, DeleteRecoveryState::CommitFinished),
            (DeleteRecoveryAction::FinalizeCommit, DeleteRecoveryState::Absent),
        ] {
            observed = disk
                .settle_delete_transaction("recovery", "object", transaction, observed.fingerprint, action)
                .await
                .expect("settle absent-key witness");
            assert_eq!(observed.state, expected);
            assert!(observed.current_digest.is_none());
            assert!(!root.join(STORAGE_FORMAT_FILE).exists(), "the witness cannot resurrect metadata");
            if let Some(journal) = &observed.journal {
                assert_eq!(journal.transaction, transaction);
                assert!(journal.original_digest.is_none() && journal.target_digest.is_none());
                assert!(journal.data_dirs.is_empty());
            } else {
                assert_eq!(expected, DeleteRecoveryState::Absent);
            }
        }
        assert!(!root.join(transaction.to_string()).exists(), "finalization retires the witness");
    }

    #[tokio::test]
    async fn delete_recovery_inspection_keeps_foreign_data_markers_inert() {
        for prefix in [
            DELETE_DATA_DIR_MARKER_PREFIX,
            RESERVED_DELETE_DATA_DIR_MARKER_PREFIX,
            RECOVERY_DELETE_DATA_DIR_MARKER_PREFIX,
        ] {
            for backup in [false, true] {
                let (_temp, disk, _info, original) = fixture().await;
                let root = disk.io_get_object_path("recovery", "object").expect("object path");
                fs::remove_file(root.join(STORAGE_FORMAT_FILE))
                    .await
                    .expect("hide current metadata");
                let candidate = Uuid::new_v4();
                let owner = Uuid::new_v4();
                let data = root.join(candidate.to_string());
                fs::create_dir(&data).await.expect("coincident data directory");
                fs::write(data.join("part.1"), b"retained shard").await.expect("retain data");
                let marker = data.join(format!("{prefix}{owner}"));
                fs::write(&marker, []).await.expect("foreign transaction marker");
                if backup {
                    fs::write(data.join(STORAGE_FORMAT_FILE_BACKUP), &original)
                        .await
                        .expect("retain ambiguous backup");
                }
                let before = fs::read(&marker).await.expect("marker before inspection");
                let state = disk
                    .inspect_delete_transaction("recovery", "object", candidate)
                    .await
                    .expect("foreign data marker is not corrupt transaction evidence");
                assert_eq!(
                    state.state,
                    if backup {
                        DeleteRecoveryState::LegacyAmbiguous
                    } else {
                        DeleteRecoveryState::Absent
                    }
                );
                assert!(!state.explicit_delete_safe, "foreign ownership cannot authorize legacy removal");
                assert_eq!(fs::read(&marker).await.expect("preserved marker"), before);
                assert_eq!(fs::read(data.join("part.1")).await.expect("preserved shard"), b"retained shard");
                if backup {
                    assert_eq!(
                        fs::read(data.join(STORAGE_FORMAT_FILE_BACKUP))
                            .await
                            .expect("preserved backup"),
                        original
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn delete_recovery_inspection_rejects_unsafe_foreign_data_entries() {
        for shape in ["malformed", "nil", "directory", "symlink", "child", "metadata", "unknown"] {
            let (_temp, disk, _info, _original) = fixture().await;
            let root = disk.io_get_object_path("recovery", "object").expect("object path");
            fs::remove_file(root.join(STORAGE_FORMAT_FILE))
                .await
                .expect("hide current metadata");
            let candidate = Uuid::new_v4();
            let data = root.join(candidate.to_string());
            fs::create_dir(&data).await.expect("coincident data directory");
            let marker = format!("{RECOVERY_DELETE_DATA_DIR_MARKER_PREFIX}{}", Uuid::new_v4());
            match shape {
                "malformed" => fs::write(data.join(format!("{RECOVERY_DELETE_DATA_DIR_MARKER_PREFIX}invalid")), [])
                    .await
                    .expect("malformed marker"),
                "nil" => fs::write(data.join(format!("{RECOVERY_DELETE_DATA_DIR_MARKER_PREFIX}{}", Uuid::nil())), [])
                    .await
                    .expect("nil marker"),
                "directory" => fs::create_dir(data.join(&marker)).await.expect("marker directory"),
                "symlink" => {
                    #[cfg(unix)]
                    std::os::unix::fs::symlink(root.join(STORAGE_FORMAT_FILE), data.join(&marker)).expect("marker symlink");
                    #[cfg(not(unix))]
                    continue;
                }
                "child" => fs::create_dir(data.join(Uuid::new_v4().to_string()))
                    .await
                    .expect("child key directory"),
                "metadata" => fs::write(data.join(STORAGE_FORMAT_FILE), b"child metadata")
                    .await
                    .expect("child metadata"),
                _ => fs::write(data.join("unknown"), []).await.expect("unknown file"),
            }
            assert!(
                matches!(
                    disk.inspect_delete_transaction("recovery", "object", candidate).await,
                    Err(DiskError::FileCorrupt | DiskError::InvalidPath)
                ),
                "{shape}"
            );
            assert!(
                fs::read_dir(&data)
                    .await
                    .expect("unsafe evidence preserved")
                    .next_entry()
                    .await
                    .expect("read evidence")
                    .is_some(),
                "{shape}"
            );
        }
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
    async fn delete_recovery_known_heal_backup_inspection_preserves_unproven_owners_after_reopen() {
        for change in [
            "unchanged",
            "generation",
            "tier-object",
            "tier-version",
            "missing-free-owner",
            "replaced-backup",
        ] {
            let (_temp, disk, mut info, _) = fixture().await;
            info.metadata.insert("etag".to_owned(), "same-visible-etag".to_owned());
            info.set_object_transaction_epoch(Uuid::new_v4());
            let free_id = Uuid::new_v4();
            info.set_tier_free_version_id(&free_id.to_string());
            let mut remote = test_file_info("object", Uuid::nil(), Some(Uuid::new_v4()), None);
            remote.mod_time = info.mod_time;
            remote.transition_status = rustfs_filemeta::TRANSITION_COMPLETE.to_owned();
            remote.transition_tier = "WARM".to_owned();
            remote.transitioned_objname = "remote/original".to_owned();
            remote.transition_version_state = rustfs_filemeta::TransitionVersionState::Exact;
            remote.transition_version = Some("original-provider-version".to_owned());
            rustfs_utils::http::metadata_compat::insert_str(
                &mut remote.metadata,
                rustfs_utils::http::metadata_compat::SUFFIX_TRANSITION_TIER_DESTINATION_ID,
                "ab".repeat(32),
            );
            let metadata = |remote: FileInfo, current: FileInfo| {
                let mut meta = FileMeta::new();
                meta.add_version(remote).expect("seed transitioned predecessor");
                meta.add_version(current)
                    .expect("commit local replacement and its free-version owner");
                meta
            };
            let original = metadata(remote.clone(), info.clone());
            let (_, free) = original.find_version(Some(free_id)).expect("original free-version owner");
            assert!(free.free_version(), "fixture must retain a separate remote cleanup owner");
            let backup_bytes = original.marshal_msg().expect("encode historical backup");
            let transaction = inline_metadata_rollback_dir(Uuid::nil(), &original);
            let root = disk.io_get_object_path("recovery", "object").expect("object path");
            let backup_path = root.join(transaction.to_string()).join(STORAGE_FORMAT_FILE_BACKUP);
            fs::create_dir_all(backup_path.parent().expect("backup parent"))
                .await
                .expect("create backup directory");
            fs::write(&backup_path, &backup_bytes)
                .await
                .expect("retain historical backup");

            let mut current = info.clone();
            match change {
                "generation" => current.set_object_transaction_epoch(Uuid::new_v4()),
                "tier-object" => remote.transitioned_objname = "remote/successor".to_owned(),
                "tier-version" => remote.transition_version = Some("successor-provider-version".to_owned()),
                _ => {}
            }
            let mut current_meta = metadata(remote, current);
            if change == "missing-free-owner" {
                current_meta
                    .versions
                    .retain(|version| version.header.version_id != Some(free_id));
            } else if change == "replaced-backup" {
                let replacement = root.join("replacement-backup");
                fs::write(&replacement, &backup_bytes).await.expect("write replacement inode");
                fs::rename(replacement, &backup_path)
                    .await
                    .expect("replace backup without changing its bytes");
            }
            let current_bytes = current_meta.marshal_msg().expect("encode current metadata");
            fs::write(root.join(STORAGE_FORMAT_FILE), &current_bytes)
                .await
                .expect("publish readable current object");
            let visible = disk
                .read_version(
                    "",
                    "recovery",
                    "object",
                    "",
                    &ReadOptions {
                        read_data: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("current object remains physically readable");
            assert_eq!(visible.data, info.data, "{change}: current payload");
            assert_eq!(visible.mod_time, info.mod_time, "{change}: unchanged modification time");
            assert_eq!(visible.metadata.get("etag"), info.metadata.get("etag"), "{change}: unchanged ETag");

            let before = disk
                .inspect_delete_transaction("recovery", "object", transaction)
                .await
                .expect("query known backup without inferring commit authority");
            let endpoint = disk.endpoint();
            drop(disk);
            let reopened = LocalDisk::new(&endpoint, false).await.expect("reopen original disk endpoint");
            // Linux I/O paths belong to the open disk's mount descriptor.
            let root = reopened
                .io_get_object_path("recovery", "object")
                .expect("reopened object path");
            let backup_path = root.join(transaction.to_string()).join(STORAGE_FORMAT_FILE_BACKUP);
            let after = reopened
                .inspect_delete_transaction("recovery", "object", transaction)
                .await
                .expect("known historical backup remains queryable after reopen");
            assert_eq!(after, before, "{change}: inspection must preserve the complete evidence snapshot");
            assert!(after.journal.is_none(), "{change}: no historical success or delete authority exists");
            assert_eq!(after.backup_digest, Some(digest(&backup_bytes)), "{change}: retained C-class evidence");
            assert!(!after.explicit_delete_safe, "{change}: current data forbids legacy discard");
            let expected_state = if matches!(change, "unchanged" | "replaced-backup") {
                DeleteRecoveryState::Absent
            } else {
                DeleteRecoveryState::LegacyAmbiguous
            };
            assert_eq!(
                after.state, expected_state,
                "{change}: delete-owner semantics remain separate from C classification"
            );
            let page = reopened
                .discover_delete_transactions("recovery", Some("object"), None, 128)
                .await
                .expect("discover only delete-recovery work");
            assert_eq!(
                page.transactions.iter().any(|id| id.transaction == transaction),
                expected_state == DeleteRecoveryState::LegacyAmbiguous,
                "{change}: a live backup must not become a delete blocker"
            );
            assert_eq!(fs::read(&backup_path).await.expect("retained backup bytes"), backup_bytes, "{change}");
            assert_eq!(
                fs::read(root.join(STORAGE_FORMAT_FILE))
                    .await
                    .expect("retained current bytes"),
                current_bytes,
                "{change}"
            );
            let backup = FileMeta::load(&backup_bytes).expect("decode retained owner evidence");
            assert_eq!(
                backup.find_version(Some(free_id)).expect("retained free-version owner").1,
                free,
                "{change}: ordinary-object readability cannot release remote ownership"
            );
        }
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

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn delete_recovery_cancelled_finalize_pins_root_after_last_disk_owner_drops() {
        use crate::disk::os::prepared_publication_test_hooks as hooks;
        use futures::FutureExt;

        struct ReleaseOnDrop(Option<std::sync::mpsc::Sender<()>>);
        impl Drop for ReleaseOnDrop {
            fn drop(&mut self) {
                if let Some(release) = self.0.take() {
                    let _ = release.send(());
                }
            }
        }

        let (temp, native, _info, _original) = fixture().await;
        let root_file = Arc::downgrade(native.publication_root.directory());
        assert!(Arc::ptr_eq(native.publication_root.directory(), &native.mount_lease));
        // The constructor's idle GC task also owns this FD. Drain that existing
        // task so it cannot mask a missing pin in the Finalize callback.
        let exit = native.exit_signal.as_ref().expect("fixture cleanup lifecycle");
        exit.send(()).expect("stop the existing idle cleanup task");
        tokio::time::timeout(Duration::from_secs(5), async {
            while exit.receiver_count() != 0 || root_file.strong_count() != 3 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("only LocalDisk's publication, mount and I/O backend fields may retain the initial root FD");
        let transaction = Uuid::new_v4();
        let applied = apply(&native, transaction).await;
        let committed = native
            .settle_delete_transaction(
                "recovery",
                "object",
                transaction,
                applied.fingerprint,
                DeleteRecoveryAction::PublishCommit,
            )
            .await
            .expect("publish actual delete decision");
        let finished = native
            .settle_delete_transaction(
                "recovery",
                "object",
                transaction,
                committed.fingerprint,
                DeleteRecoveryAction::FinishCommit,
            )
            .await
            .expect("finish actual committed deletion");
        assert_eq!(finished.state, DeleteRecoveryState::CommitFinished);
        let physical_object = temp.path().join("recovery/object");
        let physical_rollback = physical_object.join(transaction.to_string());
        assert!(physical_rollback.join(PREPARED).is_file());
        assert!(physical_rollback.join(DELETE_QUORUM_COMMITTED_FILE).is_file());
        let io_root = native.io_root().to_path_buf();
        assert!(io_root.starts_with("/proc/self/fd"), "exercise the real borrowed Linux FD path");
        let canonical_root = std::fs::canonicalize(temp.path()).expect("actual root identity");
        let fd_alias = io_root.components().collect::<PathBuf>();
        let io_rollback = native
            .io_get_object_path("recovery", &format!("object/{transaction}"))
            .expect("actual IO rollback path");
        let receipt_path = io_rollback.join(DELETE_QUORUM_COMMITTED_FILE);
        let (entered, reached) = tokio::sync::oneshot::channel();
        let (release, resumed) = std::sync::mpsc::channel::<()>();
        let release = ReleaseOnDrop(Some(release));
        let hook = hooks::install_at(hooks::Stage::Remove, &io_rollback.join(PREPARED), move || {
            let _ = entered.send(());
            let _ = resumed.recv();
        });
        let mut native = Some(Arc::new(native));
        let weak = Arc::downgrade(native.as_ref().expect("fixture disk owner"));
        let finalizing = native.as_ref().expect("fixture disk owner").clone();
        let mut task = Some(tokio::spawn(async move {
            finalizing
                .settle_delete_transaction(
                    "recovery",
                    "object",
                    transaction,
                    finished.fingerprint,
                    DeleteRecoveryAction::FinalizeCommit,
                )
                .await
        }));
        let observations = std::panic::AssertUnwindSafe(async {
            tokio::time::timeout(Duration::from_secs(10), reached)
                .await
                .expect("actual Finalize remove callback must pause")
                .expect("actual physical Remove hook");
            let cancelling = task.take().expect("live finalize waiter");
            cancelling.abort();
            assert!(cancelling.await.expect_err("cancel the real waiter").is_cancelled());
            drop(native.take());
            assert!(weak.upgrade().is_none(), "no test or cancelled future may retain a LocalDisk");
            assert_eq!(
                root_file.strong_count(),
                1,
                "only the actual callback may retain the root FD after disk retirement"
            );
            assert_eq!(
                std::fs::read_link(&fd_alias).expect("the physical callback must still own its root FD"),
                canonical_root,
                "the borrowed FD alias must keep the same actual disk identity"
            );
            assert!(
                physical_rollback.join(PREPARED).is_file(),
                "paused callback has not removed the actual prepared record"
            );
            assert!(
                physical_rollback.join(DELETE_QUORUM_COMMITTED_FILE).is_file(),
                "the actual final witness still exists before resume"
            );
        })
        .catch_unwind()
        .await;
        if let Some(task) = task.take() {
            task.abort();
            let _ = task.await;
        }
        drop(native.take());
        drop(release);
        drop(hook);
        // This path-only lease does not hold a root handle. Acquiring it proves
        // the actual callback, including its inline inspection, has returned.
        let drained = tokio::time::timeout(
            Duration::from_secs(5),
            os::acquire_namespace_mutation_lease_with_owner(&receipt_path, None),
        )
        .await
        .map(drop);
        if let Err(panic) = observations {
            std::panic::resume_unwind(panic);
        }
        drained.expect("cancelled physical Finalize callback must finish");
        for name in [TARGET, PREPARED, FINISHED, DELETE_QUORUM_COMMITTED_FILE] {
            assert!(
                !physical_rollback.join(name).exists(),
                "actual original-volume proof must be reclaimed: {name}"
            );
        }
        assert!(!physical_rollback.exists(), "the real rollback directory is pruned after proof removal");
        assert!(!physical_object.exists(), "completed deletion leaves no original-volume object residue");
        assert!(temp.path().join("recovery").is_dir(), "finalization must preserve its original volume");
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
    async fn fused_delete_recovery_survives_interrupted_prepare_and_publication() {
        for published in [false, true] {
            let (_temp, disk, _info, original) = fixture().await;
            let transaction = Uuid::new_v4();
            let prepared = disk
                .try_fused_recoverable_delete_version(
                    "recovery",
                    "object",
                    &request(),
                    &DeleteOptions {
                        recoverable_delete: true,
                        old_data_dir: Some(transaction),
                        ..Default::default()
                    },
                    transaction,
                    None,
                )
                .await
                .expect("prepare fused deletion");
            assert_eq!(prepared, Some(()), "the test must exercise the fused path");

            if published {
                let (_, sole, target_absent) = disk
                    .try_publish_prepared_delete_local("recovery", "object", transaction, None)
                    .await
                    .expect("publish fused receipt")
                    .expect("fused publication must apply");
                assert!(sole && target_absent);
            }

            let mut state = disk
                .inspect_delete_transaction("recovery", "object", transaction)
                .await
                .expect("inspect persisted state after interruption");
            if published {
                assert_eq!(state.state, DeleteRecoveryState::QuorumCommitted);
                for action in [DeleteRecoveryAction::FinishCommit, DeleteRecoveryAction::FinalizeCommit] {
                    state = disk
                        .settle_delete_transaction("recovery", "object", transaction, state.fingerprint, action)
                        .await
                        .expect("finish published deletion through recovery");
                }
                assert_eq!(state.state, DeleteRecoveryState::Absent);
                assert!(disk.read_all("recovery", "object/xl.meta").await.is_err());
            } else {
                assert_eq!(state.state, DeleteRecoveryState::NewPrepared);
                state = disk
                    .settle_delete_transaction(
                        "recovery",
                        "object",
                        transaction,
                        state.fingerprint,
                        DeleteRecoveryAction::Rollback,
                    )
                    .await
                    .expect("roll back uncommitted fused deletion");
                assert_eq!(state.state, DeleteRecoveryState::Absent);
                assert_eq!(
                    disk.read_all("recovery", "object/xl.meta")
                        .await
                        .expect("restore original metadata")
                        .as_ref(),
                    original
                );
            }
        }
    }

    #[tokio::test]
    async fn fused_delete_recovery_preserves_or_removes_shard_after_interruption() {
        use crate::erasure::coding::BitrotWriter;

        for mode in 0..3 {
            let published = mode != 0;
            let (_temp, disk, _info, _) = fixture().await;
            let data_dir = Uuid::new_v4();
            let payload = Bytes::from_static(b"original shard");
            let mut info = test_file_info("object", Uuid::nil(), Some(data_dir), None);
            info.size = i64::try_from(payload.len()).expect("payload size");
            info.erasure.block_size = payload.len();
            info.parts[0].size = payload.len();
            info.parts[0].actual_size = info.size;
            let original = test_meta(info.clone());
            let mut writer = BitrotWriter::new(
                std::io::Cursor::new(Vec::new()),
                info.erasure.shard_size(),
                HashAlgorithm::HighwayHash256S,
            );
            writer.write(&payload).await.expect("encode shard");
            writer.shutdown().await.expect("finish shard encoding");
            let encoded = writer.into_inner().into_inner();
            let root = disk.io_get_object_path("recovery", "object").expect("object path");
            fs::write(root.join(STORAGE_FORMAT_FILE), &original)
                .await
                .expect("write shard metadata");
            let shard_dir = root.join(data_dir.to_string());
            fs::create_dir_all(&shard_dir).await.expect("create shard directory");
            fs::write(shard_dir.join("part.1"), &encoded)
                .await
                .expect("write original shard");
            let transaction = Uuid::new_v4();
            assert_eq!(
                disk.try_fused_recoverable_delete_version(
                    "recovery",
                    "object",
                    &request(),
                    &DeleteOptions {
                        recoverable_delete: true,
                        old_data_dir: Some(transaction),
                        ..Default::default()
                    },
                    transaction,
                    None,
                )
                .await
                .expect("prepare fused shard deletion"),
                Some(()),
                "the test must exercise the fused path"
            );
            if published {
                let (journal, sole, target_absent) = disk
                    .try_publish_prepared_delete_local("recovery", "object", transaction, None)
                    .await
                    .expect("publish fused receipt")
                    .expect("fused publication must apply");
                assert!(sole && target_absent);
                if mode == 2 {
                    assert!(
                        disk.try_finish_and_finalize_prepared_delete_local("recovery", "object", transaction, &journal, None)
                            .await
                            .expect("finish fused deletion"),
                        "the fused finalization must apply"
                    );
                    assert!(!shard_dir.exists(), "fused finalization must remove the shard");
                    assert!(!root.join(STORAGE_FORMAT_FILE).exists());
                    assert!(!root.join(transaction.to_string()).exists());
                    continue;
                }
            }
            let mut state = disk
                .inspect_delete_transaction("recovery", "object", transaction)
                .await
                .expect("inspect persisted state");
            let actions = if published {
                vec![DeleteRecoveryAction::FinishCommit, DeleteRecoveryAction::FinalizeCommit]
            } else {
                vec![DeleteRecoveryAction::Rollback]
            };
            for action in actions {
                state = disk
                    .settle_delete_transaction("recovery", "object", transaction, state.fingerprint, action)
                    .await
                    .expect("recover shard deletion");
            }
            assert_eq!(state.state, DeleteRecoveryState::Absent);
            if published {
                assert!(!shard_dir.exists(), "committed deletion must remove the shard");
            } else {
                assert_eq!(fs::read(root.join(STORAGE_FORMAT_FILE)).await.expect("restore metadata"), original);
                assert_eq!(fs::read(shard_dir.join("part.1")).await.expect("restore shard"), encoded);
            }
        }
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
    async fn delete_marker_collision_rejects_new_journals_and_recovers_legacy_proof() {
        let (temp, disk, _info, original) = fixture().await;
        let root = disk.io_get_object_path("recovery", "object").expect("object path");
        let staged_child = temp.path().join("child-fixture");
        fs::rename(&root, &staged_child).await.expect("move intact object");
        fs::create_dir(&root).await.expect("create parent prefix");
        fs::rename(staged_child, root.join(STORAGE_FORMAT_FILE))
            .await
            .expect("seed an independent object named object/xl.meta");
        let transaction = Uuid::new_v4();
        let marker = FileInfo {
            volume: "recovery".to_owned(),
            name: "object".to_owned(),
            version_id: Some(Uuid::new_v4()),
            deleted: true,
            mod_time: Some(OffsetDateTime::now_utc()),
            ..Default::default()
        };
        disk.delete_version_recoverable(
            "recovery",
            "object",
            marker.clone(),
            true,
            DeleteOptions {
                old_data_dir: Some(transaction),
                ..Default::default()
            },
        )
        .await
        .expect_err("a child xl.meta directory cannot be replaced by a marker");
        assert!(!root.join(transaction.to_string()).exists(), "rejection must precede journal publication");

        // Reproduce the durable evidence written by releases that prepared a
        // missing-object marker before attempting to replace the directory.
        disk.prepare_delete_recovery(
            "recovery",
            "object",
            None,
            &[marker],
            None,
            DeletePreparationContext {
                transaction,
                bucket_incarnation: None,
                namespace_owner: None,
            },
        )
        .await
        .expect("seed legacy prepared marker");
        let journal_path = root.join(transaction.to_string()).join(PREPARED);
        let journal_bytes = fs::read(&journal_path).await.expect("prepared journal");
        let mut incompatible: DeleteRecoveryJournal = serde_json::from_slice(&journal_bytes).expect("journal");
        incompatible.original_digest = Some([7; 32]);
        fs::write(&journal_path, serde_json::to_vec(&incompatible).expect("serialize journal"))
            .await
            .expect("write incompatible evidence");
        assert!(
            disk.inspect_delete_transaction("recovery", "object", transaction)
                .await
                .is_err(),
            "a journal that claims existing metadata cannot ignore a directory collision"
        );
        fs::write(&journal_path, journal_bytes)
            .await
            .expect("restore exact legacy proof");
        let observed = disk
            .inspect_delete_transaction("recovery", "object", transaction)
            .await
            .expect("inspect legacy proof");
        assert_eq!(observed.state, DeleteRecoveryState::NewPrepared);
        let recovered = disk
            .settle_delete_transaction("recovery", "object", transaction, observed.fingerprint, DeleteRecoveryAction::Rollback)
            .await
            .expect("rollback legacy marker without unlinking the child directory");
        assert_eq!(recovered.state, DeleteRecoveryState::Absent);
        assert!(!root.join(transaction.to_string()).exists());
        assert_eq!(
            disk.read_all("recovery", "object/xl.meta/xl.meta")
                .await
                .expect("child metadata")
                .as_ref(),
            original
        );
        assert!(
            disk.read_version("", "recovery", "object/xl.meta", "", &Default::default())
                .await
                .is_ok()
        );
        assert!(
            discover_all_keys(&disk, Some("object")).await.is_empty(),
            "no proof may keep replacement heal pending"
        );
    }

    // Data movement copies a delete marker onto a target that holds no
    // metadata yet. Rolling back that sub-quorum write removes the marker it
    // published, and must not report the backup it never wrote as missing:
    // that NotFound read as the first error of rustfs/backlog#2873 defect F.
    #[tokio::test]
    async fn delete_recovery_rollback_of_marker_on_missing_object_is_clean() {
        for applied in [true, false] {
            let (_temp, disk, _info, _original) = fixture().await;
            let root = disk.io_get_object_path("recovery", "object").expect("object path");
            fs::remove_file(root.join(STORAGE_FORMAT_FILE)).await.expect("absent object");
            let transaction = Uuid::new_v4();
            let marker = FileInfo {
                volume: "recovery".to_owned(),
                name: "object".to_owned(),
                version_id: Some(Uuid::new_v4()),
                deleted: true,
                mod_time: Some(OffsetDateTime::now_utc()),
                ..Default::default()
            };
            disk.delete_version_recoverable(
                "recovery",
                "object",
                marker,
                true,
                DeleteOptions {
                    old_data_dir: Some(transaction),
                    ..Default::default()
                },
            )
            .await
            .expect("prepared marker application");
            assert!(root.join(STORAGE_FORMAT_FILE).is_file());
            if !applied {
                // A drive that prepared the journal but never published.
                fs::remove_file(root.join(STORAGE_FORMAT_FILE)).await.unwrap();
            }
            let observed = disk
                .inspect_delete_transaction("recovery", "object", transaction)
                .await
                .expect("inspect prepared marker");
            assert_eq!(observed.state, DeleteRecoveryState::NewPrepared);
            assert!(observed.journal.as_ref().unwrap().original_digest.is_none());

            let restored = disk
                .settle_delete_transaction(
                    "recovery",
                    "object",
                    transaction,
                    observed.fingerprint,
                    DeleteRecoveryAction::Rollback,
                )
                .await
                .expect("rollback of a marker published on a missing object");
            assert_eq!(restored.state, DeleteRecoveryState::Absent);
            assert!(
                !root.join(STORAGE_FORMAT_FILE).exists(),
                "rollback removes the uncommitted marker (applied={applied})"
            );
            assert!(!root.join(transaction.to_string()).exists());
            // The NotFound this rename logged read as the first error of the
            // failed write. Assert the attempt rather than the log, which
            // tracing's process-wide callsite cache can hide under cargo test.
            let backup = root.join(transaction.to_string()).join(STORAGE_FORMAT_FILE_BACKUP);
            assert!(
                !crate::disk::local::DELETE_ROLLBACK_BACKUP_RESTORES
                    .lock()
                    .unwrap()
                    .contains(&backup),
                "a rollback must not restore a backup it never wrote (applied={applied})"
            );
        }
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
    async fn delete_recovery_batch_inline_matches_sequential_and_survives_settlement() {
        for commit in [false, true] {
            let (_temp, disk, _, _) = fixture().await;
            let mut original = FileMeta::new();
            let mut requests = Vec::new();
            for id in 1_u128..=64 {
                let mut info = test_file_info(
                    "object",
                    Uuid::from_u128(id),
                    Some(Uuid::from_u128(1000 + id)),
                    Some(Bytes::from(vec![u8::try_from(id).expect("small id"); 9 * 1024])),
                );
                info.set_inline_data();
                original.add_version(info).expect("seed inline version");
                if id <= 48 {
                    requests.push(FileInfo {
                        name: "object".into(),
                        version_id: Some(Uuid::from_u128(id)),
                        ..Default::default()
                    });
                }
            }
            let original_bytes = original.marshal_msg().expect("encode original");
            let meta_path = disk.io_get_object_path("recovery", "object/xl.meta").expect("metadata path");
            fs::write(&meta_path, &original_bytes).await.expect("persist version chain");
            let mut expected = original;
            for request in &requests {
                if let Some(dir) = expected.delete_version(request).expect("sequential delete") {
                    expected
                        .data
                        .remove(vec![request.version_id.expect("explicit version"), dir])
                        .expect("sequential inline removal");
                }
            }
            let transaction = Uuid::new_v4();
            let errors = disk
                .delete_versions_recoverable(
                    "recovery",
                    vec![FileInfoVersions {
                        name: "object".into(),
                        volume: "recovery".into(),
                        versions: requests,
                        ..Default::default()
                    }],
                    DeleteOptions {
                        old_data_dir: Some(transaction),
                        ..Default::default()
                    },
                )
                .await;
            assert_eq!(errors, vec![None]);
            let applied = disk.read_all("recovery", "object/xl.meta").await.expect("applied metadata");
            assert_eq!(applied.as_ref(), expected.marshal_msg().expect("expected metadata"));
            let target = disk
                .read_all("recovery", &format!("object/{transaction}/{TARGET}"))
                .await
                .expect("prepared target");
            assert_eq!(applied, target, "publication must use the exact prepared target");
            let current = FileMeta::load(&applied).expect("load survivors");
            assert_eq!(current.versions.len(), 16);
            for id in 49_u128..=64 {
                let fi = current
                    .into_fileinfo("recovery", "object", &Uuid::from_u128(id).to_string(), true, false, true)
                    .expect("surviving version");
                assert_eq!(
                    fi.data.expect("surviving inline body").as_ref(),
                    vec![u8::try_from(id).expect("small id"); 9 * 1024]
                );
            }
            let observed = disk
                .inspect_delete_transaction("recovery", "object", transaction)
                .await
                .expect("inspect prepared batch");
            if commit {
                let committed = disk
                    .settle_delete_transaction(
                        "recovery",
                        "object",
                        transaction,
                        observed.fingerprint,
                        DeleteRecoveryAction::PublishCommit,
                    )
                    .await
                    .expect("publish commit");
                let finished = disk
                    .settle_delete_transaction(
                        "recovery",
                        "object",
                        transaction,
                        committed.fingerprint,
                        DeleteRecoveryAction::FinishCommit,
                    )
                    .await
                    .expect("finish cleanup");
                assert_eq!(finished.state, DeleteRecoveryState::CommitFinished);
                assert_eq!(fs::read(&meta_path).await.expect("committed survivors"), applied.as_ref());
            } else {
                let restored = disk
                    .settle_delete_transaction(
                        "recovery",
                        "object",
                        transaction,
                        observed.fingerprint,
                        DeleteRecoveryAction::Rollback,
                    )
                    .await
                    .expect("rollback batch");
                assert_eq!(restored.state, DeleteRecoveryState::Absent);
                assert_eq!(fs::read(&meta_path).await.expect("restored metadata"), original_bytes);
            }
        }
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
    async fn delete_recovery_fingerprint_ignores_prefix_child_keys() {
        // `object/child` is an independent key that only shares this key's
        // directory as a prefix. Its delete removes `object/child/` under its
        // own exact-key lock, so it can vanish (or appear) between this key's
        // inspection and settlement.
        // A content-addressed child key may look like a UUID in a spelling
        // that this protocol never writes.
        let names = ["child".to_owned(), Uuid::new_v4().simple().to_string()];
        for (child_appears, name) in [false, true]
            .into_iter()
            .flat_map(|appears| names.iter().map(move |name| (appears, name)))
        {
            let (_temp, disk, _info, _original) = fixture().await;
            let root = disk.io_get_object_path("recovery", "object").expect("object path");
            let child = root.join(name);
            if !child_appears {
                std::fs::create_dir_all(&child).expect("create child key prefix");
                std::fs::write(child.join(STORAGE_FORMAT_FILE), b"child").expect("write child key metadata");
            }
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
            let observed = disk
                .inspect_delete_transaction("recovery", "object", transaction)
                .await
                .expect("GC inspection");
            if child_appears {
                std::fs::create_dir_all(&child).expect("create child key prefix");
                std::fs::write(child.join(STORAGE_FORMAT_FILE), b"child").expect("write child key metadata");
            } else {
                std::fs::remove_dir_all(&child).expect("concurrent child key delete");
            }
            let settled = disk
                .settle_delete_transaction(
                    "recovery",
                    "object",
                    transaction,
                    observed.fingerprint,
                    DeleteRecoveryAction::FinalizeCommit,
                )
                .await
                .expect("a child key change is not a change of this key's protocol evidence");
            assert_eq!(settled.state, DeleteRecoveryState::Absent);
            assert!(!root.join(transaction.to_string()).exists(), "transaction proof must be finalized");
            assert_eq!(root.exists(), child_appears, "only a live child key may keep the prefix directory");
            if child_appears {
                assert_eq!(std::fs::read(child.join(STORAGE_FORMAT_FILE)).expect("child key survives"), b"child");
            }
        }
    }

    #[tokio::test]
    async fn delete_recovery_settlement_ignores_non_canonical_data_dir_spelling() {
        // `object/<simple hex>` names a child key, even when it spells a data
        // dir referenced by this key's metadata.
        for legacy in [false, true] {
            let (_temp, disk, _info, _original) = fixture().await;
            let root = disk.io_get_object_path("recovery", "object").expect("object path");
            let data_dir = Uuid::new_v4();
            let metadata = test_meta(test_file_info("object", Uuid::nil(), Some(data_dir), None));
            let alias = root.join(data_dir.simple().to_string());
            let transaction = Uuid::new_v4();
            if legacy {
                legacy_fixture(&disk, &metadata, transaction).await;
                std::fs::create_dir(&alias).expect("create child key prefix");
                let state = disk
                    .inspect_delete_transaction("recovery", "object", transaction)
                    .await
                    .expect("inspect legacy residue");
                assert_eq!(state.state, DeleteRecoveryState::LegacyAmbiguous);
                assert!(state.explicit_delete_safe);
                let cleared = disk
                    .settle_delete_transaction(
                        "recovery",
                        "object",
                        transaction,
                        state.fingerprint,
                        DeleteRecoveryAction::DiscardLegacy,
                    )
                    .await
                    .expect("discard legacy residue");
                assert_eq!(cleared.state, DeleteRecoveryState::Absent);
            } else {
                std::fs::write(root.join(STORAGE_FORMAT_FILE), &metadata).expect("write data dir metadata");
                std::fs::create_dir(root.join(data_dir.to_string())).expect("create data dir");
                std::fs::write(root.join(data_dir.to_string()).join("part.1"), b"x").expect("write part");
                std::fs::create_dir(&alias).expect("create child key prefix");
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
                assert!(!root.join(data_dir.to_string()).exists(), "the canonical data dir is deleted");
            }
            assert!(alias.is_dir(), "legacy={legacy}: a non-canonical spelling is not this key's data dir");
        }
    }

    #[tokio::test]
    async fn delete_recovery_fingerprint_still_rejects_own_evidence_changes() {
        #[derive(Clone, Copy, Debug)]
        enum Change {
            RootFile,
            DataDirPart,
            DataDir,
        }
        for change in [Change::RootFile, Change::DataDirPart, Change::DataDir] {
            for finalize in [false, true] {
                let (_temp, disk, _info, _original) = fixture().await;
                let root = disk.io_get_object_path("recovery", "object").expect("object path");
                // An unreferenced data dir that exists before inspection.
                let data_dir = root.join(Uuid::new_v4().to_string());
                std::fs::create_dir(&data_dir).expect("create data dir");
                let transaction = Uuid::new_v4();
                let mut observed = apply(&disk, transaction).await;
                if finalize {
                    let committed = disk
                        .settle_delete_transaction(
                            "recovery",
                            "object",
                            transaction,
                            observed.fingerprint,
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
                    observed = disk
                        .inspect_delete_transaction("recovery", "object", transaction)
                        .await
                        .expect("GC inspection");
                }
                match change {
                    Change::RootFile => std::fs::write(root.join("part.1"), b"x").expect("write root file"),
                    Change::DataDirPart => std::fs::write(data_dir.join("part.1"), b"x").expect("write data dir part"),
                    Change::DataDir => std::fs::create_dir(root.join(Uuid::new_v4().to_string())).expect("create data dir"),
                }
                let action = if finalize {
                    DeleteRecoveryAction::FinalizeCommit
                } else {
                    DeleteRecoveryAction::PublishCommit
                };
                assert_eq!(
                    disk.settle_delete_transaction("recovery", "object", transaction, observed.fingerprint, action)
                        .await
                        .expect_err("a change of this key's own evidence must invalidate the snapshot"),
                    DiskError::OutdatedXLMeta,
                    "{change:?} before {}",
                    if finalize { "FinalizeCommit" } else { "PublishCommit" }
                );
            }
        }
    }

    #[tokio::test]
    async fn delete_recovery_reclaims_legacy_reservation_after_commit_and_snapshot_release() {
        for batch in [false, true] {
            for snapshot in [false, true] {
                let (temp, disk, _, _) = fixture().await;
                let data_dir = Uuid::new_v4();
                let transaction = Uuid::new_v4();
                let root = disk.io_get_object_path("recovery", "object").expect("object path");
                let data = root.join(data_dir.to_string());
                fs::create_dir(&data).await.expect("external data directory");
                fs::write(data.join("part.1"), b"x").await.expect("external payload");
                let reservation = data.join(format!("{RESERVED_DELETE_DATA_DIR_MARKER_PREFIX}{}", Uuid::new_v4()));
                fs::write(&reservation, []).await.expect("interrupted legacy reservation");
                fs::write(
                    root.join(STORAGE_FORMAT_FILE),
                    test_meta(test_file_info("object", Uuid::nil(), Some(data_dir), None)),
                )
                .await
                .expect("live metadata still owns reserved data");
                let path = format!("object/{data_dir}");
                let lease = if snapshot {
                    Some(disk.acquire_snapshot_lease("recovery", &path).await.expect("active reader"))
                } else {
                    None
                };
                let options = DeleteOptions {
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
                                versions: vec![request()],
                                ..Default::default()
                            }],
                            options,
                        )
                        .await;
                    assert_eq!(errors, vec![None]);
                } else {
                    disk.delete_version_recoverable("recovery", "object", request(), false, options)
                        .await
                        .expect("apply single delete");
                }
                let applied = disk
                    .inspect_delete_transaction("recovery", "object", transaction)
                    .await
                    .expect("applied");
                let committed = disk
                    .settle_delete_transaction(
                        "recovery",
                        "object",
                        transaction,
                        applied.fingerprint,
                        DeleteRecoveryAction::PublishCommit,
                    )
                    .await
                    .expect("publish certified decision");
                let finish = disk
                    .settle_delete_transaction(
                        "recovery",
                        "object",
                        transaction,
                        committed.fingerprint,
                        DeleteRecoveryAction::FinishCommit,
                    )
                    .await;
                if let Some(lease) = lease {
                    assert_eq!(finish.expect_err("snapshot retains payload"), DiskError::DiskOngoingReq);
                    assert_eq!(fs::read(data.join("part.1")).await.expect("snapshot payload"), b"x");
                    assert!(reservation.is_file());
                    disk.release_snapshot_lease("recovery", &path, lease)
                        .await
                        .expect("release replays safe cleanup");
                } else {
                    assert_eq!(
                        finish.expect("old reservation must not block commit").state,
                        DeleteRecoveryState::CommitFinished
                    );
                }
                assert!(!data.exists(), "batch={batch}, snapshot={snapshot}");
                drop(disk);
                let endpoint = Endpoint::try_from(temp.path().to_str().expect("UTF-8 disk path")).expect("endpoint");
                let disk = LocalDisk::new(&endpoint, false).await.expect("reopen after cleanup");
                let root = disk.io_get_object_path("recovery", "object").expect("reopened object path");
                let observed = disk
                    .inspect_delete_transaction("recovery", "object", transaction)
                    .await
                    .expect("replay inspection");
                let finished = disk
                    .settle_delete_transaction(
                        "recovery",
                        "object",
                        transaction,
                        observed.fingerprint,
                        DeleteRecoveryAction::FinishCommit,
                    )
                    .await
                    .expect("idempotent finish after restart");
                disk.settle_delete_transaction(
                    "recovery",
                    "object",
                    transaction,
                    finished.fingerprint,
                    DeleteRecoveryAction::FinalizeCommit,
                )
                .await
                .expect("finalize converged transaction");
                assert!(!root.exists(), "committed deletion leaves no physical residue");
            }
        }
    }

    #[tokio::test]
    async fn delete_recovery_legacy_reservation_never_authorizes_unrelated_data() {
        for shape in [
            "no-proof",
            "foreign-commit",
            "nil",
            "invalid",
            "directory",
            "child",
            "metadata",
        ] {
            let (_temp, disk, _, _) = fixture().await;
            let transaction = Uuid::new_v4();
            let data_dir = Uuid::new_v4();
            let path = format!("object/{data_dir}");
            let data = disk.io_get_object_path("recovery", &path).expect("data path");
            fs::create_dir(&data).await.expect("data directory");
            fs::write(data.join("part.1"), b"preserve").await.expect("payload");
            let old = format!("{RESERVED_DELETE_DATA_DIR_MARKER_PREFIX}{}", Uuid::new_v4());
            fs::write(data.join(&old), []).await.expect("old reservation");
            if shape != "no-proof" {
                fs::write(data.join(format!("{RECOVERY_DELETE_DATA_DIR_MARKER_PREFIX}{transaction}")), [])
                    .await
                    .expect("current proof");
            }
            match shape {
                "foreign-commit" => fs::write(data.join(format!("{DELETE_DATA_DIR_MARKER_PREFIX}{}", Uuid::new_v4())), [])
                    .await
                    .expect("foreign proof"),
                "nil" => fs::write(data.join(format!("{RESERVED_DELETE_DATA_DIR_MARKER_PREFIX}{}", Uuid::nil())), [])
                    .await
                    .expect("nil reservation"),
                "invalid" => fs::write(data.join(format!("{RESERVED_DELETE_DATA_DIR_MARKER_PREFIX}invalid")), [])
                    .await
                    .expect("malformed reservation"),
                "directory" => {
                    fs::remove_file(data.join(&old)).await.expect("replace marker");
                    fs::create_dir(data.join(&old)).await.expect("marker is not a regular file");
                }
                "child" => fs::create_dir(data.join("child")).await.expect("UUID-prefix descendant"),
                "metadata" => fs::write(data.join(STORAGE_FORMAT_FILE), b"child metadata")
                    .await
                    .expect("UUID-prefix object"),
                _ => {}
            }
            let error = disk
                .delete_unleased(
                    "recovery",
                    &path,
                    &DeleteOptions {
                        recursive: true,
                        old_data_dir: Some(transaction),
                        ..Default::default()
                    },
                )
                .await
                .expect_err("unproven or non-regular entries must prevent all cleanup");
            assert_eq!(error, DiskError::FileCorrupt, "shape={shape}");
            assert_eq!(
                fs::read(data.join("part.1")).await.expect("payload survives"),
                b"preserve",
                "shape={shape}"
            );
            assert!(data.join(old).exists(), "reservation survives failed preflight");
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn delete_recovery_legacy_reservation_rejects_symlink() {
        let (temp, disk, _, _) = fixture().await;
        let transaction = Uuid::new_v4();
        let path = format!("object/{}", Uuid::new_v4());
        let data = disk.io_get_object_path("recovery", &path).expect("data path");
        fs::create_dir(&data).await.expect("data directory");
        let external = temp.path().join("external");
        fs::write(&external, b"external").await.expect("external payload");
        fs::write(data.join("part.1"), b"preserve").await.expect("payload");
        fs::write(data.join(format!("{RECOVERY_DELETE_DATA_DIR_MARKER_PREFIX}{transaction}")), [])
            .await
            .expect("current proof");
        std::os::unix::fs::symlink(
            &external,
            data.join(format!("{RESERVED_DELETE_DATA_DIR_MARKER_PREFIX}{}", Uuid::new_v4())),
        )
        .expect("symlink reservation");
        assert_eq!(
            disk.delete_unleased(
                "recovery",
                &path,
                &DeleteOptions {
                    recursive: true,
                    old_data_dir: Some(transaction),
                    ..Default::default()
                }
            )
            .await
            .expect_err("non-regular reservation must fail closed"),
            DiskError::FileCorrupt
        );
        assert_eq!(fs::read(data.join("part.1")).await.expect("payload survives"), b"preserve");
        assert_eq!(fs::read(external).await.expect("symlink target survives"), b"external");
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
    async fn delete_recovery_activation_preserves_occupied_transaction_directory() {
        let (_source_temp, source, _, _) = fixture().await;
        let transaction = Uuid::new_v4();
        let applied = apply(&source, transaction).await;
        let mut witness = source
            .settle_delete_transaction(
                "recovery",
                "object",
                transaction,
                applied.fingerprint,
                DeleteRecoveryAction::PublishCommit,
            )
            .await
            .unwrap()
            .journal
            .unwrap();
        witness.original_digest = None;
        witness.target_digest = None;
        witness.data_dirs.clear();
        for nested in [false, true] {
            let (_peer_temp, peer, _, _) = fixture().await;
            let root = peer.io_get_object_path("recovery", "object").unwrap();
            fs::remove_file(root.join(STORAGE_FORMAT_FILE)).await.unwrap();
            let retained = root.join(transaction.to_string());
            fs::create_dir(&retained).await.unwrap();
            if nested {
                fs::create_dir(retained.join("prefix-child")).await.unwrap();
            } else {
                fs::write(retained.join("unknown-file"), b"retained").await.unwrap();
            }
            assert!(matches!(
                peer.activate_delete_preparation(&witness, None, None, None).await,
                Err(DiskError::OutdatedXLMeta)
            ));
            assert!(!retained.join(PREPARED).exists());
            if nested {
                assert!(retained.join("prefix-child").is_dir());
            } else {
                assert_eq!(fs::read(retained.join("unknown-file")).await.unwrap(), b"retained");
            }
            assert!(!root.join(STORAGE_FORMAT_FILE).exists());
        }
    }

    #[tokio::test]
    async fn delete_recovery_raw_discovery_keeps_malformed_records_and_later_keys() {
        let (temp, disk, _, _) = fixture().await;
        let transaction = Uuid::new_v4();
        apply(&disk, transaction).await;
        let bad = temp
            .path()
            .join("recovery/object")
            .join(transaction.to_string())
            .join(PREPARED);
        fs::write(&bad, b"{invalid journal").await.expect("corrupt prepared record");
        let legacy = Uuid::new_v4();
        let later = temp.path().join("recovery/later").join(legacy.to_string());
        fs::create_dir_all(&later).await.expect("another hidden key");
        fs::write(later.join(STORAGE_FORMAT_FILE_BACKUP), b"legacy backup")
            .await
            .expect("later transaction");
        let mut cursor = None;
        let mut found = BTreeSet::new();
        loop {
            let page = disk
                .discover_delete_transactions("recovery", None, cursor, 1)
                .await
                .expect("bounded discovery continues");
            found.extend(page.transactions.into_iter().map(|id| (id.object, id.transaction)));
            cursor = page.next;
            if let Some(cursor) = cursor.as_ref() {
                assert!(!disk.renew_delete_recovery_cursor("different-bucket", cursor));
                assert!(disk.renew_delete_recovery_cursor("recovery", cursor));
            }
            if cursor.is_none() {
                break;
            }
        }
        assert!(found.contains(&("object".to_owned(), transaction)));
        assert!(found.contains(&("later".to_owned(), legacy)));
        assert_eq!(
            disk.inspect_delete_transaction("recovery", "object", transaction)
                .await
                .expect_err("inspection still rejects corruption"),
            DiskError::FileCorrupt
        );
        assert_eq!(fs::read(bad).await.expect("corrupt evidence retained"), b"{invalid journal");
        assert_eq!(
            fs::read(later.join(STORAGE_FORMAT_FILE_BACKUP))
                .await
                .expect("legacy evidence retained"),
            b"legacy backup"
        );
    }

    async fn prepare_discovery_key(
        disk: &LocalDisk,
        object: &str,
        transaction: Uuid,
        metadata: &[u8],
    ) -> DeleteRecoveryInspection {
        let root = disk.io_get_object_path("recovery", object).expect("discovery fixture path");
        fs::create_dir_all(&root).await.expect("discovery fixture directory");
        fs::write(root.join(STORAGE_FORMAT_FILE), metadata)
            .await
            .expect("discovery fixture metadata");
        let mut request = request();
        request.name = object.to_owned();
        disk.delete_version_recoverable(
            "recovery",
            object,
            request,
            false,
            DeleteOptions {
                old_data_dir: Some(transaction),
                ..Default::default()
            },
        )
        .await
        .expect("prepare discoverable transaction");
        disk.inspect_delete_transaction("recovery", object, transaction)
            .await
            .expect("healthy prepared transaction")
    }

    async fn discover_all_keys(disk: &LocalDisk, object: Option<&str>) -> BTreeSet<(String, Uuid)> {
        let mut cursor = None;
        let mut found = BTreeSet::new();
        for _ in 0..64 {
            let page = disk
                .discover_delete_transactions("recovery", object, cursor, 1)
                .await
                .expect("record corruption must not stop bounded discovery");
            found.extend(page.transactions.into_iter().map(|id| (id.object, id.transaction)));
            cursor = page.next;
            if cursor.is_none() {
                return found;
            }
        }
        panic!("discovery did not finish the small fixture within its page bound");
    }

    #[tokio::test]
    async fn delete_recovery_raw_discovery_isolates_corrupt_record_bindings() {
        for record_name in [PREPARED, DELETE_QUORUM_COMMITTED_FILE] {
            for damage in ["protocol", "volume", "transaction", "object", "unsafe_object", "invalid_json"] {
                let (temp, disk, _, original) = fixture().await;
                let transaction = Uuid::new_v4();
                let prepared = apply(&disk, transaction).await;
                let root = temp.path().join("recovery/object").join(transaction.to_string());
                if record_name == DELETE_QUORUM_COMMITTED_FILE {
                    disk.settle_delete_transaction(
                        "recovery",
                        "object",
                        transaction,
                        prepared.fingerprint,
                        DeleteRecoveryAction::PublishCommit,
                    )
                    .await
                    .expect("write receipt fixture");
                    fs::remove_file(root.join(PREPARED))
                        .await
                        .expect("discover the receipt without its prepared journal");
                }
                let later = Uuid::new_v4();
                let later_before = prepare_discovery_key(&disk, "later", later, &original).await;
                let path = root.join(record_name);
                let mut record: serde_json::Value =
                    serde_json::from_slice(&fs::read(&path).await.expect("original record")).expect("valid fixture record");
                let journal = if record_name == PREPARED {
                    &mut record
                } else {
                    record.get_mut("journal").expect("receipt journal")
                };
                match damage {
                    "protocol" => journal["protocol_version"] = serde_json::json!(DELETE_RECOVERY_PROTOCOL_VERSION + 1),
                    "volume" => journal["volume"] = serde_json::json!("another-bucket"),
                    "transaction" => journal["transaction"] = serde_json::json!(Uuid::new_v4()),
                    "object" => journal["object"] = serde_json::json!("another-key"),
                    "unsafe_object" => journal["object"] = serde_json::json!("../another-key"),
                    "invalid_json" => {}
                    _ => unreachable!("fixture damage is exhaustive"),
                }
                let bytes = if damage == "invalid_json" {
                    b"{invalid record".to_vec()
                } else {
                    serde_json::to_vec(&record).expect("encode damaged record")
                };
                fs::write(&path, &bytes).await.expect("write corrupt record");

                let found = discover_all_keys(&disk, None).await;
                assert_eq!(
                    found,
                    BTreeSet::from([("object".to_owned(), transaction), ("later".to_owned(), later)]),
                    "{record_name}: {damage}"
                );
                assert_eq!(
                    discover_all_keys(&disk, Some("object")).await,
                    BTreeSet::from([("object".to_owned(), transaction)])
                );
                assert_eq!(
                    disk.inspect_delete_transaction("recovery", "object", transaction)
                        .await
                        .expect_err("canonical inspection must reject corrupt record bindings"),
                    DiskError::FileCorrupt,
                    "{record_name}: {damage}"
                );
                assert_eq!(fs::read(path).await.expect("retained corrupt evidence"), bytes);
                assert_eq!(
                    disk.inspect_delete_transaction("recovery", "later", later)
                        .await
                        .expect("later transaction remains healthy")
                        .fingerprint,
                    later_before.fingerprint,
                    "discovery does not mutate a later healthy transaction"
                );
            }
        }
    }

    #[tokio::test]
    async fn delete_recovery_raw_discovery_keeps_corrupt_write_backups() {
        for corrupt_current in [false, true] {
            let (temp, disk, _, original) = fixture().await;
            let transaction = Uuid::new_v4();
            let root = temp.path().join("recovery/object");
            let backup = root.join(transaction.to_string()).join(STORAGE_FORMAT_FILE_BACKUP);
            fs::create_dir_all(backup.parent().expect("rollback directory"))
                .await
                .expect("rollback directory");
            fs::write(&backup, &original).await.expect("valid write backup");
            let damaged = if corrupt_current {
                root.join(STORAGE_FORMAT_FILE)
            } else {
                backup
            };
            fs::write(&damaged, b"corrupt metadata")
                .await
                .expect("record-local metadata corruption");
            let later = Uuid::new_v4();
            prepare_discovery_key(&disk, "later", later, &original).await;

            assert!(!live_write_backup(&root, transaction).expect("corrupt decoded metadata is not a proven live backup"));
            assert_eq!(
                discover_all_keys(&disk, None).await,
                BTreeSet::from([("object".to_owned(), transaction), ("later".to_owned(), later)])
            );
            assert_eq!(
                disk.inspect_delete_transaction("recovery", "object", transaction)
                    .await
                    .expect_err("inspection rejects corrupt backup metadata"),
                DiskError::FileCorrupt
            );
            assert_eq!(fs::read(damaged).await.expect("corrupt metadata remains untouched"), b"corrupt metadata");
        }
    }

    #[tokio::test]
    async fn delete_recovery_raw_discovery_preserves_bound_directory_keys_and_exact_scope() {
        let (_temp, disk, _, original) = fixture().await;
        let directory_transaction = Uuid::new_v4();
        prepare_discovery_key(&disk, "directory/", directory_transaction, &original).await;
        let literal_transaction = Uuid::new_v4();
        prepare_discovery_key(&disk, "literal__XLDIR__", literal_transaction, &original).await;

        assert_eq!(
            discover_all_keys(&disk, None).await,
            BTreeSet::from([
                ("directory/".to_owned(), directory_transaction),
                ("literal__XLDIR__".to_owned(), literal_transaction),
            ])
        );
        assert_eq!(
            discover_all_keys(&disk, Some("directory/")).await,
            BTreeSet::from([("directory/".to_owned(), directory_transaction)])
        );
        assert_eq!(
            discover_all_keys(&disk, Some("directory")).await,
            BTreeSet::from([("directory".to_owned(), directory_transaction)]),
            "a journal cannot redirect an exact object scope to a trailing-slash alias"
        );
        assert_eq!(
            disk.inspect_delete_transaction("recovery", "directory", directory_transaction)
                .await
                .expect_err("the alternate scope is not journal authority"),
            DiskError::FileCorrupt
        );
        assert_eq!(
            disk.inspect_delete_transaction("recovery", "directory/", directory_transaction)
                .await
                .expect("the bound trailing-slash object remains valid")
                .state,
            DeleteRecoveryState::NewPrepared
        );

        let literal_record = disk
            .io_get_object_path("recovery", "literal__XLDIR__")
            .expect("literal key path")
            .join(literal_transaction.to_string())
            .join(PREPARED);
        let mut record: DeleteRecoveryJournal =
            serde_json::from_slice(&fs::read(&literal_record).await.expect("literal journal")).expect("valid literal journal");
        record.object = "literal/".to_owned();
        let damaged = serde_json::to_vec(&record).expect("encode invalid literal binding");
        fs::write(&literal_record, &damaged)
            .await
            .expect("inject invalid literal binding");
        let found = discover_all_keys(&disk, None).await;
        assert!(found.contains(&("literal__XLDIR__".to_owned(), literal_transaction)));
        assert!(
            !found.contains(&("literal/".to_owned(), literal_transaction)),
            "physical suffixes are never decoded into authority"
        );
        assert_eq!(
            disk.inspect_delete_transaction("recovery", "literal__XLDIR__", literal_transaction)
                .await
                .expect_err("the physical fallback cannot certify an invalid logical binding"),
            DiskError::FileCorrupt
        );
        assert_eq!(fs::read(literal_record).await.expect("retained invalid literal binding"), damaged);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn delete_recovery_raw_discovery_keeps_structural_backup_errors_fatal() {
        let (temp, disk, _, original) = fixture().await;
        let transaction = Uuid::new_v4();
        let root = temp.path().join("recovery/object");
        let rollback = root.join(transaction.to_string());
        fs::create_dir_all(&rollback).await.expect("backup directory");
        fs::write(rollback.join(STORAGE_FORMAT_FILE_BACKUP), &original)
            .await
            .expect("valid backup metadata");
        let outside = tempfile::tempdir().expect("external target");
        let sentinel = outside.path().join("sentinel");
        fs::write(&sentinel, b"external bytes").await.expect("external sentinel");
        std::os::unix::fs::symlink(&sentinel, rollback.join(TARGET)).expect("structurally invalid delete target");

        assert!(
            live_write_backup(&root, transaction).is_err(),
            "only decoded record corruption is isolated"
        );
        assert!(
            disk.discover_delete_transactions("recovery", Some("object"), None, 128)
                .await
                .is_err(),
            "a structural error must terminate discovery"
        );
        assert_eq!(fs::read(sentinel).await.expect("unchanged external sentinel"), b"external bytes");
        assert_eq!(
            fs::read(rollback.join(STORAGE_FORMAT_FILE_BACKUP))
                .await
                .expect("unchanged backup"),
            original
        );
    }

    #[tokio::test]
    async fn delete_recovery_bounded_inspection_charges_root_and_uuid_children() {
        let (_temp, disk, _, _) = fixture().await;
        let transaction = Uuid::new_v4();
        apply(&disk, transaction).await;
        let full = disk
            .inspect_delete_transaction_bounded("recovery", "object", transaction, 65536)
            .await
            .expect("full bounded inspection");
        assert!(full.visited > 1, "both root and transaction directory entries are charged");
        let error = disk
            .inspect_delete_transaction_bounded("recovery", "object", transaction, full.visited - 1)
            .await
            .expect_err("nested entry exceeds allowance");
        assert_eq!(error, DiskError::DeleteRecoveryWorkLimit);
        let unchanged = disk
            .inspect_delete_transaction("recovery", "object", transaction)
            .await
            .expect("unchanged proof");
        assert_eq!(full.inspection.fingerprint, unchanged.fingerprint);
    }

    #[tokio::test]
    async fn delete_recovery_receipt_publication_distinguishes_deferred_inspection_from_write_failure() {
        for write_fails in [false, true] {
            let (_temp, disk, _, _) = fixture().await;
            let transaction = Uuid::new_v4();
            apply(&disk, transaction).await;
            let full = disk
                .inspect_delete_transaction_bounded("recovery", "object", transaction, 65536)
                .await
                .expect("measure preparation work");
            let receipt = disk
                .io_get_object_path("recovery", &format!("object/{transaction}/{DELETE_QUORUM_COMMITTED_FILE}"))
                .expect("receipt path");
            if write_fails {
                let destination = receipt.clone();
                set_owned_file_before_open(&receipt, move || std::fs::create_dir(&destination).expect("reject receipt rename"));
            }
            let result = disk
                .settle_delete_transaction_bounded(
                    "recovery",
                    super::super::super::BoundedDeleteRecoverySettlement {
                        object: "object".to_owned(),
                        transaction,
                        expected_fingerprint: full.inspection.fingerprint,
                        action: DeleteRecoveryAction::PublishCommit,
                        max_entries: full.visited,
                    },
                    None,
                )
                .await;
            if write_fails {
                assert!(result.is_err(), "a failed receipt write never counts as publication");
                assert!(receipt.is_dir());
            } else {
                let result = result.expect("persisted receipt is acknowledged independently of post-write inspection");
                assert!(matches!(
                    result.outcome,
                    super::super::super::DeleteRecoverySettlementOutcome::ReceiptPublishedInspectionDeferred
                ));
                assert_eq!(result.visited, full.visited);
                let state = disk
                    .inspect_delete_transaction("recovery", "object", transaction)
                    .await
                    .expect("durable receipt");
                assert_eq!(state.state, DeleteRecoveryState::QuorumCommitted);
            }
        }
    }

    #[tokio::test]
    async fn delete_recovery_bounded_discovery_charges_uuid_named_children_once_and_keeps_evidence() {
        let (_temp, disk, _, _) = fixture().await;
        let path = disk.delete_discovery_path("recovery", Some("prefix")).expect("prefix path");
        std::fs::create_dir_all(&path).expect("physical prefix");
        // UUID-named child keys of the folder: each holds its own `xl.meta` and a
        // data directory, neither of which can be evidence for the folder's key.
        const CHILDREN: u32 = 2000;
        for _ in 0..CHILDREN {
            let child = path.join(Uuid::new_v4().to_string());
            std::fs::create_dir_all(child.join(Uuid::new_v4().to_string())).expect("child key data dir");
            std::fs::write(child.join(STORAGE_FORMAT_FILE), b"meta").expect("child key metadata");
        }
        // A UUID-named sub-prefix with children of its own.
        const SUB_PREFIX_CHILDREN: u32 = 640;
        let sub_prefix = path.join(Uuid::new_v4().to_string());
        for index in 0..SUB_PREFIX_CHILDREN {
            std::fs::create_dir_all(sub_prefix.join(format!("child-{index}"))).expect("sub-prefix child");
        }
        // Evidence of two interrupted transactions of the key itself: a journaled
        // rollback directory, and a data-directory marker whose rollback is gone.
        let journaled = Uuid::new_v4();
        std::fs::create_dir_all(path.join(journaled.to_string())).expect("rollback directory");
        std::fs::write(path.join(journaled.to_string()).join(PREPARED), b"{}").expect("journal");
        let marker_only = Uuid::new_v4();
        let data_dir = path.join(Uuid::new_v4().to_string());
        std::fs::create_dir_all(&data_dir).expect("residual data directory");
        std::fs::write(data_dir.join(format!("{RESERVED_DELETE_DATA_DIR_MARKER_PREFIX}{marker_only}")), [])
            .expect("reservation marker");

        let page = disk
            .discover_delete_transactions_bounded(
                "recovery",
                "prefix",
                super::super::super::DELETE_RECOVERY_MAX_DISCOVERY_ENTRIES,
            )
            .await
            .expect("bounded probe");
        assert!(page.complete, "a folder of UUID-named keys is a complete inventory");
        // One entry per descent (children, sub-prefix, rollback and data
        // directory) and per evidence file; inert names at 1/64.
        let inert = 2 * CHILDREN + SUB_PREFIX_CHILDREN;
        assert_eq!(page.visited, CHILDREN + 3 + 2 + inert / PLAIN_CHILDREN_PER_BUDGET_ENTRY);
        let found = page.transactions.iter().map(|id| id.transaction).collect::<BTreeSet<_>>();
        assert!(found.contains(&journaled), "a journaled rollback directory is evidence");
        assert!(found.contains(&marker_only), "a marker-only residue is evidence");
        assert_eq!(found.len(), 2, "no child key is mistaken for a transaction");
    }

    #[tokio::test]
    async fn delete_recovery_bounded_discovery_stops_on_request_and_skips_only_inert_irregular_entries() {
        let (_temp, disk, _, _) = fixture().await;
        let path = disk.delete_discovery_path("recovery", Some("prefix")).expect("prefix path");
        let child = path.join(Uuid::new_v4().to_string());
        std::fs::create_dir_all(&child).expect("UUID-named child key");
        // A stop request (cancellation or the wall-clock limit) ends the walk
        // before any entry, leaving continuation state: never evidence of absence.
        let session = disk
            .new_delete_discovery_session("recovery", Some("prefix"), &path)
            .expect("private session");
        let (transactions, next, visited) = session
            .advance_bounded(256, super::super::super::DELETE_RECOVERY_MAX_DISCOVERY_ENTRIES, &|| true)
            .expect("stopped walk");
        assert!(transactions.is_empty());
        assert_eq!(visited, 0);
        assert!(next.is_some(), "a stopped walk is incomplete");
        drop(next);
        #[cfg(unix)]
        {
            // An irregular entry whose name cannot be evidence is skipped unread.
            std::os::unix::fs::symlink(&path, child.join("inert-link")).expect("inert symlink");
            let page = disk
                .discover_delete_transactions_bounded("recovery", "prefix", 4096)
                .await
                .expect("inert irregular entries are not evidence");
            assert!(page.complete && page.transactions.is_empty());
            // One with an evidence name still fails closed.
            std::os::unix::fs::symlink(&path, child.join(PREPARED)).expect("evidence-named symlink");
            assert!(
                disk.discover_delete_transactions_bounded("recovery", "prefix", 4096)
                    .await
                    .is_err(),
                "an evidence-named symlink is corruption, not a skipped name"
            );
        }
    }

    #[tokio::test]
    async fn delete_recovery_bounded_discovery_refuses_partial_inventory_without_public_cursor() {
        let (_temp, disk, _, _) = fixture().await;
        let path = disk.delete_discovery_path("recovery", Some("prefix")).expect("prefix path");
        fs::create_dir_all(&path).await.expect("physical prefix");
        // Transaction-shaped children are the entries a bounded probe pays for.
        for _ in 0..64 {
            fs::create_dir(path.join(Uuid::new_v4().to_string()))
                .await
                .expect("legitimate child");
        }
        for _ in 0..140 {
            let page = disk
                .discover_delete_transactions_bounded("recovery", "prefix", 8)
                .await
                .expect("bounded probe");
            assert_eq!(page.visited, 8);
            assert!(!page.complete, "an empty partial page cannot establish absence");
            assert!(page.transactions.is_empty());
        }
        let page = disk
            .discover_delete_transactions_bounded("recovery", "prefix", 128)
            .await
            .expect("complete probe");
        assert!(page.complete);
        assert_eq!(page.visited, 64);
        assert!(page.transactions.is_empty());
        let transaction = Uuid::new_v4();
        apply(&disk, transaction).await;
        let before = disk
            .inspect_delete_transaction("recovery", "object", transaction)
            .await
            .expect("before probe");
        let page = disk
            .discover_delete_transactions_bounded("recovery", "object", 4096)
            .await
            .expect("complete evidence");
        assert!(page.complete);
        assert!(page.transactions.iter().any(|id| id.transaction == transaction));
        let after = disk
            .inspect_delete_transaction("recovery", "object", transaction)
            .await
            .expect("after probe");
        assert_eq!(before.fingerprint, after.fingerprint);
        assert!(
            DISCOVERY_SESSIONS
                .lock()
                .values()
                .all(|session| session.disk_root != disk.io_root())
        );
    }

    #[tokio::test]
    async fn delete_recovery_cancelled_bounded_discovery_drops_private_session() {
        let (_temp, disk, _, _) = fixture().await;
        let path = disk.delete_discovery_path("recovery", Some("object")).expect("probe scope");
        let lease = os::acquire_namespace_mutation_lease_with_owner(&path, None).await;
        let mut probe = Box::pin(disk.discover_delete_transactions_bounded("recovery", "object", 4096));
        assert!(futures::poll!(probe.as_mut()).is_pending());
        drop(probe);
        assert!(
            DISCOVERY_SESSIONS
                .lock()
                .values()
                .all(|session| session.disk_root != disk.io_root())
        );
        drop(lease);
        assert!(
            disk.discover_delete_transactions_bounded("recovery", "object", 4096)
                .await
                .expect("subsequent probe")
                .complete
        );
    }

    #[tokio::test]
    async fn delete_recovery_evidence_probes_do_not_register_public_cursors() {
        let (_temp, disk, _, _) = fixture().await;
        apply(&disk, Uuid::new_v4()).await;
        for _ in 0..256 {
            assert!(
                disk.delete_recovery_has_evidence("recovery", "object")
                    .await
                    .expect("positive private probe")
            );
        }
        assert!(
            DISCOVERY_SESSIONS
                .lock()
                .values()
                .all(|session| session.disk_root != disk.io_root()),
            "early positive probes must not consume any public cursor slots"
        );
    }

    #[tokio::test]
    async fn delete_recovery_evidence_probe_advances_empty_bounded_pages() {
        let (_temp, disk, _, _) = fixture().await;
        let path = disk.delete_discovery_path("recovery", Some("object")).expect("probe scope");
        let filler_id = Uuid::new_v4().to_string();
        let filler = path.join(&filler_id);
        fs::create_dir_all(&filler).await.expect("non-evidence directory");
        // Inert names are skipped at 1/64, so fill the step with entries that
        // pass the evidence-name filter but are directories: each costs a full
        // budget entry and names no transaction.
        for _ in 0..5000 {
            fs::create_dir(filler.join(format!("{DELETE_DATA_DIR_MARKER_PREFIX}{}", Uuid::new_v4())))
                .await
                .expect("non-evidence entry");
        }
        let session = disk
            .new_delete_discovery_session("recovery", Some("object"), &path)
            .expect("private session");
        let (first, next) = disk
            .advance_delete_discovery(&path, session, 1)
            .await
            .expect("bounded first step");
        assert!(first.is_empty());
        assert!(next.is_some(), "an empty 4096-entry step must retain real continuation state");
        drop(next);
        assert!(
            !disk
                .delete_recovery_has_evidence("recovery", "object")
                .await
                .expect("negative probe reaches EOF")
        );

        let transaction = Uuid::new_v4();
        let proof = path.join(transaction.to_string());
        fs::create_dir_all(&proof).await.expect("later evidence directory");
        fs::write(proof.join(PREPARED), b"{invalid journal")
            .await
            .expect("discoverable record-local corruption");
        let mut session = disk
            .new_delete_discovery_session("recovery", Some("object"), &path)
            .expect("private late-evidence session");
        // Seed a valid DFS continuation so this assertion is independent of
        // filesystem directory ordering: the large empty frame runs first.
        session.frames = vec![
            (transaction.to_string(), std::fs::read_dir(&proof).expect("later evidence frame")),
            (filler_id, std::fs::read_dir(&filler).expect("empty intermediate frame")),
        ];
        assert!(
            session
                .has_evidence(&disk, &path)
                .await
                .expect("late evidence survives an empty intermediate page")
        );
        assert!(
            DISCOVERY_SESSIONS
                .lock()
                .values()
                .all(|session| session.disk_root != disk.io_root()),
            "private continuation never enters the public cursor table"
        );
    }

    #[tokio::test]
    async fn delete_recovery_cancelled_evidence_probe_drops_private_session() {
        let (_temp, disk, _, _) = fixture().await;
        apply(&disk, Uuid::new_v4()).await;
        let path = disk.delete_discovery_path("recovery", Some("object")).expect("probe scope");
        let lease = os::acquire_namespace_mutation_lease_with_owner(&path, None).await;
        let mut probe = Box::pin(disk.delete_recovery_has_evidence("recovery", "object"));
        assert!(
            futures::poll!(probe.as_mut()).is_pending(),
            "the private session waits for the held namespace lease"
        );
        drop(probe);
        assert!(
            DISCOVERY_SESSIONS
                .lock()
                .values()
                .all(|session| session.disk_root != disk.io_root()),
            "cancelling before a page runs must not retain a global cursor"
        );
        drop(lease);
        assert!(
            disk.delete_recovery_has_evidence("recovery", "object")
                .await
                .expect("later probes remain live")
        );
        assert!(
            DISCOVERY_SESSIONS
                .lock()
                .values()
                .all(|session| session.disk_root != disk.io_root())
        );
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

    #[tokio::test]
    async fn delete_recovery_fused_versioned_delete_marker_and_publish_fast_path() {
        let (_temp, disk, info, _) = fixture().await;
        let root = disk.io_get_object_path("recovery", "object").expect("object root");
        let transaction = Uuid::new_v4();
        let mut marker = FileInfo {
            name: "object".to_owned(),
            version_id: Some(Uuid::new_v4()),
            deleted: true,
            mark_deleted: true,
            mod_time: info.mod_time.map(|t| t + time::Duration::seconds(1)),
            ..Default::default()
        };
        let opts = DeleteOptions {
            old_data_dir: Some(transaction),
            recoverable_delete: true,
            ..Default::default()
        };
        let fused = disk
            .try_fused_recoverable_delete_version("recovery", "object", &marker, &opts, transaction, None)
            .await
            .expect("fused delete marker preparation must succeed");
        assert_eq!(fused, Some(()), "versioned delete marker must use fused path");
        assert!(root.join(transaction.to_string()).join(TARGET).is_file());
        assert!(root.join(STORAGE_FORMAT_FILE).is_file());

        let published = disk
            .try_publish_prepared_delete_local("recovery", "object", transaction, None)
            .await
            .expect("local publish must succeed")
            .expect("versioned target must satisfy local publish");
        assert!(published.1, "sole transaction must be true");
        assert!(published.2, "target_satisfied must be true");

        let finalized = disk
            .try_finish_and_finalize_prepared_delete_local("recovery", "object", transaction, &published.0, None)
            .await
            .expect("local finalize must succeed");
        assert!(finalized);
        assert!(!root.join(transaction.to_string()).exists());
        let final_meta = FileMeta::load(&fs::read(root.join(STORAGE_FORMAT_FILE)).await.expect("read final xl.meta"))
            .expect("decode final xl.meta");
        assert_eq!(final_meta.versions.len(), 2);
        assert_eq!(final_meta.versions[0].header.version_id, marker.version_id);
        marker.deleted = false;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn delete_recovery_fused_prepare_checkpoint_reopens_before_head_publication() {
        use std::os::unix::fs::MetadataExt;

        let _mode = durability_mode_override::set(DurabilityMode::Strict);
        let (_temp, disk, mut first, _) = fixture().await;
        first.version_id = Some(Uuid::new_v4());
        let mut second = first.clone();
        second.version_id = Some(Uuid::new_v4());
        let mut meta = FileMeta::new();
        meta.add_version(first).expect("add surviving version");
        meta.add_version(second.clone()).expect("add deleted version");
        let original = meta.marshal_msg().expect("encode original versions");
        let (_, root) = disk.io_get_bucket_and_object_path("recovery", "object").expect("object path");
        fs::write(root.join(STORAGE_FORMAT_FILE), &original)
            .await
            .expect("write versions");

        let (checkpoint_temp, checkpoint_disk, _, _) = fixture().await;
        let checkpoint_root = checkpoint_temp.path().join("recovery/object");
        drop(checkpoint_disk);
        let transaction = Uuid::new_v4();
        let source_root = root.clone();
        let copy_root = checkpoint_root.clone();
        let _capture = fused_prepare_checkpoint::install(&root, move || {
            let source = source_root.join(transaction.to_string());
            let destination = copy_root.join(transaction.to_string());
            std::fs::create_dir(&destination).expect("create checkpoint transaction");
            let source_head = source_root.join(STORAGE_FORMAT_FILE);
            let source_head_meta = std::fs::metadata(&source_head).expect("stat source head");
            let copy_head = copy_root.join(STORAGE_FORMAT_FILE);
            std::fs::copy(&source_head, &copy_head).expect("capture head before publication");
            for entry in std::fs::read_dir(source).expect("read published preparation") {
                let entry = entry.expect("prepared file");
                assert!(entry.file_type().expect("prepared file type").is_file());
                let dst = destination.join(entry.file_name());
                if entry.file_name() == STORAGE_FORMAT_FILE_BACKUP {
                    let backup_meta = entry.metadata().expect("stat prepared backup");
                    assert_eq!(
                        (backup_meta.dev(), backup_meta.ino()),
                        (source_head_meta.dev(), source_head_meta.ino()),
                        "fused preparation must hardlink backup to live head"
                    );
                    std::fs::hard_link(&copy_head, &dst).expect("preserve hardlinked backup");
                } else {
                    std::fs::copy(entry.path(), dst).expect("capture prepared file");
                }
            }
        });
        let opts = DeleteOptions {
            recoverable_delete: true,
            ..Default::default()
        };
        assert_eq!(
            disk.try_fused_recoverable_delete_version("recovery", "object", &second, &opts, transaction, None)
                .await
                .expect("finish original preparation"),
            Some(())
        );
        assert!(checkpoint_root.join(transaction.to_string()).join(PREPARED).is_file());
        let endpoint =
            Endpoint::try_from(checkpoint_temp.path().to_str().expect("checkpoint path")).expect("checkpoint endpoint");
        let reopened = LocalDisk::new(&endpoint, false)
            .await
            .expect("reopen checkpoint as a fresh disk");
        let inspection = reopened
            .delete_recovery_inspect("recovery", "object", transaction)
            .await
            .expect("actual preparation checkpoint must remain recoverable");
        assert_eq!(inspection.state, DeleteRecoveryState::NewPrepared);
        let reopened_tx = reopened
            .io_get_object_path("recovery", &format!("object/{transaction}"))
            .expect("reopened transaction path");
        os::fsync_dir_recorder::set_failure(&reopened_tx, ErrorKind::Other);
        assert!(
            reopened
                .settle_delete_transaction(
                    "recovery",
                    "object",
                    transaction,
                    inspection.fingerprint,
                    DeleteRecoveryAction::Rollback
                )
                .await
                .is_err(),
            "failed transaction directory sync after backup removal must retain journal"
        );
        assert!(reopened_tx.join(PREPARED).is_file());
        assert!(!reopened_tx.join(STORAGE_FORMAT_FILE_BACKUP).exists());
        let retry_inspection = reopened
            .delete_recovery_inspect("recovery", "object", transaction)
            .await
            .expect("inspect interrupted rollback");
        assert_eq!(retry_inspection.state, DeleteRecoveryState::NewPrepared);
        let settled = reopened
            .settle_delete_transaction(
                "recovery",
                "object",
                transaction,
                retry_inspection.fingerprint,
                DeleteRecoveryAction::Rollback,
            )
            .await
            .expect("roll back interrupted preparation");
        assert_eq!(settled.state, DeleteRecoveryState::Absent, "same-inode backup must not survive rollback");
        assert!(!checkpoint_root.join(transaction.to_string()).exists());
        assert_eq!(
            fs::read(checkpoint_root.join(STORAGE_FORMAT_FILE))
                .await
                .expect("read rolled-back versions"),
            original
        );
    }

    #[tokio::test]
    async fn delete_recovery_fused_versioned_delete_with_surviving_version_and_interrupted_target_cleanup() {
        let (_temp, disk, mut v1, _) = fixture().await;
        let root = disk.io_get_object_path("recovery", "object").expect("object path");

        // Configure v1 and v2 with distinct physical data directories so deleting v2
        // exercises both `old_dir` reservation and `is_surviving_data_dir`.
        v1.data = None;
        rustfs_utils::http::metadata_compat::remove_str(
            &mut v1.metadata,
            rustfs_utils::http::metadata_compat::SUFFIX_INLINE_DATA,
        );
        let v1_dir = Uuid::new_v4();
        v1.data_dir = Some(v1_dir);
        fs::create_dir_all(root.join(v1_dir.to_string()))
            .await
            .expect("create v1 dir");
        fs::write(root.join(v1_dir.to_string()).join("part.1"), b"v1-data")
            .await
            .expect("write v1 part");

        let mut v2 = v1.clone();
        v2.version_id = Some(Uuid::new_v4());
        let v2_dir = Uuid::new_v4();
        v2.data_dir = Some(v2_dir);
        fs::create_dir_all(root.join(v2_dir.to_string()))
            .await
            .expect("create v2 dir");
        fs::write(root.join(v2_dir.to_string()).join("part.1"), b"v2-data")
            .await
            .expect("write v2 part");

        let mut meta = FileMeta::new();
        meta.add_version(v1.clone()).expect("add v1");
        meta.add_version(v2.clone()).expect("add v2");
        fs::write(root.join(STORAGE_FORMAT_FILE), meta.marshal_msg().expect("marshal meta"))
            .await
            .expect("write xl.meta");

        let transaction = Uuid::new_v4();
        let opts = DeleteOptions {
            recoverable_delete: true,
            ..Default::default()
        };
        let fused = disk
            .try_fused_recoverable_delete_version("recovery", "object", &v2, &opts, transaction, None)
            .await
            .expect("fused versioned delete preparation must succeed");
        assert_eq!(fused, Some(()));
        assert!(
            root.join(v2_dir.to_string())
                .join(format!("{RECOVERY_DELETE_DATA_DIR_MARKER_PREFIX}{transaction}"))
                .is_file(),
            "deleted version data_dir must have recovery marker"
        );

        let (journal, sole_transaction, target_satisfied) = disk
            .try_publish_prepared_delete_local("recovery", "object", transaction, None)
            .await
            .expect("local publish must succeed")
            .expect("versioned target must satisfy local publish");
        assert!(sole_transaction, "surviving v1 data_dir must not clear sole_transaction");
        assert!(target_satisfied);
        assert!(journal.target_digest.is_some());

        // Simulate interruption during finish-before-finalize right after TARGET is removed
        // (after finished receipt + FINISHED marker were persisted, before receipt_path is removed).
        let rollback = root.join(transaction.to_string());
        let finished_receipt = CommitReceipt {
            journal: journal.clone(),
            finished: true,
        };
        fs::write(
            rollback.join(DELETE_QUORUM_COMMITTED_FILE),
            serde_json::to_vec(&finished_receipt).expect("serialize finished receipt"),
        )
        .await
        .expect("write finished receipt");
        fs::write(rollback.join(FINISHED), []).await.expect("write FINISHED");
        fs::remove_file(rollback.join(STORAGE_FORMAT_FILE_BACKUP))
            .await
            .expect("remove backup");
        fs::remove_file(rollback.join(TARGET)).await.expect("remove TARGET");

        let inspected = disk
            .inspect_delete_transaction("recovery", "object", transaction)
            .await
            .expect("interrupted post-TARGET state must remain inspectable without FileCorrupt");
        assert_eq!(inspected.state, DeleteRecoveryState::CommitFinished);

        // Complete cleanup via try_finish_and_finalize_prepared_delete_local.
        let finalized = disk
            .try_finish_and_finalize_prepared_delete_local("recovery", "object", transaction, &journal, None)
            .await
            .expect("local finalize must succeed after interruption");
        assert!(finalized);
        assert!(!rollback.exists(), "rollback directory must be pruned");
        assert!(!root.join(v2_dir.to_string()).exists(), "deleted v2 data_dir must be removed");
        assert!(
            root.join(v1_dir.to_string()).join("part.1").is_file(),
            "surviving v1 data_dir must remain intact"
        );
    }

    #[tokio::test]
    async fn delete_recovery_fused_unversioned_physical_delete_retains_receipt_until_finalize() {
        let (_temp, disk, mut info, _) = fixture().await;
        let root = disk.io_get_object_path("recovery", "object").expect("object path");

        info.data = None;
        rustfs_utils::http::metadata_compat::remove_str(
            &mut info.metadata,
            rustfs_utils::http::metadata_compat::SUFFIX_INLINE_DATA,
        );
        let data_dir = Uuid::new_v4();
        info.data_dir = Some(data_dir);
        fs::create_dir_all(root.join(data_dir.to_string()))
            .await
            .expect("create data_dir");
        fs::write(root.join(data_dir.to_string()).join("part.1"), b"physical-payload")
            .await
            .expect("write part.1");
        let mut meta = FileMeta::new();
        meta.add_version(info.clone()).expect("add version");
        fs::write(root.join(STORAGE_FORMAT_FILE), meta.marshal_msg().expect("marshal meta"))
            .await
            .expect("write xl.meta");

        let transaction = Uuid::new_v4();
        let opts = DeleteOptions {
            recoverable_delete: true,
            ..Default::default()
        };
        let fused = disk
            .try_fused_recoverable_delete_version("recovery", "object", &info, &opts, transaction, None)
            .await
            .expect("fused unversioned physical delete must succeed");
        assert_eq!(fused, Some(()));

        let (journal, sole_transaction, target_satisfied) = disk
            .try_publish_prepared_delete_local("recovery", "object", transaction, None)
            .await
            .expect("local publish must succeed")
            .expect("absent target must satisfy local publish");
        assert!(sole_transaction);
        assert!(target_satisfied);
        assert!(journal.target_digest.is_none());

        let rollback = root.join(transaction.to_string());
        assert!(rollback.join(DELETE_QUORUM_COMMITTED_FILE).is_file());

        let finalized = disk
            .try_finish_and_finalize_prepared_delete_local("recovery", "object", transaction, &journal, None)
            .await
            .expect("local finalize must succeed");
        assert!(finalized);
        assert!(!rollback.exists(), "rollback directory must be pruned");
        assert!(!root.join(data_dir.to_string()).exists(), "physical data_dir must be removed");
        assert!(!root.exists(), "empty object directory must be pruned");
    }

    #[cfg(all(any(test, feature = "test-util"), not(windows)))]
    #[tokio::test]
    async fn delete_recovery_receipt_formatting_and_post_finished_unlink_interruption() {
        let (_temp, disk, info, _) = fixture().await;
        let root = disk.io_get_object_path("recovery", "object").expect("object path");

        let mut v1 = info.clone();
        v1.version_id = Some(Uuid::new_v4());
        let mut v2 = info.clone();
        v2.version_id = Some(Uuid::new_v4());
        let mut meta = FileMeta::new();
        meta.add_version(v1.clone()).expect("add v1");
        meta.add_version(v2.clone()).expect("add v2");
        fs::write(root.join(STORAGE_FORMAT_FILE), meta.marshal_msg().expect("marshal meta"))
            .await
            .expect("write xl.meta");

        let mut rev_info = v1.clone();
        rev_info
            .metadata
            .extend([("x-amz-meta-z".into(), "1".into()), ("x-amz-meta-a".into(), "2".into())]);
        let mut fwd_info = v1.clone();
        fwd_info
            .metadata
            .extend([("x-amz-meta-a".into(), "2".into()), ("x-amz-meta-z".into(), "1".into())]);
        assert_eq!(
            generation(rev_info).expect("rev").metadata_digest,
            generation(fwd_info).expect("fwd").metadata_digest
        );

        let transaction = Uuid::new_v4();
        let opts = DeleteOptions {
            recoverable_delete: true,
            ..Default::default()
        };
        assert_eq!(
            disk.try_fused_recoverable_delete_version("recovery", "object", &v2, &opts, transaction, None)
                .await
                .expect("fused versioned delete preparation"),
            Some(())
        );
        let (journal, sole_transaction, target_satisfied) = disk
            .try_publish_prepared_delete_local("recovery", "object", transaction, None)
            .await
            .expect("local publish")
            .expect("versioned target satisfied");
        assert!(sole_transaction && target_satisfied && journal.target_digest.is_some());

        let rollback = root.join(transaction.to_string());
        // Real finalizer failure at the second fsync_dir_std(&rollback) (after [TARGET, PREPARED, FINISHED] unlink)
        // must leave a durable finished=true receipt that inspects as CommitFinished.
        let rollback_for_hook = rollback.clone();
        let _hook = os::prepared_publication_test_hooks::install_at(
            os::prepared_publication_test_hooks::Stage::Remove,
            &rollback.join(FINISHED),
            move || {
                os::fsync_dir_recorder::set_failure(&rollback_for_hook, std::io::ErrorKind::Other);
            },
        );
        assert!(
            disk.try_finish_and_finalize_prepared_delete_local("recovery", "object", transaction, &journal, None)
                .await
                .is_err()
        );
        assert!(!rollback.join(FINISHED).exists());
        assert_eq!(
            disk.inspect_delete_transaction("recovery", "object", transaction)
                .await
                .expect("post-FINISHED-unlink failure")
                .state,
            DeleteRecoveryState::CommitFinished
        );
        assert!(
            disk.try_finish_and_finalize_prepared_delete_local("recovery", "object", transaction, &journal, None)
                .await
                .expect("finalize")
        );
        assert!(!rollback.exists());
    }
}

#[cfg(test)]
mod multipart_scope_tests {
    include!("multipart_delete_recovery_scope_tests.rs");
}
