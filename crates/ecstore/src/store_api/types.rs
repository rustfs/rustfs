use super::*;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct MakeBucketOptions {
    pub lock_enabled: bool,
    pub versioning_enabled: bool,
    pub force_create: bool,                 // Create buckets even if they are already created.
    pub created_at: Option<OffsetDateTime>, // only for site replication
    pub no_lock: bool,
}

#[derive(Debug, Default, Clone, PartialEq)]
pub enum SRBucketDeleteOp {
    #[default]
    NoOp,
    MarkDelete,
    Purge,
}

#[derive(Debug, Default, Clone)]
pub struct DeleteBucketOptions {
    pub no_lock: bool,
    pub no_recreate: bool,
    pub force: bool, // Force deletion
    pub srdelete_op: SRBucketDeleteOp,
}

#[derive(Debug, Default, Clone)]
pub struct HTTPPreconditions {
    pub if_match: Option<String>,
    pub if_none_match: Option<String>,
    pub if_modified_since: Option<OffsetDateTime>,
    pub if_unmodified_since: Option<OffsetDateTime>,
}

#[derive(Debug, Default, Clone)]
pub struct ObjectOptions {
    // Use the maximum parity (N/2), used when saving server configuration files
    pub max_parity: bool,
    pub mod_time: Option<OffsetDateTime>,
    pub part_number: Option<usize>,

    pub delete_prefix: bool,
    pub delete_prefix_object: bool,
    pub version_id: Option<String>,
    pub no_lock: bool,

    pub versioned: bool,
    pub version_suspended: bool,

    pub skip_decommissioned: bool,
    pub skip_rebalancing: bool,
    pub skip_free_version: bool,

    pub data_movement: bool,
    pub src_pool_idx: usize,
    pub user_defined: HashMap<String, String>,
    pub preserve_etag: Option<String>,
    pub metadata_chg: bool,
    pub http_preconditions: Option<HTTPPreconditions>,

    pub delete_replication: Option<ReplicationState>,
    pub replication_request: bool,
    pub delete_marker: bool,

    pub transition: TransitionOptions,
    pub expiration: ExpirationOptions,
    pub lifecycle_audit_event: LcAuditEvent,

    pub eval_metadata: Option<HashMap<String, String>>,

    pub want_checksum: Option<Checksum>,
}

impl ObjectOptions {
    pub fn set_delete_replication_state(&mut self, dsc: ReplicateDecision) {
        let mut rs = ReplicationState {
            replicate_decision_str: dsc.to_string(),
            ..Default::default()
        };
        if self.version_id.is_none() {
            rs.replication_status_internal = dsc.pending_status();
            rs.targets = replication_statuses_map(rs.replication_status_internal.as_deref().unwrap_or_default());
        } else {
            rs.version_purge_status_internal = dsc.pending_status();
            rs.purge_targets = version_purge_statuses_map(rs.version_purge_status_internal.as_deref().unwrap_or_default());
        }

        self.delete_replication = Some(rs)
    }

    pub fn set_replica_status(&mut self, status: ReplicationStatusType) {
        if let Some(rs) = self.delete_replication.as_mut() {
            rs.replica_status = status;
            rs.replica_timestamp = Some(OffsetDateTime::now_utc());
        } else {
            self.delete_replication = Some(ReplicationState {
                replica_status: status,
                replica_timestamp: Some(OffsetDateTime::now_utc()),
                ..Default::default()
            });
        }
    }

    pub fn version_purge_status(&self) -> VersionPurgeStatusType {
        self.delete_replication
            .as_ref()
            .map(|v| v.composite_version_purge_status())
            .unwrap_or(VersionPurgeStatusType::Empty)
    }

    pub fn delete_marker_replication_status(&self) -> ReplicationStatusType {
        self.delete_replication
            .as_ref()
            .map(|v| v.composite_replication_status())
            .unwrap_or(ReplicationStatusType::Empty)
    }

    pub fn put_replication_state(&self) -> ReplicationState {
        let rs = match self
            .user_defined
            .get(format!("{RESERVED_METADATA_PREFIX_LOWER}{REPLICATION_STATUS}").as_str())
        {
            Some(v) => v.to_string(),
            None => return ReplicationState::default(),
        };

        ReplicationState {
            replication_status_internal: Some(rs.to_string()),
            targets: replication_statuses_map(rs.as_str()),
            ..Default::default()
        }
    }

    pub fn precondition_check(&self, obj_info: &ObjectInfo) -> Result<()> {
        let has_valid_mod_time = obj_info.mod_time.is_some_and(|t| t != OffsetDateTime::UNIX_EPOCH);

        if let Some(part_number) = self.part_number
            && part_number > 1
            && !obj_info.parts.is_empty()
        {
            let part_found = obj_info.parts.iter().any(|pi| pi.number == part_number);
            if !part_found {
                return Err(Error::InvalidPartNumber(part_number));
            }
        }

        if let Some(pre) = &self.http_preconditions {
            if let Some(if_none_match) = &pre.if_none_match
                && let Some(etag) = &obj_info.etag
                && is_etag_equal(etag, if_none_match)
            {
                return Err(Error::NotModified);
            }

            if has_valid_mod_time
                && let Some(if_modified_since) = &pre.if_modified_since
                && let Some(mod_time) = &obj_info.mod_time
                && !is_modified_since(mod_time, if_modified_since)
            {
                return Err(Error::NotModified);
            }

            if let Some(if_match) = &pre.if_match {
                if let Some(etag) = &obj_info.etag {
                    if !is_etag_equal(etag, if_match) {
                        return Err(Error::PreconditionFailed);
                    }
                } else {
                    return Err(Error::PreconditionFailed);
                }
            }
            if has_valid_mod_time
                && pre.if_match.is_none()
                && let Some(if_unmodified_since) = &pre.if_unmodified_since
                && let Some(mod_time) = &obj_info.mod_time
                && is_modified_since(mod_time, if_unmodified_since)
            {
                return Err(Error::PreconditionFailed);
            }
        }

        Ok(())
    }
}

fn is_etag_equal(etag1: &str, etag2: &str) -> bool {
    let e1 = etag1.trim_matches('"');
    let e2 = etag2.trim_matches('"');
    // Handle wildcard "*" - matches any ETag (per HTTP/1.1 RFC 7232)
    if e2 == "*" {
        return true;
    }
    e1 == e2
}

fn is_modified_since(mod_time: &OffsetDateTime, given_time: &OffsetDateTime) -> bool {
    let mod_secs = mod_time.unix_timestamp();
    let given_secs = given_time.unix_timestamp();
    mod_secs > given_secs
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct BucketOptions {
    pub deleted: bool, // true only when site replication is enabled
    pub cached: bool, // true only when we are requesting a cached response instead of hitting the disk for example ListBuckets() call.
    pub no_metadata: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BucketInfo {
    pub name: String,
    pub created: Option<OffsetDateTime>,
    pub deleted: Option<OffsetDateTime>,
    pub versioning: bool,
    pub object_locking: bool,
}

#[derive(Debug, Default, Clone)]
pub struct MultipartUploadResult {
    pub upload_id: String,
    pub checksum_algo: Option<String>,
    pub checksum_type: Option<String>,
}

#[derive(Debug, Default, Clone)]
pub struct PartInfo {
    pub part_num: usize,
    pub last_mod: Option<OffsetDateTime>,
    pub size: usize,
    pub etag: Option<String>,
    pub actual_size: i64,
}

#[derive(Debug, Clone, Default)]
pub struct CompletePart {
    pub part_num: usize,
    pub etag: Option<String>,
    // pub size: Option<usize>,
    pub checksum_crc32: Option<String>,
    pub checksum_crc32c: Option<String>,
    pub checksum_sha1: Option<String>,
    pub checksum_sha256: Option<String>,
    pub checksum_crc64nvme: Option<String>,
}

impl From<s3s::dto::CompletedPart> for CompletePart {
    fn from(value: s3s::dto::CompletedPart) -> Self {
        Self {
            part_num: value.part_number.unwrap_or_default() as usize,
            etag: value.e_tag.map(|v| v.value().to_owned()),
            checksum_crc32: value.checksum_crc32,
            checksum_crc32c: value.checksum_crc32c,
            checksum_sha1: value.checksum_sha1,
            checksum_sha256: value.checksum_sha256,
            checksum_crc64nvme: value.checksum_crc64nvme,
        }
    }
}

#[derive(Debug, Default)]
pub struct ObjectInfo {
    pub bucket: String,
    pub name: String,
    pub storage_class: Option<String>,
    pub mod_time: Option<OffsetDateTime>,
    pub size: i64,
    // Actual size is the real size of the object uploaded by client.
    pub actual_size: i64,
    pub is_dir: bool,
    pub user_defined: HashMap<String, String>,
    pub parity_blocks: usize,
    pub data_blocks: usize,
    pub version_id: Option<Uuid>,
    pub delete_marker: bool,
    pub transitioned_object: TransitionedObject,
    pub restore_ongoing: bool,
    pub restore_expires: Option<OffsetDateTime>,
    pub user_tags: String,
    pub parts: Vec<ObjectPartInfo>,
    pub is_latest: bool,
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub expires: Option<OffsetDateTime>,
    pub num_versions: usize,
    pub successor_mod_time: Option<OffsetDateTime>,
    pub put_object_reader: Option<PutObjReader>,
    pub etag: Option<String>,
    pub inlined: bool,
    pub metadata_only: bool,
    pub version_only: bool,
    pub replication_status_internal: Option<String>,
    pub replication_status: ReplicationStatusType,
    pub version_purge_status_internal: Option<String>,
    pub version_purge_status: VersionPurgeStatusType,
    pub replication_decision: String,
    pub checksum: Option<Bytes>,
}

impl Clone for ObjectInfo {
    fn clone(&self) -> Self {
        Self {
            bucket: self.bucket.clone(),
            name: self.name.clone(),
            storage_class: self.storage_class.clone(),
            mod_time: self.mod_time,
            size: self.size,
            actual_size: self.actual_size,
            is_dir: self.is_dir,
            user_defined: self.user_defined.clone(),
            parity_blocks: self.parity_blocks,
            data_blocks: self.data_blocks,
            version_id: self.version_id,
            delete_marker: self.delete_marker,
            transitioned_object: self.transitioned_object.clone(),
            restore_ongoing: self.restore_ongoing,
            restore_expires: self.restore_expires,
            user_tags: self.user_tags.clone(),
            parts: self.parts.clone(),
            is_latest: self.is_latest,
            content_type: self.content_type.clone(),
            content_encoding: self.content_encoding.clone(),
            num_versions: self.num_versions,
            successor_mod_time: self.successor_mod_time,
            put_object_reader: None, // reader can not clone
            etag: self.etag.clone(),
            inlined: self.inlined,
            metadata_only: self.metadata_only,
            version_only: self.version_only,
            replication_status_internal: self.replication_status_internal.clone(),
            replication_status: self.replication_status.clone(),
            version_purge_status_internal: self.version_purge_status_internal.clone(),
            version_purge_status: self.version_purge_status.clone(),
            replication_decision: self.replication_decision.clone(),
            checksum: self.checksum.clone(),
            expires: self.expires,
        }
    }
}

impl ObjectInfo {
    pub fn is_compressed(&self) -> bool {
        self.user_defined
            .contains_key(&format!("{RESERVED_METADATA_PREFIX_LOWER}compression"))
    }

    pub fn is_compressed_ok(&self) -> Result<(CompressionAlgorithm, bool)> {
        let scheme = self
            .user_defined
            .get(&format!("{RESERVED_METADATA_PREFIX_LOWER}compression"))
            .cloned();

        if let Some(scheme) = scheme {
            let algorithm = CompressionAlgorithm::from_str(&scheme)?;
            Ok((algorithm, true))
        } else {
            Ok((CompressionAlgorithm::None, false))
        }
    }

    pub fn is_multipart(&self) -> bool {
        self.etag.as_ref().is_some_and(|v| v.len() != 32)
    }

    pub fn get_actual_size(&self) -> std::io::Result<i64> {
        if self.actual_size > 0 {
            return Ok(self.actual_size);
        }

        if self.is_compressed() {
            if let Some(size_str) = self.user_defined.get(&format!("{RESERVED_METADATA_PREFIX_LOWER}actual-size"))
                && !size_str.is_empty()
            {
                // Todo: deal with error
                let size = size_str.parse::<i64>().map_err(|e| std::io::Error::other(e.to_string()))?;
                return Ok(size);
            }
            let mut actual_size = 0;
            self.parts.iter().for_each(|part| {
                actual_size += part.actual_size;
            });
            if actual_size == 0 && actual_size != self.size {
                return Err(std::io::Error::other(format!("invalid decompressed size {} {}", actual_size, self.size)));
            }

            return Ok(actual_size);
        }

        // Check if object is encrypted
        // Managed SSE stores original size in x-rustfs-encryption-original-size metadata
        // SSE-C stores original size in x-amz-server-side-encryption-customer-original-size
        if let Some(size_str) = self
            .user_defined
            .get("x-rustfs-encryption-original-size")
            .or_else(|| self.user_defined.get("x-amz-server-side-encryption-customer-original-size"))
            && !size_str.is_empty()
        {
            let size = size_str
                .parse::<i64>()
                .map_err(|e| std::io::Error::other(format!("Failed to parse encryption original size: {e}")))?;
            return Ok(size);
        }

        Ok(self.size)
    }

    pub fn from_file_info(fi: &FileInfo, bucket: &str, object: &str, versioned: bool) -> ObjectInfo {
        let name = decode_dir_object(object);

        let mut version_id = fi.version_id;

        if versioned && version_id.is_none() {
            version_id = Some(Uuid::nil())
        }

        // etag
        let (content_type, content_encoding, etag) = {
            let content_type = fi.metadata.get("content-type").cloned();
            let content_encoding = fi.metadata.get("content-encoding").cloned();
            let etag = fi.metadata.get("etag").cloned();

            (content_type, content_encoding, etag)
        };

        // tags
        let user_tags = fi.metadata.get(AMZ_OBJECT_TAGGING).cloned().unwrap_or_default();

        let inlined = fi.inline_data();

        // Parse expires from metadata (HTTP date format RFC 7231 or ISO 8601)
        let expires = fi.metadata.get("expires").and_then(|s| {
            // Try parsing as ISO 8601 first
            time::OffsetDateTime::parse(s, &time::format_description::well_known::Iso8601::DEFAULT)
                .or_else(|_| {
                    // Try RFC 2822 format
                    time::OffsetDateTime::parse(s, &time::format_description::well_known::Rfc2822)
                })
                .or_else(|_| {
                    // Try RFC 3339 format
                    time::OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
                })
                .ok()
        });

        let replication_status_internal = fi
            .replication_state_internal
            .as_ref()
            .and_then(|v| v.replication_status_internal.clone());
        let version_purge_status_internal = fi
            .replication_state_internal
            .as_ref()
            .and_then(|v| v.version_purge_status_internal.clone());

        let mut replication_status = fi.replication_status();
        if replication_status.is_empty()
            && let Some(status) = fi.metadata.get(AMZ_BUCKET_REPLICATION_STATUS).cloned()
            && status == ReplicationStatusType::Replica.as_str()
        {
            replication_status = ReplicationStatusType::Replica;
        }

        let version_purge_status = fi.version_purge_status();

        let transitioned_object = TransitionedObject {
            name: fi.transitioned_objname.clone(),
            version_id: if let Some(transition_version_id) = fi.transition_version_id {
                transition_version_id.to_string()
            } else {
                "".to_string()
            },
            status: fi.transition_status.clone(),
            free_version: fi.tier_free_version(),
            tier: fi.transition_tier.clone(),
        };

        let metadata = {
            let mut v = fi.metadata.clone();
            clean_metadata(&mut v);
            v
        };

        // Extract storage class from metadata, default to STANDARD if not found
        let storage_class = if !fi.transition_tier.is_empty() {
            Some(fi.transition_tier.clone())
        } else {
            fi.metadata
                .get(AMZ_STORAGE_CLASS)
                .cloned()
                .or_else(|| Some(storageclass::STANDARD.to_string()))
        };

        let mut restore_ongoing = false;
        let mut restore_expires = None;
        if let Some(restore_status) = fi.metadata.get(AMZ_RESTORE).cloned()
            && let Ok(restore_status) = parse_restore_obj_status(&restore_status)
        {
            restore_ongoing = restore_status.on_going();
            restore_expires = restore_status.expiry();
        }

        // Convert parts from rustfs_filemeta::ObjectPartInfo to store_api::ObjectPartInfo
        let parts = fi
            .parts
            .iter()
            .map(|part| ObjectPartInfo {
                etag: part.etag.clone(),
                index: part.index.clone(),
                size: part.size,
                actual_size: part.actual_size,
                mod_time: part.mod_time,
                checksums: part.checksums.clone(),
                number: part.number,
                error: part.error.clone(),
            })
            .collect();

        // TODO: part checksums

        ObjectInfo {
            bucket: bucket.to_string(),
            name,
            is_dir: object.starts_with('/'),
            parity_blocks: fi.erasure.parity_blocks,
            data_blocks: fi.erasure.data_blocks,
            version_id,
            delete_marker: fi.deleted,
            mod_time: fi.mod_time,
            size: fi.size,
            parts,
            is_latest: fi.is_latest,
            user_tags,
            content_type,
            content_encoding,
            expires,
            num_versions: fi.num_versions,
            successor_mod_time: fi.successor_mod_time,
            etag,
            inlined,
            user_defined: metadata,
            transitioned_object,
            checksum: fi.checksum.clone(),
            storage_class,
            restore_ongoing,
            restore_expires,
            replication_status_internal,
            replication_status,
            version_purge_status_internal,
            version_purge_status,
            ..Default::default()
        }
    }

    pub async fn from_meta_cache_entries_sorted_versions(
        entries: &MetaCacheEntriesSorted,
        bucket: &str,
        prefix: &str,
        delimiter: Option<String>,
        after_version_id: Option<Uuid>,
    ) -> Vec<ObjectInfo> {
        let vcfg = get_versioning_config(bucket).await.ok();
        let mut objects = Vec::with_capacity(entries.entries().len());
        let mut prev_prefix = "";
        for entry in entries.entries() {
            if entry.is_object() {
                if let Some(delimiter) = &delimiter {
                    let remaining = if entry.name.starts_with(prefix) {
                        &entry.name[prefix.len()..]
                    } else {
                        entry.name.as_str()
                    };
                    if let Some(idx) = remaining.find(delimiter.as_str()) {
                        let idx = prefix.len() + idx + delimiter.len();
                        if let Some(curr_prefix) = entry.name.get(0..idx) {
                            if curr_prefix == prev_prefix {
                                continue;
                            }

                            prev_prefix = curr_prefix;

                            objects.push(ObjectInfo {
                                is_dir: true,
                                bucket: bucket.to_owned(),
                                name: curr_prefix.to_owned(),
                                ..Default::default()
                            });
                        }
                        continue;
                    }
                }

                let file_infos = match entry.file_info_versions(bucket) {
                    Ok(res) => res,
                    Err(err) => {
                        warn!("file_info_versions err {:?}", err);
                        continue;
                    }
                };

                let versions = if let Some(vid) = after_version_id {
                    if let Some(idx) = file_infos.find_version_index(vid) {
                        &file_infos.versions[idx + 1..]
                    } else {
                        &file_infos.versions
                    }
                } else {
                    &file_infos.versions
                };

                for fi in versions.iter() {
                    if !fi.version_purge_status().is_empty() {
                        continue;
                    }

                    let versioned = vcfg.clone().map(|v| v.0.versioned(&entry.name)).unwrap_or_default();
                    objects.push(ObjectInfo::from_file_info(fi, bucket, &entry.name, versioned));
                }
                continue;
            }

            if entry.is_dir()
                && let Some(delimiter) = &delimiter
                && let Some(idx) = {
                    let remaining = if entry.name.starts_with(prefix) {
                        &entry.name[prefix.len()..]
                    } else {
                        entry.name.as_str()
                    };
                    remaining.find(delimiter.as_str())
                }
            {
                let idx = prefix.len() + idx + delimiter.len();
                if let Some(curr_prefix) = entry.name.get(0..idx) {
                    if curr_prefix == prev_prefix {
                        continue;
                    }

                    prev_prefix = curr_prefix;

                    objects.push(ObjectInfo {
                        is_dir: true,
                        bucket: bucket.to_owned(),
                        name: curr_prefix.to_owned(),
                        ..Default::default()
                    });
                }
            }
        }

        objects
    }

    pub async fn from_meta_cache_entries_sorted_infos(
        entries: &MetaCacheEntriesSorted,
        bucket: &str,
        prefix: &str,
        delimiter: Option<String>,
    ) -> Vec<ObjectInfo> {
        let vcfg = get_versioning_config(bucket).await.ok();
        let mut objects = Vec::with_capacity(entries.entries().len());
        let mut prev_prefix = "";
        for entry in entries.entries() {
            if entry.is_object() {
                if let Some(delimiter) = &delimiter {
                    let remaining = if entry.name.starts_with(prefix) {
                        &entry.name[prefix.len()..]
                    } else {
                        entry.name.as_str()
                    };
                    if let Some(idx) = remaining.find(delimiter.as_str()) {
                        let idx = prefix.len() + idx + delimiter.len();
                        if let Some(curr_prefix) = entry.name.get(0..idx) {
                            if curr_prefix == prev_prefix {
                                continue;
                            }

                            prev_prefix = curr_prefix;

                            objects.push(ObjectInfo {
                                is_dir: true,
                                bucket: bucket.to_owned(),
                                name: curr_prefix.to_owned(),
                                ..Default::default()
                            });
                        }
                        continue;
                    }
                }

                let fi = match entry.to_fileinfo(bucket) {
                    Ok(res) => res,
                    Err(err) => {
                        warn!("file_info_versions err {:?}", err);
                        continue;
                    }
                };

                // TODO:VersionPurgeStatus
                let versioned = vcfg.clone().map(|v| v.0.versioned(&entry.name)).unwrap_or_default();
                objects.push(ObjectInfo::from_file_info(&fi, bucket, &entry.name, versioned));

                continue;
            }

            if entry.is_dir()
                && let Some(delimiter) = &delimiter
                && let Some(idx) = {
                    let remaining = if entry.name.starts_with(prefix) {
                        &entry.name[prefix.len()..]
                    } else {
                        entry.name.as_str()
                    };
                    remaining.find(delimiter.as_str())
                }
            {
                let idx = prefix.len() + idx + delimiter.len();
                if let Some(curr_prefix) = entry.name.get(0..idx) {
                    if curr_prefix == prev_prefix {
                        continue;
                    }

                    prev_prefix = curr_prefix;

                    objects.push(ObjectInfo {
                        is_dir: true,
                        bucket: bucket.to_owned(),
                        name: curr_prefix.to_owned(),
                        ..Default::default()
                    });
                }
            }
        }

        objects
    }

    pub fn replication_state(&self) -> ReplicationState {
        ReplicationState {
            replication_status_internal: self.replication_status_internal.clone(),
            version_purge_status_internal: self.version_purge_status_internal.clone(),
            replicate_decision_str: self.replication_decision.clone(),
            targets: replication_statuses_map(self.replication_status_internal.clone().unwrap_or_default().as_str()),
            purge_targets: version_purge_statuses_map(self.version_purge_status_internal.clone().unwrap_or_default().as_str()),
            reset_statuses_map: self
                .user_defined
                .iter()
                .filter_map(|(k, v)| {
                    if k.starts_with(&format!("{RESERVED_METADATA_PREFIX_LOWER}{REPLICATION_RESET}")) {
                        Some((
                            k.trim_start_matches(&format!("{RESERVED_METADATA_PREFIX_LOWER}{REPLICATION_RESET}-"))
                                .to_string(),
                            v.clone(),
                        ))
                    } else {
                        None
                    }
                })
                .collect(),
            ..Default::default()
        }
    }

    pub fn decrypt_checksums(&self, part: usize, _headers: &HeaderMap) -> Result<(HashMap<String, String>, bool)> {
        if part > 0
            && let Some(checksums) = self.parts.iter().find(|p| p.number == part).and_then(|p| p.checksums.clone())
        {
            return Ok((checksums, true));
        }

        // TODO: decrypt checksums

        if let Some(data) = &self.checksum {
            let (checksums, is_multipart) = rustfs_rio::read_checksums(data.as_ref(), 0);
            return Ok((checksums, is_multipart));
        }

        Ok((HashMap::new(), false))
    }
}

#[derive(Debug, Default)]
pub struct ListObjectsInfo {
    // Indicates whether the returned list objects response is truncated. A
    // value of true indicates that the list was truncated. The list can be truncated
    // if the number of objects exceeds the limit allowed or specified
    // by max keys.
    pub is_truncated: bool,

    // When response is truncated (the IsTruncated element value in the response
    // is true), you can use the key name in this field as marker in the subsequent
    // request to get next set of objects.
    pub next_marker: Option<String>,

    // List of objects info for this request.
    pub objects: Vec<ObjectInfo>,

    // List of prefixes for this request.
    pub prefixes: Vec<String>,
}

#[derive(Debug, Default)]
pub struct ListObjectsV2Info {
    // Indicates whether the returned list objects response is truncated. A
    // value of true indicates that the list was truncated. The list can be truncated
    // if the number of objects exceeds the limit allowed or specified
    // by max keys.
    pub is_truncated: bool,

    // When response is truncated (the IsTruncated element value in the response
    // is true), you can use the key name in this field as marker in the subsequent
    // request to get next set of objects.
    //
    // NOTE: This element is returned only if you have delimiter request parameter
    // specified.
    pub continuation_token: Option<String>,
    pub next_continuation_token: Option<String>,

    // List of objects info for this request.
    pub objects: Vec<ObjectInfo>,

    // List of prefixes for this request.
    pub prefixes: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct MultipartInfo {
    // Name of the bucket.
    pub bucket: String,

    // Name of the object.
    pub object: String,

    // Upload ID identifying the multipart upload whose parts are being listed.
    pub upload_id: String,

    // Date and time at which the multipart upload was initiated.
    pub initiated: Option<OffsetDateTime>,

    // Any metadata set during InitMultipartUpload, including encryption headers.
    pub user_defined: HashMap<String, String>,
}

// ListMultipartsInfo - represents bucket resources for incomplete multipart uploads.
#[derive(Debug, Clone, Default)]
pub struct ListMultipartsInfo {
    // Together with upload-id-marker, this parameter specifies the multipart upload
    // after which listing should begin.
    pub key_marker: Option<String>,

    // Together with key-marker, specifies the multipart upload after which listing
    // should begin. If key-marker is not specified, the upload-id-marker parameter
    // is ignored.
    pub upload_id_marker: Option<String>,

    // When a list is truncated, this element specifies the value that should be
    // used for the key-marker request parameter in a subsequent request.
    pub next_key_marker: Option<String>,

    // When a list is truncated, this element specifies the value that should be
    // used for the upload-id-marker request parameter in a subsequent request.
    pub next_upload_id_marker: Option<String>,

    // Maximum number of multipart uploads that could have been included in the
    // response.
    pub max_uploads: usize,

    // Indicates whether the returned list of multipart uploads is truncated. A
    // value of true indicates that the list was truncated. The list can be truncated
    // if the number of multipart uploads exceeds the limit allowed or specified
    // by max uploads.
    pub is_truncated: bool,

    // List of all pending uploads.
    pub uploads: Vec<MultipartInfo>,

    // When a prefix is provided in the request, The result contains only keys
    // starting with the specified prefix.
    pub prefix: String,

    // A character used to truncate the object prefixes.
    // NOTE: only supported delimiter is '/'.
    pub delimiter: Option<String>,

    // CommonPrefixes contains all (if there are any) keys between Prefix and the
    // next occurrence of the string specified by delimiter.
    pub common_prefixes: Vec<String>,
    // encoding_type: String, // Not supported yet.
}

/// ListPartsInfo - represents list of all parts.
#[derive(Debug, Clone, Default)]
pub struct ListPartsInfo {
    /// Name of the bucket.
    pub bucket: String,

    /// Name of the object.
    pub object: String,

    /// Upload ID identifying the multipart upload whose parts are being listed.
    pub upload_id: String,

    /// The class of storage used to store the object.
    pub storage_class: String,

    /// Part number after which listing begins.
    pub part_number_marker: usize,

    /// When a list is truncated, this element specifies the last part in the list,
    /// as well as the value to use for the part-number-marker request parameter
    /// in a subsequent request.
    pub next_part_number_marker: usize,

    /// Maximum number of parts that were allowed in the response.
    pub max_parts: usize,

    /// Indicates whether the returned list of parts is truncated.
    pub is_truncated: bool,

    /// List of all parts.
    pub parts: Vec<PartInfo>,

    /// Any metadata set during InitMultipartUpload, including encryption headers.
    pub user_defined: HashMap<String, String>,

    /// ChecksumAlgorithm if set
    pub checksum_algorithm: String,

    /// ChecksumType if set
    pub checksum_type: String,
}

#[derive(Debug, Default, Clone)]
pub struct ObjectToDelete {
    pub object_name: String,
    pub version_id: Option<Uuid>,
    pub delete_marker_replication_status: Option<String>,
    pub version_purge_status: Option<VersionPurgeStatusType>,
    pub version_purge_statuses: Option<String>,
    pub replicate_decision_str: Option<String>,
}

impl ObjectToDelete {
    pub fn replication_state(&self) -> ReplicationState {
        ReplicationState {
            replication_status_internal: self.delete_marker_replication_status.clone(),
            version_purge_status_internal: self.version_purge_statuses.clone(),
            replicate_decision_str: self.replicate_decision_str.clone().unwrap_or_default(),
            targets: replication_statuses_map(self.delete_marker_replication_status.as_deref().unwrap_or_default()),
            purge_targets: version_purge_statuses_map(self.version_purge_statuses.as_deref().unwrap_or_default()),
            ..Default::default()
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct DeletedObject {
    pub delete_marker: bool,
    pub delete_marker_version_id: Option<Uuid>,
    pub object_name: String,
    pub version_id: Option<Uuid>,
    // MTime of DeleteMarker on source that needs to be propagated to replica
    pub delete_marker_mtime: Option<OffsetDateTime>,
    // to support delete marker replication
    pub replication_state: Option<ReplicationState>,
    pub found: bool,
    pub force_delete: bool,
}

impl DeletedObject {
    pub fn version_purge_status(&self) -> VersionPurgeStatusType {
        self.replication_state
            .as_ref()
            .map(|v| v.composite_version_purge_status())
            .unwrap_or(VersionPurgeStatusType::Empty)
    }

    pub fn delete_marker_replication_status(&self) -> ReplicationStatusType {
        self.replication_state
            .as_ref()
            .map(|v| v.composite_replication_status())
            .unwrap_or(ReplicationStatusType::Empty)
    }
}

#[derive(Debug, Default, Clone)]
pub struct ListObjectVersionsInfo {
    pub is_truncated: bool,
    pub next_marker: Option<String>,
    pub next_version_idmarker: Option<String>,
    pub objects: Vec<ObjectInfo>,
    pub prefixes: Vec<String>,
}

type WalkFilter = fn(&FileInfo) -> bool;

#[derive(Clone, Default)]
pub struct WalkOptions {
    pub filter: Option<WalkFilter>,           // return WalkFilter returns 'true/false'
    pub marker: Option<String>,               // set to skip until this object
    pub latest_only: bool,                    // returns only latest versions for all matching objects
    pub ask_disks: String,                    // dictates how many disks are being listed
    pub versions_sort: WalkVersionsSortOrder, // sort order for versions of the same object; default: Ascending order in ModTime
    pub limit: usize,                         // maximum number of items, 0 means no limit
}

#[derive(Clone, Default, PartialEq, Eq)]
pub enum WalkVersionsSortOrder {
    #[default]
    Ascending,
    Descending,
}

#[derive(Debug)]
pub struct ObjectInfoOrErr {
    pub item: Option<ObjectInfo>,
    pub err: Option<Error>,
}
